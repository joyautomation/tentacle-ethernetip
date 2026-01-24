/**
 * PLC Scanner - handles polling loops for configured PLCs
 *
 * Each enabled PLC gets its own polling loop that:
 * 1. Connects to the PLC
 * 2. Reads configured tags (or browses all if no tags specified)
 * 3. Publishes values to NATS (pub/sub, no KV storage)
 * 4. Repeats at the configured scan rate
 *
 * Also handles NATS request/reply for:
 * - plc.variables.{projectId} - returns current poll list with latest values
 */

import { type NatsConnection, type Subscription } from "@nats-io/transport-deno";
import {
  createCip,
  destroyCip,
  readTag,
  browseTags,
  expandUdtMembers,
  getTemplateId,
  type Cip,
} from "../ethernetip/mod.ts";
import { decodeFloat32, decodeUint } from "../ethernetip/encode.ts";
import { log } from "../utils/logger.ts";
import type { PlcConfig, ConfigManager, ConfigChangeEvent } from "./config.ts";
import type { MqttConfigManager } from "./mqttConfig.ts";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Simplified tag info for polling (subset of DiscoveredTag)
 */
type PollTag = {
  name: string;
  datatype: string;
};

/**
 * Variable with current value (for request/reply)
 */
type VariableInfo = {
  variableId: string;
  value: number | boolean | string | null;
  datatype: string;
  quality: "good" | "bad" | "unknown";
  source: string;
  lastUpdated: number;
};

type PlcConnection = {
  config: PlcConfig;
  cip: Cip | null;
  tags: PollTag[];
  /** Current values for each tag (keyed by variableId) */
  values: Map<string, VariableInfo>;
  polling: boolean;
  abortController: AbortController;
};

export type ScannerManager = {
  /** Start the scanner */
  start: () => void;
  /** Stop all polling loops */
  stop: () => Promise<void>;
};

// ═══════════════════════════════════════════════════════════════════════════
// Scanner
// ═══════════════════════════════════════════════════════════════════════════

const ATOMIC_TYPES = [
  "BOOL", "SINT", "INT", "DINT", "LINT",
  "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL",
];

/**
 * Create a scanner that manages polling loops for all configured PLCs
 */
export async function createScanner(
  nc: NatsConnection,
  projectId: string,
  configManager: ConfigManager,
  mqttConfigManager?: MqttConfigManager,
): Promise<ScannerManager> {
  const connections = new Map<string, PlcConnection>();
  let requestSub: Subscription | null = null;

  // NATS subjects for real-time data (pub/sub)
  const dataSubject = `plc.data.${projectId}`;
  const mqttDataSubjectBase = `plc.data.${projectId}`;

  // NATS subject for request/reply (get current variables)
  const variablesSubject = `plc.variables.${projectId}`;

  /**
   * Sanitize variableId for use in NATS subject
   * Replaces dots with underscores so NATS doesn't treat them as level separators
   */
  function sanitizeForSubject(variableId: string): string {
    return variableId.replace(/\./g, "_");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Value decoding
  // ─────────────────────────────────────────────────────────────────────────

  function decodeTagValue(
    data: Uint8Array,
    datatype: string,
  ): { value: number | boolean | string; typeCode: number } {
    const typeCode = decodeUint(data.subarray(0, 2));
    const valueData = data.subarray(2);

    if (datatype === "REAL" || typeCode === 0xca) {
      return { value: decodeFloat32(valueData), typeCode };
    } else if (datatype === "LREAL" || typeCode === 0xcb) {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 8);
      return { value: view.getFloat64(0, true), typeCode };
    } else if (datatype === "BOOL" || typeCode === 0xc1) {
      return { value: valueData[0] !== 0, typeCode };
    } else if (datatype === "SINT" || typeCode === 0xc2) {
      return { value: new Int8Array(valueData.buffer, valueData.byteOffset, 1)[0], typeCode };
    } else if (datatype === "INT" || typeCode === 0xc3) {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 2);
      return { value: view.getInt16(0, true), typeCode };
    } else if (datatype === "DINT" || typeCode === 0xc4) {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 4);
      return { value: view.getInt32(0, true), typeCode };
    } else if (datatype === "USINT" || typeCode === 0xc6) {
      return { value: valueData[0], typeCode };
    } else if (datatype === "UINT" || typeCode === 0xc7) {
      return { value: decodeUint(valueData.subarray(0, 2)), typeCode };
    } else if (datatype === "UDINT" || typeCode === 0xc8) {
      return { value: decodeUint(valueData.subarray(0, 4)), typeCode };
    } else {
      // Unknown type - return hex string
      const hex = Array.from(valueData.slice(0, 8))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
      return { value: `0x${hex}`, typeCode };
    }
  }

  function getNatsDatatype(plcDatatype: string): "number" | "boolean" | "string" {
    if (plcDatatype === "BOOL") return "boolean";
    if (ATOMIC_TYPES.includes(plcDatatype)) return "number";
    return "string";
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Publishing (pub/sub only, no KV)
  // ─────────────────────────────────────────────────────────────────────────

  function publishValue(
    conn: PlcConnection,
    variableId: string,
    value: number | boolean | string,
    datatype: string,
    quality: "good" | "bad" = "good",
  ): void {
    const now = Date.now();
    const natsDatatype = getNatsDatatype(datatype);

    // Store in connection's value cache
    conn.values.set(variableId, {
      variableId,
      value,
      datatype: natsDatatype,
      quality,
      source: "plc",
      lastUpdated: now,
    });

    // Build message matching PlcDataMessage schema from nats-schema
    const message = {
      projectId,
      variableId,
      value,
      timestamp: now,
      datatype: natsDatatype,
    };

    const payload = new TextEncoder().encode(JSON.stringify(message));

    // Publish to flat subject (internal use, tentacle-graphql)
    nc.publish(dataSubject, payload);

    // Only publish to MQTT subject if variable is explicitly enabled
    if (mqttConfigManager?.isEnabled(variableId)) {
      const mqttSubject = `${mqttDataSubjectBase}.${sanitizeForSubject(variableId)}`;
      nc.publish(mqttSubject, payload);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Request/Reply Handler
  // ─────────────────────────────────────────────────────────────────────────

  function getAllVariables(): VariableInfo[] {
    const allVariables: VariableInfo[] = [];

    for (const conn of connections.values()) {
      for (const variable of conn.values.values()) {
        allVariables.push(variable);
      }
    }

    return allVariables;
  }

  async function startRequestHandler(): Promise<void> {
    requestSub = nc.subscribe(variablesSubject);
    log.eip.info(`Listening for variable requests on ${variablesSubject}`);

    for await (const msg of requestSub) {
      try {
        const variables = getAllVariables();
        log.eip.info(`Variables request received, returning ${variables.length} variables`);
        const response = JSON.stringify(variables);
        msg.respond(new TextEncoder().encode(response));
      } catch (err) {
        log.eip.error(`Error handling variables request: ${err}`);
        msg.respond(new TextEncoder().encode("[]"));
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // PLC Connection Management
  // ─────────────────────────────────────────────────────────────────────────

  async function connectPlc(plcId: string): Promise<boolean> {
    const conn = connections.get(plcId);
    if (!conn) return false;

    if (conn.cip) {
      // Already connected
      return true;
    }

    try {
      log.eip.info(`Connecting to PLC ${plcId} at ${conn.config.host}:${conn.config.port}...`);
      conn.cip = await createCip({
        host: conn.config.host,
        port: conn.config.port,
      });
      log.eip.info(`Connected to PLC ${plcId}: ${conn.cip.identity?.productName || "Unknown"}`);

      // Browse tags if not specified
      if (conn.config.tags.length === 0) {
        log.eip.info(`Browsing tags for PLC ${plcId}...`);
        const allTags = await browseTags(conn.cip);

        // Separate atomic and struct tags
        const atomicTags = allTags.filter(
          (t) => ATOMIC_TYPES.includes(t.datatype) && !t.isArray,
        );
        const structTags = allTags.filter(
          (t) => t.isStruct && !t.isArray,
        );

        log.eip.info(`Found ${atomicTags.length} atomic tags, ${structTags.length} struct tags`);

        // Start with atomic tags
        conn.tags = atomicTags.map((t) => ({ name: t.name, datatype: t.datatype }));

        // Expand struct tags synchronously (must complete before polling starts)
        if (structTags.length > 0) {
          log.eip.info(`Expanding ${structTags.length} struct tags...`);
          let expandedCount = 0;

          for (const structTag of structTags) {
            try {
              // Get template ID from symbolType
              const templateId = getTemplateId(structTag.symbolType);
              if (templateId === null) {
                log.eip.debug(`No template ID for ${structTag.name}, skipping`);
                continue;
              }

              // Expand UDT members
              const members = await expandUdtMembers(conn.cip!, structTag.name, templateId);

              // Add expanded members to poll list (already filtered to atomic types)
              for (const member of members) {
                conn.tags.push({ name: member.path, datatype: member.datatype });
                expandedCount++;
              }
            } catch (err) {
              log.eip.debug(`Failed to expand ${structTag.name}: ${err}`);
            }
          }

          log.eip.info(`UDT expansion complete: added ${expandedCount} members, total ${conn.tags.length} tags`);
        }

        log.eip.info(`Ready to poll ${conn.tags.length} tags for PLC ${plcId}`);
      } else {
        // Use configured tags
        conn.tags = conn.config.tags.map((name) => ({
          name,
          datatype: "UNKNOWN",
        }));
        log.eip.info(`Using ${conn.tags.length} configured tags for PLC ${plcId}`);
      }

      return true;
    } catch (err) {
      log.eip.error(`Failed to connect to PLC ${plcId}: ${err}`);
      conn.cip = null;
      return false;
    }
  }

  async function disconnectPlc(plcId: string): Promise<void> {
    const conn = connections.get(plcId);
    if (!conn) return;

    if (conn.cip) {
      try {
        await destroyCip(conn.cip);
      } catch {
        // Ignore disconnect errors
      }
      conn.cip = null;
    }

    // Clear cached values
    conn.values.clear();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Polling Loop
  // ─────────────────────────────────────────────────────────────────────────

  async function pollPlc(plcId: string): Promise<void> {
    const conn = connections.get(plcId);
    if (!conn || !conn.config.enabled) return;

    conn.polling = true;
    log.eip.info(`Starting polling loop for PLC ${plcId} (${conn.config.scanRate}ms)`);

    while (!conn.abortController.signal.aborted && conn.config.enabled) {
      const startTime = Date.now();

      // Ensure connected
      if (!conn.cip) {
        const connected = await connectPlc(plcId);
        if (!connected) {
          // Wait before retry
          await sleep(5000, conn.abortController.signal);
          continue;
        }
      }

      // Read all tags (take snapshot to handle background UDT expansion)
      const tagsToRead = [...conn.tags];
      log.eip.info(`Poll cycle: ${tagsToRead.length} tags to read (conn.tags has ${conn.tags.length})`);
      let readCount = 0;
      let failCount = 0;
      for (const tag of tagsToRead) {
        if (conn.abortController.signal.aborted) break;

        try {
          const response = await readTag(conn.cip!, tag.name);
          if (response.success) {
            const { value } = decodeTagValue(response.data, tag.datatype);
            // Use original tag name as variableId (preserves UDT path like "MyUDT.Member.Value")
            publishValue(conn, tag.name, value, tag.datatype, "good");
            readCount++;
          } else {
            failCount++;
            if (failCount <= 5) {
              log.eip.info(`Read failed for "${tag.name}": status 0x${response.status.toString(16)}`);
            }
          }
        } catch (err) {
          failCount++;
          if (failCount <= 5) {
            log.eip.info(`Error reading "${tag.name}": ${err}`);
          }
        }
      }

      if (readCount > 0 || failCount > 0) {
        log.eip.info(`Polling ${plcId}: ${readCount} success, ${failCount} failed, ${conn.values.size} cached`);
      }

      // Wait for next scan
      const elapsed = Date.now() - startTime;
      const delay = Math.max(0, conn.config.scanRate - elapsed);
      if (delay > 0) {
        await sleep(delay, conn.abortController.signal);
      }
    }

    conn.polling = false;
    log.eip.info(`Stopped polling for PLC ${plcId}`);
  }

  async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      const timeout = setTimeout(resolve, ms);
      signal?.addEventListener("abort", () => {
        clearTimeout(timeout);
        resolve();
      });
    });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Config Change Handling
  // ─────────────────────────────────────────────────────────────────────────

  function handleConfigChange(event: ConfigChangeEvent): void {
    const plcId = event.plcId;

    if (event.type === "plc-added" || event.type === "plc-updated") {
      const config = configManager.getPlc(plcId);
      if (!config) return;

      const existing = connections.get(plcId);
      if (existing) {
        // Update config and restart if needed
        const wasEnabled = existing.config.enabled;
        existing.config = config;

        if (!wasEnabled && config.enabled) {
          // Start polling
          pollPlc(plcId);
        } else if (wasEnabled && !config.enabled) {
          // Stop polling
          existing.abortController.abort();
          existing.abortController = new AbortController();
          disconnectPlc(plcId);
        }
      } else {
        // New PLC
        const conn: PlcConnection = {
          config,
          cip: null,
          tags: [],
          values: new Map(),
          polling: false,
          abortController: new AbortController(),
        };
        connections.set(plcId, conn);

        if (config.enabled) {
          pollPlc(plcId);
        }
      }
    } else if (event.type === "plc-removed") {
      const conn = connections.get(plcId);
      if (conn) {
        conn.abortController.abort();
        disconnectPlc(plcId);
        connections.delete(plcId);
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Public API
  // ─────────────────────────────────────────────────────────────────────────

  const manager: ScannerManager = {
    start() {
      log.eip.info("Starting scanner...");

      // Subscribe to config changes
      configManager.onChange(handleConfigChange);

      // Start request handler (non-blocking)
      startRequestHandler();

      // Initialize connections for existing PLCs
      for (const [plcId, plcConfig] of configManager.config.plcs) {
        const conn: PlcConnection = {
          config: plcConfig,
          cip: null,
          tags: [],
          values: new Map(),
          polling: false,
          abortController: new AbortController(),
        };
        connections.set(plcId, conn);

        if (plcConfig.enabled) {
          pollPlc(plcId);
        }
      }

      log.eip.info(`Scanner started with ${connections.size} PLCs configured`);
    },

    async stop() {
      log.eip.info("Stopping scanner...");

      // Stop request handler
      if (requestSub) {
        requestSub.unsubscribe();
        requestSub = null;
      }

      // Stop all polling loops
      for (const [plcId, conn] of connections) {
        conn.abortController.abort();
        await disconnectPlc(plcId);
      }

      connections.clear();
      log.eip.info("Scanner stopped");
    },
  };

  return manager;
}
