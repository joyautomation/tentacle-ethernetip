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
import { jetstream } from "@nats-io/jetstream";
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
 * Cached variable with tag info and last known value
 * This is stored in NATS KV and persists across restarts
 */
type CachedVariable = {
  name: string;           // tag name / variableId
  datatype: string;       // PLC datatype (REAL, DINT, etc.)
  value: number | boolean | string | null;
  quality: "good" | "bad" | "unknown";
  lastUpdated: number;
};

/**
 * Variable info for request/reply (API response format)
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
  /** All variables with their current values (keyed by name) */
  variables: Map<string, CachedVariable>;
  polling: boolean;
  abortController: AbortController;
  /** Flag to batch cache saves */
  cacheModified: boolean;
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
  // Tag Cache (NATS KV)
  // ─────────────────────────────────────────────────────────────────────────

  const cacheStreamName = `KV_field-config-${projectId}`;
  const cacheSubjectPrefix = `$KV.field-config-${projectId}`;
  const js = jetstream(nc);
  let jsm: Awaited<ReturnType<typeof js.jetstreamManager>> | null = null;

  async function getJsm() {
    if (!jsm) {
      jsm = await js.jetstreamManager();
    }
    return jsm;
  }

  /**
   * Load cached variables (with values) for a PLC from NATS KV
   */
  async function loadVariableCache(plcId: string): Promise<Map<string, CachedVariable>> {
    const variables = new Map<string, CachedVariable>();
    try {
      const manager = await getJsm();
      const msg = await manager.streams.getMessage(cacheStreamName, {
        last_by_subj: `${cacheSubjectPrefix}.cache.variables.${plcId}`,
      });
      if (msg?.data && msg.data.length > 0) {
        const cached = JSON.parse(new TextDecoder().decode(msg.data)) as CachedVariable[];
        for (const v of cached) {
          variables.set(v.name, v);
        }
        log.eip.info(`Loaded ${variables.size} cached variables for PLC ${plcId}`);
      }
    } catch {
      // No cache exists yet
      log.eip.debug(`No variable cache found for PLC ${plcId}`);
    }
    return variables;
  }

  /**
   * Save variables (with values) to NATS KV cache
   */
  async function saveVariableCache(plcId: string, variables: Map<string, CachedVariable>): Promise<void> {
    const subject = `${cacheSubjectPrefix}.cache.variables.${plcId}`;
    const payload = new TextEncoder().encode(JSON.stringify([...variables.values()]));
    nc.publish(subject, payload);
    await nc.flush(); // Ensure it's persisted before continuing
    log.eip.info(`Saved ${variables.size} variables to cache for PLC ${plcId}`);
  }

  /**
   * Delete variable cache for a PLC
   */
  function deleteVariableCache(plcId: string): void {
    const subject = `${cacheSubjectPrefix}.cache.variables.${plcId}`;
    nc.publish(subject, new Uint8Array(0));
    log.eip.debug(`Cleared variable cache for PLC ${plcId}`);
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

    // Update variable in cache (or create if new)
    const existing = conn.variables.get(variableId);
    if (existing) {
      existing.value = value;
      existing.quality = quality;
      existing.lastUpdated = now;
    } else {
      conn.variables.set(variableId, {
        name: variableId,
        datatype,
        value,
        quality,
        lastUpdated: now,
      });
    }
    conn.cacheModified = true;

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
      for (const cached of conn.variables.values()) {
        allVariables.push({
          variableId: cached.name,
          value: cached.value,
          datatype: getNatsDatatype(cached.datatype),
          quality: cached.quality,
          source: "plc",
          lastUpdated: cached.lastUpdated,
        });
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

        // Build set of discovered tag names with datatypes
        const discoveredTags = new Map<string, string>(); // name -> datatype
        for (const t of atomicTags) {
          discoveredTags.set(t.name, t.datatype);
        }

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

              // Add expanded members (already filtered to atomic types)
              for (const member of members) {
                discoveredTags.set(member.path, member.datatype);
                expandedCount++;
              }
            } catch (err) {
              log.eip.debug(`Failed to expand ${structTag.name}: ${err}`);
            }
          }

          log.eip.info(`UDT expansion complete: added ${expandedCount} members, total ${discoveredTags.size} tags`);
        }

        // Remove stale variables no longer in PLC
        const staleKeys = [...conn.variables.keys()].filter(k => !discoveredTags.has(k));
        if (staleKeys.length > 0) {
          log.eip.info(`Removing ${staleKeys.length} stale variables no longer in PLC`);
          for (const key of staleKeys) {
            conn.variables.delete(key);
          }
        }

        // Add new variables (preserve existing values where possible)
        let newCount = 0;
        for (const [name, datatype] of discoveredTags) {
          if (!conn.variables.has(name)) {
            conn.variables.set(name, {
              name,
              datatype,
              value: null,
              quality: "unknown",
              lastUpdated: 0,
            });
            newCount++;
          } else {
            // Update datatype if changed
            const existing = conn.variables.get(name)!;
            if (existing.datatype !== datatype) {
              existing.datatype = datatype;
            }
          }
        }

        if (newCount > 0) {
          log.eip.info(`Added ${newCount} new variables from browse`);
        }

        // Save updated cache
        conn.cacheModified = true;
        await saveVariableCache(plcId, conn.variables);

        log.eip.info(`Ready to poll ${conn.variables.size} variables for PLC ${plcId}`);
      } else {
        // Use configured tags - add to variables if not already present
        for (const name of conn.config.tags) {
          if (!conn.variables.has(name)) {
            conn.variables.set(name, {
              name,
              datatype: "UNKNOWN",
              value: null,
              quality: "unknown",
              lastUpdated: 0,
            });
          }
        }
        log.eip.info(`Using ${conn.variables.size} configured tags for PLC ${plcId}`);
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
    // Note: Don't clear conn.variables - we want to preserve cached values
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

      // Read all variables (take snapshot in case browse updates during poll)
      const variablesToRead = [...conn.variables.values()];
      log.eip.debug(`Poll cycle: ${variablesToRead.length} variables to read`);
      let readCount = 0;
      let failCount = 0;
      for (const variable of variablesToRead) {
        if (conn.abortController.signal.aborted) break;

        try {
          const response = await readTag(conn.cip!, variable.name);
          if (response.success) {
            const { value } = decodeTagValue(response.data, variable.datatype);
            publishValue(conn, variable.name, value, variable.datatype, "good");
            readCount++;
          } else {
            failCount++;
            if (failCount <= 5) {
              log.eip.debug(`Read failed for "${variable.name}": status 0x${response.status.toString(16)}`);
            }
          }
        } catch (err) {
          failCount++;
          if (failCount <= 5) {
            log.eip.debug(`Error reading "${variable.name}": ${err}`);
          }
        }
      }

      if (readCount > 0 || failCount > 0) {
        log.eip.debug(`Polling ${plcId}: ${readCount} success, ${failCount} failed`);
      }

      // Save cache if modified (batch saves to reduce writes)
      if (conn.cacheModified) {
        conn.cacheModified = false;
        saveVariableCache(plcId, conn.variables).catch((err) => {
          log.eip.warn(`Failed to save variable cache: ${err}`);
        });
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
  // Connection Initialization with Cache
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Initialize a PLC connection, loading cached variables (with values) first
   */
  async function initConnection(plcId: string, config: PlcConfig): Promise<PlcConnection> {
    // Load cached variables (including last known values)
    const cachedVariables = await loadVariableCache(plcId);

    const conn: PlcConnection = {
      config,
      cip: null,
      variables: cachedVariables,
      polling: false,
      abortController: new AbortController(),
      cacheModified: false,
    };

    if (cachedVariables.size > 0) {
      log.eip.info(`PLC ${plcId} initialized with ${cachedVariables.size} cached variables (ready to poll)`);
    }

    return conn;
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
        // New PLC - initialize with cached tags
        initConnection(plcId, config).then((conn) => {
          connections.set(plcId, conn);
          if (config.enabled) {
            pollPlc(plcId);
          }
        });
      }
    } else if (event.type === "plc-removed") {
      const conn = connections.get(plcId);
      if (conn) {
        conn.abortController.abort();
        disconnectPlc(plcId);
        deleteVariableCache(plcId);
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

      // Initialize connections for existing PLCs (async but fire-and-forget)
      (async () => {
        for (const [plcId, plcConfig] of configManager.config.plcs) {
          const conn = await initConnection(plcId, plcConfig);
          connections.set(plcId, conn);

          if (plcConfig.enabled) {
            pollPlc(plcId);
          }
        }
        log.eip.info(`Scanner started with ${connections.size} PLCs configured`);
      })();
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
