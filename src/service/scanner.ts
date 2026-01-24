/**
 * PLC Scanner - handles polling loops for configured PLCs
 *
 * Each enabled PLC gets its own polling loop that:
 * 1. Connects to the PLC
 * 2. Reads configured tags (or browses all if no tags specified)
 * 3. Publishes values to NATS
 * 4. Repeats at the configured scan rate
 */

import { type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, StorageType, DiscardPolicy } from "@nats-io/jetstream";
import {
  createCip,
  destroyCip,
  readTag,
  browseTags,
  type Cip,
  type TagInfo,
} from "../ethernetip/mod.ts";
import { decodeFloat32, decodeUint } from "../ethernetip/encode.ts";
import { log } from "../utils/logger.ts";
import type { PlcConfig, ConfigManager, ConfigChangeEvent } from "./config.ts";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

type PlcConnection = {
  config: PlcConfig;
  cip: Cip | null;
  tags: TagInfo[];
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
): Promise<ScannerManager> {
  const connections = new Map<string, PlcConnection>();

  // Setup variables KV bucket
  const variablesBucket = `plc-variables-${projectId}`;
  // NATS KV stream naming: stream is "KV_<bucket>", subjects are "$KV.<bucket>.>"
  const streamName = `KV_${variablesBucket}`;
  const subjectPrefix = `$KV.${variablesBucket}`;

  const js = jetstream(nc);
  const jsm = await js.jetstreamManager();

  try {
    await jsm.streams.info(streamName);
  } catch {
    await jsm.streams.add({
      name: streamName,
      subjects: [`${subjectPrefix}.>`],
      storage: StorageType.File,
      discard: DiscardPolicy.New,
      max_msgs_per_subject: 1,
      max_age: 0,
      allow_rollup_hdrs: true, // Required for KV delete operations
    });
    log.eip.info(`Created variables bucket: ${variablesBucket}`);
  }

  // NATS topic for real-time data
  const dataTopic = `plc.data.${projectId}`;

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
  // Publishing
  // ─────────────────────────────────────────────────────────────────────────

  function publishValue(
    variableId: string,
    value: number | boolean | string,
    datatype: string,
    quality: "good" | "bad" = "good",
  ): void {
    const message = {
      projectId,
      variableId,
      value,
      lastUpdated: Date.now(),
      datatype: getNatsDatatype(datatype),
      source: "plc",
      quality,
    };

    const payload = JSON.stringify(message);
    const payloadBytes = new TextEncoder().encode(payload);

    // Publish to topic (real-time)
    nc.publish(`${dataTopic}.${variableId}`, payloadBytes);

    // Store in KV (persistence)
    nc.publish(`${subjectPrefix}.${variableId}`, payloadBytes);
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
        // Filter to atomic types only
        conn.tags = allTags.filter(
          (t) => ATOMIC_TYPES.includes(t.datatype) && !t.isArray,
        );
        log.eip.info(`Found ${conn.tags.length} atomic tags on PLC ${plcId}`);
      } else {
        // Use configured tags
        conn.tags = conn.config.tags.map((name) => ({
          name,
          datatype: "UNKNOWN",
          dimensions: [],
          isStruct: false,
          isArray: false,
          instanceId: 0,
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

      // Read all tags
      let readCount = 0;
      for (const tag of conn.tags) {
        if (conn.abortController.signal.aborted) break;

        try {
          const response = await readTag(conn.cip!, tag.name);
          if (response.success) {
            const { value } = decodeTagValue(response.data, tag.datatype);
            // Use underscore for UDT member access in variable IDs
            const variableId = tag.name.replace(/\./g, "_");
            publishValue(variableId, value, tag.datatype, "good");
            readCount++;
          } else {
            log.eip.debug(`Failed to read ${tag.name}: status 0x${response.status.toString(16)}`);
          }
        } catch (err) {
          log.eip.warn(`Error reading ${tag.name}: ${err}`);
          // Connection might be broken
          await disconnectPlc(plcId);
          break;
        }
      }

      if (readCount > 0) {
        log.eip.debug(`PLC ${plcId}: read ${readCount}/${conn.tags.length} tags`);
      }

      // Calculate time to wait
      const elapsed = Date.now() - startTime;
      const waitTime = Math.max(0, conn.config.scanRate - elapsed);

      if (waitTime > 0) {
        await sleep(waitTime, conn.abortController.signal);
      }
    }

    conn.polling = false;
    log.eip.info(`Stopped polling loop for PLC ${plcId}`);
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
    const { type, plcId, config } = event;

    if (type === "plc-added" && config) {
      // New PLC added
      const conn: PlcConnection = {
        config,
        cip: null,
        tags: [],
        polling: false,
        abortController: new AbortController(),
      };
      connections.set(plcId, conn);

      if (config.enabled) {
        pollPlc(plcId); // Start polling (async, non-blocking)
      }
    } else if (type === "plc-updated" && config) {
      // PLC config updated
      const conn = connections.get(plcId);
      if (conn) {
        const wasEnabled = conn.config.enabled;
        conn.config = config;

        // Handle enable/disable
        if (!wasEnabled && config.enabled) {
          // Start polling
          conn.abortController = new AbortController();
          pollPlc(plcId);
        } else if (wasEnabled && !config.enabled) {
          // Stop polling
          conn.abortController.abort();
        }
      } else if (config.enabled) {
        // Treat as new
        handleConfigChange({ type: "plc-added", plcId, config });
      }
    } else if (type === "plc-removed") {
      // PLC removed
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
      // Subscribe to config changes
      configManager.onChange(handleConfigChange);

      // Start polling for all enabled PLCs
      for (const [plcId, plcConfig] of configManager.config.plcs) {
        const conn: PlcConnection = {
          config: plcConfig,
          cip: null,
          tags: [],
          polling: false,
          abortController: new AbortController(),
        };
        connections.set(plcId, conn);

        if (plcConfig.enabled) {
          pollPlc(plcId);
        }
      }

      if (connections.size === 0) {
        log.eip.info("No PLCs configured. Waiting for configuration via NATS KV...");
      }
    },

    async stop() {
      // Stop all polling loops
      for (const [plcId, conn] of connections) {
        conn.abortController.abort();
        await disconnectPlc(plcId);
      }
      connections.clear();
    },
  };

  return manager;
}
