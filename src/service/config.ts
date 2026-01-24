/**
 * Configuration management via NATS KV
 *
 * All config lives in NATS KV bucket: field-config-{projectId}
 * Service watches for changes and applies them in real-time.
 */

import { type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, StorageType, DiscardPolicy } from "@nats-io/jetstream";
import { log } from "../utils/logger.ts";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

export type PlcConfig = {
  id: string;
  host: string;
  port: number;
  scanRate: number; // ms
  enabled: boolean;
  tags: string[]; // Tag names to poll (empty = browse all)
};

export type ServiceConfig = {
  projectId: string;
  plcs: Map<string, PlcConfig>;
};

export type ConfigChangeEvent = {
  type: "plc-added" | "plc-updated" | "plc-removed";
  plcId: string;
  config?: PlcConfig;
};

// ═══════════════════════════════════════════════════════════════════════════
// Config Manager
// ═══════════════════════════════════════════════════════════════════════════

export type ConfigManager = {
  /** Current configuration */
  config: ServiceConfig;
  /** Subscribe to config changes */
  onChange: (handler: (event: ConfigChangeEvent) => void) => void;
  /** Add or update a PLC config */
  setPlc: (plcId: string, config: Omit<PlcConfig, "id">) => Promise<void>;
  /** Remove a PLC config */
  removePlc: (plcId: string) => Promise<void>;
  /** Get a specific PLC config */
  getPlc: (plcId: string) => PlcConfig | undefined;
  /** Stop watching for changes */
  stop: () => void;
};

const CONFIG_BUCKET_PREFIX = "field-config-";

/**
 * Create a config manager that uses NATS KV for storage
 */
export async function createConfigManager(
  nc: NatsConnection,
  projectId: string,
): Promise<ConfigManager> {
  const bucketName = `${CONFIG_BUCKET_PREFIX}${projectId}`;
  // NATS KV stream naming: stream is "KV_<bucket>", subjects are "$KV.<bucket>.>"
  const streamName = `KV_${bucketName}`;
  const subjectPrefix = `$KV.${bucketName}`;

  const js = jetstream(nc);
  const jsm = await js.jetstreamManager();

  // Create or verify the KV bucket stream exists
  try {
    await jsm.streams.info(streamName);
    log.eip.info(`Using config bucket: ${bucketName}`);
  } catch {
    await jsm.streams.add({
      name: streamName,
      subjects: [`${subjectPrefix}.>`],
      storage: StorageType.File,
      discard: DiscardPolicy.New,
      max_msgs_per_subject: 1, // KV semantics - keep only latest
      max_age: 0,
      allow_rollup_hdrs: true, // Required for KV delete operations
    });
    log.eip.info(`Created config bucket: ${bucketName}`);
  }

  // Current config state
  const config: ServiceConfig = {
    projectId,
    plcs: new Map(),
  };

  // Change handlers
  const changeHandlers: Array<(event: ConfigChangeEvent) => void> = [];

  // Helper to get a value from KV
  async function kvGet(key: string): Promise<Uint8Array | null> {
    try {
      const msg = await jsm.streams.getMessage(streamName, {
        last_by_subj: `${subjectPrefix}.${key}`,
      });
      return msg?.data ?? null;
    } catch {
      return null;
    }
  }

  // Helper to put a value to KV
  function kvPut(key: string, value: string): void {
    nc.publish(`${subjectPrefix}.${key}`, new TextEncoder().encode(value));
  }

  // Helper to delete a value from KV
  async function kvDelete(key: string): Promise<void> {
    // In NATS KV, delete is done by publishing an empty message with a purge header
    // For simplicity, we'll just publish an empty value
    nc.publish(`${subjectPrefix}.${key}`, new Uint8Array(0));
  }

  // Load existing config from KV
  async function loadConfig(): Promise<void> {
    try {
      const streamInfo = await jsm.streams.info(streamName, {
        subjects_filter: `${subjectPrefix}.plc.>`,
      });

      const subjects = streamInfo.state.subjects;
      if (!subjects) {
        log.eip.info("No PLC configurations found in KV");
        return;
      }

      // Get unique PLC IDs from subjects like $KV.bucket.plc.{id} and $KV.bucket.plc.{id}.tags
      const plcIds = new Set<string>();
      for (const subject of Object.keys(subjects)) {
        const parts = subject.replace(`${subjectPrefix}.`, "").split(".");
        if (parts[0] === "plc" && parts[1]) {
          plcIds.add(parts[1]);
        }
      }

      // Load each PLC config
      for (const plcId of plcIds) {
        const configData = await kvGet(`plc.${plcId}`);
        if (configData && configData.length > 0) {
          try {
            const data = JSON.parse(new TextDecoder().decode(configData));
            const tagsData = await kvGet(`plc.${plcId}.tags`);
            const tags = tagsData && tagsData.length > 0
              ? JSON.parse(new TextDecoder().decode(tagsData))
              : [];

            const plcConfig: PlcConfig = {
              id: plcId,
              host: data.host || "localhost",
              port: data.port || 44818,
              scanRate: data.scanRate || 1000,
              enabled: data.enabled ?? false,
              tags: Array.isArray(tags) ? tags : [],
            };
            config.plcs.set(plcId, plcConfig);
            log.eip.info(`Loaded PLC config: ${plcId} (${plcConfig.host}, enabled=${plcConfig.enabled})`);
          } catch (err) {
            log.eip.warn(`Failed to parse config for plc.${plcId}: ${err}`);
          }
        }
      }

      log.eip.info(`Loaded ${config.plcs.size} PLC configurations`);
    } catch (err) {
      log.eip.debug(`No existing config found: ${err}`);
    }
  }

  // Watch for config changes
  let watchAbort = false;

  async function watchConfig(): Promise<void> {
    const sub = nc.subscribe(`${subjectPrefix}.plc.>`);

    try {
      for await (const msg of sub) {
        if (watchAbort) break;

        const key = msg.subject.replace(`${subjectPrefix}.`, "");
        const parts = key.split(".");

        // Handle plc.{id} changes
        if (parts.length === 2 && parts[0] === "plc") {
          const plcId = parts[1];

          if (!msg.data || msg.data.length === 0) {
            // PLC removed
            config.plcs.delete(plcId);
            notifyChange({ type: "plc-removed", plcId });
            log.eip.info(`PLC removed: ${plcId}`);
          } else {
            // PLC added or updated
            try {
              const data = JSON.parse(new TextDecoder().decode(msg.data));
              const existing = config.plcs.get(plcId);
              const plcConfig: PlcConfig = {
                id: plcId,
                host: data.host || "localhost",
                port: data.port || 44818,
                scanRate: data.scanRate || 1000,
                enabled: data.enabled ?? false,
                tags: existing?.tags || [],
              };
              config.plcs.set(plcId, plcConfig);

              const eventType = existing ? "plc-updated" : "plc-added";
              notifyChange({ type: eventType, plcId, config: plcConfig });
              log.eip.info(`PLC ${eventType}: ${plcId} (${plcConfig.host}, enabled=${plcConfig.enabled})`);
            } catch (err) {
              log.eip.warn(`Failed to parse PLC config: ${err}`);
            }
          }
        }

        // Handle plc.{id}.tags changes
        if (parts.length === 3 && parts[0] === "plc" && parts[2] === "tags") {
          const plcId = parts[1];
          const plcConfig = config.plcs.get(plcId);

          if (plcConfig && msg.data && msg.data.length > 0) {
            try {
              const tags = JSON.parse(new TextDecoder().decode(msg.data));
              plcConfig.tags = Array.isArray(tags) ? tags : [];
              notifyChange({ type: "plc-updated", plcId, config: plcConfig });
              log.eip.info(`PLC ${plcId} tags updated: ${plcConfig.tags.length} tags`);
            } catch (err) {
              log.eip.warn(`Failed to parse tags for ${plcId}: ${err}`);
            }
          }
        }
      }
    } catch (err) {
      if (!watchAbort) {
        log.eip.error(`Config watch error: ${err}`);
      }
    }
  }

  function notifyChange(event: ConfigChangeEvent): void {
    for (const handler of changeHandlers) {
      try {
        handler(event);
      } catch (err) {
        log.eip.error(`Config change handler error: ${err}`);
      }
    }
  }

  // Public API
  const manager: ConfigManager = {
    config,

    onChange(handler) {
      changeHandlers.push(handler);
    },

    async setPlc(plcId, plcData) {
      const configData = {
        host: plcData.host,
        port: plcData.port,
        scanRate: plcData.scanRate,
        enabled: plcData.enabled,
      };
      kvPut(`plc.${plcId}`, JSON.stringify(configData));
      kvPut(`plc.${plcId}.tags`, JSON.stringify(plcData.tags));
    },

    async removePlc(plcId) {
      await kvDelete(`plc.${plcId}`);
      await kvDelete(`plc.${plcId}.tags`);
    },

    getPlc(plcId) {
      return config.plcs.get(plcId);
    },

    stop() {
      watchAbort = true;
    },
  };

  // Initialize
  await loadConfig();
  watchConfig(); // Start watching (non-blocking)

  return manager;
}
