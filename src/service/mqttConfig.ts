/**
 * MQTT Variable Configuration via NATS KV
 *
 * Controls which variables are published to MQTT and their settings (deadband, etc.)
 * Config stored in: mqtt-config-{projectId} bucket
 *
 * Structure:
 *   - mqtt.defaults: { deadband: { value: 0, maxTime: 60000 } }
 *   - mqtt.variables: { "varId": { enabled: true, deadband?: {...} }, ... }
 */

import { type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, StorageType, DiscardPolicy } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { log } from "../utils/logger.ts";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

export type DeadbandConfig = {
  /** Threshold: only publish if change exceeds this amount */
  value: number;
  /** Max time (ms) between publishes regardless of change */
  maxTime?: number;
};

export type MqttVariableConfig = {
  enabled: boolean;
  /** Per-variable deadband (overrides defaults if set) */
  deadband?: DeadbandConfig;
};

export type MqttDefaults = {
  deadband: DeadbandConfig;
};

export type MqttConfig = {
  defaults: MqttDefaults;
  variables: Map<string, MqttVariableConfig>;
};

export type MqttConfigChangeEvent = {
  type: "defaults-changed" | "variable-changed" | "variables-bulk-changed";
  variableIds?: string[];
};

// ═══════════════════════════════════════════════════════════════════════════
// MQTT Config Manager
// ═══════════════════════════════════════════════════════════════════════════

export type MqttConfigManager = {
  /** Current MQTT configuration */
  config: MqttConfig;
  /** Check if a variable is MQTT-enabled */
  isEnabled: (variableId: string) => boolean;
  /** Get effective deadband for a variable (per-var or defaults) */
  getDeadband: (variableId: string) => DeadbandConfig;
  /** Subscribe to config changes */
  onChange: (handler: (event: MqttConfigChangeEvent) => void) => void;
  /** Set defaults */
  setDefaults: (defaults: MqttDefaults) => Promise<void>;
  /** Enable/configure a single variable */
  setVariable: (variableId: string, config: MqttVariableConfig) => Promise<void>;
  /** Bulk enable variables with optional config */
  enableVariables: (variableIds: string[], config?: Partial<MqttVariableConfig>) => Promise<void>;
  /** Bulk disable variables */
  disableVariables: (variableIds: string[]) => Promise<void>;
  /** Get all enabled variable IDs */
  getEnabledVariables: () => string[];
  /** Stop watching for changes */
  stop: () => void;
};

const MQTT_CONFIG_BUCKET_PREFIX = "mqtt-config-";

/**
 * Create an MQTT config manager that uses NATS KV for storage
 */
export async function createMqttConfigManager(
  nc: NatsConnection,
  projectId: string,
): Promise<MqttConfigManager> {
  const bucketName = `${MQTT_CONFIG_BUCKET_PREFIX}${projectId}`;
  const streamName = `KV_${bucketName}`;
  const subjectPrefix = `$KV.${bucketName}`;

  const js = jetstream(nc);
  const jsm = await js.jetstreamManager();

  // Create or verify the KV bucket stream exists
  try {
    await jsm.streams.info(streamName);
    log.eip.debug(`Using MQTT config bucket: ${bucketName}`);
  } catch {
    await jsm.streams.add({
      name: streamName,
      subjects: [`${subjectPrefix}.>`],
      storage: StorageType.File,
      discard: DiscardPolicy.New,
      max_msgs_per_subject: 1,
      max_age: 0,
      allow_rollup_hdrs: true,
    });
    log.eip.info(`Created MQTT config bucket: ${bucketName}`);
  }

  // Current config state
  const config: MqttConfig = {
    defaults: {
      deadband: { value: 0, maxTime: 60000 },
    },
    variables: new Map(),
  };

  // Change handlers
  const changeHandlers: Array<(event: MqttConfigChangeEvent) => void> = [];

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

  // Load existing config from KV
  async function loadConfig(): Promise<void> {
    try {
      // Load defaults
      const defaultsData = await kvGet("mqtt.defaults");
      if (defaultsData && defaultsData.length > 0) {
        try {
          const data = JSON.parse(new TextDecoder().decode(defaultsData));
          config.defaults = {
            deadband: {
              value: data.deadband?.value ?? 0,
              maxTime: data.deadband?.maxTime ?? 60000,
            },
          };
          log.eip.debug(`Loaded MQTT defaults: deadband=${config.defaults.deadband.value}`);
        } catch (err) {
          log.eip.warn(`Failed to parse MQTT defaults: ${err}`);
        }
      }

      // Load variables config
      const variablesData = await kvGet("mqtt.variables");
      if (variablesData && variablesData.length > 0) {
        try {
          const data = JSON.parse(new TextDecoder().decode(variablesData));
          if (typeof data === "object" && data !== null) {
            for (const [varId, varConfig] of Object.entries(data)) {
              const vc = varConfig as MqttVariableConfig;
              config.variables.set(varId, {
                enabled: vc.enabled ?? false,
                deadband: vc.deadband,
              });
            }
          }
          const enabledCount = [...config.variables.values()].filter(v => v.enabled).length;
          log.eip.info(`Loaded MQTT config: ${enabledCount} variables enabled for MQTT`);
        } catch (err) {
          log.eip.warn(`Failed to parse MQTT variables config: ${err}`);
        }
      } else {
        log.eip.info("No MQTT variable configuration found (nothing will be published to MQTT)");
      }
    } catch (err) {
      log.eip.debug(`No existing MQTT config found: ${err}`);
    }
  }

  // Save variables config to KV
  async function saveVariables(): Promise<void> {
    const data: Record<string, MqttVariableConfig> = {};
    for (const [varId, varConfig] of config.variables) {
      data[varId] = varConfig;
    }
    kvPut("mqtt.variables", JSON.stringify(data));
  }

  // Watch for config changes using KV watch API
  let watchAbort = false;
  let kvWatcher: AsyncIterable<unknown> | null = null;

  async function watchConfig(): Promise<void> {
    try {
      const kvm = new Kvm(nc);
      const kv = await kvm.open(bucketName);

      // Watch all keys in the bucket
      const watch = await kv.watch({ key: "mqtt.>" });
      kvWatcher = watch;

      for await (const entry of watch) {
        if (watchAbort) break;

        const key = entry.key;
        const data = entry.string();

        if (key === "mqtt.defaults" && data) {
          try {
            const parsed = JSON.parse(data);
            config.defaults = {
              deadband: {
                value: parsed.deadband?.value ?? 0,
                maxTime: parsed.deadband?.maxTime ?? 60000,
              },
            };
            notifyChange({ type: "defaults-changed" });
            log.eip.info(`MQTT defaults updated: deadband=${config.defaults.deadband.value}`);
          } catch (err) {
            log.eip.warn(`Failed to parse MQTT defaults update: ${err}`);
          }
        }

        if (key === "mqtt.variables" && data) {
          try {
            const parsed = JSON.parse(data);
            const changedIds: string[] = [];

            // Update config
            config.variables.clear();
            if (typeof parsed === "object" && parsed !== null) {
              for (const [varId, varConfig] of Object.entries(parsed)) {
                const vc = varConfig as MqttVariableConfig;
                config.variables.set(varId, {
                  enabled: vc.enabled ?? false,
                  deadband: vc.deadband,
                });
                changedIds.push(varId);
              }
            }

            notifyChange({ type: "variables-bulk-changed", variableIds: changedIds });
            const enabledCount = [...config.variables.values()].filter(v => v.enabled).length;
            log.eip.info(`MQTT variables config updated: ${enabledCount} enabled`);
          } catch (err) {
            log.eip.warn(`Failed to parse MQTT variables update: ${err}`);
          }
        }
      }
    } catch (err) {
      if (!watchAbort) {
        log.eip.error(`MQTT config watch error: ${err}`);
      }
    }
  }

  function notifyChange(event: MqttConfigChangeEvent): void {
    for (const handler of changeHandlers) {
      try {
        handler(event);
      } catch (err) {
        log.eip.error(`MQTT config change handler error: ${err}`);
      }
    }
  }

  // Public API
  const manager: MqttConfigManager = {
    config,

    isEnabled(variableId: string): boolean {
      const varConfig = config.variables.get(variableId);
      return varConfig?.enabled ?? false;
    },

    getDeadband(variableId: string): DeadbandConfig {
      const varConfig = config.variables.get(variableId);
      return varConfig?.deadband ?? config.defaults.deadband;
    },

    onChange(handler) {
      changeHandlers.push(handler);
    },

    async setDefaults(defaults: MqttDefaults) {
      config.defaults = defaults;
      kvPut("mqtt.defaults", JSON.stringify(defaults));
    },

    async setVariable(variableId: string, varConfig: MqttVariableConfig) {
      config.variables.set(variableId, varConfig);
      await saveVariables();
      notifyChange({ type: "variable-changed", variableIds: [variableId] });
    },

    async enableVariables(variableIds: string[], partialConfig?: Partial<MqttVariableConfig>) {
      for (const varId of variableIds) {
        const existing = config.variables.get(varId);
        config.variables.set(varId, {
          enabled: true,
          deadband: partialConfig?.deadband ?? existing?.deadband,
        });
      }
      await saveVariables();
      notifyChange({ type: "variables-bulk-changed", variableIds });
    },

    async disableVariables(variableIds: string[]) {
      for (const varId of variableIds) {
        const existing = config.variables.get(varId);
        if (existing) {
          existing.enabled = false;
        } else {
          config.variables.set(varId, { enabled: false });
        }
      }
      await saveVariables();
      notifyChange({ type: "variables-bulk-changed", variableIds });
    },

    getEnabledVariables(): string[] {
      return [...config.variables.entries()]
        .filter(([_, v]) => v.enabled)
        .map(([id, _]) => id);
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
