/**
 * PLC Scanner - handles polling loops for configured PLCs
 *
 * Subscription-based model:
 * - Browse on demand (fills cache for UI discovery)
 * - Only polls tags that have active subscriptions
 * - Services subscribe/unsubscribe to tags via NATS
 * - MQTT-enabled tags are auto-subscribed
 *
 * NATS topics:
 * - plc.browse.{projectId} - Trigger browse, returns available tags
 * - plc.variables.{projectId} - Returns cached variables from last browse
 * - plc.subscribe.{projectId} - Subscribe tags to polling
 * - plc.unsubscribe.{projectId} - Unsubscribe tags from polling
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
  /** All discovered variables (from browse) with their current values */
  variables: Map<string, CachedVariable>;
  polling: boolean;
  abortController: AbortController;
  /** Flag to batch cache saves */
  cacheModified: boolean;
};

/**
 * Subscription request payload
 */
type SubscribeRequest = {
  tags: string[];
  subscriberId: string;
};

/**
 * Browse request payload
 */
type BrowseRequest = {
  plcId?: string;  // Optional: browse specific PLC, or all if omitted
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

  // Subscription tracking: variableId -> Set of subscriberIds
  const subscriptions = new Map<string, Set<string>>();

  // MQTT deadband tracking: variableId -> last published value and timestamp
  const mqttLastPublish = new Map<string, { value: number | boolean | string | null, timestamp: number }>();

  // NATS subscriptions
  let variablesSub: Subscription | null = null;
  let browseSub: Subscription | null = null;
  let subscribeSub: Subscription | null = null;
  let unsubscribeSub: Subscription | null = null;

  // NATS subjects
  const dataSubject = `plc.data.${projectId}`;
  const mqttDataSubjectBase = `plc.data.${projectId}`;
  const variablesSubject = `plc.variables.${projectId}`;
  const browseSubject = `plc.browse.${projectId}`;
  const subscribeSubject = `plc.subscribe.${projectId}`;
  const unsubscribeSubject = `plc.unsubscribe.${projectId}`;

  /**
   * Sanitize variableId for use in NATS subject
   */
  function sanitizeForSubject(variableId: string): string {
    return variableId.replace(/\./g, "_");
  }

  /**
   * Validate tag name - must be printable ASCII, no weird unicode or garbage
   */
  function isValidTagName(name: string): boolean {
    if (!name || name.length === 0) return false;
    if (!/^[\x20-\x7E]+$/.test(name)) return false;
    if (!/^[A-Za-z_]/.test(name)) return false;
    if (/_member\d+$/.test(name)) return false;
    return true;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Subscription Management
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Subscribe tags to be polled
   */
  function subscribeTags(tags: string[], subscriberId: string): void {
    let addedCount = 0;
    for (const tag of tags) {
      if (!subscriptions.has(tag)) {
        subscriptions.set(tag, new Set());
      }
      const subscribers = subscriptions.get(tag)!;
      if (!subscribers.has(subscriberId)) {
        subscribers.add(subscriberId);
        addedCount++;
      }
    }
    if (addedCount > 0) {
      log.eip.info(`Subscribed ${addedCount} tags for ${subscriberId}, total active: ${subscriptions.size}`);
    }
  }

  /**
   * Unsubscribe tags from polling
   */
  function unsubscribeTags(tags: string[], subscriberId: string): void {
    let removedCount = 0;
    for (const tag of tags) {
      const subscribers = subscriptions.get(tag);
      if (subscribers) {
        if (subscribers.delete(subscriberId)) {
          removedCount++;
        }
        // Remove tag entirely if no subscribers left
        if (subscribers.size === 0) {
          subscriptions.delete(tag);
        }
      }
    }
    if (removedCount > 0) {
      log.eip.info(`Unsubscribed ${removedCount} tags for ${subscriberId}, total active: ${subscriptions.size}`);
    }
  }

  /**
   * Get all tags that have active subscriptions
   */
  function getSubscribedTags(): Set<string> {
    return new Set(subscriptions.keys());
  }

  /**
   * Check if a tag is subscribed
   */
  function isSubscribed(tag: string): boolean {
    const subscribers = subscriptions.get(tag);
    return subscribers !== undefined && subscribers.size > 0;
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
      log.eip.debug(`No variable cache found for PLC ${plcId}`);
    }
    return variables;
  }

  /**
   * Save variables to NATS KV cache
   */
  async function saveVariableCache(plcId: string, variables: Map<string, CachedVariable>): Promise<void> {
    const subject = `${cacheSubjectPrefix}.cache.variables.${plcId}`;
    const validVariables = [...variables.values()].filter(v => !/_member\d+$/.test(v.name));
    const payload = new TextEncoder().encode(JSON.stringify(validVariables));
    nc.publish(subject, payload);
    await nc.flush();
    log.eip.info(`Saved ${validVariables.length} variables to cache for PLC ${plcId}`);
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

  /**
   * Map CIP typeCode to PLC datatype string
   */
  function typeCodeToDatatype(typeCode: number): string | null {
    const TYPE_MAP: Record<number, string> = {
      0xc1: "BOOL",
      0xc2: "SINT",
      0xc3: "INT",
      0xc4: "DINT",
      0xc5: "LINT",
      0xc6: "USINT",
      0xc7: "UINT",
      0xc8: "UDINT",
      0xc9: "ULINT",
      0xca: "REAL",
      0xcb: "LREAL",
    };
    return TYPE_MAP[typeCode] ?? null;
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
    let natsDatatype = getNatsDatatype(datatype);

    // Failsafe: infer correct datatype from actual value
    if (typeof value === "number" && natsDatatype !== "number") {
      log.eip.debug(`Datatype mismatch for ${variableId}: cached=${datatype}, correcting to "number"`);
      natsDatatype = "number";
      const cached = conn.variables.get(variableId);
      if (cached) {
        cached.datatype = "REAL";
        conn.cacheModified = true; // Save cache when datatype is corrected
      }
    } else if (typeof value === "boolean" && natsDatatype !== "boolean") {
      log.eip.debug(`Datatype mismatch for ${variableId}: cached=${datatype}, correcting to "boolean"`);
      natsDatatype = "boolean";
      const cached = conn.variables.get(variableId);
      if (cached) {
        cached.datatype = "BOOL";
        conn.cacheModified = true; // Save cache when datatype is corrected
      }
    }

    // Update variable in memory (not persisted to cache on every update)
    const existing = conn.variables.get(variableId);
    if (existing) {
      existing.value = value;
      existing.quality = quality;
      existing.lastUpdated = now;
    } else {
      // New variable discovered during polling - this is unusual but save it
      conn.variables.set(variableId, {
        name: variableId,
        datatype,
        value,
        quality,
        lastUpdated: now,
      });
      conn.cacheModified = true;
    }

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

    // Publish to MQTT subject if enabled (with deadband filtering)
    if (mqttConfigManager?.isEnabled(variableId)) {
      const rawDeadband = mqttConfigManager.getDeadband(variableId);
      const lastPublish = mqttLastPublish.get(variableId);

      // Determine if we should publish based on deadband
      let shouldPublish = false;

      if (!lastPublish) {
        // First publish for this variable - always publish
        shouldPublish = true;
      } else {
        const timeSinceLastPublish = now - lastPublish.timestamp;

        // Check maxTime (always publish if exceeded)
        if (rawDeadband.maxTime && timeSinceLastPublish >= rawDeadband.maxTime) {
          shouldPublish = true;
        } else if (typeof value === "number" && typeof lastPublish.value === "number") {
          // Numeric comparison: check if change exceeds deadband threshold
          const change = Math.abs(value - lastPublish.value);
          if (change > rawDeadband.value) {
            shouldPublish = true;
          }
        } else if (typeof value === "boolean" || typeof value === "string") {
          // Boolean/string: publish on any change (deadband.value doesn't apply)
          if (value !== lastPublish.value) {
            shouldPublish = true;
          }
        }
      }

      if (shouldPublish) {
        const mqttSubject = `${mqttDataSubjectBase}.${sanitizeForSubject(variableId)}`;
        const deadband = {
          value: rawDeadband.value,
          maxTime: rawDeadband.maxTime ? rawDeadband.maxTime / 1000 : undefined,
        };
        const mqttMessage = { ...message, deadband };
        nc.publish(mqttSubject, new TextEncoder().encode(JSON.stringify(mqttMessage)));

        // Update last publish tracking
        mqttLastPublish.set(variableId, { value, timestamp: now });
        log.eip.debug(`MQTT publish: ${variableId} = ${value} (deadband: ${rawDeadband.value})`);
      } else {
        log.eip.debug(`MQTT filtered: ${variableId} = ${value} (within deadband ${rawDeadband.value}, last=${lastPublish?.value})`);
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Browse Handler (on-demand)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Browse tags from PLC and update cache
   * Does NOT start polling - just discovers available tags
   */
  async function browseAndCacheTags(plcId: string): Promise<VariableInfo[]> {
    const conn = connections.get(plcId);
    if (!conn) {
      log.eip.warn(`Browse requested for unknown PLC: ${plcId}`);
      return [];
    }

    // Ensure connected
    if (!conn.cip) {
      try {
        log.eip.info(`Connecting to PLC ${plcId} for browse...`);
        conn.cip = await createCip({
          host: conn.config.host,
          port: conn.config.port,
        });
        log.eip.info(`Connected to PLC ${plcId}: ${conn.cip.identity?.productName || "Unknown"}`);
      } catch (err) {
        log.eip.error(`Failed to connect to PLC ${plcId} for browse: ${err}`);
        return [];
      }
    }

    log.eip.info(`Browsing tags for PLC ${plcId}...`);
    const allTags = await browseTags(conn.cip);

    const atomicTags = allTags.filter(
      (t) => ATOMIC_TYPES.includes(t.datatype) && !t.isArray,
    );
    const structTags = allTags.filter(
      (t) => t.isStruct && !t.isArray,
    );

    log.eip.info(`Found ${atomicTags.length} atomic tags, ${structTags.length} struct tags`);

    const discoveredTags = new Map<string, string>();
    for (const t of atomicTags) {
      if (isValidTagName(t.name)) {
        discoveredTags.set(t.name, t.datatype);
      }
    }

    // Expand struct tags
    if (structTags.length > 0) {
      log.eip.info(`Expanding ${structTags.length} struct tags...`);
      let expandedCount = 0;

      for (const structTag of structTags) {
        try {
          const templateId = getTemplateId(structTag.symbolType);
          if (templateId === null) continue;

          const members = await expandUdtMembers(conn.cip!, structTag.name, templateId);
          for (const member of members) {
            if (isValidTagName(member.path)) {
              discoveredTags.set(member.path, member.datatype);
              expandedCount++;
            }
          }
        } catch (err) {
          log.eip.debug(`Failed to expand ${structTag.name}: ${err}`);
        }
      }

      log.eip.info(`UDT expansion complete: ${expandedCount} members, total ${discoveredTags.size} tags`);
    }

    // Update cache with discovered tags (preserve existing values)
    for (const [name, datatype] of discoveredTags) {
      if (!conn.variables.has(name)) {
        conn.variables.set(name, {
          name,
          datatype,
          value: null,
          quality: "unknown",
          lastUpdated: 0,
        });
      } else {
        const existing = conn.variables.get(name)!;
        if (existing.datatype !== datatype) {
          existing.datatype = datatype;
        }
      }
    }

    // Remove stale variables
    const staleKeys = [...conn.variables.keys()].filter(k => !discoveredTags.has(k));
    for (const key of staleKeys) {
      conn.variables.delete(key);
    }

    await saveVariableCache(plcId, conn.variables);

    log.eip.info(`Browse complete: ${conn.variables.size} tags cached for PLC ${plcId}`);

    // Return as VariableInfo array
    return [...conn.variables.values()]
      .filter(v => isValidTagName(v.name))
      .map(v => ({
        variableId: v.name,
        value: v.value,
        datatype: getNatsDatatype(v.datatype),
        quality: v.quality,
        source: "plc",
        lastUpdated: v.lastUpdated,
      }));
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Request/Reply Handlers
  // ─────────────────────────────────────────────────────────────────────────

  function getAllVariables(): VariableInfo[] {
    const allVariables: VariableInfo[] = [];

    for (const conn of connections.values()) {
      for (const cached of conn.variables.values()) {
        if (/_member\d+$/.test(cached.name)) continue;
        if (!/^[\x20-\x7E]+$/.test(cached.name)) continue;

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

  async function startRequestHandlers(): Promise<void> {
    // Variables request handler (get cached variables)
    variablesSub = nc.subscribe(variablesSubject);
    log.eip.info(`Listening for variable requests on ${variablesSubject}`);

    (async () => {
      for await (const msg of variablesSub!) {
        try {
          const variables = getAllVariables();
          log.eip.info(`Variables request: returning ${variables.length} cached variables`);
          msg.respond(new TextEncoder().encode(JSON.stringify(variables)));
        } catch (err) {
          log.eip.error(`Error handling variables request: ${err}`);
          msg.respond(new TextEncoder().encode("[]"));
        }
      }
    })();

    // Browse request handler (on-demand discovery)
    browseSub = nc.subscribe(browseSubject);
    log.eip.info(`Listening for browse requests on ${browseSubject}`);

    (async () => {
      for await (const msg of browseSub!) {
        try {
          let request: BrowseRequest = {};
          if (msg.data && msg.data.length > 0) {
            request = JSON.parse(new TextDecoder().decode(msg.data));
          }

          const results: VariableInfo[] = [];

          if (request.plcId) {
            // Browse specific PLC
            const plcResults = await browseAndCacheTags(request.plcId);
            results.push(...plcResults);
          } else {
            // Browse all PLCs
            for (const plcId of connections.keys()) {
              const plcResults = await browseAndCacheTags(plcId);
              results.push(...plcResults);
            }
          }

          log.eip.info(`Browse request: returning ${results.length} tags`);
          msg.respond(new TextEncoder().encode(JSON.stringify(results)));
        } catch (err) {
          log.eip.error(`Error handling browse request: ${err}`);
          msg.respond(new TextEncoder().encode("[]"));
        }
      }
    })();

    // Subscribe request handler
    subscribeSub = nc.subscribe(subscribeSubject);
    log.eip.info(`Listening for subscribe requests on ${subscribeSubject}`);

    (async () => {
      for await (const msg of subscribeSub!) {
        try {
          const request = JSON.parse(new TextDecoder().decode(msg.data)) as SubscribeRequest;
          subscribeTags(request.tags, request.subscriberId);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: true, count: request.tags.length })));
        } catch (err) {
          log.eip.error(`Error handling subscribe request: ${err}`);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: false, error: String(err) })));
        }
      }
    })();

    // Unsubscribe request handler
    unsubscribeSub = nc.subscribe(unsubscribeSubject);
    log.eip.info(`Listening for unsubscribe requests on ${unsubscribeSubject}`);

    (async () => {
      for await (const msg of unsubscribeSub!) {
        try {
          const request = JSON.parse(new TextDecoder().decode(msg.data)) as SubscribeRequest;
          unsubscribeTags(request.tags, request.subscriberId);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: true, count: request.tags.length })));
        } catch (err) {
          log.eip.error(`Error handling unsubscribe request: ${err}`);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: false, error: String(err) })));
        }
      }
    })();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // PLC Connection Management
  // ─────────────────────────────────────────────────────────────────────────

  async function connectPlc(plcId: string): Promise<boolean> {
    const conn = connections.get(plcId);
    if (!conn) return false;
    if (conn.cip) return true;

    try {
      log.eip.info(`Connecting to PLC ${plcId} at ${conn.config.host}:${conn.config.port}...`);
      conn.cip = await createCip({
        host: conn.config.host,
        port: conn.config.port,
      });
      log.eip.info(`Connected to PLC ${plcId}: ${conn.cip.identity?.productName || "Unknown"}`);
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
  // Polling Loop (only reads subscribed tags)
  // ─────────────────────────────────────────────────────────────────────────

  async function pollPlc(plcId: string): Promise<void> {
    const conn = connections.get(plcId);
    if (!conn || !conn.config.enabled) return;

    conn.polling = true;
    log.eip.info(`Starting polling loop for PLC ${plcId} (${conn.config.scanRate}ms)`);

    while (!conn.abortController.signal.aborted && conn.config.enabled) {
      const startTime = Date.now();

      // Get currently subscribed tags
      const subscribedTags = getSubscribedTags();

      if (subscribedTags.size === 0) {
        // No subscriptions - sleep and check again
        await sleep(1000, conn.abortController.signal);
        continue;
      }

      // Ensure connected
      if (!conn.cip) {
        const connected = await connectPlc(plcId);
        if (!connected) {
          await sleep(5000, conn.abortController.signal);
          continue;
        }
      }

      // Read only subscribed variables that exist in this PLC's cache
      const variablesToRead = [...conn.variables.values()]
        .filter(v => subscribedTags.has(v.name));

      if (variablesToRead.length === 0) {
        await sleep(conn.config.scanRate, conn.abortController.signal);
        continue;
      }

      log.eip.debug(`Poll cycle: ${variablesToRead.length} subscribed tags to read`);
      let readCount = 0;
      let failCount = 0;

      for (const variable of variablesToRead) {
        if (conn.abortController.signal.aborted) break;

        try {
          const response = await readTag(conn.cip!, variable.name);
          if (response.success) {
            const { value, typeCode } = decodeTagValue(response.data, variable.datatype);

            // Proactively correct cached datatype
            const actualDatatype = typeCodeToDatatype(typeCode);
            if (actualDatatype && actualDatatype !== variable.datatype) {
              log.eip.debug(`Correcting datatype for ${variable.name}: ${variable.datatype} -> ${actualDatatype}`);
              variable.datatype = actualDatatype;
              conn.cacheModified = true;
            }

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

      // Save cache if modified
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
  // Connection Initialization
  // ─────────────────────────────────────────────────────────────────────────

  async function initConnection(plcId: string, config: PlcConfig): Promise<PlcConnection> {
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
      log.eip.info(`PLC ${plcId} initialized with ${cachedVariables.size} cached variables`);
    }

    return conn;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // MQTT Auto-subscription
  // ─────────────────────────────────────────────────────────────────────────

  function autoSubscribeMqttTags(): void {
    if (!mqttConfigManager) return;

    const enabledTags = mqttConfigManager.getEnabledVariables();
    if (enabledTags.length > 0) {
      subscribeTags(enabledTags, "mqtt");
      log.eip.info(`Auto-subscribed ${enabledTags.length} MQTT-enabled tags`);
    }
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
        const wasEnabled = existing.config.enabled;
        existing.config = config;

        if (!wasEnabled && config.enabled) {
          pollPlc(plcId);
        } else if (wasEnabled && !config.enabled) {
          existing.abortController.abort();
          existing.abortController = new AbortController();
          disconnectPlc(plcId);
        }
      } else {
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
      log.eip.info("Starting scanner (subscription-based polling)...");

      // Subscribe to config changes
      configManager.onChange(handleConfigChange);

      // Start request handlers
      startRequestHandlers();

      // Initialize connections for existing PLCs
      (async () => {
        for (const [plcId, plcConfig] of configManager.config.plcs) {
          const conn = await initConnection(plcId, plcConfig);
          connections.set(plcId, conn);

          if (plcConfig.enabled) {
            pollPlc(plcId);
          }
        }

        // Auto-subscribe MQTT-enabled tags after connections are initialized
        autoSubscribeMqttTags();

        log.eip.info(`Scanner started with ${connections.size} PLCs, ${subscriptions.size} subscribed tags`);
      })();
    },

    async stop() {
      log.eip.info("Stopping scanner...");

      // Stop request handlers
      variablesSub?.unsubscribe();
      browseSub?.unsubscribe();
      subscribeSub?.unsubscribe();
      unsubscribeSub?.unsubscribe();

      // Stop all polling loops
      for (const [plcId, conn] of connections) {
        conn.abortController.abort();
        await disconnectPlc(plcId);
      }

      connections.clear();
      subscriptions.clear();
      log.eip.info("Scanner stopped");
    },
  };

  return manager;
}
