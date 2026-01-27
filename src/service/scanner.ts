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
  writeTag,
  browseTags,
  expandUdtMembers,
  getTemplateId,
  type Cip,
} from "../ethernetip/mod.ts";
import { decodeFloat32, decodeUint, encodeUint } from "../ethernetip/encode.ts";
import { log } from "../utils/logger.ts";
import type { PlcConfig, ConfigManager, ConfigChangeEvent } from "./config.ts";
import type { MqttConfigManager } from "./mqttConfig.ts";
import type { BrowsePhase, BrowseProgressMessage } from "@tentacle/nats-schema";
import { NATS_TOPICS, substituteTopic } from "@tentacle/nats-schema";

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
  deviceId: string;
  variableId: string;
  value: number | boolean | string | null;
  datatype: string;
  quality: "good" | "bad" | "unknown";
  source: string;
  lastUpdated: number;
};

type ConnectionState = "disconnected" | "connecting" | "connected";

type PlcConnection = {
  config: PlcConfig;
  cip: Cip | null;
  /** All discovered variables (from browse) with their current values */
  variables: Map<string, CachedVariable>;
  polling: boolean;
  abortController: AbortController;
  /** Flag to batch cache saves */
  cacheModified: boolean;
  /** Current connection state for observability */
  connectionState: ConnectionState;
  /** Number of consecutive connection failures (for exponential backoff) */
  consecutiveFailures: number;
  /** Timestamp of last successful read */
  lastSuccessfulRead: number;
  /** Timestamp of last connection attempt */
  lastConnectAttempt: number;
  /** Total successful reads since last connect */
  totalReads: number;
  /** Total failed reads since last connect */
  totalFailures: number;
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
  browseId?: string;  // Optional: unique ID for async browse with progress tracking
  async?: boolean;  // Optional: if true, return immediately and publish progress
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
  let writeSub: Subscription | null = null;

  // NATS subjects
  const dataSubject = `plc.data.${projectId}`;
  const mqttDataSubjectBase = `plc.data.${projectId}`;
  const variablesSubject = `plc.variables.${projectId}`;
  const browseSubject = `plc.browse.${projectId}`;
  const subscribeSubject = `plc.subscribe.${projectId}`;
  const unsubscribeSubject = `plc.unsubscribe.${projectId}`;
  // Write subject matches what tentacle-mqtt publishes for DCMD: ${projectId}/${variableId}
  // Since NATS uses . as delimiter and variableId may contain dots, we subscribe broadly
  const writeSubjectPrefix = `${projectId}/`;

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
    let corrected = false;
    try {
      const manager = await getJsm();
      const msg = await manager.streams.getMessage(cacheStreamName, {
        last_by_subj: `${cacheSubjectPrefix}.cache.variables.${plcId}`,
      });
      if (msg?.data && msg.data.length > 0) {
        const cached = JSON.parse(new TextDecoder().decode(msg.data)) as CachedVariable[];
        for (const v of cached) {
          // Correct datatype based on actual value if there's a mismatch
          // This fixes issues where UDT members were incorrectly typed during initial browse
          if (v.value !== null && v.value !== undefined) {
            if (typeof v.value === "number" && v.datatype === "BOOL") {
              log.eip.info(`Correcting cached datatype for ${v.name}: BOOL -> REAL (value is number)`);
              v.datatype = "REAL";
              corrected = true;
            } else if (typeof v.value === "boolean" && v.datatype !== "BOOL") {
              log.eip.info(`Correcting cached datatype for ${v.name}: ${v.datatype} -> BOOL (value is boolean)`);
              v.datatype = "BOOL";
              corrected = true;
            }
          }
          variables.set(v.name, v);
        }
        log.eip.info(`Loaded ${variables.size} cached variables for PLC ${plcId}`);

        // Save corrected cache if any datatypes were fixed
        if (corrected) {
          saveVariableCache(plcId, variables).catch(err => {
            log.eip.warn(`Failed to save corrected cache: ${err}`);
          });
        }
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

    // IMPORTANT: Prioritize typeCode from actual PLC response over cached datatype.
    // The cached datatype may be wrong due to template parsing issues, but the
    // typeCode in the response is always correct from the PLC.
    if (typeCode === 0xca) {  // REAL
      return { value: decodeFloat32(valueData), typeCode };
    } else if (typeCode === 0xcb) {  // LREAL
      const view = new DataView(valueData.buffer, valueData.byteOffset, 8);
      return { value: view.getFloat64(0, true), typeCode };
    } else if (typeCode === 0xc1) {  // BOOL
      return { value: valueData[0] !== 0, typeCode };
    } else if (typeCode === 0xc2) {  // SINT
      return { value: new Int8Array(valueData.buffer, valueData.byteOffset, 1)[0], typeCode };
    } else if (typeCode === 0xc3) {  // INT
      const view = new DataView(valueData.buffer, valueData.byteOffset, 2);
      return { value: view.getInt16(0, true), typeCode };
    } else if (typeCode === 0xc4) {  // DINT
      const view = new DataView(valueData.buffer, valueData.byteOffset, 4);
      return { value: view.getInt32(0, true), typeCode };
    } else if (typeCode === 0xc6) {  // USINT
      return { value: valueData[0], typeCode };
    } else if (typeCode === 0xc7) {  // UINT
      return { value: decodeUint(valueData.subarray(0, 2)), typeCode };
    } else if (typeCode === 0xc8) {  // UDINT
      return { value: decodeUint(valueData.subarray(0, 4)), typeCode };
    }

    // Fallback: use cached datatype for unknown type codes (e.g., UDT types)
    if (datatype === "REAL") {
      return { value: decodeFloat32(valueData), typeCode };
    } else if (datatype === "LREAL") {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 8);
      return { value: view.getFloat64(0, true), typeCode };
    } else if (datatype === "BOOL") {
      return { value: valueData[0] !== 0, typeCode };
    } else if (datatype === "SINT") {
      return { value: new Int8Array(valueData.buffer, valueData.byteOffset, 1)[0], typeCode };
    } else if (datatype === "INT") {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 2);
      return { value: view.getInt16(0, true), typeCode };
    } else if (datatype === "DINT") {
      const view = new DataView(valueData.buffer, valueData.byteOffset, 4);
      return { value: view.getInt32(0, true), typeCode };
    } else if (datatype === "USINT") {
      return { value: valueData[0], typeCode };
    } else if (datatype === "UINT") {
      return { value: decodeUint(valueData.subarray(0, 2)), typeCode };
    } else if (datatype === "UDINT") {
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

  /**
   * Map PLC datatype string to CIP typeCode
   */
  function datatypeToTypeCode(datatype: string): number | null {
    const TYPE_MAP: Record<string, number> = {
      "BOOL": 0xc1,
      "SINT": 0xc2,
      "INT": 0xc3,
      "DINT": 0xc4,
      "LINT": 0xc5,
      "USINT": 0xc6,
      "UINT": 0xc7,
      "UDINT": 0xc8,
      "ULINT": 0xc9,
      "REAL": 0xca,
      "LREAL": 0xcb,
    };
    return TYPE_MAP[datatype] ?? null;
  }

  /**
   * Encode a JS value to Uint8Array for writing to PLC
   */
  function encodeTagValue(
    value: number | boolean | string,
    datatype: string,
  ): Uint8Array | null {
    // Parse string values to appropriate type
    let numValue: number;
    if (typeof value === "string") {
      if (datatype === "BOOL") {
        const lower = value.toLowerCase();
        return new Uint8Array([lower === "true" || lower === "1" ? 1 : 0]);
      }
      numValue = parseFloat(value);
      if (isNaN(numValue)) {
        log.eip.warn(`Cannot parse "${value}" as number for datatype ${datatype}`);
        return null;
      }
    } else if (typeof value === "boolean") {
      return new Uint8Array([value ? 1 : 0]);
    } else {
      numValue = value;
    }

    switch (datatype) {
      case "BOOL":
        return new Uint8Array([numValue !== 0 ? 1 : 0]);
      case "SINT": {
        const buf = new ArrayBuffer(1);
        new DataView(buf).setInt8(0, numValue);
        return new Uint8Array(buf);
      }
      case "INT": {
        const buf = new ArrayBuffer(2);
        new DataView(buf).setInt16(0, numValue, true);
        return new Uint8Array(buf);
      }
      case "DINT": {
        const buf = new ArrayBuffer(4);
        new DataView(buf).setInt32(0, numValue, true);
        return new Uint8Array(buf);
      }
      case "LINT": {
        const buf = new ArrayBuffer(8);
        new DataView(buf).setBigInt64(0, BigInt(Math.floor(numValue)), true);
        return new Uint8Array(buf);
      }
      case "USINT":
        return new Uint8Array([numValue & 0xff]);
      case "UINT": {
        const buf = new ArrayBuffer(2);
        new DataView(buf).setUint16(0, numValue, true);
        return new Uint8Array(buf);
      }
      case "UDINT": {
        const buf = new ArrayBuffer(4);
        new DataView(buf).setUint32(0, numValue, true);
        return new Uint8Array(buf);
      }
      case "ULINT": {
        const buf = new ArrayBuffer(8);
        new DataView(buf).setBigUint64(0, BigInt(Math.floor(numValue)), true);
        return new Uint8Array(buf);
      }
      case "REAL": {
        const buf = new ArrayBuffer(4);
        new DataView(buf).setFloat32(0, numValue, true);
        return new Uint8Array(buf);
      }
      case "LREAL": {
        const buf = new ArrayBuffer(8);
        new DataView(buf).setFloat64(0, numValue, true);
        return new Uint8Array(buf);
      }
      default:
        log.eip.warn(`Unsupported datatype for write: ${datatype}`);
        return null;
    }
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
      deviceId: conn.config.id,
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
  // Browse Progress Publishing
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Publish browse progress update to NATS
   */
  function publishBrowseProgress(
    browseId: string,
    deviceId: string,
    phase: BrowsePhase,
    totalTags: number,
    completedTags: number,
    errorCount: number,
    message?: string,
  ): void {
    const progressMessage: BrowseProgressMessage = {
      browseId,
      projectId,
      deviceId,
      phase,
      totalTags,
      completedTags,
      errorCount,
      message,
      timestamp: Date.now(),
    };
    const subject = substituteTopic(NATS_TOPICS.plc.browseProgress, { browseId });
    nc.publish(subject, new TextEncoder().encode(JSON.stringify(progressMessage)));
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Browse Handler (on-demand)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Browse tags from PLC and update cache
   * Does NOT start polling - just discovers available tags
   * If browseId is provided, publishes progress updates to NATS
   */
  async function browseAndCacheTags(plcId: string, browseId?: string): Promise<VariableInfo[]> {
    const conn = connections.get(plcId);
    if (!conn) {
      log.eip.warn(`Browse requested for unknown PLC: ${plcId}`);
      if (browseId) {
        publishBrowseProgress(browseId, plcId, "failed", 0, 0, 1, `Unknown PLC: ${plcId}`);
      }
      return [];
    }

    // Ensure connected
    if (!conn.cip) {
      try {
        log.eip.info(`Connecting to PLC ${plcId} for browse...`);
        if (browseId) {
          publishBrowseProgress(browseId, plcId, "discovering", 0, 0, 0, "Connecting to PLC...");
        }
        conn.cip = await createCip({
          host: conn.config.host,
          port: conn.config.port,
        });
        log.eip.info(`Connected to PLC ${plcId}: ${conn.cip.identity?.productName || "Unknown"}`);
      } catch (err) {
        log.eip.error(`Failed to connect to PLC ${plcId} for browse: ${err}`);
        if (browseId) {
          publishBrowseProgress(browseId, plcId, "failed", 0, 0, 1, `Connection failed: ${err}`);
        }
        return [];
      }
    }

    log.eip.info(`Browsing tags for PLC ${plcId}...`);
    if (browseId) {
      publishBrowseProgress(browseId, plcId, "discovering", 0, 0, 0, "Discovering tags from PLC...");
    }
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
      if (browseId) {
        publishBrowseProgress(browseId, plcId, "expanding", structTags.length, 0, 0, `Expanding ${structTags.length} struct tags...`);
      }
      let expandedCount = 0;
      let structsProcessed = 0;

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
        structsProcessed++;
        // Publish progress every 10 struct tags
        if (browseId && structsProcessed % 10 === 0) {
          publishBrowseProgress(browseId, plcId, "expanding", structTags.length, structsProcessed, 0, `Expanded ${structsProcessed}/${structTags.length} structs (${expandedCount} members)`);
        }
      }

      log.eip.info(`UDT expansion complete: ${expandedCount} members, total ${discoveredTags.size} tags`);
    }

    // Update cache with discovered tags (preserve existing values AND corrected datatypes)
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
        // Only update datatype from template if the existing variable has never been read successfully.
        // If value is not null, the datatype may have been corrected based on actual PLC response,
        // which is more reliable than template parsing. Template parsing can have alignment issues
        // that cause wrong datatypes, but actual PLC reads always return the correct type code.
        if (existing.value === null && existing.datatype !== datatype) {
          log.eip.debug(`Updating datatype for unread variable ${name}: ${existing.datatype} -> ${datatype}`);
          existing.datatype = datatype;
        } else if (existing.value !== null && existing.datatype !== datatype) {
          log.eip.debug(`Preserving corrected datatype for ${name}: keeping ${existing.datatype} (template says ${datatype})`);
        }
      }
    }

    // Remove stale variables
    const staleKeys = [...conn.variables.keys()].filter(k => !discoveredTags.has(k));
    for (const key of staleKeys) {
      conn.variables.delete(key);
    }

    // Read values for all discovered tags once (so UI shows actual values instead of null/unknown)
    const variablesToRead = [...conn.variables.values()].filter(v => v.value === null);
    if (variablesToRead.length > 0) {
      log.eip.info(`Reading initial values for ${variablesToRead.length} tags...`);
      if (browseId) {
        publishBrowseProgress(browseId, plcId, "reading", variablesToRead.length, 0, 0, `Reading values for ${variablesToRead.length} tags...`);
      }
      let readCount = 0;
      let failCount = 0;

      for (const variable of variablesToRead) {
        try {
          const response = await readTag(conn.cip!, variable.name);
          if (response.success) {
            const { value, typeCode } = decodeTagValue(response.data, variable.datatype);

            // Correct cached datatype based on actual PLC response
            const actualDatatype = typeCodeToDatatype(typeCode);
            if (actualDatatype && actualDatatype !== variable.datatype) {
              log.eip.debug(`Correcting datatype for ${variable.name}: ${variable.datatype} -> ${actualDatatype}`);
              variable.datatype = actualDatatype;
            }

            variable.value = value;
            variable.quality = "good";
            variable.lastUpdated = Date.now();
            readCount++;
          } else {
            failCount++;
            log.eip.debug(`Initial read failed for "${variable.name}": status 0x${response.status.toString(16)}`);
          }
        } catch (err) {
          failCount++;
          log.eip.debug(`Error reading "${variable.name}": ${err}`);
        }

        const totalProcessed = readCount + failCount;
        // Publish progress every 50 tags for smooth UI updates
        if (browseId && totalProcessed % 50 === 0) {
          publishBrowseProgress(browseId, plcId, "reading", variablesToRead.length, totalProcessed, failCount, `Read ${totalProcessed}/${variablesToRead.length} tags`);
        }
        // Log progress every 100 tags
        if (totalProcessed % 100 === 0) {
          log.eip.info(`  Progress: ${totalProcessed}/${variablesToRead.length} tags read...`);
        }
      }

      log.eip.info(`Initial value read complete: ${readCount} success, ${failCount} failed`);
    }

    if (browseId) {
      publishBrowseProgress(browseId, plcId, "caching", conn.variables.size, 0, 0, "Saving to cache...");
    }

    await saveVariableCache(plcId, conn.variables);

    log.eip.info(`Browse complete: ${conn.variables.size} tags cached for PLC ${plcId}`);

    // Publish completion
    if (browseId) {
      publishBrowseProgress(browseId, plcId, "completed", conn.variables.size, conn.variables.size, 0, `Browse complete: ${conn.variables.size} tags`);
    }

    // Return as VariableInfo array
    return [...conn.variables.values()]
      .filter(v => isValidTagName(v.name))
      .map(v => {
        // Infer correct datatype from actual value (same failsafe as publishValue)
        let datatype = getNatsDatatype(v.datatype);
        if (typeof v.value === "number" && datatype !== "number") {
          datatype = "number";
        } else if (typeof v.value === "boolean" && datatype !== "boolean") {
          datatype = "boolean";
        }
        return {
          deviceId: plcId,
          variableId: v.name,
          value: v.value,
          datatype,
          quality: v.quality,
          source: "plc",
          lastUpdated: v.lastUpdated,
        };
      });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Write Handler (from MQTT DCMD via tentacle-mqtt)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Find which PLC has a variable and return the connection
   */
  function findPlcWithVariable(variableId: string): PlcConnection | null {
    for (const conn of connections.values()) {
      if (conn.variables.has(variableId)) {
        return conn;
      }
    }
    return null;
  }

  /**
   * Handle write request from MQTT DCMD
   * Subject format: ${projectId}/${variableId}
   * Payload: string value
   */
  async function handleWriteCommand(subject: string, data: Uint8Array): Promise<void> {
    // Extract variableId from subject
    if (!subject.startsWith(writeSubjectPrefix)) {
      return;
    }
    const variableId = subject.slice(writeSubjectPrefix.length);
    if (!variableId) {
      log.eip.warn(`Write command with empty variableId: ${subject}`);
      return;
    }

    // Parse value from payload (tentacle-mqtt sends String(convertedValue))
    const valueStr = new TextDecoder().decode(data);
    log.eip.info(`Write command received: ${variableId} = ${valueStr}`);

    // Find which PLC has this variable
    const conn = findPlcWithVariable(variableId);
    if (!conn) {
      log.eip.warn(`Write failed: variable "${variableId}" not found in any PLC cache`);
      return;
    }

    // Get variable info for datatype
    const variable = conn.variables.get(variableId);
    if (!variable) {
      log.eip.warn(`Write failed: variable "${variableId}" not in cache`);
      return;
    }

    // Ensure PLC is connected
    if (!conn.cip) {
      log.eip.warn(`Write failed: PLC ${conn.config.id} not connected`);
      return;
    }

    // Get CIP type code
    const typeCode = datatypeToTypeCode(variable.datatype);
    if (typeCode === null) {
      log.eip.warn(`Write failed: unsupported datatype "${variable.datatype}" for ${variableId}`);
      return;
    }

    // Encode value
    const encodedValue = encodeTagValue(valueStr, variable.datatype);
    if (!encodedValue) {
      log.eip.warn(`Write failed: could not encode value "${valueStr}" for datatype ${variable.datatype}`);
      return;
    }

    // Write to PLC
    try {
      const response = await writeTag(conn.cip, variableId, typeCode, encodedValue);
      if (response.success) {
        log.eip.info(`Write success: ${variableId} = ${valueStr} (${variable.datatype})`);
        // Update cached value
        variable.value = variable.datatype === "BOOL"
          ? (valueStr.toLowerCase() === "true" || valueStr === "1")
          : parseFloat(valueStr);
        variable.lastUpdated = Date.now();
      } else {
        log.eip.warn(`Write failed: ${variableId} - CIP status 0x${response.status.toString(16)}`);
      }
    } catch (err) {
      log.eip.error(`Write error for ${variableId}: ${err}`);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Request/Reply Handlers
  // ─────────────────────────────────────────────────────────────────────────

  function getAllVariables(): VariableInfo[] {
    const allVariables: VariableInfo[] = [];

    for (const [plcId, conn] of connections.entries()) {
      for (const cached of conn.variables.values()) {
        if (/_member\d+$/.test(cached.name)) continue;
        if (!/^[\x20-\x7E]+$/.test(cached.name)) continue;

        // Infer correct datatype from actual value (same failsafe as publishValue)
        let datatype = getNatsDatatype(cached.datatype);
        if (typeof cached.value === "number" && datatype !== "number") {
          datatype = "number";
        } else if (typeof cached.value === "boolean" && datatype !== "boolean") {
          datatype = "boolean";
        }

        allVariables.push({
          deviceId: plcId,
          variableId: cached.name,
          value: cached.value,
          datatype,
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

          // Generate browseId if async mode or if one was provided
          const browseId = request.browseId || (request.async ? crypto.randomUUID() : undefined);

          // Async mode: return immediately with browseId, run browse in background
          if (request.async && browseId) {
            log.eip.info(`Browse request (async): starting browse with ID ${browseId}`);
            msg.respond(new TextEncoder().encode(JSON.stringify({ browseId })));

            // Run browse in background
            (async () => {
              try {
                if (request.plcId) {
                  await browseAndCacheTags(request.plcId, browseId);
                } else {
                  for (const plcId of connections.keys()) {
                    await browseAndCacheTags(plcId, browseId);
                  }
                }
              } catch (err) {
                log.eip.error(`Async browse failed: ${err}`);
                if (browseId) {
                  publishBrowseProgress(browseId, request.plcId || "all", "failed", 0, 0, 1, `Browse failed: ${err}`);
                }
              }
            })();
            continue;
          }

          // Sync mode: wait for browse to complete and return results
          const results: VariableInfo[] = [];

          if (request.plcId) {
            // Browse specific PLC
            const plcResults = await browseAndCacheTags(request.plcId, browseId);
            results.push(...plcResults);
          } else {
            // Browse all PLCs
            for (const plcId of connections.keys()) {
              const plcResults = await browseAndCacheTags(plcId, browseId);
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

    // Write command handler (from tentacle-mqtt DCMD)
    // Subscribe to all subjects and filter for ${projectId}/* pattern
    // This is necessary because NATS wildcards can't match the ${projectId}/${variableId} format
    writeSub = nc.subscribe(">");
    log.eip.info(`Listening for write commands on ${writeSubjectPrefix}*`);

    (async () => {
      for await (const msg of writeSub!) {
        // Filter for our projectId
        if (msg.subject.startsWith(writeSubjectPrefix)) {
          handleWriteCommand(msg.subject, msg.data).catch((err) => {
            log.eip.error(`Error handling write command: ${err}`);
          });
        }
      }
    })();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // PLC Connection Management
  // ─────────────────────────────────────────────────────────────────────────

  /** Calculate exponential backoff delay: 2^failures seconds, capped at 60s */
  function getBackoffDelay(failures: number): number {
    const baseDelay = 2000; // 2 seconds
    const maxDelay = 60000; // 60 seconds
    const delay = Math.min(baseDelay * Math.pow(2, failures), maxDelay);
    return delay;
  }

  async function connectPlc(plcId: string): Promise<boolean> {
    const conn = connections.get(plcId);
    if (!conn) return false;
    if (conn.cip) return true;

    conn.connectionState = "connecting";
    conn.lastConnectAttempt = Date.now();

    try {
      log.eip.info(`[${plcId}] Connecting to ${conn.config.host}:${conn.config.port}...`);
      conn.cip = await createCip({
        host: conn.config.host,
        port: conn.config.port,
      });

      // Success - reset failure count and update state
      conn.connectionState = "connected";
      conn.consecutiveFailures = 0;
      conn.totalReads = 0;
      conn.totalFailures = 0;
      log.eip.info(`[${plcId}] Connected to ${conn.cip.identity?.productName || "Unknown"}`);
      return true;
    } catch (err) {
      conn.connectionState = "disconnected";
      conn.consecutiveFailures++;
      conn.cip = null;

      const backoff = getBackoffDelay(conn.consecutiveFailures);
      log.eip.warn(`[${plcId}] Connection failed (attempt ${conn.consecutiveFailures}): ${err}`);
      log.eip.info(`[${plcId}] Will retry in ${(backoff / 1000).toFixed(1)}s`);
      return false;
    }
  }

  async function disconnectPlc(plcId: string): Promise<void> {
    const conn = connections.get(plcId);
    if (!conn) return;

    if (conn.cip) {
      log.eip.info(`[${plcId}] Disconnecting...`);
      try {
        await destroyCip(conn.cip);
      } catch {
        // Ignore disconnect errors
      }
      conn.cip = null;
      conn.connectionState = "disconnected";
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Polling Loop (only reads subscribed tags)
  // ─────────────────────────────────────────────────────────────────────────

  async function pollPlc(plcId: string): Promise<void> {
    const conn = connections.get(plcId);
    if (!conn || !conn.config.enabled) return;

    conn.polling = true;
    log.eip.info(`[${plcId}] Starting polling loop (scan rate: ${conn.config.scanRate}ms)`);

    // For periodic status logging
    let lastStatusLog = Date.now();
    const STATUS_LOG_INTERVAL = 30000; // Log status every 30 seconds

    while (!conn.abortController.signal.aborted && conn.config.enabled) {
      const startTime = Date.now();

      // Get currently subscribed tags
      const subscribedTags = getSubscribedTags();

      if (subscribedTags.size === 0) {
        if (conn.connectionState === "connected") {
          log.eip.info(`[${plcId}] No subscribed tags, waiting...`);
        }
        await sleep(1000, conn.abortController.signal);
        continue;
      }

      // Ensure connected (with exponential backoff on failures)
      if (!conn.cip) {
        const backoffDelay = getBackoffDelay(conn.consecutiveFailures);
        const timeSinceLastAttempt = Date.now() - conn.lastConnectAttempt;

        // Wait for backoff period before retrying
        if (conn.consecutiveFailures > 0 && timeSinceLastAttempt < backoffDelay) {
          const remaining = backoffDelay - timeSinceLastAttempt;
          await sleep(Math.min(remaining, 1000), conn.abortController.signal);
          continue;
        }

        const connected = await connectPlc(plcId);
        if (!connected) {
          continue; // Backoff handled in connectPlc
        }
      }

      // Read only subscribed variables that exist in this PLC's cache
      const variablesToRead = [...conn.variables.values()]
        .filter(v => subscribedTags.has(v.name));

      if (variablesToRead.length === 0) {
        await sleep(conn.config.scanRate, conn.abortController.signal);
        continue;
      }

      let readCount = 0;
      let failCount = 0;
      let connectionError = false;
      const wasFirstRead = conn.totalReads === 0;
      const sampleValues: string[] = []; // Collect sample values to log

      for (const variable of variablesToRead) {
        if (conn.abortController.signal.aborted) break;

        try {
          const response = await readTag(conn.cip!, variable.name);
          if (response.success) {
            const { value, typeCode } = decodeTagValue(response.data, variable.datatype);

            // Proactively correct cached datatype
            const actualDatatype = typeCodeToDatatype(typeCode);
            if (actualDatatype && actualDatatype !== variable.datatype) {
              log.eip.debug(`[${plcId}] Correcting datatype for ${variable.name}: ${variable.datatype} -> ${actualDatatype}`);
              variable.datatype = actualDatatype;
              conn.cacheModified = true;
            }

            // Collect sample values for logging (first 2 tags)
            if (sampleValues.length < 2) {
              const shortName = variable.name.length > 30 ? "..." + variable.name.slice(-27) : variable.name;
              sampleValues.push(`${shortName}=${value}`);
            }

            publishValue(conn, variable.name, value, variable.datatype, "good");
            readCount++;
            conn.lastSuccessfulRead = Date.now();
          } else {
            failCount++;
            if (failCount <= 3) {
              log.eip.debug(`[${plcId}] Read failed for "${variable.name}": status 0x${response.status.toString(16)}`);
            }
          }
        } catch (err) {
          failCount++;
          // Check if this is a connection error
          const errStr = String(err);
          if (errStr.includes("connection") || errStr.includes("socket") || errStr.includes("ECONNRESET") || errStr.includes("timeout")) {
            connectionError = true;
            log.eip.warn(`[${plcId}] Connection error during read: ${err}`);
            break;
          }
          if (failCount <= 3) {
            log.eip.debug(`[${plcId}] Error reading "${variable.name}": ${err}`);
          }
        }
      }

      // Update statistics
      conn.totalReads += readCount;
      conn.totalFailures += failCount;

      // Log first successful read (confirms data is flowing)
      if (wasFirstRead && readCount > 0) {
        log.eip.info(`[${plcId}] Data flowing: ${sampleValues.join(", ")}${variablesToRead.length > 2 ? ` (+${variablesToRead.length - 2} more)` : ""}`);
      }

      // Handle connection errors - force reconnect
      if (connectionError) {
        log.eip.warn(`[${plcId}] Lost connection, will reconnect...`);
        conn.cip = null;
        conn.connectionState = "disconnected";
        conn.consecutiveFailures++;
        continue;
      }

      // Periodic status logging (every 30s, with sample values)
      const now = Date.now();
      if (now - lastStatusLog >= STATUS_LOG_INTERVAL && readCount > 0) {
        const sampleStr = sampleValues.length > 0 ? ` [${sampleValues.join(", ")}]` : "";
        log.eip.info(`[${plcId}] Polling ${variablesToRead.length} tags @ ${conn.config.scanRate}ms` +
          ` (${conn.totalReads} reads, ${conn.totalFailures} failures)${sampleStr}`);
        lastStatusLog = now;
      }

      // Log summary at debug level
      if (readCount > 0 || failCount > 0) {
        log.eip.debug(`[${plcId}] Poll: ${readCount}/${variablesToRead.length} success, ${failCount} failed`);
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
    conn.connectionState = "disconnected";
    log.eip.info(`[${plcId}] Polling stopped (total reads: ${conn.totalReads}, failures: ${conn.totalFailures})`);
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
      connectionState: "disconnected",
      consecutiveFailures: 0,
      lastSuccessfulRead: 0,
      lastConnectAttempt: 0,
      totalReads: 0,
      totalFailures: 0,
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

  /**
   * Handle MQTT config changes - sync subscriptions with enabled tags
   */
  function handleMqttConfigChange(): void {
    if (!mqttConfigManager) return;

    const enabledTags = new Set(mqttConfigManager.getEnabledVariables());
    const currentMqttTags = new Set<string>();

    // Find all tags currently subscribed by "mqtt"
    for (const [tag, subscribers] of subscriptions) {
      if (subscribers.has("mqtt")) {
        currentMqttTags.add(tag);
      }
    }

    // Subscribe newly enabled tags
    const toSubscribe = [...enabledTags].filter(tag => !currentMqttTags.has(tag));
    if (toSubscribe.length > 0) {
      subscribeTags(toSubscribe, "mqtt");
    }

    // Unsubscribe disabled tags
    const toUnsubscribe = [...currentMqttTags].filter(tag => !enabledTags.has(tag));
    if (toUnsubscribe.length > 0) {
      unsubscribeTags(toUnsubscribe, "mqtt");
    }

    if (toSubscribe.length > 0 || toUnsubscribe.length > 0) {
      log.eip.info(`MQTT subscriptions updated: +${toSubscribe.length} -${toUnsubscribe.length}, total: ${enabledTags.size}`);
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
      mqttConfigManager?.onChange(handleMqttConfigChange);

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
      writeSub?.unsubscribe();

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
