/**
 * PLC Scanner - handles polling loops for EtherNet/IP devices
 *
 * Stateless, subscriber-driven model:
 * - Starts with zero connections
 * - Browse requests include { deviceId, host, port } — creates temporary connection
 * - Subscribe requests include { deviceId, host, port, scanRate, tags, subscriberId }
 *   → creates persistent connection on-demand, starts polling
 * - Unsubscribe removes tags; when zero subscribers remain, connection is closed
 *
 * NATS topics:
 * - ethernetip.browse - Browse device tags (requires host/port)
 * - ethernetip.variables - Returns discovered variables
 * - ethernetip.subscribe - Subscribe tags with connection info
 * - ethernetip.unsubscribe - Unsubscribe tags
 * - ethernetip.data.{deviceId}.{variableId} - Published variable data
 * - ethernetip.command.{variableId} - Write commands
 */

import { type NatsConnection, type Subscription } from "@nats-io/transport-deno";
import {
  createCip,
  destroyCip,
  readTag,
  readMultipleTags,
  writeTag,
  browseTags,
  expandUdtMembers,
  getTemplateId,
  type Cip,
} from "../ethernetip/mod.ts";
import { decodeFloat32, decodeUint, encodeUint } from "../ethernetip/encode.ts";
import { log } from "../utils/logger.ts";
import type { BrowsePhase, BrowseProgressMessage } from "@tentacle/nats-schema";
import { NATS_TOPICS, substituteTopic } from "@tentacle/nats-schema";

/** Module ID for this service */
const MODULE_ID = "ethernetip";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Discovered variable with tag info and last known value
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
  moduleId: string;
  deviceId: string;
  variableId: string;
  value: number | boolean | string | null;
  datatype: string;
  quality: "good" | "bad" | "unknown";
  origin: string;
  lastUpdated: number;
};

type ConnectionState = "disconnected" | "connecting" | "connected";

type PlcConnection = {
  deviceId: string;
  host: string;
  port: number;
  scanRate: number;
  cip: Cip | null;
  /** All discovered variables with their current values */
  variables: Map<string, CachedVariable>;
  polling: boolean;
  abortController: AbortController;
  /** Current connection state */
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
 * Subscribe request payload — includes connection info for on-demand connections
 */
type SubscribeRequest = {
  deviceId: string;
  host: string;
  port: number;
  scanRate?: number;
  tags: string[];
  subscriberId: string;
};

/**
 * Unsubscribe request payload
 */
type UnsubscribeRequest = {
  deviceId: string;
  tags: string[];
  subscriberId: string;
};

/**
 * Browse request payload — includes connection info
 */
type BrowseRequest = {
  deviceId: string;
  host: string;
  port: number;
  browseId?: string;
  async?: boolean;
};

/**
 * Per-subscriber tracking within a device
 */
type DeviceSubscription = {
  subscriberId: string;
  tags: Set<string>;
  scanRate: number;
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
 * Create a scanner that manages polling loops for EtherNet/IP devices.
 * Starts with zero connections — devices are connected on-demand via subscribe/browse.
 */
export async function createScanner(
  nc: NatsConnection,
): Promise<ScannerManager> {
  const connections = new Map<string, PlcConnection>();

  // Per-device subscriber tracking: deviceId → (subscriberId → DeviceSubscription)
  const deviceSubscribers = new Map<string, Map<string, DeviceSubscription>>();

  // NATS subscriptions
  let variablesSub: Subscription | null = null;
  let browseSub: Subscription | null = null;
  let subscribeSub: Subscription | null = null;
  let unsubscribeSub: Subscription | null = null;
  let writeSub: Subscription | null = null;

  // NATS subjects (module-namespaced)
  const dataSubject = `${MODULE_ID}.data`;
  const variablesSubject = `${MODULE_ID}.variables`;
  const browseSubject = `${MODULE_ID}.browse`;
  const subscribeSubject = `${MODULE_ID}.subscribe`;
  const unsubscribeSubject = `${MODULE_ID}.unsubscribe`;
  // Write command subject: ethernetip.command.>
  const writeCommandSubject = `${MODULE_ID}.command.>`;

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
  // Per-Device Subscription Management
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Get all subscribed tags for a specific device
   */
  function getDeviceSubscribedTags(deviceId: string): Set<string> {
    const subs = deviceSubscribers.get(deviceId);
    if (!subs) return new Set();
    const tags = new Set<string>();
    for (const sub of subs.values()) {
      for (const tag of sub.tags) {
        tags.add(tag);
      }
    }
    return tags;
  }

  /**
   * Get the effective scan rate for a device (fastest among subscribers)
   */
  function getDeviceScanRate(deviceId: string, defaultRate: number): number {
    const subs = deviceSubscribers.get(deviceId);
    if (!subs || subs.size === 0) return defaultRate;
    let fastest = Infinity;
    for (const sub of subs.values()) {
      if (sub.scanRate < fastest) fastest = sub.scanRate;
    }
    return fastest === Infinity ? defaultRate : fastest;
  }

  /**
   * Check if a device has any subscribers
   */
  function hasSubscribers(deviceId: string): boolean {
    const subs = deviceSubscribers.get(deviceId);
    return subs !== undefined && subs.size > 0;
  }

  /**
   * Add a subscriber for a device
   */
  function addSubscriber(
    deviceId: string,
    subscriberId: string,
    tags: string[],
    scanRate: number,
  ): void {
    if (!deviceSubscribers.has(deviceId)) {
      deviceSubscribers.set(deviceId, new Map());
    }
    const subs = deviceSubscribers.get(deviceId)!;
    const existing = subs.get(subscriberId);
    if (existing) {
      // Merge tags and update scan rate
      for (const tag of tags) {
        existing.tags.add(tag);
      }
      existing.scanRate = scanRate;
    } else {
      subs.set(subscriberId, {
        subscriberId,
        tags: new Set(tags),
        scanRate,
      });
    }
    log.eip.info(
      `Subscribed ${tags.length} tags for ${subscriberId} on device ${deviceId}, ` +
      `total subscribers: ${subs.size}`,
    );
  }

  /**
   * Remove a subscriber's tags for a device.
   * Returns true if the device has zero subscribers remaining.
   */
  function removeSubscriber(
    deviceId: string,
    subscriberId: string,
    tags: string[],
  ): boolean {
    const subs = deviceSubscribers.get(deviceId);
    if (!subs) return true;

    const existing = subs.get(subscriberId);
    if (existing) {
      for (const tag of tags) {
        existing.tags.delete(tag);
      }
      // Remove subscriber entirely if no tags left
      if (existing.tags.size === 0) {
        subs.delete(subscriberId);
      }
    }

    // Clean up device entry if no subscribers
    if (subs.size === 0) {
      deviceSubscribers.delete(deviceId);
      return true;
    }
    return false;
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
  // Publishing (pub/sub only)
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
      }
    } else if (typeof value === "boolean" && natsDatatype !== "boolean") {
      log.eip.debug(`Datatype mismatch for ${variableId}: cached=${datatype}, correcting to "boolean"`);
      natsDatatype = "boolean";
      const cached = conn.variables.get(variableId);
      if (cached) {
        cached.datatype = "BOOL";
      }
    }

    // Update variable in memory
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

    const message = {
      moduleId: MODULE_ID,
      deviceId: conn.deviceId,
      variableId,
      value,
      timestamp: now,
      datatype: natsDatatype,
    };

    const payload = new TextEncoder().encode(JSON.stringify(message));

    // Publish to per-variable subject: ethernetip.data.{deviceId}.{variableId}
    nc.publish(`${dataSubject}.${conn.deviceId}.${sanitizeForSubject(variableId)}`, payload);
  }

  /**
   * Batch publish multiple values in a single NATS message
   * Used after batch CIP reads to efficiently send all values at once
   */
  function publishBatch(
    conn: PlcConnection,
    values: { variableId: string; value: number | boolean | string; datatype: string }[],
  ): void {
    if (values.length === 0) return;

    const now = Date.now();
    const batchMessages: Array<{
      variableId: string;
      value: number | boolean | string;
      datatype: string;
    }> = [];

    for (const { variableId, value, datatype } of values) {
      let natsDatatype = getNatsDatatype(datatype);

      // Failsafe: infer correct datatype from actual value
      if (typeof value === "number" && natsDatatype !== "number") {
        natsDatatype = "number";
        const cached = conn.variables.get(variableId);
        if (cached) {
          cached.datatype = "REAL";
        }
      } else if (typeof value === "boolean" && natsDatatype !== "boolean") {
        natsDatatype = "boolean";
        const cached = conn.variables.get(variableId);
        if (cached) {
          cached.datatype = "BOOL";
        }
      }

      // Update variable in memory
      const existing = conn.variables.get(variableId);
      if (existing) {
        existing.value = value;
        existing.quality = "good";
        existing.lastUpdated = now;
      }

      batchMessages.push({ variableId, value, datatype: natsDatatype });
    }

    // Publish batch to subject: ethernetip.data.{deviceId}
    const batchPayload = {
      moduleId: MODULE_ID,
      deviceId: conn.deviceId,
      timestamp: now,
      values: batchMessages,
    };
    nc.publish(`${dataSubject}.${conn.deviceId}`, new TextEncoder().encode(JSON.stringify(batchPayload)));

    log.eip.debug(`Batch published ${batchMessages.length} values`);
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
      moduleId: MODULE_ID,
      deviceId,
      phase,
      totalTags,
      completedTags,
      errorCount,
      message,
      timestamp: Date.now(),
    };
    const subject = substituteTopic(NATS_TOPICS.ethernetip.browseProgress, { browseId });
    nc.publish(subject, new TextEncoder().encode(JSON.stringify(progressMessage)));
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Browse Handler (on-demand, creates temporary connection)
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Browse tags from a PLC device.
   * Creates a temporary connection if one doesn't already exist for this device.
   * If browseId is provided, publishes progress updates to NATS.
   */
  async function browseDevice(
    deviceId: string,
    host: string,
    port: number,
    browseId?: string,
  ): Promise<VariableInfo[]> {
    // Reuse existing connection if available, otherwise create temporary one
    let conn = connections.get(deviceId);
    let tempCip: Cip | null = null;
    let cip: Cip;

    if (conn?.cip) {
      cip = conn.cip;
    } else {
      try {
        log.eip.info(`Connecting to ${deviceId} (${host}:${port}) for browse...`);
        if (browseId) {
          publishBrowseProgress(browseId, deviceId, "discovering", 0, 0, 0, "Connecting to PLC...");
        }
        tempCip = await createCip({ host, port });
        cip = tempCip;
        log.eip.info(`Connected to ${deviceId}: ${cip.identity?.productName || "Unknown"}`);
      } catch (err) {
        log.eip.error(`Failed to connect to ${deviceId} for browse: ${err}`);
        if (browseId) {
          publishBrowseProgress(browseId, deviceId, "failed", 0, 0, 1, `Connection failed: ${err}`);
        }
        return [];
      }
    }

    log.eip.info(`Browsing tags for ${deviceId}...`);
    if (browseId) {
      publishBrowseProgress(browseId, deviceId, "discovering", 0, 0, 0, "Discovering tags from PLC...");
    }
    const allTags = await browseTags(cip);

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
        publishBrowseProgress(browseId, deviceId, "expanding", structTags.length, 0, 0, `Expanding ${structTags.length} struct tags...`);
      }
      let expandedCount = 0;
      let structsProcessed = 0;

      for (const structTag of structTags) {
        try {
          const templateId = getTemplateId(structTag.symbolType);
          if (templateId === null) continue;

          const members = await expandUdtMembers(cip, structTag.name, templateId);
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
          publishBrowseProgress(browseId, deviceId, "expanding", structTags.length, structsProcessed, 0, `Expanded ${structsProcessed}/${structTags.length} structs (${expandedCount} members)`);
        }
      }

      log.eip.info(`UDT expansion complete: ${expandedCount} members, total ${discoveredTags.size} tags`);
    }

    // Build variables map from discovered tags
    const variables = new Map<string, CachedVariable>();
    for (const [name, datatype] of discoveredTags) {
      variables.set(name, {
        name,
        datatype,
        value: null,
        quality: "unknown",
        lastUpdated: 0,
      });
    }

    // Read values for all discovered tags once
    const variablesToRead = [...variables.values()];
    if (variablesToRead.length > 0) {
      log.eip.info(`Reading initial values for ${variablesToRead.length} tags...`);
      if (browseId) {
        publishBrowseProgress(browseId, deviceId, "reading", variablesToRead.length, 0, 0, `Reading values for ${variablesToRead.length} tags...`);
      }
      let readCount = 0;
      let failCount = 0;

      for (const variable of variablesToRead) {
        try {
          const response = await readTag(cip, variable.name);
          if (response.success) {
            const { value, typeCode } = decodeTagValue(response.data, variable.datatype);

            // Correct datatype based on actual PLC response
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
          publishBrowseProgress(browseId, deviceId, "reading", variablesToRead.length, totalProcessed, failCount, `Read ${totalProcessed}/${variablesToRead.length} tags`);
        }
        // Log progress every 100 tags
        if (totalProcessed % 100 === 0) {
          log.eip.info(`  Progress: ${totalProcessed}/${variablesToRead.length} tags read...`);
        }
      }

      log.eip.info(`Initial value read complete: ${readCount} success, ${failCount} failed`);
    }

    // Clean up temporary connection (don't close if we were reusing an existing one)
    if (tempCip) {
      try {
        await destroyCip(tempCip);
      } catch {
        // Ignore disconnect errors
      }
    }

    log.eip.info(`Browse complete: ${variables.size} tags for device ${deviceId}`);

    // Publish completion
    if (browseId) {
      publishBrowseProgress(browseId, deviceId, "completed", variables.size, variables.size, 0, `Browse complete: ${variables.size} tags`);
    }

    // Return as VariableInfo array
    return [...variables.values()]
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
          moduleId: MODULE_ID,
          deviceId,
          variableId: v.name,
          value: v.value,
          datatype,
          quality: v.quality,
          origin: "plc",
          lastUpdated: v.lastUpdated,
        };
      });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Write Handler
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
   * Subject format: ethernetip.command.{variableId}
   * Payload: string value
   */
  async function handleWriteCommand(subject: string, data: Uint8Array): Promise<void> {
    // Extract variableId from subject: ethernetip.command.{variableId}
    const commandPrefix = `${MODULE_ID}.command.`;
    if (!subject.startsWith(commandPrefix)) {
      return;
    }
    const variableId = subject.slice(commandPrefix.length);
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
      log.eip.warn(`Write failed: variable "${variableId}" not found in any PLC`);
      return;
    }

    // Get variable info for datatype
    const variable = conn.variables.get(variableId);
    if (!variable) {
      log.eip.warn(`Write failed: variable "${variableId}" not found`);
      return;
    }

    // Ensure PLC is connected
    if (!conn.cip) {
      log.eip.warn(`Write failed: PLC ${conn.deviceId} not connected`);
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

    for (const [deviceId, conn] of connections.entries()) {
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
          moduleId: MODULE_ID,
          deviceId,
          variableId: cached.name,
          value: cached.value,
          datatype,
          quality: cached.quality,
          origin: "plc",
          lastUpdated: cached.lastUpdated,
        });
      }
    }

    return allVariables;
  }

  /**
   * Create a new PlcConnection for a device (on-demand)
   */
  function createConnection(
    deviceId: string,
    host: string,
    port: number,
    scanRate: number,
  ): PlcConnection {
    const conn: PlcConnection = {
      deviceId,
      host,
      port,
      scanRate,
      cip: null,
      variables: new Map(),
      polling: false,
      abortController: new AbortController(),
      connectionState: "disconnected",
      consecutiveFailures: 0,
      lastSuccessfulRead: 0,
      lastConnectAttempt: 0,
      totalReads: 0,
      totalFailures: 0,
    };
    connections.set(deviceId, conn);
    log.eip.info(`Created connection entry for ${deviceId} (${host}:${port})`);
    return conn;
  }

  /**
   * Remove a connection and clean up
   */
  async function removeConnection(deviceId: string): Promise<void> {
    const conn = connections.get(deviceId);
    if (!conn) return;

    // Stop polling
    conn.abortController.abort();

    // Disconnect
    if (conn.cip) {
      try {
        await destroyCip(conn.cip);
      } catch {
        // Ignore disconnect errors
      }
      conn.cip = null;
    }

    connections.delete(deviceId);
    log.eip.info(`Removed connection for ${deviceId}`);
  }

  async function startRequestHandlers(): Promise<void> {
    // Variables request handler (get cached variables)
    variablesSub = nc.subscribe(variablesSubject);
    log.eip.info(`Listening for variable requests on ${variablesSubject}`);

    (async () => {
      for await (const msg of variablesSub!) {
        try {
          const variables = getAllVariables();
          log.eip.info(`Variables request: returning ${variables.length} variables`);
          msg.respond(new TextEncoder().encode(JSON.stringify(variables)));
        } catch (err) {
          log.eip.error(`Error handling variables request: ${err}`);
          msg.respond(new TextEncoder().encode("[]"));
        }
      }
    })();

    // Browse request handler (on-demand discovery with connection info)
    browseSub = nc.subscribe(browseSubject);
    log.eip.info(`Listening for browse requests on ${browseSubject}`);

    (async () => {
      for await (const msg of browseSub!) {
        try {
          let request: BrowseRequest | Record<string, unknown> = {};
          if (msg.data && msg.data.length > 0) {
            request = JSON.parse(new TextDecoder().decode(msg.data));
          }

          const browseReq = request as BrowseRequest;
          if (!browseReq.deviceId || !browseReq.host || !browseReq.port) {
            msg.respond(new TextEncoder().encode(JSON.stringify({
              error: "Browse requires deviceId, host, and port",
            })));
            continue;
          }

          // Generate browseId if async mode or if one was provided
          const browseId = browseReq.browseId || (browseReq.async ? crypto.randomUUID() : undefined);

          // Async mode: return immediately with browseId, run browse in background
          if (browseReq.async && browseId) {
            log.eip.info(`Browse request (async): ${browseReq.deviceId} at ${browseReq.host}:${browseReq.port}, ID ${browseId}`);
            msg.respond(new TextEncoder().encode(JSON.stringify({ browseId })));

            // Run browse in background
            (async () => {
              try {
                const results = await browseDevice(browseReq.deviceId, browseReq.host, browseReq.port, browseId);
                publishBrowseProgress(browseId, "_all", "completed", results.length, results.length, 0, `Browse complete: ${results.length} total tags`);
              } catch (err) {
                log.eip.error(`Async browse failed: ${err}`);
                publishBrowseProgress(browseId, browseReq.deviceId, "failed", 0, 0, 1, `Browse failed: ${err}`);
              }
            })();
            continue;
          }

          // Sync mode: wait for browse to complete and return results
          const results = await browseDevice(browseReq.deviceId, browseReq.host, browseReq.port, browseId);
          log.eip.info(`Browse request: returning ${results.length} tags`);
          msg.respond(new TextEncoder().encode(JSON.stringify(results)));
        } catch (err) {
          log.eip.error(`Error handling browse request: ${err}`);
          msg.respond(new TextEncoder().encode("[]"));
        }
      }
    })();

    // Subscribe request handler (on-demand connection creation)
    subscribeSub = nc.subscribe(subscribeSubject);
    log.eip.info(`Listening for subscribe requests on ${subscribeSubject}`);

    (async () => {
      for await (const msg of subscribeSub!) {
        try {
          const request = JSON.parse(new TextDecoder().decode(msg.data)) as SubscribeRequest;

          if (!request.deviceId || !request.host || !request.port || !request.tags || !request.subscriberId) {
            msg.respond(new TextEncoder().encode(JSON.stringify({
              success: false,
              error: "Subscribe requires deviceId, host, port, tags, and subscriberId",
            })));
            continue;
          }

          const scanRate = request.scanRate ?? 1000;

          // Track subscriber
          addSubscriber(request.deviceId, request.subscriberId, request.tags, scanRate);

          // Create connection if it doesn't exist
          let conn = connections.get(request.deviceId);
          if (!conn) {
            conn = createConnection(request.deviceId, request.host, request.port, scanRate);
          }

          // Update scan rate to fastest among subscribers
          conn.scanRate = getDeviceScanRate(request.deviceId, scanRate);

          // Populate variables map from subscribe tags so polling knows what to read
          for (const tag of request.tags) {
            if (!conn.variables.has(tag)) {
              conn.variables.set(tag, {
                name: tag,
                datatype: "REAL", // Default — will be corrected on first read via typeCode
                value: null,
                quality: "unknown",
                lastUpdated: 0,
              });
            }
          }

          // Start polling if not already running
          if (!conn.polling) {
            pollPlc(request.deviceId);
          }

          msg.respond(new TextEncoder().encode(JSON.stringify({ success: true, count: request.tags.length })));
        } catch (err) {
          log.eip.error(`Error handling subscribe request: ${err}`);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: false, error: String(err) })));
        }
      }
    })();

    // Unsubscribe request handler (connection cleanup when zero subscribers)
    unsubscribeSub = nc.subscribe(unsubscribeSubject);
    log.eip.info(`Listening for unsubscribe requests on ${unsubscribeSubject}`);

    (async () => {
      for await (const msg of unsubscribeSub!) {
        try {
          const request = JSON.parse(new TextDecoder().decode(msg.data)) as UnsubscribeRequest;

          const zeroSubscribers = removeSubscriber(
            request.deviceId,
            request.subscriberId,
            request.tags,
          );

          if (zeroSubscribers) {
            log.eip.info(`No subscribers remaining for ${request.deviceId}, closing connection`);
            await removeConnection(request.deviceId);
          } else {
            // Update scan rate (fastest remaining subscriber)
            const conn = connections.get(request.deviceId);
            if (conn) {
              conn.scanRate = getDeviceScanRate(request.deviceId, conn.scanRate);
            }
          }

          msg.respond(new TextEncoder().encode(JSON.stringify({ success: true, count: request.tags.length })));
        } catch (err) {
          log.eip.error(`Error handling unsubscribe request: ${err}`);
          msg.respond(new TextEncoder().encode(JSON.stringify({ success: false, error: String(err) })));
        }
      }
    })();

    // Write command handler
    // Subscribe to ethernetip.command.> for targeted write commands
    writeSub = nc.subscribe(writeCommandSubject);
    log.eip.info(`Listening for write commands on ${writeCommandSubject}`);

    (async () => {
      for await (const msg of writeSub!) {
        handleWriteCommand(msg.subject, msg.data).catch((err) => {
          log.eip.error(`Error handling write command: ${err}`);
        });
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

  async function connectPlc(deviceId: string): Promise<boolean> {
    const conn = connections.get(deviceId);
    if (!conn) return false;
    if (conn.cip) return true;

    conn.connectionState = "connecting";
    conn.lastConnectAttempt = Date.now();

    try {
      log.eip.info(`[${deviceId}] Connecting to ${conn.host}:${conn.port}...`);
      conn.cip = await createCip({
        host: conn.host,
        port: conn.port,
      });

      // Success - reset failure count and update state
      conn.connectionState = "connected";
      conn.consecutiveFailures = 0;
      conn.totalReads = 0;
      conn.totalFailures = 0;
      log.eip.info(`[${deviceId}] Connected to ${conn.cip.identity?.productName || "Unknown"}`);
      return true;
    } catch (err) {
      conn.connectionState = "disconnected";
      conn.consecutiveFailures++;
      conn.cip = null;

      const backoff = getBackoffDelay(conn.consecutiveFailures);
      log.eip.warn(`[${deviceId}] Connection failed (attempt ${conn.consecutiveFailures}): ${err}`);
      log.eip.info(`[${deviceId}] Will retry in ${(backoff / 1000).toFixed(1)}s`);
      return false;
    }
  }

  async function disconnectPlc(deviceId: string): Promise<void> {
    const conn = connections.get(deviceId);
    if (!conn) return;

    if (conn.cip) {
      log.eip.info(`[${deviceId}] Disconnecting...`);
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

  async function pollPlc(deviceId: string): Promise<void> {
    const conn = connections.get(deviceId);
    if (!conn) return;

    conn.polling = true;
    log.eip.info(`[${deviceId}] Starting polling loop (scan rate: ${conn.scanRate}ms)`);

    // For periodic status logging
    let lastStatusLog = Date.now();
    const STATUS_LOG_INTERVAL = 30000; // Log status every 30 seconds

    while (!conn.abortController.signal.aborted) {
      const startTime = Date.now();

      // Get currently subscribed tags for this device
      const subscribedTags = getDeviceSubscribedTags(deviceId);

      if (subscribedTags.size === 0) {
        // No subscribers — stop polling and close connection
        log.eip.info(`[${deviceId}] No subscribed tags, stopping poll loop`);
        break;
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

        const connected = await connectPlc(deviceId);
        if (!connected) {
          continue; // Backoff handled in connectPlc
        }
      }

      // Read only subscribed variables that exist in this PLC's variables
      const variablesToRead = [...conn.variables.values()]
        .filter(v => subscribedTags.has(v.name));

      if (variablesToRead.length === 0) {
        await sleep(conn.scanRate, conn.abortController.signal);
        continue;
      }

      let readCount = 0;
      let failCount = 0;
      let connectionError = false;
      const wasFirstRead = conn.totalReads === 0;
      const sampleValues: string[] = []; // Collect sample values to log

      // Batch read using Multiple Service Request
      const tagNames = variablesToRead.map(v => v.name);
      const variableMap = new Map(variablesToRead.map(v => [v.name, v]));

      try {
        const results = await readMultipleTags(conn.cip!, tagNames);

        // Collect successful reads for batch publishing
        const valuesToPublish: { variableId: string; value: number | boolean | string; datatype: string }[] = [];

        for (const result of results) {
          if (conn.abortController.signal.aborted) break;

          const variable = variableMap.get(result.tagName);
          if (!variable) continue;

          if (result.response.success) {
            const { value, typeCode } = decodeTagValue(result.response.data, variable.datatype);

            // Proactively correct cached datatype
            const actualDatatype = typeCodeToDatatype(typeCode);
            if (actualDatatype && actualDatatype !== variable.datatype) {
              log.eip.debug(`[${deviceId}] Correcting datatype for ${variable.name}: ${variable.datatype} -> ${actualDatatype}`);
              variable.datatype = actualDatatype;
            }

            // Collect sample values for logging (first 2 tags)
            if (sampleValues.length < 2) {
              const shortName = variable.name.length > 30 ? "..." + variable.name.slice(-27) : variable.name;
              sampleValues.push(`${shortName}=${value}`);
            }

            valuesToPublish.push({ variableId: variable.name, value, datatype: variable.datatype });
            readCount++;
          } else {
            failCount++;
            if (failCount <= 3) {
              log.eip.debug(`[${deviceId}] Read failed for "${variable.name}": status 0x${result.response.status.toString(16)}`);
            }
          }
        }

        // Batch publish all values
        if (valuesToPublish.length > 0) {
          publishBatch(conn, valuesToPublish);
          conn.lastSuccessfulRead = Date.now();
        }
      } catch (err) {
        // Check if this is a connection error
        const errStr = String(err);
        if (errStr.includes("connection") || errStr.includes("socket") || errStr.includes("ECONNRESET") || errStr.includes("timeout")) {
          connectionError = true;
          log.eip.warn(`[${deviceId}] Connection error during batch read: ${err}`);
        } else {
          log.eip.warn(`[${deviceId}] Batch read error: ${err}`);
          failCount = variablesToRead.length;
        }
      }

      // Update statistics
      conn.totalReads += readCount;
      conn.totalFailures += failCount;

      // Log first successful read (confirms data is flowing)
      if (wasFirstRead && readCount > 0) {
        log.eip.info(`[${deviceId}] Data flowing: ${sampleValues.join(", ")}${variablesToRead.length > 2 ? ` (+${variablesToRead.length - 2} more)` : ""}`);
      }

      // Handle connection errors - force reconnect
      if (connectionError) {
        log.eip.warn(`[${deviceId}] Lost connection, will reconnect...`);
        conn.cip = null;
        conn.connectionState = "disconnected";
        conn.consecutiveFailures++;
        continue;
      }

      // Periodic status logging (every 30s, with sample values)
      const now = Date.now();
      if (now - lastStatusLog >= STATUS_LOG_INTERVAL && readCount > 0) {
        const sampleStr = sampleValues.length > 0 ? ` [${sampleValues.join(", ")}]` : "";
        log.eip.info(`[${deviceId}] Polling ${variablesToRead.length} tags @ ${conn.scanRate}ms` +
          ` (${conn.totalReads} reads, ${conn.totalFailures} failures)${sampleStr}`);
        lastStatusLog = now;
      }

      // Log summary at debug level
      if (readCount > 0 || failCount > 0) {
        log.eip.debug(`[${deviceId}] Poll: ${readCount}/${variablesToRead.length} success, ${failCount} failed`);
      }

      // Wait for next scan (use current scan rate which may have been updated)
      const elapsed = Date.now() - startTime;
      const delay = Math.max(0, conn.scanRate - elapsed);
      if (delay > 0) {
        await sleep(delay, conn.abortController.signal);
      }
    }

    conn.polling = false;
    conn.connectionState = "disconnected";
    log.eip.info(`[${deviceId}] Polling stopped (total reads: ${conn.totalReads}, failures: ${conn.totalFailures})`);
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
  // Public API
  // ─────────────────────────────────────────────────────────────────────────

  const manager: ScannerManager = {
    start() {
      log.eip.info("Starting scanner (stateless, subscriber-driven)...");
      log.eip.info("Zero connections — devices will connect on subscribe/browse");

      // Start request handlers
      startRequestHandlers();

      log.eip.info("Scanner started, waiting for subscribe/browse requests");
    },

    async stop() {
      log.eip.info("Stopping scanner...");

      // Stop request handlers
      variablesSub?.unsubscribe();
      browseSub?.unsubscribe();
      subscribeSub?.unsubscribe();
      unsubscribeSub?.unsubscribe();
      writeSub?.unsubscribe();

      // Stop all polling loops and disconnect
      for (const [deviceId, conn] of connections) {
        conn.abortController.abort();
        await disconnectPlc(deviceId);
      }

      connections.clear();
      deviceSubscribers.clear();
      log.eip.info("Scanner stopped");
    },
  };

  return manager;
}
