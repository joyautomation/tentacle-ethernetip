/**
 * tentacle-ethernetip - EtherNet/IP to NATS bridge
 *
 * @module
 */

// Service
export { createService } from "./src/service.ts";
export type { EtherNetIPService } from "./src/service.ts";

// Configuration types
export type {
  ServiceConfig,
  RuntimeConfig,
  NatsConfig,
  MqttConfig,
  DefaultsConfig,
  DeadbandConfig,
  CipDataTypeName,
  DeviceConfig,
  RockwellDeviceConfig,
  GenericDeviceConfig,
  TagConfig,
  RockwellTagConfig,
  GenericTagConfig,
} from "./types/config.ts";
export {
  isRockwellDevice,
  isGenericDevice,
  isRockwellTag,
  isGenericTag,
} from "./types/config.ts";

// Device types
export type {
  DeviceState,
  DeviceStats,
  TagValue,
  DeviceEventHandlers,
} from "./src/devices/mod.ts";
export {
  BaseDevice,
  RockwellDevice,
  GenericDevice,
  createDevice,
} from "./src/devices/mod.ts";

// EtherNet/IP (low-level)
export {
  createCip,
  destroyCip,
  readTag,
  writeTag,
  getAttributeSingle,
  setAttributeSingle,
  browseTags,
  browseAllTags,
  CIP_DATA_TYPES,
  service,
  classCode,
  // Encoding/decoding utilities
  encodeUint,
  decodeUint,
  encodeInt,
  decodeInt,
  encodeFloat32,
  decodeFloat32,
  encodeFloat64,
  decodeFloat64,
  encodeString,
  decodeString,
} from "./src/ethernetip/mod.ts";
export type { Cip, DeviceIdentity, CipResponse, DiscoveredTag } from "./src/ethernetip/mod.ts";

// Logging
export { setLogLevel, LogLevel } from "./src/utils/logger.ts";
