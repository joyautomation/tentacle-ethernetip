/**
 * EtherNet/IP module exports
 */

// Core connection and CIP
export { createCip, destroyCip, getConnectionSize } from "./cip.ts";
export type { Cip, DeviceIdentity } from "./cip.ts";

// Request/response
export {
  sendConnectedRequest,
  sendUnconnectedRequest,
  readTag,
  writeTag,
  getAttributeSingle,
  setAttributeSingle,
} from "./request.ts";
export type { CipResponse } from "./request.ts";

// Services and class codes
export {
  encapsulation,
  connectionManager,
  service,
  CIP_DATA_TYPES,
} from "./services.ts";
export type { CipDataType } from "./services.ts";

export {
  classCode,
  connectionManagerInstances,
  SYMBOL_TYPE,
} from "./objectLibrary.ts";

// Data types and encoding
export {
  createLogicalSegment,
  encodeEpath,
  buildRequestPath,
  encodeSymbolicPath,
} from "./dataTypes.ts";
export type { LogicalSegment, LogicalType } from "./dataTypes.ts";

export {
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
  joinBytes,
  bufferToHex,
  hexToBuffer,
  getRandomBytes,
} from "./encode.ts";

// Constants
export {
  DEFAULT_PORT,
  EXTENDED_SYMBOL,
  MIN_VER_INSTANCE_IDS,
  MIN_VER_EXTERNAL_ACCESS,
} from "./constants.ts";

// Status and lookups
export {
  SERVICE_STATUS,
  EXTERNAL_ACCESS,
  getVendorName,
  getProductTypeName,
  getServiceStatusMessage,
} from "./status.ts";

// Tag discovery
export {
  browseTags,
  browseAllTags,
  listPrograms,
} from "./discovery.ts";
export type { DiscoveredTag } from "./discovery.ts";
