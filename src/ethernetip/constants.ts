/**
 * EtherNet/IP protocol constants
 */

import { encodeString, encodeUint } from "./encode.ts";
import { createLogicalSegment } from "./dataTypes.ts";
import { classCode } from "./objectLibrary.ts";

// Default EtherNet/IP port
export const DEFAULT_PORT = 44818;

// Protocol version
export const PROTOCOL_VERSION = encodeUint(1, 2);

// Context string (pycomm compatible)
export const CONTEXT = encodeString("_pycomm_");

// Connection parameters
export const PRIORITY = new Uint8Array([0x0a]);
export const TIMEOUT_TICKS = new Uint8Array([0x05]);
export const TIMEOUT_MULTIPLIER = new Uint8Array([0x07]);
export const TRANSPORT_CLASS = new Uint8Array([0xa3]);

// Default RPI (Requested Packet Interval) in microseconds
// 100ms = 100,000 µs = 0x000186A0 (little-endian: a0 86 01 00)
export const DEFAULT_RPI = new Uint8Array([0xa0, 0x86, 0x01, 0x00]);

// Message router path: Class 0x02, Instance 0x01
export const MESSAGE_ROUTER_PATH = [
  createLogicalSegment(classCode.messageRouter, "classId"),
  createLogicalSegment(new Uint8Array([0x01]), "instanceId"),
];

// Connection Manager path: Class 0x06, Instance 0x01
// Used for ForwardOpen/ForwardClose requests
export const CONNECTION_MANAGER_PATH = [
  createLogicalSegment(classCode.connectionManager, "classId"),
  createLogicalSegment(new Uint8Array([0x01]), "instanceId"),
];

// Connection sizes
export const SMALL_CONNECTION_SIZE = 500;
export const LARGE_CONNECTION_SIZE = 4000;

// Extended symbol segment type
export const EXTENDED_SYMBOL = 0x91;

// Minimum firmware versions for features
export const MIN_VER_INSTANCE_IDS = 21;
export const MIN_VER_EXTERNAL_ACCESS = 18;
