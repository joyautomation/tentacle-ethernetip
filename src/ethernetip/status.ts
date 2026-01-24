/**
 * EtherNet/IP and CIP status codes and lookup tables
 */

/**
 * Encapsulation status codes
 */
export const ENCAP_STATUS = {
  0x0000: "Success",
  0x0001: "Invalid or unsupported encapsulation command",
  0x0002: "Insufficient memory",
  0x0003: "Poorly formed or incorrect data",
  0x0064: "Invalid session handle",
  0x0065: "Invalid message length",
  0x0069: "Unsupported Protocol Version",
} as const;

/**
 * CIP service status codes
 */
export const SERVICE_STATUS = {
  0x00: "Success",
  0x01: "Connection failure",
  0x02: "Insufficient resource",
  0x03: "Invalid value",
  0x04: "IOI syntax error",
  0x05: "Destination unknown",
  0x06: "Insufficient Packet Space",
  0x07: "Connection lost",
  0x08: "Service not supported",
  0x09: "Error in data segment",
  0x0a: "Attribute list error",
  0x0b: "State already exists",
  0x0c: "Object state conflict",
  0x0d: "Object already exists",
  0x0e: "Attribute not settable",
  0x0f: "Permission denied",
  0x10: "Device state conflict",
  0x11: "Reply data too large",
  0x12: "Fragmentation error",
  0x13: "Insufficient command data",
  0x14: "Attribute not supported",
  0x15: "Too much data",
  0x16: "Object does not exist",
  0x1f: "Connection related failure",
  0xff: "General Error",
} as const;

/**
 * Extended status codes for specific service status values
 */
export const EXTENDED_STATUS = {
  0x01: {
    0x0100: "Connection in use",
    0x0103: "Transport not supported",
    0x0107: "Connection not found",
    0x0109: "Invalid connection size",
    0x0203: "Connection timeout",
  },
  0x05: {
    0x0000: "Extended status out of memory",
    0x0001: "Extended status out of instances",
  },
  0xff: {
    0x2001: "Excessive IOI",
    0x2002: "Bad parameter value",
    0x2107: "Data type mismatch",
    0x210a: "Invalid symbol name",
    0x210b: "Symbol does not exist",
  },
} as const;

/**
 * External access levels for tags
 */
export const EXTERNAL_ACCESS = {
  0: "Read/Write",
  1: "Reserved",
  2: "Read Only",
  3: "None",
} as const;

/**
 * Common product types
 */
export const PRODUCT_TYPES: Record<number, string> = {
  0x00: "Generic Device",
  0x02: "AC Drive",
  0x03: "Motor Overload",
  0x07: "General Purpose Discrete I/O",
  0x0c: "Communications Adapter",
  0x0e: "Programmable Logic Controller",
  0x10: "Position Controller",
  0x18: "Human-Machine Interface",
  0x22: "Encoder",
  0x25: "CIP Motion Drive",
  0x2c: "Managed Switch",
};

/**
 * Common vendors (subset)
 */
export const VENDORS: Record<number, string> = {
  1: "Rockwell Automation/Allen-Bradley",
  2: "Namco Controls Corp.",
  3: "Honeywell International Inc.",
  4: "Parker Hannifin Corporation",
  5: "Rockwell Automation/Reliance Elec.",
  46: "ABB, Inc.",
  47: "Omron Corporation",
  48: "TURCK",
  82: "Mitsubishi Electric Automation, Inc.",
  145: "Siemens",
  161: "Mitsubishi Electric Corporation",
  243: "Schneider Electric",
  562: "Phoenix Contact",
  591: "FANUC CORPORATION",
  722: "ABB Inc.",
  796: "Siemens Industry, Inc.",
  808: "SICK AG",
  1251: "Siemens AG",
};

/**
 * Get vendor name from ID
 */
export function getVendorName(id: number): string {
  return VENDORS[id] ?? `Unknown Vendor (${id})`;
}

/**
 * Get product type name from ID
 */
export function getProductTypeName(id: number): string {
  return PRODUCT_TYPES[id] ?? `Unknown Type (${id})`;
}

/**
 * Get service status message
 */
export function getServiceStatusMessage(status: number): string {
  return (
    SERVICE_STATUS[status as keyof typeof SERVICE_STATUS] ??
      `Unknown status (0x${status.toString(16)})`
  );
}
