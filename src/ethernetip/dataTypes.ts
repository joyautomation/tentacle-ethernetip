/**
 * CIP path segment encoding
 */

import { encodeUint, joinBytes } from "./encode.ts";

/**
 * Logical segment types
 */
const logicalTypes = {
  classId: 0b00000000,
  instanceId: 0b00000100,
  memberId: 0b00001000,
  connectionPoint: 0b00001100,
  attributeId: 0b00010000,
  special: 0b00010100,
  serviceId: 0b00011000,
} as const;

export type LogicalType = keyof typeof logicalTypes;

/**
 * Logical format sizes
 */
const logicalFormat: Record<number, number> = {
  1: 0b00000000, // 8-bit
  2: 0b00000001, // 16-bit
  4: 0b00000011, // 32-bit
};

/**
 * A CIP logical segment
 */
export type LogicalSegment = {
  segmentType: 0b00100000;
  logicalType: LogicalType;
  logicalValue: Uint8Array;
  encode: (padded?: boolean) => Uint8Array;
};

/**
 * Encode a logical segment to bytes
 *
 * CIP logical segment format:
 * - 8-bit: [header, value] (2 bytes)
 * - 16-bit: [header, 0x00, value_lo, value_hi] (4 bytes) - pad AFTER header
 * - 32-bit: [header, 0x00, value bytes...] (6 bytes) - pad AFTER header
 */
export function encodeLogicalSegment(
  segment: LogicalSegment,
  padded: boolean = false,
): Uint8Array {
  const logicalType = logicalTypes[segment.logicalType];
  const value = segment.logicalValue.length === 1
    ? segment.logicalValue[0]
    : segment.logicalValue.length === 2
    ? new DataView(segment.logicalValue.buffer).getUint16(0, true)
    : new DataView(segment.logicalValue.buffer).getUint32(0, true);

  const size = value <= 0xff ? 1 : value <= 0xffff ? 2 : 4;
  const format = logicalFormat[size];
  const { segmentType } = segment;

  const header = new Uint8Array([segmentType | logicalType | format]);
  const valueBytes = encodeUint(value, size as 1 | 2 | 4);

  // For 16-bit and 32-bit values, CIP requires a pad byte AFTER the header
  if (size > 1) {
    const result = joinBytes([header, new Uint8Array([0x00]), valueBytes]);
    // Add trailing pad if needed for word alignment
    if (padded && result.length % 2 !== 0) {
      return joinBytes([result, new Uint8Array([0x00])]);
    }
    return result;
  }

  // 8-bit: no internal padding needed
  if (padded && (header.length + valueBytes.length) % 2 !== 0) {
    return joinBytes([header, valueBytes, new Uint8Array([0x00])]);
  }
  return joinBytes([header, valueBytes]);
}

/**
 * Create a logical segment
 */
export function createLogicalSegment(
  logicalValue: Uint8Array,
  logicalType: LogicalType,
): LogicalSegment {
  return {
    segmentType: 0b00100000,
    logicalValue,
    logicalType,
    encode(padded?: boolean) {
      return encodeLogicalSegment(this, padded);
    },
  };
}

/**
 * Port segment types
 */
const portSegments = {
  backplane: 0b00000001,
  bp: 0b00000001,
  enet: 0b00000010,
  dhrioa: 0b00000010,
  dhriob: 0b00000011,
  dnet: 0b00000010,
  cnet: 0b00000010,
  dh485a: 0b00000010,
  dh485b: 0b00000011,
} as const;

export type PortType = keyof typeof portSegments;

/**
 * Encode an EPATH from an array of segments
 */
export function encodeEpath(
  segments: (LogicalSegment | Uint8Array)[],
  includeLength: boolean = false,
  padLength: boolean = false,
): Uint8Array {
  const path = segments.reduce((acc: Uint8Array, segment) => {
    if (segment instanceof Uint8Array) {
      return joinBytes([acc, segment]);
    }
    return joinBytes([acc, segment.encode(false)]);
  }, new Uint8Array());

  if (includeLength) {
    const lenBytes = encodeUint(Math.floor(path.length / 2), 1);
    const lenArray = padLength
      ? joinBytes([lenBytes, new Uint8Array([0x00])])
      : lenBytes;
    return joinBytes([lenArray, path]);
  }

  return path;
}

/**
 * Build a request path from class, instance, and optional attribute
 */
export function buildRequestPath(
  classCodeValue: Uint8Array,
  instance: Uint8Array,
  attribute?: Uint8Array,
): Uint8Array {
  const segments = [
    createLogicalSegment(classCodeValue, "classId"),
    createLogicalSegment(instance, "instanceId"),
  ];
  if (attribute && attribute.length > 0) {
    segments.push(createLogicalSegment(attribute, "attributeId"));
  }
  return encodeEpath(segments, true);
}

/**
 * Encode a single symbolic segment
 */
function encodeSymbolicSegment(name: string): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const needsPad = nameBytes.length % 2 === 1;

  // 0x91 = Extended symbolic segment
  return joinBytes([
    new Uint8Array([0x91, nameBytes.length]),
    nameBytes,
    needsPad ? new Uint8Array([0x00]) : new Uint8Array(),
  ]);
}

/**
 * Encode a symbolic tag name path (for Rockwell controllers)
 * Handles dot notation for UDT/AOI member access (e.g., "MyTag.Member")
 * Also handles array indices (e.g., "MyArray[0]" or "MyTag.Array[5].Member")
 */
export function encodeSymbolicPath(tagName: string): Uint8Array {
  const segments: Uint8Array[] = [];

  // Split by dots, but preserve array indices
  // e.g., "Tag.Member[0].SubMember" -> ["Tag", "Member[0]", "SubMember"]
  const parts = tagName.split(".");

  for (const part of parts) {
    // Check for array index: "Name[index]"
    const arrayMatch = part.match(/^(.+?)\[(\d+)\]$/);

    if (arrayMatch) {
      const [, name, indexStr] = arrayMatch;
      const index = parseInt(indexStr, 10);

      // Encode the symbolic name
      segments.push(encodeSymbolicSegment(name));

      // Encode the array index as a member element segment
      // Use appropriate size based on index value
      if (index <= 0xff) {
        segments.push(new Uint8Array([0x28, index]));
      } else if (index <= 0xffff) {
        segments.push(
          joinBytes([
            new Uint8Array([0x29, 0x00]),
            encodeUint(index, 2),
          ]),
        );
      } else {
        segments.push(
          joinBytes([
            new Uint8Array([0x2a, 0x00]),
            encodeUint(index, 4),
          ]),
        );
      }
    } else {
      // Plain symbolic name
      segments.push(encodeSymbolicSegment(part));
    }
  }

  return joinBytes(segments);
}
