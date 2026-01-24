/**
 * Binary encoding/decoding utilities for EtherNet/IP
 * All integers are little-endian as per CIP specification
 */

/**
 * Encode a string to UTF-8 bytes
 */
export function encodeString(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

/**
 * Decode UTF-8 bytes to a string
 */
export function decodeString(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

/**
 * Encode an unsigned integer to little-endian bytes
 */
export function encodeUint(value: number, size: 1 | 2 | 4): Uint8Array {
  const buffer = new ArrayBuffer(size);
  const view = new DataView(buffer);
  switch (size) {
    case 1:
      view.setUint8(0, value);
      break;
    case 2:
      view.setUint16(0, value, true);
      break;
    case 4:
      view.setUint32(0, value, true);
      break;
  }
  return new Uint8Array(buffer);
}

/**
 * Decode little-endian bytes to an unsigned integer
 */
export function decodeUint(value: Uint8Array): number {
  const buffer = value.slice().buffer;
  const view = new DataView(buffer);
  switch (value.length) {
    case 1:
      return view.getUint8(0);
    case 2:
      return view.getUint16(0, true);
    case 4:
      return view.getUint32(0, true);
    default:
      return NaN;
  }
}

/**
 * Encode a signed integer to little-endian bytes
 */
export function encodeInt(value: number, size: 1 | 2 | 4): Uint8Array {
  const buffer = new ArrayBuffer(size);
  const view = new DataView(buffer);
  switch (size) {
    case 1:
      view.setInt8(0, value);
      break;
    case 2:
      view.setInt16(0, value, true);
      break;
    case 4:
      view.setInt32(0, value, true);
      break;
  }
  return new Uint8Array(buffer);
}

/**
 * Decode little-endian bytes to a signed integer
 */
export function decodeInt(value: Uint8Array): number {
  const buffer = value.slice().buffer;
  const view = new DataView(buffer);
  switch (value.length) {
    case 1:
      return view.getInt8(0);
    case 2:
      return view.getInt16(0, true);
    case 4:
      return view.getInt32(0, true);
    default:
      return NaN;
  }
}

/**
 * Encode a 32-bit float to little-endian bytes
 */
export function encodeFloat32(value: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  const view = new DataView(buffer);
  view.setFloat32(0, value, true);
  return new Uint8Array(buffer);
}

/**
 * Decode little-endian bytes to a 32-bit float
 */
export function decodeFloat32(value: Uint8Array): number {
  const buffer = value.slice().buffer;
  const view = new DataView(buffer);
  return view.getFloat32(0, true);
}

/**
 * Encode a 64-bit float to little-endian bytes
 */
export function encodeFloat64(value: number): Uint8Array {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, value, true);
  return new Uint8Array(buffer);
}

/**
 * Decode little-endian bytes to a 64-bit float
 */
export function decodeFloat64(value: Uint8Array): number {
  const buffer = value.slice().buffer;
  const view = new DataView(buffer);
  return view.getFloat64(0, true);
}

/**
 * Join multiple Uint8Arrays into a single array
 */
export function joinBytes(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((acc, curr) => acc + curr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const array of arrays) {
    result.set(array, offset);
    offset += array.length;
  }
  return result;
}

/**
 * Convert a Uint8Array to a hex string for debugging
 */
export function bufferToHex(buffer: Uint8Array): string {
  return Array.from(buffer)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Convert a hex string to a Uint8Array
 */
export function hexToBuffer(hex: string): Uint8Array {
  const bytes = hex.match(/.{1,2}/g) || [];
  return new Uint8Array(bytes.map((byte) => parseInt(byte, 16)));
}

/**
 * Get random bytes using crypto API
 */
export function getRandomBytes(length: number): Uint8Array {
  const array = new Uint8Array(length);
  crypto.getRandomValues(array);
  return array;
}
