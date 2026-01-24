/**
 * Direct unconnected ReadTag (simpler approach without UnconnectedSend wrapper)
 */

import { DEFAULT_PORT } from "../src/ethernetip/constants.ts";

const plcIp = Deno.args[0] || "client4";
const port = DEFAULT_PORT;

function toHex(data: Uint8Array): string {
  return Array.from(data).map((b) => b.toString(16).padStart(2, "0")).join(" ");
}

function encodeUint(value: number, size: 1 | 2 | 4): Uint8Array {
  const buffer = new ArrayBuffer(size);
  const view = new DataView(buffer);
  if (size === 1) view.setUint8(0, value);
  else if (size === 2) view.setUint16(0, value, true);
  else view.setUint32(0, value, true);
  return new Uint8Array(buffer);
}

function decodeUint(data: Uint8Array): number {
  const view = new DataView(data.buffer, data.byteOffset, data.length);
  if (data.length === 1) return view.getUint8(0);
  if (data.length === 2) return view.getUint16(0, true);
  if (data.length === 4) return view.getUint32(0, true);
  return 0;
}

function decodeFloat32(data: Uint8Array): number {
  const view = new DataView(data.buffer, data.byteOffset, 4);
  return view.getFloat32(0, true);
}

function joinBytes(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((acc, curr) => acc + curr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const array of arrays) { result.set(array, offset); offset += array.length; }
  return result;
}

function encodeSymbolicPath(tagName: string): Uint8Array {
  const nameBytes = new TextEncoder().encode(tagName);
  const needsPad = nameBytes.length % 2 === 1;
  return joinBytes([
    new Uint8Array([0x91, nameBytes.length]),
    nameBytes,
    needsPad ? new Uint8Array([0x00]) : new Uint8Array(),
  ]);
}

async function main() {
  console.log(`Connecting to ${plcIp}:${port}...`);
  const conn = await Deno.connect({ hostname: plcIp, port });

  // Register session
  const regReq = joinBytes([
    new Uint8Array([0x65, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
    new TextEncoder().encode("_pycomm_"),
    new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]),
  ]);
  await conn.write(regReq);
  const regBuf = new Uint8Array(128);
  await conn.read(regBuf);
  const session = regBuf.slice(4, 8);
  console.log(`Session: ${decodeUint(session)}\n`);

  // Direct ReadTag via SendRRData (like tag browsing does with GetInstanceAttributeList)
  const tagName = "P2P_RTU46_RESRCL_LIT_00A_LI";
  console.log(`Reading tag: ${tagName}`);

  const tagPath = encodeSymbolicPath(tagName);
  const pathSizeWords = Math.floor(tagPath.length / 2);

  // CIP Message: ReadTag (0x4c) + path size + path + element count
  const cipMessage = joinBytes([
    new Uint8Array([0x4c]),  // ReadTag service
    encodeUint(pathSizeWords, 1),  // Path size in words
    tagPath,
    encodeUint(1, 2),  // Element count
  ]);

  console.log(`CIP message: ${toHex(cipMessage)}`);
  console.log(`  Service: 0x4c (ReadTag)`);
  console.log(`  Path size: ${pathSizeWords} words`);
  console.log(`  Tag path: ${toHex(tagPath)}`);

  // Wrap in SendRRData
  const cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]),  // Interface handle
    new Uint8Array([0x0a, 0x00]),  // Timeout
    new Uint8Array([0x02, 0x00]),  // Item count
    new Uint8Array([0x00, 0x00]),  // Null address type
    new Uint8Array([0x00, 0x00]),  // Address length 0
    new Uint8Array([0xb2, 0x00]),  // Unconnected data type
    encodeUint(cipMessage.length, 2),
    cipMessage,
  ]);

  const request = joinBytes([
    new Uint8Array([0x6f, 0x00]),  // SendRRData
    encodeUint(cpf.length, 2),
    session,
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    cpf,
  ]);

  console.log(`\nSending direct ReadTag request...`);
  console.log(`TX: ${toHex(request)}`);

  await conn.write(request);
  const buffer = new Uint8Array(4096);
  const n = await conn.read(buffer);
  const response = buffer.slice(0, n!);

  console.log(`RX: ${toHex(response)}`);

  // Parse response
  const cipOffset = 40;
  const service = response[cipOffset];
  const status = response[cipOffset + 2];
  const extSize = response[cipOffset + 3];

  console.log(`\nService: 0x${service.toString(16)}, Status: 0x${status.toString(16)}`);

  if (status === 0) {
    const dataOffset = cipOffset + 4 + extSize * 2;
    const typeCode = decodeUint(response.slice(dataOffset, dataOffset + 2));
    const valueData = response.slice(dataOffset + 2, dataOffset + 6);

    console.log(`Type code: 0x${typeCode.toString(16)}`);

    if (typeCode === 0x00ca) {
      const value = decodeFloat32(valueData);
      console.log(`Value: ${value}`);
    } else if (typeCode === 0x00c1) {
      console.log(`Value (BOOL): ${valueData[0] !== 0}`);
    } else {
      console.log(`Raw value: ${toHex(valueData)}`);
    }
  } else if (extSize > 0) {
    const extCode = decodeUint(response.slice(cipOffset + 4, cipOffset + 6));
    console.log(`Extended status: 0x${extCode.toString(16)}`);
  }

  conn.close();
}

main().catch(console.error);
