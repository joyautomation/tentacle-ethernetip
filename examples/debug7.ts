/**
 * Debug - try backplane connection path instead of Message Router
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

function joinBytes(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((acc, curr) => acc + curr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const array of arrays) { result.set(array, offset); offset += array.length; }
  return result;
}

async function main() {
  console.log(`Connecting to ${plcIp}:${port}...`);
  const conn = await Deno.connect({ hostname: plcIp, port });

  async function sendReceive(name: string, request: Uint8Array): Promise<Uint8Array> {
    console.log(`\n--- ${name} ---`);
    console.log(`TX: ${toHex(request.slice(0, 60))}...`);
    await conn.write(request);
    const buffer = new Uint8Array(4096);
    const n = await conn.read(buffer);
    const response = buffer.slice(0, n!);
    console.log(`RX: ${toHex(response)}`);
    return response;
  }

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

  const cid = crypto.getRandomValues(new Uint8Array(4));
  const csn = new Uint8Array([0x27, 0x04]);
  const vid = new Uint8Array([0x09, 0x10]);
  const vsn = crypto.getRandomValues(new Uint8Array(4));

  // STANDARD ForwardOpen (0x4d) with 2-byte net params
  const connectionSize = 500;
  const netParams = encodeUint((connectionSize & 0x01ff) | 0x4200, 2);

  // RPI 500ms
  const rpi = encodeUint(500000, 4);

  // Request path to Connection Manager (class 0x06, instance 0x01)
  const requestPath = new Uint8Array([0x02, 0x20, 0x06, 0x24, 0x01]);

  // Try backplane/slot 0 connection path: port segment (0x01) + link address (0x00)
  // For CompactLogix, slot 0 is the CPU
  const backplanePath = new Uint8Array([0x01, 0x01, 0x00]);  // 1 word: port 1, slot 0

  const forwardOpenParams = joinBytes([
    new Uint8Array([0x0a, 0x05]),
    new Uint8Array([0x00, 0x00, 0x00, 0x00]),
    cid, csn, vid, vsn,
    new Uint8Array([0x07, 0x00, 0x00, 0x00]),
    rpi, netParams,
    rpi, netParams,
    new Uint8Array([0xa3]),
  ]);

  console.log("Test 1: Backplane path (port 1, slot 0)");
  let cipMessage = joinBytes([
    new Uint8Array([0x4d]),
    requestPath,
    forwardOpenParams,
    backplanePath,
  ]);

  let cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb2, 0x00]),
    encodeUint(cipMessage.length, 2),
    cipMessage,
  ]);

  let request = joinBytes([
    new Uint8Array([0x6f, 0x00]),
    encodeUint(cpf.length, 2),
    session,
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    cpf,
  ]);

  let response = await sendReceive("ForwardOpen (backplane)", request);
  console.log(`Status: 0x${response[42].toString(16)}, Ext: ${response[43] > 0 ? '0x' + decodeUint(response.slice(44, 46)).toString(16) : 'none'}`);

  // Test 2: Empty connection path (null path)
  console.log("\nTest 2: Empty connection path");
  cipMessage = joinBytes([
    new Uint8Array([0x4d]),
    requestPath,
    forwardOpenParams,
    new Uint8Array([0x00]),  // 0 words connection path
  ]);

  cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb2, 0x00]),
    encodeUint(cipMessage.length, 2),
    cipMessage,
  ]);

  request = joinBytes([
    new Uint8Array([0x6f, 0x00]),
    encodeUint(cpf.length, 2),
    session,
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    cpf,
  ]);

  response = await sendReceive("ForwardOpen (empty path)", request);
  console.log(`Status: 0x${response[42].toString(16)}, Ext: ${response[43] > 0 ? '0x' + decodeUint(response.slice(44, 46)).toString(16) : 'none'}`);

  // Test 3: Large ForwardOpen with backplane path
  console.log("\nTest 3: Large ForwardOpen with backplane path");
  const largeNetParams = encodeUint((4000 & 0xFFFF) | (0x4200 << 16), 4);

  const largeParams = joinBytes([
    new Uint8Array([0x0a, 0x05]),
    new Uint8Array([0x00, 0x00, 0x00, 0x00]),
    cid, csn, vid, vsn,
    new Uint8Array([0x07, 0x00, 0x00, 0x00]),
    rpi, largeNetParams,
    rpi, largeNetParams,
    new Uint8Array([0xa3]),
  ]);

  cipMessage = joinBytes([
    new Uint8Array([0x54]),  // Large ForwardOpen
    requestPath,
    largeParams,
    backplanePath,
  ]);

  cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb2, 0x00]),
    encodeUint(cipMessage.length, 2),
    cipMessage,
  ]);

  request = joinBytes([
    new Uint8Array([0x6f, 0x00]),
    encodeUint(cpf.length, 2),
    session,
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    cpf,
  ]);

  response = await sendReceive("Large ForwardOpen (backplane)", request);
  console.log(`Status: 0x${response[42].toString(16)}, Ext: ${response[43] > 0 ? '0x' + decodeUint(response.slice(44, 46)).toString(16) : 'none'}`);

  conn.close();
}

main().catch(console.error);
