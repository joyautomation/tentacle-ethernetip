/**
 * Debug script - try standard ForwardOpen directly
 */

import { DEFAULT_PORT } from "../src/ethernetip/constants.ts";

const plcIp = Deno.args[0] || "client4";
const port = DEFAULT_PORT;

function toHex(data: Uint8Array): string {
  return Array.from(data)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join(" ");
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
  for (const array of arrays) {
    result.set(array, offset);
    offset += array.length;
  }
  return result;
}

async function main() {
  console.log(`\n=== Connecting to ${plcIp}:${port} ===\n`);

  const conn = await Deno.connect({ hostname: plcIp, port });
  console.log("TCP connected");

  async function sendReceive(name: string, request: Uint8Array): Promise<Uint8Array> {
    console.log(`\n--- ${name} ---`);
    console.log(`TX (${request.length} bytes):`);
    // Print in rows of 16 bytes
    for (let i = 0; i < request.length; i += 16) {
      const chunk = request.slice(i, Math.min(i + 16, request.length));
      console.log(`  ${toHex(chunk)}`);
    }

    await conn.write(request);

    const buffer = new Uint8Array(4096);
    const n = await conn.read(buffer);
    const response = buffer.slice(0, n!);

    console.log(`RX (${response.length} bytes):`);
    for (let i = 0; i < response.length; i += 16) {
      const chunk = response.slice(i, Math.min(i + 16, response.length));
      console.log(`  ${toHex(chunk)}`);
    }
    return response;
  }

  // 1. RegisterSession
  const registerSession = joinBytes([
    new Uint8Array([0x65, 0x00]),       // Command: RegisterSession
    encodeUint(4, 2),                   // Length: 4
    encodeUint(0, 4),                   // Session: 0
    new Uint8Array([0, 0, 0, 0]),       // Status
    new TextEncoder().encode("_pycomm_"), // Context
    encodeUint(0, 4),                   // Options
    encodeUint(1, 2),                   // Protocol version
    new Uint8Array([0, 0]),             // Option flags
  ]);

  const regResp = await sendReceive("RegisterSession", registerSession);
  const session = regResp.slice(4, 8);
  console.log(`\nSession handle: ${decodeUint(session)}`);

  // 2. Standard ForwardOpen (0x4d)
  const cid = crypto.getRandomValues(new Uint8Array(4));
  const csn = new Uint8Array([0x27, 0x04]);
  const vid = new Uint8Array([0x09, 0x10]);
  const vsn = crypto.getRandomValues(new Uint8Array(4));

  // Connection size and network params for STANDARD ForwardOpen (2 bytes)
  const connectionSize = 500;
  // Bits: Owner=0 (exclusive), Type=01 (P2P), Priority=01 (low), Fixed=0, Size=500
  const netParams = encodeUint((connectionSize & 0x01ff) | 0x4200, 2);

  // Message router path: Class 0x02 (Message Router), Instance 0x01
  // Encoded as: 0x20 0x02 0x24 0x01
  const routePath = new Uint8Array([0x20, 0x02, 0x24, 0x01]);

  console.log(`\nConnection params:`);
  console.log(`  CID: ${toHex(cid)}`);
  console.log(`  CSN: ${toHex(csn)}`);
  console.log(`  VID: ${toHex(vid)}`);
  console.log(`  VSN: ${toHex(vsn)}`);
  console.log(`  Connection size: ${connectionSize}`);
  console.log(`  Net params: ${toHex(netParams)}`);

  const forwardOpenCip = joinBytes([
    new Uint8Array([0x4d]),               // Service: ForwardOpen (0x4d)
    encodeUint(2, 1),                     // Path size: 2 words
    routePath,                            // Route path
    new Uint8Array([0x0a]),               // Priority/Time_tick
    new Uint8Array([0x05]),               // Timeout_ticks
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // O->T CID (filled by target)
    cid,                                   // T->O CID (our CID)
    csn,                                   // Connection serial number
    vid,                                   // Vendor ID
    vsn,                                   // Vendor serial
    new Uint8Array([0x07]),               // Connection timeout multiplier
    new Uint8Array([0x00, 0x00, 0x00]),   // Reserved
    new Uint8Array([0x01, 0x40, 0x20, 0x00]), // O->T RPI (2097153 us = ~2s)
    netParams,                             // O->T Network params (2 bytes for standard)
    new Uint8Array([0x01, 0x40, 0x20, 0x00]), // T->O RPI
    netParams,                             // T->O Network params (2 bytes for standard)
    new Uint8Array([0xa3]),               // Transport type/trigger: Class 3, server
  ]);

  console.log(`\nCIP message length: ${forwardOpenCip.length}`);

  // Wrap in SendRRData
  const cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // Interface handle
    new Uint8Array([0x0a, 0x00]),              // Timeout
    new Uint8Array([0x02, 0x00]),              // Item count: 2
    new Uint8Array([0x00, 0x00]),              // Address type: Null
    new Uint8Array([0x00, 0x00]),              // Address length: 0
    new Uint8Array([0xb2, 0x00]),              // Data type: Unconnected
    encodeUint(forwardOpenCip.length, 2),     // Data length
    forwardOpenCip,
  ]);

  const forwardOpen = joinBytes([
    new Uint8Array([0x6f, 0x00]),        // Command: SendRRData
    encodeUint(cpf.length, 2),           // Length
    session,                             // Session
    new Uint8Array([0, 0, 0, 0]),        // Status
    new TextEncoder().encode("_pycomm_"), // Context
    encodeUint(0, 4),                    // Options
    cpf,
  ]);

  const foResp = await sendReceive("Standard ForwardOpen (0x4d)", forwardOpen);

  // Parse
  const cipOffset = 40;
  console.log(`\nParsing response:`);
  console.log(`  Service reply: 0x${foResp[cipOffset].toString(16)}`);
  console.log(`  Reserved: 0x${foResp[cipOffset + 1].toString(16)}`);
  console.log(`  Status: 0x${foResp[cipOffset + 2].toString(16)}`);
  console.log(`  Ext status size: ${foResp[cipOffset + 3]}`);

  const status = foResp[cipOffset + 2];
  const extSize = foResp[cipOffset + 3];

  if (extSize > 0) {
    const extStatus = foResp.slice(cipOffset + 4, cipOffset + 4 + extSize * 2);
    console.log(`  Extended status: ${toHex(extStatus)}`);
  }

  if (status === 0) {
    const dataOffset = cipOffset + 4 + extSize * 2;
    const targetOTCid = foResp.slice(dataOffset, dataOffset + 4);
    console.log(`  Success! Target CID: ${toHex(targetOTCid)}`);
  } else {
    console.log(`  FAILED!`);
  }

  conn.close();
}

main().catch(console.error);
