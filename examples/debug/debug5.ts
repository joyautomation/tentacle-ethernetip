/**
 * Debug script - with fixed RPI (10ms)
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

const FO_ERROR_CODES: Record<number, string> = {
  0x0100: "Connection in use or duplicate Forward Open",
  0x0103: "Transport class and trigger combination not supported",
  0x0106: "Ownership conflict",
  0x0107: "Target connection not found",
  0x0108: "Invalid network connection parameter",
  0x0109: "Invalid connection size",
  0x0110: "Connection closed by application",
  0x0111: "RPI not supported",
  0x0113: "Out of connections",
  0x0114: "Vendor ID or product code mismatch",
  0x0115: "Device type mismatch",
  0x0116: "Revision mismatch",
  0x0117: "Invalid produced or consumed application path",
  0x0118: "Invalid or inconsistent configuration application path",
  0x0119: "Non-listen only connection not opened",
  0x011a: "Target object out of connections",
  0x011b: "RPI is smaller than the production inhibit time",
  0x0203: "Connection timed out",
  0x0204: "Unconnected request timed out",
  0x0205: "Parameter error in unconnected request",
  0x0206: "Message too large for unconnected_send service",
  0x0301: "No buffer memory available",
  0x0302: "Network bandwidth not available",
  0x0311: "Port not available",
  0x0312: "Link address not valid",
  0x0315: "Invalid segment in connection path",
  0x0316: "Forward Close service failed",
};

async function main() {
  console.log(`\n=== Connecting to ${plcIp}:${port} ===\n`);

  const conn = await Deno.connect({ hostname: plcIp, port });
  console.log("TCP connected");

  async function sendReceive(name: string, request: Uint8Array): Promise<Uint8Array> {
    console.log(`\n--- ${name} ---`);
    console.log(`TX (${request.length} bytes):`);
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
    new Uint8Array([0x65, 0x00]),
    encodeUint(4, 2),
    encodeUint(0, 4),
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    encodeUint(1, 2),
    new Uint8Array([0, 0]),
  ]);

  const regResp = await sendReceive("RegisterSession", registerSession);
  const session = regResp.slice(4, 8);
  console.log(`\nSession handle: ${decodeUint(session)}`);

  // 2. Large ForwardOpen with fixed RPI (10ms = 10000 µs)
  const cid = crypto.getRandomValues(new Uint8Array(4));
  const csn = new Uint8Array([0x27, 0x04]);
  const vid = new Uint8Array([0x09, 0x10]);
  const vsn = crypto.getRandomValues(new Uint8Array(4));

  // Large ForwardOpen (4 byte net params)
  const connectionSize = 4000;
  const initNetParam = 0b0100001000000000;
  const netParams = encodeUint((connectionSize & 0xFFFF) | (initNetParam << 16), 4);

  // RPI: 10ms = 10000 µs = 0x00002710 (little endian: 10 27 00 00)
  const rpi = new Uint8Array([0x10, 0x27, 0x00, 0x00]);

  // Request path to Connection Manager
  const requestPath = new Uint8Array([0x02, 0x20, 0x06, 0x24, 0x01]);

  // Connection path to Message Router
  const connectionPath = new Uint8Array([0x02, 0x20, 0x02, 0x24, 0x01]);

  const forwardOpenParams = joinBytes([
    new Uint8Array([0x0a]),               // Priority/Time_tick
    new Uint8Array([0x05]),               // Timeout_ticks
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // O->T CID
    cid,                                   // T->O CID
    csn,                                   // CSN
    vid,                                   // VID
    vsn,                                   // VSN
    new Uint8Array([0x07]),               // Timeout multiplier
    new Uint8Array([0x00, 0x00, 0x00]),   // Reserved
    rpi,                                   // O->T RPI (10ms)
    netParams,                             // O->T Network params
    rpi,                                   // T->O RPI (10ms)
    netParams,                             // T->O Network params
    new Uint8Array([0xa3]),               // Transport type/trigger
  ]);

  const cipMessage = joinBytes([
    new Uint8Array([0x54]),  // Large ForwardOpen service
    requestPath,
    forwardOpenParams,
    connectionPath,
  ]);

  console.log(`\nRPI value: ${toHex(rpi)} = ${decodeUint(rpi)} µs = ${decodeUint(rpi)/1000} ms`);
  console.log(`Net params: ${toHex(netParams)}`);

  const cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]),
    new Uint8Array([0x0a, 0x00]),
    new Uint8Array([0x02, 0x00]),
    new Uint8Array([0x00, 0x00]),
    new Uint8Array([0x00, 0x00]),
    new Uint8Array([0xb2, 0x00]),
    encodeUint(cipMessage.length, 2),
    cipMessage,
  ]);

  const forwardOpen = joinBytes([
    new Uint8Array([0x6f, 0x00]),
    encodeUint(cpf.length, 2),
    session,
    new Uint8Array([0, 0, 0, 0]),
    new TextEncoder().encode("_pycomm_"),
    encodeUint(0, 4),
    cpf,
  ]);

  const foResp = await sendReceive("Large ForwardOpen (0x54)", forwardOpen);

  const cipOffset = 40;
  console.log(`\nParsing response:`);
  const serviceReply = foResp[cipOffset];
  const status = foResp[cipOffset + 2];
  const extSize = foResp[cipOffset + 3];

  console.log(`  Service: 0x${serviceReply.toString(16)}`);
  console.log(`  Status: 0x${status.toString(16)}`);
  console.log(`  Ext status size: ${extSize}`);

  if (extSize > 0) {
    const extCode = decodeUint(foResp.slice(cipOffset + 4, cipOffset + 6));
    console.log(`  Extended status: 0x${extCode.toString(16)}`);
    console.log(`  Meaning: ${FO_ERROR_CODES[extCode] || "Unknown"}`);
  }

  if (status === 0) {
    const dataOffset = cipOffset + 4 + extSize * 2;
    const targetCid = foResp.slice(dataOffset, dataOffset + 4);
    console.log(`  SUCCESS! Target CID: ${toHex(targetCid)}`);
  }

  conn.close();
}

main().catch(console.error);
