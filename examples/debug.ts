/**
 * Debug script to trace EtherNet/IP communication
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

  // Helper to send and receive
  async function sendReceive(name: string, request: Uint8Array): Promise<Uint8Array> {
    console.log(`\n--- ${name} ---`);
    console.log(`TX (${request.length} bytes): ${toHex(request.slice(0, 60))}${request.length > 60 ? "..." : ""}`);

    await conn.write(request);

    const buffer = new Uint8Array(4096);
    const n = await conn.read(buffer);
    const response = buffer.slice(0, n!);

    console.log(`RX (${response.length} bytes): ${toHex(response.slice(0, 60))}${response.length > 60 ? "..." : ""}`);
    return response;
  }

  // 1. RegisterSession
  const registerSession = joinBytes([
    new Uint8Array([0x65, 0x00]),       // Command: RegisterSession
    encodeUint(4, 2),                     // Length: 4
    encodeUint(0, 4),                     // Session: 0
    new Uint8Array([0, 0, 0, 0]),         // Status
    new TextEncoder().encode("_pycomm_"), // Context
    encodeUint(0, 4),                     // Options
    encodeUint(1, 2),                     // Protocol version
    new Uint8Array([0, 0]),               // Option flags
  ]);

  const regResp = await sendReceive("RegisterSession", registerSession);
  const session = regResp.slice(4, 8);
  console.log(`Session handle: ${decodeUint(session)}`);

  // 2. ListIdentity (optional, just for info)
  const listIdentity = joinBytes([
    new Uint8Array([0x63, 0x00]),       // Command: ListIdentity
    encodeUint(0, 2),                   // Length: 0
    session,                            // Session
    new Uint8Array([0, 0, 0, 0]),       // Status
    new TextEncoder().encode("_pycomm_"), // Context
    encodeUint(0, 4),                   // Options
  ]);

  await sendReceive("ListIdentity", listIdentity);

  // 3. ForwardOpen (Large)
  // Build the CIP message
  const cid = crypto.getRandomValues(new Uint8Array(4));
  const csn = new Uint8Array([0x27, 0x04]);
  const vid = new Uint8Array([0x09, 0x10]);
  const vsn = crypto.getRandomValues(new Uint8Array(4));

  // Message router path: 0x20 0x02 0x24 0x01 (class 0x02, instance 0x01)
  const routePath = new Uint8Array([0x20, 0x02, 0x24, 0x01]);
  const routePathWithLen = joinBytes([encodeUint(2, 1), routePath]); // Length in words

  // Large ForwardOpen parameters
  const connectionSize = 4000;
  const netParams = encodeUint((connectionSize & 0xffff) | (0x4200 << 16), 4);

  const forwardOpenCip = joinBytes([
    new Uint8Array([0x54]),               // Service: Large ForwardOpen
    routePathWithLen,                      // Route path
    new Uint8Array([0x0a]),               // Priority/Time_tick
    new Uint8Array([0x05]),               // Timeout_ticks
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // O->T CID (filled by target)
    cid,                                   // T->O CID (our CID)
    csn,                                   // Connection serial number
    vid,                                   // Vendor ID
    vsn,                                   // Vendor serial
    new Uint8Array([0x07]),               // Connection timeout multiplier
    new Uint8Array([0x00, 0x00, 0x00]),   // Reserved
    new Uint8Array([0x01, 0x40, 0x20, 0x00]), // O->T RPI
    netParams,                             // O->T Network params
    new Uint8Array([0x01, 0x40, 0x20, 0x00]), // T->O RPI
    netParams,                             // T->O Network params
    new Uint8Array([0xa3]),               // Transport type/trigger
  ]);

  // Wrap in SendRRData
  const cpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // Interface handle
    new Uint8Array([0x0a, 0x00]),              // Timeout
    new Uint8Array([0x02, 0x00]),              // Item count: 2
    new Uint8Array([0x00, 0x00]),              // Address type: Null
    new Uint8Array([0x00, 0x00]),              // Address length: 0
    new Uint8Array([0xb2, 0x00]),              // Data type: Unconnected
    encodeUint(forwardOpenCip.length, 2),      // Data length
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

  const foResp = await sendReceive("ForwardOpen", forwardOpen);

  // Parse ForwardOpen response
  // Header: 24 bytes
  // CPF: Interface handle (4) + timeout (2) + item count (2) + addr type (2) + addr len (2) + data type (2) + data len (2)
  // That's 24 + 16 = 40 bytes before CIP data
  // CIP reply: service (1) + reserved (1) + status (1) + ext status size (1)
  // Then: O->T CID (4) + T->O CID (4) + CSN (2) + VID (2) + VSN (4) + O->T API (4) + T->O API (4)

  const cipOffset = 40;
  console.log(`\nParsing ForwardOpen response at offset ${cipOffset}:`);
  console.log(`  Service: 0x${foResp[cipOffset].toString(16)}`);
  console.log(`  Reserved: 0x${foResp[cipOffset + 1].toString(16)}`);
  console.log(`  Status: 0x${foResp[cipOffset + 2].toString(16)}`);
  console.log(`  Ext status size: ${foResp[cipOffset + 3]}`);

  const dataOffset = cipOffset + 4 + (foResp[cipOffset + 3] * 2);
  console.log(`  Data starts at offset: ${dataOffset}`);

  const targetOTCid = foResp.slice(dataOffset, dataOffset + 4);
  const targetTOCid = foResp.slice(dataOffset + 4, dataOffset + 8);
  console.log(`  O->T CID (target's): ${toHex(targetOTCid)} = ${decodeUint(targetOTCid)}`);
  console.log(`  T->O CID (ours): ${toHex(targetTOCid)} = ${decodeUint(targetTOCid)}`);

  // For connected messaging, we use the O->T CID returned by the target
  const connectedCid = targetOTCid;

  // 4. Try a ReadTag with connected messaging
  const tagName = "P2P_RTU46_RESRCL_LIT_00A_LI";
  const tagBytes = new TextEncoder().encode(tagName);
  const tagPath = joinBytes([
    new Uint8Array([0x91, tagBytes.length]),  // Extended symbolic segment
    tagBytes,
    tagBytes.length % 2 === 1 ? new Uint8Array([0x00]) : new Uint8Array(), // Pad
  ]);

  const sequence = encodeUint(1, 2);
  const readTagCip = joinBytes([
    sequence,                                  // Sequence count
    new Uint8Array([0x4c]),                   // Service: ReadTag
    encodeUint(Math.floor(tagPath.length / 2), 1), // Path size in words
    tagPath,                                   // Tag path
    encodeUint(1, 2),                         // Element count
  ]);

  // Wrap in SendUnitData (connected messaging)
  const readCpf = joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // Interface handle
    new Uint8Array([0x0a, 0x00]),              // Timeout
    new Uint8Array([0x02, 0x00]),              // Item count: 2
    new Uint8Array([0xa1, 0x00]),              // Address type: Connected
    encodeUint(4, 2),                          // Address length
    connectedCid,                              // Connection ID
    new Uint8Array([0xb1, 0x00]),              // Data type: Connected
    encodeUint(readTagCip.length, 2),          // Data length
    readTagCip,
  ]);

  const readTag = joinBytes([
    new Uint8Array([0x70, 0x00]),        // Command: SendUnitData
    encodeUint(readCpf.length, 2),       // Length
    session,                             // Session
    new Uint8Array([0, 0, 0, 0]),        // Status
    new TextEncoder().encode("_pycomm_"), // Context
    encodeUint(0, 4),                    // Options
    readCpf,
  ]);

  console.log(`\nReading tag: ${tagName}`);

  try {
    const readResp = await sendReceive("ReadTag", readTag);

    // Parse response
    const readCipOffset = 46; // Header (24) + CPF prefix (22)
    console.log(`\nParsing ReadTag response at offset ${readCipOffset}:`);
    console.log(`  Sequence: ${decodeUint(readResp.slice(readCipOffset, readCipOffset + 2))}`);
    console.log(`  Service: 0x${readResp[readCipOffset + 2].toString(16)}`);
    console.log(`  Reserved: 0x${readResp[readCipOffset + 3].toString(16)}`);
    console.log(`  Status: 0x${readResp[readCipOffset + 4].toString(16)}`);

    if (readResp[readCipOffset + 4] === 0) {
      const valueOffset = readCipOffset + 6;
      const typeCode = decodeUint(readResp.slice(valueOffset, valueOffset + 2));
      const value = readResp.slice(valueOffset + 2, valueOffset + 6);
      console.log(`  Type code: 0x${typeCode.toString(16)}`);
      console.log(`  Value bytes: ${toHex(value)}`);

      // Decode REAL
      const view = new DataView(value.buffer, value.byteOffset, 4);
      console.log(`  Value (REAL): ${view.getFloat32(0, true)}`);
    }
  } catch (e) {
    console.log(`ReadTag error: ${e.message}`);
  }

  // Cleanup
  conn.close();
  console.log("\nConnection closed");
}

main().catch(console.error);
