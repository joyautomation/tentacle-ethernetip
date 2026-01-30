/**
 * Integration test for batch CIP reads using Multiple Service Request
 * Tests against client4:44818 with RTU45_RECLM_PIT_001 tags
 */

import { createCip, destroyCip, readTag, readMultipleTags, decodeUint, decodeFloat32 } from "../../src/ethernetip/mod.ts";

const HOST = "client4";
const PORT = 44818;

// Tags from RTU45_RECLM_PIT_001 AOI
const TEST_TAGS = [
  "RTU45_RECLM_PIT_001.VALUE",
  "RTU45_RECLM_PIT_001.UNDERRANGE",
  "RTU45_RECLM_PIT_001.SIMVALUE",
  "RTU45_RECLM_PIT_001.SIMULATE",
  "RTU45_RECLM_PIT_001.SIGNALFAIL",
  "RTU45_RECLM_PIT_001.RAWMIN",
  "RTU45_RECLM_PIT_001.RAWMAX",
  "RTU45_RECLM_PIT_001.OVERRANGE",
  "RTU45_RECLM_PIT_001.OOR",
  "RTU45_RECLM_PIT_001.LSP",
  "RTU45_RECLM_PIT_001.LOWSCL",
  "RTU45_RECLM_PIT_001.LOS",
  "RTU45_RECLM_PIT_001.LOLENB",
  "RTU45_RECLM_PIT_001.LLENB",
  "RTU45_RECLM_PIT_001.LENB",
  "RTU45_RECLM_PIT_001.LL",
  "RTU45_RECLM_PIT_001.LLSP",
  "RTU45_RECLM_PIT_001.LOLSP",
  "RTU45_RECLM_PIT_001.L",
  "RTU45_RECLM_PIT_001.HSP",
  "RTU45_RECLM_PIT_001.H",
  "RTU45_RECLM_PIT_001.HIGHSCL",
  "RTU45_RECLM_PIT_001.HHSP",
  "RTU45_RECLM_PIT_001.HHENB",
  "RTU45_RECLM_PIT_001.HH",
  "RTU45_RECLM_PIT_001.HENB",
  "RTU45_RECLM_PIT_001.EnableOut",
  "RTU45_RECLM_PIT_001.EnableIn",
  "RTU45_RECLM_PIT_001.DB",
  "RTU45_RECLM_PIT_001.ALMENB",
  "RTU45_RECLM_PIT_001.ALMDLY",
  "RTU45_RECLM_PIT_001.AI",
];

function decodeValue(data: Uint8Array): { typeCode: number; value: number | boolean } {
  if (data.length < 2) {
    return { typeCode: 0, value: 0 };
  }

  const typeCode = decodeUint(data.subarray(0, 2));
  const valueData = data.subarray(2);

  // Common Rockwell types
  switch (typeCode) {
    case 0x00c1: // BOOL
      return { typeCode, value: valueData[0] !== 0 };
    case 0x00c2: // SINT
      return { typeCode, value: new Int8Array(valueData.buffer, valueData.byteOffset, 1)[0] };
    case 0x00c3: // INT
      return { typeCode, value: new Int16Array(new Uint8Array(valueData).buffer)[0] };
    case 0x00c4: // DINT
      return { typeCode, value: new Int32Array(new Uint8Array(valueData).buffer)[0] };
    case 0x00ca: // REAL
      return { typeCode, value: decodeFloat32(valueData) };
    default:
      // Try as REAL if 4 bytes
      if (valueData.length >= 4) {
        return { typeCode, value: decodeFloat32(valueData) };
      }
      return { typeCode, value: decodeUint(valueData) };
  }
}

async function testSequentialReads(cip: ReturnType<typeof createCip> extends Promise<infer T> ? T : never) {
  console.log("\n=== Sequential Read Test (one at a time) ===");
  const startTime = performance.now();

  let successCount = 0;
  let failCount = 0;

  for (const tag of TEST_TAGS) {
    try {
      const response = await readTag(cip, tag);
      if (response.success) {
        successCount++;
      } else {
        failCount++;
        console.log(`  FAIL: ${tag} - status 0x${response.status.toString(16)}`);
      }
    } catch (err) {
      failCount++;
      console.log(`  ERROR: ${tag} - ${err}`);
    }
  }

  const elapsed = performance.now() - startTime;
  console.log(`Sequential: ${TEST_TAGS.length} tags in ${elapsed.toFixed(1)}ms`);
  console.log(`  Success: ${successCount}, Failed: ${failCount}`);
  console.log(`  Average: ${(elapsed / TEST_TAGS.length).toFixed(2)}ms per tag`);

  return elapsed;
}

async function testBatchReads(cip: ReturnType<typeof createCip> extends Promise<infer T> ? T : never) {
  console.log("\n=== Batch Read Test (Multiple Service Request) ===");
  console.log(`Testing with ${TEST_TAGS.length} tags`);

  const startTime = performance.now();

  try {
    const results = await readMultipleTags(cip, TEST_TAGS);
    const elapsed = performance.now() - startTime;

    let successCount = 0;
    let failCount = 0;

    console.log("\nResults:");
    for (const result of results) {
      if (result.response?.success) {
        successCount++;
        const decoded = decodeValue(result.response.data);
        const tagShort = result.tagName.split(".").pop();
        console.log(`  ${tagShort}: ${decoded.value}`);
      } else {
        failCount++;
        const status = result.response?.status;
        console.log(`  FAIL: ${result.tagName} - status ${status !== undefined ? '0x' + status.toString(16) : 'undefined'}`);
      }
    }

    console.log(`\nBatch: ${TEST_TAGS.length} tags in ${elapsed.toFixed(1)}ms`);
    console.log(`  Success: ${successCount}, Failed: ${failCount}`);

    return elapsed;
  } catch (err) {
    console.log(`Batch read error: ${err}`);
    return -1;
  }
}

async function main() {
  console.log(`Connecting to ${HOST}:${PORT}...`);

  const cip = await createCip({ host: HOST, port: PORT });
  console.log(`Connected! Device: ${cip.identity?.productName}`);

  try {
    // Test batch reads first
    const batchTime = await testBatchReads(cip);

    // Test sequential reads for comparison
    const seqTime = await testSequentialReads(cip);

    if (batchTime > 0 && seqTime > 0) {
      console.log(`\n=== Performance Comparison ===`);
      console.log(`Sequential: ${seqTime.toFixed(1)}ms`);
      console.log(`Batch:      ${batchTime.toFixed(1)}ms`);
      console.log(`Speedup:    ${(seqTime / batchTime).toFixed(1)}x faster`);
    }
  } finally {
    await destroyCip(cip);
    console.log("\nDisconnected.");
  }
}

main().catch(console.error);
