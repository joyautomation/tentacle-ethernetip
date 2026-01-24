/**
 * Full Integration Test
 *
 * Tests tentacle-ethernetip with NATS, and optionally tentacle-mqtt/graphql
 *
 * Run with: deno run --allow-net examples/integration_test.ts
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, StorageType, DiscardPolicy } from "@nats-io/jetstream";
import {
  createCip,
  destroyCip,
  readTag,
  browseTags,
  decodeFloat32,
  decodeUint,
} from "../mod.ts";

// Configuration
const PLC_HOST = "client4";
const NATS_URL = "nats://localhost:4222";
const PROJECT_ID = "ethernetip-test";

// NATS topic pattern from nats-schema: plc.data.{projectId}.{variableId}
const NATS_TOPIC = `plc.data.${PROJECT_ID}`;

// KV bucket for variables (used by tentacle-graphql)
const KV_BUCKET = `plc-variables-${PROJECT_ID}`;

// Helpers
function toHex(data: Uint8Array): string {
  return Array.from(data)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join(" ");
}

function decodeValue(
  data: Uint8Array,
  datatype: string,
): { typeCode: number; value: number | boolean | string } {
  const typeCode = decodeUint(data.subarray(0, 2));
  const valueData = data.subarray(2);

  if (datatype === "REAL" || typeCode === 0xca) {
    return { typeCode, value: decodeFloat32(valueData) };
  } else if (datatype === "BOOL" || typeCode === 0xc1) {
    return { typeCode, value: valueData[0] !== 0 };
  } else if (datatype === "INT" || typeCode === 0xc7) {
    return { typeCode, value: decodeUint(valueData.subarray(0, 2)) };
  } else if (datatype === "DINT" || typeCode === 0xc4) {
    return { typeCode, value: decodeUint(valueData.subarray(0, 4)) };
  } else {
    return { typeCode, value: `raw: ${toHex(valueData.slice(0, 8))}` };
  }
}

async function main() {
  console.log("═══════════════════════════════════════════════════════════════");
  console.log("           TENTACLE-ETHERNETIP INTEGRATION TEST");
  console.log("═══════════════════════════════════════════════════════════════\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 1. Connect to NATS and setup KV bucket
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ NATS Connection ─────────────────────────────────────────────┐");
  let nc: NatsConnection | null = null;
  let kvPut: ((key: string, value: Uint8Array) => Promise<void>) | null = null;

  try {
    nc = await connect({ servers: NATS_URL });
    console.log(`│ ✓ Connected to NATS at ${NATS_URL}`);
    console.log(`│ ✓ Topic pattern: ${NATS_TOPIC}.<variableId>`);

    // Setup KV bucket for tentacle-graphql
    const js = jetstream(nc);
    const jsm = await js.jetstreamManager();
    const streamName = `$KV_${KV_BUCKET}`;
    const subjectPrefix = `$KV.${KV_BUCKET}`;

    try {
      await jsm.streams.info(streamName);
      console.log(`│ ✓ Using existing KV bucket: ${KV_BUCKET}`);
    } catch {
      await jsm.streams.add({
        name: streamName,
        subjects: [`${subjectPrefix}.>`],
        storage: StorageType.File,
        discard: DiscardPolicy.New,
        max_age: 0,
      });
      console.log(`│ ✓ Created KV bucket: ${KV_BUCKET}`);
    }

    kvPut = async (key: string, value: Uint8Array) => {
      nc!.publish(`${subjectPrefix}.${key}`, value);
    };
  } catch (error) {
    console.log(`│ ✗ Failed to connect to NATS: ${error}`);
    console.log(`│   (Continuing without NATS publishing)`);
  }
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 2. Connect to Allen Bradley PLC
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ PLC Connection ──────────────────────────────────────────────┐");
  const cip = await createCip({ host: PLC_HOST });
  console.log(`│ ✓ Connected to PLC at ${PLC_HOST}:44818`);
  console.log(`│   Device: ${cip.identity?.productName}`);
  console.log(`│   Vendor: ${cip.identity?.vendor}`);
  console.log(`│   Serial: ${cip.identity?.serial}`);
  console.log(`│   Firmware: ${cip.identity?.revision.major}.${cip.identity?.revision.minor}`);
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 3. Browse Tags
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ Tag Discovery ───────────────────────────────────────────────┐");
  const tags = await browseTags(cip);

  // Categorize tags
  const atomicTypes = [
    "BOOL", "SINT", "INT", "DINT", "LINT",
    "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL",
  ];
  const atomicTags = tags.filter((t) => atomicTypes.includes(t.datatype) && !t.isArray);
  const udtTags = tags.filter((t) => t.datatype.startsWith("UNKNOWN"));
  const arrayTags = tags.filter((t) => t.isArray);

  console.log(`│ ✓ Found ${tags.length} total tags`);
  console.log(`│   • Atomic tags: ${atomicTags.length}`);
  console.log(`│   • UDT/AOI tags: ${udtTags.length}`);
  console.log(`│   • Array tags: ${arrayTags.length}`);
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 4. Read Atomic Tags and Publish to NATS
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ Reading Atomic Tags ─────────────────────────────────────────┐");

  const readResults: Array<{
    name: string;
    datatype: string;
    value: number | boolean | string;
    success: boolean;
  }> = [];

  // Read up to 10 atomic tags
  for (const tag of atomicTags.slice(0, 10)) {
    const response = await readTag(cip, tag.name);
    if (response.success) {
      const { value } = decodeValue(response.data, tag.datatype);
      readResults.push({
        name: tag.name,
        datatype: tag.datatype,
        value,
        success: true,
      });
      console.log(`│ ✓ ${tag.name} (${tag.datatype}) = ${value}`);

      // Publish to NATS if connected
      if (nc && kvPut) {
        const message = {
          projectId: PROJECT_ID,
          variableId: tag.name,
          value,
          lastUpdated: Date.now(),
          datatype: tag.datatype.toLowerCase() === "bool" ? "boolean" : "number",
          source: "plc",
          quality: "good",
        };
        // Publish to topic (for real-time subscribers)
        const subject = `${NATS_TOPIC}.${tag.name}`;
        nc.publish(subject, JSON.stringify(message));
        // Write to KV (for GraphQL queries)
        await kvPut(tag.name, new TextEncoder().encode(JSON.stringify(message)));
      }
    } else {
      readResults.push({
        name: tag.name,
        datatype: tag.datatype,
        value: `Error: 0x${response.status.toString(16)}`,
        success: false,
      });
      console.log(`│ ✗ ${tag.name} - Error: 0x${response.status.toString(16)}`);
    }
  }
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 5. Read UDT/AOI Members
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ Reading UDT/AOI Members ─────────────────────────────────────┐");

  const udtMemberTests = [
    { name: "RTU45_RECLM_BP_00A_MTN.OILHRS", datatype: "REAL" },
    { name: "RTU45_RECLM_BP_00A_MTN.MTRHRS", datatype: "REAL" },
    { name: "RTU45_RECLM_BP_00A_MTN.PMPHRS", datatype: "REAL" },
    { name: "RTU45_RECLM_BP_00A.RUNFB", datatype: "BOOL" },
    { name: "RTU45_RECLM_BP_00A.AUTOCMD", datatype: "BOOL" },
  ];

  for (const tag of udtMemberTests) {
    const response = await readTag(cip, tag.name);
    if (response.success) {
      const { value } = decodeValue(response.data, tag.datatype);
      console.log(`│ ✓ ${tag.name} = ${value}`);

      // Publish to NATS
      if (nc && kvPut) {
        const varId = tag.name.replace(".", "_");
        const message = {
          projectId: PROJECT_ID,
          variableId: varId,
          value,
          lastUpdated: Date.now(),
          datatype: tag.datatype.toLowerCase() === "real" ? "number" : "boolean",
          source: "plc",
          quality: "good",
        };
        const subject = `${NATS_TOPIC}.${varId}`;
        nc.publish(subject, JSON.stringify(message));
        await kvPut(varId, new TextEncoder().encode(JSON.stringify(message)));
      }
    } else {
      console.log(`│ ✗ ${tag.name} - Error: 0x${response.status.toString(16)}`);
    }
  }
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // 6. Summary
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("┌─ Summary ─────────────────────────────────────────────────────┐");
  const successCount = readResults.filter((r) => r.success).length;
  const failCount = readResults.filter((r) => !r.success).length;
  console.log(`│ Atomic tags read: ${successCount} success, ${failCount} failed`);
  console.log(`│ UDT members read: ${udtMemberTests.length}`);
  if (nc) {
    console.log(`│ NATS: Published ${successCount + udtMemberTests.length} messages`);
    console.log(`│ Topic: ${NATS_TOPIC}.<variableId>`);
  }
  console.log("└───────────────────────────────────────────────────────────────┘\n");

  // ═══════════════════════════════════════════════════════════════════════════
  // Cleanup
  // ═══════════════════════════════════════════════════════════════════════════
  console.log("Cleaning up...");
  await destroyCip(cip);
  if (nc) {
    await nc.drain();
  }

  console.log("\n✓ Integration test complete!");
}

main().catch(console.error);
