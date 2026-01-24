/**
 * Test reading UDT/AOI members
 */
import { createCip, destroyCip, readTag, decodeFloat32, decodeUint } from "../mod.ts";

function toHex(data: Uint8Array): string {
  return Array.from(data).map((b) => b.toString(16).padStart(2, "0")).join(" ");
}

async function main() {
  const cip = await createCip({ host: "client4" });

  const testTags = [
    // Atomic tag (should work)
    "P2P_RTU46_RESRCL_LIT_00A_LI",

    // AOI (Pump_Maintenance) members
    "RTU45_RECLM_BP_00A_MTN.OILHRS",   // REAL
    "RTU45_RECLM_BP_00A_MTN.MTRHRS",   // REAL
    "RTU45_RECLM_BP_00A_MTN.PMPHRS",   // REAL
    "RTU45_RECLM_BP_00A_MTN.RUN_FB",   // BOOL

    // AOI (EQ_MTR_CS) members - correct names from L5X
    "RTU45_RECLM_BP_00A.RUNFB",        // BOOL
    "RTU45_RECLM_BP_00A.REMOTE",       // BOOL
    "RTU45_RECLM_BP_00A.AUTOCMD",      // BOOL
    "RTU45_RECLM_BP_00A.INTERLOCK",    // BOOL

    // Full AOI (will show raw structure data)
    "RTU45_RECLM_BP_00A",
  ];

  console.log("\n--- Testing tag reads ---\n");

  for (const tagName of testTags) {
    const response = await readTag(cip, tagName);

    if (response.success) {
      const typeCode = decodeUint(response.data.subarray(0, 2));
      const valueData = response.data.subarray(2);

      let value: string;
      // REAL type = 0xCA
      if (typeCode === 0x00ca) {
        value = decodeFloat32(valueData).toString();
      } else if (typeCode === 0x00c1) { // BOOL
        value = (valueData[0] !== 0).toString();
      } else if (typeCode === 0x00c3) { // DINT
        value = decodeUint(valueData).toString();
      } else if (typeCode === 0x00c2) { // SINT
        value = valueData[0].toString();
      } else if (typeCode === 0x00c7) { // INT
        value = decodeUint(valueData.subarray(0, 2)).toString();
      } else {
        value = `raw[${response.data.length}]: ${toHex(response.data.slice(0, 20))}...`;
      }

      console.log(`✓ ${tagName}`);
      console.log(`  Type: 0x${typeCode.toString(16)}, Value: ${value}`);
    } else {
      console.log(`✗ ${tagName}`);
      console.log(`  Status: 0x${response.status.toString(16)} (${getStatusName(response.status)})`);
    }
    console.log();
  }

  await destroyCip(cip);
}

function getStatusName(status: number): string {
  const names: Record<number, string> = {
    0x00: "Success",
    0x04: "Path segment error",
    0x05: "Path destination unknown",
    0x06: "Partial transfer",
    0x0a: "Routing failure",
    0x0f: "Permission denied",
    0x13: "Not enough data",
    0x14: "Attribute not supported",
    0x26: "Bridge request too large",
  };
  return names[status] || "Unknown";
}

main().catch(console.error);
