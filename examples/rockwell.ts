/**
 * Example: Connect to a Rockwell CompactLogix/ControlLogix PLC
 *
 * This example shows how to:
 * 1. Create a CIP connection to a Rockwell PLC
 * 2. Browse available tags
 * 3. Read and write tag values
 *
 * Usage:
 *   deno run --allow-net examples/rockwell.ts <PLC_IP>
 */

import {
  createCip,
  destroyCip,
  readTag,
  writeTag,
  browseTags,
  CIP_DATA_TYPES,
  decodeFloat32,
  decodeUint,
  encodeFloat32,
} from "../mod.ts";

async function main() {
  const plcIp = Deno.args[0];

  if (!plcIp) {
    console.log("Usage: deno run --allow-net examples/rockwell.ts <PLC_IP>");
    console.log("Example: deno run --allow-net examples/rockwell.ts 192.168.1.100");
    Deno.exit(1);
  }

  console.log(`Connecting to Rockwell PLC at ${plcIp}...`);

  try {
    // Create CIP connection
    const cip = await createCip({ host: plcIp });

    console.log(`Connected!`);
    console.log(`Device: ${cip.identity?.productName}`);
    console.log(`Vendor: ${cip.identity?.vendor}`);
    console.log(`Serial: ${cip.identity?.serial}`);

    // Browse tags
    console.log("\nBrowsing tags...");
    const tags = await browseTags(cip);

    console.log(`Found ${tags.length} tags:`);
    for (const tag of tags.slice(0, 10)) {
      console.log(`  - ${tag.name} (${tag.datatype})${tag.dimensions.length > 0 ? ` [${tag.dimensions.join(",")}]` : ""}`);
    }
    if (tags.length > 10) {
      console.log(`  ... and ${tags.length - 10} more`);
    }

    // Read a tag (if any exist)
    // Only select known atomic datatypes (not UDTs or unknown types)
    const atomicTypes = ["BOOL", "SINT", "INT", "DINT", "LINT", "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL"];
    if (tags.length > 0) {
      const firstTag = tags.find((t) => !t.isStruct && !t.isArray && atomicTypes.includes(t.datatype));
      if (firstTag) {
        console.log(`\nReading tag: ${firstTag.name} (${firstTag.datatype})`);
        const response = await readTag(cip, firstTag.name);

        if (response.success) {
          const data = response.data;
          // Skip first 2 bytes (type code)
          const valueData = data.subarray(2);

          let value: unknown;
          if (firstTag.datatype === "REAL") {
            value = decodeFloat32(valueData);
          } else if (firstTag.datatype === "DINT" || firstTag.datatype === "INT") {
            value = decodeUint(valueData);
          } else if (firstTag.datatype === "BOOL") {
            value = valueData[0] !== 0;
          } else {
            value = Array.from(valueData);
          }

          console.log(`Value: ${value}`);
        } else {
          console.log(`Read failed: status 0x${response.status.toString(16)}`);
        }
      }
    }

    // Cleanup
    console.log("\nDisconnecting...");
    await destroyCip(cip);
    console.log("Done!");
  } catch (error) {
    console.error("Error:", error);
    Deno.exit(1);
  }
}

main();
