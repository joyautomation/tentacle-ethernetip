/**
 * Diagnostic script to explore PLC tags
 */

import {
  createCip,
  destroyCip,
  browseTags,
  readTag,
  decodeFloat32,
  decodeUint,
  decodeInt,
} from "../mod.ts";

async function main() {
  const plcIp = Deno.args[0];

  if (!plcIp) {
    console.log("Usage: deno run --allow-net examples/diagnose.ts <PLC_IP>");
    Deno.exit(1);
  }

  console.log(`Connecting to ${plcIp}...`);

  const cip = await createCip({ host: plcIp });

  console.log(`Connected to: ${cip.identity?.productName}`);
  console.log(`Serial: ${cip.identity?.serial}\n`);

  // Browse all tags
  const tags = await browseTags(cip);

  // Group by datatype
  const byType: Record<string, string[]> = {};
  for (const tag of tags) {
    const dt = tag.datatype;
    if (!byType[dt]) byType[dt] = [];
    byType[dt].push(tag.name);
  }

  console.log("\n=== Tags by Datatype ===");
  for (const [dt, names] of Object.entries(byType).sort()) {
    console.log(`\n${dt}: ${names.length} tags`);
    for (const name of names.slice(0, 5)) {
      console.log(`  - ${name}`);
    }
    if (names.length > 5) console.log(`  ... and ${names.length - 5} more`);
  }

  // Find atomic types we can read
  const atomicTypes = ["BOOL", "SINT", "INT", "DINT", "LINT", "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL"];
  const readableTags = tags.filter(
    (t) => atomicTypes.includes(t.datatype) && !t.isArray
  );

  console.log(`\n\n=== Readable Atomic Tags (${readableTags.length}) ===`);
  for (const tag of readableTags.slice(0, 20)) {
    console.log(`  ${tag.name} (${tag.datatype})`);
  }

  // Try to read first few atomic tags
  if (readableTags.length > 0) {
    console.log("\n\n=== Reading Sample Tags ===");
    for (const tag of readableTags.slice(0, 5)) {
      try {
        const response = await readTag(cip, tag.name);
        if (response.success) {
          const data = response.data.subarray(2); // Skip type code
          let value: unknown;

          if (tag.datatype === "REAL") {
            value = decodeFloat32(data);
          } else if (tag.datatype === "BOOL") {
            value = data[0] !== 0;
          } else if (["SINT", "INT", "DINT"].includes(tag.datatype)) {
            value = decodeInt(data);
          } else {
            value = decodeUint(data);
          }

          console.log(`  ${tag.name} = ${value}`);
        } else {
          console.log(`  ${tag.name} = READ FAILED (0x${response.status.toString(16)})`);
        }
      } catch (e) {
        console.log(`  ${tag.name} = ERROR: ${e.message}`);
      }
    }
  }

  // If no atomic tags, try reading a UDT member
  if (readableTags.length === 0) {
    console.log("\n\nNo atomic tags found. Try reading a struct member directly.");
    console.log("Example tag names from your structs:");

    // Show some struct names - we might be able to access members
    const structs = tags.filter(t => t.datatype.startsWith("UNKNOWN") || t.datatype === "STRUCT");
    for (const s of structs.slice(0, 5)) {
      console.log(`  ${s.name}.* (type: ${s.datatype})`);
    }
  }

  await destroyCip(cip);
  console.log("\nDone!");
}

main();
