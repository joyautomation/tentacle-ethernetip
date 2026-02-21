/**
 * Trigger a browse request via NATS
 */
import { connect } from "@nats-io/transport-deno";

async function main() {
  const nc = await connect({ servers: "localhost:4222" });

  console.log("Triggering browse for PLC client4...");

  // Send browse request
  const response = await nc.request(
    "ethernetip.browse",
    new TextEncoder().encode(JSON.stringify({
      plcId: "client4",
      browseId: "test-browse-1",
    })),
    { timeout: 30000 }
  );

  const result = JSON.parse(new TextDecoder().decode(response.data));
  console.log(`Browse returned ${result.variables?.length || 0} variables`);

  await nc.close();
}

main().catch(console.error);
