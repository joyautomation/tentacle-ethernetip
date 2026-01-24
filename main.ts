/**
 * tentacle-ethernetip Service
 *
 * Standalone service that polls Allen-Bradley/Rockwell PLCs via EtherNet/IP
 * and publishes tag values to NATS.
 *
 * Configuration is stored in NATS KV and can be changed at runtime.
 *
 * Environment variables:
 *   NATS_SERVERS - NATS server URL(s), comma-separated (default: localhost:4222)
 *   PROJECT_ID   - Project identifier for NATS topics (required)
 *
 * Run with:
 *   deno run --allow-net --allow-env main.ts
 *
 * To configure PLCs, use nats CLI:
 *   nats kv put field-config-{projectId} plc.myplc '{"host":"192.168.1.100","port":44818,"scanRate":1000,"enabled":true}'
 *   nats kv put field-config-{projectId} plc.myplc.tags '["Tag1","Tag2.Member"]'
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { createLogger, LogLevel } from "@joyautomation/coral";
import { createConfigManager } from "./src/service/config.ts";
import { createScanner } from "./src/service/scanner.ts";

const log = createLogger("ethernetip", LogLevel.info);

// ═══════════════════════════════════════════════════════════════════════════
// Configuration from environment
// ═══════════════════════════════════════════════════════════════════════════

function loadEnvConfig(): { natsServers: string; projectId: string } {
  const natsServers = Deno.env.get("NATS_SERVERS") || "localhost:4222";
  const projectId = Deno.env.get("PROJECT_ID");

  if (!projectId) {
    log.error("PROJECT_ID environment variable is required");
    Deno.exit(1);
  }

  return { natsServers, projectId };
}

// ═══════════════════════════════════════════════════════════════════════════
// NATS Connection with retry
// ═══════════════════════════════════════════════════════════════════════════

async function connectToNats(servers: string): Promise<NatsConnection> {
  const serverList = servers.split(",").map((s) => s.trim());

  while (true) {
    try {
      log.info(`Connecting to NATS at ${servers}...`);
      const nc = await connect({ servers: serverList });
      log.info("Connected to NATS");
      return nc;
    } catch (err) {
      log.warn(`Failed to connect to NATS: ${err}. Retrying in 5 seconds...`);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

async function main(): Promise<void> {
  log.info("═══════════════════════════════════════════════════════════════");
  log.info("            tentacle-ethernetip Service");
  log.info("═══════════════════════════════════════════════════════════════");

  // Load environment config
  const { natsServers, projectId } = loadEnvConfig();
  log.info(`Project ID: ${projectId}`);
  log.info(`NATS Servers: ${natsServers}`);

  // Connect to NATS (retries forever)
  const nc = await connectToNats(natsServers);

  // Handle NATS connection close - reconnect
  (async () => {
    const err = await nc.closed();
    if (err) {
      log.error(`NATS connection closed with error: ${err}`);
      log.info("Attempting to reconnect...");
      // The NATS client handles reconnection automatically
    }
  })();

  // Create config manager (loads from NATS KV)
  log.info("Loading configuration from NATS KV...");
  const configManager = await createConfigManager(nc, projectId);

  // Create scanner
  log.info("Initializing scanner...");
  const scanner = await createScanner(nc, projectId, configManager);

  // Start scanning
  scanner.start();

  log.info("");
  log.info("Service running. Press Ctrl+C to stop.");
  log.info("");
  log.info("To add a PLC, use nats CLI:");
  log.info(`  nats kv put field-config-${projectId} plc.myplc '{"host":"192.168.1.100","port":44818,"scanRate":1000,"enabled":true}'`);
  log.info(`  nats kv put field-config-${projectId} plc.myplc.tags '["Tag1","Tag2"]'`);
  log.info("");

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    log.info(`Received ${signal}, shutting down...`);

    // Stop scanner
    await scanner.stop();

    // Stop config manager
    configManager.stop();

    // Drain NATS connection
    await nc.drain();

    log.info("Shutdown complete");
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", () => shutdown("SIGINT"));
  Deno.addSignalListener("SIGTERM", () => shutdown("SIGTERM"));
}

// Run
if (import.meta.main) {
  main().catch((err) => {
    log.error(`Fatal error: ${err}`);
    Deno.exit(1);
  });
}
