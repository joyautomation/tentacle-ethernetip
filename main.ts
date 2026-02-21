/**
 * tentacle-ethernetip Service
 *
 * Standalone service that polls Allen-Bradley/Rockwell PLCs via EtherNet/IP
 * and publishes tag values to NATS.
 *
 * Stateless: starts with zero PLC connections. Connections are created
 * on-demand when clients send subscribe or browse requests with connection info.
 *
 * Environment variables:
 *   NATS_SERVERS - NATS server URL(s), comma-separated (default: localhost:4222)
 *
 * Run with:
 *   deno run --allow-net --allow-env main.ts
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";
import { createScanner } from "./src/service/scanner.ts";
import { enableNatsLogging } from "./src/utils/logger.ts";
import type { ServiceHeartbeat } from "@tentacle/nats-schema";

const log = createLogger("ethernetip", LogLevel.info);

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

  const natsServers = Deno.env.get("NATS_SERVERS") || "localhost:4222";
  log.info(`Module ID: ethernetip`);
  log.info(`NATS Servers: ${natsServers}`);

  // Connect to NATS (retries forever)
  const nc = await connectToNats(natsServers);

  // Enable NATS log streaming for all loggers
  enableNatsLogging(nc, "ethernetip", "ethernetip");

  // Handle NATS connection close - reconnect
  (async () => {
    const err = await nc.closed();
    if (err) {
      log.error(`NATS connection closed with error: ${err}`);
      log.info("Attempting to reconnect...");
    }
  })();

  // Create and start scanner (stateless — zero connections on startup)
  log.info("Initializing scanner...");
  const scanner = await createScanner(nc);
  scanner.start();

  // ═══════════════════════════════════════════════════════════════════════════
  // Heartbeat publishing for service discovery
  // ═══════════════════════════════════════════════════════════════════════════
  const js = jetstream(nc);
  const kvm = new Kvm(js);
  const heartbeatsKv = await kvm.create("service_heartbeats", {
    history: 1,
    ttl: 60 * 1000, // 1 minute TTL
  });

  const heartbeatKey = "ethernetip";
  const startedAt = Date.now();

  const publishHeartbeat = async () => {
    const heartbeat: ServiceHeartbeat = {
      serviceType: "ethernetip",
      moduleId: "ethernetip",
      lastSeen: Date.now(),
      startedAt,
      metadata: {},
    };
    try {
      const encoder = new TextEncoder();
      await heartbeatsKv.put(heartbeatKey, encoder.encode(JSON.stringify(heartbeat)));
    } catch (err) {
      log.warn(`Failed to publish heartbeat: ${err}`);
    }
  };

  // Publish initial heartbeat
  await publishHeartbeat();
  log.info("Service heartbeat started (moduleId: ethernetip)");

  // Publish heartbeat every 10 seconds
  const heartbeatInterval = setInterval(publishHeartbeat, 10000);

  log.info("");
  log.info("Service running. Press Ctrl+C to stop.");
  log.info("Waiting for subscribe/browse requests...");
  log.info("");

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    log.info(`Received ${signal}, shutting down...`);

    // Stop heartbeat publishing
    clearInterval(heartbeatInterval);
    try {
      await heartbeatsKv.delete(heartbeatKey);
      log.info("Removed service heartbeat");
    } catch {
      // Ignore - may already be expired
    }

    // Stop scanner
    await scanner.stop();

    // Drain NATS connection
    await nc.drain();

    log.info("Shutdown complete");
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", () => shutdown("SIGINT"));
  Deno.addSignalListener("SIGTERM", () => shutdown("SIGTERM"));

  // Listen for NATS shutdown command from graphql
  const shutdownSub = nc.subscribe("ethernetip.shutdown");
  (async () => {
    for await (const _msg of shutdownSub) {
      log.info("Received shutdown command via NATS");
      await shutdown("NATS shutdown");
      break;
    }
  })();
}

// Run
if (import.meta.main) {
  main().catch((err) => {
    log.error(`Fatal error: ${err}`);
    Deno.exit(1);
  });
}
