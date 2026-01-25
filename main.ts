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
import { jetstream } from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";
import { createConfigManager } from "./src/service/config.ts";
import { createMqttConfigManager } from "./src/service/mqttConfig.ts";
import { createScanner } from "./src/service/scanner.ts";
import type { ServiceHeartbeat } from "@tentacle/nats-schema";

const log = createLogger("ethernetip", LogLevel.info);

// ═══════════════════════════════════════════════════════════════════════════
// Configuration from environment
// ═══════════════════════════════════════════════════════════════════════════

function loadEnvConfig(): { natsServers: string; projectId: string; clearCache: boolean } {
  const natsServers = Deno.env.get("NATS_SERVERS") || "localhost:4222";
  const projectId = Deno.env.get("PROJECT_ID");
  const clearCache = Deno.env.get("CLEAR_CACHE") === "true" || Deno.args.includes("--clear-cache");

  if (!projectId) {
    log.error("PROJECT_ID environment variable is required");
    Deno.exit(1);
  }

  return { natsServers, projectId, clearCache };
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
// Cache Management
// ═══════════════════════════════════════════════════════════════════════════

async function clearVariableCache(nc: NatsConnection, projectId: string): Promise<void> {
  try {
    const kvm = new Kvm(nc);
    const bucketName = `field-config-${projectId}`;
    const kv = await kvm.open(bucketName);

    // Find and delete all cache.variables.* keys
    const keys = await kv.keys("cache.variables.>");
    let count = 0;
    for await (const key of keys) {
      await kv.delete(key);
      count++;
      log.info(`  Deleted cache: ${key}`);
    }

    if (count === 0) {
      log.info("  No variable cache found to clear");
    } else {
      log.info(`  Cleared ${count} variable cache entries`);
    }
  } catch (err) {
    log.warn(`Failed to clear cache: ${err}`);
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
  const { natsServers, projectId, clearCache } = loadEnvConfig();
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

  // Clear variable cache if requested
  if (clearCache) {
    log.info("Clearing variable cache (--clear-cache)...");
    await clearVariableCache(nc, projectId);
  }

  // Create config manager (loads from NATS KV)
  log.info("Loading configuration from NATS KV...");
  const configManager = await createConfigManager(nc, projectId);

  // Create MQTT config manager (controls which variables go to MQTT)
  log.info("Loading MQTT configuration...");
  const mqttConfigManager = await createMqttConfigManager(nc, projectId);

  // Create scanner
  log.info("Initializing scanner...");
  const scanner = await createScanner(nc, projectId, configManager, mqttConfigManager);

  // Start scanning
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

  const hostname = Deno.hostname();
  const instanceId = `${hostname}-${crypto.randomUUID().slice(0, 8)}`;
  const heartbeatKey = `ethernetip.${instanceId}.${projectId}`;
  const startedAt = Date.now();

  const publishHeartbeat = async () => {
    const heartbeat: ServiceHeartbeat = {
      serviceType: "ethernetip",
      instanceId,
      projectId,
      lastSeen: Date.now(),
      startedAt,
      metadata: {
        plcCount: configManager.config.plcs.size,
      },
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
  log.info(`Service heartbeat started (instance: ${instanceId})`);

  // Publish heartbeat every 10 seconds
  const heartbeatInterval = setInterval(publishHeartbeat, 10000);

  log.info("");
  log.info("Service running. Press Ctrl+C to stop.");
  log.info("");
  log.info("To add a PLC, use nats CLI:");
  log.info(`  nats kv put field-config-${projectId} plc.myplc '{"host":"192.168.1.100","port":44818,"scanRate":1000,"enabled":true}'`);
  log.info(`  nats kv put field-config-${projectId} plc.myplc.tags '["Tag1","Tag2"]'`);
  log.info("");
  log.info("To enable MQTT for variables:");
  log.info(`  nats kv put mqtt-config-${projectId} mqtt.variables '{"TagName":{"enabled":true}}'`);
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

    // Stop config managers
    configManager.stop();
    mqttConfigManager.stop();

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
