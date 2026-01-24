/**
 * NATS client and KV store management
 */

import { connect, type NatsConnection, type Subscription } from "@nats-io/transport-deno";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import { log } from "../utils/logger.ts";
import type { NatsConfig, RuntimeConfig } from "../../types/config.ts";

export type NatsClient = {
  nc: NatsConnection;
  js: JetStreamClient;
  kv: {
    config: KV;
    values: KV;
  };
  serviceId: string;
  close: () => Promise<void>;
};

/**
 * Create and initialize NATS client
 */
export async function createNatsClient(
  config: NatsConfig,
  serviceId: string,
): Promise<NatsClient> {
  log.nats.info(`Connecting to NATS at ${config.servers}...`);

  const servers = Array.isArray(config.servers)
    ? config.servers
    : [config.servers];

  const nc = await connect({
    servers,
    user: config.user,
    pass: config.pass,
    token: config.token,
  });

  log.nats.info("Connected to NATS");

  // Create JetStream client
  const js = jetstream(nc);

  // Create KV stores
  const kvm = new Kvm(js);

  const configKv = await kvm.create(`ethernetip-config-${serviceId}`, {
    history: 5,
    ttl: 0, // No TTL - persist forever
  });

  const valuesKv = await kvm.create(`ethernetip-values-${serviceId}`, {
    history: 1,
    ttl: 60 * 60 * 1000, // 1 hour TTL for values
  });

  log.nats.info(`Created KV stores for service ${serviceId}`);

  return {
    nc,
    js,
    kv: {
      config: configKv,
      values: valuesKv,
    },
    serviceId,
    close: async () => {
      await nc.close();
      log.nats.info("NATS connection closed");
    },
  };
}

/**
 * Load runtime configuration from KV
 */
export async function loadRuntimeConfig(
  client: NatsClient,
): Promise<RuntimeConfig> {
  try {
    const entry = await client.kv.config.get("runtime");
    if (entry?.value) {
      const decoder = new TextDecoder();
      const json = decoder.decode(entry.value);
      return JSON.parse(json) as RuntimeConfig;
    }
  } catch {
    // No config found
  }

  return { devices: {} };
}

/**
 * Save runtime configuration to KV
 */
export async function saveRuntimeConfig(
  client: NatsClient,
  config: RuntimeConfig,
): Promise<void> {
  const encoder = new TextEncoder();
  const json = encoder.encode(JSON.stringify(config));
  await client.kv.config.put("runtime", json);
  log.nats.debug("Saved runtime config to KV");
}

/**
 * Subscribe to a subject with a handler
 */
export function subscribe(
  client: NatsClient,
  subject: string,
  handler: (data: Uint8Array, reply?: string, subject?: string) => Promise<void> | void,
): Subscription {
  const sub = client.nc.subscribe(subject);

  (async () => {
    for await (const msg of sub) {
      try {
        await handler(msg.data, msg.reply, msg.subject);
      } catch (error) {
        log.nats.error(`Error handling message on ${subject}: ${error}`);
      }
    }
  })();

  log.nats.debug(`Subscribed to ${subject}`);
  return sub;
}

/**
 * Publish a message
 */
export function publish(
  client: NatsClient,
  subject: string,
  data: unknown,
): void {
  const encoder = new TextEncoder();
  const json = encoder.encode(JSON.stringify(data));
  client.nc.publish(subject, json);
}

/**
 * Publish a response to a request
 */
export function respond(
  client: NatsClient,
  reply: string,
  data: unknown,
): void {
  const encoder = new TextEncoder();
  const json = encoder.encode(JSON.stringify(data));
  client.nc.publish(reply, json);
}
