/**
 * Tag data publishing to NATS
 */

import { log } from "../utils/logger.ts";
import type { NatsClient } from "./client.ts";
import { publish } from "./client.ts";
import type { TagValue } from "../devices/mod.ts";

/**
 * Published tag data message format
 */
export type TagDataMessage = {
  serviceId: string;
  deviceId: string;
  tagId: string;
  value: unknown;
  datatype: string;
  quality: "good" | "bad" | "uncertain";
  timestamp: number;
};

/**
 * Create a tag data publisher
 */
export function createPublisher(client: NatsClient) {
  return {
    /**
     * Publish a tag value update
     */
    publishTagValue(deviceId: string, tagValue: TagValue): void {
      const subject = `ethernetip.data.${client.serviceId}.${deviceId}.${tagValue.tagId}`;

      const message: TagDataMessage = {
        serviceId: client.serviceId,
        deviceId,
        tagId: tagValue.tagId,
        value: tagValue.value,
        datatype: tagValue.datatype,
        quality: tagValue.quality,
        timestamp: tagValue.timestamp,
      };

      publish(client, subject, message);

      // Also store in KV for persistence
      const key = `${deviceId}/${tagValue.tagId}`;
      const encoder = new TextEncoder();
      client.kv.values.put(key, encoder.encode(JSON.stringify(message))).catch(
        (error) => {
          log.nats.warn(`Failed to store tag value in KV: ${error}`);
        },
      );
    },

    /**
     * Publish multiple tag values at once
     */
    publishTagValues(deviceId: string, tagValues: TagValue[]): void {
      for (const tagValue of tagValues) {
        this.publishTagValue(deviceId, tagValue);
      }
    },
  };
}

export type Publisher = ReturnType<typeof createPublisher>;
