/**
 * NATS command handler for dynamic configuration
 */

import { log } from "../utils/logger.ts";
import type { NatsClient } from "./client.ts";
import { subscribe, respond } from "./client.ts";
import type { DeviceConfig, RockwellTagConfig, GenericTagConfig } from "../../types/config.ts";
import type { BaseDevice } from "../devices/mod.ts";

/**
 * Command request types
 */
export type AddDeviceRequest = {
  id: string;
  name?: string;
  type: "rockwell" | "generic-cip";
  host: string;
  port?: number;
  slot?: number;
  pollRate?: number;
};

export type RemoveDeviceRequest = {
  deviceId: string;
};

export type BrowseTagsRequest = {
  filter?: string;
};

export type ConfigurePollingRequest = {
  pollRate?: number;
  tags: Array<{
    id: string;
    address?: string; // For Rockwell
    classCode?: number; // For Generic
    instance?: number;
    attribute?: number;
    datatype?: string;
    writable?: boolean;
  }>;
};

export type WriteTagRequest = {
  value: unknown;
  requestId?: string;
};

/**
 * Command response types
 */
export type CommandResponse = {
  success: boolean;
  error?: string;
  data?: unknown;
};

/**
 * Command handlers interface
 */
export type CommandHandlers = {
  onAddDevice: (request: AddDeviceRequest) => Promise<DeviceConfig>;
  onRemoveDevice: (deviceId: string) => Promise<void>;
  onBrowseTags: (
    deviceId: string,
    filter?: string,
  ) => Promise<
    Array<{ name: string; datatype: string; dimensions: number[] }>
  >;
  onConfigurePolling: (
    deviceId: string,
    config: ConfigurePollingRequest,
  ) => Promise<void>;
  onWriteTag: (
    deviceId: string,
    tagId: string,
    value: unknown,
  ) => Promise<void>;
};

/**
 * Setup command subscriptions
 */
export function setupCommands(
  client: NatsClient,
  handlers: CommandHandlers,
): void {
  const { serviceId } = client;

  // Add device
  subscribe(
    client,
    `ethernetip.command.${serviceId}.device.add`,
    async (data, reply) => {
      if (!reply) return;

      try {
        const request: AddDeviceRequest = JSON.parse(
          new TextDecoder().decode(data),
        );
        log.nats.info(`Received add device request: ${request.id}`);

        const config = await handlers.onAddDevice(request);

        respond(client, reply, {
          success: true,
          data: { deviceId: config.id },
        } as CommandResponse);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);
        respond(client, reply, {
          success: false,
          error: message,
        } as CommandResponse);
      }
    },
  );

  // Remove device
  subscribe(
    client,
    `ethernetip.command.${serviceId}.device.remove`,
    async (data, reply) => {
      if (!reply) return;

      try {
        const request: RemoveDeviceRequest = JSON.parse(
          new TextDecoder().decode(data),
        );
        log.nats.info(`Received remove device request: ${request.deviceId}`);

        await handlers.onRemoveDevice(request.deviceId);

        respond(client, reply, {
          success: true,
        } as CommandResponse);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);
        respond(client, reply, {
          success: false,
          error: message,
        } as CommandResponse);
      }
    },
  );

  // Browse tags (wildcard for device ID)
  subscribe(
    client,
    `ethernetip.command.${serviceId}.*.browse`,
    async (data, reply, subject) => {
      if (!reply) return;

      // Extract device ID from subject
      const parts = subject?.split(".") ?? [];
      const deviceId = parts[3];
      if (!deviceId) {
        respond(client, reply, {
          success: false,
          error: "Invalid subject format",
        } as CommandResponse);
        return;
      }

      try {
        const request: BrowseTagsRequest = JSON.parse(
          new TextDecoder().decode(data),
        );
        log.nats.info(`Received browse tags request for ${deviceId}`);

        const tags = await handlers.onBrowseTags(deviceId, request.filter);

        respond(client, reply, {
          success: true,
          data: { tags },
        } as CommandResponse);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);
        respond(client, reply, {
          success: false,
          error: message,
        } as CommandResponse);
      }
    },
  );

  // Configure polling (wildcard for device ID)
  subscribe(
    client,
    `ethernetip.command.${serviceId}.*.configure`,
    async (data, reply, subject) => {
      if (!reply) return;

      const parts = subject?.split(".") ?? [];
      const deviceId = parts[3];
      if (!deviceId) {
        respond(client, reply, {
          success: false,
          error: "Invalid subject format",
        } as CommandResponse);
        return;
      }

      try {
        const request: ConfigurePollingRequest = JSON.parse(
          new TextDecoder().decode(data),
        );
        log.nats.info(
          `Received configure polling request for ${deviceId}: ${request.tags.length} tags`,
        );

        await handlers.onConfigurePolling(deviceId, request);

        respond(client, reply, {
          success: true,
          data: { pollingStarted: true },
        } as CommandResponse);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);
        respond(client, reply, {
          success: false,
          error: message,
        } as CommandResponse);
      }
    },
  );

  // Write to tag (wildcard for device ID and tag ID)
  subscribe(
    client,
    `ethernetip.command.${serviceId}.*.write.*`,
    async (data, reply, subject) => {
      const parts = subject?.split(".") ?? [];
      const deviceId = parts[3];
      const tagId = parts[5];

      if (!deviceId || !tagId) {
        if (reply) {
          respond(client, reply, {
            success: false,
            error: "Invalid subject format",
          } as CommandResponse);
        }
        return;
      }

      try {
        const request: WriteTagRequest = JSON.parse(
          new TextDecoder().decode(data),
        );
        log.nats.info(`Received write request for ${deviceId}/${tagId}`);

        await handlers.onWriteTag(deviceId, tagId, request.value);

        if (reply) {
          respond(client, reply, {
            success: true,
            requestId: request.requestId,
          } as CommandResponse);
        }

        // Also publish response on response topic
        const responseSubject = `ethernetip.response.${serviceId}.${deviceId}.${tagId}`;
        respond(client, responseSubject, {
          success: true,
          requestId: request.requestId,
          timestamp: Date.now(),
        });
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);

        if (reply) {
          respond(client, reply, {
            success: false,
            error: message,
          } as CommandResponse);
        }
      }
    },
  );

  log.nats.info(`Command subscriptions set up for service ${serviceId}`);
}
