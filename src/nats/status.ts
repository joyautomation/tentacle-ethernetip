/**
 * Service and device status publishing
 */

import { log } from "../utils/logger.ts";
import type { NatsClient } from "./client.ts";
import { publish } from "./client.ts";
import type { DeviceState, DeviceStats } from "../devices/mod.ts";
import type { DeviceIdentity } from "../ethernetip/mod.ts";

/**
 * Service status message
 */
export type ServiceStatusMessage = {
  serviceId: string;
  status: "starting" | "running" | "stopping" | "stopped" | "error";
  timestamp: number;
  uptime: number;
  deviceCount: number;
  devices: Record<
    string,
    {
      status: DeviceState;
      lastPoll: number | null;
      pollCount: number;
      errorCount: number;
    }
  >;
};

/**
 * Device status message
 */
export type DeviceStatusMessage = {
  serviceId: string;
  deviceId: string;
  status: DeviceState;
  timestamp: number;
  identity?: {
    vendor: string;
    productType: string;
    productName: string;
    revision: { major: number; minor: number };
    serial: number;
  };
  statistics: DeviceStats;
};

/**
 * Health check message (compatible with nats-schema)
 */
export type HealthCheckMessage = {
  service: string;
  status: "healthy" | "unhealthy" | "degraded";
  timestamp: number;
  uptime: number;
  checks: Record<string, boolean>;
};

/**
 * Create a status publisher
 */
export function createStatusPublisher(client: NatsClient, startTime: number) {
  return {
    /**
     * Publish service status
     */
    publishServiceStatus(
      status: ServiceStatusMessage["status"],
      devices: Map<
        string,
        { state: DeviceState; stats: DeviceStats }
      >,
    ): void {
      const subject = `ethernetip.status.${client.serviceId}`;

      const deviceStatuses: ServiceStatusMessage["devices"] = {};
      for (const [id, device] of devices) {
        deviceStatuses[id] = {
          status: device.state,
          lastPoll: device.stats.lastPoll,
          pollCount: device.stats.pollCount,
          errorCount: device.stats.errorCount,
        };
      }

      const message: ServiceStatusMessage = {
        serviceId: client.serviceId,
        status,
        timestamp: Date.now(),
        uptime: Date.now() - startTime,
        deviceCount: devices.size,
        devices: deviceStatuses,
      };

      publish(client, subject, message);
    },

    /**
     * Publish device status
     */
    publishDeviceStatus(
      deviceId: string,
      state: DeviceState,
      stats: DeviceStats,
      identity?: DeviceIdentity,
    ): void {
      const subject = `ethernetip.device.${client.serviceId}.${deviceId}`;

      const message: DeviceStatusMessage = {
        serviceId: client.serviceId,
        deviceId,
        status: state,
        timestamp: Date.now(),
        identity: identity
          ? {
              vendor: identity.vendor,
              productType: identity.productType,
              productName: identity.productName,
              revision: identity.revision,
              serial: identity.serial,
            }
          : undefined,
        statistics: stats,
      };

      publish(client, subject, message);
    },

    /**
     * Publish health check
     */
    publishHealthCheck(
      devices: Map<string, { state: DeviceState }>,
    ): void {
      const subject = `system.health.${client.serviceId}`;

      const checks: Record<string, boolean> = {
        nats: true, // If we can publish, NATS is healthy
      };

      let hasError = false;
      let hasUnhealthy = false;

      for (const [id, device] of devices) {
        const isHealthy =
          device.state === "connected" || device.state === "polling";
        checks[`device-${id}`] = isHealthy;
        if (device.state === "error") hasError = true;
        if (!isHealthy) hasUnhealthy = true;
      }

      const message: HealthCheckMessage = {
        service: client.serviceId,
        status: hasError ? "unhealthy" : hasUnhealthy ? "degraded" : "healthy",
        timestamp: Date.now(),
        uptime: Date.now() - startTime,
        checks,
      };

      publish(client, subject, message);
    },
  };
}

export type StatusPublisher = ReturnType<typeof createStatusPublisher>;
