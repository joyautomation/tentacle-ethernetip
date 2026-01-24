/**
 * Main EtherNet/IP service orchestrator
 */

import { log, setLogLevel, LogLevel } from "./utils/logger.ts";
import type {
  ServiceConfig,
  RuntimeConfig,
  DeviceConfig,
  RockwellDeviceConfig,
  GenericDeviceConfig,
  RockwellTagConfig,
  GenericTagConfig,
} from "../types/config.ts";
import { isRockwellDevice, isRockwellTag } from "../types/config.ts";
import {
  createNatsClient,
  loadRuntimeConfig,
  saveRuntimeConfig,
  setupCommands,
  createPublisher,
  createStatusPublisher,
} from "./nats/mod.ts";
import type {
  NatsClient,
  Publisher,
  StatusPublisher,
  AddDeviceRequest,
  ConfigurePollingRequest,
} from "./nats/mod.ts";
import { createDevice, BaseDevice } from "./devices/mod.ts";
import type { DeviceState, DeviceStats, TagValue } from "./devices/mod.ts";

export type EtherNetIPService = {
  config: ServiceConfig;
  devices: Map<string, BaseDevice>;
  start: () => Promise<void>;
  stop: () => Promise<void>;
};

/**
 * Create the EtherNet/IP service
 */
export async function createService(
  config: ServiceConfig,
): Promise<EtherNetIPService> {
  const startTime = Date.now();
  let natsClient: NatsClient | null = null;
  let publisher: Publisher | null = null;
  let statusPublisher: StatusPublisher | null = null;
  const devices = new Map<string, BaseDevice>();
  let runtimeConfig: RuntimeConfig = { devices: {} };
  let statusInterval: number | undefined;

  log.service.info(`Creating EtherNet/IP service: ${config.serviceId}`);

  /**
   * Initialize NATS connection and KV
   */
  async function initNats(): Promise<void> {
    natsClient = await createNatsClient(config.nats, config.serviceId);
    publisher = createPublisher(natsClient);
    statusPublisher = createStatusPublisher(natsClient, startTime);

    // Load runtime config from KV
    runtimeConfig = await loadRuntimeConfig(natsClient);
    log.service.info(
      `Loaded runtime config with ${Object.keys(runtimeConfig.devices).length} devices`,
    );
  }

  /**
   * Create a device from config
   */
  function createDeviceFromConfig(deviceConfig: DeviceConfig): BaseDevice {
    const device = createDevice(deviceConfig, {
      onTagUpdate: (tagId: string, value: TagValue) => {
        publisher?.publishTagValue(deviceConfig.id, value);
      },
      onStateChange: (state: DeviceState, error?: Error) => {
        statusPublisher?.publishDeviceStatus(
          deviceConfig.id,
          state,
          device.statistics,
          device.identity ?? undefined,
        );
      },
      onIdentity: (identity: { productName?: string }) => {
        log.service.info(
          `Device ${deviceConfig.id} identified: ${identity.productName}`,
        );
      },
    });

    // Configure tags if present
    if (deviceConfig.tags) {
      device.configureTags(deviceConfig.tags as Record<string, RockwellTagConfig | GenericTagConfig>);
    }

    return device;
  }

  /**
   * Add a device
   */
  async function addDevice(request: AddDeviceRequest): Promise<DeviceConfig> {
    if (devices.has(request.id)) {
      throw new Error(`Device already exists: ${request.id}`);
    }

    const deviceConfig: DeviceConfig =
      request.type === "rockwell"
        ? ({
            id: request.id,
            name: request.name,
            type: "rockwell",
            host: request.host,
            port: request.port,
            slot: request.slot ?? 0,
            pollRate: request.pollRate ?? config.defaults?.pollRate ?? 1000,
            tags: {},
          } as RockwellDeviceConfig)
        : ({
            id: request.id,
            name: request.name,
            type: "generic-cip",
            host: request.host,
            port: request.port,
            pollRate: request.pollRate ?? config.defaults?.pollRate ?? 1000,
            tags: {},
          } as GenericDeviceConfig);

    // Create and start device
    const device = createDeviceFromConfig(deviceConfig);
    devices.set(request.id, device);
    await device.start();

    // Update runtime config
    runtimeConfig.devices[request.id] = deviceConfig;
    if (natsClient) {
      await saveRuntimeConfig(natsClient, runtimeConfig);
    }

    log.service.info(`Added device: ${request.id}`);
    return deviceConfig;
  }

  /**
   * Remove a device
   */
  async function removeDevice(deviceId: string): Promise<void> {
    const device = devices.get(deviceId);
    if (!device) {
      throw new Error(`Device not found: ${deviceId}`);
    }

    await device.stop();
    devices.delete(deviceId);

    // Update runtime config
    delete runtimeConfig.devices[deviceId];
    if (natsClient) {
      await saveRuntimeConfig(natsClient, runtimeConfig);
    }

    log.service.info(`Removed device: ${deviceId}`);
  }

  /**
   * Browse tags on a device
   */
  async function browseTags(
    deviceId: string,
    filter?: string,
  ): Promise<Array<{ name: string; datatype: string; dimensions: number[] }>> {
    const device = devices.get(deviceId);
    if (!device) {
      throw new Error(`Device not found: ${deviceId}`);
    }

    return await device.browseTags(filter);
  }

  /**
   * Configure polling on a device
   */
  async function configurePolling(
    deviceId: string,
    request: ConfigurePollingRequest,
  ): Promise<void> {
    const device = devices.get(deviceId);
    if (!device) {
      throw new Error(`Device not found: ${deviceId}`);
    }

    const deviceConfig = runtimeConfig.devices[deviceId];
    if (!deviceConfig) {
      throw new Error(`Device config not found: ${deviceId}`);
    }

    // Update poll rate if specified
    if (request.pollRate) {
      deviceConfig.pollRate = request.pollRate;
    }

    // Build tags config
    const tags: Record<string, RockwellTagConfig | GenericTagConfig> = {};

    for (const tag of request.tags) {
      if (isRockwellDevice(deviceConfig)) {
        if (!tag.address) {
          throw new Error(`Tag ${tag.id} requires address for Rockwell device`);
        }
        tags[tag.id] = {
          id: tag.id,
          address: tag.address,
          datatype: tag.datatype as RockwellTagConfig["datatype"],
          writable: tag.writable,
        };
      } else {
        if (
          tag.classCode === undefined ||
          tag.instance === undefined ||
          tag.attribute === undefined
        ) {
          throw new Error(
            `Tag ${tag.id} requires classCode, instance, and attribute for generic CIP device`,
          );
        }
        tags[tag.id] = {
          id: tag.id,
          classCode: tag.classCode,
          instance: tag.instance,
          attribute: tag.attribute,
          datatype: tag.datatype as GenericTagConfig["datatype"],
          writable: tag.writable,
        };
      }
    }

    // Configure device tags
    device.configureTags(tags);

    // Update runtime config
    deviceConfig.tags = tags as any;
    if (natsClient) {
      await saveRuntimeConfig(natsClient, runtimeConfig);
    }

    log.service.info(`Configured ${request.tags.length} tags on ${deviceId}`);
  }

  /**
   * Write to a tag
   */
  async function writeTag(
    deviceId: string,
    tagId: string,
    value: unknown,
  ): Promise<void> {
    const device = devices.get(deviceId);
    if (!device) {
      throw new Error(`Device not found: ${deviceId}`);
    }

    await device.writeTag(tagId, value);
  }

  /**
   * Start the service
   */
  async function start(): Promise<void> {
    log.service.info("Starting EtherNet/IP service...");

    // Initialize NATS
    await initNats();

    // Setup command handlers
    if (natsClient) {
      setupCommands(natsClient, {
        onAddDevice: addDevice,
        onRemoveDevice: removeDevice,
        onBrowseTags: browseTags,
        onConfigurePolling: configurePolling,
        onWriteTag: writeTag,
      });
    }

    // Create and start devices from saved config
    for (const [id, deviceConfig] of Object.entries(runtimeConfig.devices)) {
      const device = createDeviceFromConfig(deviceConfig);
      devices.set(id, device);
      await device.start();
    }

    // Start status publishing interval
    statusInterval = setInterval(() => {
      const deviceInfos = new Map<
        string,
        { state: DeviceState; stats: DeviceStats }
      >();
      for (const [id, device] of devices) {
        deviceInfos.set(id, {
          state: device.currentState,
          stats: device.statistics,
        });
      }

      statusPublisher?.publishServiceStatus("running", deviceInfos);
      statusPublisher?.publishHealthCheck(
        new Map(
          [...devices].map(([id, d]) => [id, { state: d.currentState }]),
        ),
      );
    }, 10000);

    log.service.info("EtherNet/IP service started");
  }

  /**
   * Stop the service
   */
  async function stop(): Promise<void> {
    log.service.info("Stopping EtherNet/IP service...");

    // Stop status interval
    if (statusInterval) {
      clearInterval(statusInterval);
    }

    // Stop all devices
    for (const [id, device] of devices) {
      await device.stop();
    }
    devices.clear();

    // Close NATS connection
    if (natsClient) {
      await natsClient.close();
    }

    log.service.info("EtherNet/IP service stopped");
  }

  return {
    config,
    devices,
    start,
    stop,
  };
}
