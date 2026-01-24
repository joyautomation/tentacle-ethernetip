/**
 * NATS module exports
 */

export {
  createNatsClient,
  loadRuntimeConfig,
  saveRuntimeConfig,
  subscribe,
  publish,
  respond,
} from "./client.ts";
export type { NatsClient } from "./client.ts";

export { createPublisher } from "./publisher.ts";
export type { Publisher, TagDataMessage } from "./publisher.ts";

export { createStatusPublisher } from "./status.ts";
export type {
  StatusPublisher,
  ServiceStatusMessage,
  DeviceStatusMessage,
  HealthCheckMessage,
} from "./status.ts";

export { setupCommands } from "./commands.ts";
export type {
  CommandHandlers,
  CommandResponse,
  AddDeviceRequest,
  RemoveDeviceRequest,
  BrowseTagsRequest,
  ConfigurePollingRequest,
  WriteTagRequest,
} from "./commands.ts";
