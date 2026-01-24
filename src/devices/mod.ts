/**
 * Device module exports
 */

export { BaseDevice } from "./device.ts";
export type {
  DeviceState,
  DeviceStats,
  TagValue,
  DeviceEventHandlers,
} from "./device.ts";

export { RockwellDevice } from "./rockwell.ts";
export { GenericDevice } from "./generic.ts";

import type { DeviceConfig } from "../../types/config.ts";
import { isRockwellDevice, isGenericDevice } from "../../types/config.ts";
import type { DeviceEventHandlers } from "./device.ts";
import { BaseDevice } from "./device.ts";
import { RockwellDevice } from "./rockwell.ts";
import { GenericDevice } from "./generic.ts";

/**
 * Create a device instance based on configuration
 */
export function createDevice(
  config: DeviceConfig,
  handlers: DeviceEventHandlers = {},
): BaseDevice {
  if (isRockwellDevice(config)) {
    return new RockwellDevice(config, handlers);
  } else if (isGenericDevice(config)) {
    return new GenericDevice(config, handlers);
  } else {
    throw new Error(`Unknown device type: ${(config as DeviceConfig).type}`);
  }
}
