/**
 * Configuration types for tentacle-ethernetip
 */

/**
 * NATS connection configuration
 */
export type NatsConfig = {
  servers: string | string[];
  user?: string;
  pass?: string;
  token?: string;
};

/**
 * MQTT bridge configuration (optional)
 */
export type MqttConfig = {
  enabled: boolean;
  broker?: string;
  topicPrefix?: string;
  username?: string;
  password?: string;
};

/**
 * Default settings
 */
export type DefaultsConfig = {
  pollRate?: number;
  timeout?: number;
  retryDelay?: number;
  maxRetries?: number;
};

/**
 * Deadband (RBE) configuration
 */
export type DeadbandConfig = {
  value: number;
  maxTime?: number;
};

/**
 * CIP data type names
 */
export type CipDataTypeName =
  | "BOOL"
  | "SINT"
  | "INT"
  | "DINT"
  | "LINT"
  | "USINT"
  | "UINT"
  | "UDINT"
  | "ULINT"
  | "REAL"
  | "LREAL"
  | "STRING";

/**
 * Rockwell tag configuration (symbolic addressing)
 */
export type RockwellTagConfig = {
  id: string;
  address: string; // Symbolic tag name e.g., "Motor_Speed"
  datatype?: CipDataTypeName;
  writable?: boolean;
  deadband?: DeadbandConfig;
  disableRBE?: boolean;
};

/**
 * Generic CIP tag configuration (class/instance/attribute addressing)
 */
export type GenericTagConfig = {
  id: string;
  classCode: number;
  instance: number;
  attribute: number;
  datatype?: CipDataTypeName;
  writable?: boolean;
  deadband?: DeadbandConfig;
  disableRBE?: boolean;
};

/**
 * Tag configuration (either type)
 */
export type TagConfig = RockwellTagConfig | GenericTagConfig;

/**
 * Type guard for Rockwell tag config
 */
export function isRockwellTag(tag: TagConfig): tag is RockwellTagConfig {
  return "address" in tag;
}

/**
 * Type guard for Generic CIP tag config
 */
export function isGenericTag(tag: TagConfig): tag is GenericTagConfig {
  return "classCode" in tag;
}

/**
 * Base device configuration
 */
export type BaseDeviceConfig = {
  id: string;
  name?: string;
  host: string;
  port?: number;
  pollRate?: number;
  timeout?: number;
  retryDelay?: number;
  maxRetries?: number;
  enabled?: boolean;
};

/**
 * Rockwell device configuration
 */
export type RockwellDeviceConfig = BaseDeviceConfig & {
  type: "rockwell";
  slot?: number;
  tags?: Record<string, RockwellTagConfig>;
};

/**
 * Generic CIP device configuration
 */
export type GenericDeviceConfig = BaseDeviceConfig & {
  type: "generic-cip";
  tags?: Record<string, GenericTagConfig>;
};

/**
 * Device configuration (either type)
 */
export type DeviceConfig = RockwellDeviceConfig | GenericDeviceConfig;

/**
 * Type guard for Rockwell device
 */
export function isRockwellDevice(
  device: DeviceConfig,
): device is RockwellDeviceConfig {
  return device.type === "rockwell";
}

/**
 * Type guard for Generic CIP device
 */
export function isGenericDevice(
  device: DeviceConfig,
): device is GenericDeviceConfig {
  return device.type === "generic-cip";
}

/**
 * Service startup configuration (minimal, from file or env)
 */
export type ServiceConfig = {
  serviceId: string;
  nats: NatsConfig;
  mqtt?: MqttConfig;
  defaults?: DefaultsConfig;
};

/**
 * Runtime configuration (stored in NATS KV)
 */
export type RuntimeConfig = {
  devices: Record<string, DeviceConfig>;
};
