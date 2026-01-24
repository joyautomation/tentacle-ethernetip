/**
 * Rockwell/Allen-Bradley device implementation
 *
 * Uses symbolic tag addressing with ReadTag/WriteTag services
 */

import { log } from "../utils/logger.ts";
import { BaseDevice, DeviceEventHandlers, DeviceState, TagValue } from "./device.ts";
import type { RockwellDeviceConfig, RockwellTagConfig } from "../../types/config.ts";
import {
  readTag,
  writeTag,
  browseTags as browseTagsCip,
  CIP_DATA_TYPES,
  decodeFloat32,
  decodeUint,
  decodeInt,
  encodeFloat32,
  encodeUint,
  encodeInt,
} from "../ethernetip/mod.ts";
import type { CipResponse, DiscoveredTag } from "../ethernetip/mod.ts";

/**
 * Parse tag value from CIP response
 */
function parseTagValue(
  response: CipResponse,
  datatype: string,
): unknown {
  const data = response.data;

  // First 2 bytes are the data type code, followed by the actual data
  const typeCode = data[0] | (data[1] << 8);
  const valueData = data.subarray(2);

  switch (datatype) {
    case "BOOL":
      return valueData[0] !== 0;
    case "SINT":
      return new DataView(valueData.buffer, valueData.byteOffset).getInt8(0);
    case "INT":
      return new DataView(valueData.buffer, valueData.byteOffset).getInt16(0, true);
    case "DINT":
      return new DataView(valueData.buffer, valueData.byteOffset).getInt32(0, true);
    case "USINT":
      return valueData[0];
    case "UINT":
      return new DataView(valueData.buffer, valueData.byteOffset).getUint16(0, true);
    case "UDINT":
      return new DataView(valueData.buffer, valueData.byteOffset).getUint32(0, true);
    case "REAL":
      return decodeFloat32(valueData.subarray(0, 4));
    case "LREAL":
      return new DataView(valueData.buffer, valueData.byteOffset).getFloat64(0, true);
    case "STRING": {
      // Rockwell strings: 4-byte length + data
      const len = new DataView(valueData.buffer, valueData.byteOffset).getUint32(0, true);
      return new TextDecoder().decode(valueData.subarray(4, 4 + len));
    }
    default:
      // Return raw bytes for unknown types
      return Array.from(valueData);
  }
}

/**
 * Encode a value for writing to a tag
 */
function encodeTagValue(
  value: unknown,
  datatype: string,
): { typeCode: number; data: Uint8Array } {
  switch (datatype) {
    case "BOOL":
      return {
        typeCode: CIP_DATA_TYPES.BOOL,
        data: new Uint8Array([value ? 0xff : 0x00]),
      };
    case "SINT": {
      const buf = new ArrayBuffer(1);
      new DataView(buf).setInt8(0, Number(value));
      return { typeCode: CIP_DATA_TYPES.SINT, data: new Uint8Array(buf) };
    }
    case "INT":
      return {
        typeCode: CIP_DATA_TYPES.INT,
        data: encodeInt(Number(value), 2),
      };
    case "DINT":
      return {
        typeCode: CIP_DATA_TYPES.DINT,
        data: encodeInt(Number(value), 4),
      };
    case "USINT":
      return {
        typeCode: CIP_DATA_TYPES.USINT,
        data: new Uint8Array([Number(value) & 0xff]),
      };
    case "UINT":
      return {
        typeCode: CIP_DATA_TYPES.UINT,
        data: encodeUint(Number(value), 2),
      };
    case "UDINT":
      return {
        typeCode: CIP_DATA_TYPES.UDINT,
        data: encodeUint(Number(value), 4),
      };
    case "REAL":
      return {
        typeCode: CIP_DATA_TYPES.REAL,
        data: encodeFloat32(Number(value)),
      };
    case "LREAL": {
      const buf = new ArrayBuffer(8);
      new DataView(buf).setFloat64(0, Number(value), true);
      return { typeCode: CIP_DATA_TYPES.LREAL, data: new Uint8Array(buf) };
    }
    default:
      throw new Error(`Unsupported data type for writing: ${datatype}`);
  }
}

/**
 * Rockwell device class
 */
export class RockwellDevice extends BaseDevice {
  protected declare config: RockwellDeviceConfig;
  private discoveredTags: Map<string, DiscoveredTag> = new Map();

  constructor(config: RockwellDeviceConfig, handlers: DeviceEventHandlers = {}) {
    super(config, handlers);
  }

  /**
   * Get slot number
   */
  get slot(): number {
    return this.config.slot ?? 0;
  }

  /**
   * Poll all configured tags
   */
  protected async pollAllTags(): Promise<void> {
    if (!this.cip) throw new Error("Not connected");

    for (const [tagId, tagConfig] of this.tags) {
      const rockwellTag = tagConfig as RockwellTagConfig;

      try {
        const response = await readTag(this.cip, rockwellTag.address, 1);

        if (response.success) {
          const datatype = rockwellTag.datatype ?? this.getDiscoveredDatatype(rockwellTag.address);
          const value = parseTagValue(response, datatype);

          this.updateTagValue(tagId, {
            tagId,
            value,
            datatype,
            quality: "good",
            timestamp: Date.now(),
          });
        } else {
          this.updateTagValue(tagId, {
            tagId,
            value: null,
            datatype: rockwellTag.datatype ?? "UNKNOWN",
            quality: "bad",
            timestamp: Date.now(),
          });
        }
      } catch (error) {
        log.device.warn(`Device ${this.id}: Error reading tag ${tagId}: ${error}`);
        this.updateTagValue(tagId, {
          tagId,
          value: null,
          datatype: rockwellTag.datatype ?? "UNKNOWN",
          quality: "bad",
          timestamp: Date.now(),
        });
      }
    }
  }

  /**
   * Get discovered datatype for a tag address
   */
  private getDiscoveredDatatype(address: string): string {
    const tag = this.discoveredTags.get(address);
    return tag?.datatype ?? "DINT"; // Default to DINT
  }

  /**
   * Read a single tag
   */
  async readTag(tagId: string): Promise<TagValue> {
    if (!this.cip) throw new Error("Not connected");

    const tagConfig = this.tags.get(tagId) as RockwellTagConfig | undefined;
    if (!tagConfig) throw new Error(`Tag not configured: ${tagId}`);

    const response = await readTag(this.cip, tagConfig.address, 1);

    if (!response.success) {
      throw new Error(`Read failed with status 0x${response.status.toString(16)}`);
    }

    const datatype = tagConfig.datatype ?? this.getDiscoveredDatatype(tagConfig.address);
    const value = parseTagValue(response, datatype);

    const tagValue: TagValue = {
      tagId,
      value,
      datatype,
      quality: "good",
      timestamp: Date.now(),
    };

    this.updateTagValue(tagId, tagValue);
    return tagValue;
  }

  /**
   * Write to a tag
   */
  async writeTag(tagId: string, value: unknown): Promise<void> {
    if (!this.cip) throw new Error("Not connected");

    const tagConfig = this.tags.get(tagId) as RockwellTagConfig | undefined;
    if (!tagConfig) throw new Error(`Tag not configured: ${tagId}`);

    if (tagConfig.writable === false) {
      throw new Error(`Tag is not writable: ${tagId}`);
    }

    const datatype = tagConfig.datatype ?? this.getDiscoveredDatatype(tagConfig.address);
    const { typeCode, data } = encodeTagValue(value, datatype);

    const response = await writeTag(this.cip, tagConfig.address, typeCode, data, 1);

    if (!response.success) {
      throw new Error(`Write failed with status 0x${response.status.toString(16)}`);
    }

    log.device.info(`Device ${this.id}: Wrote ${value} to ${tagConfig.address}`);
  }

  /**
   * Browse available tags on the controller
   */
  async browseTags(filter?: string): Promise<Array<{
    name: string;
    datatype: string;
    dimensions: number[];
  }>> {
    if (!this.cip) throw new Error("Not connected");

    const tags = await browseTagsCip(this.cip, undefined, filter);

    // Cache discovered tags
    for (const tag of tags) {
      this.discoveredTags.set(tag.name, tag);
    }

    return tags.map((tag) => ({
      name: tag.name,
      datatype: tag.datatype,
      dimensions: tag.dimensions,
    }));
  }
}
