/**
 * Generic CIP device implementation
 *
 * Uses class/instance/attribute addressing with GetAttributeSingle/SetAttributeSingle
 */

import { log } from "../utils/logger.ts";
import { BaseDevice, DeviceEventHandlers, TagValue } from "./device.ts";
import type { GenericDeviceConfig, GenericTagConfig } from "../../types/config.ts";
import {
  getAttributeSingle,
  setAttributeSingle,
  decodeFloat32,
  decodeUint,
  decodeInt,
  encodeFloat32,
  encodeUint,
  encodeInt,
} from "../ethernetip/mod.ts";
import type { CipResponse } from "../ethernetip/mod.ts";

/**
 * Parse attribute value from CIP response
 */
function parseAttributeValue(
  response: CipResponse,
  datatype: string,
): unknown {
  const data = response.data;

  switch (datatype) {
    case "BOOL":
      return data[0] !== 0;
    case "SINT":
      return new DataView(data.buffer, data.byteOffset).getInt8(0);
    case "INT":
      return new DataView(data.buffer, data.byteOffset).getInt16(0, true);
    case "DINT":
      return new DataView(data.buffer, data.byteOffset).getInt32(0, true);
    case "USINT":
      return data[0];
    case "UINT":
      return new DataView(data.buffer, data.byteOffset).getUint16(0, true);
    case "UDINT":
      return new DataView(data.buffer, data.byteOffset).getUint32(0, true);
    case "REAL":
      return decodeFloat32(data.subarray(0, 4));
    case "LREAL":
      return new DataView(data.buffer, data.byteOffset).getFloat64(0, true);
    default:
      // Return raw bytes for unknown types
      return Array.from(data);
  }
}

/**
 * Encode a value for writing to an attribute
 */
function encodeAttributeValue(
  value: unknown,
  datatype: string,
): Uint8Array {
  switch (datatype) {
    case "BOOL":
      return new Uint8Array([value ? 0x01 : 0x00]);
    case "SINT": {
      const buf = new ArrayBuffer(1);
      new DataView(buf).setInt8(0, Number(value));
      return new Uint8Array(buf);
    }
    case "INT":
      return encodeInt(Number(value), 2);
    case "DINT":
      return encodeInt(Number(value), 4);
    case "USINT":
      return new Uint8Array([Number(value) & 0xff]);
    case "UINT":
      return encodeUint(Number(value), 2);
    case "UDINT":
      return encodeUint(Number(value), 4);
    case "REAL":
      return encodeFloat32(Number(value));
    case "LREAL": {
      const buf = new ArrayBuffer(8);
      new DataView(buf).setFloat64(0, Number(value), true);
      return new Uint8Array(buf);
    }
    default:
      throw new Error(`Unsupported data type for writing: ${datatype}`);
  }
}

/**
 * Generic CIP device class
 */
export class GenericDevice extends BaseDevice {
  protected declare config: GenericDeviceConfig;

  constructor(config: GenericDeviceConfig, handlers: DeviceEventHandlers = {}) {
    super(config, handlers);
  }

  /**
   * Poll all configured tags
   */
  protected async pollAllTags(): Promise<void> {
    if (!this.cip) throw new Error("Not connected");

    for (const [tagId, tagConfig] of this.tags) {
      const genericTag = tagConfig as GenericTagConfig;

      try {
        const response = await getAttributeSingle(
          this.cip,
          genericTag.classCode,
          genericTag.instance,
          genericTag.attribute,
        );

        if (response.success) {
          const datatype = genericTag.datatype ?? "REAL";
          const value = parseAttributeValue(response, datatype);

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
            datatype: genericTag.datatype ?? "UNKNOWN",
            quality: "bad",
            timestamp: Date.now(),
          });
        }
      } catch (error) {
        log.device.warn(`Device ${this.id}: Error reading tag ${tagId}: ${error}`);
        this.updateTagValue(tagId, {
          tagId,
          value: null,
          datatype: genericTag.datatype ?? "UNKNOWN",
          quality: "bad",
          timestamp: Date.now(),
        });
      }
    }
  }

  /**
   * Read a single tag (attribute)
   */
  async readTag(tagId: string): Promise<TagValue> {
    if (!this.cip) throw new Error("Not connected");

    const tagConfig = this.tags.get(tagId) as GenericTagConfig | undefined;
    if (!tagConfig) throw new Error(`Tag not configured: ${tagId}`);

    const response = await getAttributeSingle(
      this.cip,
      tagConfig.classCode,
      tagConfig.instance,
      tagConfig.attribute,
    );

    if (!response.success) {
      throw new Error(`Read failed with status 0x${response.status.toString(16)}`);
    }

    const datatype = tagConfig.datatype ?? "REAL";
    const value = parseAttributeValue(response, datatype);

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
   * Write to a tag (attribute)
   */
  async writeTag(tagId: string, value: unknown): Promise<void> {
    if (!this.cip) throw new Error("Not connected");

    const tagConfig = this.tags.get(tagId) as GenericTagConfig | undefined;
    if (!tagConfig) throw new Error(`Tag not configured: ${tagId}`);

    if (tagConfig.writable === false) {
      throw new Error(`Tag is not writable: ${tagId}`);
    }

    const datatype = tagConfig.datatype ?? "REAL";
    const data = encodeAttributeValue(value, datatype);

    const response = await setAttributeSingle(
      this.cip,
      tagConfig.classCode,
      tagConfig.instance,
      tagConfig.attribute,
      data,
    );

    if (!response.success) {
      throw new Error(`Write failed with status 0x${response.status.toString(16)}`);
    }

    log.device.info(
      `Device ${this.id}: Wrote ${value} to class ${tagConfig.classCode}, instance ${tagConfig.instance}, attr ${tagConfig.attribute}`,
    );
  }

  /**
   * Browse available tags
   * Generic CIP devices don't support tag discovery like Rockwell
   * Returns empty array - tags must be configured manually
   */
  async browseTags(_filter?: string): Promise<Array<{
    name: string;
    datatype: string;
    dimensions: number[];
  }>> {
    log.device.warn(
      `Device ${this.id}: Tag browsing not supported for generic CIP devices`,
    );
    return [];
  }
}
