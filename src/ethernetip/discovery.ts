/**
 * Tag discovery for Rockwell/Allen-Bradley controllers
 *
 * Uses GetInstanceAttributeList (0x55) service against the Symbol class (0x6B)
 * to enumerate all tags on the controller.
 */

import { log } from "../utils/logger.ts";
import { Cip, sendRaw, buildHeader, buildCommonPacketFormat } from "./cip.ts";
import {
  decodeString,
  decodeUint,
  encodeUint,
  joinBytes,
} from "./encode.ts";
import { createLogicalSegment, encodeEpath, encodeSymbolicPath } from "./dataTypes.ts";
import { encapsulation, service } from "./services.ts";
import { classCode, SYMBOL_TYPE } from "./objectLibrary.ts";
import { EXTERNAL_ACCESS } from "./status.ts";

/**
 * Discovered tag information
 */
export type DiscoveredTag = {
  name: string;
  instanceId: number;
  symbolType: number;
  symbolAddress: number;
  datatype: string;
  dimensions: number[];
  isStruct: boolean;
  isArray: boolean;
  externalAccess: string;
  program?: string; // Program name if program-scoped
};

/**
 * Parse symbol type to get data type info
 */
function parseSymbolType(symbolType: number): {
  datatype: string;
  isStruct: boolean;
  isArray: boolean;
  dimensions: number;
} {
  const isStruct = (symbolType & SYMBOL_TYPE.STRUCT) !== 0;
  const arrayMask = symbolType & 0x6000;

  let dimensions = 0;
  if (arrayMask === SYMBOL_TYPE.ARRAY_1D) dimensions = 1;
  else if (arrayMask === SYMBOL_TYPE.ARRAY_2D) dimensions = 2;
  else if (arrayMask === SYMBOL_TYPE.ARRAY_3D) dimensions = 3;

  const typeCode = symbolType & SYMBOL_TYPE.TYPE_MASK;

  // Map CIP type codes to data type names
  const typeMap: Record<number, string> = {
    0xc1: "BOOL",
    0xc2: "SINT",
    0xc3: "INT",
    0xc4: "DINT",
    0xc5: "LINT",
    0xc6: "USINT",
    0xc7: "UINT",
    0xc8: "UDINT",
    0xc9: "ULINT",
    0xca: "REAL",
    0xcb: "LREAL",
    0xd0: "STRING",
    0xd1: "BYTE",
    0xd2: "WORD",
    0xd3: "DWORD",
    0xd4: "LWORD",
  };

  const datatype = isStruct
    ? "STRUCT"
    : typeMap[typeCode] ?? `UNKNOWN(0x${typeCode.toString(16)})`;

  return {
    datatype,
    isStruct,
    isArray: dimensions > 0,
    dimensions,
  };
}

/**
 * Parse tag data from GetInstanceAttributeList response
 */
function parseTagData(
  data: Uint8Array,
  offset: number,
): { tag: Partial<DiscoveredTag>; bytesConsumed: number } | null {
  if (offset >= data.length) return null;

  // Instance ID (4 bytes)
  const instanceId = decodeUint(data.subarray(offset, offset + 4));
  offset += 4;

  // Attribute 1: Tag name (string with length prefix)
  const nameLength = decodeUint(data.subarray(offset, offset + 2));
  offset += 2;
  const name = decodeString(data.subarray(offset, offset + nameLength));
  offset += nameLength;

  // Attribute 2: Symbol type (2 bytes)
  const symbolType = decodeUint(data.subarray(offset, offset + 2));
  offset += 2;

  // Attribute 3: Symbol address (4 bytes)
  const symbolAddress = decodeUint(data.subarray(offset, offset + 4));
  offset += 4;

  // Attribute 5: Symbol object address (4 bytes) - skip for now
  offset += 4;

  // Attribute 6: Software control (4 bytes) - skip for now
  offset += 4;

  // Attribute 8: Array dimensions (3 x 4 bytes)
  const dim1 = decodeUint(data.subarray(offset, offset + 4));
  offset += 4;
  const dim2 = decodeUint(data.subarray(offset, offset + 4));
  offset += 4;
  const dim3 = decodeUint(data.subarray(offset, offset + 4));
  offset += 4;

  const dimensions: number[] = [];
  if (dim1 > 0) dimensions.push(dim1);
  if (dim2 > 0) dimensions.push(dim2);
  if (dim3 > 0) dimensions.push(dim3);

  const { datatype, isStruct, isArray } = parseSymbolType(symbolType);

  return {
    tag: {
      name,
      instanceId,
      symbolType,
      symbolAddress,
      datatype,
      dimensions,
      isStruct,
      isArray,
      externalAccess: "Read/Write", // Default, may be overwritten
    },
    bytesConsumed: offset,
  };
}

/**
 * Build GetInstanceAttributeList request for Symbol class
 */
function buildGetInstanceAttributeListRequest(
  cip: Cip,
  lastInstance: number,
  program?: string,
): Uint8Array {
  // Build path to Symbol class
  const segments: Uint8Array[] = [];

  // If program-scoped, add program segment first
  if (program) {
    const programPath = encodeSymbolicPath(`Program:${program}`);
    segments.push(programPath);
  }

  // Add Symbol class and starting instance
  const classSegment = createLogicalSegment(classCode.symbolObject, "classId");
  const instanceSegment = createLogicalSegment(
    encodeUint(lastInstance, lastInstance > 255 ? 2 : 1),
    "instanceId",
  );

  segments.push(classSegment.encode(false));
  segments.push(instanceSegment.encode(false));

  const path = joinBytes(segments);

  // Attributes to request:
  // 1 = Symbol name
  // 2 = Symbol type
  // 3 = Symbol address
  // 5 = Symbol object address
  // 6 = Software control
  // 8 = Array dimensions [dim1, dim2, dim3]
  const attributes = joinBytes([
    encodeUint(6, 2), // Number of attributes
    encodeUint(1, 2), // Attribute 1
    encodeUint(2, 2), // Attribute 2
    encodeUint(3, 2), // Attribute 3
    encodeUint(5, 2), // Attribute 5
    encodeUint(6, 2), // Attribute 6
    encodeUint(8, 2), // Attribute 8
  ]);

  // Build CIP message
  const cipMessage = joinBytes([
    service.getInstanceAttributeList,
    encodeUint(Math.floor(path.length / 2), 1), // Path length in words
    path,
    attributes,
  ]);

  // Wrap in common packet format (unconnected)
  const cpf = buildCommonPacketFormat({
    timeout: new Uint8Array([0x0a, 0x00]),
    addressType: new Uint8Array([0x00, 0x00]),
    message: cipMessage,
    messageType: new Uint8Array([0xb2, 0x00]),
    addressData: null,
  });

  const header = buildHeader({
    command: encapsulation.sendRRData,
    session: cip.session,
    context: cip.context,
    option: cip.option,
    length: cpf.length,
  });

  return joinBytes([header, cpf]);
}

/**
 * Browse all tags from a Rockwell controller
 *
 * @param cip - The CIP connection
 * @param program - Optional program name for program-scoped tags
 * @param filter - Optional glob pattern to filter tags (e.g., "Motor*")
 */
export async function browseTags(
  cip: Cip,
  program?: string,
  filter?: string,
): Promise<DiscoveredTag[]> {
  const tags: DiscoveredTag[] = [];
  let lastInstance = 0;
  let hasMore = true;

  log.eip.info(`Browsing tags${program ? ` in program ${program}` : ""}...`);

  while (hasMore) {
    const request = buildGetInstanceAttributeListRequest(cip, lastInstance, program);
    const response = await sendRaw(cip, request);

    // Check response status
    const status = response[42]; // CIP status at offset 42
    const extendedStatusSize = response[43];
    const dataOffset = 44 + extendedStatusSize * 2;

    if (status === 0x00) {
      // Success - no more tags
      hasMore = false;
    } else if (status === 0x06) {
      // Partial response - more tags available
      hasMore = true;
    } else {
      // Error
      log.eip.warn(`Tag browse error: status 0x${status.toString(16)}`);
      break;
    }

    // Parse returned tags
    const data = response.subarray(dataOffset);
    let offset = 0;

    while (offset < data.length - 4) {
      const result = parseTagData(data, offset);
      if (!result) break;

      const { tag, bytesConsumed } = result;

      // Update lastInstance for pagination
      if (tag.instanceId !== undefined && tag.instanceId > lastInstance) {
        lastInstance = tag.instanceId;
      }

      // Apply filter if specified
      if (filter && tag.name) {
        const pattern = filter
          .replace(/\*/g, ".*")
          .replace(/\?/g, ".");
        const regex = new RegExp(`^${pattern}$`, "i");
        if (!regex.test(tag.name)) {
          offset = bytesConsumed;
          continue;
        }
      }

      // Skip internal/system tags (starting with __)
      if (tag.name?.startsWith("__")) {
        offset = bytesConsumed;
        continue;
      }

      tags.push({
        name: tag.name ?? "",
        instanceId: tag.instanceId ?? 0,
        symbolType: tag.symbolType ?? 0,
        symbolAddress: tag.symbolAddress ?? 0,
        datatype: tag.datatype ?? "UNKNOWN",
        dimensions: tag.dimensions ?? [],
        isStruct: tag.isStruct ?? false,
        isArray: tag.isArray ?? false,
        externalAccess: tag.externalAccess ?? "Read/Write",
        program,
      });

      offset = bytesConsumed;
    }
  }

  log.eip.info(`Found ${tags.length} tags${program ? ` in program ${program}` : ""}`);
  return tags;
}

/**
 * Get list of program names from controller
 */
export async function listPrograms(cip: Cip): Promise<string[]> {
  // TODO: Implement program listing via Program class (0x64)
  // For now, return empty array
  return [];
}

/**
 * Browse all tags including program-scoped tags
 */
export async function browseAllTags(
  cip: Cip,
  filter?: string,
): Promise<DiscoveredTag[]> {
  // Get controller-scoped tags
  const controllerTags = await browseTags(cip, undefined, filter);

  // Get program names
  const programs = await listPrograms(cip);

  // Get program-scoped tags
  const programTags: DiscoveredTag[] = [];
  for (const program of programs) {
    const tags = await browseTags(cip, program, filter);
    programTags.push(...tags);
  }

  return [...controllerTags, ...programTags];
}
