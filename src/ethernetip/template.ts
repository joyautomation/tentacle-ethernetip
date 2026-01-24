/**
 * UDT Template reading for Rockwell/Allen-Bradley controllers
 *
 * Uses ReadTag service on Template class (0x6C) to read structure
 * definitions and expand UDT tags into their member components.
 *
 * Template structure:
 * - For each member (memberCount * 8 bytes):
 *   - Member info (2 bytes): array dimensions
 *   - Type code (2 bytes): CIP type or template ID (with 0x8000 flag for structs)
 *   - Offset (4 bytes): byte offset in structure
 * - After member definitions: null-terminated member name strings
 */

import { log } from "../utils/logger.ts";
import { Cip, sendRaw, buildHeader, buildCommonPacketFormat } from "./cip.ts";
import {
  decodeUint,
  encodeUint,
  joinBytes,
} from "./encode.ts";
import { createLogicalSegment } from "./dataTypes.ts";
import { encapsulation, service } from "./services.ts";
import { classCode, SYMBOL_TYPE } from "./objectLibrary.ts";

/**
 * Template member information
 */
export type TemplateMember = {
  name: string;
  typeCode: number;
  datatype: string;
  offset: number;
  size: number;
  isStruct: boolean;
  isArray: boolean;
  arrayDimensions: number;
  templateId?: number; // For nested structs
};

/**
 * Template information for a UDT
 */
export type Template = {
  templateId: number;
  name: string;
  structureSize: number;
  memberCount: number;
  members: TemplateMember[];
};

// Cache for templates to avoid re-reading
const templateCache = new Map<number, Template>();

/**
 * Map CIP type codes to data type names and sizes
 */
const TYPE_INFO: Record<number, { name: string; size: number }> = {
  0xc1: { name: "BOOL", size: 1 },
  0xc2: { name: "SINT", size: 1 },
  0xc3: { name: "INT", size: 2 },
  0xc4: { name: "DINT", size: 4 },
  0xc5: { name: "LINT", size: 8 },
  0xc6: { name: "USINT", size: 1 },
  0xc7: { name: "UINT", size: 2 },
  0xc8: { name: "UDINT", size: 4 },
  0xc9: { name: "ULINT", size: 8 },
  0xca: { name: "REAL", size: 4 },
  0xcb: { name: "LREAL", size: 8 },
  0xd0: { name: "STRING", size: 88 }, // Default STRING size
  0xd1: { name: "BYTE", size: 1 },
  0xd2: { name: "WORD", size: 2 },
  0xd3: { name: "DWORD", size: 4 },
  0xd4: { name: "LWORD", size: 8 },
};

/**
 * Atomic types that can be read directly
 */
export const ATOMIC_TYPES = [
  "BOOL", "SINT", "INT", "DINT", "LINT",
  "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL",
];

/**
 * Get template ID from symbol type (lower 12 bits for structs)
 */
export function getTemplateId(symbolType: number): number | null {
  const isStruct = (symbolType & SYMBOL_TYPE.STRUCT) !== 0;
  if (!isStruct) return null;
  return symbolType & SYMBOL_TYPE.STRUCT_MASK;
}

/**
 * Build request for reading a template instance
 * Uses GetAttributeList to get structure size, member count, and definition
 */
function buildTemplateAttributeRequest(
  cip: Cip,
  templateId: number,
): Uint8Array {
  // Path: Template class (0x6C) -> Instance (templateId)
  const classSegment = createLogicalSegment(classCode.templateObject, "classId");
  const instanceSegment = createLogicalSegment(
    encodeUint(templateId, templateId > 255 ? 2 : 1),
    "instanceId",
  );

  const path = joinBytes([
    classSegment.encode(false),
    instanceSegment.encode(false),
  ]);

  // GetAttributeList: get member count and structure handle
  // Attribute 1 = Structure Handle (4 bytes) - needed to get size
  // Attribute 2 = Template Member Count (2 bytes)
  const attributes = joinBytes([
    encodeUint(2, 2), // Number of attributes
    encodeUint(1, 2), // Attribute 1: Structure Handle
    encodeUint(2, 2), // Attribute 2: Member Count
  ]);

  const cipMessage = joinBytes([
    service.getAttributeList,
    encodeUint(Math.floor(path.length / 2), 1),
    path,
    attributes,
  ]);

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
 * Build request to read template definition using Read Tag service
 * Uses class/instance path with offset (words) and element count
 */
function buildReadTemplateRequest(
  cip: Cip,
  templateId: number,
  wordOffset: number,
  wordsToRead: number,
): Uint8Array {
  // Path: Template class (0x6C) -> Instance (templateId)
  const classSegment = createLogicalSegment(classCode.templateObject, "classId");
  const instanceSegment = createLogicalSegment(
    encodeUint(templateId, templateId > 255 ? 2 : 1),
    "instanceId",
  );

  const path = joinBytes([
    classSegment.encode(false),
    instanceSegment.encode(false),
  ]);

  // Read Tag service (0x4C) with:
  // - Offset in 32-bit words (4 bytes)
  // - Number of 32-bit words to read (2 bytes)
  // Note: The PLC returns (wordsToRead * 4) bytes of data
  const requestData = joinBytes([
    encodeUint(wordOffset, 4),
    encodeUint(wordsToRead, 2),
  ]);

  const cipMessage = joinBytes([
    service.readTag,
    encodeUint(Math.floor(path.length / 2), 1),
    path,
    requestData,
  ]);

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
 * Read template attributes (size, member count)
 */
async function readTemplateAttributes(
  cip: Cip,
  templateId: number,
): Promise<{ structureHandle: number; memberCount: number } | null> {
  const request = buildTemplateAttributeRequest(cip, templateId);
  const response = await sendRaw(cip, request);

  // Check status at offset 42
  const status = response[42];
  if (status !== 0x00) {
    log.eip.warn(`Template ${templateId} attribute read failed: status=0x${status.toString(16)}`);
    return null;
  }

  // Parse response
  const extStatusSize = response[43];
  let offset = 44 + extStatusSize * 2;

  let structureHandle = 0;
  let memberCount = 0;

  // GetAttributeList response format:
  //   - Attribute count (2 bytes)
  //   - For each attribute:
  //     - Attribute ID (2 bytes)
  //     - Status (2 bytes) - 0 = success
  //     - Attribute value (if status is 0)

  const attrCount = decodeUint(response.subarray(offset, offset + 2));
  offset += 2;

  for (let i = 0; i < attrCount; i++) {
    const attrId = decodeUint(response.subarray(offset, offset + 2));
    offset += 2;
    const attrStatus = decodeUint(response.subarray(offset, offset + 2));
    offset += 2;

    if (attrStatus === 0) {
      if (attrId === 1) {
        // Object Definition Size (UINT, 2 bytes) - size in 32-bit words
        structureHandle = decodeUint(response.subarray(offset, offset + 2));
        offset += 2;
      } else if (attrId === 2) {
        // Member Count (UINT, 2 bytes)
        memberCount = decodeUint(response.subarray(offset, offset + 2));
        offset += 2;
      }
    }
  }

  return { structureHandle, memberCount };
}

/**
 * Read the template definition data
 *
 * Template definition structure:
 * - Member definitions: memberCount * 8 bytes
 * - String data: template name + member names (null-terminated)
 *
 * We calculate a reasonable read size based on member count since
 * the "definition size" attribute can be unreliable for large templates.
 */
async function readTemplateDefinition(
  cip: Cip,
  templateId: number,
  memberCount: number,
): Promise<Uint8Array | null> {
  // Calculate needed size: 8 bytes per member definition + ~32 bytes per name + 200 overhead
  // Rockwell AOIs/UDTs can have long member names (up to 40 chars), so we estimate generously
  // Note: word count in request is interpreted as byte count by the PLC
  const estimatedBytes = memberCount * 8 + memberCount * 32 + 200;

  // We may need multiple reads for large templates
  const maxBytesPerRead = 480;  // Safe limit for unconnected messaging
  const allData: Uint8Array[] = [];
  let offset = 0;

  while (offset < estimatedBytes) {
    const bytesToRead = Math.min(maxBytesPerRead, estimatedBytes - offset);
    const request = buildReadTemplateRequest(cip, templateId, offset, bytesToRead);
    const response = await sendRaw(cip, request);

    const status = response[42];
    // Status 0x00 = success, 0x06 = partial (more data available)
    if (status !== 0x00 && status !== 0x06) {
      if (offset === 0) {
        log.eip.warn(`Template ${templateId} definition read failed: status=0x${status.toString(16)}`);
        return null;
      }
      // Might have reached end of data
      break;
    }

    const extStatusSize = response[43];
    const dataOffset = 44 + extStatusSize * 2;
    const chunk = response.subarray(dataOffset);

    if (chunk.length === 0) {
      break;  // No more data
    }

    allData.push(chunk);
    offset += chunk.length;

    // If we got less than requested, we're done
    if (chunk.length < bytesToRead) {
      break;
    }
  }

  if (allData.length === 0) {
    log.eip.warn(`Template ${templateId} definition read returned empty data`);
    return null;
  }

  // Concatenate all chunks
  const totalLength = allData.reduce((sum, arr) => sum + arr.length, 0);
  const data = new Uint8Array(totalLength);
  let pos = 0;
  for (const chunk of allData) {
    data.set(chunk, pos);
    pos += chunk.length;
  }

  log.eip.info(`Template ${templateId}: read ${data.length} bytes total`);

  if (data.length === 0) {
    log.eip.warn(`Template ${templateId} definition read returned empty data`);
    return null;
  }

  return data;
}

/**
 * Parse null-terminated string from buffer
 */
function parseNullTerminatedString(data: Uint8Array, offset: number): { str: string; length: number } {
  let end = offset;
  while (end < data.length && data[end] !== 0) {
    end++;
  }
  const str = new TextDecoder().decode(data.subarray(offset, end));
  return { str, length: end - offset + 1 }; // +1 for null terminator
}

/**
 * Read and parse a template from the PLC
 */
export async function readTemplate(
  cip: Cip,
  templateId: number,
): Promise<Template | null> {
  // Check cache first
  if (templateCache.has(templateId)) {
    return templateCache.get(templateId)!;
  }

  // Step 1: Get template attributes
  const attrs = await readTemplateAttributes(cip, templateId);
  if (!attrs) {
    return null;
  }

  const { structureHandle: definitionWords, memberCount } = attrs;

  if (memberCount === 0) {
    const template: Template = {
      templateId,
      name: `Template_${templateId}`,
      structureSize: 0,
      memberCount: 0,
      members: [],
    };
    templateCache.set(templateId, template);
    return template;
  }

  // Step 2: Read template definition (calculate size from memberCount)
  const definition = await readTemplateDefinition(cip, templateId, memberCount);
  if (!definition) {
    // Return a minimal template if we can't read the definition
    const template: Template = {
      templateId,
      name: `Template_${templateId}`,
      structureSize: 0,
      memberCount,
      members: [],
    };
    templateCache.set(templateId, template);
    return template;
  }

  // Step 3: Parse template definition
  // First part: member definitions (8 bytes each)
  // - Array info (2 bytes)
  // - Type code (2 bytes)
  // - Offset (4 bytes)
  const memberDefs: Array<{
    arrayInfo: number;
    typeCode: number;
    offset: number;
  }> = [];

  let offset = 0;
  for (let i = 0; i < memberCount && offset + 8 <= definition.length; i++) {
    const arrayInfo = decodeUint(definition.subarray(offset, offset + 2));
    const typeCode = decodeUint(definition.subarray(offset + 2, offset + 4));
    const memberOffset = decodeUint(definition.subarray(offset + 4, offset + 8));
    memberDefs.push({ arrayInfo, typeCode, offset: memberOffset });
    offset += 8;
  }

  // Second part: member names (null-terminated strings)
  // First comes the template name, then a semicolon separator, then member names
  const templateNameResult = parseNullTerminatedString(definition, offset);
  const templateName = templateNameResult.str;
  offset += templateNameResult.length;

  // Skip the semicolon separator between template name and member names
  // Rockwell templates use ";" as a separator after the template name
  while (offset < definition.length && (definition[offset] === 0x3b || definition[offset] === 0x00)) {
    offset++;
  }

  // Debug: show what we have after template name
  const remainingBytes = definition.length - offset;
  log.eip.debug(`Template ${templateId}: after name "${templateName}", remaining ${remainingBytes} bytes at offset ${offset}`);
  if (remainingBytes > 0 && remainingBytes < 200) {
    log.eip.debug(`  Raw bytes: ${Array.from(definition.subarray(offset, Math.min(offset + remainingBytes, offset + 100))).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
  }

  const memberNames: string[] = [];
  for (let i = 0; i < memberCount && offset < definition.length; i++) {
    const nameResult = parseNullTerminatedString(definition, offset);
    if (nameResult.str.length > 0) {
      memberNames.push(nameResult.str);
    } else {
      // Empty string means we hit padding or end of valid data
      break;
    }
    offset += nameResult.length;
  }

  // Debug: how many names were parsed
  log.eip.info(`Template ${templateId}: parsed ${memberNames.length}/${memberCount} member names`);

  // Build member list
  const members: TemplateMember[] = [];
  for (let i = 0; i < memberDefs.length; i++) {
    const def = memberDefs[i];
    const name = memberNames[i] ?? `_member${i}`;

    // Skip hidden members (starting with ZZZZZ or __)
    if (name.startsWith("ZZZZZ") || name.startsWith("__")) {
      continue;
    }

    const isStruct = (def.typeCode & 0x8000) !== 0;
    const typeCode = def.typeCode & 0x0fff;
    const arrayDims = (def.arrayInfo >> 13) & 0x03;

    let datatype = "UNKNOWN";
    let size = 0;

    if (isStruct) {
      datatype = "STRUCT";
    } else if (TYPE_INFO[typeCode]) {
      const info = TYPE_INFO[typeCode];
      datatype = info.name;
      size = info.size;
    }

    members.push({
      name,
      typeCode,
      datatype,
      offset: def.offset,
      size,
      isStruct,
      isArray: arrayDims > 0,
      arrayDimensions: arrayDims,
      templateId: isStruct ? typeCode : undefined,
    });
  }

  const template: Template = {
    templateId,
    name: templateName,
    structureSize: (definitionWords - 6) * 4, // Actual definition size in bytes
    memberCount,
    members,
  };

  log.eip.info(`Template ${templateId} "${templateName}": ${members.length} readable members (raw: ${memberCount})`);
  if (members.length > 0) {
    log.eip.debug(`  Members: ${members.map(m => `${m.name}:${m.datatype}`).join(", ")}`);
  }
  templateCache.set(templateId, template);
  return template;
}

/**
 * Expand a UDT tag into its atomic member tags
 * Returns an array of tag paths that can be read individually
 *
 * @param cip - CIP connection
 * @param tagName - Base tag name (e.g., "MyUDT")
 * @param templateId - Template ID from symbolType
 * @param maxDepth - Maximum nesting depth for nested structs (default: 3)
 */
export async function expandUdtMembers(
  cip: Cip,
  tagName: string,
  templateId: number,
  maxDepth: number = 3,
): Promise<Array<{ path: string; datatype: string }>> {
  if (maxDepth <= 0) {
    return [];
  }

  const template = await readTemplate(cip, templateId);
  if (!template) {
    log.eip.warn(`expandUdtMembers: readTemplate returned null for ${templateId}`);
    return [];
  }
  if (template.members.length === 0) {
    log.eip.debug(`expandUdtMembers: template ${templateId} "${template.name}" has 0 parsed members`);
    return [];
  }

  const results: Array<{ path: string; datatype: string }> = [];

  for (const member of template.members) {
    const memberPath = `${tagName}.${member.name}`;

    if (member.isArray) {
      // Skip arrays for now - they require index notation
      log.eip.debug(`  Skipping array member: ${member.name}`);
      continue;
    }

    if (member.isStruct && member.templateId) {
      // Recursively expand nested struct
      log.eip.debug(`  Recursing into struct member: ${member.name} (template ${member.templateId})`);
      const nested = await expandUdtMembers(
        cip,
        memberPath,
        member.templateId,
        maxDepth - 1,
      );
      results.push(...nested);
    } else if (ATOMIC_TYPES.includes(member.datatype)) {
      // Add atomic member
      log.eip.debug(`  Adding atomic member: ${memberPath} (${member.datatype})`);
      results.push({ path: memberPath, datatype: member.datatype });
    } else {
      log.eip.debug(`  Skipping non-atomic member: ${member.name} (${member.datatype})`);
    }
  }

  return results;
}

/**
 * Clear the template cache
 */
export function clearTemplateCache(): void {
  templateCache.clear();
}

/**
 * Get cached template count
 */
export function getTemplateCacheSize(): number {
  return templateCache.size;
}
