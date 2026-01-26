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

  // GetAttributeList: get definition size and member count
  // CIP Template Object (0x6C) attributes (per libplctag):
  // - Attribute 4 = Definition Size (UDINT, 4 bytes) - size in 32-bit WORDS (used for string offset calc)
  // - Attribute 5 = Instance Size (UDINT, 4 bytes) - structure size in bytes on the wire
  // - Attribute 2 = Member Count (UINT, 2 bytes) - number of structure members
  // - Attribute 1 = Handle/Type (UINT, 2 bytes) - structure type/handle
  // Request in same order as libplctag for compatibility
  const attributes = joinBytes([
    encodeUint(4, 2), // Number of attributes
    encodeUint(4, 2), // Attribute 4: Definition Size (32-bit words) - for libplctag formula
    encodeUint(5, 2), // Attribute 5: Instance Size (bytes)
    encodeUint(2, 2), // Attribute 2: Member Count
    encodeUint(1, 2), // Attribute 1: Handle/Type
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
 * Uses class/instance path with offset (words) and byte count
 */
function buildReadTemplateRequest(
  cip: Cip,
  templateId: number,
  wordOffset: number,
  bytesToRead: number,
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
  // - Count: Rockwell interprets this as BYTES to read (2 bytes)
  const requestData = joinBytes([
    encodeUint(wordOffset, 4),
    encodeUint(bytesToRead, 2),
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
 * Based on libplctag's approach - requests attributes 4, 5, 2, 1
 */
async function readTemplateAttributes(
  cip: Cip,
  templateId: number,
): Promise<{ definitionSizeWords: number; instanceSizeBytes: number; memberCount: number } | null> {
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

  let definitionSizeWords = 0;
  let instanceSizeBytes = 0;
  let memberCount = 0;

  // GetAttributeList response format:
  //   - Attribute count (2 bytes)
  //   - For each attribute:
  //     - Attribute ID (2 bytes)
  //     - Status (2 bytes) - 0 = success
  //     - Attribute value (if status is 0)

  const attrCount = decodeUint(response.subarray(offset, offset + 2));
  offset += 2;

  log.eip.debug(`Template ${templateId}: parsing ${attrCount} attributes from response`);

  for (let i = 0; i < attrCount; i++) {
    const attrId = decodeUint(response.subarray(offset, offset + 2));
    offset += 2;
    const attrStatus = decodeUint(response.subarray(offset, offset + 2));
    offset += 2;

    log.eip.debug(`Template ${templateId}: attr[${i}] id=${attrId}, status=${attrStatus}`);

    if (attrStatus === 0) {
      if (attrId === 4) {
        // Definition Size (UDINT, 4 bytes) - size in 32-bit WORDS
        // This is what libplctag uses for the string offset calculation
        definitionSizeWords = decodeUint(response.subarray(offset, offset + 4));
        log.eip.debug(`Template ${templateId}: attr 4 (definitionSizeWords) = ${definitionSizeWords}`);
        offset += 4;
      } else if (attrId === 5) {
        // Instance Size (UDINT, 4 bytes) - structure size in bytes
        instanceSizeBytes = decodeUint(response.subarray(offset, offset + 4));
        log.eip.debug(`Template ${templateId}: attr 5 (instanceSizeBytes) = ${instanceSizeBytes}`);
        offset += 4;
      } else if (attrId === 2) {
        // Member Count (UINT, 2 bytes) - number of structure members
        memberCount = decodeUint(response.subarray(offset, offset + 2));
        log.eip.debug(`Template ${templateId}: attr 2 (memberCount) = ${memberCount}`);
        offset += 2;
      } else if (attrId === 1) {
        // Handle/Type (UINT, 2 bytes) - structure type/handle, skip
        log.eip.debug(`Template ${templateId}: attr 1 (handle) = ${decodeUint(response.subarray(offset, offset + 2))}`);
        offset += 2;
      }
    } else {
      // Attribute failed, skip based on expected size
      // Attributes 4 and 5 are 4 bytes, attributes 1 and 2 are 2 bytes
      if (attrId === 4 || attrId === 5) {
        log.eip.debug(`Template ${templateId}: attr ${attrId} failed (status=${attrStatus}), skipping 4 bytes`);
        // Don't advance offset for failed attributes - no value returned
      } else {
        log.eip.debug(`Template ${templateId}: attr ${attrId} failed (status=${attrStatus}), skipping 2 bytes`);
        // Don't advance offset for failed attributes - no value returned
      }
    }
  }

  log.eip.debug(`Template ${templateId} attributes: sizeWords=${definitionSizeWords}, instanceBytes=${instanceSizeBytes}, memberCount=${memberCount}`);
  return { definitionSizeWords, instanceSizeBytes, memberCount };
}

/**
 * Read the template definition data with exact byte count
 *
 * Template definition structure:
 * - Member definitions: N * 8 bytes (where N includes hidden members)
 * - Type encoding: variable length
 * - String data: template name + member names (null-terminated)
 */
async function readTemplateDefinitionBySize(
  cip: Cip,
  templateId: number,
  totalBytes: number,
): Promise<Uint8Array | null> {
  // Template read for Rockwell controllers:
  // Despite CIP spec saying "words", Rockwell interprets the count as BYTES
  // Offset is still in 32-bit words, but count is bytes to read
  const maxBytesPerRead = 480;  // Safe limit for unconnected messaging
  const allData: Uint8Array[] = [];
  let byteOffset = 0;

  log.eip.debug(`Template ${templateId}: reading ${totalBytes} bytes of definition data`);

  while (byteOffset < totalBytes) {
    // Offset is in 32-bit words
    const wordOffset = Math.floor(byteOffset / 4);
    const bytesRemaining = totalBytes - byteOffset;
    const bytesToRead = Math.min(maxBytesPerRead, bytesRemaining);

    const request = buildReadTemplateRequest(cip, templateId, wordOffset, bytesToRead);
    const response = await sendRaw(cip, request);

    const status = response[42];
    const extStatusSize = response[43];

    // Status 0x00 = success, 0x06 = partial (more data available)
    if (status !== 0x00 && status !== 0x06) {
      if (byteOffset === 0) {
        log.eip.warn(`Template ${templateId} definition read failed: status=0x${status.toString(16)}`);
        return null;
      }
      break;
    }

    const dataOffset = 44 + extStatusSize * 2;
    const chunk = response.subarray(dataOffset);

    if (chunk.length === 0) {
      break;  // No more data
    }

    allData.push(new Uint8Array(chunk)); // Copy the chunk
    byteOffset += chunk.length;

    log.eip.debug(`Template ${templateId}: read chunk of ${chunk.length} bytes at offset ${byteOffset - chunk.length}`);

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

  log.eip.debug(`Template ${templateId}: total definition data = ${data.length} bytes`);

  if (data.length === 0) {
    log.eip.warn(`Template ${templateId} definition read returned empty data`);
    return null;
  }

  return data;
}

/**
 * Find the actual start of string data in template definition
 *
 * Rockwell templates have TYPE ENCODING data between member definitions and strings.
 * The string data format is: "TEMPLATE_NAME:ver:rev;MEMBER1\0MEMBER2\0..."
 *
 * We scan through the ENTIRE definition data to find the template name pattern.
 * The template name always contains ':' (version info) and ends with ';'.
 * This approach is more robust than relying on calculated offsets which can be wrong.
 */
function findStringDataStart(data: Uint8Array): number {
  // Scan from the beginning to find the template name pattern
  // Template name formats:
  // 1. I/O modules: "AB:1734_IE4:C:0;" (has colons for vendor:module:ver:rev)
  // 2. AOIs/UDTs: "Analog_Input;" (just name, no version info)
  // The key is finding the semicolon at the end of the template name

  // Scan for semicolon which ends the template name
  for (let i = 10; i < data.length - 2; i++) { // Start at 10 to skip any header bytes
    if (data[i] === 0x3b) { // semicolon

      // Scan backwards to find the start of the template name
      let start = i - 1;
      let colonCount = 0;
      let hasUppercase = false;
      let hasUnderscore = false;
      let letterCount = 0;

      while (start >= 0) {
        const b = data[start];
        // Valid template name chars: letters, digits, underscore, colon, dash
        const isValidChar = (b >= 0x41 && b <= 0x5a) || // A-Z
                            (b >= 0x61 && b <= 0x7a) || // a-z
                            (b >= 0x30 && b <= 0x39) || // 0-9
                            b === 0x5f ||               // underscore
                            b === 0x2d ||               // dash
                            b === 0x3a;                 // colon

        if (b >= 0x41 && b <= 0x5a) { hasUppercase = true; letterCount++; }
        if (b >= 0x61 && b <= 0x7a) letterCount++;
        if (b === 0x5f) hasUnderscore = true;
        if (b === 0x3a) colonCount++;

        if (!isValidChar) {
          start++; // Move forward to the first valid char
          break;
        }
        start--;
      }

      // Ensure start is valid
      if (start < 0) start = 0;

      // Verify this looks like a valid template name:
      // - At least 3 characters long
      // - Contains at least 2 letters (to avoid matching random byte sequences)
      // - Contains uppercase OR underscore (common in Rockwell names)
      const nameSlice = data.subarray(start, i + 1);
      const nameStr = new TextDecoder().decode(nameSlice);

      // Must be long enough and look like a real name
      // AOIs are like "Analog_Input;" - has underscore and uppercase
      // I/O modules are like "AB:1734_IE4:C:0;" - has colons and uppercase
      const isLongEnough = nameStr.length >= 3;
      const looksLikeName = letterCount >= 2 && (hasUppercase || hasUnderscore);

      log.eip.debug(`findStringDataStart: checking semicolon at ${i}, name="${nameStr}", letters=${letterCount}, hasUpper=${hasUppercase}, hasUnderscore=${hasUnderscore}`);

      if (isLongEnough && looksLikeName) {
        log.eip.info(`findStringDataStart: Found template name at offset ${start}: "${nameStr}"`);
        return start;
      }
    }
  }

  log.eip.warn(`findStringDataStart: Could not find string data start in ${data.length} bytes`);
  return -1; // Return -1 to indicate failure
}

/**
 * Parse all strings from template definition data
 *
 * Template string format: "TEMPLATE_NAME:ver:rev;[TYPE_ENCODING]MEMBER1\0MEMBER2\0..."
 *
 * The data structure after the member definitions section contains:
 * - Template name ending with ';'
 * - Type encoding data (NO NULL BYTES)
 * - Member names as null-terminated strings
 *
 * We scan for null-terminated strings that look like valid member names.
 */
function parseTemplateStrings(
  data: Uint8Array,
  stringDataOffset: number,
  memberCount: number,
): { templateName: string; memberNames: string[] } {
  const stringData = data.subarray(stringDataOffset);
  const decoder = new TextDecoder();
  let templateName = "";

  // First, find the semicolon that ends the template name
  let semicolonPos = -1;
  for (let i = 0; i < stringData.length && i < 200; i++) {
    if (stringData[i] === 0x3b) { // semicolon
      semicolonPos = i;
      break;
    }
  }

  if (semicolonPos === -1) {
    log.eip.warn(`parseTemplateStrings: No semicolon found in first 200 bytes`);
    return { templateName: "", memberNames: [] };
  }

  // Extract template name (everything before semicolon)
  const fullTemplateName = decoder.decode(stringData.subarray(0, semicolonPos));
  // Template name format might be "NAME:version:rev" - extract just the name
  const colonIdx = fullTemplateName.indexOf(":");
  templateName = colonIdx > 0 ? fullTemplateName.substring(0, colonIdx) : fullTemplateName;

  log.eip.debug(`parseTemplateStrings: template name "${templateName}", semicolon at ${semicolonPos}`);

  // Scan for all null-terminated strings that look like valid member names
  // Member names are typically:
  // - 1-40 characters long
  // - Start with letter or underscore
  // - Contain only letters, digits, underscores
  // - Are null-terminated
  const memberNames: string[] = [];
  const seenNames = new Set<string>();

  // Start scanning from just after the semicolon
  let searchStart = semicolonPos + 1;

  // Scan through the data looking for null bytes that terminate strings
  let currentStart = searchStart;
  for (let i = searchStart; i < stringData.length; i++) {
    if (stringData[i] === 0) { // null byte found
      if (i > currentStart) {
        // Extract the string
        const str = decoder.decode(stringData.subarray(currentStart, i));

        // Check if it looks like a valid member name
        const isValidName =
          str.length >= 1 &&
          str.length <= 50 &&
          /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(str) &&
          !str.includes(";") && // Not a template name
          !seenNames.has(str); // Not a duplicate

        if (isValidName) {
          memberNames.push(str);
          seenNames.add(str);

          // Stop if we have enough names
          if (memberNames.length >= memberCount) {
            break;
          }
        }
      }
      currentStart = i + 1;
    }
  }

  log.eip.debug(`parseTemplateStrings: Found ${memberNames.length} unique member names (expected ${memberCount})`);

  return { templateName, memberNames };
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

  const { definitionSizeWords, instanceSizeBytes, memberCount } = attrs;

  // Calculate total definition size from attribute 4
  // definitionSizeWords is in 32-bit words, so multiply by 4 for bytes
  // Add extra bytes for member name strings - Rockwell templates often have
  // member names that extend beyond the definitionSize
  const baseDefinitionBytes = definitionSizeWords * 4;
  // Estimate additional bytes needed for member name strings
  // Average member name is ~15 chars + null terminator
  const estimatedNameBytes = memberCount * 16;
  const totalDefinitionBytes = baseDefinitionBytes + estimatedNameBytes;

  log.eip.debug(`Template ${templateId}: sizeWords=${definitionSizeWords}, baseBytes=${baseDefinitionBytes}, estimatedTotal=${totalDefinitionBytes}, visibleMembers=${memberCount}`);

  if (totalDefinitionBytes <= 0) {
    log.eip.warn(`Template ${templateId}: invalid definition size (${totalDefinitionBytes} bytes)`);
    const template: Template = {
      templateId,
      name: `Template_${templateId}`,
      structureSize: instanceSizeBytes,
      memberCount: 0,
      members: [],
    };
    templateCache.set(templateId, template);
    return template;
  }

  // Step 2: Read template definition
  // Use definitionSizeWords to calculate exact read size
  const definition = await readTemplateDefinitionBySize(cip, templateId, totalDefinitionBytes);
  if (!definition) {
    // Return a minimal template if we can't read the definition
    const template: Template = {
      templateId,
      name: `Template_${templateId}`,
      structureSize: instanceSizeBytes,
      memberCount,
      members: [],
    };
    templateCache.set(templateId, template);
    return template;
  }

  // Step 3: Find where strings actually start by scanning the definition data
  // The string section starts with "TEMPLATE_NAME:ver:rev;"
  // This is more reliable than using calculated offsets
  const stringDataOffset = findStringDataStart(definition);

  if (stringDataOffset < 0) {
    log.eip.warn(`Template ${templateId}: Could not find string data, returning minimal template`);
    const template: Template = {
      templateId,
      name: `Template_${templateId}`,
      structureSize: instanceSizeBytes,
      memberCount,
      members: [],
    };
    templateCache.set(templateId, template);
    return template;
  }

  log.eip.info(`Template ${templateId}: found string data at offset ${stringDataOffset}`);

  // Log the bytes at the string data start for debugging
  const previewBytes = definition.subarray(stringDataOffset, Math.min(stringDataOffset + 80, definition.length));
  const previewAscii = new TextDecoder().decode(previewBytes).replace(/[^\x20-\x7e]/g, '.');
  log.eip.debug(`Template ${templateId}: string data ascii="${previewAscii}"`);

  // Calculate actual member count from where strings start
  // Member definitions are 8 bytes each, starting at offset 0
  const actualMemberCountFromOffset = Math.floor(stringDataOffset / 8);
  log.eip.info(`Template ${templateId}: actualMembers=${actualMemberCountFromOffset}, visibleMembers=${memberCount}`);

  // Step 4: Parse member definitions
  // Member definitions (8 bytes each):
  // - Array info (2 bytes)
  // - Type code (2 bytes)
  // - Offset (4 bytes)
  const memberDefs: Array<{
    arrayInfo: number;
    typeCode: number;
    offset: number;
  }> = [];

  let offset = 0;
  // Use the offset-based member count since it's more accurate
  const memberDefsToRead = actualMemberCountFromOffset;
  for (let i = 0; i < memberDefsToRead && offset + 8 <= stringDataOffset; i++) {
    const arrayInfo = decodeUint(definition.subarray(offset, offset + 2));
    const typeCode = decodeUint(definition.subarray(offset + 2, offset + 4));
    const memberOffset = decodeUint(definition.subarray(offset + 4, offset + 8));
    memberDefs.push({ arrayInfo, typeCode, offset: memberOffset });
    offset += 8;
  }

  log.eip.info(`Template ${templateId}: read ${memberDefs.length} member definitions`);

  // Log first few member definitions for debugging
  const defCount = Math.min(memberDefs.length, 8);
  for (let i = 0; i < defCount; i++) {
    const def = memberDefs[i];
    const rawTypeCode = def.typeCode;
    const isStruct = (rawTypeCode & 0x8000) !== 0;
    const typeCode = rawTypeCode & 0x0fff;
    const typeInfo = TYPE_INFO[typeCode];
    const typeName = isStruct ? "STRUCT" : (typeInfo?.name || "UNKNOWN");
    log.eip.debug(`  def[${i}]: arrayInfo=0x${def.arrayInfo.toString(16)}, typeCode=0x${rawTypeCode.toString(16)} (${typeName}), offset=${def.offset}`);
  }

  // Parse strings - split by null bytes
  // Pass memberCount (from offset) so we can calculate type encoding size
  const { templateName, memberNames } = parseTemplateStrings(definition, stringDataOffset, actualMemberCountFromOffset);

  log.eip.debug(`Template ${templateId}: parsed templateName="${templateName}", ${memberNames.length} member names`);
  log.eip.debug(`Template ${templateId}: member names (first 10): ${memberNames.slice(0, 10).map((n, i) => `[${i}]${n}`).join(", ")}`);

  // Log name/typeCode pairing for diagnosing misalignment
  if (memberDefs.length !== memberNames.length) {
    log.eip.warn(`Template ${templateId}: COUNT MISMATCH - ${memberDefs.length} definitions but ${memberNames.length} names`);
    // Log all pairings to help diagnose
    const pairCount = Math.max(memberDefs.length, memberNames.length);
    for (let i = 0; i < Math.min(pairCount, 15); i++) {
      const def = memberDefs[i];
      const name = memberNames[i] || "(missing name)";
      if (def) {
        const typeCode = def.typeCode & 0x0fff;
        const typeInfo = TYPE_INFO[typeCode];
        const datatype = (def.typeCode & 0x8000) ? "STRUCT" : (typeInfo?.name || "UNKNOWN");
        log.eip.warn(`  [${i}] name="${name}" typeCode=0x${def.typeCode.toString(16)} -> ${datatype}`);
      } else {
        log.eip.warn(`  [${i}] name="${name}" (no definition)`);
      }
    }
  }

  // Build member list - pair definitions with names in order
  const members: TemplateMember[] = [];
  let fallbackCount = 0;

  for (let i = 0; i < memberDefs.length; i++) {
    const def = memberDefs[i];
    let name = memberNames[i];

    if (!name) {
      // Fallback name - this indicates we didn't read enough template data
      name = `_member${i}`;
      fallbackCount++;
    }

    // Skip hidden members (starting with ZZZZZ or __)
    // These are Rockwell internal members that shouldn't be exposed
    if (name.startsWith("ZZZZZ") || name.startsWith("__")) {
      log.eip.debug(`Template ${templateId}: skipping hidden member at index ${i}: "${name}"`);
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

    // Log member parsing for debugging
    log.eip.debug(`Template ${templateId}: member[${i}] "${name}" typeCode=0x${def.typeCode.toString(16)} -> ${datatype}`);

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
    name: templateName || `Template_${templateId}`,
    structureSize: instanceSizeBytes, // Structure size in bytes (from attribute 5)
    memberCount,
    members,
  };

  if (fallbackCount > 0) {
    log.eip.warn(`Template ${templateId} "${template.name}": ${fallbackCount} members used fallback names (not enough data read)`);
    log.eip.warn(`  Read ${definition.length} bytes, found ${memberNames.length} names for ${memberDefs.length} definitions`);
  }
  log.eip.info(`Template ${templateId} "${template.name}": ${members.length} readable members (memberCount: ${memberCount})`);
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
    } else if (member.isStruct && !member.templateId) {
      // Struct without template ID - can't expand, skip it
      log.eip.warn(`  Struct member ${member.name} has no templateId (typeCode=0x${member.typeCode.toString(16)}), skipping`);
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
