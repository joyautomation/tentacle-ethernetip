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
  // CIP Template Object (0x6C) attributes:
  // - Attribute 1 = Object Definition Size (UDINT, 4 bytes) - size of template data
  // - Attribute 2 = Structure Handle (UINT, 2 bytes) - some controllers put member count here
  // - Attribute 3 = Template Member Count (UINT, 2 bytes) - per CIP spec
  // Request all three for compatibility with different controllers
  const attributes = joinBytes([
    encodeUint(3, 2), // Number of attributes
    encodeUint(1, 2), // Attribute 1: Object Definition Size
    encodeUint(2, 2), // Attribute 2: Structure Handle (might be member count on some)
    encodeUint(3, 2), // Attribute 3: Template Member Count
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
        // Object Definition Size (UDINT, 4 bytes) - size of template data in bytes
        structureHandle = decodeUint(response.subarray(offset, offset + 4));
        offset += 4;
      } else if (attrId === 2) {
        // Structure Handle (UDINT, 4 bytes) - skip
        offset += 4;
      } else if (attrId === 3) {
        // Template Member Count (UINT, 2 bytes) - per CIP spec, this is authoritative
        memberCount = decodeUint(response.subarray(offset, offset + 2));
        offset += 2;
      }
    }
  }

  log.eip.debug(`Template ${templateId} attributes: definitionSize=${structureHandle}, memberCount=${memberCount}`);
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
  // Calculate needed size: 8 bytes per member definition + ~40 bytes per name + overhead
  // IMPORTANT: memberCount doesn't include hidden members (ZZZZZ*, __*) which ARE in the
  // definition data. We multiply by 2.5 to account for potentially many hidden members.
  // Rockwell AOIs/UDTs can have long member names (up to 40 chars).
  const estimatedBytes = Math.ceil(memberCount * 2.5) * 8 + Math.ceil(memberCount * 2.5) * 40 + 500;

  // Template read for Rockwell controllers:
  // Despite CIP spec saying "words", Rockwell interprets the count as BYTES
  // Offset is still in 32-bit words, but count is bytes to read
  const maxBytesPerRead = 480;  // Safe limit for unconnected messaging
  const allData: Uint8Array[] = [];
  let byteOffset = 0;

  while (byteOffset < estimatedBytes) {
    // Offset is in 32-bit words
    const wordOffset = Math.floor(byteOffset / 4);
    const bytesRemaining = estimatedBytes - byteOffset;
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

    allData.push(chunk);
    byteOffset += chunk.length;

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
 * Parse template name which is terminated by semicolon (;) or null byte
 * Rockwell template format: "name:version:rev;member1\x00member2\x00..."
 */
function parseTemplateName(data: Uint8Array, offset: number): { name: string; length: number } {
  let end = offset;
  // Find semicolon or null byte, whichever comes first
  while (end < data.length && data[end] !== 0 && data[end] !== 0x3b) {
    end++;
  }
  const fullName = new TextDecoder().decode(data.subarray(offset, end));
  // Extract just the template name (before first colon) for display
  const colonIdx = fullName.indexOf(':');
  const name = colonIdx > 0 ? fullName.substring(0, colonIdx) : fullName;
  // Length includes the terminator (semicolon or null)
  return { name, length: end - offset + 1 };
}

/**
 * Find the start of string data in template definition
 *
 * The challenge: Rockwell templates include hidden members (ZZZZZ*, __*) in the
 * member definitions that are NOT counted in memberCount. Additionally, there's
 * often a "type encoding" section between member definitions and the actual
 * string data (containing patterns like "AECHAECH..." or "iEBEBEB...").
 *
 * Template string data format: "template_name:version:rev;member1\x00member2\x00..."
 *
 * We find the string data by looking for the semicolon that ends the template name,
 * which should be followed by a letter or underscore (start of first member name).
 * Then we scan backwards to find the start of the template name.
 */
function findStringDataOffset(data: Uint8Array, estimatedOffset: number): number {
  const maxScan = Math.min(estimatedOffset + 1024, data.length - 5);

  // Strategy: Find semicolon followed by a valid member name start (letter, underscore, or ZZZZZ)
  // This pattern marks: "template:ver:rev;FirstMemberName\0..."
  for (let i = estimatedOffset; i < maxScan; i++) {
    if (data[i] === 0x3b) { // semicolon
      const nextByte = data[i + 1];
      // Check if next byte starts a member name (letter, underscore, or digit for ZZZZZ...)
      const isLetter = (nextByte >= 0x41 && nextByte <= 0x5a) || (nextByte >= 0x61 && nextByte <= 0x7a);
      const isUnderscore = nextByte === 0x5f;
      const isDigit = nextByte >= 0x30 && nextByte <= 0x39;

      if (isLetter || isUnderscore || isDigit) {
        // Found the end of template name! Now scan backwards to find the start.
        // Template names are like "FBD_ONESHOT:1:0" - alphanumeric with underscores and colons
        // But they DON'T contain the type encoding letters pattern (pure alternating A/B/C/E/H/D)

        let start = i - 1;
        // Scan backwards to find the start of the template name
        // Stop when we hit a byte that can't be part of a template name (non-ASCII or null)
        while (start > estimatedOffset) {
          const b = data[start];
          // Valid template name chars: letters, digits, underscore, colon
          const isValidChar = (b >= 0x41 && b <= 0x5a) || // A-Z
                              (b >= 0x61 && b <= 0x7a) || // a-z
                              (b >= 0x30 && b <= 0x39) || // 0-9
                              b === 0x5f ||               // underscore
                              b === 0x3a;                 // colon
          if (!isValidChar) {
            start++; // Move forward to the first valid char
            break;
          }
          start--;
        }

        // Verify this looks like a real template name (should have colons for version)
        const slice = data.subarray(start, i + 1);
        const str = new TextDecoder().decode(slice);
        const colonCount = (str.match(/:/g) || []).length;

        // Real template names have at least 2 colons (name:version:rev;)
        // Type encoding like "AECHAECH" has zero colons
        if (colonCount >= 1) {
          log.eip.debug(`  Found template name at offset ${start} (scanned ${start - estimatedOffset} bytes forward): "${str}"`);
          return start;
        }
      }
    }
  }

  // Fallback: look for the old pattern (letter followed by colon)
  for (let offset = estimatedOffset; offset < maxScan; offset++) {
    const byte = data[offset];
    const isLetter = (byte >= 0x41 && byte <= 0x5a) || (byte >= 0x61 && byte <= 0x7a);

    if (isLetter) {
      // Look for version pattern ":digit:" or ":letter:" within reasonable distance
      for (let i = 1; i < 60 && offset + i + 2 < data.length; i++) {
        if (data[offset + i] === 0x3a) { // colon
          const afterColon = data[offset + i + 1];
          // Check if this looks like version start (letter or digit)
          const isVersionStart = (afterColon >= 0x41 && afterColon <= 0x5a) ||
                                 (afterColon >= 0x61 && afterColon <= 0x7a) ||
                                 (afterColon >= 0x30 && afterColon <= 0x39);
          if (isVersionStart) {
            log.eip.debug(`  Found string data at offset ${offset} via fallback pattern`);
            return offset;
          }
        }
        if (data[offset + i] === 0 || data[offset + i] === 0x3b) {
          break;
        }
        if (data[offset + i] < 0x20 || data[offset + i] > 0x7e) {
          break;
        }
      }
    }
  }

  log.eip.debug(`  Could not find string data start, using estimated offset ${estimatedOffset}`);
  return estimatedOffset;
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
  //
  // IMPORTANT: The memberCount from attributes doesn't include hidden members (ZZZZZ*, __*)
  // but those hidden members ARE in the definition data. We first find where the string
  // data starts, then read ALL member definitions up to that point.

  const estimatedStringOffset = memberCount * 8;
  const actualStringOffset = findStringDataOffset(definition, estimatedStringOffset);

  if (actualStringOffset !== estimatedStringOffset) {
    const hiddenBytes = actualStringOffset - estimatedStringOffset;
    const hiddenCount = Math.floor(hiddenBytes / 8);
    log.eip.debug(`Template ${templateId}: detected ${hiddenCount} hidden members (${hiddenBytes} extra bytes)`);
  }

  // Read ALL member definitions up to the string data start
  const memberDefs: Array<{
    arrayInfo: number;
    typeCode: number;
    offset: number;
  }> = [];

  let offset = 0;
  while (offset + 8 <= actualStringOffset && offset + 8 <= definition.length) {
    const arrayInfo = decodeUint(definition.subarray(offset, offset + 2));
    const typeCode = decodeUint(definition.subarray(offset + 2, offset + 4));
    const memberOffset = decodeUint(definition.subarray(offset + 4, offset + 8));
    memberDefs.push({ arrayInfo, typeCode, offset: memberOffset });
    offset += 8;
  }

  log.eip.debug(`Template ${templateId}: read ${memberDefs.length} member definitions (reported memberCount=${memberCount})`);

  // Move to string data start
  offset = actualStringOffset;

  const templateNameResult = parseTemplateName(definition, offset);
  const templateName = templateNameResult.name;
  offset += templateNameResult.length;

  // Read ALL member names - there may be more names than memberCount due to hidden members
  // The string section contains names for all members including hidden ones (ZZZZZ*, __*)
  // which are in the definition data but not counted in memberCount.
  const memberNames: string[] = [];
  const maxNames = memberDefs.length + 10; // Read names for all member definitions + buffer
  const startOffset = offset;
  while (offset < definition.length && memberNames.length < maxNames) {
    const nameResult = parseNullTerminatedString(definition, offset);
    if (nameResult.str.length > 0) {
      memberNames.push(nameResult.str);
      offset += nameResult.length;
    } else {
      // Empty string (null byte at start) means we hit padding or end of valid data
      log.eip.debug(`Template ${templateId}: string parsing stopped at offset ${offset} (null byte)`);
      break;
    }
  }

  // Check if we hit end of buffer before getting all names
  if (memberNames.length < memberDefs.length && offset >= definition.length) {
    log.eip.warn(`Template ${templateId}: buffer exhausted at ${offset} bytes, only got ${memberNames.length}/${memberDefs.length} names`);
  }

  // Build member list
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

  if (fallbackCount > 0) {
    log.eip.warn(`Template ${templateId} "${templateName}": ${fallbackCount} members used fallback names (not enough data read)`);
    log.eip.warn(`  Read ${definition.length} bytes, found ${memberNames.length} names for ${memberDefs.length} definitions`);
  }
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
