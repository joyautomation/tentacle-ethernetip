/**
 * CIP request/response handling
 */

import { log } from "../utils/logger.ts";
import {
  decodeUint,
  encodeUint,
  joinBytes,
} from "./encode.ts";
import {
  buildCommonPacketFormat,
  buildHeader,
  Cip,
  sendRaw,
} from "./cip.ts";
import { encapsulation, service } from "./services.ts";
import { buildRequestPath, encodeSymbolicPath } from "./dataTypes.ts";
import { getServiceStatusMessage } from "./status.ts";
import { MESSAGE_ROUTER_PATH } from "./constants.ts";
import { encodeEpath } from "./dataTypes.ts";

/**
 * CIP response structure
 */
export type CipResponse = {
  service: number;
  status: number;
  extendedStatus: number[];
  data: Uint8Array;
  success: boolean;
};

/**
 * Parse a CIP response from raw bytes
 */
function parseCipResponse(raw: Uint8Array, offset: number): CipResponse {
  const serviceResponse = raw[offset];
  const status = raw[offset + 2];
  const extendedStatusSize = raw[offset + 3];

  const extendedStatus: number[] = [];
  for (let i = 0; i < extendedStatusSize; i++) {
    extendedStatus.push(
      decodeUint(raw.subarray(offset + 4 + i * 2, offset + 6 + i * 2)),
    );
  }

  const dataOffset = offset + 4 + extendedStatusSize * 2;
  const data = raw.subarray(dataOffset);

  return {
    service: serviceResponse & 0x7f, // Remove reply bit
    status,
    extendedStatus,
    data,
    success: status === 0,
  };
}

/**
 * Send a connected CIP request (via SendUnitData)
 */
export async function sendConnectedRequest(
  cip: Cip,
  params: {
    service: Uint8Array;
    classCode?: Uint8Array;
    instance?: Uint8Array;
    attribute?: Uint8Array;
    requestData?: Uint8Array;
    requestPath?: Uint8Array;
  },
): Promise<CipResponse> {
  if (!cip.targetIsConnected || !cip.targetCid) {
    throw new Error("CIP connection not established");
  }

  const sequence = cip.sequence.next();

  // Build request path
  let path: Uint8Array;
  if (params.requestPath) {
    path = params.requestPath;
  } else if (params.classCode && params.instance) {
    path = buildRequestPath(
      params.classCode,
      params.instance,
      params.attribute,
    );
  } else {
    path = new Uint8Array();
  }

  // CIP message: sequence + service + path + data
  const cipMessage = joinBytes([
    encodeUint(sequence, 2),
    params.service,
    path,
    params.requestData ?? new Uint8Array(),
  ]);

  const cpf = buildCommonPacketFormat({
    timeout: new Uint8Array([0x0a, 0x00]),
    addressType: new Uint8Array([0xa1, 0x00]), // Connected address
    message: cipMessage,
    messageType: new Uint8Array([0xb1, 0x00]), // Connected data
    addressData: cip.targetCid,
  });

  const header = buildHeader({
    command: encapsulation.sendUnitData,
    session: cip.session,
    context: cip.context,
    option: cip.option,
    length: cpf.length,
  });

  const request = joinBytes([header, cpf]);
  const response = await sendRaw(cip, request);

  // Parse response - CIP data starts at offset 46 for connected
  const cipResponse = parseCipResponse(response, 46);

  if (!cipResponse.success) {
    log.eip.warn(
      `CIP request failed: ${getServiceStatusMessage(cipResponse.status)} (0x${cipResponse.status.toString(16)})`,
    );
  }

  return cipResponse;
}

/**
 * Send an unconnected CIP request (via SendRRData)
 */
export async function sendUnconnectedRequest(
  cip: Cip,
  params: {
    service: Uint8Array;
    classCode?: Uint8Array;
    instance?: Uint8Array;
    attribute?: Uint8Array;
    requestData?: Uint8Array;
    requestPath?: Uint8Array;
  },
): Promise<CipResponse> {
  // Build request path
  let path: Uint8Array;
  if (params.requestPath) {
    path = params.requestPath;
  } else if (params.classCode && params.instance) {
    path = buildRequestPath(
      params.classCode,
      params.instance,
      params.attribute,
    );
  } else {
    path = new Uint8Array();
  }

  // CIP message: service + path + data
  const cipMessage = joinBytes([
    params.service,
    path,
    params.requestData ?? new Uint8Array(),
  ]);

  const cpf = buildCommonPacketFormat({
    timeout: new Uint8Array([0x0a, 0x00]),
    addressType: new Uint8Array([0x00, 0x00]), // Null address
    message: cipMessage,
    messageType: new Uint8Array([0xb2, 0x00]), // Unconnected data
    addressData: null,
  });

  const header = buildHeader({
    command: encapsulation.sendRRData,
    session: cip.session,
    context: cip.context,
    option: cip.option,
    length: cpf.length,
  });

  const request = joinBytes([header, cpf]);
  const response = await sendRaw(cip, request);

  // Parse response - CIP data starts at offset 40 for unconnected
  const cipResponse = parseCipResponse(response, 40);

  if (!cipResponse.success) {
    log.eip.warn(
      `CIP request failed: ${getServiceStatusMessage(cipResponse.status)}`,
    );
  }

  return cipResponse;
}

/**
 * Read a tag from a Rockwell controller
 * Uses unconnected messaging (works with all PLCs)
 */
export async function readTag(
  cip: Cip,
  tagName: string,
  elementCount: number = 1,
): Promise<CipResponse> {
  const tagPath = encodeSymbolicPath(tagName);

  // ReadTag request data: element count (2 bytes)
  const requestData = encodeUint(elementCount, 2);

  // Build path with length prefix
  const pathWithLength = joinBytes([
    encodeUint(Math.floor(tagPath.length / 2), 1),
    tagPath,
  ]);

  // Use unconnected messaging - works without ForwardOpen
  return await sendUnconnectedRequest(cip, {
    service: service.readTag,
    requestPath: pathWithLength,
    requestData,
  });
}

/**
 * Write a tag to a Rockwell controller
 * Uses unconnected messaging (works with all PLCs)
 */
export async function writeTag(
  cip: Cip,
  tagName: string,
  dataType: number,
  value: Uint8Array,
  elementCount: number = 1,
): Promise<CipResponse> {
  const tagPath = encodeSymbolicPath(tagName);

  // WriteTag request data: data type (2 bytes) + element count (2 bytes) + data
  const requestData = joinBytes([
    encodeUint(dataType, 2),
    encodeUint(elementCount, 2),
    value,
  ]);

  // Build path with length prefix
  const pathWithLength = joinBytes([
    encodeUint(Math.floor(tagPath.length / 2), 1),
    tagPath,
  ]);

  // Use unconnected messaging - works without ForwardOpen
  return await sendUnconnectedRequest(cip, {
    service: service.writeTag,
    requestPath: pathWithLength,
    requestData,
  });
}

/**
 * Read a single attribute from any CIP device
 */
export async function getAttributeSingle(
  cip: Cip,
  classCodeValue: number,
  instance: number,
  attribute: number,
): Promise<CipResponse> {
  return await sendConnectedRequest(cip, {
    service: service.getAttributeSingle,
    classCode: encodeUint(classCodeValue, 1),
    instance: encodeUint(instance, 2),
    attribute: encodeUint(attribute, 1),
  });
}

/**
 * Write a single attribute to any CIP device
 */
export async function setAttributeSingle(
  cip: Cip,
  classCodeValue: number,
  instance: number,
  attribute: number,
  data: Uint8Array,
): Promise<CipResponse> {
  return await sendConnectedRequest(cip, {
    service: service.setAttributeSingle,
    classCode: encodeUint(classCodeValue, 1),
    instance: encodeUint(instance, 2),
    attribute: encodeUint(attribute, 1),
    requestData: data,
  });
}

/**
 * Result of a batch read operation
 */
export type BatchReadResult = {
  tagName: string;
  response: CipResponse;
};

/**
 * Build a single ReadTag request for embedding in Multiple Service Request
 * Returns the raw request without encapsulation
 */
function buildReadTagRequest(tagName: string, elementCount: number = 1): Uint8Array {
  const tagPath = encodeSymbolicPath(tagName);
  const pathWithLength = joinBytes([
    encodeUint(Math.floor(tagPath.length / 2), 1),
    tagPath,
  ]);
  const requestData = encodeUint(elementCount, 2);

  return joinBytes([
    service.readTag,
    pathWithLength,
    requestData,
  ]);
}

/**
 * Parse individual responses from a Multiple Service Response
 */
function parseMultipleServiceResponse(
  data: Uint8Array,
  tagNames: string[],
): BatchReadResult[] {
  const results: BatchReadResult[] = [];

  // First 2 bytes: service count
  const serviceCount = decodeUint(data.subarray(0, 2));

  if (serviceCount !== tagNames.length) {
    log.eip.warn(`Multiple service response count mismatch: expected ${tagNames.length}, got ${serviceCount}`);
  }

  // Next 2 bytes per service: offset array
  const offsets: number[] = [];
  for (let i = 0; i < serviceCount; i++) {
    offsets.push(decodeUint(data.subarray(2 + i * 2, 4 + i * 2)));
  }

  // Parse each individual response
  for (let i = 0; i < serviceCount; i++) {
    const offset = offsets[i];
    const tagName = tagNames[i] || `unknown_${i}`;

    // Individual response format:
    // [0] service reply (0xCC where CC is original service | 0x80)
    // [1] reserved (0x00)
    // [2] status
    // [3] extended status size (in words)
    // [4+] extended status (if any), then data

    const serviceReply = data[offset];
    const status = data[offset + 2];
    const extendedStatusSize = data[offset + 3];

    const extendedStatus: number[] = [];
    for (let j = 0; j < extendedStatusSize; j++) {
      extendedStatus.push(
        decodeUint(data.subarray(offset + 4 + j * 2, offset + 6 + j * 2))
      );
    }

    // Calculate where data starts and ends
    const dataStart = offset + 4 + extendedStatusSize * 2;
    // Data ends at next offset or end of data
    const dataEnd = i + 1 < serviceCount ? offsets[i + 1] : data.length;
    const responseData = data.subarray(dataStart, dataEnd);

    results.push({
      tagName,
      response: {
        service: serviceReply & 0x7f,
        status,
        extendedStatus,
        data: responseData,
        success: status === 0,
      },
    });
  }

  return results;
}

// Maximum packet size for unconnected messaging (conservative estimate)
// Actual limit depends on PLC, but 500 bytes is safe for most
const MAX_UNCONNECTED_PACKET_SIZE = 480;

/**
 * Estimate the size of a Multiple Service Request packet
 */
function estimatePacketSize(requests: Uint8Array[]): number {
  // Header: encapsulation (24) + CPF (16) + service (1) + path (4)
  const overhead = 45;
  // Request data: count (2) + offsets (2 per request) + request data
  const dataSize = 2 + (requests.length * 2) + requests.reduce((sum, r) => sum + r.length, 0);
  return overhead + dataSize;
}

/**
 * Execute a single batch read request
 */
async function executeBatchRead(
  cip: Cip,
  tagNames: string[],
  requests: Uint8Array[],
): Promise<BatchReadResult[]> {
  // Calculate offsets - start after service count (2 bytes) and offset array (2 bytes per request)
  const headerSize = 2 + (requests.length * 2);
  const offsets: number[] = [];
  let currentOffset = headerSize;

  for (const request of requests) {
    offsets.push(currentOffset);
    currentOffset += request.length;
  }

  // Build Multiple Service Request data
  const offsetBytes = offsets.map(o => encodeUint(o, 2));
  const requestData = joinBytes([
    encodeUint(requests.length, 2),
    ...offsetBytes,
    ...requests,
  ]);

  // Path to Message Router (class 0x02, instance 1)
  const requestPath = encodeEpath(MESSAGE_ROUTER_PATH, true);

  // Send using unconnected messaging
  const cipMessage = joinBytes([
    service.multipleServiceRequest,
    requestPath,
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

  const request = joinBytes([header, cpf]);
  const response = await sendRaw(cip, request);

  // Parse outer response - CIP data starts at offset 40 for unconnected
  const outerResponse = parseCipResponse(response, 40);

  if (!outerResponse.success) {
    log.eip.warn(
      `Multiple Service Request failed: ${getServiceStatusMessage(outerResponse.status)}`
    );
    return tagNames.map(tagName => ({
      tagName,
      response: outerResponse,
    }));
  }

  return parseMultipleServiceResponse(outerResponse.data, tagNames);
}

/**
 * Read multiple tags in a single CIP request using Multiple Service Packet
 * Automatically chunks large requests to fit within packet size limits
 *
 * @param cip - CIP connection
 * @param tagNames - Array of tag names to read
 * @param elementCount - Number of elements per tag (default 1)
 * @returns Array of results, one per tag
 */
export async function readMultipleTags(
  cip: Cip,
  tagNames: string[],
  elementCount: number = 1,
): Promise<BatchReadResult[]> {
  if (tagNames.length === 0) {
    return [];
  }

  // For a single tag, just use regular readTag
  if (tagNames.length === 1) {
    const response = await readTag(cip, tagNames[0], elementCount);
    return [{ tagName: tagNames[0], response }];
  }

  // Build all requests first
  const allRequests: Uint8Array[] = tagNames.map(name =>
    buildReadTagRequest(name, elementCount)
  );

  // Chunk requests to fit within packet size limit
  const chunks: { names: string[]; requests: Uint8Array[] }[] = [];
  let currentChunk: { names: string[]; requests: Uint8Array[] } = { names: [], requests: [] };

  for (let i = 0; i < tagNames.length; i++) {
    const testChunk = {
      names: [...currentChunk.names, tagNames[i]],
      requests: [...currentChunk.requests, allRequests[i]],
    };

    if (estimatePacketSize(testChunk.requests) > MAX_UNCONNECTED_PACKET_SIZE && currentChunk.requests.length > 0) {
      // Current chunk is full, start a new one
      chunks.push(currentChunk);
      currentChunk = { names: [tagNames[i]], requests: [allRequests[i]] };
    } else {
      currentChunk = testChunk;
    }
  }

  // Don't forget the last chunk
  if (currentChunk.requests.length > 0) {
    chunks.push(currentChunk);
  }

  log.eip.debug(`Batch read: ${tagNames.length} tags split into ${chunks.length} chunks`);

  // Execute all chunks and combine results
  const allResults: BatchReadResult[] = [];
  for (const chunk of chunks) {
    const results = await executeBatchRead(cip, chunk.names, chunk.requests);
    allResults.push(...results);
  }

  return allResults;
}
