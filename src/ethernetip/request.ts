/**
 * CIP request/response handling
 */

import { log } from "../utils/logger.ts";
import {
  bufferToHex,
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
