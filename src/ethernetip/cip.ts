/**
 * CIP (Common Industrial Protocol) session and connection management
 */

import { log } from "../utils/logger.ts";
import { cycle } from "../utils/cycle.ts";
import {
  bufferToHex,
  decodeUint,
  encodeUint,
  getRandomBytes,
  joinBytes,
} from "./encode.ts";
import {
  Connection,
  ConnectionConfig,
  closeConnection,
  createConnection,
  readData,
  writeData,
} from "./connection.ts";
import {
  CONNECTION_MANAGER_PATH,
  CONTEXT,
  DEFAULT_RPI,
  LARGE_CONNECTION_SIZE,
  MESSAGE_ROUTER_PATH,
  PRIORITY,
  PROTOCOL_VERSION,
  SMALL_CONNECTION_SIZE,
  TIMEOUT_MULTIPLIER,
  TIMEOUT_TICKS,
  TRANSPORT_CLASS,
} from "./constants.ts";
import { encodeEpath } from "./dataTypes.ts";
import { connectionManager, encapsulation } from "./services.ts";
import { classCode, connectionManagerInstances } from "./objectLibrary.ts";
import { getProductTypeName, getVendorName, VENDORS } from "./status.ts";

/**
 * CIP connection state
 */
export type Cip = {
  connection: Connection;
  session: Uint8Array;
  targetCid: Uint8Array | null;
  targetIsConnected: boolean;
  context: Uint8Array;
  option: number;
  sequence: ReturnType<typeof cycle>;
  protocolVersion: Uint8Array;
  extendedForwardOpen: boolean;
  cid: Uint8Array;
  csn: Uint8Array;
  vid: Uint8Array;
  vsn: Uint8Array;
  identity: DeviceIdentity | null;
};

/**
 * Device identity information from ListIdentity
 */
export type DeviceIdentity = {
  vendor: string;
  vendorId: number;
  productType: string;
  productTypeId: number;
  productCode: number;
  revision: { major: number; minor: number };
  serial: number;
  productName: string;
};

/**
 * Build encapsulation header
 */
function buildHeader(params: {
  command: Uint8Array;
  session: Uint8Array;
  context: Uint8Array;
  option: number;
  length: number;
}): Uint8Array {
  return joinBytes([
    params.command,
    encodeUint(params.length, 2),
    params.session,
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // Status (4 bytes)
    params.context,
    encodeUint(params.option, 4),
  ]);
}

/**
 * Build common packet format
 */
function buildCommonPacketFormat(params: {
  timeout: Uint8Array;
  addressType: Uint8Array | null;
  message: Uint8Array;
  messageType: Uint8Array | null;
  addressData: Uint8Array | null;
}): Uint8Array {
  const addressLength = params.addressData
    ? joinBytes([encodeUint(params.addressData.length, 2), params.addressData])
    : new Uint8Array([0x00, 0x00]);

  return joinBytes([
    new Uint8Array([0x00, 0x00, 0x00, 0x00]), // Interface handle
    params.timeout,
    new Uint8Array([0x02, 0x00]), // Item count: 2
    params.addressType ?? new Uint8Array(),
    addressLength,
    params.messageType ?? new Uint8Array(),
    encodeUint(params.message.length, 2),
    params.message,
  ]);
}

/**
 * Send a raw request and receive response
 */
async function sendRaw(cip: Cip, data: Uint8Array): Promise<Uint8Array> {
  await writeData(cip.connection, data);
  return await readData(cip.connection, 5000);
}

/**
 * Register a session with the device
 */
async function registerSession(cip: Cip): Promise<Uint8Array> {
  const message = joinBytes([cip.protocolVersion, new Uint8Array([0x00, 0x00])]);

  const header = buildHeader({
    command: encapsulation.registerSession,
    session: encodeUint(0, 4),
    context: cip.context,
    option: cip.option,
    length: message.length,
  });

  const request = joinBytes([header, message]);
  const response = await sendRaw(cip, request);

  // Session handle is at bytes 4-8
  const session = response.slice(4, 8);
  log.eip.info(`Registered session: ${decodeUint(session)}`);

  return session;
}

/**
 * Unregister (close) a session
 */
async function unregisterSession(cip: Cip): Promise<void> {
  const header = buildHeader({
    command: encapsulation.unregisterSession,
    session: cip.session,
    context: cip.context,
    option: cip.option,
    length: 0,
  });

  await sendRaw(cip, header);
  log.eip.info(`Unregistered session: ${decodeUint(cip.session)}`);
}

/**
 * Get device identity via ListIdentity
 */
async function listIdentity(cip: Cip): Promise<DeviceIdentity> {
  const header = buildHeader({
    command: encapsulation.listIdentity,
    session: cip.session,
    context: cip.context,
    option: cip.option,
    length: 0,
  });

  const response = await sendRaw(cip, header);

  // Parse identity data starting at offset 26
  const data = response.subarray(26);
  const productNameLength = data[36];

  const vendorId = decodeUint(data.subarray(22, 24));
  const productTypeId = decodeUint(data.subarray(24, 26));

  const identity: DeviceIdentity = {
    vendorId,
    vendor: getVendorName(vendorId),
    productTypeId,
    productType: getProductTypeName(productTypeId),
    productCode: decodeUint(data.subarray(26, 28)),
    revision: {
      major: data[28],
      minor: data[29],
    },
    serial: decodeUint(data.subarray(32, 36)),
    productName: new TextDecoder().decode(
      data.subarray(37, 37 + productNameLength),
    ),
  };

  log.eip.info(
    `Device: ${identity.vendor}, ${identity.productType}, ${identity.productName}`,
  );

  return identity;
}

/**
 * Open a CIP connection (ForwardOpen)
 * Tries Large ForwardOpen first, falls back to standard if not supported
 */
async function forwardOpen(cip: Cip): Promise<void> {
  if (cip.targetIsConnected) return;

  // Try Large ForwardOpen first, fall back to standard if not supported
  const attempts = cip.extendedForwardOpen
    ? [true, false] // Try extended first, then standard
    : [false]; // Only try standard

  for (const useExtended of attempts) {
    const connectionSize = useExtended
      ? LARGE_CONNECTION_SIZE
      : SMALL_CONNECTION_SIZE;
    const initNetParam = 0b0100001000000000;

    const netParams = useExtended
      ? encodeUint((connectionSize & 0xffff) | (initNetParam << 16), 4)
      : encodeUint((connectionSize & 0x01ff) | initNetParam, 2);

    // Request path to Connection Manager (where ForwardOpen service runs)
    const requestPath = encodeEpath(CONNECTION_MANAGER_PATH, true);

    // Connection path to Message Router (target of the connection for ReadTag/WriteTag)
    const connectionPath = encodeEpath(MESSAGE_ROUTER_PATH, true);

    const service = useExtended
      ? connectionManager.largeForwardOpen
      : connectionManager.forwardOpen;

    // ForwardOpen parameters
    const forwardOpenParams = joinBytes([
      PRIORITY,
      TIMEOUT_TICKS,
      new Uint8Array([0x00, 0x00, 0x00, 0x00]), // O->T CID (filled by target)
      cip.cid,
      cip.csn,
      cip.vid,
      cip.vsn,
      TIMEOUT_MULTIPLIER,
      new Uint8Array([0x00, 0x00, 0x00]),
      DEFAULT_RPI,  // O->T RPI (10ms default)
      netParams,
      DEFAULT_RPI,  // T->O RPI (10ms default)
      netParams,
      TRANSPORT_CLASS,
    ]);

    // Full CIP message: [service][request_path][FO_params][connection_path]
    const cipMessage = joinBytes([
      service,
      requestPath,
      forwardOpenParams,
      connectionPath,
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

    // Parse response - check status first
    // CPF starts at offset 24 (after header)
    // CIP response starts at offset 40 (after CPF header)
    const cipStatus = response[42]; // Status byte in CIP reply

    if (cipStatus === 0x08 && useExtended) {
      // Service not supported - try standard ForwardOpen
      log.eip.info("Large ForwardOpen not supported, trying standard ForwardOpen");
      continue;
    }

    if (cipStatus !== 0x00) {
      throw new Error(`ForwardOpen failed with status 0x${cipStatus.toString(16)}`);
    }

    // Parse successful response - target CID is in the CIP data portion
    const cipData = response.subarray(44);
    cip.targetCid = cipData.subarray(0, 4);
    cip.targetIsConnected = true;
    cip.extendedForwardOpen = useExtended;

    log.eip.info(
      `ForwardOpen complete (${useExtended ? "extended" : "standard"}), target CID: ${bufferToHex(cip.targetCid)}`
    );
    return;
  }

  throw new Error("ForwardOpen failed - all attempts exhausted");
}

/**
 * Close a CIP connection (ForwardClose)
 */
async function forwardClose(cip: Cip): Promise<void> {
  if (!cip.targetIsConnected) return;

  // Request path to Connection Manager
  const requestPath = encodeEpath(CONNECTION_MANAGER_PATH, true);

  // Connection path to Message Router (same as ForwardOpen)
  const connectionPath = encodeEpath(MESSAGE_ROUTER_PATH, true);

  // ForwardClose parameters
  const forwardCloseParams = joinBytes([
    PRIORITY,
    TIMEOUT_TICKS,
    cip.csn,
    cip.vid,
    cip.vsn,
  ]);

  const cipMessage = joinBytes([
    connectionManager.forwardClose,
    requestPath,
    forwardCloseParams,
    connectionPath,
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
  await sendRaw(cip, request);

  cip.targetIsConnected = false;
  log.eip.info("ForwardClose complete");
}

/**
 * Create and initialize a CIP connection
 */
export async function createCip(config: ConnectionConfig): Promise<Cip> {
  const connection = await createConnection(config);

  const cip: Cip = {
    connection,
    session: encodeUint(0, 4),
    targetCid: null,
    targetIsConnected: false,
    context: CONTEXT,
    option: 0,
    sequence: cycle(65535, 1),
    protocolVersion: PROTOCOL_VERSION,
    extendedForwardOpen: true, // Use large connection by default
    cid: getRandomBytes(4),
    csn: new Uint8Array([0x27, 0x04]),
    vid: new Uint8Array([0x09, 0x10]),
    vsn: getRandomBytes(4),
    identity: null,
  };

  // Register session
  cip.session = await registerSession(cip);

  // Try to open connection (ForwardOpen) - not required for unconnected messaging
  try {
    await forwardOpen(cip);
  } catch (error) {
    // ForwardOpen failed - this is OK, we can still use unconnected messaging
    log.eip.warn(
      `ForwardOpen failed: ${error instanceof Error ? error.message : error}. Using unconnected messaging only.`
    );
  }

  // Get device identity
  cip.identity = await listIdentity(cip);

  return cip;
}

/**
 * Close and cleanup a CIP connection
 */
export async function destroyCip(cip: Cip): Promise<void> {
  try {
    await forwardClose(cip);
  } catch (error) {
    log.eip.debug(`ForwardClose error (connection may already be closed): ${error}`);
  }

  try {
    await unregisterSession(cip);
  } catch (error) {
    log.eip.debug(`UnregisterSession error (connection may already be closed): ${error}`);
  }

  closeConnection(cip.connection);
}

/**
 * Get the connection size for the CIP connection
 */
export function getConnectionSize(cip: Cip): number {
  return cip.extendedForwardOpen ? LARGE_CONNECTION_SIZE : SMALL_CONNECTION_SIZE;
}

export { sendRaw, buildHeader, buildCommonPacketFormat };
