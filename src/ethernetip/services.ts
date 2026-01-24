/**
 * EtherNet/IP and CIP service codes
 */

function createService<T extends Record<string, number[]>>(
  entries: T,
): { [K in keyof T]: Uint8Array } {
  return Object.fromEntries(
    Object.entries(entries).map(([key, value]) => [key, new Uint8Array(value)]),
  ) as { [K in keyof T]: Uint8Array };
}

/**
 * Encapsulation commands (2 bytes, little-endian)
 */
const encapsulationEntries = {
  nop: [0x00, 0x00],
  listTargets: [0x01, 0x00],
  listServices: [0x04, 0x00],
  listIdentity: [0x63, 0x00],
  listInterfaces: [0x64, 0x00],
  registerSession: [0x65, 0x00],
  unregisterSession: [0x66, 0x00],
  sendRRData: [0x6f, 0x00],
  sendUnitData: [0x70, 0x00],
};

export const encapsulation = createService(encapsulationEntries);

/**
 * Connection Manager services (1 byte)
 */
const connectionManagerEntries = {
  forwardOpen: [0x4d],         // Standard ForwardOpen
  forwardClose: [0x4e],
  unconnectedSend: [0x52],
  largeForwardOpen: [0x54],    // Large ForwardOpen (extended)
  getConnectionData: [0x56],
  searchConnectionData: [0x57],
  getConnectionOwner: [0x5a],
};

export const connectionManager = createService(connectionManagerEntries);

/**
 * CIP service codes (1 byte)
 */
const serviceEntries = {
  // Standard CIP services
  getAttributesAll: [0x01],
  setAttributesAll: [0x02],
  getAttributeList: [0x03],
  setAttributeList: [0x04],
  reset: [0x05],
  start: [0x06],
  stop: [0x07],
  create: [0x08],
  delete: [0x09],
  multipleServiceRequest: [0x0a],
  applyAttributes: [0x0d],
  getAttributeSingle: [0x0e],
  setAttributeSingle: [0x10],
  findNextObjectInstance: [0x11],
  errorResponse: [0x14],
  restore: [0x15],
  save: [0x16],
  nop: [0x17],
  getMember: [0x18],
  setMember: [0x19],
  insertMember: [0x1a],
  removeMember: [0x1b],
  groupSync: [0x1c],
  // Rockwell Custom Services
  readTag: [0x4c],
  writeTag: [0x4d],
  readModifyWrite: [0x4e],
  readTagFragmented: [0x52],
  writeTagFragmented: [0x53],
  getInstanceAttributeList: [0x55],
};

export const service = createService(serviceEntries);

/**
 * CIP data type codes for Rockwell controllers
 */
export const CIP_DATA_TYPES = {
  BOOL: 0x00c1,
  SINT: 0x00c2,
  INT: 0x00c3,
  DINT: 0x00c4,
  LINT: 0x00c5,
  USINT: 0x00c6,
  UINT: 0x00c7,
  UDINT: 0x00c8,
  ULINT: 0x00c9,
  REAL: 0x00ca,
  LREAL: 0x00cb,
  STRING: 0x00d0,
  BYTE: 0x00d1,
  WORD: 0x00d2,
  DWORD: 0x00d3,
  LWORD: 0x00d4,
} as const;

export type CipDataType = keyof typeof CIP_DATA_TYPES;
