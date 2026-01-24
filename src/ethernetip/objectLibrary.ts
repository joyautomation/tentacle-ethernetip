/**
 * CIP Object class codes and instances
 */

function createObject<T extends Record<string, number>>(
  entries: T,
): { [K in keyof T]: Uint8Array } {
  return Object.fromEntries(
    Object.entries(entries).map(([key, value]) => [
      key,
      new Uint8Array([value]),
    ]),
  ) as { [K in keyof T]: Uint8Array };
}

/**
 * Connection Manager instance IDs
 */
const connectionManagerInstancesEntries = {
  openRequest: 0x01,
  openFormatRejected: 0x02,
  openResourceRejected: 0x03,
  openOtherRejected: 0x04,
  closeRequest: 0x05,
  closeFormatRequest: 0x06,
  closeOtherRequest: 0x07,
  connectionTimeout: 0x08,
};

export const connectionManagerInstances = createObject(
  connectionManagerInstancesEntries,
);

/**
 * CIP class codes
 */
const classCodeEntries = {
  identityObject: 0x01,
  messageRouter: 0x02,
  deviceNet: 0x03,
  assembly: 0x04,
  connection: 0x05,
  connectionManager: 0x06,
  register: 0x07,
  discreteInput: 0x08,
  discreteOutput: 0x09,
  analogInput: 0x0a,
  analogOutput: 0x0b,
  presenceSensing: 0x0e,
  parameter: 0x0f,
  parameterGroup: 0x10,
  group: 0x12,
  discreteInputGroup: 0x1d,
  discreteOutputGroup: 0x1e,
  discreteGroup: 0x1f,
  analogInputGroup: 0x20,
  analogOutputGroup: 0x21,
  analogGroup: 0x22,
  positionSensor: 0x23,
  positionControllerSupervisor: 0x24,
  positionController: 0x25,
  blockSequencer: 0x26,
  commandBlock: 0x27,
  motorData: 0x28,
  controlSupervisor: 0x29,
  acDcDrive: 0x2a,
  acknowledgeHandler: 0x2b,
  overload: 0x2c,
  softstart: 0x2d,
  selection: 0x2e,
  fileObject: 0x37,
  safetySupervisor: 0x39,
  safetyValidator: 0x3a,
  eventLog: 0x41,
  motionAxis: 0x42,
  timeSync: 0x43,
  modbus: 0x44,
  modbusSerialLink: 0x46,
  programName: 0x64, // Rockwell
  symbolObject: 0x6b, // Rockwell - for tag discovery
  templateObject: 0x6c, // Rockwell - for UDT templates
  wallClockTime: 0x8b,
  controlnet: 0xf0,
  controlnetKeeper: 0xf1,
  controlnetScheduling: 0xf2,
  connectionConfiguration: 0xf3,
  port: 0xf4,
  tcpIpInterface: 0xf5,
  ethernetLink: 0xf6,
  componetLink: 0xf7,
  componetRepeater: 0xf8,
};

export const classCode = createObject(classCodeEntries);

/**
 * Symbol type bit masks for parsing tag metadata
 */
export const SYMBOL_TYPE = {
  ATOMIC: 0x0000,
  STRUCT: 0x8000,
  ARRAY_1D: 0x2000,
  ARRAY_2D: 0x4000,
  ARRAY_3D: 0x6000,
  TYPE_MASK: 0x00ff,
  STRUCT_MASK: 0x0fff,
};
