import { createLogger, LogLevel, type Log } from "@joyautomation/coral";

export { LogLevel, type Log };

let currentLevel = LogLevel.info;

export const log = {
  service: createLogger("ethernetip", currentLevel),
  eip: createLogger("ethernetip:eip", currentLevel),
  nats: createLogger("ethernetip:nats", currentLevel),
  device: createLogger("ethernetip:device", currentLevel),
};

export function setLogLevel(level: LogLevel) {
  currentLevel = level;
  // Note: coral loggers don't dynamically update level after creation
}
