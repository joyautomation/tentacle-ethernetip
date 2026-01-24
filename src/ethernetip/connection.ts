/**
 * EtherNet/IP TCP connection management
 */

import { log } from "../utils/logger.ts";
import { DEFAULT_PORT } from "./constants.ts";

export type ConnectionConfig = {
  host: string;
  port?: number;
  timeout?: number;
};

export type Connection = {
  socket: Deno.TcpConn;
  host: string;
  port: number;
  connected: boolean;
};

/**
 * Create a TCP connection to an EtherNet/IP device
 */
export async function createConnection(
  config: ConnectionConfig,
): Promise<Connection> {
  const port = config.port ?? DEFAULT_PORT;
  const timeout = config.timeout ?? 5000;

  log.eip.info(`Connecting to ${config.host}:${port}...`);

  // Create connection with timeout
  const socket = await Promise.race([
    Deno.connect({
      hostname: config.host,
      port,
      transport: "tcp",
    }),
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("Connection timeout")), timeout)
    ),
  ]);

  log.eip.info(`Connected to ${config.host}:${port}`);

  return {
    socket,
    host: config.host,
    port,
    connected: true,
  };
}

/**
 * Close a TCP connection
 */
export function closeConnection(conn: Connection): void {
  try {
    conn.socket.close();
    conn.connected = false;
    log.eip.info(`Closed connection to ${conn.host}:${conn.port}`);
  } catch (error) {
    log.eip.warn(`Error closing connection: ${error}`);
  }
}

/**
 * Write data to the connection
 */
export async function writeData(
  conn: Connection,
  data: Uint8Array,
): Promise<void> {
  await conn.socket.write(data);
}

/**
 * Read data from the connection with optional timeout
 */
export async function readData(
  conn: Connection,
  timeout?: number,
): Promise<Uint8Array> {
  const buffer = new Uint8Array(4096);

  if (timeout) {
    const result = await Promise.race([
      conn.socket.read(buffer),
      new Promise<null>((_, reject) =>
        setTimeout(() => reject(new Error("Read timeout")), timeout)
      ),
    ]);
    if (result === null) {
      throw new Error("Connection closed");
    }
    return buffer.subarray(0, result);
  }

  const bytesRead = await conn.socket.read(buffer);
  if (bytesRead === null) {
    throw new Error("Connection closed");
  }
  return buffer.subarray(0, bytesRead);
}
