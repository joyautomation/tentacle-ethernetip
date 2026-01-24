/**
 * Base device class with state management and polling
 */

import { log } from "../utils/logger.ts";
import { sleep, isConnectionError, withRetry } from "../utils/retry.ts";
import type { DeviceConfig, TagConfig } from "../../types/config.ts";
import type { Cip, DeviceIdentity, CipResponse } from "../ethernetip/mod.ts";
import { createCip, destroyCip } from "../ethernetip/mod.ts";

/**
 * Device connection state
 */
export type DeviceState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "polling"
  | "reconnecting"
  | "error"
  | "stopped";

/**
 * Device statistics
 */
export type DeviceStats = {
  pollCount: number;
  errorCount: number;
  lastPoll: number | null;
  lastError: string | null;
  avgResponseTime: number;
};

/**
 * Tag value with metadata
 */
export type TagValue = {
  tagId: string;
  value: unknown;
  datatype: string;
  quality: "good" | "bad" | "uncertain";
  timestamp: number;
};

/**
 * Event handler types
 */
export type DeviceEventHandlers = {
  onTagUpdate?: (tagId: string, value: TagValue) => void;
  onStateChange?: (state: DeviceState, error?: Error) => void;
  onIdentity?: (identity: DeviceIdentity) => void;
};

/**
 * Base device class
 */
export abstract class BaseDevice {
  protected config: DeviceConfig;
  protected cip: Cip | null = null;
  protected state: DeviceState = "disconnected";
  protected running = false;
  protected tags: Map<string, TagConfig> = new Map();
  protected tagValues: Map<string, TagValue> = new Map();
  protected stats: DeviceStats = {
    pollCount: 0,
    errorCount: 0,
    lastPoll: null,
    lastError: null,
    avgResponseTime: 0,
  };
  protected handlers: DeviceEventHandlers;

  constructor(config: DeviceConfig, handlers: DeviceEventHandlers = {}) {
    this.config = config;
    this.handlers = handlers;
  }

  /**
   * Get device ID
   */
  get id(): string {
    return this.config.id;
  }

  /**
   * Get current state
   */
  get currentState(): DeviceState {
    return this.state;
  }

  /**
   * Get device statistics
   */
  get statistics(): DeviceStats {
    return { ...this.stats };
  }

  /**
   * Get device identity (if connected)
   */
  get identity(): DeviceIdentity | null {
    return this.cip?.identity ?? null;
  }

  /**
   * Get all current tag values
   */
  get values(): Map<string, TagValue> {
    return new Map(this.tagValues);
  }

  /**
   * Set state and notify handlers
   */
  protected setState(newState: DeviceState, error?: Error): void {
    if (this.state !== newState) {
      const oldState = this.state;
      this.state = newState;
      log.device.info(`Device ${this.id}: ${oldState} -> ${newState}`);
      this.handlers.onStateChange?.(newState, error);
    }
  }

  /**
   * Configure tags to poll
   */
  configureTags(tags: Record<string, TagConfig>): void {
    this.tags.clear();
    for (const [id, tag] of Object.entries(tags)) {
      this.tags.set(id, { ...tag, id });
    }
    log.device.info(`Device ${this.id}: Configured ${this.tags.size} tags`);
  }

  /**
   * Add a single tag
   */
  addTag(id: string, tag: TagConfig): void {
    this.tags.set(id, { ...tag, id });
  }

  /**
   * Remove a tag
   */
  removeTag(id: string): void {
    this.tags.delete(id);
    this.tagValues.delete(id);
  }

  /**
   * Start the device (connect and begin polling)
   */
  async start(): Promise<void> {
    if (this.running) return;

    this.running = true;
    this.connectionLoop();
  }

  /**
   * Stop the device
   */
  async stop(): Promise<void> {
    this.running = false;
    this.setState("stopped");

    if (this.cip) {
      try {
        await destroyCip(this.cip);
      } catch (error) {
        log.device.warn(`Device ${this.id}: Error closing connection: ${error}`);
      }
      this.cip = null;
    }
  }

  /**
   * Main connection loop with reconnection
   */
  protected async connectionLoop(): Promise<void> {
    while (this.running) {
      try {
        this.setState("connecting");

        // Connect with retry
        await withRetry(
          async () => {
            this.cip = await createCip({
              host: this.config.host,
              port: this.config.port,
              timeout: this.config.timeout ?? 5000,
            });
          },
          {
            maxRetries: this.config.maxRetries ?? 5,
            baseDelay: this.config.retryDelay ?? 2000,
          },
          (attempt, error) => {
            log.device.warn(
              `Device ${this.id}: Connection attempt ${attempt} failed: ${error.message}`,
            );
          },
        );

        this.setState("connected");

        // Notify identity
        if (this.cip?.identity) {
          this.handlers.onIdentity?.(this.cip.identity);
        }

        // Start polling loop
        await this.pollLoop();
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        this.stats.errorCount++;
        this.stats.lastError = err.message;
        this.setState("error", err);

        if (this.cip) {
          try {
            await destroyCip(this.cip);
          } catch {
            // Ignore cleanup errors
          }
          this.cip = null;
        }

        if (this.running) {
          this.setState("reconnecting");
          await sleep(this.config.retryDelay ?? 5000);
        }
      }
    }
  }

  /**
   * Polling loop
   */
  protected async pollLoop(): Promise<void> {
    while (this.running && this.state === "connected") {
      if (this.tags.size === 0) {
        // No tags configured, wait and check again
        await sleep(1000);
        continue;
      }

      this.setState("polling");
      const startTime = Date.now();

      try {
        await this.pollAllTags();
        this.stats.pollCount++;
        this.stats.lastPoll = Date.now();

        // Update average response time
        const elapsed = Date.now() - startTime;
        this.stats.avgResponseTime =
          (this.stats.avgResponseTime * 0.9 + elapsed * 0.1);

        this.setState("connected");
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        this.stats.errorCount++;
        this.stats.lastError = err.message;

        // Check if it's a connection error
        if (isConnectionError(err)) {
          throw err; // Break out to reconnect
        }

        // Non-fatal error, continue polling
        log.device.warn(`Device ${this.id}: Poll error (continuing): ${err.message}`);
        this.setState("connected");
      }

      // Wait for next poll cycle
      const elapsed = Date.now() - startTime;
      const pollRate = this.config.pollRate ?? 1000;
      const waitTime = Math.max(0, pollRate - elapsed);
      await sleep(waitTime);
    }
  }

  /**
   * Poll all configured tags - implemented by subclasses
   */
  protected abstract pollAllTags(): Promise<void>;

  /**
   * Read a tag - implemented by subclasses
   */
  abstract readTag(tagId: string): Promise<TagValue>;

  /**
   * Write a tag - implemented by subclasses
   */
  abstract writeTag(tagId: string, value: unknown): Promise<void>;

  /**
   * Browse available tags - implemented by subclasses
   */
  abstract browseTags(filter?: string): Promise<Array<{
    name: string;
    datatype: string;
    dimensions: number[];
  }>>;

  /**
   * Update a tag value and notify handlers
   */
  protected updateTagValue(tagId: string, value: TagValue): void {
    const existing = this.tagValues.get(tagId);

    // Check if value changed (simple comparison)
    if (existing && JSON.stringify(existing.value) === JSON.stringify(value.value)) {
      // Value unchanged, check deadband if configured
      const tagConfig = this.tags.get(tagId);
      if (tagConfig?.deadband) {
        const numValue = typeof value.value === "number" ? value.value : 0;
        const prevValue = typeof existing.value === "number" ? existing.value : 0;
        const diff = Math.abs(numValue - prevValue);

        if (diff < tagConfig.deadband.value) {
          // Check maxTime
          if (tagConfig.deadband.maxTime) {
            const timeSinceUpdate = Date.now() - existing.timestamp;
            if (timeSinceUpdate < tagConfig.deadband.maxTime) {
              return; // Skip update
            }
          } else {
            return; // Skip update
          }
        }
      } else {
        return; // No change, no deadband, skip update
      }
    }

    this.tagValues.set(tagId, value);
    this.handlers.onTagUpdate?.(tagId, value);
  }
}
