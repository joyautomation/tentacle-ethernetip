/**
 * Retry configuration
 */
export type RetryConfig = {
  maxRetries: number;
  baseDelay: number;
  maxDelay: number;
  multiplier: number;
};

export const defaultRetryConfig: RetryConfig = {
  maxRetries: 5,
  baseDelay: 1000,
  maxDelay: 30000,
  multiplier: 2,
};

/**
 * Sleep for a given number of milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Execute an operation with retry logic and exponential backoff
 */
export async function withRetry<T>(
  operation: () => Promise<T>,
  config: Partial<RetryConfig> = {},
  onRetry?: (attempt: number, error: Error) => void,
): Promise<T> {
  const { maxRetries, baseDelay, maxDelay, multiplier } = {
    ...defaultRetryConfig,
    ...config,
  };

  let lastError: Error;
  let delay = baseDelay;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      if (attempt < maxRetries) {
        onRetry?.(attempt, lastError);
        await sleep(delay);
        delay = Math.min(delay * multiplier, maxDelay);
      }
    }
  }

  throw lastError!;
}

/**
 * Check if an error is a connection-related error
 */
export function isConnectionError(error: Error): boolean {
  const connectionErrors = [
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "EPIPE",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "Connection closed",
    "Connection reset",
    "Broken pipe",
  ];

  return connectionErrors.some(
    (msg) => error.message.includes(msg) || error.name.includes(msg),
  );
}
