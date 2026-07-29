// ---------------------------------------------------------------------------
// Base
// ---------------------------------------------------------------------------

export class PolarisError extends Error {
  override readonly name: string = "PolarisError";

  /** HTTP status code, if the error originated from an API response. */
  readonly statusCode: number | undefined;

  /** Raw response body string. */
  readonly body: string | undefined;

  constructor(message: string, statusCode?: number, body?: string) {
    super(message);
    this.statusCode = statusCode;
    this.body = body;
  }

  override toString(): string {
    return this.statusCode != null
      ? `${this.message} (status=${this.statusCode})`
      : this.message;
  }
}

// ---------------------------------------------------------------------------
// 401
// ---------------------------------------------------------------------------

export class UnauthorizedError extends PolarisError {
  override readonly name: string = "UnauthorizedError";
}

// ---------------------------------------------------------------------------
// 404
// ---------------------------------------------------------------------------

export class NotFoundError extends PolarisError {
  override readonly name: string = "NotFoundError";
}

// ---------------------------------------------------------------------------
// 429
// ---------------------------------------------------------------------------

export class RateLimitedError extends PolarisError {
  override readonly name: string = "RateLimitedError";

  /** ISO 8601 timestamp (or epoch) indicating when the rate limit resets. */
  readonly resetAt: string | undefined;

  constructor(
    message: string,
    statusCode?: number,
    body?: string,
    resetAt?: string,
  ) {
    super(message, statusCode, body);
    this.resetAt = resetAt;
  }
}

// ---------------------------------------------------------------------------
// Stream / decode failures
// ---------------------------------------------------------------------------

export class StreamDecodeError extends PolarisError {
  override readonly name: string = "StreamDecodeError";
}

// ---------------------------------------------------------------------------
// Download blocked by server policy
// ---------------------------------------------------------------------------

export class DownloadNotAllowedError extends PolarisError {
  override readonly name: string = "DownloadNotAllowedError";
}
