import type { TimeInput } from "./types";

const EPOCH_RE = /^\d+$/;

// ---------------------------------------------------------------------------
// toIso8601
// ---------------------------------------------------------------------------

/**
 * Convert a {@link TimeInput} value to an ISO 8601 UTC string with a `Z` suffix.
 *
 * - `string` → returned as-is (assumed ISO 8601), unless it looks like an
 *   epoch millisecond integer (13+ digits) in which case it is converted.
 * - `Date`   → converted via `toISOString()`.
 * - `number` → treated as **epoch milliseconds**.
 */
export function toIso8601(value: TimeInput): string {
  if (typeof value === "string") {
    if (EPOCH_RE.test(value) && value.length >= 13) return epochMsToIso(Number(value));
    return value;
  }
  if (value instanceof Date) return dateToIso(value);
  return epochMsToIso(value);
}

// ---------------------------------------------------------------------------
// toEpochMs
// ---------------------------------------------------------------------------

/** Convert a {@link TimeInput} to **epoch milliseconds** (integer). */
export function toEpochMs(value: TimeInput): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    if (EPOCH_RE.test(value) && value.length >= 13) return Number(value);
    return new Date(value).getTime();
  }
  return value.getTime();
}

// ---------------------------------------------------------------------------
// datesInRange
// ---------------------------------------------------------------------------

/**
 * Return an array of `"YYYY-MM-DD"` strings covering every UTC calendar date
 * that intersects `[fromMs, toMs)` (epoch milliseconds, inclusive start /
 * exclusive end).
 */
export function datesInRange(fromMs: number, toMs: number): string[] {
  if (toMs <= fromMs) return [];

  const dates: string[] = [];
  const start = new Date(fromMs);
  const end = new Date(toMs - 1);

  const cur = new Date(
    Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate()),
  );
  const last = new Date(
    Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate()),
  );

  while (cur <= last) {
    dates.push(formatUtcDate(cur));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }

  return dates;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function epochMsToIso(ms: number): string {
  return new Date(ms).toISOString().replace(/\.000Z$/, "Z");
}

function dateToIso(d: Date): string {
  return d.toISOString().replace(/\.000Z$/, "Z");
}

function formatUtcDate(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}
