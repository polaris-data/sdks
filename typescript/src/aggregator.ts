import type { OhlcvBar, OhlcvInterval } from "./types";

// ---------------------------------------------------------------------------
// Interval → milliseconds
// ---------------------------------------------------------------------------

function intervalToMs(interval: OhlcvInterval): number {
  const m = interval.match(/^(\d+)(ms|s|m|h)$/);
  if (!m) throw new Error(`Invalid OHLCV interval: ${interval}`);
  const n = parseInt(m[1], 10);
  switch (m[2]) {
    case "ms": return n;
    case "s":  return n * 1_000;
    case "m":  return n * 60_000;
    case "h":  return n * 3_600_000;
    default:  throw new Error(`Unreachable`);
  }
}

// ---------------------------------------------------------------------------
// Internal bucket
// ---------------------------------------------------------------------------

interface _Bucket {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  /** Scaled by 1e12 to avoid floating-point precision loss on accumulation. */
  volScaled: number;
  trades: number;
}

// ---------------------------------------------------------------------------
// OhlcvAggregator
// ---------------------------------------------------------------------------

/**
 * In-memory OHLCV bar aggregator that buckets trades by timestamp into
 * fixed-width intervals.
 *
 * Volume is accumulated as `quantity × 1e12` (rounded to integer) to match the
 * precision strategy used by the Python SDK's `_LocalOhlcvAggregator`.
 */
export class OhlcvAggregator {
  private readonly _intervalMs: number;
  private readonly _bars = new Map<number, _Bucket>();

  constructor(interval: OhlcvInterval) {
    this._intervalMs = intervalToMs(interval);
  }

  /** Ingest a single trade event. */
  add(timestamp: number, price: number, quantity: number): void {
    const bucketTs =
      Math.floor(timestamp / this._intervalMs) * this._intervalMs;

    let b = this._bars.get(bucketTs);
    if (!b) {
      b = {
        ts: bucketTs,
        open: price,
        high: price,
        low: price,
        close: price,
        volScaled: 0,
        trades: 0,
      };
      this._bars.set(bucketTs, b);
    }

    if (price > b.high) b.high = price;
    if (price < b.low) b.low = price;
    b.close = price;
    b.volScaled += Math.round(quantity * 1e12);
    b.trades += 1;
  }

  /**
   * Finalise the aggregation and return sorted bars.
   *
   * The `open` of each subsequent bar is overwritten with the `close` of the
   * previous bar, matching the convention used by the Python SDK.
   */
  finish(): OhlcvBar[] {
    const bars = Array.from(this._bars.values()).sort(
      (a, b) => a.ts - b.ts,
    );

    for (let i = 1; i < bars.length; i++) {
      bars[i].open = bars[i - 1].close;
    }

    return bars.map((b) => ({
      timestamp: b.ts,
      open: b.open,
      high: b.high,
      low: b.low,
      close: b.close,
      volume: b.volScaled / 1e12,
      trades: b.trades,
    }));
  }
}
