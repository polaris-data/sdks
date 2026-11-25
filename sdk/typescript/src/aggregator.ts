import { ownHelper, type Core } from "./core";
import type { OhlcvBar, OhlcvInterval } from "./types";

/** Aggregate trades into fixed-width OHLCV bars. */
export class OhlcvAggregator {
  private readonly native: ReturnType<Core["ohlcv"]>;

  constructor(interval: OhlcvInterval) {
    this.native = ownHelper(this, (core) => core.ohlcv(interval));
  }

  add(timestamp: number, price: number, quantity: number): void {
    this.native.add(timestamp, price, quantity);
  }

  /** Return sorted bars, using the preceding close as each subsequent open. */
  finish(): OhlcvBar[] {
    return this.native.finish();
  }
}
