import assert from "node:assert/strict";
import test from "node:test";

import { OhlcvAggregator } from "../dist/node/index.js";

test("OHLCV rounds signed ties per trade and can keep aggregating after finish", () => {
  const bars = new OhlcvAggregator("1s");
  assert.deepEqual(bars.finish(), []);
  bars.add(1000, 20, 1.5e-12);
  bars.add(-1, 15, -0.5e-12);
  bars.add(1999, 25, -1.5e-12);
  bars.add(1001, 23, 0.4999999999999998e-12);

  const expected = [
    { timestamp: -1000, open: 15, high: 15, low: 15, close: 15, volume: 0, trades: 1 },
    { timestamp: 1000, open: 15, high: 25, low: 20, close: 23, volume: 1e-12, trades: 3 },
  ];
  assert.deepEqual(bars.finish(), expected);
  assert.deepEqual(bars.finish(), expected);

  bars.add(1010, 18, 0);
  assert.deepEqual(bars.finish()[1], { ...expected[1], low: 18, close: 18, trades: 4 });
  assert.throws(() => bars.add(1001, Infinity, 0), RangeError);
});

test("OHLCV keeps large numeric prices and timestamps as numbers", () => {
  const bars = new OhlcvAggregator("1s");
  bars.add(1e16, 1e16, 0);
  assert.equal(bars.finish()[0].timestamp, 1e16);
  assert.equal(bars.finish()[0].close, 1e16);
});

test("OHLCV preserves the serialized zero representation", () => {
  const bars = new OhlcvAggregator("1s");
  bars.add(-0, -0, -0);
  assert.deepEqual(bars.finish()[0], {
    timestamp: 0, open: 0, high: 0, low: 0, close: 0, volume: 0, trades: 1,
  });
});
