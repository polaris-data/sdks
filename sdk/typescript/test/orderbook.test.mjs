import assert from "node:assert/strict";
import test from "node:test";

test("OrderbookBuilder handles snapshots, deltas, deletion, and per-book clearing", async () => {
  const { OrderbookBuilder } = await import("../dist/node/index.js");
  const builder = new OrderbookBuilder();
  const event = (type, market, data) => ({
    timestamp: 1,
    source: "lighter",
    market,
    type,
    data,
  });

  assert.equal(builder.apply(event("orderbook_delta", "BTC-USD", { bids: [[100, 1]] })), undefined);
  const snapshot = builder.apply(event("orderbook", "BTC-USD", {
    bids: [[99, 1], [100, 2]],
    asks: [{ price: "101", size: "3" }],
  }));
  assert.deepEqual(snapshot.data.bids, [
    { price: 100, quantity: 2 },
    { price: 99, quantity: 1 },
  ]);

  const updated = builder.apply(event("orderbook_delta", "BTC-USD", {
    bids: [[100, 0], [98, 4]],
    asks: [[101, 5]],
  }));
  assert.equal(updated.type, "orderbook");
  assert.deepEqual(updated.data, {
    bids: [{ price: 99, quantity: 1 }, { price: 98, quantity: 4 }],
    asks: [{ price: 101, quantity: 5 }],
  });

  const replaced = builder.apply(event("orderbook", "BTC-USD", {
    bids: [[97, 6]],
    asks: [[103, 7]],
  }));
  assert.deepEqual(replaced.data.bids, [{ price: 97, quantity: 6 }]);

  builder.apply(event("orderbook", "ETH-USD", { bids: [[10, 1]], asks: [[11, 1]] }));
  builder.clearBook("lighter", "BTC-USD");
  assert.equal(builder.apply(event("orderbook_delta", "BTC-USD", { asks: [[102, 1]] })), undefined);
  assert.ok(builder.apply(event("orderbook_delta", "ETH-USD", { bids: [[10, 2]] })));
  builder.clear();
  assert.equal(builder.apply(event("orderbook_delta", "ETH-USD", { bids: [[10, 3]] })), undefined);
});

test("OrderbookBuilder updates state without materializing until requested", async () => {
  const { OrderbookBuilder } = await import("../dist/node/index.js");
  const builder = new OrderbookBuilder();
  const event = (type, data) => ({
    timestamp: 1,
    source: "lighter",
    market: "BTC-USD",
    type,
    data,
  });

  assert.equal(builder.update(event("orderbook_delta", { bids: [[100, 1]] })), false);
  assert.equal(builder.snapshot("lighter", "BTC-USD"), undefined);
  assert.equal(builder.update(event("orderbook", {
    bids: [[100, 2]],
    asks: [[101, 3]],
  })), true);
  assert.equal(builder.update(event("orderbook_delta", { bids: [[100, 4]] })), true);
  assert.deepEqual(builder.snapshot("lighter", "BTC-USD"), {
    bids: [{ price: 100, quantity: 4 }],
    asks: [{ price: 101, quantity: 3 }],
  });
});

test("a malformed update leaves both sides intact and the adapter preserves metadata", async () => {
  const { OrderbookBuilder, PolarisError } = await import("../dist/node/index.js");
  const builder = new OrderbookBuilder();
  const snapshot = {
    timestamp: 1, source: "s", market: "m", type: "orderbook", id: 18446744073709551615n,
    data: { bids: [[99, " 1 "]], asks: [[101, 2]], detail: { sequence: 18446744073709551614n } },
  };
  const result = builder.apply(snapshot);
  assert.equal(result.id, snapshot.id);
  assert.equal(result.data.detail, snapshot.data.detail);
  assert.throws(() => builder.update({
    ...snapshot, type: "orderbook_delta", data: { bids: [[99, 0]], asks: [[101, -1]] },
  }), PolarisError);
  assert.deepEqual(builder.snapshot("s", "m"), {
    bids: [{ price: 99, quantity: 1 }],
    asks: [{ price: 101, quantity: 2 }],
  });
});

test("orderbook snapshots retain the numeric types of large levels", async () => {
  const { OrderbookBuilder } = await import("../dist/node/index.js");
  const builder = new OrderbookBuilder();
  builder.update({
    timestamp: 1, source: "s", market: "m", type: "orderbook",
    data: { bids: [[1e16, 1e30]], asks: [] },
  });
  assert.deepEqual(builder.snapshot("s", "m").bids, [{ price: 1e16, quantity: 1e30 }]);
});
