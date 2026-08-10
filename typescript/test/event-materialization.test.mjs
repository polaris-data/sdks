import assert from "node:assert/strict";
import test from "node:test";

test("event APIs materialize orderbooks by default and expose raw L2 updates", async () => {
  const { OrderbookBuilder, PolarisClient } = await import("../dist/node/index.js");
  const rows = [
    { timestamp: 1, source: "lighter", market: "BTC-USD", type: "orderbook_delta", data: { bids: [[999, 1]] } },
    { timestamp: 2, source: "lighter", market: "BTC-USD", type: "orderbook", data: { bids: [[100, 2]], asks: [[101, 3]] } },
    { timestamp: 3, source: "lighter", market: "BTC-USD", type: "trade", data: { price: 100, quantity: 1 } },
    { timestamp: 4, source: "lighter", market: "BTC-USD", type: "orderbook_delta", data: { bids: [[100, 0], [99, 4]] } },
  ];
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  client._resolveHistoricalRange = async () => ({ fromMs: 0, toMs: 10 });
  client._readSnapshotEvents = async function* (_source, _market, _from, _to, filter) {
    for (const row of rows) if (!filter || filter(row)) yield structuredClone(row);
  };

  const materialized = await client.events({ source: "lighter", market: "BTC-USD" });
  assert.deepEqual(materialized.map(({ type }) => type), ["orderbook", "trade", "orderbook"]);
  assert.deepEqual(materialized.at(-1).data, {
    bids: [{ price: 99, quantity: 4 }],
    asks: [{ price: 101, quantity: 3 }],
  });

  const raw = await client.events({
    source: "lighter",
    market: "BTC-USD",
    materializeOrderbooks: false,
  });
  assert.deepEqual(raw, rows);

  const replayed = [];
  for await (const row of client.replay({ source: "lighter", market: "BTC-USD" })) {
    replayed.push(row);
  }
  assert.deepEqual(replayed, materialized);

  const l2 = await client.l2Snapshots({ source: "lighter", market: "BTC-USD" });
  assert.deepEqual(l2.map(({ type }) => type), ["orderbook", "orderbook"]);

  const updates = await client.l2Updates({ source: "lighter", market: "BTC-USD" });
  assert.deepEqual(updates, [rows[0], rows[1], rows[3]]);

  const books = new OrderbookBuilder();
  const rebuilt = updates.flatMap((update) => {
    const book = books.apply(update);
    return book ? [book] : [];
  });
  assert.deepEqual(rebuilt, l2);
  client.close();
});
