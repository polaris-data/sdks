import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
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

test("v2 decoder consumes metadata and materializes unified books without changing delta identity", async () => {
  const { OrderbookBuilder, PolarisClient } = await import("../dist/node/index.js");
  const fixture = await readFile(new URL("../../tests/fixtures/events/schema-v2.jsonl", import.meta.url), "utf8");
  const legacyFixture = await readFile(new URL("../../tests/fixtures/events/legacy-v1.jsonl", import.meta.url), "utf8");
  const lines = fixture.trim().split("\n");
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  const decoded = client._decodeSnapshotLines(lines, "schema-v2.jsonl");
  const legacyDecoded = client._decodeSnapshotLines(legacyFixture.trim().split("\n"), "legacy-v1.jsonl");

  assert.deepEqual(legacyDecoded, legacyFixture.trim().split("\n").map(JSON.parse));

  assert.equal(decoded.length, 8);
  assert.deepEqual(decoded.map(({ collector_timestamp }) => collector_timestamp), [
    1704067200100,
    1704067200300,
    1704067200200,
    1704067200400,
    1704067200500,
    1704067200450,
    1704067261000,
    1704067200550,
  ]);
  assert.equal(decoded[2].exchange_timestamp, 1704067198000);
  assert.equal(decoded[2].data.order_id, null);
  assert.equal(decoded[2].data.side, null);
  assert.equal(decoded[3].data.value, "100.75");
  assert.equal(new OrderbookBuilder().apply(decoded[1]), undefined);

  client._resolveHistoricalRange = async () => ({ fromMs: 1704067200000, toMs: 1704067201000 });
  client._readSnapshotEvents = async function* (_source, _market, from, to, filter) {
    for (const row of decoded) {
      const timestamp = row.collector_timestamp;
      if (timestamp >= from && timestamp < to && (!filter || filter(row))) yield structuredClone(row);
    }
  };
  const materialized = await client.events({ source: "lighter", market: "BTC-USD" });
  assert.equal(materialized[1].data.is_snapshot, false);
  assert.deepEqual(materialized[1].data.bids, [{ price: 100, quantity: 4 }]);
  assert.deepEqual(materialized[1].data.asks, [{ price: 102, quantity: 5 }]);

  const bbo = await client.bbo({ source: "lighter", market: "BTC-USD" });
  assert.deepEqual(bbo.map(({ timestamp }) => timestamp), [
    1704067200100,
    1704067200300,
    1704067200550,
  ]);
  const depth = await client.depthMetrics({ source: "lighter", market: "BTC-USD" });
  assert.deepEqual(depth.map(({ timestamp }) => timestamp), [
    1704067200100,
    1704067200300,
    1704067200550,
  ]);
  const bars = await client.ohlcv({ source: "lighter", market: "BTC-USD", interval: "1m" });
  assert.deepEqual(bars, [{
    timestamp: 1704067200000,
    open: 100.5,
    high: 102,
    low: 99,
    close: 99,
    volume: 6.25,
    trades: 3,
  }]);
  const volatility = await client.volatility({
    source: "lighter",
    market: "BTC-USD",
    interval: "1m",
  });
  assert.equal(volatility[0].returns, 2);
  const marks = await client.markPrices({ source: "lighter", market: "BTC-USD" });
  assert.equal(marks.length, 1);
  assert.equal(marks[0].data.series, "mark_px");

  const unsupported = [...lines];
  unsupported[0] = unsupported[0].replace('"v2"', '"v3"');
  assert.throws(
    () => client._decodeSnapshotLines(unsupported, "unknown.jsonl"),
    /Unsupported standard event schema version 'v3'/,
  );
  const missingSnapshotFlag = [...lines];
  const malformedBook = JSON.parse(missingSnapshotFlag[1]);
  delete malformedBook.data.is_snapshot;
  missingSnapshotFlag[1] = JSON.stringify(malformedBook);
  assert.throws(
    () => client._decodeSnapshotLines(missingSnapshotFlag, "missing-is-snapshot.jsonl"),
    /Invalid v2 orderbook payload/,
  );
  assert.deepEqual(
    client._decodeSnapshotLines(
      [...legacyFixture.trim().split("\n"), lines[1]],
      "headerless-v1.jsonl",
    ),
    legacyDecoded,
  );
  client.close();
});

test("PropAMM quote ladders inherit metadata market, filter records, and validate payloads", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const fermiFixture = await readFile(
    new URL("../../tests/fixtures/events/propamm-fermiswap-v2.jsonl", import.meta.url),
    "utf8",
  );
  const metricFixture = await readFile(
    new URL("../../tests/fixtures/events/propamm-metric-v2.jsonl", import.meta.url),
    "utf8",
  );
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  const fermi = client._decodeSnapshotLines(
    fermiFixture.trim().split("\n"),
    "propamm-fermiswap-v2.jsonl",
  );
  const metric = client._decodeSnapshotLines(
    metricFixture.trim().split("\n"),
    "propamm-metric-v2.jsonl",
  );

  assert.deepEqual(fermi.map(({ market }) => market), ["ethereum", "ethereum"]);
  assert.equal("market" in JSON.parse(fermiFixture.trim().split("\n")[2]), false);

  client._resolveHistoricalRange = async () => ({
    fromMs: 1704067200000,
    toMs: 1704067201000,
  });
  client._readSnapshotEvents = async function* (source) {
    for (const row of source === "metric" ? metric : fermi) yield structuredClone(row);
  };

  const fermiLadders = await client.propammQuoteLadders({
    source: "fermiswap",
    market: "ethereum",
  });
  const metricLadders = await client.propammQuoteLadders({
    source: "metric",
    market: "ethereum",
  });
  assert.equal(fermiLadders.length, 1);
  assert.equal(
    fermiLadders[0].data.values.quotes[0].amount_in,
    (2n ** 256n - 1n).toString(),
  );
  assert.equal(fermiLadders[0].data.values.oracle, null);
  assert.equal("pool" in fermiLadders[0].data.values, false);
  assert.equal(metricLadders[0].data.values.pool, "0xpool");

  const malformed = structuredClone(fermi);
  malformed[1].data.values.quotes[0].amount_in = 10;
  client._readSnapshotEvents = async function* () {
    for (const row of malformed) yield row;
  };
  await assert.rejects(
    client.propammQuoteLadders({ source: "fermiswap", market: "ethereum" }),
    /Invalid PropAMM quote-ladder payload/,
  );
  client.close();
});
