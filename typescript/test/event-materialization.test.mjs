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

  const updates = raw.filter(({ type }) => type.startsWith("orderbook"));
  const books = new OrderbookBuilder();
  const rebuilt = updates.flatMap((update) => {
    const book = books.apply(update);
    return book ? [book] : [];
  });
  assert.deepEqual(rebuilt, materialized.filter(({ type }) => type === "orderbook"));
  client.close();
});

test("direct L2 routes paginate and accept variable ranges", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const row = (snapshot) => {
    const value = {
      event_id: snapshot ? "snapshot" : "delta", source: "hyperliquid", market: "0G",
      instrument: null, collector_timestamp: 10, exchange_timestamp: null,
      source_capture_id: "capture", schema_version: 1, source_event_is_snapshot: snapshot,
    };
    for (let index = 0; index < 25; index++) {
      for (const side of ["bid", "ask"]) for (const field of ["px", "sz"]) {
        value[`${side}_${field}_${String(index).padStart(2, "0")}`] = null;
      }
    }
    value.bid_px_00 = 100;
    value.bid_sz_00 = 2;
    value.ask_px_00 = 101;
    value.ask_sz_00 = 1;
    return value;
  };
  const calls = [];
  const client = new PolarisClient({ baseUrl: "https://api.example", apiKey: "secret", fetch: async (input, init) => {
    const url = new URL(input);
    calls.push({ url, headers: init.headers });
    const second = url.searchParams.has("cursor");
    const body = url.pathname === "/historical/l2-updates"
      ? { items: [row(!second)], has_more: !second, next_cursor: second ? null : "next" }
      : { items: [row(false)], has_more: false, next_cursor: null };
    return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  } });
  assert.deepEqual(await client.l2Updates({ source: "hyperliquid", market: "0G", instrument: "0G", start: 10, end: 10 }), [row(true), row(false)]);
  assert.deepEqual(await client.l2Snapshots({ source: "hyperliquid", market: "0G", start: 10, end: 300010 }), [row(false)]);
  assert.deepEqual(await client.l2Snapshots({ source: "hyperliquid", market: "0G", start: 10, end: 600010 }), [row(false)]);
  const bbo = await client.bbo({ source: "hyperliquid", market: "0G", start: 10, end: 600010 });
  assert.equal(bbo[0].bid_price, 100);
  assert.equal(bbo[0].ask_price, 101);
  const depth = await client.depthMetrics({ source: "hyperliquid", market: "0G", start: 10, end: 600010 });
  assert.equal(depth[0].bid_depth_notional, 200);
  assert.equal(depth[0].ask_depth_notional, 101);
  assert.equal(calls.length, 6);
  assert.equal(calls[0].url.searchParams.get("instrument"), "0G");
  assert.equal(calls[1].url.searchParams.get("cursor"), "next");
  assert.equal(calls[2].url.searchParams.get("end"), "300010");
  assert.equal(calls[4].url.searchParams.get("end"), "600010");
  assert.equal(calls[0].headers.Authorization, "Bearer secret");
  client.close();
});

test("direct historical rows paginate, filter, and keep flat nullable fields", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const identity = { source: "deribit", market: "BTC", source_capture_id: "capture", schema_version: 1 };
  const trade = { ...identity, event_id: "t1", collector_timestamp: 10, price: 100, quantity: 2, side: null };
  const option = { ...identity, event_id: "o1", collector_timestamp: 10, instrument: "BTC-29MAR24-50000-C", delta: "0.431", mark_price: null };
  const funding = { ...identity, event_id: "f1", collector_timestamp: 10, funding_rate: null, mark_price: "100" };
  const fundingWithoutMark = { ...identity, event_id: "f2", collector_timestamp: 11, funding_rate: "0.0001", mark_price: null };
  const calls = [];
  const client = new PolarisClient({ baseUrl: "https://api.example", apiKey: "secret", fetch: async (input, init) => {
    const url = new URL(input);
    calls.push({ url, headers: init.headers });
    let body;
    if (url.pathname === "/historical/trades") {
      body = url.searchParams.has("cursor")
        ? { items: [trade], has_more: false, next_cursor: null }
        : { items: [trade], has_more: true, next_cursor: "next" };
    } else if (url.pathname === "/historical/options-ticker") {
      body = { items: [option], has_more: false, next_cursor: null };
    } else {
      body = { items: [funding, fundingWithoutMark], has_more: false, next_cursor: null };
    }
    return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  } });

  const trades = await client.trades({ source: "deribit", market: "BTC", start: 10, end: 10 });
  assert.deepEqual(trades, [trade, trade]);
  assert.equal(calls[0].url.searchParams.get("start"), "10");
  assert.equal(calls[0].url.searchParams.get("end"), "10");
  assert.equal(calls[0].url.searchParams.get("limit"), "1000");
  assert.equal(calls[1].url.searchParams.get("cursor"), "next");
  assert.equal(calls[0].headers.Authorization, "Bearer secret");
  const exact = await client.optionTickers({
    source: "deribit",
    market: "BTC",
    instrument: "BTC-29MAR24-50000-C",
  });
  assert.deepEqual(exact, [option]);
  assert.equal(calls[2].url.searchParams.get("instrument"), option.instrument);
  assert.deepEqual(await client.fundingRates({}), [funding, fundingWithoutMark]);
  assert.deepEqual(await client.perpetualTickers({}), [funding, fundingWithoutMark]);
  assert.deepEqual(await client.markPrices({}), [funding]);
  assert.equal(calls[3].url.searchParams.has("start"), false);
  await assert.rejects(
    client.optionTickers({ source: "deribit", market: "BTC", instrument: "" }),
    /instrument must be non-empty/,
  );
  await assert.rejects(
    client.trades({ start: -1 }),
    /start must be a non-negative/,
  );
  client.close();
});

test("rawChannel pages exact captures with required channel and time bounds", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const calls = [];
  const capture = {
    capture_id: "c1", collector_timestamp: 10, recorder_version: "v1", ingested_at: 11,
    additional_context: { channel: "trades" }, original_json: '{ "price": 1.0 }',
  };
  const client = new PolarisClient({ baseUrl: "https://api.example", apiKey: "secret", fetch: async (input, init) => {
    const url = new URL(input);
    calls.push({ url, headers: init.headers });
    const second = url.searchParams.get("cursor") === "next";
    const body = { items: [{ ...capture, capture_id: second ? "c2" : "c1" }],
      has_more: !second, next_cursor: second ? null : "next" };
    return new Response(JSON.stringify(body), { status: 200 });
  } });
  const rows = await client.rawChannel({ exchange: "binance", event: "trades", start: 10, end: 10 });
  assert.deepEqual(rows.map((row) => row.capture_id), ["c1", "c2"]);
  assert.equal(rows[0].original_json, '{ "price": 1.0 }');
  assert.equal(calls[0].url.pathname, "/raw/binance/trades");
  assert.equal(calls[0].url.searchParams.get("start"), "10");
  assert.equal(calls[0].url.searchParams.get("end"), "10");
  assert.equal(calls[0].headers.Authorization, "Bearer secret");
  assert.equal(calls[1].url.searchParams.get("cursor"), "next");
  await assert.rejects(client.rawChannel({ exchange: "", event: "trades", start: 10, end: 10 }), /exchange and event/);
  await assert.rejects(client.rawChannel({ exchange: "binance", event: "trades", start: 11, end: 10 }), /start and end/);
  client.close();
});

test("flat OHLCV, intent, and quote rows use their distinct direct routes", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const identity = { collector_timestamp: 10, source_capture_id: "capture", schema_version: 1 };
  const candle = { ...identity, event_id: "c1", source: "binance", market: "BTC-USDT",
    interval: "1m", open_timestamp: 10, open: 100, high: 102, low: 99, close: 100, is_closed: false };
  const intent = { ...identity, event_id: "i1", source: "uniswapx", market: "intents",
    intent_id: "intent-1", input_asset_id: null };
  const quote = { ...identity, event_id: "q1", source: "propamm", market: "ethereum",
    instrument: "pool-1", observation_id: "obs-1", input_asset_id: "ETH", input_chain_id: "1",
    input_amount: "1000000000000000000", input_decimals: 18, output_asset_id: "USDC",
    output_chain_id: "1", output_amount: "2000000", output_decimals: 6,
    amount_kind: "exact_input", block_number: 100, block_hash: "0xblock",
    transaction_hash: "0xtx", transaction_index: 0, router: "0xrouter", pool: null };
  const calls = [];
  const client = new PolarisClient({ baseUrl: "https://api.example", apiKey: "secret", fetch: async (input, init) => {
    const url = new URL(input);
    calls.push({ url, headers: init.headers });
    const second = url.searchParams.has("cursor");
    const body = url.pathname === "/historical/ohlcv"
      ? { items: [{ ...candle, event_id: second ? "c2" : "c1" }], has_more: !second, next_cursor: second ? null : "next" }
      : { items: [url.pathname === "/historical/intents" ? intent : quote], has_more: false, next_cursor: null };
    return new Response(JSON.stringify(body), { status: 200 });
  } });
  const candles = await client.ohlcvRows({ interval: "1m", start: 10, end: 10 });
  assert.deepEqual(candles.map((row) => row.event_id), ["c1", "c2"]);
  assert.equal(candles[0].open_timestamp, candles[1].open_timestamp);
  assert.equal(calls[0].url.searchParams.get("interval"), "1m");
  assert.equal(calls[1].url.searchParams.get("cursor"), "next");
  assert.deepEqual(await client.intentRows({ intentId: "intent-1" }), [intent]);
  assert.deepEqual(await client.intents({ intentId: "intent-1" }), [intent]);
  assert.equal(calls[2].url.searchParams.get("intent_id"), "intent-1");
  assert.deepEqual(await client.quoteRows({ observationId: "obs-1", instrument: "pool-1" }), [quote]);
  assert.equal(calls[4].url.searchParams.get("observation_id"), "obs-1");
  assert.equal(calls[4].url.searchParams.get("instrument"), "pool-1");
  assert.equal(calls[4].headers.Authorization, "Bearer secret");
  await assert.rejects(client.intentRows({ intentId: " " }), /intentId must be non-empty/);
  client.close();
});

test("venue candle aggregates use latest revisions and reported volumes", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const start = 1_704_067_200_000;
  const identity = { source: "binance", market: "BTC-USDT", source_capture_id: "capture", schema_version: 1 };
  const first = { ...identity, event_id: "open", collector_timestamp: start + 1,
    interval: "1m", open_timestamp: start, open: 100, high: 101, low: 99, close: 101,
    base_volume: 1, quote_volume: 101, trade_count: 2 };
  const final = { ...first, event_id: "closed", collector_timestamp: start + 2,
    high: 103, close: 102, base_volume: 2, quote_volume: 204, trade_count: 5, is_closed: true };
  const fine = [100, 110, 99, 108.9].map((price, i) => ({ ...identity,
    event_id: `fine-${i}`, collector_timestamp: start + i, interval: "10s",
    open_timestamp: start + i * 10_000, open: price, high: price, low: price, close: price }));
  const client = new PolarisClient({ baseUrl: "https://api.example", fetch: async (input) => {
    const url = new URL(input);
    assert.equal(url.pathname, "/historical/ohlcv");
    assert.equal(url.searchParams.get("start"), String(start));
    assert.equal(url.searchParams.get("end"), String(start + 60_000));
    const second = url.searchParams.has("cursor");
    const body = url.searchParams.get("interval") === "1m"
      ? { items: [second ? final : first], has_more: !second, next_cursor: second ? null : "next" }
      : { items: fine, has_more: false, next_cursor: null };
    return new Response(JSON.stringify(body), { status: 200 });
  } });
  const options = { source: "binance", market: "BTC-USDT", interval: "1m",
    from: "2024-01-01T00:00:00Z", to: "2024-01-01T00:01:00Z" };
  assert.deepEqual(await client.ohlcv(options), [{ timestamp: start, open: 100, high: 103,
    low: 99, close: 102, volume: 2, trades: 5 }]);
  assert.deepEqual(await client.volume(options), [{ timestamp: start, volume: 2 }]);
  assert.deepEqual(await client.vwap(options), [{ timestamp: start, vwap: 102,
    volume: 2, quote_volume: 204, trades: 5 }]);
  assert.equal((await client.ohlcvTradingView(options)).candles[0].c, 102);
  const volatility = await client.volatility(options);
  assert.equal(volatility[0].returns, 3);
  assert.ok(volatility[0].volatility > 0);
  client.close();
});

test("new event shapes are typed, filtered, and accepted by v2 decoding", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const fixture = await readFile(
    new URL("../../tests/fixtures/events/new-event-shapes-v2.jsonl", import.meta.url),
    "utf8",
  );
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  const lines = fixture.trim().split("\n");
  const decoded = client._decodeSnapshotLines(lines, "new-event-shapes-v2.jsonl");

  assert.deepEqual(decoded.map(({ type }) => type), [
    "perpetual_ticker",
    "perpetual_ticker",
    "option_ticker",
    "trade",
    "trade",
    "intent",
  ]);
  assert.equal(decoded[2].data.underlying, "BTC");
  assert.equal(decoded[2].data.strike, "50000");
  assert.equal(decoded[2].data.option_type, "call");
  assert.equal(decoded[3].data.maker, "0xmaker");
  assert.equal(decoded[3].data.taker, "0xtaker");
  assert.equal("maker" in decoded[4].data, false);

  client._resolveHistoricalRange = async () => ({ fromMs: 0, toMs: 10 });
  client._readSnapshotEvents = async function* (_source, _market, _from, _to, filter) {
    for (const row of decoded) if (!filter || filter(row)) yield structuredClone(row);
  };
  for (const badData of [{}, { funding_rate: 0.1 }]) {
    const malformed = [...lines];
    const row = JSON.parse(malformed[1]);
    row.data = badData;
    malformed[1] = JSON.stringify(row);
    assert.throws(
      () => client._decodeSnapshotLines(malformed, "bad-perpetual-ticker.jsonl"),
      /Invalid v2 perpetual ticker payload/,
    );
  }
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

test("v2 decoder accepts option tickers only with exact instrument identity", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const fixture = await readFile(
    new URL("../../tests/fixtures/events/options-v2.jsonl", import.meta.url),
    "utf8",
  );
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  const lines = fixture.trim().split("\n");
  const decoded = client._decodeSnapshotLines(lines, "options-v2.jsonl");
  assert.deepEqual(decoded.slice(0, 2).map(({ market }) => market), ["BTC", "BTC"]);
  assert.deepEqual(decoded.slice(0, 2).map(({ instrument }) => instrument), [
    "BTC-29MAR24-50000-C",
    "BTC-29MAR24-45000-P",
  ]);

  const malformed = [...lines];
  const missingInstrument = JSON.parse(malformed[1]);
  delete missingInstrument.instrument;
  malformed[1] = JSON.stringify(missingInstrument);
  assert.throws(
    () => client._decodeSnapshotLines(malformed, "missing-option-instrument.jsonl"),
    /Invalid v2 option ticker payload/,
  );
  client.close();
});

test("v2 decoder accepts canonical intents and rejects malformed nested payloads", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const fixture = await readFile(
    new URL("../../tests/fixtures/events/intents-v2.jsonl", import.meta.url),
    "utf8",
  );
  const client = new PolarisClient({ baseUrl: "https://api.example" });
  const lines = fixture.trim().split("\n");
  const decoded = client._decodeSnapshotLines(lines, "intents-v2.jsonl");
  assert.equal(decoded.filter(({ type }) => type === "intent").length, 4);
  assert.deepEqual(
    decoded.filter(({ type }) => type === "intent").map(({ collector_sequence }) => collector_sequence),
    [1, 2, 3, 5],
  );

  const malformed = [...lines];
  const missingAssetId = JSON.parse(malformed[1]);
  delete missingAssetId.data.inputs[0].asset_id;
  malformed[1] = JSON.stringify(missingAssetId);
  assert.throws(
    () => client._decodeSnapshotLines(malformed, "missing-intent-asset.jsonl"),
    /Invalid v2 intent payload/,
  );

  const invalidStatus = [...lines];
  const badStatus = JSON.parse(invalidStatus[2]);
  badStatus.data.status = "pending_forever";
  invalidStatus[2] = JSON.stringify(badStatus);
  assert.throws(
    () => client._decodeSnapshotLines(invalidStatus, "invalid-intent-status.jsonl"),
    /Invalid v2 intent payload/,
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
