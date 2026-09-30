import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("snapshot and replay methods are absent", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient();
  for (const name of ["events", "replay", "listSnapshots", "getSnapshotDownloadUrls", "propammQuoteLadders"]) {
    assert.equal(name in client, false, `${name} should be removed`);
  }
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

test("TypeScript BBO filters quote changes and buckets reconstructed books", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const base = 1_700_000_000_000;
  const row = (offset, bidPrice, bidQuantity, askPrice, askQuantity) => {
    const value = {
      event_id: String(offset), source: "hyperliquid", market: "0G", instrument: null,
      collector_timestamp: base + offset, exchange_timestamp: null,
      source_capture_id: String(offset), schema_version: 1, source_event_is_snapshot: false,
    };
    for (let index = 0; index < 25; index++) {
      for (const side of ["bid", "ask"]) for (const field of ["px", "sz"]) {
        value[`${side}_${field}_${String(index).padStart(2, "0")}`] = null;
      }
    }
    value.bid_px_00 = bidPrice;
    value.bid_sz_00 = bidQuantity;
    value.ask_px_00 = askPrice;
    value.ask_sz_00 = askQuantity;
    return value;
  };
  const rows = [
    row(100, 100, 2, 101, 3),
    row(800, 100, 2, 101, 3), // Only deeper levels changed.
    row(1200, 100, 2, 101, 4), // Quantity changes also count.
    row(1500, 100, 2, 101, 4),
    row(3100, 100.25, 1, 101, 4),
    row(3200, 100.25, 1, null, null), // No two-sided BBO.
  ];
  const calls = [];
  const client = new PolarisClient({ baseUrl: "https://api.example", fetch: async (input) => {
    const url = new URL(input);
    calls.push(url);
    assert.equal(url.pathname, "/historical/l2-orderbooks");
    const second = url.searchParams.has("cursor");
    return new Response(JSON.stringify({
      items: second ? rows.slice(3) : rows.slice(0, 3),
      has_more: !second, next_cursor: second ? null : "next",
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  } });
  const options = { source: "hyperliquid", market: "0G", start: base, end: base + 4000 };
  const quote = (timestamp, bid_price, bid_quantity, ask_price, ask_quantity) =>
    ({ timestamp, bid_price, bid_quantity, ask_price, ask_quantity });
  const expected = [
    quote(base + 100, 100, 2, 101, 3),
    quote(base + 800, 100, 2, 101, 3),
    quote(base + 1200, 100, 2, 101, 4),
    quote(base + 1500, 100, 2, 101, 4),
    quote(base + 3100, 100.25, 1, 101, 4),
  ];
  assert.deepEqual(await client.bbo(options), expected);
  assert.deepEqual(await client.bbo({ ...options, changesOnly: true }),
    [expected[0], expected[2], expected[4]]);
  const bucketed = [
    { ...expected[1], timestamp: base },
    { ...expected[3], timestamp: base + 1000 },
    { ...expected[4], timestamp: base + 3000 },
  ];
  assert.deepEqual(await client.bbo({ ...options, interval: "1s" }), bucketed);
  assert.deepEqual(await client.bbo({ ...options, interval: "1s", changesOnly: true }), bucketed);
  assert.equal(calls.length, 8);
  assert.equal(calls[0].searchParams.get("start"), String(base));
  assert.equal(calls[0].searchParams.get("end"), String(base + 4000));
  assert.equal(calls[1].searchParams.get("cursor"), "next");
  assert.equal(calls[0].searchParams.has("interval"), false);
  assert.equal(calls[0].searchParams.has("changesOnly"), false);
  await assert.rejects(client.bbo({ ...options, interval: "2s" }), /Invalid interval: 2s/);
  assert.equal(calls.length, 8);
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
