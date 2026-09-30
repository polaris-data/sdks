import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("removed client methods are absent", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient();
  for (const name of ["events", "replay", "listSnapshots", "getSnapshotDownloadUrls", "propammQuoteLadders", "bboChanges", "bbo", "depthMetrics", "intentRows", "ohlcvRows", "quoteRows", "volume", "vwap", "volatility", "markPrices"]) {
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
  assert.equal(calls.length, 4);
  assert.equal(calls[0].url.searchParams.get("instrument"), "0G");
  assert.equal(calls[1].url.searchParams.get("cursor"), "next");
  assert.equal(calls[2].url.searchParams.get("end"), "300010");
  assert.equal(calls[3].url.searchParams.get("end"), "600010");
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

test("intents use the direct route with an exact filter", async () => {
  const { PolarisClient } = await import("../dist/node/index.js");
  const intent = { event_id: "i1", source: "uniswapx", market: "intents",
    collector_timestamp: 10, source_capture_id: "capture", schema_version: 1,
    intent_id: "intent-1", input_asset_id: null };
  const calls = [];
  const client = new PolarisClient({ baseUrl: "https://api.example", apiKey: "secret", fetch: async (input, init) => {
    const url = new URL(input);
    calls.push({ url, headers: init.headers });
    assert.equal(url.pathname, "/historical/intents");
    return new Response(JSON.stringify({ items: [intent], has_more: false, next_cursor: null }), { status: 200 });
  } });
  assert.deepEqual(await client.intents({ intentId: "intent-1" }), [intent]);
  assert.equal(calls[0].url.searchParams.get("intent_id"), "intent-1");
  assert.equal(calls[0].headers.Authorization, "Bearer secret");
  await assert.rejects(client.intents({ intentId: " " }), /intentId must be non-empty/);
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
  assert.equal((await client.ohlcvTradingView(options)).candles[0].c, 102);
  client.close();
});
