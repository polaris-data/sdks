import assert from "node:assert/strict";
import test from "node:test";

test("catalog exposes provider symbols and falls back to market", async () => {
  const fetch = async (url) => {
    const parsed = new URL(url);
    assert.equal(parsed.pathname, "/catalog");

    return new Response(JSON.stringify({
      updatedAt: "2026-08-08T07:14:24.077Z",
      markets: [
        {
          source: "arcus",
          market: "AAPL-USD",
          symbol: "AAPLUSD",
        },
        {
          source: "binance",
          market: "BTC-USDT",
        },
      ],
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient({
    apiKey: "test-key",
    baseUrl: "https://api.example",
    fetch,
  });

  const catalog = await client.catalog();

  assert.equal(catalog.markets[0].symbol, "AAPLUSD");
  assert.equal(catalog.markets[1].symbol, "BTC-USDT");
});

test("catalog auto-paginates across cursor pages", async () => {
  const fetch = async (url) => {
    const parsed = new URL(url);
    assert.equal(parsed.pathname, "/catalog");

    if (parsed.searchParams.get("cursor") === "next-token") {
      return new Response(JSON.stringify({
        updatedAt: "2026-08-08T07:14:24.077Z",
        has_more: false,
        next_cursor: null,
        markets: [{ source: "hyperliquid", market: "BTC" }],
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({
      updatedAt: "2026-08-08T07:14:24.077Z",
      has_more: true,
      next_cursor: "next-token",
      markets: [
        { source: "arcus", market: "AAPL-USD", symbol: "AAPLUSD" },
        { source: "binance", market: "BTC-USDT" },
      ],
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient({
    apiKey: "test-key",
    baseUrl: "https://api.example",
    fetch,
  });

  const catalog = await client.catalog();

  assert.equal(catalog.markets.length, 3);
  assert.equal(catalog.markets[0].symbol, "AAPLUSD");
  assert.equal(catalog.markets[1].symbol, "BTC-USDT");
  assert.equal(catalog.markets[2].symbol, "BTC");
});

test("instruments follows cursors and preserves option contract metadata", async () => {
  const calls = [];
  const first = {
    source: "deribit", market: "BTC", instrument: "BTC-1OCT26-70000-C",
    status: "active", option_type: "call", underlying: "BTC", strike: "70000.0",
    expiry_timestamp: 1790812800000, contract_size: "1.0", premium_currency: "BTC",
    statistics: { source: "deribit", market: "BTC", instrument: "BTC-1OCT26-70000-C",
      fields: { latest_price: { value: "0.025", observed_at: 1790800000000, unit: "BTC" } } },
  };
  const second = { ...first, instrument: "BTC-1OCT26-75000-C", strike: "75000.0", statistics: null };
  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient({ baseUrl: "https://api.example", fetch: async (input) => {
    const url = new URL(input);
    calls.push(url);
    assert.equal(url.pathname, "/catalog/instruments");
    const later = url.searchParams.has("cursor");
    const exact = url.searchParams.has("instrument");
    return new Response(JSON.stringify({
      updatedAt: "2026-09-30T00:00:00Z",
      instruments: later ? [second] : [first],
      next_cursor: later || exact ? null : "contract-cursor",
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  } });
  const result = await client.instruments({
    source: "deribit", market: "BTC", expiry: first.expiry_timestamp, optionType: "call",
  });
  assert.deepEqual(result, { updatedAt: "2026-09-30T00:00:00Z", instruments: [first, second] });
  assert.equal(calls.length, 2);
  for (const url of calls) {
    for (const [key, value] of Object.entries({ source: "deribit", market: "BTC",
      expiry: String(first.expiry_timestamp), option_type: "call", limit: "1000" })) {
      assert.equal(url.searchParams.get(key), value);
    }
  }
  assert.equal(calls[1].searchParams.get("cursor"), "contract-cursor");
  const exact = await client.instruments({ source: "deribit", market: "BTC",
    instrument: first.instrument, q: "70000" });
  assert.deepEqual(exact.instruments, [first]);
  assert.equal(calls[2].searchParams.get("instrument"), first.instrument);
  assert.equal(calls[2].searchParams.get("q"), "70000");
  await assert.rejects(client.instruments({ source: "deribit", market: "BTC", expiry: -1 }), /expiry/);
  assert.equal(calls.length, 3);
  client.close();
});

test("count returns catalog totals", async () => {
  const fetch = async (url) => {
    const parsed = new URL(url);
    assert.equal(parsed.pathname, "/count");

    return new Response(JSON.stringify({
      updatedAt: "2026-08-08T07:14:24.077Z",
      sources: 46,
      markets: 3645,
      by_source: { binance: 10, hyperliquid: 558 },
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  const { PolarisClient } = await import("../dist/node/index.js");
  const client = new PolarisClient({
    apiKey: "test-key",
    baseUrl: "https://api.example",
    fetch,
  });

  const count = await client.count();

  assert.equal(count.sources, 46);
  assert.equal(count.markets, 3645);
  assert.equal(count.by_source.hyperliquid, 558);
});
