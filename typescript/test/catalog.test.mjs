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
