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
