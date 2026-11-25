import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import test from "node:test";

import { AccessDeniedError, PolarisClient, RateLimitedError, StreamProtocolError, UnauthorizedError } from "polaris-data";
import { startFixture } from "./fixture.mjs";

const table = "standard.trades";
const exact = "0.12345678901234567890123456789012345678";

test("generated routes preserve delivery shape, precision and custom fetch", async () => {
  const fixture = await startFixture();
  const requests = [];
  const client = new PolarisClient({
    apiKey: "fixture-key",
    baseUrl: fixture.url,
    fetch: (...args) => {
      requests.push(args);
      return fetch(...args);
    },
  });
  try {
    assert.deepEqual(await client.health(), { ok: true });
    assert.equal((await client.openapi()).openapi, "3.1.0");
    assert.equal((await client.coverage()).tables[0].row_count, 9_007_199_254_740_993n);
    assert.deepEqual((await client.sql({ query: "SELECT 1" })).data, [[1]]);
    const candles = await client.candles({ source: "uniswap-v3", chain_id: 1, pool: "pool", interval: "1m", start: 0, end: 60_000 });
    assert.deepEqual(candles.data, [{ close: exact }]);
    assert.equal(requests[0][1].headers.Authorization, undefined);
    const rows = client.dataRows({ table, filters: { "market.eq": "BTC/USDC + test" } });
    assert.equal(requests.length, 5);
    assert.equal((await rows.next()).value.price, exact);
    assert.equal(requests.length, 6);
    assert.equal((await rows.next()).value.price, null);
    assert.equal((await rows.next()).done, true);
    assert.match(requests[5][0], /market.eq=BTC%2FUSDC%20%2B%20test/);
    assert.equal(requests[5][1].headers.Authorization, "Bearer fixture-key");
  } finally {
    client.close();
    fixture.close();
  }
});

test("errors and malformed pages stay typed", async () => {
  const fixture = await startFixture();
  const client = new PolarisClient({ apiKey: "fixture-key", baseUrl: fixture.url });
  const publicClient = new PolarisClient({ apiKey: "", baseUrl: fixture.url });
  try {
    await assert.rejects(() => publicClient.data({ table }), UnauthorizedError);
    await assert.rejects(() => client.data({ table, filters: { scenario: "forbidden" } }),
      (error) => error instanceof AccessDeniedError && error.code === "forbidden" && error.statusCode === 403);
    await assert.rejects(() => client.coverage({ table: "rate-limited" }),
      (error) => error instanceof RateLimitedError && error.resetAt === "60");
    await assert.rejects(() => client.data({ table, filters: { scenario: "malformed" } }), /Invalid Polaris response/);
    await assert.rejects(async () => {
      for await (const row of client.dataRows({ table, filters: { scenario: "repeat" } })) assert.ok(row);
    }, /repeated a cursor/);
  } finally {
    publicClient.close();
    client.close();
    fixture.close();
  }
});

test("stream resumes consumed cursors and close wakes a pending read", async () => {
  const fixture = await startFixture();
  const client = new PolarisClient({ apiKey: "fixture-key", baseUrl: fixture.url });
  try {
    const cursors = [];
    for await (const page of client.stream({ subscriptions: [{ table }] })) cursors.push(page.cursor);
    assert.deepEqual(cursors, ["stream:1", "stream:2"]);
    const quiet = client.stream({ subscriptions: [{ table, filters: { scenario: "quiet" } }] });
    const pending = quiet[Symbol.asyncIterator]().next();
    await sleep(100);
    quiet.close();
    assert.equal((await pending).done, true);
    const malformed = client.stream({ subscriptions: [{ table, filters: { scenario: "malformed" } }] });
    await assert.rejects(() => malformed[Symbol.asyncIterator]().next(), StreamProtocolError);
  } finally {
    client.close();
    fixture.close();
  }
});
