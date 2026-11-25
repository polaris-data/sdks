import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { startFixture } from "../sdk/typescript/test/fixture.mjs";

const archive = path.resolve(process.argv[2]);
const work = await mkdtemp(path.join(tmpdir(), "polaris-npm-"));
const fixture = await startFixture();
try {
  await writeFile(path.join(work, "package.json"), '{"name":"polaris-consumer","private":true}');
  execFileSync("npm", ["install", "--ignore-scripts", archive], { cwd: work, stdio: "inherit" });
  const code = `
    import assert from "node:assert/strict";
    import { OhlcvAggregator, OrderbookBuilder, PolarisClient } from "polaris-data";
    const client = new PolarisClient({ baseUrl: process.argv[1], apiKey: "fixture-key" });
    try {
      assert.equal((await client.health()).ok, true);
      const ids = [];
      for await (const row of client.dataRows({ table: "standard.trades" })) ids.push(row.id);
      assert.equal(ids.length, 2);
      const cursors = [];
      for await (const page of client.stream({ subscriptions: [{ table: "standard.trades" }] })) cursors.push(page.cursor);
      assert.deepEqual(cursors, ["stream:1", "stream:2"]);
      const books = new OrderbookBuilder();
      books.update({ timestamp: 1, source: "fixture", market: "BTC", type: "orderbook", data: { bids: [[99, 1]], asks: [] } });
      assert.equal(books.snapshot("fixture", "BTC").bids[0].price, 99);
      const bars = new OhlcvAggregator("1m");
      bars.add(1, 100, 2);
      assert.equal(bars.finish()[0].volume, 2);
    } finally {
      client.close();
    }
  `;
  assert.ok(fixture.url.startsWith("http://"));
  execFileSync(process.execPath, ["--input-type=module", "-e", code, fixture.url], {
    cwd: work, stdio: "inherit", timeout: 30_000,
  });
  execFileSync(process.execPath, ["-e", "require('polaris-data')"], { cwd: work, stdio: "inherit" });
  if (process.env.POLARIS_TEST_BROWSER) {
    const tests = new URL("../sdk/typescript/test/", import.meta.url);
    await copyFile(new URL("fixtures/browser-entry.js", tests), path.join(work, "browser-entry.js"));
    execFileSync(process.execPath, [
      "--test", "--test-name-pattern=installed browser bundle",
      fileURLToPath(new URL("browser-bundle.test.mjs", tests)),
    ], {
      cwd: work,
      env: { ...process.env, POLARIS_BROWSER_CONSUMER: work },
      stdio: "inherit",
      timeout: 30_000,
    });
  }
} finally {
  fixture.close();
  await rm(work, { recursive: true, force: true });
}
