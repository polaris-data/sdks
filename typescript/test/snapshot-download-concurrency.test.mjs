import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

test("snapshot downloads use bounded parallelism", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "polaris-downloads-"));

  let inFlightDownloads = 0;
  let maxInFlightDownloads = 0;
  let manifestRequests = 0;
  let downloadRequests = 0;

  const snapshotKeys = Array.from({ length: 6 }, (_, index) =>
    `standard-binance-btc-usdt-2026-07-20-${index}`,
  );

  const fetch = async (url) => {
    const parsed = new URL(url);

    if (parsed.pathname === "/download") {
      manifestRequests += 1;
      return new Response(JSON.stringify({
        source: "binance",
        market: "btc-usdt",
        date: "2026-07-20",
        total: snapshotKeys.length,
        total_bytes: snapshotKeys.length * 3,
        snapshots: snapshotKeys.map((key, index) => ({
          date: "2026-07-20",
          timestamp: `2026-07-20T${String(index).padStart(2, "0")}:00:00Z`,
          key,
          url: `https://files.example/${key}.jsonl.zst`,
          expires_in_seconds: 300,
        })),
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }

    if (parsed.hostname === "files.example") {
      downloadRequests += 1;
      inFlightDownloads += 1;
      maxInFlightDownloads = Math.max(maxInFlightDownloads, inFlightDownloads);
      await new Promise((resolve) => setTimeout(resolve, 25));
      inFlightDownloads -= 1;

      return new Response(new Uint8Array([1, 2, 3]), { status: 200 });
    }

    throw new Error(`Unexpected URL: ${url}`);
  };

  try {
    const { PolarisClient } = await import("../dist/node/index.js");
    const client = new PolarisClient({
      apiKey: "test-key",
      datasetRoot: root,
      fetch,
      snapshotDownloadConcurrency: 2,
    });

    const layout = await client._getLayout();
    const ensured = await client._ensureLocalSnapshots(
      snapshotKeys.map((key, hour) => ({
        key,
        source: "binance",
        market: "btc-usdt",
        date: "2026-07-20",
        hour,
      })),
      layout,
    );

    assert.equal(ensured.length, snapshotKeys.length);
    assert.equal(manifestRequests, 1);
    assert.equal(downloadRequests, snapshotKeys.length);
    assert.equal(maxInFlightDownloads, 2);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
