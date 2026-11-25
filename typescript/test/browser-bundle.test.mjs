import assert from "node:assert/strict";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { fileURLToPath } from "node:url";

import webpack from "webpack";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "..");

async function collectFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return collectFiles(entryPath);
    }
    return [entryPath];
  }));
  return files.flat();
}

async function runWebpackBundle(outputPath) {
  const compiler = webpack({
    mode: "production",
    context: repoRoot,
    target: ["web", "es2020"],
    entry: path.join(testDir, "fixtures", "browser-entry.js"),
    output: {
      path: outputPath,
      filename: "bundle.js",
    },
    resolve: {
      alias: {
        "polaris-data$": path.join(repoRoot, "dist", "browser", "index.js"),
      },
    },
  });

  await new Promise((resolve, reject) => {
    compiler.run((error, stats) => {
      void compiler.close(() => undefined);

      if (error) {
        reject(error);
        return;
      }

      if (!stats || stats.hasErrors()) {
        reject(new Error(stats?.toString({ all: false, errors: true }) ?? "Webpack build failed"));
        return;
      }

      resolve();
    });
  });
}

test("browser build does not contain node builtins", async () => {
  const browserDist = path.join(repoRoot, "dist", "browser");
  const files = await collectFiles(browserDist);
  assert.notEqual(files.length, 0, "expected browser dist files to exist");

  for (const filePath of files.filter((candidate) => candidate.endsWith(".js"))) {
    const contents = await readFile(filePath, "utf8");
    assert.equal(contents.includes("fs/promises"), false, `${filePath} still references fs/promises`);
    assert.equal(contents.includes("node:fs"), false, `${filePath} still references node:fs`);
    assert.equal(contents.includes("node:path"), false, `${filePath} still references node:path`);
    assert.equal(contents.includes("node:os"), false, `${filePath} still references node:os`);
  }
});

test("browser dist bundles cleanly in webpack", async () => {
  const outputPath = await mkdtemp(path.join(tmpdir(), "polaris-data-webpack-"));
  try {
    await runWebpackBundle(outputPath);
  } finally {
    await rm(outputPath, { recursive: true, force: true });
  }
});

test("installed browser bundle can page and resume with or without Wasm",
  { timeout: 20_000, skip: !process.env.POLARIS_TEST_BROWSER }, async () => {
    const { chromium } = await import("playwright");
    const { createServer } = await import("node:http");
    const { startFixture } = await import("./fixture.mjs");
    const outputPath = await mkdtemp(path.join(tmpdir(), "polaris-data-browser-"));
    let browser;
    let fixture;
    let server;
    try {
      await runWebpackBundle(outputPath);
      const scripts = new Map();
      for (const file of await collectFiles(outputPath)) {
        if (file.endsWith(".js")) scripts.set("/" + path.relative(outputPath, file), await readFile(file));
      }
      fixture = await startFixture();
      browser = await chromium.launch({ executablePath: process.env.POLARIS_BROWSER_EXECUTABLE });
      server = createServer((req, res) => {
        const script = scripts.get(req.url);
        if (script) {
          res.writeHead(200, { "Content-Type": "text/javascript" }).end(script);
          return;
        }
        res.writeHead(200, {
          "Content-Type": "text/html",
          "Content-Security-Policy": `script-src 'self'${req.url === "/wasm" ? " 'wasm-unsafe-eval'" : ""}`,
        }).end('<!doctype html><title>Polaris SDK fixture</title><script defer src="/bundle.js"></script>');
      });
      await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
      const page = await browser.newPage();
      for (const mode of ["wasm", "csp"]) {
        await page.goto(`http://127.0.0.1:${server.address().port}/${mode}`);
        await page.waitForFunction(() => typeof globalThis.runPolarisFixture === "function");
        const wasmAllowed = await page.evaluate(async () => {
          try {
            await WebAssembly.compile(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]));
            return true;
          } catch {
            return false;
          }
        });
        assert.equal(wasmAllowed, mode === "wasm");
        assert.deepEqual(await page.evaluate((url) => globalThis.runPolarisFixture(url), fixture.url), {
          health: { ok: true },
          ids: ["18446744073709551614", "18446744073709551615"],
          cursors: ["stream:1", "stream:2"],
          bars: [
            { timestamp: 0, open: 100, high: 100, low: 100, close: 100, volume: 1, trades: 1 },
            { timestamp: 60_000, open: 100, high: 101, low: 101, close: 101, volume: 2, trades: 1 },
          ],
        });
      }
    } finally {
      await browser?.close();
      fixture?.close();
      if (server?.listening) await new Promise((resolve) => server.close(resolve));
      await rm(outputPath, { recursive: true, force: true });
    }
  },
);
