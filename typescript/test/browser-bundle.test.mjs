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
