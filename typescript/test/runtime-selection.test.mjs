import assert from "node:assert/strict";
import test from "node:test";

test("browser entry ignores datasetRoot and uses BrowserStorage", async () => {
  const { PolarisClient } = await import("../dist/browser/index.js");

  const client = new PolarisClient({
    apiKey: "browser-key",
    datasetRoot: "/tmp/should-be-ignored",
  });

  assert.equal(client._root, "polaris");
  const storage = await client._getStorage();
  assert.notEqual(storage.constructor.name, "NodeStorage");
});

test("node entry respects POLARIS_ROOT and uses NodeStorage", async () => {
  const previousRoot = process.env.POLARIS_ROOT;
  process.env.POLARIS_ROOT = "/tmp/polaris-root";

  try {
    const { PolarisClient } = await import("../dist/node/index.js");
    const client = new PolarisClient({ apiKey: "node-key" });

    assert.equal(client._root, "/tmp/polaris-root");
    const storage = await client._getStorage();
    assert.equal(storage.constructor.name, "NodeStorage");
  } finally {
    if (previousRoot === undefined) {
      delete process.env.POLARIS_ROOT;
    } else {
      process.env.POLARIS_ROOT = previousRoot;
    }
  }
});
