import { defineConfig } from "tsup";

export default defineConfig([
  // Node.js build (current, maintained for backward compatibility)
  {
    entry: {
      index: "src/index.node.ts",
    },
    format: ["esm", "cjs"],
    dts: true,
    sourcemap: true,
    clean: false,
    target: "node18",
    outDir: "dist/node",
    platform: "node",
  },
  // Browser build (new, for browser environments)
  {
    entry: {
      index: "src/index.browser.ts",
    },
    format: ["esm"],
    dts: true,
    sourcemap: true,
    clean: false,
    target: "es2020",
    outDir: "dist/browser",
    platform: "browser",
    // Exclude Node.js-specific code from browser build
    external: [],
  },
]);
