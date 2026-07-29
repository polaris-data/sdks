import { BrowserStorage } from "../storage/browser/indexeddb";
import type { PolarisRuntime } from "./types";

const BROWSER_ROOT = "polaris";

export const browserRuntime: PolarisRuntime = {
  resolveApiKey(explicit) {
    return explicit;
  },

  resolveRoot() {
    return BROWSER_ROOT;
  },

  async createStorage() {
    return new BrowserStorage();
  },
};
