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

  createWebSocket(url) {
    const Constructor = (globalThis as unknown as {
      WebSocket?: new (value: string) => import("./types").WebSocketLike;
    }).WebSocket;
    if (!Constructor) throw new Error("WebSocket is not available in this browser");
    return new Constructor(url);
  },
};
