import type { PolarisRuntime } from "./types";

export const browserRuntime: PolarisRuntime = {
  resolveApiKey(explicit) {
    return explicit;
  },

  createWebSocket(url) {
    const Constructor = (globalThis as unknown as {
      WebSocket?: new (value: string) => import("./types").WebSocketLike;
    }).WebSocket;
    if (!Constructor) throw new Error("WebSocket is not available in this browser");
    return new Constructor(url);
  },
};
