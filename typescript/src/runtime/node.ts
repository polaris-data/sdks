import WebSocket from "ws";

import type { PolarisRuntime } from "./types";

export const nodeRuntime: PolarisRuntime = {
  resolveApiKey(explicit) {
    return explicit ?? process.env.POLARIS_API_KEY;
  },

  createWebSocket(url) {
    return new WebSocket(url) as unknown as import("./types").WebSocketLike;
  },
};
