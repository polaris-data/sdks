import assert from "node:assert/strict";
import test from "node:test";
import { WebSocketServer } from "ws";

function startServer(onConnection) {
  return new Promise((resolve) => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    server.on("connection", onConnection);
    server.on("listening", () => {
      const address = server.address();
      resolve({ server, url: `ws://127.0.0.1:${address.port}/stream` });
    });
  });
}

function closeServer(server) {
  for (const client of server.clients) client.terminate();
  return new Promise((resolve) => server.close(resolve));
}

test("stream subscribes with token, deduplicates markets, and yields standard events", async () => {
  let command;
  const { server, url } = await startServer((socket) => {
    socket.once("message", (payload) => {
      command = JSON.parse(payload.toString());
      socket.send(JSON.stringify({
        type: "ack",
        request_id: "polaris-sdk-subscribe",
        action: "subscribe",
        changed: 2,
        active_subscriptions: 2,
      }));
      socket.send(JSON.stringify({
        source: "binance",
        market: "BTC-USDT",
        timestamp: "2026-08-06T12:00:00Z",
        kind: {
          type: "persistence_checkpoint",
          stream: "standard",
          reason: "manual",
          persisted_through_timestamp: "2026-08-06T12:00:00Z",
        },
      }));
      socket.send(JSON.stringify({
        source: "binance",
        market: "BTC-USDT",
        timestamp: "2026-08-06T12:00:01Z",
        kind: {
          type: "data",
          stream: "standard",
          event: {
            timestamp: 1786017601000,
            type: "trade",
            data: { price: 1, quantity: 2, side: "buy" },
          },
        },
      }));
    });
  });

  try {
    const { PolarisClient } = await import("../dist/node/index.js");
    const client = new PolarisClient({ apiKey: "secret", streamUrl: url });
    const realtime = client.stream({
      source: " binance ",
      markets: ["BTC-USDT", "BTC-USDT", "ETH-USDT"],
      includeBuffer: true,
    });
    let received;
    for await (const event of realtime) {
      received = event;
      break;
    }
    assert.equal(command.token, "secret");
    assert.equal(command.include_buffer, true);
    assert.deepEqual(command.subscriptions.map(({ source, market, stream }) => ({ source, market, stream })), [
      { source: "binance", market: "BTC-USDT", stream: "standard" },
      { source: "binance", market: "ETH-USDT", stream: "standard" },
    ]);
    assert.equal(received.source, "binance");
    assert.equal(received.market, "BTC-USDT");
    assert.equal(received.type, "trade");
    client.close();
  } finally {
    await closeServer(server);
  }
});

test("stream reconnects and resubscribes after an abnormal close", async () => {
  let connections = 0;
  const { server, url } = await startServer((socket) => {
    const attempt = ++connections;
    socket.once("message", () => {
      socket.send(JSON.stringify({
        type: "ack",
        request_id: "polaris-sdk-subscribe",
        action: "subscribe",
        changed: 1,
        active_subscriptions: 1,
      }));
      if (attempt === 1) socket.close(1013, "retry");
      else {
        socket.send(JSON.stringify({
          source: "afx",
          market: "AAPLUSDC",
          timestamp: "2026-08-06T12:00:01Z",
          kind: {
            type: "data",
            stream: "standard",
            event: {
              timestamp: 1786017601000,
              source: "afx",
              market: "AAPLUSDC",
              type: "point",
              data: { series: "mark_price", value: "1.0" },
            },
          },
        }));
      }
    });
  });

  try {
    const { PolarisClient } = await import("../dist/node/index.js");
    const client = new PolarisClient({ streamUrl: url });
    const realtime = client.stream({ source: "afx", markets: ["AAPLUSDC"] });
    const event = await Promise.race([
      realtime[Symbol.asyncIterator]().next(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timed out")), 3_000)),
    ]);
    assert.equal(event.value.type, "point");
    assert.equal(connections, 2);
    realtime.close();
    client.close();
  } finally {
    await closeServer(server);
  }
});

test("browser entry uses the browser WebSocket constructor", async () => {
  const previous = globalThis.WebSocket;
  let constructedUrl;
  class BrowserSocket {
    constructor(url) {
      constructedUrl = url;
      this.listeners = new Map();
      queueMicrotask(() => this.listeners.get("open")?.({}));
    }
    addEventListener(type, listener) { this.listeners.set(type, listener); }
    send() {
      queueMicrotask(() => this.listeners.get("message")?.({ data: JSON.stringify({
        type: "error", code: "test", message: "stop",
      }) }));
    }
    close() {}
  }
  globalThis.WebSocket = BrowserSocket;
  try {
    const { PolarisClient, StreamProtocolError } = await import("../dist/browser/index.js");
    const client = new PolarisClient({ streamUrl: "ws://browser.example/stream" });
    const realtime = client.stream({ source: "afx", markets: ["AAPLUSDC"] });
    await assert.rejects(realtime[Symbol.asyncIterator]().next(), StreamProtocolError);
    assert.equal(constructedUrl, "ws://browser.example/stream");
    client.close();
  } finally {
    globalThis.WebSocket = previous;
  }
});
