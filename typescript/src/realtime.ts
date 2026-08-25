import {
  StreamConnectionError,
  StreamProtocolError,
} from "./errors";
import type { StandardEvent, StreamOptions } from "./types";
import { OrderbookBuilder } from "./orderbook";
import type {
  PolarisRuntime,
  WebSocketLike,
} from "./runtime/types";

const MAX_SUBSCRIPTIONS = 1_000;
const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;
const HEALTHY_CONNECTION_MS = 30_000;
const INITIAL_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 30_000;
const SUBSCRIBE_REQUEST_ID = "polaris-sdk-subscribe";

type SocketEvent =
  | { type: "open" }
  | { type: "message"; data: unknown }
  | { type: "close"; code: number; reason: string }
  | { type: "error" }
  | { type: "tick" }
  | { type: "client-close" };

type ParsedMessage =
  | { type: "data"; event: StandardEvent }
  | { type: "pong"; requestId?: string }
  | { type: "control" };

class AsyncQueue<T> {
  private readonly values: T[] = [];
  private readonly waiters: Array<(value: T) => void> = [];

  push(value: T): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter(value);
    else this.values.push(value);
  }

  shift(): Promise<T> {
    const value = this.values.shift();
    if (value !== undefined) return Promise.resolve(value);
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

interface ConnectedSocket {
  socket: WebSocketLike;
  queue: AsyncQueue<SocketEvent>;
}

export class RealtimeStream implements AsyncIterable<StandardEvent> {
  private readonly source: string;
  private readonly markets: string[];
  private readonly instrument: string | undefined;
  private readonly includeBuffer: boolean;
  private readonly materializeOrderbooks: boolean;
  private readonly streamUrl: string;
  private readonly apiKey: string | undefined;
  private readonly runtime: PolarisRuntime;
  private readonly onClose: () => void;
  private socket: WebSocketLike | undefined;
  private queue: AsyncQueue<SocketEvent> | undefined;
  private ticker: ReturnType<typeof setInterval> | undefined;
  private closed = false;
  private started = false;

  constructor(
    options: StreamOptions,
    config: {
      streamUrl: string;
      apiKey?: string;
      runtime: PolarisRuntime;
      onClose: () => void;
    },
  ) {
    this.source = options.source.trim();
    const seen = new Set<string>();
    this.markets = options.markets
      .map((market) => market.trim())
      .filter((market) => market.length > 0 && !seen.has(market) && Boolean(seen.add(market)));
    if (!this.source) throw new StreamProtocolError("stream source must not be empty");
    if (this.markets.length === 0) {
      throw new StreamProtocolError("stream markets must contain at least one non-empty market");
    }
    if (this.markets.length > MAX_SUBSCRIPTIONS) {
      throw new StreamProtocolError(`stream markets must contain at most ${MAX_SUBSCRIPTIONS} unique markets`);
    }
    this.instrument = options.instrument?.trim();
    if (this.instrument !== undefined && this.instrument.length === 0) {
      throw new StreamProtocolError("instrument must be non-empty");
    }
    this.includeBuffer = options.includeBuffer ?? false;
    this.materializeOrderbooks = options.materializeOrderbooks ?? true;
    this.streamUrl = config.streamUrl;
    this.apiKey = config.apiKey;
    this.runtime = config.runtime;
    this.onClose = config.onClose;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.ticker !== undefined) {
      clearInterval(this.ticker);
      this.ticker = undefined;
    }
    this.queue?.push({ type: "client-close" });
    try {
      this.socket?.close(1000, "client closed realtime stream");
    } finally {
      this.onClose();
    }
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<StandardEvent> {
    if (this.started) {
      throw new StreamProtocolError("a realtime stream can only be iterated once");
    }
    this.started = true;
    let connectedOnce = false;
    let backoffMs = INITIAL_BACKOFF_MS;
    const orderbooks = new OrderbookBuilder();

    try {
      while (!this.closed) {
        let connection: ConnectedSocket;
        try {
          connection = await this.connectAndSubscribe();
          connectedOnce = true;
        } catch (error) {
          if (this.closed) return;
          if (!connectedOnce || error instanceof StreamProtocolError) throw error;
          await this.waitToReconnect(backoffMs);
          backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
          continue;
        }

        const connectedAt = Date.now();
        let lastPingAt = Date.now();
        let awaitingPong: { requestId: string; sentAt: number } | undefined;
        let pingCounter = 0;
        let reconnect = false;
        const ticker = setInterval(() => connection.queue.push({ type: "tick" }), 1_000);
        this.ticker = ticker;

        try {
          while (!this.closed) {
            const event = await connection.queue.shift();
            if (event.type === "client-close") return;
            if (event.type === "close") {
              if (event.code === 1000 || event.code === 1001) return;
              reconnect = true;
              break;
            }
            if (event.type === "error") {
              reconnect = true;
              break;
            }
            if (event.type === "message") {
              const parsed = parseServerMessage(event.data);
              if (parsed.type === "data") {
                const output = this.materializeOrderbooks
                  ? orderbooks.apply(parsed.event)
                  : parsed.event;
                if (output) yield output;
              }
              if (
                parsed.type === "pong" &&
                awaitingPong &&
                parsed.requestId === awaitingPong.requestId
              ) {
                awaitingPong = undefined;
              }
            }

            const now = Date.now();
            if (awaitingPong && now - awaitingPong.sentAt >= PONG_TIMEOUT_MS) {
              reconnect = true;
              break;
            }
            if (!awaitingPong && now - lastPingAt >= PING_INTERVAL_MS) {
              const requestId = `polaris-sdk-ping-${++pingCounter}`;
              try {
                connection.socket.send(JSON.stringify({ action: "ping", request_id: requestId }));
              } catch {
                reconnect = true;
                break;
              }
              awaitingPong = { requestId, sentAt: now };
              lastPingAt = now;
            }
          }
        } finally {
          clearInterval(ticker);
          if (this.ticker === ticker) this.ticker = undefined;
        }

        try {
          connection.socket.close();
        } catch {
          // The transport is already gone.
        }
        if (!reconnect || this.closed) return;
        orderbooks.clear();
        if (Date.now() - connectedAt >= HEALTHY_CONNECTION_MS) {
          backoffMs = INITIAL_BACKOFF_MS;
        }
        await this.waitToReconnect(backoffMs);
        backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
      }
    } finally {
      this.close();
    }
  }

  private async connectAndSubscribe(): Promise<ConnectedSocket> {
    let socket: WebSocketLike;
    try {
      socket = this.runtime.createWebSocket(this.streamUrl);
    } catch (error) {
      throw new StreamConnectionError(`Failed to create realtime WebSocket: ${String(error)}`);
    }
    const queue = new AsyncQueue<SocketEvent>();
    this.socket = socket;
    this.queue = queue;
    socket.addEventListener("open", () => queue.push({ type: "open" }));
    socket.addEventListener("message", (event) => queue.push({ type: "message", data: event.data }));
    socket.addEventListener("close", (event) => queue.push({
      type: "close",
      code: event.code ?? 1006,
      reason: event.reason ?? "",
    }));
    socket.addEventListener("error", () => queue.push({ type: "error" }));

    while (!this.closed) {
      const event = await queue.shift();
      if (event.type === "client-close") throw new StreamConnectionError("Realtime stream closed");
      if (event.type === "error") throw new StreamConnectionError("Realtime WebSocket connection failed");
      if (event.type === "close") {
        throw new StreamConnectionError(`Realtime WebSocket closed before subscribing (${event.code}): ${event.reason}`);
      }
      if (event.type !== "open") continue;

      const command: Record<string, unknown> = {
        action: "subscribe",
        request_id: SUBSCRIBE_REQUEST_ID,
        include_buffer: this.includeBuffer,
        subscriptions: this.markets.map((market) => ({
          source: this.source,
          market,
          ...(this.instrument === undefined ? {} : { instrument: this.instrument }),
          stream: "standard",
        })),
      };
      if (this.apiKey) command.token = this.apiKey;
      try {
        socket.send(JSON.stringify(command));
      } catch (error) {
        throw new StreamConnectionError(`Failed to subscribe realtime WebSocket: ${String(error)}`);
      }
      break;
    }

    while (!this.closed) {
      const event = await queue.shift();
      if (event.type === "client-close") throw new StreamConnectionError("Realtime stream closed");
      if (event.type === "error") throw new StreamConnectionError("Realtime WebSocket failed before subscription acknowledgement");
      if (event.type === "close") {
        throw new StreamConnectionError(`Realtime WebSocket closed before subscription acknowledgement (${event.code}): ${event.reason}`);
      }
      if (event.type !== "message") continue;
      const value = parseJsonObject(event.data);
      if (value.type === "error") throw protocolErrorFromMessage(value);
      if (value.type !== "ack") {
        throw new StreamProtocolError("Unexpected server message before subscription acknowledgement");
      }
      if (
        value.request_id !== SUBSCRIBE_REQUEST_ID ||
        value.action !== "subscribe" ||
        value.changed !== this.markets.length ||
        value.active_subscriptions !== this.markets.length
      ) {
        throw new StreamProtocolError("Invalid realtime subscription acknowledgement");
      }
      return { socket, queue };
    }
    throw new StreamConnectionError("Realtime stream closed");
  }

  private async waitToReconnect(backoffMs: number): Promise<void> {
    const jittered = Math.round(backoffMs * (0.8 + Math.random() * 0.4));
    await Promise.race([
      delay(jittered),
      this.queue?.shift().then(() => undefined) ?? new Promise<void>(() => undefined),
    ]);
  }
}

function parseServerMessage(data: unknown): ParsedMessage {
  const value = parseJsonObject(data);
  if (typeof value.type === "string") {
    if (value.type === "pong") {
      return {
        type: "pong",
        requestId: typeof value.request_id === "string" ? value.request_id : undefined,
      };
    }
    if (value.type === "ack") return { type: "control" };
    if (value.type === "error") throw protocolErrorFromMessage(value);
    throw new StreamProtocolError(`Unexpected realtime server message type '${value.type}'`);
  }
  if (!isObject(value.kind) || typeof value.kind.type !== "string") {
    throw new StreamProtocolError("Realtime message did not include a valid kind object");
  }
  if (value.kind.type === "persistence_checkpoint") return { type: "control" };
  if (value.kind.type !== "data") {
    throw new StreamProtocolError(`Unexpected realtime message kind '${value.kind.type}'`);
  }
  if (value.kind.stream !== "standard" || !isObject(value.kind.event)) {
    throw new StreamProtocolError("Standard realtime stream received an invalid data message");
  }
  const event = value.kind.event as unknown as StandardEvent;
  const isV2 = [
    "collector_timestamp",
    "collector_sequence",
    "exchange_timestamp",
    "exchange_sequence",
  ].every((field) => field in event);
  const isLegacy = "timestamp" in event && typeof event.timestamp === "number";
  const validV2 = isV2 &&
    "collector_timestamp" in event && typeof event.collector_timestamp === "number" &&
    typeof event.collector_sequence === "number" &&
    (event.exchange_timestamp === null || typeof event.exchange_timestamp === "number") &&
    (event.exchange_sequence === null || typeof event.exchange_sequence === "string");
  if (
    (!isLegacy && !validV2) ||
    typeof event.type !== "string" ||
    !isObject(event.data)
  ) {
    throw new StreamProtocolError("Invalid standardized realtime event");
  }
  if (!event.source) event.source = typeof value.source === "string" ? value.source : "";
  if (!event.market) event.market = typeof value.market === "string" ? value.market : "";
  if (
    event.type === "option_ticker" &&
    (typeof event.instrument !== "string" || event.instrument.length === 0)
  ) {
    throw new StreamProtocolError("option_ticker instrument must be non-empty");
  }
  return { type: "data", event };
}

function parseJsonObject(data: unknown): Record<string, unknown> {
  let text: string;
  if (typeof data === "string") text = data;
  else if (data instanceof Uint8Array) text = new TextDecoder().decode(data);
  else if (data && typeof (data as { toString?: unknown }).toString === "function") text = String(data);
  else throw new StreamProtocolError("Realtime WebSocket delivered an unsupported message payload");
  try {
    const value: unknown = JSON.parse(text);
    if (!isObject(value)) throw new Error("expected an object");
    return value;
  } catch (error) {
    throw new StreamProtocolError(`Invalid realtime JSON: ${String(error)}`);
  }
}

function protocolErrorFromMessage(value: Record<string, unknown>): StreamProtocolError {
  return new StreamProtocolError(
    typeof value.message === "string" ? value.message : "Realtime server returned an error",
    typeof value.code === "string" ? value.code : undefined,
  );
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
