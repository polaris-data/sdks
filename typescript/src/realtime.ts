import { getCore } from "./core";
import { StreamConnectionError, StreamDecodeError, StreamProtocolError } from "./errors";
import type { TableDataMessage } from "./generated";
import type { PolarisRuntime, WebSocketLike } from "./runtime/types";
import type { StreamOptions } from "./types";

const PING_INTERVAL = 30_000;
const PONG_TIMEOUT = 10_000;
const PING_ID = "polaris-sdk-ping";
const MAX_QUEUED_BYTES = 32 * 1024 * 1024;
const MAX_QUEUED_FRAMES = 1024;

type Event =
  | { type: "open" | "retry" | "timeout" | "closed" }
  | { type: "end"; code: number }
  | { type: "message"; data: unknown };

class Socket {
  private readonly queue: Event[] = [];
  private waiter?: (event: Event) => void;
  private closed = false;
  private bytes = 0;

  constructor(private readonly socket: WebSocketLike) {
    socket.addEventListener("open", () => this.push({ type: "open" }));
    socket.addEventListener("message", ({ data }) => this.push({ type: "message", data }));
    socket.addEventListener("error", () => this.push({ type: "retry" }));
    socket.addEventListener("close", ({ code }) => this.push({ type: "end", code: code ?? 1006 }));
  }

  private push(event: Event): void {
    if (this.closed) return;
    if (this.waiter) {
      this.waiter(event);
      return;
    }
    const size = event.type === "message" && typeof event.data === "string" ? event.data.length * 2 : 0;
    if (this.queue.length >= MAX_QUEUED_FRAMES || this.bytes + size > MAX_QUEUED_BYTES) {
      this.queue.length = 0;
      this.bytes = 0;
      this.queue.push({ type: "retry" });
      this.socket.close();
      return;
    }
    this.bytes += size;
    this.queue.push(event);
  }

  async read(timeout: number): Promise<Event> {
    if (this.closed) return { type: "closed" };
    const event = this.queue.shift();
    if (event) {
      if (event.type === "message" && typeof event.data === "string") this.bytes -= event.data.length * 2;
      return event;
    }
    return new Promise((resolve) => {
      const finish = (value: Event) => {
        clearTimeout(timer);
        this.waiter = undefined;
        resolve(value);
      };
      const timer = setTimeout(() => finish({ type: "timeout" }), Math.max(0, timeout));
      this.waiter = finish;
    });
  }

  send(data: string): void {
    this.socket.send(data);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.queue.length = 0;
    this.waiter?.({ type: "closed" });
    try {
      this.socket.close();
    } catch {
      // The peer may have already closed the connection.
    }
  }
}

export class RealtimeStream implements AsyncIterable<TableDataMessage> {
  private socket?: Socket;
  private wake?: () => void;
  private closed = false;
  private started = false;

  constructor(
    private readonly options: StreamOptions,
    private readonly config: {
      url: string;
      apiKey?: string;
      timeout: number;
      runtime: PolarisRuntime;
      onClose: () => void;
    },
  ) {}

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.wake?.();
    this.socket?.close();
    this.config.onClose();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<TableDataMessage> {
    if (this.started) throw new StreamProtocolError("A realtime stream can only be iterated once");
    this.started = true;
    if (this.closed) return;
    const core = await getCore();
    if (this.closed) return;
    let session: ReturnType<typeof core.session> | undefined;
    let connected = false;
    let backoff = 500;
    try {
      session = core.session({ query: this.options, api_key: this.config.apiKey });
      while (!this.closed) {
        const socket = new Socket(this.config.runtime.createWebSocket(this.config.url));
        this.socket = socket;
        let connectedAt = Date.now();
        try {
          const opened = await socket.read(this.config.timeout);
          if (this.closed) return;
          if (opened.type !== "open") throw new StreamConnectionError("WebSocket did not open");
          socket.send(session.subscribe());
          let acknowledged = false;
          let pingAt = Date.now() + this.config.timeout;
          let pongPending = false;
          while (!this.closed) {
            const event = await socket.read(pingAt - Date.now());
            if (this.closed || event.type === "closed") return;
            if (event.type === "end") {
              if (event.code === 1008) throw new StreamProtocolError("WebSocket policy violation");
              if (acknowledged && (event.code === 1000 || event.code === 1001)) return;
              throw new StreamConnectionError("WebSocket closed");
            }
            if (event.type === "retry") throw new StreamConnectionError("WebSocket disconnected");
            if (event.type === "message") {
              if (typeof event.data !== "string") throw new StreamDecodeError("Expected a WebSocket text message");
              const frame = session.receive(event.data);
              if (frame.type === "ack") {
                connected = acknowledged = true;
                connectedAt = Date.now();
                pingAt = connectedAt + PING_INTERVAL;
              } else if (frame.type === "data") {
                yield frame;
                if (this.closed) return;
                session.advance();
              } else if (frame.request_id === PING_ID) {
                pongPending = false;
                pingAt = Date.now() + PING_INTERVAL;
              }
            }
            if (Date.now() >= pingAt) {
              if (!acknowledged) throw new StreamConnectionError("WebSocket subscription timed out");
              if (pongPending) throw new StreamConnectionError("WebSocket heartbeat timed out");
              socket.send(JSON.stringify({ action: "ping", request_id: PING_ID }));
              pongPending = true;
              pingAt = Date.now() + PONG_TIMEOUT;
            }
          }
        } catch (error) {
          if (this.closed) return;
          if (!connected || error instanceof StreamProtocolError || error instanceof StreamDecodeError) {
            if (error instanceof Error && !(error instanceof StreamConnectionError) &&
                !(error instanceof StreamProtocolError) && !(error instanceof StreamDecodeError)) {
              throw new StreamConnectionError(error.message);
            }
            throw error;
          }
        } finally {
          socket.close();
          this.socket = undefined;
        }
        if (Date.now() - connectedAt >= 30_000) backoff = 500;
        await this.delay(backoff);
        backoff = Math.min(30_000, backoff * 2);
      }
    } finally {
      session?.close();
      this.close();
    }
  }

  private delay(ms: number): Promise<void> {
    if (this.closed) return Promise.resolve();
    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
      const timer = setTimeout(wake, ms);
      this.wake = wake;
    });
  }
}
