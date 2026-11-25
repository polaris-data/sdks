import { getCore, type PreparedRequest } from "./core";
import { PolarisError } from "./errors";
import { GeneratedClient, type DataQuery } from "./generated";
import { RealtimeStream } from "./realtime";
import type { PolarisRuntime } from "./runtime/types";
import type { FetchLike, Json, PolarisClientOptions, StreamOptions } from "./types";

const VERSION = "0.10.0";

export class BasePolarisClient extends GeneratedClient {
  private readonly apiKey: string | undefined;
  private readonly baseUrl: string;
  private readonly streamUrl: string;
  private readonly timeout: number;
  private readonly fetch: FetchLike;
  private readonly streams = new Set<RealtimeStream>();
  private closed = false;

  constructor(options: PolarisClientOptions, private readonly runtime: PolarisRuntime) {
    super();
    this.apiKey = runtime.resolveApiKey(options.apiKey);
    const url = new URL(options.baseUrl ?? "https://api.polaris.supply");
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new PolarisError("Invalid API base URL");
    }
    this.baseUrl = url.toString();
    const ws = options.streamUrl ? new URL(options.streamUrl) : new URL("/stream", url);
    if (!options.streamUrl) ws.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    if (ws.protocol !== "ws:" && ws.protocol !== "wss:") {
      throw new PolarisError("Invalid WebSocket URL");
    }
    this.streamUrl = ws.toString();
    this.timeout = Math.min(options.timeout ?? 30_000, 2_147_483_647);
    if (!Number.isFinite(this.timeout) || this.timeout <= 0) {
      throw new PolarisError("Timeout must be greater than zero");
    }
    this.fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  protected async request<T>(operation: string, query: object): Promise<T> {
    this.requireOpen();
    const core = await getCore();
    this.requireOpen();
    const prepared = core.request<PreparedRequest>({
      operation, query, api_key: this.apiKey, base_url: this.baseUrl,
    });
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    let response: Response;
    let body: string;
    try {
      response = await this.fetch(prepared.url, {
        headers: {
          Accept: "application/json",
          "User-Agent": `polaris-ts/${VERSION}`,
          ...(prepared.authorization ? { Authorization: prepared.authorization } : {}),
        },
        signal: controller.signal,
        redirect: "follow",
      });
      body = await response.text();
    } catch (error) {
      throw new PolarisError(controller.signal.aborted ? "Request timed out" : `Request failed: ${error}`);
    } finally {
      clearTimeout(timer);
    }
    return core.request<T>({
      operation, status: response.status, body,
      reset_at: response.headers.get("x-ratelimit-reset") ?? response.headers.get("retry-after"),
    });
  }

  async *dataRows(query: DataQuery): AsyncGenerator<Record<string, Json>> {
    const core = await getCore();
    const cursors = core.cursors();
    try {
      let cursor = query.cursor;
      if (cursor) cursors.remember(cursor);
      while (true) {
        const page = await this.data({ ...query, cursor });
        if (page.has_more) cursors.remember(page.next_cursor!);
        yield* page.data;
        if (!page.has_more) return;
        cursor = page.next_cursor!;
      }
    } finally {
      cursors.close();
    }
  }

  stream(options: StreamOptions): RealtimeStream {
    this.requireOpen();
    const stream = new RealtimeStream(options, {
      url: this.streamUrl,
      timeout: this.timeout,
      apiKey: this.apiKey,
      runtime: this.runtime,
      onClose: () => this.streams.delete(stream),
    });
    this.streams.add(stream);
    return stream;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const stream of this.streams) stream.close();
    this.streams.clear();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  private requireOpen(): void {
    if (this.closed) throw new PolarisError("Polaris client is closed");
  }
}
