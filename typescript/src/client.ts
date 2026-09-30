import type {
  AuthMode,
  CatalogCount,
  CatalogInstrument,
  CatalogMarket,
  CatalogOptions,
  CatalogResponse,
  InstrumentsOptions,
  InstrumentsResponse,
  OptionContract,
  FetchLike,
  FundingRateRow,
  IntentRow,
  IntentRowsOptions,
  OhlcvRow,
  RawCaptureRow,
  RawChannelOptions,
  HistoricalQueryOptions,
  HistoricalRowsOptions,
  L2UpdatesOptions,
  L2OrderbooksOptions,
  OptionTickerRow,
  OptionTickerRowsOptions,
  OhlcvBar,
  OhlcvOptions,
  OrderbookL2Row,
  TradeRow,
  PolarisClientOptions,
  StreamOptions,
  TradingViewOhlcvResponse,
} from "./types";

import {
  PolarisError,
  UnauthorizedError,
  NotFoundError,
  RateLimitedError,
} from "./errors";

import { toEpochMs } from "./utils";
import type { PolarisRuntime } from "./runtime/types";
import { RealtimeStream } from "./realtime";

// ---------------------------------------------------------------------------
// SDK version – bumped manually during releases
// ---------------------------------------------------------------------------

const VERSION = "0.8.0";

// ---------------------------------------------------------------------------
// Internal shorthand
// ---------------------------------------------------------------------------

type Json = Record<string, unknown>;

interface FetchOptions {
  params?: Record<string, string>;
  auth?: AuthMode;
  headers?: Record<string, string>;
}

type RedirectMode = "error" | "follow" | "manual";

function resolveStreamUrl(baseUrl: URL, explicit?: string): string {
  if (explicit) {
    const url = new URL(explicit);
    if (url.protocol !== "ws:" && url.protocol !== "wss:") {
      throw new PolarisError("streamUrl must use ws:// or wss://");
    }
    return url.toString();
  }
  const url = new URL(baseUrl.toString());
  if (url.protocol === "https:") url.protocol = "wss:";
  else if (url.protocol === "http:") url.protocol = "ws:";
  else throw new PolarisError(`Cannot derive streamUrl from ${url.protocol}`);
  url.pathname = "/stream";
  url.search = "";
  url.hash = "";
  return url.toString();
}

// ===========================================================================
// BasePolarisClient
// ===========================================================================

export class BasePolarisClient {
  private readonly _apiKey: string | undefined;
  private readonly _baseUrl: URL;
  private readonly _streamUrl: string;
  private readonly _timeout: number;
  private readonly _fetch: FetchLike;
  private readonly _runtime: PolarisRuntime;
  private readonly _streams = new Set<RealtimeStream>();
  private _closed = false;

  constructor(
    options: PolarisClientOptions = {},
    runtime: PolarisRuntime,
  ) {
    this._apiKey = runtime.resolveApiKey(options.apiKey);
    this._baseUrl = new URL(options.baseUrl ?? "https://api.polaris.supply");
    this._streamUrl = resolveStreamUrl(this._baseUrl, options.streamUrl);
    this._timeout = options.timeout ?? 30_000;
    this._fetch = options.fetch ?? globalThis.fetch;
    this._runtime = runtime;
  }

  // -----------------------------------------------------------------------
  // Discovery
  // -----------------------------------------------------------------------

  /** Check API availability. */
  async health(): Promise<Json> {
    return this._getJson("/health", { auth: "none" });
  }

  stream(options: StreamOptions): RealtimeStream {
    if (this._closed) throw new PolarisError("PolarisClient is closed");
    let realtime!: RealtimeStream;
    realtime = new RealtimeStream(options, {
      streamUrl: this._streamUrl,
      apiKey: this._apiKey,
      runtime: this._runtime,
      onClose: () => this._streams.delete(realtime),
    });
    this._streams.add(realtime);
    return realtime;
  }

  /**
   * Browse supported sources and markets.
   *
   * If neither `source` nor `market` is provided, returns the full catalog.
   * `market` requires `source`.
   *
   * Pagination is handled transparently: the client follows the server
   * cursor until every matching market has been returned.
   */
  async catalog(options: CatalogOptions = {}): Promise<CatalogResponse> {
    const params: Record<string, string> = {};
    if (options.source) params.source = options.source;
    if (options.market) params.market = options.market;
    params.limit = "1000";

    const markets: CatalogMarket[] = [];
    const seen = new Set<string>();
    let updatedAt: string | undefined;
    let cursor: string | undefined;

    do {
      const pageParams = { ...params };
      if (cursor) pageParams.cursor = cursor;

      const payload = await this._getJson<{
        updatedAt?: string;
        markets?: unknown;
        sources?: unknown;
        has_more?: boolean;
        next_cursor?: string | null;
      }>("/catalog", {
        params: pageParams,
        auth: "if-available",
      });

      if (!Array.isArray(payload.markets)) {
        return normalizeCatalogResponse(payload);
      }

      if (updatedAt === undefined) {
        updatedAt = payload.updatedAt;
      }

      for (const entry of payload.markets) {
        const market = normalizeFlatCatalogMarket(entry);
        const key = `${market.source}\u0000${market.market}`;
        if (!seen.has(key)) {
          seen.add(key);
          markets.push(market);
        }
      }

      cursor = payload.has_more && payload.next_cursor
        ? payload.next_cursor
        : undefined;
    } while (cursor);

    if (typeof updatedAt !== "string" || updatedAt.length === 0) {
      throw new PolarisError("Catalog response did not include a valid updatedAt timestamp");
    }

    return { updatedAt, markets };
  }

  /** Discover venue-native option contracts for one underlying market. */
  async instruments(options: InstrumentsOptions): Promise<InstrumentsResponse> {
    const source = optionalFilter("source", options.source);
    const market = optionalFilter("market", options.market);
    if (!source || !market) throw new PolarisError("source and market are required");
    const params: Record<string, string> = { source, market, limit: "1000" };
    const instrument = optionalFilter("instrument", options.instrument);
    const q = optionalFilter("q", options.q);
    if (instrument) params.instrument = instrument;
    if (q) params.q = q;
    if (options.expiry !== undefined) {
      if (!Number.isSafeInteger(options.expiry) || options.expiry < 0) {
        throw new PolarisError("expiry must be a non-negative Unix-millisecond integer");
      }
      params.expiry = String(options.expiry);
    }
    if (options.optionType !== undefined) {
      if (options.optionType !== "call" && options.optionType !== "put") {
        throw new PolarisError("optionType must be call or put");
      }
      params.option_type = options.optionType;
    }

    const instruments: OptionContract[] = [];
    let updatedAt: string | undefined;
    let cursor: string | undefined;
    const cursors = new Set<string>();
    while (true) {
      const payload = await this._getJson<{
        updatedAt?: unknown;
        instruments?: unknown;
        next_cursor?: unknown;
      }>("/catalog/instruments", {
        params: cursor ? { ...params, cursor } : params,
        auth: "if-available",
      });
      if (typeof payload.updatedAt !== "string" || !payload.updatedAt ||
          !Array.isArray(payload.instruments)) {
        throw new PolarisError("Invalid instrument catalog page");
      }
      updatedAt ??= payload.updatedAt;
      for (const item of payload.instruments) {
        if (!isOptionContract(item)) throw new PolarisError("Invalid option contract");
        instruments.push(item);
      }
      const next = payload.next_cursor;
      if (next === undefined || next === null) break;
      if (typeof next !== "string" || !next || cursors.has(next)) {
        throw new PolarisError("Invalid instrument catalog next_cursor");
      }
      cursors.add(next);
      cursor = next;
    }
    return { updatedAt: updatedAt!, instruments };
  }

  /** Return global public source and market totals for the catalog. */
  async count(): Promise<CatalogCount> {
    return this._getJson<CatalogCount>("/count", { auth: "if-available" });
  }

  // -----------------------------------------------------------------------
  // Historical data – direct rows and snapshot-backed methods
  // -----------------------------------------------------------------------

  /** Return flat trades from the direct historical API. */
  async trades(options: HistoricalRowsOptions = {}): Promise<TradeRow[]> {
    return this._historicalRows("/trades", options, isTradeRow);
  }

  /** Return exact raw captures from one venue-native channel via `/raw`. */
  async rawChannel(options: RawChannelOptions): Promise<RawCaptureRow[]> {
    if (!options.exchange.trim() || !options.event.trim() ||
      options.exchange === "." || options.exchange === ".." ||
      options.event === "." || options.event === "..") {
      throw new PolarisError("exchange and event must be non-empty channel identifiers");
    }
    if (!Number.isSafeInteger(options.start) || options.start < 0 ||
      !Number.isSafeInteger(options.end) || options.end < options.start) {
      throw new PolarisError("start and end must be non-negative inclusive milliseconds with start <= end");
    }
    const start = new Date(options.start);
    const end = new Date(options.end);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
      throw new PolarisError("start and end must be representable ISO timestamps");
    }
    return this._pagedRows("/raw", {
      source: options.exchange,
      channel: options.event,
      start: start.toISOString(),
      end: end.toISOString(),
      limit: "1000",
    }, isRawCaptureRow, "data");
  }

  /** Return pair-shaped flat intent observations. */
  async intents(options: IntentRowsOptions = {}): Promise<IntentRow[]> {
    const instrument = optionalFilter("instrument", options.instrument);
    const intentId = optionalFilter("intentId", options.intentId);
    return this._historicalRows("/intents", options, isIntentRow, {
      ...(instrument !== undefined && { instrument }),
      ...(intentId !== undefined && { intent_id: intentId }),
    });
  }

  /** Return flat option ticker rows for a whole chain or one exact venue-native contract. */
  async optionTickers(options: OptionTickerRowsOptions = {}): Promise<OptionTickerRow[]> {
    const instrument = normalizeInstrumentFilter(options.instrument);
    return this._historicalRows(
      "/options-ticker", options, isOptionTickerRow, instrument ? { instrument } : {},
    );
  }

  /** Return funding-bearing perpetual ticker observations. */
  async perpetualTickers(options: HistoricalRowsOptions = {}): Promise<FundingRateRow[]> {
    return this._historicalRows("/perpetual-ticker", options, isFundingRateRow);
  }

  /** Return reconstructed, sorted top-25 books after each L2 event. */
  async l2Snapshots(options: L2OrderbooksOptions): Promise<OrderbookL2Row[]> {
    const source = optionalFilter("source", options.source);
    const market = optionalFilter("market", options.market);
    if (!source || !market) {
      throw new PolarisError("source and market are required");
    }
    if (!Number.isSafeInteger(options.start) || !Number.isSafeInteger(options.end) ||
        options.start < 0 || options.end < options.start) {
      throw new PolarisError("l2Snapshots requires non-negative inclusive bounds with start <= end");
    }
    const instrument = optionalFilter("instrument", options.instrument);
    return this._historicalRows("/l2-orderbooks", { ...options, source, market }, isOrderbookL2Row,
      instrument ? { instrument } : {});
  }

  /** Return flat source snapshots and sparse deltas from the direct API. */
  async l2Updates(options: L2UpdatesOptions = {}): Promise<OrderbookL2Row[]> {
    const instrument = optionalFilter("instrument", options.instrument);
    return this._historicalRows("/l2-updates", options, isOrderbookL2Row,
      instrument ? { instrument } : {});
  }

  /** Return partial flat funding observations from the direct historical API. */
  async fundingRates(options: HistoricalRowsOptions = {}): Promise<FundingRateRow[]> {
    return this._historicalRows("/funding-rates", options, isFundingRateRow);
  }

  /** Return the latest venue-published revision of each candle. */
  async ohlcv(options: OhlcvOptions): Promise<OhlcvBar[]> {
    return (await this._venueCandles(options, options.interval)).map((row) => ({
      timestamp: row.open_timestamp, open: row.open, high: row.high, low: row.low,
      close: row.close, volume: row.base_volume ?? 0, trades: row.trade_count ?? 0,
    }));
  }

  private async _venueCandles(
    options: HistoricalQueryOptions,
    interval?: string,
  ): Promise<OhlcvRow[]> {
    const rows = await this._historicalRows("/ohlcv", {
      source: options.source, market: options.market,
      ...(options.from !== undefined && { start: toEpochMs(options.from) }),
      ...(options.to !== undefined && { end: toEpochMs(options.to) }),
    }, isOhlcvRow, interval ? { interval } : {});
    const latest = new Map<string, OhlcvRow>();
    for (const row of rows) {
      const key = JSON.stringify([row.source, row.market, row.instrument, row.interval, row.open_timestamp]);
      const prior = latest.get(key);
      if (!prior || row.collector_timestamp > prior.collector_timestamp ||
          (row.collector_timestamp === prior.collector_timestamp && row.event_id > prior.event_id)) {
        latest.set(key, row);
      }
    }
    return [...latest.values()].sort((a, b) => a.open_timestamp - b.open_timestamp || a.event_id.localeCompare(b.event_id));
  }

  /**
   * Return a TradingView-shaped OHLCV response.
   *
   * Reshapes venue candle bars to `{ candles, volumes }`.
   */
  async ohlcvTradingView(
    options: OhlcvOptions,
  ): Promise<TradingViewOhlcvResponse> {
    const bars = await this.ohlcv(options);
    return {
      candles: bars.map((b) => ({
        t: b.timestamp,
        o: b.open,
        h: b.high,
        l: b.low,
        c: b.close,
      })),
      volumes: bars.map((b) => ({
        t: b.timestamp,
        v: b.volume,
        trades: b.trades,
      })),
    };
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /** Close all active realtime streams and release client resources. */
  close(): void {
    if (this._closed) return;
    this._closed = true;
    for (const stream of [...this._streams]) stream.close();
    this._streams.clear();
  }

  /** Async disposable support (Node ≥ 18 / TypeScript ≥ 5.2). */
  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  // -----------------------------------------------------------------------
  // Internals – HTTP layer
  // -----------------------------------------------------------------------

  private async _historicalRows<T>(
    path: string,
    options: HistoricalRowsOptions,
    isRow: (value: unknown) => value is T,
    extraFilters: Record<string, string> = {},
  ): Promise<T[]> {
    for (const [name, value] of [["start", options.start], ["end", options.end]] as const) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) {
        throw new PolarisError(`${name} must be a non-negative Unix millisecond integer`);
      }
    }
    if (options.start !== undefined && options.end !== undefined && options.start > options.end) {
      throw new PolarisError("start must be less than or equal to end");
    }
    const params: Record<string, string> = { limit: "1000" };
    if (options.source !== undefined) params.source = options.source;
    if (options.market !== undefined) params.market = options.market;
    if (options.start !== undefined) params.start = String(options.start);
    if (options.end !== undefined) params.end = String(options.end);
    Object.assign(params, extraFilters);

    return this._pagedRows(path, params, isRow);
  }

  private async _pagedRows<T>(
    path: string,
    params: Record<string, string>,
    isRow: (value: unknown) => value is T,
    rowKey: "items" | "data" = "items",
  ): Promise<T[]> {
    const rows: T[] = [];
    let cursor: string | undefined;
    while (true) {
      const page = await this._getJson<Record<string, unknown>>(path, {
        params: cursor ? { ...params, cursor } : params, auth: "if-available",
      });
      const values = page[rowKey];
      if (!Array.isArray(values) || typeof page.has_more !== "boolean") {
        throw new PolarisError(`Invalid ${path} page`);
      }
      for (const item of values) {
        if (!isRow(item)) throw new PolarisError(`Invalid ${path} row`);
        rows.push(item);
      }
      if (!page.has_more) return rows;
      if (typeof page.next_cursor !== "string" || !page.next_cursor || page.next_cursor === cursor) {
        throw new PolarisError(`Invalid ${path} next_cursor`);
      }
      cursor = page.next_cursor;
    }
  }

  private async _getJson<T = Json>(
    path: string,
    opts: FetchOptions = {},
  ): Promise<T> {
    const { response, body } = await this._fetchRaw(path, {
      ...opts,
      headers: { Accept: "application/json", ...opts.headers },
    });

    assertOk(response, body);

    let json: unknown;
    try {
      json = JSON.parse(body);
    } catch {
      throw new PolarisError("Failed to parse response as JSON");
    }

    if (typeof json !== "object" || json === null || Array.isArray(json)) {
      throw new PolarisError("Expected a JSON object response");
    }

    return json as T;
  }

  private async _fetchRaw(
    path: string,
    opts: FetchOptions = {},
  ): Promise<{ response: Response; body: string }> {
    const response = await this._request(path, {
      ...opts,
      redirect: "follow",
    });
    const body = await response.text();
    return { response, body };
  }

  private async _request(
    path: string,
    opts: FetchOptions & { redirect?: RedirectMode } = {},
  ): Promise<Response> {
    const headers = this._buildHeaders(opts.auth ?? "none", opts.headers);
    const url = this._buildUrl(path, opts.params);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this._timeout);

    try {
      return await this._fetch(url, {
        headers,
        signal: controller.signal,
        redirect: opts.redirect ?? "follow",
      });
    } catch (e) {
      if (e instanceof Error && e.name === "AbortError") {
        throw new PolarisError("Request timed out");
      }
      throw new PolarisError(`Request failed: ${e}`);
    } finally {
      clearTimeout(timer);
    }
  }

  private _buildHeaders(
    authMode: AuthMode,
    extra?: Record<string, string>,
  ): Record<string, string> {
    const out: Record<string, string> = {
      "User-Agent": `polaris-ts/${VERSION}`,
      ...extra,
    };

    if (authMode === "required" && !this._apiKey) {
      throw new UnauthorizedError("API key is required for this endpoint");
    }

    if (this._apiKey && authMode !== "none") {
      out["Authorization"] = `Bearer ${this._apiKey}`;
    }

    return out;
  }

  private _buildUrl(
    path: string,
    params?: Record<string, string>,
  ): string {
    const url = new URL(path, this._baseUrl);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined) url.searchParams.set(k, v);
      }
    }
    return url.toString();
  }

  // -----------------------------------------------------------------------
}

// ===========================================================================
// Module-level helpers (not exported)
// ===========================================================================

function normalizeCatalogResponse(payload: {
  updatedAt?: string;
  markets?: unknown;
  sources?: unknown;
}): CatalogResponse {
  const { updatedAt } = payload;
  if (typeof updatedAt !== "string" || updatedAt.length === 0) {
    throw new PolarisError("Catalog response did not include a valid updatedAt timestamp");
  }

  if (Array.isArray(payload.markets)) {
    return {
      updatedAt,
      markets: payload.markets.map((entry) => normalizeFlatCatalogMarket(entry)),
    };
  }

  if (Array.isArray(payload.sources)) {
    const markets: CatalogMarket[] = [];

    for (const sourceEntry of payload.sources) {
      if (!isRecord(sourceEntry)) continue;
      const source = sourceEntry.id;
      const sourceMarkets = sourceEntry.markets;
      if (typeof source !== "string" || !Array.isArray(sourceMarkets)) continue;

      for (const marketEntry of sourceMarkets) {
        if (!isRecord(marketEntry)) continue;
        markets.push(
          normalizeFlatCatalogMarket({
            ...marketEntry,
            source,
            market: marketEntry.id,
          }),
        );
      }
    }

    return { updatedAt, markets };
  }

  throw new PolarisError("Catalog response did not include a valid markets array");
}

function normalizeFlatCatalogMarket(entry: unknown): CatalogMarket {
  if (!isRecord(entry)) {
    throw new PolarisError("Catalog market entry was not an object");
  }

  const { source, market } = entry;
  if (typeof source !== "string" || source.length === 0) {
    throw new PolarisError("Catalog market entry did not include a valid source");
  }
  if (typeof market !== "string" || market.length === 0) {
    throw new PolarisError("Catalog market entry did not include a valid market");
  }

  return {
    source,
    market,
    symbol: typeof entry.symbol === "string" ? entry.symbol : market,
    start: typeof entry.start === "string" ? entry.start : undefined,
    end: typeof entry.end === "string" ? entry.end : undefined,
    source_type:
      typeof entry.source_type === "string" ? entry.source_type : undefined,
    categories: Array.isArray(entry.categories)
      ? entry.categories.filter((value): value is string => typeof value === "string")
      : undefined,
    access: normalizeCatalogAccess(entry.access),
    instrument: normalizeCatalogInstrument(entry.instrument),
  };
}

function normalizeCatalogAccess(entry: unknown): CatalogMarket["access"] | undefined {
  if (!isRecord(entry) || typeof entry.status !== "string") {
    return undefined;
  }

  return {
    status: entry.status,
    public_cutoff_date:
      typeof entry.public_cutoff_date === "string" || entry.public_cutoff_date === null
        ? entry.public_cutoff_date
        : undefined,
  };
}

function normalizeCatalogInstrument(entry: unknown): CatalogInstrument {
  if (!isRecord(entry)) {
    return emptyCatalogInstrument();
  }

  return {
    base: typeof entry.base === "string" ? entry.base : null,
    quote: typeof entry.quote === "string" ? entry.quote : null,
    tick_size: normalizeCatalogInstrumentNumber(entry.tick_size),
    lot_size: normalizeCatalogInstrumentNumber(entry.lot_size),
    min_notional: normalizeCatalogInstrumentNumber(entry.min_notional),
  };
}

function normalizeCatalogInstrumentNumber(
  value: unknown,
): string | number | null {
  return typeof value === "string" || typeof value === "number" ? value : null;
}

function emptyCatalogInstrument(): CatalogInstrument {
  return {
    base: null,
    quote: null,
    tick_size: null,
    lot_size: null,
    min_notional: null,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isOptionContract(value: unknown): value is OptionContract {
  if (!isRecord(value) ||
      !["source", "market", "instrument", "status", "option_type", "underlying", "strike"]
        .every((field) => typeof value[field] === "string") ||
      !Number.isSafeInteger(value.expiry_timestamp) ||
      !nullableFields(value, ["contract_size", "exercise_style", "premium_currency",
        "quantity_unit", "settlement_currency"], "string")) return false;
  if (value.statistics === undefined || value.statistics === null) return true;
  const statistics = value.statistics;
  if (!isRecord(statistics) || typeof statistics.source !== "string" ||
      typeof statistics.market !== "string" ||
      !nullableFields(statistics, ["instrument"], "string") ||
      !isRecord(statistics.fields) || Array.isArray(statistics.fields)) return false;
  return Object.values(statistics.fields).every((field) =>
    isRecord(field) && typeof field.value === "string" &&
    Number.isSafeInteger(field.observed_at) &&
    nullableFields(field, ["exchange_timestamp"], "number") &&
    nullableFields(field, ["unit", "convention"], "string"));
}

function isHistoricalIdentity(value: unknown): value is Record<string, unknown> {
  return isRecord(value) &&
    ["event_id", "source", "market", "source_capture_id"].every(
      (field) => typeof value[field] === "string",
    ) &&
    Number.isSafeInteger(value.collector_timestamp) &&
    Number.isSafeInteger(value.schema_version) &&
    (value.schema_version as number) >= 0;
}

function nullableFields(
  value: Record<string, unknown>,
  fields: readonly string[],
  type: "string" | "number" | "boolean",
): boolean {
  return fields.every((field) => value[field] === undefined || value[field] === null ||
    (typeof value[field] === type &&
      (type !== "number" || Number.isSafeInteger(value[field]))));
}

function isTradeRow(value: unknown): value is TradeRow {
  return isHistoricalIdentity(value) &&
    typeof value.price === "number" && Number.isFinite(value.price) &&
    typeof value.quantity === "number" && Number.isFinite(value.quantity) &&
    nullableFields(value, ["exchange_timestamp"], "number") &&
    nullableFields(value, ["instrument", "maker", "order_id", "side", "taker"], "string") &&
    nullableFields(value, ["liquidation"], "boolean");
}

function isOptionTickerRow(value: unknown): value is OptionTickerRow {
  return isHistoricalIdentity(value) && typeof value.instrument === "string" &&
    nullableFields(value, ["exchange_timestamp", "expiry_timestamp"], "number") &&
    nullableFields(value, [
      "ask_iv", "ask_price", "ask_size", "bid_iv", "bid_price", "bid_size",
      "delta", "forward_price", "gamma", "index_price", "last_price", "mark_iv",
      "mark_price", "open_interest", "option_type", "premium_currency",
      "quantity_unit", "rho", "strike", "theta", "turnover_24h", "underlying",
      "underlying_price", "vega", "volume_24h",
    ], "string");
}

function isFundingRateRow(value: unknown): value is FundingRateRow {
  return isHistoricalIdentity(value) &&
    nullableFields(value, ["exchange_timestamp", "funding_timestamp"], "number") &&
    nullableFields(value, [
      "instrument", "funding_rate", "index_price", "mark_price", "open_interest",
      "predicted_funding_rate", "premium",
    ], "string");
}

function isOhlcvRow(value: unknown): value is OhlcvRow {
  return isHistoricalIdentity(value) &&
    typeof value.interval === "string" && Number.isSafeInteger(value.open_timestamp) &&
    ["open", "high", "low", "close"].every((key) => typeof value[key] === "number" && Number.isFinite(value[key])) &&
    nullableFields(value, ["exchange_timestamp", "close_timestamp", "trade_count"], "number") &&
    nullableFields(value, ["instrument"], "string") &&
    nullableFields(value, ["is_closed"], "boolean") &&
    ["base_volume", "quote_volume"].every((key) => value[key] === undefined || value[key] === null ||
      (typeof value[key] === "number" && Number.isFinite(value[key])));
}

function isOrderbookL2Row(value: unknown): value is OrderbookL2Row {
  if (!isHistoricalIdentity(value) ||
      typeof value.source_event_is_snapshot !== "boolean" ||
      !nullableFields(value, ["instrument"], "string") ||
      !nullableFields(value, ["exchange_timestamp"], "number") ||
      !("instrument" in value) || !("exchange_timestamp" in value)) return false;
  for (let i = 0; i < 25; i++) {
    const index = String(i).padStart(2, "0");
    for (const side of ["bid", "ask"]) {
      for (const kind of ["px", "sz"]) {
        const field = `${side}_${kind}_${index}`;
        const level = value[field];
        if (level !== null && (typeof level !== "number" || !Number.isFinite(level))) return false;
      }
    }
  }
  return true;
}

function isIntentRow(value: unknown): value is IntentRow {
  return isHistoricalIdentity(value) &&
    nullableFields(value, ["exchange_timestamp", "expires_at", "settled_at"], "number") &&
    nullableFields(value, ["instrument", "amount_kind", "input_amount", "input_asset_id", "input_chain_id",
      "intent_id", "output_amount", "output_asset_id", "output_chain_id", "quote_id", "quoted_input_amount",
      "quoted_output_amount", "rfq_id", "status"], "string");
}

function isRawCaptureRow(value: unknown): value is RawCaptureRow {
  return isRecord(value) &&
    typeof value.raw_table === "string" &&
    typeof value.capture_id === "string" &&
    Number.isSafeInteger(value.collector_timestamp) &&
    typeof value.recorder_version === "string" &&
    Number.isSafeInteger(value.ingested_at) &&
    "additional_context" in value &&
    typeof value.original_json === "string";
}

function normalizeInstrumentFilter(instrument: string | undefined): string | undefined {
  if (instrument === undefined) return undefined;
  const normalized = instrument.trim();
  if (normalized.length === 0) {
    throw new PolarisError("instrument must be non-empty");
  }
  return normalized;
}

function optionalFilter(name: string, value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const normalized = value.trim();
  if (!normalized) throw new PolarisError(`${name} must be non-empty`);
  return normalized;
}

function assertOk(response: Response, body: string): void {
  if (response.ok) return;

  let message = `HTTP ${response.status}`;
  let resetAt: string | undefined;

  try {
    const json = JSON.parse(body);
    if (typeof json === "object" && json !== null) {
      message = String(json.error ?? json.message ?? message);
      resetAt = json.reset_at;
    }
  } catch {
    /* non-JSON body – use default message */
  }

  switch (response.status) {
    case 401:
      throw new UnauthorizedError(message, response.status, body);
    case 404:
      throw new NotFoundError(message, response.status, body);
    case 429:
      throw new RateLimitedError(message, response.status, body, resetAt);
    default:
      throw new PolarisError(message, response.status, body);
  }
}
