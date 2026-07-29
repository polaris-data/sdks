import { decompress } from "fzstd";

import type {
  AuthMode,
  BboQuote,
  CatalogInstrument,
  CatalogMarket,
  CatalogOptions,
  CatalogResponse,
  DepthMetricsOptions,
  DepthMetricsRow,
  FetchLike,
  FundingRateEvent,
  HistoricalQueryOptions,
  ListSnapshotsOptions,
  MarkPriceEvent,
  OhlcvBar,
  OhlcvOptions,
  OrderbookEvent,
  PaginatedResponse,
  PolarisClientOptions,
  RawQueryOptions,
  ReplayOptions,
  SnapshotDownloadManifest,
  SnapshotDownloadManifestOptions,
  SnapshotEntry,
  TradingViewOhlcvResponse,
  VolumeBar,
  VolumeOptions,
  VolatilityBar,
  VolatilityOptions,
  VwapBar,
  VwapOptions,
} from "./types";

import {
  PolarisError,
  UnauthorizedError,
  NotFoundError,
  RateLimitedError,
} from "./errors";

import { toIso8601, toEpochMs, datesInRange } from "./utils";
import {
  ensureLayout,
  dataFilePath,
  parseSnapshotKey,
  inferSnapshotStartMs,
  inferSnapshotEndMs,
  fileExists,
  type StorageLayout,
} from "./storage";
import { OhlcvAggregator } from "./aggregator";
import type { IStorage } from "./storage/interface";
import type { PolarisRuntime } from "./runtime/types";

// ---------------------------------------------------------------------------
// SDK version – bumped manually during releases
// ---------------------------------------------------------------------------

const VERSION = "0.5.2";

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

interface ResolvedHistoricalRange {
  fromMs: number;
  toMs: number;
}

interface CatalogMarketBounds {
  startMs: number;
  endMs: number;
  accessStatus?: string;
  publicCutoffMs?: number;
}

interface GapInterval {
  startMs: number;
  endMs: number;
}

interface ResolvedSnapshotCoverage {
  paths: string[];
  gaps: GapInterval[];
}

interface LocalSnapshotFileEntry extends SnapshotEntry {
  path: string;
  source: string;
  market: string;
  date: string;
  startMs?: number;
  endMs?: number;
}

const DEFAULT_INFERRED_LOOKBACK_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_SNAPSHOT_DOWNLOAD_CONCURRENCY = 8;

// ===========================================================================
// BasePolarisClient
// ===========================================================================

export class BasePolarisClient {
  private readonly _apiKey: string | undefined;
  private readonly _baseUrl: URL;
  private readonly _timeout: number;
  private readonly _fetch: FetchLike;
  private readonly _root: string;
  private readonly _runtime: PolarisRuntime;
  private readonly _snapshotDownloadConcurrency: number;
  private _storage: IStorage | undefined;
  private _layout: StorageLayout | undefined;

  constructor(
    options: PolarisClientOptions = {},
    runtime: PolarisRuntime,
  ) {
    this._apiKey = runtime.resolveApiKey(options.apiKey);
    this._baseUrl = new URL(options.baseUrl ?? "https://api.polaris.supply");
    this._timeout = options.timeout ?? 30_000;
    this._fetch = options.fetch ?? globalThis.fetch;
    this._root = runtime.resolveRoot(options.datasetRoot);
    this._runtime = runtime;
    this._snapshotDownloadConcurrency = normalizePositiveInteger(
      options.snapshotDownloadConcurrency,
    ) ?? DEFAULT_SNAPSHOT_DOWNLOAD_CONCURRENCY;
    this._storage = options.storage;
  }

  // ---------------------------------------------------------------------
  // Storage initialization (lazy)
  // ---------------------------------------------------------------------

  /**
   * Get or create the storage implementation.
   * Lazily initialized on first use.
   */
  private async _getStorage(): Promise<IStorage> {
    if (!this._storage) {
      this._storage = await this._runtime.createStorage(this._root);
    }
    return this._storage;
  }

  // -----------------------------------------------------------------------
  // Discovery
  // -----------------------------------------------------------------------

  /** Check API availability. */
  async health(): Promise<Json> {
    return this._getJson("/health", { auth: "none" });
  }

  /**
   * Browse supported sources and markets.
   *
   * If neither `source` nor `market` is provided, returns the full catalog.
   * `market` requires `source`.
   */
  async catalog(options: CatalogOptions = {}): Promise<CatalogResponse> {
    const params: Record<string, string> = {};
    if (options.source) params.source = options.source;
    if (options.market) params.market = options.market;
    const payload = await this._getJson<{
      updatedAt?: string;
      markets?: unknown;
      sources?: unknown;
    }>("/catalog", {
      params,
      auth: "if-available",
    });
    return normalizeCatalogResponse(payload);
  }

  /**
   * List available snapshot files for a source and market over a time range.
   * Auto-paginates to return **all** matching entries.
   */
  async listSnapshots(options: ListSnapshotsOptions): Promise<SnapshotEntry[]> {
    const entries: SnapshotEntry[] = [];
    let cursor: string | undefined;

    do {
      const params = buildSnapshotParams(options);
      if (cursor) params.cursor = cursor;

      const res = await this._getJson<{
        snapshots: unknown[];
        next_cursor: string | null;
        has_more: boolean;
      }>("/snapshots", { params, auth: "if-available" });

      entries.push(...res.snapshots.map((entry) => normalizeSnapshotEntry(entry)));
      cursor = res.has_more ? (res.next_cursor ?? undefined) : undefined;
    } while (cursor);

    return entries;
  }

  // -----------------------------------------------------------------------
  // Historical data – snapshot-first
  // -----------------------------------------------------------------------

  /**
   * Return all standardised historical events for a time range.
   *
   * Reads from locally-cached standard snapshot files in `data/`.
   * Missing standardized snapshots are discovered via daily `GET /download`
   * manifests and downloaded automatically.
   */
  async events(options: HistoricalQueryOptions): Promise<Json[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: Json[] = [];
    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
    )) {
      result.push(event);
    }
    return result;
  }

  /**
   * Return all standardised trade events for a time range.
   *
   * Reads from locally-cached standard snapshot files, filtering to
   * `type === "trade"`.
   */
  async trades(options: HistoricalQueryOptions): Promise<Json[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: Json[] = [];
    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      (e) => e.type === "trade",
    )) {
      result.push(event);
    }
    return result;
  }

  /**
   * Return standardised orderbook snapshot events for a time range.
   *
   * Reads from locally-cached standard snapshot files, filtering to rows that
   * contain both bid and ask orderbook sides.
   */
  async l2Snapshots(
    options: HistoricalQueryOptions,
  ): Promise<OrderbookEvent[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: OrderbookEvent[] = [];
    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      hasOrderbookSides,
    )) {
      result.push(event);
    }
    return result;
  }

  /**
   * Derive best bid / offer quotes from standardised orderbook snapshots.
   */
  async bbo(options: HistoricalQueryOptions): Promise<BboQuote[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: BboQuote[] = [];

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      hasOrderbookSides,
    )) {
      const quote = deriveBbo(event);
      if (quote) result.push(quote);
    }

    return result;
  }

  /**
   * Return standardised funding-rate point-series events for a time range.
   */
  async fundingRates(
    options: HistoricalQueryOptions,
  ): Promise<FundingRateEvent[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: FundingRateEvent[] = [];

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      isFundingRateEvent,
    )) {
      result.push(event);
    }

    return result;
  }

  /**
   * Return standardised mark-price point-series events for a time range.
   */
  async markPrices(
    options: HistoricalQueryOptions,
  ): Promise<MarkPriceEvent[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: MarkPriceEvent[] = [];

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      isMarkPriceEvent,
    )) {
      result.push(event);
    }

    return result;
  }

  /**
   * Aggregate per-bucket trade volume from standardised trade data.
   */
  async volume(options: VolumeOptions): Promise<VolumeBar[]> {
    const bars = await this.ohlcv(options);
    return bars.map((bar) => ({
      timestamp: bar.timestamp,
      volume: bar.volume,
    }));
  }

  /**
   * Aggregate per-bucket VWAP from standardised trade data.
   */
  async vwap(options: VwapOptions): Promise<VwapBar[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const agg = new VwapAggregator(options.interval);

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      (e) => e.type === "trade",
    )) {
      const data = event.data as { price: unknown; quantity: unknown };
      const price = coerceNumeric(data.price);
      const quantity = coerceNumeric(data.quantity);
      if (price === undefined || quantity === undefined) continue;
      agg.add(event.timestamp as number, price, quantity);
    }

    return agg.finish();
  }

  /**
   * Aggregate realised volatility from standardised trade data.
   */
  async volatility(options: VolatilityOptions): Promise<VolatilityBar[]> {
    if (options.method !== undefined && options.method !== "log_returns") {
      throw new PolarisError("method must be 'log_returns'");
    }

    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const agg = new VolatilityAggregator(options.interval);

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      (e) => e.type === "trade",
    )) {
      const data = event.data as { price: unknown };
      const price = coerceNumeric(data.price);
      if (price === undefined) continue;
      agg.add(event.timestamp as number, price);
    }

    return agg.finish();
  }

  /**
   * Derive spread, depth, imbalance, and slippage metrics from orderbooks.
   */
  async depthMetrics(
    options: DepthMetricsOptions,
  ): Promise<DepthMetricsRow[]> {
    const depthPct = options.depthPct ?? 0.01;
    const slippageNotional = options.slippageNotional ?? 10_000;

    if (depthPct <= 0) {
      throw new PolarisError("depthPct must be greater than 0");
    }
    if (slippageNotional <= 0) {
      throw new PolarisError("slippageNotional must be greater than 0");
    }

    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const result: DepthMetricsRow[] = [];

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      hasOrderbookSides,
    )) {
      const row = deriveDepthMetrics(event, depthPct, slippageNotional);
      if (row) result.push(row);
    }

    return result;
  }

  /**
   * Aggregate OHLCV bars from standardised trade data.
   *
   * Reads from locally-cached standard snapshot files and aggregates in memory
   * using the same interval-bucketing strategy as the Python SDK.
   */
  async ohlcv(options: OhlcvOptions): Promise<OhlcvBar[]> {
    const { fromMs, toMs } = await this._resolveHistoricalRange(options);
    const agg = new OhlcvAggregator(options.interval);

    for await (const event of this._readSnapshotEvents(
      options.source,
      options.market,
      fromMs,
      toMs,
      (e) => e.type === "trade",
    )) {
      const data = event.data as { price: number; quantity: number };
      agg.add(event.timestamp as number, data.price, data.quantity);
    }

    return agg.finish();
  }

  /**
   * Return a TradingView-shaped OHLCV response.
   *
   * Aggregates bars from local snapshot data and reshapes to
   * `{ candles, volumes }`.
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

  /**
   * Stream historical events as an async iterable.
   *
   * Defaults to standardised events from local snapshots (`standard: true`).
   * `standard: false` is not supported because historical reads are
   * restricted to snapshot discovery plus `GET /download`.
   *
   * @example
   * ```ts
   * for await (const row of client.replay({
   *   source: "binance",
   *   market: "BTC-USDT",
   *   from: "2024-01-01T00:00:00Z",
   *   to: "2024-01-01T01:00:00Z",
   * })) {
   *   console.log(row);
   * }
   * ```
   */
  async *replay(options: ReplayOptions): AsyncGenerator<Json> {
    if (options.standard !== false) {
      const { fromMs, toMs } = await this._resolveHistoricalRange(options);
      yield* this._readSnapshotEvents(
        options.source,
        options.market,
        fromMs,
        toMs,
      );
    } else {
      throw new PolarisError(
        "replay({ standard: false }) is not supported by the TypeScript SDK. Use snapshot-backed replay instead.",
      );
    }
  }

  // -----------------------------------------------------------------------
  // Raw (API-only, not snapshot-first)
  // -----------------------------------------------------------------------

  /**
   * Raw endpoint access is intentionally disabled in the TypeScript SDK.
   *
   * Historical data access is snapshot-first: discover files via
   * `GET /snapshots` and fetch artifacts via `GET /download`.
   */
  async raw(options: RawQueryOptions): Promise<Json[]> {
    void options;
    throw new PolarisError(
      "Direct /raw access is not supported by the TypeScript SDK. Use snapshot-backed methods or download snapshots via GET /download.",
    );
  }

  // -----------------------------------------------------------------------
  // Downloads
  // -----------------------------------------------------------------------

  /**
   * Get all pre-signed download URLs for a source/market UTC date in one call.
   */
  async getSnapshotDownloadUrls(
    options: SnapshotDownloadManifestOptions,
  ): Promise<SnapshotDownloadManifest> {
    const payload = await this._getJson("/download", {
      params: {
        source: options.source,
        market: options.market,
        date: options.date,
        mode: "json",
      },
      auth: "if-available",
    });

    return normalizeSnapshotDownloadManifest(payload);
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /** Release resources. Currently a no-op (reserved for future use). */
  close(): void {}

  /** Async disposable support (Node ≥ 18 / TypeScript ≥ 5.2). */
  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  // -----------------------------------------------------------------------
  // Internals – snapshot-first local reads
  // -----------------------------------------------------------------------

  private async _resolveHistoricalRange(
    options: Pick<HistoricalQueryOptions, "source" | "market" | "from" | "to">,
  ): Promise<ResolvedHistoricalRange> {
    if (options.from !== undefined && options.to !== undefined) {
      const fromMs = toEpochMs(options.from);
      const toMs = toEpochMs(options.to);
      assertValidRange(fromMs, toMs);
      return { fromMs, toMs };
    }

    const bounds = await this._catalogMarketBounds(options.source, options.market);

    if (bounds.accessStatus === "restricted" && !this._apiKey) {
      throw new UnauthorizedError(
        `API key is required to infer a default range for restricted dataset '${options.source}/${options.market}'`,
      );
    }

    const lowerBoundMs = bounds.startMs;
    let upperBoundMs = Math.min(bounds.endMs, Date.now());

    if (!this._apiKey && bounds.publicCutoffMs !== undefined) {
      upperBoundMs = Math.min(upperBoundMs, bounds.publicCutoffMs);
    }

    if (lowerBoundMs >= upperBoundMs) {
      throw new PolarisError(
        `Catalog reported no queryable historical range for '${options.source}/${options.market}'`,
      );
    }

    let fromMs: number;
    let toMs: number;

    if (options.from === undefined && options.to === undefined) {
      toMs = upperBoundMs;
      fromMs = Math.max(lowerBoundMs, toMs - DEFAULT_INFERRED_LOOKBACK_MS);
    } else if (options.from === undefined) {
      toMs = Math.min(toEpochMs(options.to as NonNullable<typeof options.to>), upperBoundMs);
      fromMs = Math.max(lowerBoundMs, toMs - DEFAULT_INFERRED_LOOKBACK_MS);
    } else {
      fromMs = Math.max(toEpochMs(options.from), lowerBoundMs);
      toMs = Math.min(fromMs + DEFAULT_INFERRED_LOOKBACK_MS, upperBoundMs);
    }

    assertValidRange(fromMs, toMs, "from must resolve to a time before to");
    return { fromMs, toMs };
  }

  private async _catalogMarketBounds(
    source: string,
    market: string,
  ): Promise<CatalogMarketBounds> {
    const payload = await this.catalog({ source, market });

    for (const catalogMarket of payload.markets) {
      if (catalogMarket.source !== source || catalogMarket.market !== market) {
        continue;
      }
      if (!catalogMarket.start || !catalogMarket.end) {
        throw new PolarisError(
          `Catalog entry for '${source}/${market}' did not include valid start/end timestamps`,
        );
      }

      const accessStatus = catalogMarket.access?.status?.trim().toLowerCase();

      return {
        startMs: toEpochMs(catalogMarket.start),
        endMs: toEpochMs(catalogMarket.end),
        accessStatus: accessStatus || undefined,
        publicCutoffMs: endOfPublicCutoffDayMs(
          catalogMarket.access?.public_cutoff_date,
        ),
      };
    }

    throw new NotFoundError(`Catalog did not include dataset '${source}/${market}'`);
  }

  /**
   * Core routine: resolve standardized snapshot coverage for a time range,
   * materialize missing files locally, and yield matching events one at a time.
   */
  private _readSnapshotEvents<T extends Json>(
    source: string,
    market: string,
    fromMs: number,
    toMs: number,
    filter: (event: Json) => event is T,
  ): AsyncGenerator<T>;
  private _readSnapshotEvents(
    source: string,
    market: string,
    fromMs: number,
    toMs: number,
    filter?: (event: Json) => boolean,
  ): AsyncGenerator<Json>;
  private async *_readSnapshotEvents(
    source: string,
    market: string,
    fromMs: number,
    toMs: number,
    filter?: (event: Json) => boolean,
  ): AsyncGenerator<Json> {
    const layout = await this._getLayout();
    const coverage = await this._resolveSnapshotCoverage(
      source,
      market,
      fromMs,
      toMs,
      layout,
    );

    if (coverage.paths.length === 0 || coverage.gaps.length > 0) {
      throw new PolarisError(
        `Requested standardized snapshot range could not be satisfied for '${source}/${market}'`,
      );
    }

    const storage = await this._getStorage();
    for (const filePath of coverage.paths) {
      const lines = await readSnapshotLines(storage, filePath);
      for (const line of lines) {
        let event: Json;
        try {
          event = JSON.parse(line) as Json;
        } catch {
          continue;
        }

        const ts = normalizeTimestampMs(event.timestamp);
        if (ts === undefined || ts < fromMs || ts >= toMs) continue;
        if (filter && !filter(event)) continue;

        if (event.timestamp !== ts) {
          event = { ...event, timestamp: ts };
        }
        yield event;
      }
    }
  }

  private async _resolveSnapshotCoverage(
    source: string,
    market: string,
    fromMs: number,
    toMs: number,
    layout: StorageLayout,
  ): Promise<ResolvedSnapshotCoverage> {
    const requiredDates = datesInRange(fromMs, toMs);
    const requiredRangesByDay = this._requiredSnapshotRangesByDay(fromMs, toMs);

    const localSnapshots = await this._listLocalSnapshots(
      layout,
      source,
      market,
      requiredDates,
    );

    const localByDay = new Map<string, LocalSnapshotFileEntry[]>();
    for (const entry of localSnapshots) {
      const dayEntries = localByDay.get(entry.date) ?? [];
      dayEntries.push(entry);
      localByDay.set(entry.date, dayEntries);
    }

    const localPaths: string[] = [];
    const localGaps: GapInterval[] = [];
    for (const date of requiredDates) {
      const range = requiredRangesByDay.get(date);
      if (!range) continue;

      const selection = this._resolveSnapshotDaySelection(
        localByDay.get(date) ?? [],
        range.startMs,
        range.endMs,
        utcDayStartMs(date),
      );
      localPaths.push(...selection.selected.map((entry) => entry.path));
      localGaps.push(...selection.gaps);
    }

    if (localPaths.length > 0 && localGaps.length === 0) {
      return { paths: localPaths, gaps: [] };
    }

    const snapshotQueryStartMs = utcDayStartMs(requiredDates[0] ?? epochMsToDate(fromMs));
    const snapshotQueryEndMs =
      utcDayStartMs(requiredDates.at(-1) ?? epochMsToDate(toMs - 1)) + 86_400_000;
    const remoteSnapshots = await this.listSnapshots({
      source,
      market,
      from: snapshotQueryStartMs,
      to: snapshotQueryEndMs,
    });

    const remoteByDay = new Map<string, SnapshotEntry[]>();
    for (const snapshot of remoteSnapshots) {
      const date = snapshot.date ?? inferredSnapshotDate(snapshot.key);
      if (!date || !requiredRangesByDay.has(date)) continue;
      const dayEntries = remoteByDay.get(date) ?? [];
      dayEntries.push(snapshot);
      remoteByDay.set(date, dayEntries);
    }

    const selectedSnapshots: SnapshotEntry[] = [];
    const gaps: GapInterval[] = [];
    for (const date of requiredDates) {
      const range = requiredRangesByDay.get(date);
      if (!range) continue;

      const selection = this._resolveSnapshotDaySelection(
        remoteByDay.get(date) ?? [],
        range.startMs,
        range.endMs,
        utcDayStartMs(date),
      );
      selectedSnapshots.push(...selection.selected);
      gaps.push(...selection.gaps);
    }

    const localByKey = new Map(localSnapshots.map((entry) => [entry.key, entry] as const));
    const missingSnapshots = selectedSnapshots.filter((snapshot) => {
      const local = localByKey.get(snapshot.key);
      return !local;
    });
    if (missingSnapshots.length > 0) {
      const ensured = await this._ensureLocalSnapshots(
        missingSnapshots,
        layout,
      );
      for (const entry of ensured) {
        localByKey.set(entry.key, entry);
      }
    }

    const paths: string[] = [];
    for (const snapshot of selectedSnapshots) {
      const local = localByKey.get(snapshot.key);
      const storage = await this._getStorage();
      if (!local || !(await fileExists(storage, local.path))) {
        throw new PolarisError(
          "Selected standardized snapshots could not be materialized locally",
        );
      }
      paths.push(local.path);
    }

    return { paths, gaps };
  }

  private _requiredSnapshotRangesByDay(
    fromMs: number,
    toMs: number,
  ): Map<string, { startMs: number; endMs: number }> {
    const ranges = new Map<string, { startMs: number; endMs: number }>();
    for (const date of datesInRange(fromMs, toMs)) {
      const dayStartMs = utcDayStartMs(date);
      ranges.set(date, {
        startMs: Math.max(fromMs, dayStartMs),
        endMs: Math.min(toMs, dayStartMs + 86_400_000),
      });
    }
    return ranges;
  }

  private _resolveSnapshotDaySelection<T extends SnapshotEntry | LocalSnapshotFileEntry>(
    entries: T[],
    rangeStartMs: number,
    rangeEndMs: number,
    dayStartMs: number,
  ): { selected: T[]; gaps: GapInterval[] } {
    if (entries.length === 0) {
      return {
        selected: [],
        gaps: [{ startMs: rangeStartMs, endMs: rangeEndMs }],
      };
    }

    const dayEndMs = dayStartMs + 86_400_000;
    const dailyEntries = entries
      .filter((entry) => {
        const bounds = this._snapshotEntryBounds(entry);
        return bounds.startMs === undefined && bounds.endMs === undefined && entry.hour === undefined;
      })
      .sort((a, b) => a.key.localeCompare(b.key));
    if (dailyEntries.length > 0) {
      return { selected: [dailyEntries[0]], gaps: [] };
    }

    const ordered = entries
      .map((entry) => ({ entry, ...this._snapshotEntryBounds(entry) }))
      .filter(
        (
          item,
        ): item is { entry: T; startMs: number; endMs?: number } => item.startMs !== undefined,
      )
      .sort((a, b) => a.startMs - b.startMs || a.entry.key.localeCompare(b.entry.key));

    const intervals: Array<{ entry: T; startMs: number; endMs: number }> = [];
    for (let index = 0; index < ordered.length; index += 1) {
      const current = ordered[index];
      let resolvedEndMs = current.endMs;
      if (resolvedEndMs === undefined) {
        for (let next = index + 1; next < ordered.length; next += 1) {
          if (ordered[next].startMs > current.startMs) {
            resolvedEndMs = ordered[next].startMs;
            break;
          }
        }
      }
      resolvedEndMs ??= dayEndMs;
      if (resolvedEndMs <= current.startMs) continue;
      intervals.push({
        entry: current.entry,
        startMs: current.startMs,
        endMs: resolvedEndMs,
      });
    }

    const selected: T[] = [];
    const coveredRanges: Array<{ startMs: number; endMs: number }> = [];
    const seen = new Set<string>();
    for (const interval of intervals) {
      const overlapStartMs = Math.max(rangeStartMs, interval.startMs);
      const overlapEndMs = Math.min(rangeEndMs, interval.endMs);
      if (overlapStartMs >= overlapEndMs) continue;

      if (!seen.has(interval.entry.key)) {
        selected.push(interval.entry);
        seen.add(interval.entry.key);
      }
      coveredRanges.push({ startMs: overlapStartMs, endMs: overlapEndMs });
    }

    return {
      selected,
      gaps: this._gapIntervalsFromRanges(rangeStartMs, rangeEndMs, coveredRanges),
    };
  }

  private _gapIntervalsFromRanges(
    startMs: number,
    endMs: number,
    coveredRanges: Array<{ startMs: number; endMs: number }>,
  ): GapInterval[] {
    const gaps: GapInterval[] = [];
    let cursorMs = startMs;

    for (const range of coveredRanges.sort((a, b) => a.startMs - b.startMs)) {
      if (range.endMs <= cursorMs) continue;
      if (range.startMs > cursorMs) {
        gaps.push({ startMs: cursorMs, endMs: range.startMs });
      }
      cursorMs = Math.max(cursorMs, range.endMs);
    }

    if (cursorMs < endMs) {
      gaps.push({ startMs: cursorMs, endMs });
    }

    return gaps;
  }

  private _snapshotEntryBounds(
    entry: SnapshotEntry | LocalSnapshotFileEntry,
  ): { startMs?: number; endMs?: number } {
    if ("startMs" in entry || "endMs" in entry) {
      return {
        startMs: entry.startMs,
        endMs: entry.endMs,
      };
    }

    const startMs = parseSnapshotDateTime(entry.start) ??
      (entry.date ? inferSnapshotStartMs(entry.key, entry.date) : undefined);
    const endMs = parseSnapshotDateTime(entry.end) ??
      (entry.date ? inferSnapshotEndMs(entry.key, entry.date) : undefined);

    if (startMs !== undefined) {
      return { startMs, endMs };
    }

    if (entry.date && entry.hour !== undefined) {
      const fallbackStartMs = utcDayStartMs(entry.date) + entry.hour * 3_600_000;
      return {
        startMs: fallbackStartMs,
        endMs: endMs ?? fallbackStartMs + 3_600_000,
      };
    }

    return {};
  }

  private async _listLocalSnapshots(
    layout: StorageLayout,
    source: string,
    market: string,
    dates: string[],
  ): Promise<LocalSnapshotFileEntry[]> {
    const storage = await this._getStorage();
    const snapshots: LocalSnapshotFileEntry[] = [];

    for (const date of dates) {
      const dir = storage.join(layout.dataDir, "standard", source, market, date);
      let entries;
      try {
        entries = await storage.readdir(dir);
      } catch {
        continue;
      }

      for (const entry of entries) {
        if (!entry.endsWith(".jsonl.zst")) continue;

        const key = entry.slice(0, -".jsonl.zst".length);
        try {
          const parsed = parseSnapshotKey(key);
          if (
            parsed.tier !== "standard" ||
            parsed.source !== source ||
            parsed.market !== market ||
            parsed.date !== date
          ) {
            continue;
          }

          const path = storage.join(dir, entry);
          snapshots.push({
            key,
            source,
            market,
            date,
            hour: parsed.hour,
            filename: parsed.filename,
            path,
            startMs: inferSnapshotStartMs(key, date),
            endMs: inferSnapshotEndMs(key, date),
          });
        } catch {
          continue;
        }
      }
    }

    snapshots.sort((a, b) =>
      (a.startMs ?? Number.MAX_SAFE_INTEGER) - (b.startMs ?? Number.MAX_SAFE_INTEGER) ||
      a.key.localeCompare(b.key),
    );
    return snapshots;
  }

  private async _ensureLocalSnapshots(
    snapshots: SnapshotEntry[],
    layout: StorageLayout,
  ): Promise<LocalSnapshotFileEntry[]> {
    if (snapshots.length === 0) return [];

    const grouped = new Map<string, SnapshotEntry[]>();
    for (const snapshot of snapshots) {
      const source = snapshot.source ?? inferredSnapshotSource(snapshot.key);
      const market = snapshot.market ?? inferredSnapshotMarket(snapshot.key);
      const date = snapshot.date ?? inferredSnapshotDate(snapshot.key);
      if (!source || !market || !date) {
        throw new PolarisError(
          `Snapshot '${snapshot.key}' did not include source, market, or date metadata`,
        );
      }

      const groupKey = `${source}\u0000${market}\u0000${date}`;
      const group = grouped.get(groupKey) ?? [];
      group.push({ ...snapshot, source, market, date });
      grouped.set(groupKey, group);
    }

    const downloadJobs = (
      await this._mapWithConcurrency(
        Array.from(grouped.values()),
        this._snapshotDownloadConcurrency,
        async (group) => {
          const first = group[0];
          const manifest = await this.getSnapshotDownloadUrls({
            source: first.source as string,
            market: first.market as string,
            date: first.date as string,
          });
          const manifestUrls = new Map(
            manifest.snapshots.map((entry) => [entry.key, entry.url] as const),
          );

          return group.map((snapshot) => {
            const url = manifestUrls.get(snapshot.key);
            if (!url) {
              throw new NotFoundError(
                `Bulk download manifest did not include selected snapshot '${snapshot.key}'`,
              );
            }

            return {
              url,
              dataPath: dataFilePath(layout.dataDir, snapshot.key),
            };
          });
        },
      )
    ).flat();

    await this._runWithConcurrency(
      downloadJobs,
      this._snapshotDownloadConcurrency,
      async ({ url, dataPath }) => {
        await this._downloadSnapshotFromUrl(url, dataPath);
      },
    );

    const ensured = await this._listLocalSnapshots(
      layout,
      snapshots[0]?.source ?? "",
      snapshots[0]?.market ?? "",
      Array.from(new Set(snapshots.map((snapshot) => snapshot.date ?? inferredSnapshotDate(snapshot.key)).filter(Boolean) as string[])),
    );
    const byKey = new Map(ensured.map((entry) => [entry.key, entry] as const));

    return snapshots.map((snapshot) => {
      const local = byKey.get(snapshot.key);
      if (!local) {
        throw new PolarisError(
          `Snapshot '${snapshot.key}' could not be materialized locally`,
        );
      }
      return local;
    });
  }

  /**
   * Download a snapshot into the Rust-style `data/` tree.
   */
  private async _downloadSnapshotFromUrl(
    url: string,
    dataPath: string,
  ): Promise<void> {
    const storage = await this._getStorage();
    if (await fileExists(storage, dataPath)) return;

    const response = await this._request(url, {
      auth: "none",
      redirect: "follow",
    });

    if (!response.ok) {
      throw new PolarisError(
        `Failed to download snapshot from ${url}: HTTP ${response.status}`,
      );
    }

    const buffer = new Uint8Array(await response.arrayBuffer());
    await storage.mkdir(storage.dirname(dataPath));
    await storage.writeFile(dataPath, buffer);
  }

  private async _runWithConcurrency<T>(
    items: T[],
    concurrency: number,
    worker: (item: T, index: number) => Promise<void>,
  ): Promise<void> {
    await this._mapWithConcurrency(items, concurrency, async (item, index) => {
      await worker(item, index);
      return undefined;
    });
  }

  private async _mapWithConcurrency<T, U>(
    items: T[],
    concurrency: number,
    worker: (item: T, index: number) => Promise<U>,
  ): Promise<U[]> {
    if (items.length === 0) return [];

    const limit = Math.max(1, Math.min(concurrency, items.length));
    const results = new Array<U>(items.length);
    let cursor = 0;

    const runWorker = async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= items.length) return;
        results[index] = await worker(items[index], index);
      }
    };

    await Promise.all(
      Array.from({ length: limit }, () => runWorker()),
    );
    return results;
  }

  // -----------------------------------------------------------------------
  // Internals – HTTP layer
  // -----------------------------------------------------------------------

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
  // Internals – pagination
  // -----------------------------------------------------------------------

  private async _paginateAll<T = Json>(
    path: string,
    baseParams: Record<string, string>,
    auth: AuthMode,
  ): Promise<T[]> {
    const items: T[] = [];
    let cursor: string | undefined;

    do {
      const params = cursor ? { ...baseParams, cursor } : { ...baseParams };
      const res = await this._getJson<PaginatedResponse<T>>(path, {
        params,
        auth,
      });
      items.push(...res.data);
      cursor = res.next_cursor ?? undefined;
    } while (cursor);

    return items;
  }

  // -----------------------------------------------------------------------
  // Internals – layout lazy init
  // -----------------------------------------------------------------------

  private async _getLayout(): Promise<StorageLayout> {
    if (!this._layout) {
      const storage = await this._getStorage();
      this._layout = await ensureLayout(storage, this._root);
    }
    return this._layout;
  }
}

// ===========================================================================
// Module-level helpers (not exported)
// ===========================================================================

class VwapAggregator {
  private readonly _intervalMs: number;
  private readonly _rows = new Map<
    number,
    { timestamp: number; volume: number; quoteVolume: number; trades: number }
  >();

  constructor(interval: string) {
    this._intervalMs = intervalToMs(interval);
  }

  add(timestamp: number, price: number, quantity: number): void {
    if (!Number.isFinite(timestamp) || quantity <= 0) return;

    const bucket =
      Math.floor(timestamp / this._intervalMs) * this._intervalMs;
    const row = this._rows.get(bucket);

    if (!row) {
      this._rows.set(bucket, {
        timestamp: bucket,
        volume: quantity,
        quoteVolume: price * quantity,
        trades: 1,
      });
      return;
    }

    row.volume += quantity;
    row.quoteVolume += price * quantity;
    row.trades += 1;
  }

  finish(): VwapBar[] {
    return Array.from(this._rows.values())
      .sort((a, b) => a.timestamp - b.timestamp)
      .map((row) => ({
        timestamp: row.timestamp,
        vwap: row.volume > 0 ? row.quoteVolume / row.volume : null,
        volume: row.volume,
        quote_volume: row.quoteVolume,
        trades: row.trades,
      }));
  }
}

class VolatilityAggregator {
  private readonly _intervalMs: number;
  private readonly _points: Array<[timestamp: number, price: number]> = [];

  constructor(interval: string) {
    this._intervalMs = intervalToMs(interval);
  }

  add(timestamp: number, price: number): void {
    if (!Number.isFinite(timestamp) || price <= 0) return;
    this._points.push([timestamp, price]);
  }

  finish(): VolatilityBar[] {
    const points = this._points.slice().sort((a, b) => a[0] - b[0]);
    const buckets = new Map<
      number,
      {
        timestamp: number;
        returns: number;
        mean: number;
        m2: number;
        lastPrice: number | undefined;
      }
    >();

    for (const [timestamp, price] of points) {
      const bucket =
        Math.floor(timestamp / this._intervalMs) * this._intervalMs;

      let state = buckets.get(bucket);
      if (!state) {
        state = {
          timestamp: bucket,
          returns: 0,
          mean: 0,
          m2: 0,
          lastPrice: undefined,
        };
        buckets.set(bucket, state);
      }

      if (state.lastPrice !== undefined) {
        const logReturn = Math.log(price / state.lastPrice);
        state.returns += 1;
        const delta = logReturn - state.mean;
        state.mean += delta / state.returns;
        const delta2 = logReturn - state.mean;
        state.m2 += delta * delta2;
      }

      state.lastPrice = price;
    }

    return Array.from(buckets.values())
      .sort((a, b) => a.timestamp - b.timestamp)
      .flatMap((state) => {
        if (state.returns < 2) return [];
        const variance = state.m2 / (state.returns - 1);
        return [{
          timestamp: state.timestamp,
          volatility: Math.sqrt(variance),
          returns: state.returns,
        }];
      });
  }
}

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

function normalizeSnapshotEntry(entry: unknown): SnapshotEntry {
  if (!isRecord(entry)) {
    throw new PolarisError("Snapshot entry was not an object");
  }

  const key = entry.key ?? entry.path ?? entry.name;
  if (typeof key !== "string" || key.length === 0) {
    throw new PolarisError("Snapshot entry did not include a valid key");
  }

  let inferred: ReturnType<typeof parseSnapshotKey> | undefined;
  try {
    inferred = parseSnapshotKey(key);
  } catch {
    inferred = undefined;
  }

  const date =
    typeof entry.date === "string" && entry.date.length > 0
      ? entry.date
      : inferred?.date;
  const source =
    typeof entry.source === "string" && entry.source.length > 0
      ? entry.source
      : inferred?.source;
  const market =
    typeof entry.market === "string" && entry.market.length > 0
      ? entry.market
      : inferred?.market;
  const hour =
    typeof entry.hour === "number" && Number.isInteger(entry.hour) &&
    entry.hour >= 0 && entry.hour <= 23
      ? entry.hour
      : inferred?.hour;

  return {
    key,
    source,
    market,
    date,
    start: normalizeSnapshotDateTime(entry.start),
    end: normalizeSnapshotDateTime(entry.end),
    hour,
    filename:
      typeof entry.filename === "string" && entry.filename.length > 0
        ? entry.filename
        : inferred?.filename,
  };
}

function normalizeSnapshotDateTime(value: unknown): string | undefined {
  const parsedMs = parseSnapshotDateTime(value);
  return parsedMs === undefined ? undefined : new Date(parsedMs).toISOString();
}

function normalizeSnapshotDownloadManifest(
  payload: unknown,
): SnapshotDownloadManifest {
  if (!isRecord(payload)) {
    throw new PolarisError("Download manifest response was not an object");
  }

  const { source, market, date, total, total_bytes, snapshots } = payload;
  if (typeof source !== "string" || source.length === 0) {
    throw new PolarisError("Download manifest did not include a valid source");
  }
  if (typeof market !== "string" || market.length === 0) {
    throw new PolarisError("Download manifest did not include a valid market");
  }
  if (typeof date !== "string" || date.length === 0) {
    throw new PolarisError("Download manifest did not include a valid date");
  }
  if (typeof total !== "number" || !Number.isFinite(total)) {
    throw new PolarisError("Download manifest did not include a valid total");
  }
  if (typeof total_bytes !== "number" || !Number.isFinite(total_bytes)) {
    throw new PolarisError("Download manifest did not include valid total_bytes");
  }
  if (!Array.isArray(snapshots)) {
    throw new PolarisError("Download manifest did not include a valid snapshots array");
  }

  return {
    source,
    market,
    date,
    total,
    total_bytes,
    snapshots: snapshots.map((entry) => normalizeSnapshotDownloadEntry(entry)),
  };
}

function normalizeSnapshotDownloadEntry(
  entry: unknown,
): SnapshotDownloadManifest["snapshots"][number] {
  if (!isRecord(entry)) {
    throw new PolarisError("Download manifest snapshot entry was not an object");
  }

  const { date, timestamp, key, url, expires_in_seconds } = entry;
  if (typeof date !== "string" || date.length === 0) {
    throw new PolarisError("Download manifest snapshot entry did not include a valid date");
  }
  if (typeof timestamp !== "string" || timestamp.length === 0) {
    throw new PolarisError("Download manifest snapshot entry did not include a valid timestamp");
  }
  if (typeof key !== "string" || key.length === 0) {
    throw new PolarisError("Download manifest snapshot entry did not include a valid key");
  }
  if (typeof url !== "string" || url.length === 0) {
    throw new PolarisError("Download manifest snapshot entry did not include a valid url");
  }
  if (
    typeof expires_in_seconds !== "number" ||
    !Number.isFinite(expires_in_seconds)
  ) {
    throw new PolarisError(
      "Download manifest snapshot entry did not include valid expires_in_seconds",
    );
  }

  return {
    date,
    timestamp,
    key,
    url,
    expires_in_seconds,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function intervalToMs(interval: string): number {
  const match = interval.match(/^(\d+)(ms|s|m|h)$/);
  if (!match) {
    throw new PolarisError(`Invalid interval: ${interval}`);
  }

  const amount = Number.parseInt(match[1], 10);
  switch (match[2]) {
    case "ms":
      return amount;
    case "s":
      return amount * 1_000;
    case "m":
      return amount * 60_000;
    case "h":
      return amount * 3_600_000;
    default:
      throw new PolarisError(`Invalid interval: ${interval}`);
  }
}

function pointSeriesName(event: Json): string | undefined {
  if (event.type !== "point" || !isRecord(event.data)) {
    return undefined;
  }
  return typeof event.data.series === "string" ? event.data.series : undefined;
}

function isFundingRateEvent(event: Json): event is FundingRateEvent {
  return pointSeriesName(event) === "funding_rate";
}

function isMarkPriceEvent(event: Json): event is MarkPriceEvent {
  return pointSeriesName(event) === "mark_price";
}

function hasOrderbookSides(event: Json): event is OrderbookEvent {
  return extractOrderbookSides(event) !== undefined;
}

function coerceNumeric(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

function extractOrderbookSides(
  row: Json,
): { bids: unknown[]; asks: unknown[] } | undefined {
  const candidates: unknown[] = [row];
  if (isRecord(row.data)) {
    candidates.push(row.data);
  }

  for (const candidate of candidates) {
    if (!isRecord(candidate)) continue;

    const { bids, asks } = candidate;
    if (Array.isArray(bids) && Array.isArray(asks)) {
      return { bids, asks };
    }
  }

  return undefined;
}

function parseOrderbookLevel(level: unknown): [number, number] | undefined {
  let priceRaw: unknown;
  let quantityRaw: unknown;

  if (isRecord(level)) {
    priceRaw = level.price;
    quantityRaw = level.quantity ?? level.size ?? level.amount;
  } else if (Array.isArray(level) && level.length >= 2) {
    [priceRaw, quantityRaw] = level;
  }

  const price = coerceNumeric(priceRaw);
  const quantity = coerceNumeric(quantityRaw);
  if (price === undefined || quantity === undefined) {
    return undefined;
  }

  return [price, quantity];
}

function bestOrderbookLevel(
  levels: unknown[],
  side: "bid" | "ask",
): [number, number] | undefined {
  let bestPrice: number | undefined;
  let bestQuantity: number | undefined;

  for (const level of levels) {
    const parsed = parseOrderbookLevel(level);
    if (!parsed) continue;

    const [price, quantity] = parsed;
    if (
      bestPrice === undefined ||
      (side === "bid" && price > bestPrice) ||
      (side === "ask" && price < bestPrice)
    ) {
      bestPrice = price;
      bestQuantity = quantity;
    }
  }

  if (bestPrice === undefined || bestQuantity === undefined) {
    return undefined;
  }

  return [bestPrice, bestQuantity];
}

function sortedOrderbookLevels(
  levels: unknown[],
  side: "bid" | "ask",
): Array<[number, number]> {
  const parsed = levels
    .map((level) => parseOrderbookLevel(level))
    .filter((level): level is [number, number] => level !== undefined)
    .filter(([price, quantity]) => price > 0 && quantity > 0);

  parsed.sort((a, b) => (side === "bid" ? b[0] - a[0] : a[0] - b[0]));
  return parsed;
}

function deriveBbo(row: Json): BboQuote | undefined {
  const timestamp = row.timestamp;
  if (typeof timestamp !== "number" || !Number.isFinite(timestamp)) {
    return undefined;
  }

  const sides = extractOrderbookSides(row);
  if (!sides) return undefined;

  const bid = bestOrderbookLevel(sides.bids, "bid");
  const ask = bestOrderbookLevel(sides.asks, "ask");
  if (!bid || !ask) return undefined;

  return {
    timestamp,
    bid_price: bid[0],
    bid_quantity: bid[1],
    ask_price: ask[0],
    ask_quantity: ask[1],
  };
}

function depthNotionalWithinPct(
  levels: Array<[number, number]>,
  side: "bid" | "ask",
  midPrice: number,
  depthPct: number,
): number {
  if (side === "bid") {
    const cutoff = midPrice * (1 - depthPct);
    return levels.reduce(
      (sum, [price, quantity]) =>
        price >= cutoff ? sum + price * quantity : sum,
      0,
    );
  }

  const cutoff = midPrice * (1 + depthPct);
  return levels.reduce(
    (sum, [price, quantity]) =>
      price <= cutoff ? sum + price * quantity : sum,
    0,
  );
}

function quoteTotalForBaseQuantity(
  levels: Array<[number, number]>,
  targetQuantity: number,
): number | undefined {
  let remainingQuantity = targetQuantity;
  let quoteTotal = 0;

  for (const [price, availableQuantity] of levels) {
    const fillQuantity = Math.min(availableQuantity, remainingQuantity);
    quoteTotal += fillQuantity * price;
    remainingQuantity -= fillQuantity;
    if (remainingQuantity <= 1e-12) {
      return quoteTotal;
    }
  }

  return undefined;
}

function deriveDepthMetrics(
  row: Json,
  depthPct: number,
  slippageNotional: number,
): DepthMetricsRow | undefined {
  const timestamp = row.timestamp;
  if (typeof timestamp !== "number" || !Number.isFinite(timestamp)) {
    return undefined;
  }

  const sides = extractOrderbookSides(row);
  if (!sides) return undefined;

  const bids = sortedOrderbookLevels(sides.bids, "bid");
  const asks = sortedOrderbookLevels(sides.asks, "ask");
  if (bids.length === 0 || asks.length === 0) {
    return undefined;
  }

  const [bidPrice] = bids[0];
  const [askPrice] = asks[0];
  if (askPrice < bidPrice) {
    return undefined;
  }

  const midPrice = (bidPrice + askPrice) / 2;
  const spread = askPrice - bidPrice;
  const spreadBps = midPrice > 0 ? (spread / midPrice) * 10_000 : null;

  const bidDepthNotional = depthNotionalWithinPct(
    bids,
    "bid",
    midPrice,
    depthPct,
  );
  const askDepthNotional = depthNotionalWithinPct(
    asks,
    "ask",
    midPrice,
    depthPct,
  );
  const totalDepthNotional = bidDepthNotional + askDepthNotional;
  const depthImbalance =
    totalDepthNotional > 0
      ? (bidDepthNotional - askDepthNotional) / totalDepthNotional
      : null;

  const targetBaseQuantity =
    midPrice > 0 ? slippageNotional / midPrice : null;

  let buyAveragePrice: number | null = null;
  let sellAveragePrice: number | null = null;
  let buySlippage: number | null = null;
  let sellSlippage: number | null = null;
  let buySlippageBps: number | null = null;
  let sellSlippageBps: number | null = null;

  if (targetBaseQuantity !== null && targetBaseQuantity > 0) {
    const buyQuoteTotal = quoteTotalForBaseQuantity(asks, targetBaseQuantity);
    const sellQuoteTotal = quoteTotalForBaseQuantity(bids, targetBaseQuantity);

    if (buyQuoteTotal !== undefined) {
      buyAveragePrice = buyQuoteTotal / targetBaseQuantity;
      buySlippage = buyQuoteTotal - slippageNotional;
      buySlippageBps =
        ((buyAveragePrice - midPrice) / midPrice) * 10_000;
    }

    if (sellQuoteTotal !== undefined) {
      sellAveragePrice = sellQuoteTotal / targetBaseQuantity;
      sellSlippage = slippageNotional - sellQuoteTotal;
      sellSlippageBps =
        ((midPrice - sellAveragePrice) / midPrice) * 10_000;
    }
  }

  return {
    timestamp: Math.trunc(timestamp),
    bid_price: bidPrice,
    ask_price: askPrice,
    mid_price: midPrice,
    bid_ask_spread: spread,
    bid_ask_spread_bps: spreadBps,
    depth_pct: depthPct,
    bid_depth_notional: bidDepthNotional,
    ask_depth_notional: askDepthNotional,
    depth_imbalance: depthImbalance,
    slippage_notional: slippageNotional,
    target_base_quantity: targetBaseQuantity,
    buy_average_price: buyAveragePrice,
    sell_average_price: sellAveragePrice,
    buy_slippage: buySlippage,
    sell_slippage: sellSlippage,
    buy_slippage_bps: buySlippageBps,
    sell_slippage_bps: sellSlippageBps,
  };
}

function buildSnapshotParams(
  options: ListSnapshotsOptions,
): Record<string, string> {
  const p: Record<string, string> = {
    source: options.source,
    market: options.market,
  };
  if (options.from !== undefined) p.from = toIso8601(options.from);
  if (options.to !== undefined) p.to = toIso8601(options.to);
  if (options.limit !== undefined) p.limit = String(options.limit);
  return p;
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

// ---------------------------------------------------------------------------
// Snapshot file reading (zstd + NDJSON)
// ---------------------------------------------------------------------------

async function readSnapshotLines(storage: IStorage, filePath: string): Promise<string[]> {
  const compressed = await storage.readFile(filePath);
  const decompressed = decompress(compressed);
  const text = new TextDecoder().decode(decompressed);
  return text.split("\n").filter((l) => l.trim().length > 0);
}

function normalizeTimestampMs(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  const timestamp = Math.trunc(value);
  return Math.abs(timestamp) >= 100_000_000_000_000
    ? Math.trunc(timestamp / 1_000)
    : timestamp;
}

function parseSnapshotDateTime(value: unknown): number | undefined {
  if (typeof value === "string" && value.length > 0) {
    const parsed = new Date(value).getTime();
    return Number.isNaN(parsed) ? undefined : parsed;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return normalizeTimestampMs(value);
  }
  return undefined;
}

function utcDayStartMs(date: string): number {
  const parsed = new Date(`${date}T00:00:00Z`).getTime();
  if (Number.isNaN(parsed)) {
    throw new PolarisError(`Invalid UTC date: ${date}`);
  }
  return parsed;
}

function epochMsToDate(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

function inferredSnapshotSource(key: string): string | undefined {
  try {
    return parseSnapshotKey(key).source;
  } catch {
    return undefined;
  }
}

function inferredSnapshotMarket(key: string): string | undefined {
  try {
    return parseSnapshotKey(key).market;
  } catch {
    return undefined;
  }
}

function inferredSnapshotDate(key: string): string | undefined {
  try {
    return parseSnapshotKey(key).date;
  } catch {
    return undefined;
  }
}

function assertValidRange(
  fromMs: number,
  toMs: number,
  message = "from must be before to",
): void {
  if (fromMs >= toMs) {
    throw new PolarisError(message);
  }
}

function endOfPublicCutoffDayMs(
  dateText: string | null | undefined,
): number | undefined {
  if (!dateText) return undefined;

  const startMs = Date.parse(`${dateText}T00:00:00Z`);
  if (Number.isNaN(startMs)) return undefined;

  return startMs + 24 * 60 * 60 * 1000;
}

function normalizePositiveInteger(value: number | undefined): number | undefined {
  if (value === undefined || !Number.isFinite(value)) return undefined;

  const normalized = Math.floor(value);
  return normalized > 0 ? normalized : undefined;
}
