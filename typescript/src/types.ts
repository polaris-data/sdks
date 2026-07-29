// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/** Accepts ISO 8601 strings, `Date` instances, or Unix epoch milliseconds. */
export type TimeInput = string | Date | number;

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

/** Shape of the global `fetch` function so consumers can inject a custom one. */
export type FetchLike = typeof globalThis.fetch;

// ---------------------------------------------------------------------------
// Auth (internal)
// ---------------------------------------------------------------------------

export type AuthMode = "none" | "if-available" | "required";

// ---------------------------------------------------------------------------
// Paginated envelope – returned by /raw, /snapshots
// ---------------------------------------------------------------------------

export interface PaginatedResponse<T = Record<string, unknown>> {
  data: T[];
  next_cursor: string | null;
  has_more: boolean;
}

// ---------------------------------------------------------------------------
// Catalog – GET /catalog
// ---------------------------------------------------------------------------

export interface CatalogResponse {
  updatedAt: string;
  markets: CatalogMarket[];
}

/** Grouped source view retained for compatibility with older SDK examples. */
export interface CatalogSource {
  id: string;
  markets: CatalogMarket[];
}

export interface CatalogInstrument {
  base: string | null;
  quote: string | null;
  tick_size: string | number | null;
  lot_size: string | number | null;
  min_notional: string | number | null;
}

export interface CatalogMarket {
  source: string;
  market: string;
  start?: string;
  end?: string;
  source_type?: string;
  categories?: string[];
  access?: {
    status: string;
    public_cutoff_date?: string | null;
  };
  instrument: CatalogInstrument;
}

// ---------------------------------------------------------------------------
// Standardised event envelope
// ---------------------------------------------------------------------------

export interface StandardEvent extends Record<string, unknown> {
  timestamp: number;
  source: string;
  market: string;
  type: string;
  data: Record<string, unknown>;
}

export interface TradeData {
  price: number;
  quantity: number;
  side: string;
  [key: string]: unknown;
}

export interface TradeEvent extends StandardEvent {
  type: "trade";
  data: TradeData;
}

export interface PointSeriesData extends Record<string, unknown> {
  series: string;
}

export interface PointSeriesEvent extends StandardEvent {
  type: "point";
  data: PointSeriesData;
}

export interface FundingRateData extends PointSeriesData {
  series: "funding_rate";
}

export interface FundingRateEvent extends PointSeriesEvent {
  data: FundingRateData;
}

export interface MarkPriceData extends PointSeriesData {
  series: "mark_price";
}

export interface MarkPriceEvent extends PointSeriesEvent {
  data: MarkPriceData;
}

export type OrderbookLevel =
  | [number | string, number | string, ...unknown[]]
  | {
      price: number | string;
      quantity?: number | string;
      size?: number | string;
      amount?: number | string;
      [key: string]: unknown;
    };

export interface OrderbookSides {
  bids: OrderbookLevel[];
  asks: OrderbookLevel[];
}

export interface OrderbookData
  extends Record<string, unknown>, Partial<OrderbookSides> {}

export interface OrderbookEvent extends StandardEvent, Partial<OrderbookSides> {
  data: OrderbookData;
}

export interface BboQuote {
  timestamp: number;
  bid_price: number;
  bid_quantity: number;
  ask_price: number;
  ask_quantity: number;
}

export interface DepthMetricsRow {
  timestamp: number;
  bid_price: number;
  ask_price: number;
  mid_price: number;
  bid_ask_spread: number;
  bid_ask_spread_bps: number | null;
  depth_pct: number;
  bid_depth_notional: number;
  ask_depth_notional: number;
  depth_imbalance: number | null;
  slippage_notional: number;
  target_base_quantity: number | null;
  buy_average_price: number | null;
  sell_average_price: number | null;
  buy_slippage: number | null;
  sell_slippage: number | null;
  buy_slippage_bps: number | null;
  sell_slippage_bps: number | null;
}

// ---------------------------------------------------------------------------
// Trade-derived aggregates
// ---------------------------------------------------------------------------

export type OhlcvInterval = "100ms" | "1s" | "10s" | "1m" | "5m" | "15m" | "1h";

export interface OhlcvBar {
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  trades: number;
}

export interface VolumeBar {
  timestamp: number;
  volume: number;
}

export interface VwapBar {
  timestamp: number;
  vwap: number | null;
  volume: number;
  quote_volume: number;
  trades: number;
}

export interface VolatilityBar {
  timestamp: number;
  volatility: number;
  returns: number;
}

export interface TradingViewCandle {
  t: number;
  o: number;
  h: number;
  l: number;
  c: number;
}

export interface TradingViewVolume {
  t: number;
  v: number;
  trades?: number;
}

export interface TradingViewOhlcvResponse {
  candles: TradingViewCandle[];
  volumes: TradingViewVolume[];
}

// ---------------------------------------------------------------------------
// Snapshots – GET /snapshots
// ---------------------------------------------------------------------------

export interface SnapshotEntry {
  key: string;
  source?: string;
  market?: string;
  date?: string;
  start?: string;
  end?: string;
  hour?: number;
  filename?: string;
}

export interface SnapshotsResponse {
  source: string;
  market: string;
  access?: {
    status: string;
    public_cutoff_date?: string;
  };
  total: number;
  total_bytes: number;
  limit: number;
  has_more: boolean;
  next_cursor: string | null;
  snapshots: SnapshotEntry[];
}

// ---------------------------------------------------------------------------
// Snapshot download manifest – GET /download
// ---------------------------------------------------------------------------

export interface SnapshotDownloadEntry {
  date: string;
  timestamp: string;
  key: string;
  url: string;
  expires_in_seconds: number;
}

export interface SnapshotDownloadManifest {
  source: string;
  market: string;
  date: string;
  total: number;
  total_bytes: number;
  snapshots: SnapshotDownloadEntry[];
}

// ---------------------------------------------------------------------------
// Client constructor options
// ---------------------------------------------------------------------------

export interface PolarisClientOptions {
  /** Polaris API key. Falls back to `POLARIS_API_KEY` env var. */
  apiKey?: string;
  /** API base URL. Defaults to `https://api.polaris.supply`. */
  baseUrl?: string;
  /** Request timeout in milliseconds. Defaults to `30 000` (30 s). */
  timeout?: number;
  /** Custom fetch implementation (useful for testing or proxies). */
  fetch?: FetchLike;
  /**
   * Override the local dataset root directory.
   * Defaults to the platform-specific Polaris app-data directory,
   * overridable globally via `POLARIS_ROOT` env var.
   */
  datasetRoot?: string;
  /**
   * Maximum number of snapshot artifact downloads to run concurrently.
   * Defaults to `8`.
   */
  snapshotDownloadConcurrency?: number;
  /**
   * Custom storage implementation (useful for testing or advanced scenarios).
   * If not provided, storage is automatically detected based on platform.
   */
  storage?: import("./storage/interface").IStorage;
}

// ---------------------------------------------------------------------------
// Per-method option bags
// ---------------------------------------------------------------------------

export interface CatalogOptions {
  source?: string;
  market?: string;
}

/**
 * Options for snapshot-based historical data methods.
 * If `from` and/or `to` are omitted, the client infers a bounded window
 * from catalog metadata.
 */
export interface HistoricalQueryOptions {
  source: string;
  market: string;
  from?: TimeInput;
  to?: TimeInput;
}

export interface ListSnapshotsOptions {
  source: string;
  market: string;
  from?: TimeInput;
  to?: TimeInput;
  limit?: number;
}

/** Options for the /raw endpoint shape. `from`/`to` are optional. */
export interface RawQueryOptions {
  source: string;
  market: string;
  from?: TimeInput;
  to?: TimeInput;
  limit?: number;
  format?: string;
}

export interface OhlcvOptions extends HistoricalQueryOptions {
  interval: OhlcvInterval;
}

export interface VolumeOptions extends HistoricalQueryOptions {
  interval: OhlcvInterval;
}

export interface VwapOptions extends HistoricalQueryOptions {
  interval: OhlcvInterval;
}

export interface VolatilityOptions extends HistoricalQueryOptions {
  interval: OhlcvInterval;
  method?: "log_returns";
}

export interface DepthMetricsOptions extends HistoricalQueryOptions {
  depthPct?: number;
  slippageNotional?: number;
}

export interface ReplayOptions {
  source: string;
  market: string;
  from?: TimeInput;
  to?: TimeInput;
  /** `true` (default) streams standardised events from local snapshots. */
  standard?: boolean;
}

export interface SnapshotDownloadManifestOptions {
  source: string;
  market: string;
  date: string;
}
