// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/** Accepts ISO 8601 strings, `Date` instances, or Unix epoch milliseconds. */
export type TimeInput = string | Date | number;

/** Any JSON-serializable value. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

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
// Catalog – GET /catalog
// ---------------------------------------------------------------------------

export interface CatalogResponse {
  updatedAt: string;
  markets: CatalogMarket[];
}

/** Global public catalog counts returned by `GET /count`. */
export interface CatalogCount {
  updatedAt: string;
  sources: number;
  markets: number;
  by_source: Record<string, number>;
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
  symbol: string;
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

export interface LegacyStandardEvent extends Record<string, unknown> {
  timestamp: number;
  source: string;
  market: string;
  instrument?: string;
  type: string;
  data: Record<string, unknown>;
}

export interface StandardEventV2 extends Record<string, unknown> {
  collector_timestamp: number;
  collector_sequence: number;
  exchange_timestamp: number | null;
  exchange_sequence: string | null;
  source: string;
  market: string;
  instrument?: string;
  type: string;
  data: Record<string, unknown>;
}

export type StandardEvent = LegacyStandardEvent | StandardEventV2;

export interface LegacyTradeData {
  price: number;
  quantity: number;
  side: string;
  [key: string]: unknown;
}

export interface TradeDataV2 {
  order_id: string | null;
  price: number;
  quantity: number;
  side: "buy" | "sell" | null;
  [key: string]: unknown;
}

export interface LegacyTradeEvent extends LegacyStandardEvent {
  type: "trade";
  data: LegacyTradeData;
}

export interface TradeEventV2 extends StandardEventV2 {
  type: "trade";
  data: TradeDataV2;
}

export type TradeEvent = LegacyTradeEvent | TradeEventV2;

export type AmountKind = "exact_input" | "exact_output";

export type IntentStatus =
  | "submitted"
  | "open"
  | "partially_filled"
  | "executing"
  | "filled"
  | "settled"
  | "cancelled"
  | "expired"
  | "failed"
  | "unknown";

export interface AssetAmount extends Record<string, unknown> {
  asset_id: string;
  chain_id?: string;
  amount?: string;
  recipient?: string;
}

export interface IntentQuote extends Record<string, unknown> {
  quote_id: string;
  response: AssetAmount[];
}

export interface SettlementTransaction extends Record<string, unknown> {
  chain_id?: string;
  transaction_hash: string;
}

export interface IntentData extends Record<string, unknown> {
  rfq_id?: string;
  intent_id?: string;
  requester?: string;
  signer?: string;
  inputs: AssetAmount[];
  outputs: AssetAmount[];
  amount_kind?: AmountKind;
  expires_at?: number;
  quote?: IntentQuote;
  status?: IntentStatus;
  transactions: SettlementTransaction[];
  settled_at?: number;
}

export interface LegacyIntentEvent extends LegacyStandardEvent {
  type: "intent";
  data: IntentData;
  raw?: Json;
}

export interface IntentEventV2 extends StandardEventV2 {
  type: "intent";
  data: IntentData;
  raw?: Json;
}

export type IntentEvent = LegacyIntentEvent | IntentEventV2;

export interface OptionGreeks extends Record<string, unknown> {
  delta?: string;
  gamma?: string;
  vega?: string;
  theta?: string;
  rho?: string;
}

export interface OptionTickerData extends Record<string, unknown> {
  mark_price?: string;
  bid_price?: string;
  bid_size?: string;
  ask_price?: string;
  ask_size?: string;
  last_price?: string;
  index_price?: string;
  underlying_price?: string;
  forward_price?: string;
  mark_iv?: string;
  bid_iv?: string;
  ask_iv?: string;
  open_interest?: string;
  volume_24h?: string;
  turnover_24h?: string;
  premium_currency?: string;
  quantity_unit?: string;
  greeks?: OptionGreeks;
}

export interface LegacyOptionTickerEvent extends LegacyStandardEvent {
  type: "option_ticker";
  instrument: string;
  data: OptionTickerData;
}

export interface OptionTickerEventV2 extends StandardEventV2 {
  type: "option_ticker";
  instrument: string;
  data: OptionTickerData;
}

export type OptionTickerEvent = LegacyOptionTickerEvent | OptionTickerEventV2;

export interface PointSeriesData extends Record<string, unknown> {
  series: string;
  value: number;
}

export interface PointSeriesDataV2 extends Record<string, unknown> {
  series: string;
  value: string;
}

export interface LegacyPointSeriesEvent extends LegacyStandardEvent {
  type: "point";
  data: PointSeriesData;
}

export interface PointSeriesEventV2 extends StandardEventV2 {
  type: "point";
  data: PointSeriesDataV2;
}

export type PointSeriesEvent = LegacyPointSeriesEvent | PointSeriesEventV2;

export interface FundingRateData extends Record<string, unknown> {
  series: "funding_rate";
  value: number | string;
}

export type FundingRateEvent = PointSeriesEvent & { data: FundingRateData };

export interface MarkPriceData extends Record<string, unknown> {
  series: "mark_price" | "mark_px";
  value: number | string;
}

export type MarkPriceEvent = PointSeriesEvent & { data: MarkPriceData };

export interface PropammQuote {
  amount_in: string;
  amount_out: string;
}

export interface PropammQuoteLadderValues {
  event_id: string;
  chain_id: number;
  block_number: number;
  block_hash: string;
  parent_hash: string;
  transaction_hash: string;
  transaction_index: number;
  router: string;
  oracle: string | null;
  pool?: string;
  token_in: string;
  token_out: string;
  token_in_decimals: number;
  token_out_decimals: number;
  quotes: PropammQuote[];
}

export interface PropammQuoteLadderData extends Record<string, unknown> {
  series: "quote_ladder";
  values: PropammQuoteLadderValues;
}

export interface PropammQuoteLadderEvent extends StandardEventV2 {
  type: "record";
  data: PropammQuoteLadderData;
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

export interface OrderbookDataV2 extends OrderbookData {
  is_snapshot: boolean;
  bids: OrderbookLevel[];
  asks: OrderbookLevel[];
}

export interface LegacyOrderbookEvent extends LegacyStandardEvent, Partial<OrderbookSides> {
  data: OrderbookData;
}

export interface OrderbookEventV2 extends StandardEventV2 {
  type: "orderbook";
  data: OrderbookDataV2;
}

export type OrderbookEvent = LegacyOrderbookEvent | OrderbookEventV2;

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
  /** Realtime WebSocket URL. Defaults to `/stream` on the API origin. */
  streamUrl?: string;
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
  /** Materialize complete orderbooks from snapshots and deltas. Defaults to `true`. */
  materializeOrderbooks?: boolean;
}

/** Options for option ticker reads across a chain or one exact contract. */
export interface OptionTickerOptions extends HistoricalQueryOptions {
  /** Exact venue-native contract. Omit to read the whole option chain. */
  instrument?: string;
}

/** Options for raw snapshot-and-delta orderbook reads. */
export type L2UpdatesOptions = Omit<HistoricalQueryOptions, "materializeOrderbooks">;

export interface ListSnapshotsOptions {
  source: string;
  market: string;
  from?: TimeInput;
  to?: TimeInput;
  limit?: number;
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
  /** Materialize complete orderbooks from snapshots and deltas. Defaults to `true`. */
  materializeOrderbooks?: boolean;
}

export interface StreamOptions {
  source: string;
  markets: string[];
  /** Exact venue-native contract. Omit to subscribe to each whole market. */
  instrument?: string;
  includeBuffer?: boolean;
  /** Materialize complete orderbooks from snapshots and deltas. Defaults to `true`. */
  materializeOrderbooks?: boolean;
}

export interface SnapshotDownloadManifestOptions {
  source: string;
  market: string;
  date: string;
}
