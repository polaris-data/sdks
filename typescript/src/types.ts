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
  maker?: string;
  taker?: string;
  [key: string]: unknown;
}

export interface TradeDataV2 {
  order_id: string | null;
  price: number;
  quantity: number;
  side: "buy" | "sell" | null;
  maker?: string;
  taker?: string;
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

/** Flat row returned by GET /historical/trades. */
export interface TradeRow {
  event_id: string;
  source: string;
  market: string;
  collector_timestamp: number;
  source_capture_id: string;
  schema_version: number;
  price: number;
  quantity: number;
  exchange_timestamp?: number | null;
  instrument?: string | null;
  liquidation?: boolean | null;
  maker?: string | null;
  order_id?: string | null;
  side?: string | null;
  taker?: string | null;
}

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
  underlying?: string;
  strike?: string;
  expiry_timestamp?: number;
  option_type?: "call" | "put";
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

/** Partial flat row returned by GET /historical/options-ticker. */
export interface OptionTickerRow {
  event_id: string;
  source: string;
  market: string;
  instrument: string;
  collector_timestamp: number;
  source_capture_id: string;
  schema_version: number;
  exchange_timestamp?: number | null;
  expiry_timestamp?: number | null;
  ask_iv?: string | null;
  ask_price?: string | null;
  ask_size?: string | null;
  bid_iv?: string | null;
  bid_price?: string | null;
  bid_size?: string | null;
  delta?: string | null;
  forward_price?: string | null;
  gamma?: string | null;
  index_price?: string | null;
  last_price?: string | null;
  mark_iv?: string | null;
  mark_price?: string | null;
  open_interest?: string | null;
  option_type?: string | null;
  premium_currency?: string | null;
  quantity_unit?: string | null;
  rho?: string | null;
  strike?: string | null;
  theta?: string | null;
  turnover_24h?: string | null;
  underlying?: string | null;
  underlying_price?: string | null;
  vega?: string | null;
  volume_24h?: string | null;
}

export interface PerpetualTickerData extends Record<string, unknown> {
  last_price?: string;
  mark_price?: string;
  index_price?: string;
  oracle_price?: string;
  mid_price?: string;
  open_interest?: string;
  funding_rate?: string;
  funding_timestamp?: number;
  predicted_funding_rate?: string;
  premium?: string;
}

export interface LegacyPerpetualTickerEvent extends LegacyStandardEvent {
  type: "perpetual_ticker";
  data: PerpetualTickerData;
}

export interface PerpetualTickerEventV2 extends StandardEventV2 {
  type: "perpetual_ticker";
  data: PerpetualTickerData;
}

export type PerpetualTickerEvent =
  | LegacyPerpetualTickerEvent
  | PerpetualTickerEventV2;

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

/** Partial flat row returned by GET /historical/funding-rates. */
export interface FundingRateRow {
  event_id: string;
  source: string;
  market: string;
  collector_timestamp: number;
  source_capture_id: string;
  schema_version: number;
  exchange_timestamp?: number | null;
  funding_timestamp?: number | null;
  instrument?: string | null;
  funding_rate?: string | null;
  index_price?: string | null;
  mark_price?: string | null;
  open_interest?: string | null;
  predicted_funding_rate?: string | null;
  premium?: string | null;
}

/** Venue-published candle update from /historical/ohlcv. */
export interface OhlcvRow {
  event_id: string; source: string; market: string; collector_timestamp: number;
  source_capture_id: string; schema_version: number; interval: string;
  open_timestamp: number; open: number; high: number; low: number; close: number;
  exchange_timestamp?: number | null; instrument?: string | null;
  close_timestamp?: number | null; base_volume?: number | null;
  quote_volume?: number | null; trade_count?: number | null; is_closed?: boolean | null;
}

/** Pair-shaped intent observation from /historical/intents. */
export interface IntentRow {
  event_id: string; source: string; market: string; collector_timestamp: number;
  source_capture_id: string; schema_version: number;
  exchange_timestamp?: number | null; instrument?: string | null;
  amount_kind?: string | null; expires_at?: number | null;
  input_amount?: string | null; input_asset_id?: string | null;
  input_chain_id?: string | null; intent_id?: string | null;
  output_amount?: string | null; output_asset_id?: string | null;
  output_chain_id?: string | null; quote_id?: string | null;
  quoted_input_amount?: string | null; quoted_output_amount?: string | null;
  rfq_id?: string | null; settled_at?: number | null; status?: string | null;
}

/** Individual PropAMM quote point from /historical/quotes. */
export interface QuoteRow {
  event_id: string; source: string; market: string; instrument: string;
  collector_timestamp: number; source_capture_id: string; schema_version: number;
  observation_id: string; input_asset_id: string; input_chain_id: string;
  input_amount: string; input_decimals: number; output_asset_id: string;
  output_chain_id: string; output_amount: string; output_decimals: number;
  amount_kind: string; block_number: number; block_hash: string;
  transaction_hash: string; transaction_index: number; router: string;
  exchange_timestamp?: number | null; oracle?: string | null; pool?: string | null;
}

/** Exact raw capture from GET /raw/{exchange}/{event}. */
export interface RawCaptureRow {
  capture_id: string;
  collector_timestamp: number;
  recorder_version: string;
  ingested_at: number;
  additional_context: unknown;
  /** Exact upstream JSON text, intentionally unparsed. */
  original_json: string;
}

export interface RawChannelOptions {
  exchange: string;
  event: string;
  start: number;
  end: number;
}

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

type L2LevelIndex = "00" | "01" | "02" | "03" | "04" | "05" | "06" | "07" | "08" | "09"
  | "10" | "11" | "12" | "13" | "14" | "15" | "16" | "17" | "18" | "19"
  | "20" | "21" | "22" | "23" | "24";
type L2LevelField = `${"bid" | "ask"}_${"px" | "sz"}_${L2LevelIndex}`;

/** Flat top-25 source update or reconstructed book from a historical L2 route. */
export type OrderbookL2Row = {
  event_id: string;
  source: string;
  market: string;
  instrument: string | null;
  collector_timestamp: number;
  exchange_timestamp: number | null;
  source_capture_id: string;
  schema_version: number;
  source_event_is_snapshot: boolean;
} & Record<L2LevelField, number | null>;

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

/** Filters for direct historical rows. Bounds are inclusive Unix milliseconds. */
export interface HistoricalRowsOptions {
  source?: string;
  market?: string;
  start?: number;
  end?: number;
}

export interface OptionTickerRowsOptions extends HistoricalRowsOptions {
  instrument?: string;
}

export interface OhlcvRowsOptions extends HistoricalRowsOptions {
  instrument?: string;
  interval?: string;
}

export interface IntentRowsOptions extends HistoricalRowsOptions {
  instrument?: string;
  intentId?: string;
}

export interface QuoteRowsOptions extends HistoricalRowsOptions {
  instrument?: string;
  observationId?: string;
}

/** Optional filters for flat source snapshots and deltas. */
export interface L2UpdatesOptions extends HistoricalRowsOptions {
  instrument?: string;
}

/** Required inclusive window of at most five minutes for reconstructed books. */
export interface L2OrderbooksOptions extends L2UpdatesOptions {
  source: string;
  market: string;
  start: number;
  end: number;
}

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
