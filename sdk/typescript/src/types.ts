export type TimeInput = string | Date | number;

/** A decoded JSON value. Large integer tokens may be returned as bigint. */
export type Json = null | boolean | number | bigint | string | Json[] | { [key: string]: Json };

/** Shape of the global `fetch` function so consumers can inject a custom one. */
export type FetchLike = typeof globalThis.fetch;

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
}

export interface StreamOptions {
  subscriptions: ReadonlyArray<import("./generated").SubscriptionRequest>;
}
