import { PolarisError } from "./errors";
import type { OrderbookData, StandardEvent } from "./types";

interface CanonicalLevel {
  [key: string]: unknown;
  price: number;
  quantity: number;
}

interface BookState {
  bids: Map<number, number>;
  asks: Map<number, number>;
}

const SNAPSHOT_TYPES = new Set(["orderbook", "orderbook_snapshot", "l2_snapshot"]);

/** Reconstruct complete books from standardized snapshot and delta events. */
export class OrderbookBuilder {
  private readonly books = new Map<string, BookState>();

  clear(): void {
    this.books.clear();
  }

  clearBook(source: string, market: string): void {
    this.books.delete(bookKey(source, market));
  }

  apply(event: StandardEvent): StandardEvent | undefined {
    const isSnapshot = SNAPSHOT_TYPES.has(event.type);
    const isDelta = event.type === "orderbook_delta";
    if (!isSnapshot && !isDelta) return event;
    if (!this.update(event)) return undefined;

    const data: Record<string, unknown> = isObject(event.data) ? { ...event.data } : {};
    const snapshot = this.snapshot(event.source, event.market)!;
    const { bids: _topLevelBids, asks: _topLevelAsks, ...envelope } = event;
    return {
      ...envelope,
      type: "orderbook",
      data: {
        ...data,
        ...snapshot,
      },
    } as StandardEvent;
  }

  /** Update book state without constructing a complete orderbook. */
  update(event: StandardEvent): boolean {
    const isSnapshot = SNAPSHOT_TYPES.has(event.type);
    const isDelta = event.type === "orderbook_delta";
    if (!isSnapshot && !isDelta) return false;
    if (!isObject(event.data) && !Array.isArray(event.bids) && !Array.isArray(event.asks)) {
      throw new PolarisError("Invalid orderbook payload: data must be an object");
    }

    const data: Record<string, unknown> = isObject(event.data) ? { ...event.data } : {};
    if (!("bids" in data) && Array.isArray(event.bids)) data.bids = event.bids;
    if (!("asks" in data) && Array.isArray(event.asks)) data.asks = event.asks;
    const bids = parseSide(data.bids, isSnapshot, "bids");
    const asks = parseSide(data.asks, isSnapshot, "asks");
    const key = bookKey(event.source, event.market);

    if (isSnapshot) {
      const state: BookState = { bids: new Map(), asks: new Map() };
      applyLevels(state.bids, bids!);
      applyLevels(state.asks, asks!);
      this.books.set(key, state);
    } else {
      const state = this.books.get(key);
      if (!state) return false;
      if (bids) applyLevels(state.bids, bids);
      if (asks) applyLevels(state.asks, asks);
    }

    return true;
  }

  /** Materialize the current complete book for a source and market. */
  snapshot(source: string, market: string): OrderbookData | undefined {
    const state = this.books.get(bookKey(source, market));
    if (!state) return undefined;
    return {
      bids: canonicalLevels(state.bids, "bid"),
      asks: canonicalLevels(state.asks, "ask"),
    };
  }
}

function parseSide(
  value: unknown,
  required: boolean,
  side: "bids" | "asks",
): CanonicalLevel[] | undefined {
  if (value === undefined) {
    if (required) throw new PolarisError(`Invalid orderbook snapshot: data.${side} is required`);
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new PolarisError(`Invalid orderbook payload: data.${side} must be an array`);
  }
  return value.map(parseLevel);
}

function parseLevel(value: unknown): CanonicalLevel {
  let rawPrice: unknown;
  let rawQuantity: unknown;
  if (Array.isArray(value) && value.length >= 2) {
    [rawPrice, rawQuantity] = value;
  } else if (isObject(value)) {
    rawPrice = value.price;
    rawQuantity = value.quantity ?? value.size ?? value.amount;
  }
  const price = toNumber(rawPrice);
  const quantity = toNumber(rawQuantity);
  if (price === undefined || price <= 0 || quantity === undefined || quantity < 0) {
    throw new PolarisError(
      "Invalid orderbook level: price must be positive and quantity non-negative",
    );
  }
  return { price, quantity };
}

function toNumber(value: unknown): number | undefined {
  const result = typeof value === "number" ? value :
    typeof value === "string" && value.trim() ? Number(value) : NaN;
  return Number.isFinite(result) ? result : undefined;
}

function applyLevels(book: Map<number, number>, updates: CanonicalLevel[]): void {
  for (const level of updates) {
    if (level.quantity === 0) book.delete(level.price);
    else book.set(level.price, level.quantity);
  }
}

function canonicalLevels(
  book: Map<number, number>,
  side: "bid" | "ask",
): CanonicalLevel[] {
  return [...book.entries()]
    .sort(([left], [right]) => side === "bid" ? right - left : left - right)
    .map(([price, quantity]) => ({ price, quantity }));
}

function bookKey(source: string, market: string): string {
  return JSON.stringify([source, market]);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
