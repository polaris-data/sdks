import { ownHelper, type Core } from "./core";
import type { OrderbookData, StandardEvent } from "./types";

/** Reconstruct complete books from standardized snapshot and delta events. */
export class OrderbookBuilder {
  private readonly native: ReturnType<Core["orderbooks"]>;

  constructor() {
    this.native = ownHelper(this, (core) => core.orderbooks());
  }

  clear(): void {
    this.native.clear();
  }

  clearBook(source: string, market: string): void {
    this.native.clearBook(source, market);
  }

  apply(event: StandardEvent): StandardEvent | undefined {
    const result = this.native.update(bookInput(event));
    if (result === "ignored") return event;
    if (result === "suppressed") return undefined;

    const data = isObject(event.data) ? event.data : {};
    const { bids: _topLevelBids, asks: _topLevelAsks, ...envelope } = event;
    return {
      ...envelope,
      type: "orderbook",
      data: { ...data, ...this.snapshot(event.source, event.market)! },
    };
  }

  /** Update book state without constructing a complete orderbook. */
  update(event: StandardEvent): boolean {
    return this.native.update(bookInput(event)) === "applied";
  }

  /** Materialize the current complete book for a source and market. */
  snapshot(source: string, market: string): OrderbookData | undefined {
    return this.native.snapshot(source, market) ?? undefined;
  }
}

function bookInput(event: StandardEvent) {
  const data = isObject(event.data) ? { ...event.data } : undefined;
  return {
    operation: "update",
    kind: event.type,
    source: event.source,
    market: event.market,
    v2: "collector_timestamp" in event,
    data: data && {
      is_snapshot: typeof event.data.is_snapshot === "boolean" ? event.data.is_snapshot : undefined,
      bids: bridgeSide(data.bids),
      asks: bridgeSide(data.asks),
    },
    bids: !(data && "bids" in data) && Array.isArray(event.bids) ? bridgeSide(event.bids) : undefined,
    asks: !(data && "asks" in data) && Array.isArray(event.asks) ? bridgeSide(event.asks) : undefined,
  };
}

// Preserve JavaScript number coercion for level fields and keep arbitrary
// event metadata, including bigint fields, on the JavaScript side.
function bridgeSide(side: unknown) {
  if (!Array.isArray(side)) return side === undefined ? undefined : null;
  return side.map((row) => {
    if (Array.isArray(row)) return [bridgeNumber(row[0]), bridgeNumber(row[1])];
    if (isObject(row)) return [bridgeNumber(row.price), bridgeNumber(row.quantity ?? row.size ?? row.amount)];
    return null;
  });
}

function bridgeNumber(value: unknown): number | null {
  const number = typeof value === "number" ? value :
    typeof value === "string" && value.trim() ? Number(value) : NaN;
  return Number.isFinite(number) ? number : null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
