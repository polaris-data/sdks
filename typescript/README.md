# polaris-data

TypeScript SDK for the Polaris market data API, optimised for server-side workflows, trading scripts, and TypeScript projects.

Documentation can be found at [polaris.supply/docs](https://polaris.supply/docs).

## Install

```sh
npm install polaris-data
```

```sh
pnpm add polaris-data
```

```sh
yarn add polaris-data
```

```sh
bun add polaris-data
```

## Quickstart

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

try {
  const rows = await client.events({
    source: "binance",
    market: "BTC-USDT",
    from: "2024-01-01T00:00:00Z",
    to: "2024-01-01T01:00:00Z",
  });
  console.log(`Fetched ${rows.length} events`);
} finally {
  client.close();
}
```

The `apiKey` is optional — omit it to use public endpoints, or set the `POLARIS_API_KEY` environment variable.

### Async disposal (Node ≥ 18 / TypeScript ≥ 5.2)

```ts
import { PolarisClient } from "polaris-data";

await using client = new PolarisClient({ apiKey: "polaris_key_your_key" });

const rows = await client.events({
  source: "binance",
  market: "BTC-USDT",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});
console.log(`Fetched ${rows.length} events`);
```

## `PolarisClient`

```ts
new PolarisClient({
  apiKey?: string,       // optional — omit for public access, or set POLARIS_API_KEY
  baseUrl?: string,       // defaults to "https://api.polaris.supply"
  streamUrl?: string,     // defaults to wss://<API origin>/stream
  timeout?: number,       // request timeout in ms (default 30 000)
  fetch?: FetchLike,      // custom fetch for testing / proxies
  datasetRoot?: string,   // override local dataset root
});
```

Use it to inspect available data, query historical market data, and open realtime streams.

### Discovery

| Method | Returns | Use case |
| --- | --- | --- |
| `health()` | API health/status payload | Connectivity checks and startup validation |
| `catalog(opts?)` | Source/market metadata | Discover supported datasets, markets, and time coverage |
| `listSnapshots(opts)` | List of snapshot file entries | Inspect snapshot availability before downloading or replaying |

### Access patterns

| Method | Returns | Use case |
| --- | --- | --- |
| `replay(opts)` | Async iterator of historical events | Backfills and replay-style processing without materializing everything up front |
| `stream(opts)` | Closeable async iterable of realtime events | Open-ended normalized market data for one source and up to 1,000 markets |
| `getSnapshotDownloadUrls(opts)` | Daily bulk manifest with pre-signed snapshot URLs | Bulk historical downloads for a single `source` / `market` / UTC `date` |

### Standardized Data Schemas

| Method | Returns | Use case |
| --- | --- | --- |
| `events(opts)` | Array of standardised historical events | General-purpose historical analysis when you want the normalized event stream in memory |
| `trades(opts)` | Array of flat `TradeRow` values | Direct trade queries and execution studies |
| `intents(opts)` | Array of flat `IntentRow` values | Pair-shaped RFQ and intent observations |
| `optionTickers(opts)` | Array of flat `OptionTickerRow` values | Read an underlying's whole option chain or one exact contract with `instrument` |
| `perpetualTickers(opts)` | Array of flat `FundingRateRow` values | Funding-bearing perpetual ticker observations |
| `l2Snapshots(opts)` | Array of standardised orderbook snapshot rows | Order book reconstruction and microstructure analysis |
| `l2Updates(opts)` | Array of raw orderbook snapshots and deltas | High-throughput application-managed books |
| `fundingRates(opts)` | Array of flat `FundingRateRow` values | Partial funding observations |
| `ohlcvRows(opts)` | Array of flat `OhlcvRow` values | Every venue-published candle update |
| `intentRows(opts)` | Array of flat `IntentRow` values | Pair-shaped intent observations |
| `quoteRows(opts)` | Array of flat `QuoteRow` values | Individual PropAMM quote points |
| `rawChannel({ exchange, event, start, end })` | Array of `RawCaptureRow` values | Query exact captures from one venue-native channel |
| `markPrices(opts)` | Array of flat `FundingRateRow` values | Mark prices published with funding observations |
| `propammQuoteLadders(opts)` | Array of typed PropAMM quote-ladder events | Full-precision Ethereum execution-quote analysis |
| `ohlcv(opts)` | Aggregated OHLCV bars | Charting, bar-based strategies, and downstream TA workflows |
| `ohlcvTradingView(opts)` | TradingView-shaped OHLCV payload | Feeding TradingView-compatible chart consumers directly |
| `volume(opts)` | Venue candle base-volume series | Volume profiling and participation analysis |
| `vwap(opts)` | Bucketed VWAP series | Execution benchmarking and price smoothing |
| `volatility(opts)` | Candle-close realized volatility estimates | Risk modeling and intraperiod volatility analysis |
| `bbo(opts)` | Best bid/offer quote series | Spread tracking, quote analytics, and top-of-book monitoring |
| `depthMetrics(opts)` | Derived depth, spread, imbalance, and slippage metrics | Liquidity analysis and market impact estimation |

Snapshot-based methods accept `from` and `to` as ISO 8601 strings, `Date`, or epoch milliseconds. If one or both bounds are omitted, the client infers a bounded range from catalog metadata.
`replay({ standard: false })` is not supported in the TypeScript SDK.

`intents()` returns pair-shaped flat observations from `/historical/intents`:

```ts
for (const source of ["uniswapx", "lifi", "cowswap"]) {
  const observations = await client.intents({ source, market: "intents" });
  for (const row of observations) {
    console.log(source, row.rfq_id, row.intent_id, row.status);
  }
}
```

When present, `observation.raw` contains the exact captured upstream JSON next
to the canonical `observation.data` payload.

### Realtime stream

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });
const events = client.stream({
  source: "binance",
  markets: ["BTC-USDT", "ETH-USDT"],
});

try {
  for await (const event of events) {
    console.log(event);
  }
} finally {
  events.close();
  client.close();
}
```

For option sources, pass the normalized underlying in `markets` and optionally
set `instrument` to one non-empty exact option contract. Omitting `instrument`
subscribes to the complete option chain. Historical option tickers use the same
identity split:

```ts
const chain = await client.optionTickers({ source: "deribit", market: "BTC" });
const contract = await client.optionTickers({
  source: "deribit",
  market: "BTC",
  instrument: "BTC-29MAR24-50000-C",
});

const perpetuals = await client.perpetualTickers({
  source: "hyperliquid",
  market: "BTC",
});
console.log(perpetuals[0]?.data.mark_price, perpetuals[0]?.data.funding_rate);
```

Perpetual ticker decimal values remain strings, and each row is a partial
venue update rather than an accumulated snapshot. Trade data exposes optional
`maker` and `taker` identifiers when supplied by the venue.

The browser build uses the native browser WebSocket implementation; Node uses
the bundled Node transport. Streams reconnect automatically after transport
failures. Because the live protocol has no resume cursor, reconnects can contain
gaps or duplicate events. Reconstructed books are cleared on reconnect, and
deltas are suppressed until a new snapshot arrives. Authentication and protocol
errors are terminal.

Standardized orderbooks are materialized by default for `stream`, `replay`,
`events`, and `l2Snapshots`. Snapshots replace the book, deltas update listed
prices, and zero quantities delete prices. Use `l2Updates` to receive raw
snapshots and deltas. `OrderbookBuilder` exposes the same state machine for
application-managed event flows. Its `update` method mutates state without
constructing a complete book; call `snapshot` only when sorted levels are
needed. The existing `apply` method retains its combined behavior.

## Examples

### Catalog

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

const catalog = await client.catalog();
console.log(catalog);

const markets = await client.catalog({ source: "hyperliquid" });
console.log(markets.markets.map((m) => m.market));
```

### Events from local snapshots and direct trades

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

// First call downloads the required standardized snapshot files; subsequent calls read locally
const rows = await client.events({
  source: "binance",
  market: "BTC-USDT",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});
console.log(rows.length);

const trades = await client.trades({
  source: "binance",
  market: "BTC-USDT",
  start: Date.parse("2024-01-01T00:00:00Z"),
  end: Date.parse("2024-01-01T01:00:00Z"),
});
console.log(trades[0]?.price);

const quotes = await client.bbo({
  source: "binance",
  market: "BTC-USDT",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});
console.log(quotes[0]);

const depth = await client.depthMetrics({
  source: "binance",
  market: "BTC-USDT",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});
console.log(depth[0]);
```

### Funding observations and mark prices

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

const funding = await client.fundingRates({
  source: "hyperliquid",
  market: "BTC",
  start: Date.parse("2024-01-01T00:00:00Z"),
  end: Date.parse("2024-01-02T00:00:00Z"),
});

const marks = await client.markPrices({
  source: "hyperliquid",
  market: "BTC",
  start: Date.parse("2024-01-01T00:00:00Z"),
  end: Date.parse("2024-01-02T00:00:00Z"),
});

console.log(funding.length, marks.length);
```

### PropAMM quote ladders

```ts
const ladders = await client.propammQuoteLadders({
  source: "metric",
  market: "ethereum",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});

console.log(ladders[0].data.values.quotes);
```

Quote amounts remain decimal strings so the full Ethereum `uint256` range is
preserved. `oracle` is nullable and Metric records include the optional `pool`
address.

### Replay (streaming from local snapshots)

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

let count = 0;
for await (const row of client.replay({
  source: "binance",
  market: "BTC-USDT",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
})) {
  count++;
}
console.log(`Replayed ${count} rows`);
```

### OHLCV (aggregated from local snapshots)

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

// Array of bars
const bars = await client.ohlcv({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
  interval: "1m",
});

// TradingView format
const tv = await client.ohlcvTradingView({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
  interval: "1m",
});
```

Supported intervals: `100ms`, `1s`, `10s`, `1m`, `5m`, `15m`, `1h`.

### Volume, VWAP, and volatility

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

const volume = await client.volume({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
  interval: "1m",
});

const vwap = await client.vwap({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
  interval: "1m",
});

const volatility = await client.volatility({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
  interval: "1m",
});

console.log(volume[0], vwap[0], volatility[0]);
```

### Snapshots

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

// List available snapshots
const snapshots = await client.listSnapshots({
  source: "hyperliquid",
  market: "BTC-USD",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-07T00:00:00Z",
});
for (const s of snapshots) {
  console.log(s.date, s.hour, s.key);
}

// Get the daily bulk manifest
const manifest = await client.getSnapshotDownloadUrls({
  source: "hyperliquid",
  market: "BTC-USD",
  date: "2024-01-01",
});

for (const snapshot of manifest.snapshots) {
  console.log(snapshot.timestamp, snapshot.url);
}
```

## Error handling

```ts
import {
  PolarisClient,
  PolarisError,
  RateLimitedError,
  UnauthorizedError,
} from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

try {
  await client.events({
    source: "binance",
    market: "BTC-USDT",
    from: "2024-01-01T00:00:00Z",
    to: "2024-01-01T01:00:00Z",
  });
} catch (err) {
  if (err instanceof UnauthorizedError) {
    console.error("API key is required");
  } else if (err instanceof RateLimitedError) {
    console.error(`Rate limited. Reset at: ${err.resetAt}`);
  } else if (err instanceof PolarisError) {
    console.error(`Polaris error: ${err.message} (status=${err.statusCode})`);
  }
}
```

### Error classes

| Class | HTTP status | When |
| --- | --- | --- |
| `UnauthorizedError` | 401 | Missing or invalid API key |
| `NotFoundError` | 404 | Resource not found |
| `RateLimitedError` | 429 | Too many requests; check `resetAt` |
| `StreamDecodeError` | — | Failed to decode a streamed response |

All errors extend `PolarisError` which extends `Error`.

## Types

The SDK ships with full TypeScript definitions. Key types:

```ts
import type {
  // Events
  StandardEvent,
  TradeEvent,
  TradeData,

  // OHLCV
  OhlcvBar,
  OhlcvInterval,

  // Snapshots
  SnapshotEntry,
  SnapshotsResponse,

  // Catalog
  CatalogResponse,
  CatalogMarket,
} from "polaris-data";
```

## Snapshot-first architecture

`trades`, `optionTickers`, and `fundingRates` use their direct `/historical/*` routes. Their filters are optional `source` and `market`, optional `instrument` for option tickers, and inclusive Unix-millisecond `start` and `end`. They follow all cursor pages and return flat API rows instead of event envelopes. Omitted bounds use the API defaults; older history requires an API key. These three methods do not use the local snapshot cache or provide gap guarantees. This is a breaking change from the former `from`/`to` inputs and nested `data` results.

`rawChannel({ exchange, event, start, end })` uses `/raw/{exchange}/{event}` with required inclusive Unix-millisecond bounds. It follows all cursor pages and returns exact capture metadata plus an unparsed `original_json` string. The latest seven days are public; older ranges require an API key.

`ohlcvRows`, `intentRows`, and `quoteRows` use their matching `/historical/*` routes. They accept optional `source`, `market`, and `instrument` filters, inclusive Unix-millisecond `start` and `end`, plus `interval`, `intentId`, or `observationId` respectively. All cursor pages are returned. `ohlcvRows` includes open-candle revisions and filters on candle open time; `intentRows` contains pair-shaped observations; `quoteRows` returns individual points. `intents` now has the same pair-shaped scope as `intentRows`; `propammQuoteLadders` remains snapshot-backed.

`perpetualTickers` returns flat funding-bearing observations from `/historical/funding-rates`. `markPrices` returns only rows with a present `mark_price`. Both accept optional `source` and `market` and inclusive Unix-millisecond `start` and `end`; their previous `from` and `to` inputs and event-envelope results have changed. Use `events` or `replay` for the original event shapes.

`ohlcv`, `ohlcvTradingView`, `volume`, `vwap`, and `volatility` now use `/historical/ohlcv`. Each candle's latest collector revision is selected. OHLCV and volume use the requested exact venue interval and base volume; missing volume or trade count becomes zero. VWAP uses quote volume divided by base volume and falls back to close times base volume when quote volume is absent. Volatility is the sample standard deviation of log returns from the finest available candle closes shorter than the requested interval; buckets with fewer than two returns are omitted. These are candle-derived results, not trade-derived results. `from` and `to` are applied as inclusive candle-open bounds, and the aggregate methods no longer perform snapshot coverage checks.

The remaining standardised historical methods (`events`, `l2Snapshots`, `l2Updates`, `propammQuoteLadders`, `bbo`, `depthMetrics`, and `replay`) use a **snapshot-first** approach:

1. Hourly `.jsonl.zst` snapshot files are discovered via `GET /snapshots` and downloaded via `GET /download` on first access.
2. Subsequent calls for the same date range read from the local cache — no network round-trips.
3. If a requested hour has no available snapshot, the SDK raises a `PolarisError` rather than silently falling back.

### Local dataset root

The default root follows the platform convention so the CLI and SDK share the same files:

| Platform | Default root |
| --- | --- |
| macOS | `~/Library/Application Support/polaris` |
| Linux | `$XDG_DATA_HOME/polaris` or `~/.local/share/polaris` |
| Windows | `%APPDATA%\polaris` |

Inside the root:

```text
<root>/
  data/       # Rust-style snapshots: <tier>/<source>/<market>/<date>/<opaque-key>.jsonl.zst
  tmp/        # Temporary download parts
  cache/
```

Override with the `datasetRoot` constructor option or the `POLARIS_ROOT` environment variable.

## Runtime dependencies

- **`fzstd`** — Pure JavaScript zstd decompression for `.jsonl.zst` snapshot files.

No other runtime dependencies. HTTP is handled by the native `fetch` API (Node.js 18+, Deno, Bun).

## License

MIT
