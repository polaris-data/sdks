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
  timeout?: number,       // request timeout in ms (default 30 000)
  fetch?: FetchLike,      // custom fetch for testing / proxies
  datasetRoot?: string,   // override local dataset root
});
```

Use it to inspect available data and query historical market data.

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
| `raw(opts)` | Throws `PolarisError` in the TypeScript SDK | Reserved for parity with other SDKs; TypeScript historical access remains snapshot-first |
| `getSnapshotDownloadUrls(opts)` | Daily bulk manifest with pre-signed snapshot URLs | Bulk historical downloads for a single `source` / `market` / UTC `date` |

### Standardized Data Schemas

| Method | Returns | Use case |
| --- | --- | --- |
| `events(opts)` | Array of standardised historical events | General-purpose historical analysis when you want the normalized event stream in memory |
| `trades(opts)` | Array of standardised trade events | Trade-level analytics, execution studies, and derived bar calculations |
| `l2Snapshots(opts)` | Array of standardised orderbook snapshot rows | Order book reconstruction and microstructure analysis |
| `fundingRates(opts)` | Array of funding-rate point series rows | Perpetual funding studies and carry modeling |
| `markPrices(opts)` | Array of mark-price point series rows | Basis analysis, mark tracking, and liquidation-related research |
| `ohlcv(opts)` | Aggregated OHLCV bars | Charting, bar-based strategies, and downstream TA workflows |
| `ohlcvTradingView(opts)` | TradingView-shaped OHLCV payload | Feeding TradingView-compatible chart consumers directly |
| `volume(opts)` | Bucketed trade volume series | Volume profiling and participation analysis |
| `vwap(opts)` | Bucketed VWAP series | Execution benchmarking and price smoothing |
| `volatility(opts)` | Bucketed realized volatility series | Risk modeling and intraperiod volatility analysis |
| `bbo(opts)` | Best bid/offer quote series | Spread tracking, quote analytics, and top-of-book monitoring |
| `depthMetrics(opts)` | Derived depth, spread, imbalance, and slippage metrics | Liquidity analysis and market impact estimation |

All snapshot-based methods accept `from` and `to` as ISO 8601 strings, `Date`, or epoch microseconds. If one or both bounds are omitted, the client infers a bounded range from catalog metadata.
`replay({ standard: false })` is not supported in the TypeScript SDK.

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

### Events & trades (from local snapshots)

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
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-01T01:00:00Z",
});
console.log(trades.length);

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

### Point-series schemas (from local snapshots)

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });

const funding = await client.fundingRates({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
});

const marks = await client.markPrices({
  source: "hyperliquid",
  market: "BTC",
  from: "2024-01-01T00:00:00Z",
  to: "2024-01-02T00:00:00Z",
});

console.log(funding.length, marks.length);
```

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
| `DownloadNotAllowedError` | — | Server policy blocks the download |

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
  CatalogSource,
  CatalogMarket,
} from "polaris-data";
```

## Snapshot-first architecture

Standardised historical data (`events`, `trades`, `l2Snapshots`, `fundingRates`, `markPrices`, `bbo`, `depthMetrics`, `ohlcv`, `volume`, `vwap`, `volatility`, and `replay`) uses a **snapshot-first** approach:

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
