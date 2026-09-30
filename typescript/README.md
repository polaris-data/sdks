# polaris-data

TypeScript client for the Polaris market data API in Node.js and browsers.

```bash
npm install polaris-data
```

## Quickstart

```ts
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });
try {
  const trades = await client.trades({ source: "binance", market: "BTC-USDT" });
  console.log(trades[0]);
} finally {
  client.close();
}
```

Direct historical methods return arrays and follow every cursor page. `start` and `end` are inclusive Unix milliseconds. Optional bounds use the API default window when the route allows it.

| Method | Route | Result |
| --- | --- | --- |
| `trades` | `/trades` | Flat trades |
| `optionTickers` | `/options-ticker` | Flat option observations; exact `instrument` filter |
| `fundingRates` | `/funding-rates` | Partial funding observations with nullable fields |
| `perpetualTickers` | `/perpetual-ticker` | Funding-bearing observations |
| `intents` | `/intents` | Single input/output asset observations |
| `l2Updates` | `/l2-updates` | Source snapshots and sparse deltas as flat rows |
| `events` | `/events` | Authenticated mixed `{type, data}` flat rows in collector-time order |
| `l2Snapshots` | `/l2-orderbooks` | Reconstructed top-25 books after each update |
| `ohlcv` | `/ohlcv` | Every venue-published candle update as a flat row |
| `raw` | `/raw` | Exact captures for a source; optional `market` and `channel` |
| `rawChannel` | `/raw` with `channel` | Exact captures from one native channel; optional `market` |

`catalog`, `count`, `instruments`, `health`, and realtime `stream` are also available. `l2Snapshots` requires `source`, `market`, `start`, and `end`; it is unrelated to the removed `/snapshots` route. `raw` requires `source`, `start`, and `end` and accepts optional `market` and `channel`. `rawChannel` maps `exchange` and `event` to `source` and `channel` and also accepts `market`. Both convert inclusive millisecond bounds to RFC 3339 query parameters. The `market` filter matches the capture's recorded `additional_context.market` exactly; it does not search `original_json`. Raw rows include `raw_table`. `OrderbookBuilder` still handles realtime orderbook event envelopes.

`ohlcv` accepts optional `source`, `market`, `instrument`, `interval`, `start`, and `end` filters. Inclusive bounds refer to candle open time. It preserves every candle revision. `ohlcvTradingView` remains available as a derived latest-revision view.

## Breaking changes

`replay`, `listSnapshots`, `getSnapshotDownloadUrls`, and `propammQuoteLadders` are removed. Use direct data methods and `raw` or `rawChannel` for exact raw captures. The new `events` method requires an API key and inclusive `start` and `end` collector timestamps in Unix milliseconds; it returns mixed flat rows instead of the former snapshot event envelopes. The API also has `/quotes`, which the client does not yet wrap. The client no longer requests `/snapshots` or `/download`.

The API moved data routes from `/historical/*` to top-level paths. The old routes no longer work. It also replaced `/raw/{exchange}/{event}` with `/raw?source=...&channel=...`.

The client also removes `bbo`, `depthMetrics`, `intentRows`, `ohlcvRows`, `quoteRows`, `volume`, `vwap`, `volatility`, and `markPrices`. These methods may return in a later release.

`ohlcv` now returns flat `OhlcvRow[]` instead of collapsed bars and accepts inclusive Unix-millisecond `start` and `end` bounds. Existing code expecting one bar per candle must handle revisions explicitly.

The `datasetRoot`, `storage`, and `snapshotDownloadConcurrency` constructor options are removed with local snapshot storage.

## Development

```bash
npm run typecheck
npm test
```
