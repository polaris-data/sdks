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
| `trades` | `/historical/trades` | Flat trades |
| `optionTickers` | `/historical/option-tickers` | Flat option observations; exact `instrument` filter |
| `fundingRates` | `/historical/funding-rates` | Partial funding observations with nullable fields |
| `perpetualTickers` | `/historical/funding-rates` | Funding-bearing observations |
| `intents` | `/historical/intents` | Single input/output asset observations |
| `l2Updates` | `/historical/l2-updates` | Source snapshots and sparse deltas as flat rows |
| `l2Snapshots` | `/historical/l2-orderbooks` | Reconstructed top-25 books after each update |
| `ohlcv`, `ohlcvTradingView` | `/historical/ohlcv` | Bars from venue candle updates |
| `rawChannel` | `/raw/{exchange}/{event}` | Exact venue-native capture text |

`catalog`, `count`, `instruments`, `health`, and realtime `stream` are also available. `l2Snapshots` requires `source`, `market`, `start`, and `end`; it is unrelated to the removed `/snapshots` route. `OrderbookBuilder` still handles realtime orderbook event envelopes.

## Breaking changes

`events`, `replay`, `listSnapshots`, `getSnapshotDownloadUrls`, and `propammQuoteLadders` are removed. Use direct historical methods and `rawChannel` for exact raw captures. Full standardized event replay and PropAMM quote methods have no replacement yet. The client no longer requests `/snapshots` or `/download`.

The client also removes `bbo`, `depthMetrics`, `intentRows`, `ohlcvRows`, `quoteRows`, `volume`, `vwap`, `volatility`, and `markPrices`. These methods may return in a later release.

The `datasetRoot`, `storage`, and `snapshotDownloadConcurrency` constructor options are removed with local snapshot storage.

## Development

```bash
npm run typecheck
npm test
```
