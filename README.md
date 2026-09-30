# Polaris SDKs

Rust, Python, and TypeScript clients for the Polaris API. Rust and Python share the Rust core; TypeScript supports Node.js and browsers. All three packages are named `polaris-data`, and the Python import is `polaris_data`.

## Install

```bash
pip install polaris-data
cargo add polaris-data
npm install polaris-data
```

For Python Arrow batches or Pandas DataFrames, install `polaris-data[arrow]` or `polaris-data[dataframe]`.

## Quickstart

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    for trade in client.trades(source="binance", market="BTC-USDT"):
        print(trade["price"], trade["quantity"])
```

```typescript
import { PolarisClient } from "polaris-data";

const client = new PolarisClient({ apiKey: "polaris_key_your_key" });
const trades = await client.trades({ source: "binance", market: "BTC-USDT" });
console.log(trades[0]?.price);
client.close();
```

```rust
use futures_util::StreamExt;
use polaris_data::{HistoricalRowsQuery, PolarisClient};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = PolarisClient::builder().build()?;
let mut trades = client.trades(HistoricalRowsQuery {
    source: Some("binance".into()),
    market: Some("BTC-USDT".into()),
    ..Default::default()
}).await?;
while let Some(row) = trades.next().await {
    println!("{}", row?.price);
}
# Ok(())
# }
```

## Historical methods

All direct historical methods fetch every cursor page. Their `start` and `end` filters are inclusive Unix milliseconds; omitted bounds use the API default window where the route permits it. Older ranges may require an API key.

| Rust / Python | TypeScript | API route | Result |
| --- | --- | --- | --- |
| `trades` | `trades` | `/historical/trades` | Flat trade rows |
| `option_tickers` | `optionTickers` | `/historical/option-tickers` | Flat option observations; optional exact `instrument` |
| `funding_rates` | `fundingRates` | `/historical/funding-rates` | Flat partial funding observations with nullable fields |
| `perpetual_tickers` | `perpetualTickers` | `/historical/funding-rates` | Funding-bearing observations; not a complete ticker stream |
| `mark_prices` | `markPrices` | `/historical/funding-rates` | Funding observations with a present `mark_price`; not a complete mark stream |
| `ohlcv_rows` | `ohlcvRows` | `/historical/ohlcv` | Each venue candle update, including revisions |
| `intent_rows`, `intents` | `intentRows`, `intents` | `/historical/intents` | Single input/output asset observations |
| `quote_rows` | `quoteRows` | `/historical/quotes` | Individual quote observations |
| `l2_updates` | `l2Updates` | `/historical/l2-updates` | Flat source snapshots and sparse deltas |
| `l2_snapshots` | `l2Snapshots` | `/historical/l2-orderbooks` | Reconstructed, sorted top-25 books after each update |
| `bbo`, `bbo_changes` (Rust) | `bbo` | `/historical/l2-orderbooks` | Quotes derived from reconstructed books |
| `depth_metrics` | `depthMetrics` | `/historical/l2-orderbooks` | Metrics from the available top-25 levels |
| `ohlcv`, `volume`, `vwap`, `volatility` | Same names, plus `ohlcvTradingView` | `/historical/ohlcv` | Aggregates from venue candle updates |
| `raw_channel` | `rawChannel` | `/raw/{exchange}/{event}` | Exact venue-native capture text |

Python returns iterators by default and supports Arrow batches or Pandas DataFrames on methods with an `output` option. Rust uses async streams and also offers a blocking facade. TypeScript returns arrays. `catalog`, `count`, `instruments`, `health`, and realtime `stream` remain available. Rust and Python retain the older source/market `raw` method separately from `raw_channel`.

`l2_snapshots` refers to reconstructed L2 books, not the removed `/snapshots` API. Its `source`, `market`, `start`, and `end` inputs are required. The book rows contain at most 25 levels per side, so depth calculations use only those levels. `OrderbookBuilder` still accepts realtime event envelopes.

## Breaking changes

The SDKs no longer expose `events`, `replay`, `list_snapshots` / `listSnapshots`, or the snapshot-backed `propamm_quote_ladders` / `propammQuoteLadders`. TypeScript also removes `getSnapshotDownloadUrls`. Rust and Python remove their cross-channel `raw_replay` variants; Python's `replay(standard=False)` disappears with `replay`. The channel-specific `raw_channel` / `rawChannel` method remains.

Use the matching direct historical method for flat rows, `quote_rows` / `quoteRows` for individual PropAMM quotes, and `raw_channel` / `rawChannel` for exact raw captures. Full standardized event replay and complete PropAMM quote ladders have no replacement method yet. These SDKs no longer call `/snapshots` or `/download`.

The snapshot-backed methods' `allow_gaps` and local replay cache options are removed. TypeScript also removes `datasetRoot`, `storage`, and `snapshotDownloadConcurrency` constructor options because historical requests no longer use local snapshot storage. Python removes `replay_cache_enabled` and `replay_cache_dir` constructor options.

## Tests

```bash
cargo test -p polaris-data
uv run --with maturin maturin develop --manifest-path crates/polaris-python/Cargo.toml
uv run pytest
cd typescript && npm test
```
