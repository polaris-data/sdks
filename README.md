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
| `trades` | `trades` | `/trades` | Flat trade rows |
| `events` | `events` | `/events` | Authenticated mixed `{type, data}` flat rows in collector-time order |
| `option_tickers` | `optionTickers` | `/options-ticker` | Flat option observations; optional exact `instrument` |
| `funding_rates` | `fundingRates` | `/funding-rates` | Flat partial funding observations with nullable fields |
| `perpetual_tickers` | `perpetualTickers` | `/perpetual-ticker` | Funding-bearing observations; not a complete ticker stream |
| `intents` | `intents` | `/intents` | Single input/output asset observations |
| `l2_updates` | `l2Updates` | `/l2-updates` | Flat source snapshots and sparse deltas |
| `l2_snapshots` | `l2Snapshots` | `/l2-orderbooks` | Reconstructed, sorted top-25 books after each update |
| `ohlcv` | `ohlcv` | `/ohlcv` | Every venue-published candle update as a flat row |
| `raw` | `raw` | `/raw` | Paged raw captures for a source; optional exact `market` and `channel` |
| `raw_channel` | `rawChannel` | `/raw` with `channel` | Exact captures from one native channel; optional `market` |

Python returns iterators by default and supports Arrow batches or Pandas DataFrames on methods with an `output` option. Rust uses async streams and also offers a blocking facade. TypeScript returns arrays. `catalog`, `count`, `instruments`, `health`, and realtime `stream` remain available. All three SDKs offer `raw` for a source with optional `market` and `channel` filters, plus `raw_channel` / `rawChannel` as a channel-specific convenience. Both use the paged JSON `/raw` route. Rust and Python infer a recent seven-day window when `raw` bounds are omitted, leaving a one-minute margin inside the public cutoff for anonymous requests; TypeScript requires inclusive millisecond `start` and `end`. The SDKs send RFC 3339 time query parameters. The `market` filter matches the capture's recorded `additional_context.market` exactly; it does not search `original_json`. Raw responses include `raw_table`. Recent raw history is public, while older ranges may require an API key.

`ohlcv` uses optional `source`, `market`, `instrument`, `interval`, `start`, and `end` filters. Its inclusive bounds refer to candle open time. It preserves every published revision, so multiple rows can share an `open_timestamp`. TypeScript's `ohlcvTradingView` remains a derived latest-revision view.

`l2_snapshots` refers to reconstructed L2 books, not the removed `/snapshots` API. Its `source`, `market`, `start`, and `end` inputs are required. The book rows contain at most 25 levels per side. `OrderbookBuilder` still accepts realtime event envelopes.

## Breaking changes

The SDKs no longer expose `replay`, `list_snapshots` / `listSnapshots`, or the snapshot-backed `propamm_quote_ladders` / `propammQuoteLadders`. TypeScript also removes `getSnapshotDownloadUrls`. Rust and Python remove their cross-channel `raw_replay` variants; Python's `replay(standard=False)` disappears with `replay`. The channel-specific `raw_channel` / `rawChannel` method remains.

Use the matching direct data method for flat rows and `raw` or `raw_channel` / `rawChannel` for exact raw captures. The new `events` method requires an API key and inclusive `start` and `end` collector timestamps in Unix milliseconds. It returns mixed flat rows with typed `data`, not the former snapshot event envelopes. The API also has `/quotes`, which these SDKs do not yet wrap. These SDKs no longer call `/snapshots` or `/download`.

The API moved data routes from `/historical/*` to top-level paths. The old routes no longer work. It also replaced `/raw/{exchange}/{event}` with `/raw?source=...&channel=...`; raw responses now include `raw_table`, and the server supports paged JSON rather than file export.

Rust `RawQuery.market` is now optional and `RawQuery.channel` is new; `RawChannelQuery.market` is also optional. Python `raw(market=...)` is optional and accepts `channel=...`. TypeScript adds `raw()` with required inclusive millisecond bounds. Existing channel-specific methods also accept an optional exact market filter.

The clients also remove `bbo`, Rust `bbo_changes`, `depth_metrics` / `depthMetrics`, `intent_rows` / `intentRows`, `ohlcv_rows` / `ohlcvRows`, `quote_rows` / `quoteRows`, `volume`, `vwap`, `volatility`, and `mark_prices` / `markPrices`. These methods may return in a later release.

The snapshot-backed methods' `allow_gaps` and local replay cache options are removed. TypeScript also removes `datasetRoot`, `storage`, and `snapshotDownloadConcurrency` constructor options because historical requests no longer use local snapshot storage. Python removes `replay_cache_enabled` and `replay_cache_dir` constructor options.

`ohlcv` now returns flat `OhlcvRow` values, with inclusive Unix-millisecond `start` and `end` inputs. Rust's former `OhlcvQuery` and Python's `from_`, `to`, `format`, and `allow_gaps` inputs no longer apply to the method. Python uses `output="iterator"`, `"batches"`, or `"dataframe"`; TypeScript accepts the direct row filters. Existing code expecting collapsed bars must handle candle revisions explicitly.

## Tests

```bash
cargo test -p polaris-data
uv run --with maturin maturin develop --manifest-path crates/polaris-python/Cargo.toml
uv run pytest
cd typescript && npm test
```
