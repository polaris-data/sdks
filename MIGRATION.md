# Migrate to the Zig-backed SDKs

Rust 0.15, Python 0.18, and TypeScript 0.10 use the current production data and WebSocket APIs. Zig starts at version 0.1. Package names remain `polaris-data`; Python imports it as `polaris_data`.

| Previous method or feature | Production API |
| --- | --- |
| `meta` | `openapi` for the API contract, or `health` for availability |
| `catalog`, `count`, `instruments` | `coverage` for table and column discovery. Query published metadata tables via `data` or `sql`. Coverage counts tables and reports physical row counts where available. |
| `trades`, `funding_rates`, `option_tickers`, `intents` and their camel-case variants | `data` or `data_rows` on the corresponding published table, for example `standard.trades`. Use `coverage` for column names and `sql` for aggregation. |
| `raw`, `raw_channel` | `data` on an exact source-owned table, for example `hyperliquid.raw__trades`. |
| `events`, `l2_updates`, `l2_snapshots`, `perpetual_tickers`, `ohlcv` | Discover the relevant published table in `coverage`, then query it with `data` or `sql`. For execution-price candle windows on published DEX pools, use `candles`. |
| `ohlcvTradingView` | Aggregate stored data with `sql` or use the standalone `OhlcvAggregator` on local trades. |
| Python iterator, Arrow, and DataFrame outputs on historical methods | Use `data_rows` for a lazy iterator, or pass a page’s row dictionaries to Pandas or PyArrow. |
| `stream` with source, markets, and standardized events | Pass exact table subscriptions and consume complete table pages. Persist each subscription’s cursor after processing the page. |

Table reads return stored rows with source-specific schemas and precise string values. They require an API key even for recent history. Anonymous recent-history analytics are available through `sql`. Treat coverage bounds as observations and usage eligibility as an API response. DEX candles use exclusive upper bounds. Consult the [Polaris API reference](https://api.polaris.supply/openapi.json) for query limits and published routes.

The existing local `OrderbookBuilder` helpers accept legacy event envelopes. Realtime table pages contain stored rows, so adapt suitable rows to the helper's input shape when reconstructing a book.
