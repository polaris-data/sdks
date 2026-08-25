# Polaris SDKs

The official Rust, Python, and TypeScript SDKs for the Polaris API. Rust and
Python share one Rust engine; TypeScript is an independent Node.js and browser
package. All three distributions are named `polaris-data`, with Python
importing as `polaris_data`.

Documentation can be found at https://polaris.supply/docs

## Install

Install the Python SDK from PyPI:

```bash
pip install polaris-data
```

If you use `uv`, install it into a project with:

```bash
uv add polaris-data
```

Or install it into the active environment with:

```bash
uv pip install polaris-data
```

Install optional notebook and Arrow support with:

```bash
pip install "polaris-data[dataframe]"  # Pandas + PyArrow
pip install "polaris-data[arrow]"      # PyArrow batches only
```

Install the Rust SDK from crates.io:

```bash
cargo add polaris-data
```

Install the TypeScript SDK from npm:

```bash
npm install polaris-data
```

Python wheels always include the Rust core. CPython 3.9+ is supported through
PyO3's stable ABI; there is no pure-Python runtime fallback.

## Quickstart

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    row_count = sum(
        1
        for _ in client.replay(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
    )
    print(f"Replayed {row_count} rows")
```

If `api_key` is omitted, the client reads `POLARIS_API_KEY` from the environment.

The equivalent async Rust workflow is:

```rust,no_run
use futures_util::StreamExt;
use polaris_data::{PolarisClient, ReplayQuery};

#[tokio::main]
async fn main() -> Result<(), polaris_data::PolarisError> {
    let client = PolarisClient::builder().build()?;
    let mut rows = client
        .replay(ReplayQuery {
            source: "binance".into(),
            market: "BTC-USDT".into(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T01:00:00Z".into()),
            allow_gaps: false,
            materialize_orderbooks: true,
        })
        .await?;

    while let Some(row) = rows.next().await {
        println!("{:?}", row?);
    }
    Ok(())
}
```

For synchronous Rust applications use
`polaris_data::blocking::PolarisClient`. It owns a Tokio runtime and returns
`PolarisError::BlockingInAsyncRuntime` when called from an active Tokio runtime,
instead of panicking.

## Realtime streams

`stream(...)` opens an unbounded WebSocket feed of the same standardized event
shape returned by `replay(...)`. A stream covers one source and up to 1,000
markets, reconnects automatically after transport failures, and closes when its
iterator is dropped or explicitly closed.

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    with client.stream(source="binance", markets=["BTC-USDT", "ETH-USDT"]) as events:
        for event in events:
            print(event)
```

The equivalent async Rust workflow is:

```rust,no_run
use futures_util::StreamExt;
use polaris_data::{PolarisClient, StreamQuery};

#[tokio::main]
async fn main() -> Result<(), polaris_data::PolarisError> {
    let client = PolarisClient::builder().build()?;
    let mut events = client.stream(StreamQuery {
        source: "binance".into(),
        markets: vec!["BTC-USDT".into(), "ETH-USDT".into()],
        instrument: None,
        include_buffer: false,
        materialize_orderbooks: true,
    }).await?;

    while let Some(event) = events.next().await {
        println!("{:?}", event?);
    }
    Ok(())
}
```

Orderbooks are materialized by default. A standardized `orderbook` event replaces
the complete book; each `orderbook_delta` updates only its listed prices, and a
zero quantity deletes that price. Materialized output is relabeled `orderbook`
and uses sorted `{price, quantity}` levels. Set `materialize_orderbooks=False`
(Python), `materialize_orderbooks: false` (Rust), or
`materializeOrderbooks: false` (TypeScript) to receive raw deltas.

Reconnection is best-effort: the current live protocol has no resume cursor, so
a reconnect can introduce a gap or duplicate event. The SDK clears reconstructed
books on reconnect and suppresses later deltas until a new snapshot arrives.
Protocol and authentication errors are terminal and are not retried.

For option sources, `market` remains the normalized underlying (for example,
`BTC`) and `instrument` is the exact option contract. Omit `instrument` to
subscribe to the entire option chain, or provide a non-empty exact instrument
to narrow every market subscription in the stream.

Use `l2_updates()` in Python and Rust or `l2Updates()` in TypeScript to read the
initial snapshots and sparse deltas without reconstructing every intermediate
book. Reusable `OrderbookBuilder` exports in all three SDKs let applications
materialize those updates when needed:

```python
from polaris_data import OrderbookBuilder

books = OrderbookBuilder()
books.update(snapshot)
books.update(delta)  # False until a snapshot initializes this book
complete = books.snapshot("lighter", "BTC-USD")
books.clear_book("lighter", "BTC-USD")
```

`update()` mutates book state without constructing a full result. Call
`snapshot()` only when you need sorted levels. The existing `apply()` method
remains available as a compatibility shortcut that performs both operations.

## PolarisClient API

`PolarisClient` is the main sync client for the SDK:

```python
PolarisClient(
    api_key=None,
    base_url="https://api.polaris.supply",
    timeout=30.0,
    dataset_root=None,
    stream_url=None,
)
```

Use it to inspect available data, query historical market data, and open realtime streams.

### Discovery

| Method | Returns | Use case |
| --- | --- | --- |
| `health()` | API health/status payload | Connectivity checks and startup validation |
| `catalog(source=None, market=None, q=None)` | Source/market metadata, including normalized instrument fields | Discover supported datasets, markets, instrument metadata, and time coverage |

### Access patterns

| Method | Returns | Use case |
| --- | --- | --- |
| `replay(source=..., market=..., from_=None, to=None, standard=True, allow_gaps=False, parallel=False, materialize_orderbooks=True, output="iterator", batch_size=65536)` | Iterator, exact Arrow batches, or Pandas DataFrame | Backfills and lossless replay-style processing |
| `stream(source=..., markets=[...], instrument=None, include_buffer=False, materialize_orderbooks=True)` | Closeable iterator of realtime events | Open-ended normalized market data with automatic reconnection and optional exact-instrument filtering |
| `raw(source=..., market=..., from_=None, to=None, limit=1000)` | List of raw source payloads | Inspect exchange-native payloads and compare raw vs standardized schemas |

### Standardized Data Schemas

| Method | Returns | Use case |
| --- | --- | --- |
| `events(source=..., market=..., from_=None, to=None, allow_gaps=False, materialize_orderbooks=True, output="iterator", batch_size=65536)` | Iterator, exact Arrow batches, or Pandas DataFrame | General-purpose historical analysis and exact event transport |
| `trades(source=..., market=..., from_=None, to=None, allow_gaps=False, output="iterator", batch_size=65536)` | Iterator, Arrow batches, or Pandas DataFrame | Trade-level analytics, execution studies, and notebook analysis |
| `intents(source=..., market=..., from_=None, to=None, allow_gaps=False)` | Iterator of typed intent events | Process canonical RFQ, quote, and executable-intent observations |
| `option_tickers(source=..., market=..., instrument=None, from_=None, to=None, allow_gaps=False)` | Iterator of typed option ticker events | Read an underlying's whole option chain or filter one exact contract |
| `l2_snapshots(source=..., market=..., from_=None, to=None, allow_gaps=False, materialize_orderbooks=True)` | Iterator of complete orderbook rows | Order book reconstruction and microstructure analysis |
| `l2_updates(source=..., market=..., from_=None, to=None, allow_gaps=False)` | Iterator of raw orderbook snapshots and deltas | High-throughput application-managed books |
| `funding_rates(source=..., market=..., from_=None, to=None, allow_gaps=False, output="iterator", batch_size=65536)` | Iterator, Arrow batches, or Pandas DataFrame | Perpetual funding studies and carry modeling |
| `mark_prices(source=..., market=..., from_=None, to=None, allow_gaps=False, output="iterator", batch_size=65536)` | Iterator, Arrow batches, or Pandas DataFrame | Basis analysis, mark tracking, and liquidation-related research |
| `propamm_quote_ladders(source=..., market=..., from_=None, to=None, allow_gaps=False, output="iterator", batch_size=65536)` | Iterator, exact Arrow batches, or Pandas DataFrame | PropAMM execution-quote analysis with full-precision Ethereum amounts |
| `ohlcv(source=..., market=..., from_=None, to=None, interval=..., format=None, allow_gaps=False)` | Aggregated OHLCV bars | Charting, bar-based strategies, and downstream TA workflows |
| `volume(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)` | Bucketed trade volume series | Volume profiling and participation analysis |
| `vwap(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)` | Bucketed VWAP series | Execution benchmarking and price smoothing |
| `volatility(source=..., market=..., from_=None, to=None, interval=..., method="log_returns", allow_gaps=False)` | Bucketed realized volatility series | Risk modeling and intraperiod volatility analysis |
| `bbo(source=..., market=..., from_=None, to=None, interval=None, allow_gaps=False, changes_only=False, output="iterator", batch_size=65536)` | Iterator, Arrow batches, or Pandas DataFrame | Spread tracking, quote analytics, and top-of-book monitoring |
| `depth_metrics(source=..., market=..., from_=None, to=None, depth_pct=0.01, slippage_notional=10000.0, allow_gaps=False, output="iterator", batch_size=65536)` | Iterator, Arrow batches, or Pandas DataFrame | Liquidity analysis and market impact estimation |

Historical row methods are single-pass iterators. Iterate them directly for bounded memory, or call `list(...)` when you intentionally want an eager result. Setup and coverage errors occur when the method is called; decode errors can occur later while iterating. If you stop early, call the generator's `close()` method to promptly release its native reader. `bbo(interval="1s")` emits the last quote from each non-empty, UTC-aligned interval.

Standardized replay automatically prefetches and decompresses one subsequent
snapshot file on a bounded background worker while preserving file and row
order. The legacy `parallel` argument remains accepted for compatibility; raw
replay still uses its historical 24-hour chunking behavior.

The columnar methods above accept `output="batches"` for a bounded
iterator of `pyarrow.RecordBatch` objects or `output="dataframe"` for an eager
Pandas DataFrame. Typed-series output flattens fields, uses UTC millisecond
timestamps, and dictionary-encodes source, market, and side. Venue-specific
trade and point fields appear as sorted `extra.<name>` columns; discovering
those fields requires one schema pass before batches are emitted.

PropAMM quote ladders retain their complete v2 record envelope. Quote amounts
remain decimal strings so values across the full Ethereum `uint256` range are
lossless; `oracle` may be `None`, and Metric records additionally include
`pool`:

```python
for event in client.propamm_quote_ladders(
    source="metric",
    market="ethereum",
    from_="2024-01-01T00:00:00Z",
    to="2024-01-01T01:00:00Z",
):
    print(event["data"]["values"]["quotes"])
```

Intent recorders expose RFQs, quotes, and executable orders through the same
canonical `IntentEvent` shape. Read each venue with the `intents` market; rows
remain individual observations in storage order and are not lifecycle-reduced:

```python
for source in ("uniswapx", "lifi", "cowswap"):
    for event in client.intents(
        source=source,
        market="intents",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    ):
        data = event["data"]
        print(source, data.get("rfq_id"), data.get("intent_id"), data.get("status"))
```

When the standardized row owns the exact captured upstream payload, it is
available as `event["raw"]` alongside the canonical `event["data"]`.

Exact `events()` and standardized `replay()` batches use UTC microsecond
timestamps and preserve storage order without timestamp sorting. Every row has
stable `replay_ordinal`, `source_file_ordinal`, and `source_row_ordinal` fields,
plus nullable legacy `timestamp` and v2 `collector_timestamp`,
`collector_sequence`, `exchange_timestamp`, and `exchange_sequence` fields.
Common trade, order-book, and point payloads have typed columns, including
nullable `order_id`, `side`, and `is_snapshot`. With raw order-book
updates, `event_json` preserves the complete source event, including unknown
event types and venue-specific fields; with materialization enabled it contains
the resulting complete-book event.
Use `materialize_orderbooks=False` for execution replay so batches contain the
initial snapshot and every ordered delta.

Metadata-framed event schema v2 uses `collector_timestamp` for SDK filtering,
bucketing, and replay timing while retaining nullable venue
`exchange_timestamp` as provenance. Rust exposes this through
`StandardEvent::V2` and the shared `timestamp()` accessor; Python preserves the
v2 dictionary shape; TypeScript exports a structural legacy/v2 union. See the
[schema v2 migration guide](https://docs.polaris.supply/guides/event-schema-v2-migration).

For parameter details, response shapes, and end-to-end examples, see the
[Python SDK docs](https://docs.polaris.supply/sdks/python).

## Benchmarks

### Streaming and memory

Run the opt-in end-to-end benchmark after building the Python extension:

```bash
uv run python benchmarks/streaming_memory.py
```

It generates a 3,000-level local book with one million deltas, consumes raw standardized events, direct BBO, raw L2 updates, and lazy application-managed books in isolated processes, and reports end-to-end wall time, rows per second, and peak RSS. The command fails when peak RSS from 100,000 to one million deltas grows by more than the larger of 20% or 64 MiB, or when long-run throughput falls below 75% of short-run throughput.

Optionally set machine-specific throughput floors:

```bash
uv run python benchmarks/streaming_memory.py \
  --min-rps events=50000 --min-rps bbo=100000 \
  --min-rps l2_updates=500000 --min-rps l2_builder=250000
```

Compare iterator, native RecordBatch, and native DataFrame paths over the
788,383-trade fixture with:

```bash
uv run python benchmarks/columnar.py
```

Compare exact dictionary replay with microsecond Arrow batches across trade,
point, shallow-book, and deep-book fixtures using explicitly page-cache-warm
samples and recorded hardware metadata with:

```bash
uv run python benchmarks/exact_events.py --events 788383 --runs 3
```

Evaluate first-use Arrow IPC construction, disk amplification, and repeated
memory-mapped scans without enabling a persistent SDK cache with:

```bash
uv run python benchmarks/replay_cache_prototype.py --events 788383
```

The standardized IPC cache is deliberately not enabled by the SDK yet. The
prototype reports cache-build cost, compressed-source versus IPC disk size, and
warm scan throughput so a persistent format can be chosen against real venue
datasets. Adoption also requires versioned cache keys, atomic multi-process
construction, corruption recovery, invalidation, and an eviction policy.

The benchmark runs each mode in a separate process and reports elapsed time,
throughput, peak and incremental RSS, row count, and a price checksum. It does
not enforce machine-specific performance thresholds. Its
`list-json-normalize` mode is a worst-case convenience pattern: it first
materializes every nested row dictionary with `list(client.trades(...))`, then
calls `pandas.json_normalize(...)`. The resulting peak RSS includes the Python
row objects and Pandas conversion temporaries, not just the final DataFrame.

Full-book materialization remains available as an explicitly scaled benchmark:

```bash
uv run python benchmarks/streaming_memory.py --modes l2 \
  --short-deltas 100 --long-deltas 1000
```

Absolute throughput floors are intentionally opt-in because results vary by hardware and build profile.

### Local event replay

Use the focused local replay benchmark to compare direct zstd+orjson decoding
with `events()` and `replay()`:

```bash
uv run python benchmarks/local_replay.py \
  --fixture trade --events 788383
```

The benchmark warms the filesystem cache, runs each path in an isolated
process, and reports iterator construction time, time to first event,
steady-state throughput, and peak RSS growth. Pass `--enforce-targets` to require
the SDK to deliver the first event in under 10 ms, process at least 500,000
events/s, and stay within 2× of direct zstd+orjson.

### Reference results

The streaming and memory results below came from a local development build.
The raw modes used the default million-update scale; materialized L2 used the
explicit 1,000-update scale shown above.
The local replay results came from one Apple Silicon macOS release run using
the synthetic 788,383-event UNI-sized trade fixture.
The exact-event results are three-run medians from page-cache-warm 200,000-event
trade, point, shallow-book, and deep-book fixtures on Apple Silicon macOS.

| Benchmark | Path | Scale | Construction | First event | Throughput | Memory result |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Streaming and memory | Raw events | 1,000,001 rows | — | — | 1.10M rows/s | 30.7 MiB peak RSS |
| Streaming and memory | Direct BBO | 1,000,001 quotes | — | — | 866k rows/s | 30.4 MiB peak RSS |
| Streaming and memory | Raw L2 updates | 1,000,001 updates | — | — | 1.15M rows/s | 30.8 MiB peak RSS |
| Streaming and memory | Lazy 3,000-level builder | 1,000,001 updates | — | — | 443k rows/s | 36.3 MiB peak RSS |
| Streaming and memory | Materialized 3,000-level L2 | 1,001 books | — | — | 646 books/s | 36.9 MiB peak RSS |
| Local replay | Direct zstd+orjson | 788,383 events | <0.01 ms | 0.08 ms | 2.33M events/s | +0.4 MiB peak RSS |
| Local replay | SDK `events()` | 788,383 events | 0.71 ms | 0.07 ms | 1.20M events/s | +2.5 MiB peak RSS |
| Local replay | SDK `replay()` | 788,383 events | 0.72 ms | 0.10 ms | 1.20M events/s | +3.1 MiB peak RSS |
| Exact events | Python dictionary iterator | 200,000 events per fixture | — | — | 722k–824k events/s | +3.1–4.6 MiB peak RSS |
| Exact events | Arrow batches | 200,000 events per fixture | — | — | 976k–1.13M events/s | +50.7–56.2 MiB peak RSS |

In these runs, `events()` was 1.94× the direct decoder time while exceeding the
500,000 events/s target by more than 2×. Exact Arrow batches were 1.35–1.41×
faster than the per-event Python dictionary iterator across the four fixture
shapes. Construction and first-event latency were reported separately; together
they remained under 0.8 ms for `events()`.

## Local dataset storage

Standardized snapshots are stored under the shared Polaris app-data root so the Python SDK and CLI can reuse the same files. Legacy materialized day files are also recognized when present.

Default roots:

- macOS: `~/Library/Application Support/polaris`
- Linux: `$XDG_DATA_HOME/polaris` or `~/.local/share/polaris`
- Windows: `%APPDATA%\\polaris`

Within that root, the SDK uses the same layout as the CLI:

```text
<root>/
  data/
  daily/
  tmp/
  cache/
  locks/
```

Standardized snapshot downloads are stored under:

```text
<root>/data/<tier>/<source>/<market>/<YYYY-MM-DD>/<opaque-key>.jsonl.zst
```

When the snapshot service provides authoritative bounds, the SDK stores them in
an atomic `<opaque-key>.jsonl.zst.coverage.json` sidecar. Explicitly bounded
replays whose local files cover the requested interval do not perform a remote
coverage lookup. Older caches without sidecars remain readable using estimated
filename coverage and emit a warning until exact metadata is available.

The opaque key is the flat upstream snapshot identifier, for example:

```text
standard-aster-ASTERUSDT-2026-06-01-00
```

which is stored on disk as:

```text
<root>/data/standard/aster/ASTERUSDT/2026-06-01/standard-aster-ASTERUSDT-2026-06-01-00.jsonl.zst
```

Compatible materialized day files, when present, are stored under:

```text
<root>/daily/<source>/<market>/<YYYY-MM-DD>.jsonl.zst
```

Pass `dataset_root=...` to `PolarisClient(...)` to override the root explicitly.
`POLARIS_ROOT` overrides the shared root globally.

## Snapshot-first replay

For standardized historical data, `replay(...)`, `events(...)`, `trades(...)`, `intents(...)`, `propamm_quote_ladders(...)`, `vwap(...)`, `volatility(...)`, `bbo(...)`, `depth_metrics(...)`, `l2_snapshots(...)`, `l2_updates(...)`, `volume(...)`, and default/tradingview `ohlcv(...)` now prefer `/snapshots` plus daily bulk `/download?source=...&market=...&date=...&mode=json` manifests, and reuse local snapshot files when they already exist:

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    for row in client.replay(
        source="binance",
        market="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    ):
        print(row)
```

If the requested standardized range cannot be satisfied from available standardized snapshots, `replay(...)`, `events(...)`, `trades(...)`, `intents(...)`, `propamm_quote_ladders(...)`, `vwap(...)`, `volatility(...)`, `bbo(...)`, `depth_metrics(...)`, `l2_snapshots(...)`, `l2_updates(...)`, `volume(...)`, and `ohlcv(...)` raise by default instead of falling back. Pass `allow_gaps=True` on standardized methods to return only covered data and receive a warning with the missing intervals.

## Error handling

```python
from polaris_data import PolarisClient, RateLimitedError, UnauthorizedError

client = PolarisClient()

try:
    client.replay(
        source="binance",
        market="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    )
except UnauthorizedError:
    print("API key is required")
except RateLimitedError as err:
    print(f"Rate limited. Reset at: {err.reset_at}")
```

## Tests

```bash
uv run pytest
cargo test --workspace
cd typescript && npm ci && npm run typecheck && npm test
```

Build and inspect the native Python wheel with:

```bash
uv run --with maturin maturin build --release
```

Python, Rust, and TypeScript are versioned independently. Python releases use
`python-vX.Y.Z` tags and publish `polaris-data` to PyPI; Rust releases use
`rust-vX.Y.Z` tags and publish `polaris-data` to crates.io; TypeScript releases
use `typescript-vX.Y.Z` tags and publish `polaris-data` to npm.
