# polaris-py

Python SDK for the Polaris API, optimized for notebook workflows and trading scripts.
Documentation can be found at https://polaris.supply/docs

## Install

Install the published SDK from PyPI:

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

## PolarisClient API

`PolarisClient` is the main sync client for the SDK:

```python
PolarisClient(
    api_key=None,
    base_url="https://api.polaris.supply",
    timeout=30.0,
    dataset_root=None,
)
```

Use it to inspect available data and query historical market data.

### Discovery

| Method | Returns | Use case |
| --- | --- | --- |
| `health()` | API health/status payload | Connectivity checks and startup validation |
| `catalog(source=None, market=None, q=None)` | Source/market metadata, including normalized instrument fields | Discover supported datasets, markets, instrument metadata, and time coverage |

### Access patterns

| Method | Returns | Use case |
| --- | --- | --- |
| `replay(source=..., market=..., from_=None, to=None, standard=True, allow_gaps=False, parallel=False)` | Iterator of historical events | Backfills, notebooks, and replay-style processing without materializing everything up front |
| `raw(source=..., market=..., from_=None, to=None, limit=1000)` | List of raw source payloads | Inspect exchange-native payloads and compare raw vs standardized schemas |

### Standardized Data Schemas

| Method | Returns | Use case |
| --- | --- | --- |
| `events(source=..., market=..., from_=None, to=None, allow_gaps=False)` | List of standardized historical events | General-purpose historical analysis when you want the normalized event stream in memory |
| `trades(source=..., market=..., from_=None, to=None, allow_gaps=False)` | List of standardized trade events | Trade-level analytics, execution studies, and derived bar calculations |
| `l2_snapshots(source=..., market=..., from_=None, to=None, allow_gaps=False)` | List of standardized orderbook snapshot rows | Order book reconstruction and microstructure analysis |
| `funding_rates(source=..., market=..., from_=None, to=None, allow_gaps=False)` | List of funding-rate point series rows | Perpetual funding studies and carry modeling |
| `mark_prices(source=..., market=..., from_=None, to=None, allow_gaps=False)` | List of mark-price point series rows | Basis analysis, mark tracking, and liquidation-related research |
| `ohlcv(source=..., market=..., from_=None, to=None, interval=..., format=None, allow_gaps=False)` | Aggregated OHLCV bars | Charting, bar-based strategies, and downstream TA workflows |
| `volume(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)` | Bucketed trade volume series | Volume profiling and participation analysis |
| `vwap(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)` | Bucketed VWAP series | Execution benchmarking and price smoothing |
| `volatility(source=..., market=..., from_=None, to=None, interval=..., method="log_returns", allow_gaps=False)` | Bucketed realized volatility series | Risk modeling and intraperiod volatility analysis |
| `bbo(source=..., market=..., from_=None, to=None, allow_gaps=False)` | Best bid/offer quote series | Spread tracking, quote analytics, and top-of-book monitoring |
| `depth_metrics(source=..., market=..., from_=None, to=None, depth_pct=0.01, slippage_notional=10000.0, allow_gaps=False)` | Derived depth, spread, imbalance, and slippage metrics | Liquidity analysis and market impact estimation |

For parameter details, response shapes, and end-to-end examples, see the
[Python SDK docs](https://docs.polaris.supply/sdks/python).

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
`POLARIS_DATASET_DOWNLOAD_DIR` is still accepted as a deprecated compatibility override.

## Snapshot-first replay

For standardized historical data, `replay(...)`, `events(...)`, `trades(...)`, `vwap(...)`, `volatility(...)`, `bbo(...)`, `depth_metrics(...)`, `l2_snapshots(...)`, `volume(...)`, and default/tradingview `ohlcv(...)` now prefer `/snapshots` plus daily bulk `/download?source=...&market=...&date=...&mode=json` manifests, and reuse local snapshot files when they already exist:

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

If the requested standardized range cannot be satisfied from available standardized snapshots, `replay(...)`, `events(...)`, `trades(...)`, `vwap(...)`, `volatility(...)`, `bbo(...)`, `depth_metrics(...)`, `l2_snapshots(...)`, `volume(...)`, and `ohlcv(...)` raise by default instead of falling back. Pass `allow_gaps=True` on standardized methods to return only covered data and receive a warning with the missing intervals.

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
```
