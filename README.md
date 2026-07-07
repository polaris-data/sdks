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

- `health()`: Check API availability.
- `catalog(source=None, market=None)`: Browse supported sources and markets.
- `list_snapshots(source=..., market=..., from_=..., to=..., limit=1000)`: List available snapshot files for a time range.

### Historical data

- `replay(source=..., market=..., from_=None, to=None, standard=True, allow_gaps=False, parallel=False)`: Stream historical events for backfills, notebooks, or replay-style processing.
- `events(source=..., market=..., from_=None, to=None, allow_gaps=False)`: Return standardized historical events as a list.
- `trades(source=..., market=..., from_=None, to=None, allow_gaps=False)`: Return standardized trade events as a list.
- `vwap(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)`: Aggregate bucketed volume-weighted average price from standardized trade data.
- `volatility(source=..., market=..., from_=None, to=None, interval=..., method="log_returns", allow_gaps=False)`: Aggregate bucketed trade-price volatility as the sample standard deviation of within-bucket log returns.
- `l2_snapshots(source=..., market=..., from_=None, to=None, allow_gaps=False)`: Return standardized orderbook snapshot rows as a list.
- `bbo(source=..., market=..., from_=None, to=None, allow_gaps=False)`: Derive best bid/offer quotes from standardized orderbook snapshots.
- `depth_metrics(source=..., market=..., from_=None, to=None, depth_pct=0.01, slippage_notional=10000.0, allow_gaps=False)`: Derive orderbook depth, spread, imbalance, and slippage metrics from standardized orderbook snapshots.
- `raw(source=..., market=..., from_=None, to=None, limit=1000)`: Return raw source payloads as a list.
- `ohlcv(source=..., market=..., from_=None, to=None, interval=..., format=None, allow_gaps=False)`: Aggregate OHLCV bars from standardized trade data.
- `volume(source=..., market=..., from_=None, to=None, interval=..., allow_gaps=False)`: Aggregate bucketed trade volume from standardized trade data.

For historical event queries, `standard=True` is the default. Pass `standard=False` when you explicitly want raw schema payloads through `replay(...)`. Methods that take `from_` and `to` accept ISO 8601 strings, `datetime`, `date`, or Unix epoch microseconds. If you omit one or both bounds, the SDK uses catalog metadata to infer a bounded range. For open datasets that defaults to the most recent 7 days capped by the dataset `start`/`end`. For unauthenticated preview datasets, `to` is capped at the public cutoff date. Standardized methods also accept `allow_gaps=True` to return rows from covered snapshots only; when gaps are detected the SDK emits a warning naming the skipped intervals instead of fabricating data for the outage.

Example:

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    catalog = client.catalog()
    print(catalog)

    rows = client.events(
        source="binance",
        market="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    )
    print(len(rows))
```

Example response shape:

```python
{
    "exchanges": [
        {
            "id": "binance",
            "assets": [
                {
                    "id": "BTC-USDT",
                    "start": "2024-01-01T00:00:00.000Z",
                    "end": "2024-01-10T00:00:00.000Z",
                    "source": "manifest",
                    "categories": ["perp"],
                    "access": {"status": "open"},
                }
            ],
        }
    ],
    "updatedAt": "2026-05-19T10:28:00.000Z",
}
```

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

For standardized historical data, `replay(...)`, `events(...)`, `trades(...)`, `vwap(...)`, `volatility(...)`, `bbo(...)`, `depth_metrics(...)`, `l2_snapshots(...)`, `volume(...)`, and default/tradingview `ohlcv(...)` now prefer `/snapshots` + `/download` and reuse local snapshot files when they already exist:

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
