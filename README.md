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
    local_files = client.download_snapshots(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-02T00:00:00Z",
    )
    print(local_files[0].path)

    row_count = sum(
        1
        for _ in client.replay(
            exchange="binance",
            asset="BTC-USDT",
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

Use it to inspect available data, download snapshots for local reuse, and query historical market data.

### Discovery

- `health()`: Check API availability.
- `catalog(exchange=None, asset=None)`: Browse supported exchanges and assets.
- `list_snapshots(exchange=..., asset=..., from_=..., to=..., limit=1000)`: List available snapshot files for a time range.

Example:

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    catalog = client.catalog()
    print(catalog)
```

Example response shape:

```python
{
    "exchanges": [
        {"id": "binance", "assets": ["BTC-USDT"]},
        {"id": "hyperliquid", "assets": ["BTC", "ETH"]},
    ]
}
```

### Local dataset helpers

- `download_snapshots(exchange=..., asset=..., from_=..., to=..., force=False)`: Download snapshot files into the local Polaris dataset cache.
- `list_local_snapshots(exchange=None, asset=None, date=None)`: Inspect snapshots that already exist on disk.
- `iter_local_events(exchange=..., asset=..., from_=None, to=None)`: Stream standardized events from local day files without hitting the API.

### Historical data

- `replay(exchange=..., asset=..., from_=..., to=..., standard=True, parallel=False)`: Stream historical events for backfills, notebooks, or replay-style processing.
- `events(exchange=..., asset=..., from_=..., to=..., limit=1000)`: Return standardized historical events as a list.
- `trades(exchange=..., asset=..., from_=..., to=..., limit=1000)`: Return standardized trade events as a list.
- `raw(exchange=..., asset=..., from_=..., to=..., limit=1000)`: Return raw exchange payloads as a list.
- `ohlcv(exchange=..., asset=..., from_=..., to=..., interval=..., format=None)`: Aggregate OHLCV bars from standardized trade data.

For historical event queries, `standard=True` is the default. Pass `standard=False` when you explicitly want raw schema payloads through `replay(...)`. Methods that take `from_` and `to` accept ISO 8601 strings, `datetime`, `date`, or Unix epoch microseconds.

## Local dataset storage

Standardized snapshots and local day files are stored under the shared Polaris app-data root so the Python SDK and CLI can reuse the same files.

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

Pass `dataset_root=...` to `PolarisClient(...)` to override the root explicitly.
`POLARIS_ROOT` overrides the shared root globally.
`POLARIS_DATASET_DOWNLOAD_DIR` is still accepted as a deprecated compatibility override.

## Snapshot-first replay

For standardized historical data, `replay(...)`, `events(...)`, `trades(...)`, and default/tradingview `ohlcv(...)` now prefer `/snapshots` + `/snapshots/download` and read from local day files when they already exist:

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    client.download_snapshots(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-03T00:00:00Z",
    )

    for row in client.replay(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    ):
        print(row)
```

For notebook and local analysis workflows, use the local helpers directly:

```python
from polaris_data import PolarisClient

with PolarisClient() as client:
    local_entries = client.list_local_snapshots(exchange="binance", asset="BTC-USDT")
    print(local_entries[0].path)

    rows = list(
        client.iter_local_events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
    )
    print(len(rows))
```

If the requested standardized range cannot be satisfied from daily snapshots, the SDK falls back to the legacy `/events?format=file` flow for standardized replay, event, trade, and local OHLCV derivation.

## Error handling

```python
from polaris_data import PolarisClient, RateLimitedError, UnauthorizedError

client = PolarisClient()

try:
    client.replay(
        exchange="binance",
        asset="BTC-USDT",
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
