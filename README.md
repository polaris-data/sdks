# polaris-py

Python SDK for the Polaris API, optimized for notebook workflows and trading scripts.
Documentation can be found at https://polaris.supply/docs

## Install (uv)

```bash
uv sync --group dev
```

Useful commands:

```bash
uv run python
uv lock
```

## Quickstart

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    row_count = 0
    for row in client.replay(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    ):
        row_count += 1

    print(f"Replayed {row_count} rows")
```

If `api_key` is omitted, the client reads `POLARIS_API_KEY` from the environment.

## Supported input time types

Methods that take `from_` and `to` accept:

- ISO 8601 strings (`"2024-01-01T00:00:00Z"`)
- `datetime.datetime`
- `datetime.date`
- Unix epoch microseconds (`int`/`float`)

## Methods

Open endpoints:

- `health()`
- `catalog(exchange=None, asset=None)`

Authenticated endpoints:

- `replay(exchange=..., asset=..., from_=..., to=..., standard=True)` (iterator over `/events` or `/raw`)
- `trades(exchange=..., asset=..., from_=..., to=..., limit=1000)` (collects all pages)
- `events(exchange=..., asset=..., from_=..., to=..., limit=1000)` (prefers `format=file`, falls back to paginated JSON)
- `raw(exchange=..., asset=..., from_=..., to=..., limit=1000)` (prefers `format=file`, falls back to paginated JSON)
- `ohlcv(exchange=..., asset=..., from_=..., to=..., interval=..., format=None)`

For event/data endpoints, `standard=True` is the default. Pass `standard=False` when you explicitly need raw schema payloads.

## Dataset replay (recommended)

For row-by-row iteration without managing file paths, use `replay(...)`:

```python
from polaris_data import PolarisClient

with PolarisClient(api_key="polaris_key_your_key") as client:
    for row in client.replay(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    ):
        print(row)
```

`replay(...)` checks the local replay cache first, then requests bulk export (`format=file`) and streams the compressed `.jsonl.zst` response row-by-row. If file export is unavailable, it falls back to paginated JSON and still caches rows as NDJSON.
Replay cache is enabled by default and stored under:

- macOS: `~/Library/Caches/polaris/datasets/replay`
- Linux: `~/.cache/polaris/datasets/replay`
- Windows: `%LOCALAPPDATA%\\polaris\\datasets\\replay`

Set `POLARIS_DATASET_DOWNLOAD_DIR` to override the base directory globally.
Use `replay_cache_enabled=False` to disable caching, or `replay_cache_dir=...` to set a custom path.

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
