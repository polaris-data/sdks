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
- `list_snapshots(exchange=..., asset=..., from_=..., to=..., limit=1000)`
- `list_local_snapshots(exchange=None, asset=None, date=None)`
- `iter_local_events(exchange=..., asset=..., from_=None, to=None)`

Authenticated endpoints:

- `download_snapshots(exchange=..., asset=..., from_=..., to=..., force=False)`
- `replay(exchange=..., asset=..., from_=..., to=..., standard=True)` (snapshot-first iterator over standardized events or `/raw`)
- `trades(exchange=..., asset=..., from_=..., to=..., limit=1000)` (collects all pages)
- `events(exchange=..., asset=..., from_=..., to=..., limit=1000)` (snapshot-first list for standardized events)
- `raw(exchange=..., asset=..., from_=..., to=..., limit=1000)` (prefers `format=file`, falls back to paginated JSON)
- `ohlcv(exchange=..., asset=..., from_=..., to=..., interval=..., format=None)`

For event/data endpoints, `standard=True` is the default. Pass `standard=False` when you explicitly need raw schema payloads.

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

For standardized historical events, `replay(...)` and `events(...)` now prefer `/snapshots` + `/snapshots/download` and read from local day files when they already exist:

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

If the requested standardized range cannot be satisfied from daily snapshots, the SDK falls back to the legacy `/events?format=file` flow.

## Legacy replay cache

`replay_cache_enabled` and `replay_cache_dir` are kept for legacy replay behavior, including `standard=False` raw replays and standardized fallback-to-`/events` cases.
The default legacy replay cache path is:

- `<dataset_root>/cache/replay`

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
