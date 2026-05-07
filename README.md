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
- `exchanges()`
- `assets(exchange=...)`
- `timerange(exchange=..., asset=...)`
- `dataset_size(exchange=..., asset=..., from_=..., to=...)`
- `catalog()`
- `dataset_preview(..., standard=True)`
- `ohlcv_preview(..., interval, limit=None, format=None)`

Authenticated endpoints:

- `dataset_download_url(exchange=..., asset=..., from_=..., to=..., standard=True)`
- `replay(exchange=..., asset=..., from_=..., to=..., standard=True)` (stream rows from dataset download URL)
- `download_dataset(exchange=..., asset=..., from_=..., to=..., standard=True, destination=None, filename=None, overwrite=False, decompress=True, keep_compressed=False)`
- `trades(exchange=..., asset=..., from_=..., to=..., limit=1000)` (collects all pages)
- `iter_ohlcv(exchange=..., asset=..., from_=..., to=..., interval=...)`
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

`replay(...)` checks the local replay cache first, then fetches and caches the dataset if needed.
Replay cache is enabled by default and initialized when the client is created.

## Optional: persist dataset files

For safety, file downloads are disabled by default. Enable them explicitly when you want to save files locally:

```python
from polaris_data import PolarisClient

with PolarisClient(
    api_key="polaris_key_your_key",
    allow_dataset_downloads=True,
) as client:
    file_path = client.download_dataset(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    )
    print(file_path)
```

`download_dataset(...)` decompresses `.zst` files by default and returns the decompressed file path (for example `.jsonl`).
Set `decompress=False` to keep the original `.zst`, or `keep_compressed=True` to keep both files.

Default download directories:

- macOS: `~/Library/Caches/polaris/datasets`
- Linux: `~/.cache/polaris/datasets`
- Windows: `%LOCALAPPDATA%\\polaris\\datasets`

You can override this with `dataset_download_dir=...` on the client or `destination=...` on a single `download_dataset(...)` call.
Set `POLARIS_DATASET_DOWNLOAD_DIR` to override the default directory globally.
Replay cache defaults to a `replay/` subdirectory under `dataset_download_dir`.
Use `replay_cache_enabled=False` to disable replay caching, or `replay_cache_dir=...` to move it.

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
