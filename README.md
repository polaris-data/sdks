# polaris-py

Python SDK for the Polaris API, optimized for notebook workflows and trading scripts.

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

with PolarisClient.new("pk_live_your_key") as client:
    exchanges = client.exchanges()
    assets = client.assets(exchanges[0])

    trades = client.collect_all_trades(
        exchange=exchanges[0],
        asset=assets[0],
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
        limit=500,
    )

    print(f"Loaded {len(trades)} trades")
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
- `assets(exchange)`
- `timerange(exchange, asset)`
- `dataset_size(exchange, asset, from_, to)`
- `catalog()`
- `dataset_preview(..., standard=False)`
- `ohlcv_preview(..., interval, limit=None, format=None)`

Authenticated endpoints:

- `dataset_download_url(..., standard=False)`
- `trades_page(..., limit=1000, cursor=None)`
- `iter_trades(...)`
- `collect_all_trades(...)`
- `stream_events(..., standard=False)`
- `collect_events(..., standard=False)`
- `iter_ohlcv(..., interval)`
- `ohlcv(..., interval, format=None)`

## Error handling

```python
from polaris_data import PolarisClient, RateLimitedError, UnauthorizedError

client = PolarisClient.anonymous()

try:
    client.collect_events("binance", "BTC-USDT", "2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z")
except UnauthorizedError:
    print("API key is required")
except RateLimitedError as err:
    print(f"Rate limited. Reset at: {err.reset_at}")
```

## Tests

```bash
uv run pytest
```
