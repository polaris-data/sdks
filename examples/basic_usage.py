"""Minimal example for script and notebook workflows."""

from polaris_data import PolarisClient


with PolarisClient(api_key="pk_live_your_key") as client:
    catalog = client.catalog(exchange="binance")
    print("Catalog:", catalog)

    bars = client.ohlcv(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
        interval="1m",
    )

    print(f"Downloaded {len(bars)} bars")
