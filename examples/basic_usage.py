"""Minimal example for script and notebook workflows."""

from polaris_data import PolarisClient


with PolarisClient(api_key="pk_live_your_key") as client:
    catalog = client.catalog(exchange="binance")
    print("Catalog:", catalog)

    snapshots = client.download_snapshots(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-02T00:00:00Z",
    )
    print("Local snapshot:", snapshots[0].path)

    rows = list(
        client.iter_local_events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
    )
    print(f"Loaded {len(rows)} local event rows")

    bars = client.ohlcv(
        exchange="binance",
        asset="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
        interval="1m",
    )

    print(f"Downloaded {len(bars)} bars")
