"""Minimal example for script and notebook workflows."""

from polaris_data import PolarisClient


with PolarisClient(api_key="pk_live_your_key") as client:
    catalog = client.catalog(source="binance")
    print("Catalog:", catalog)

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

    rows = client.events(
        source="binance",
        market="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
    )
    print(f"Loaded {len(rows)} event rows")

    bars = client.ohlcv(
        source="binance",
        market="BTC-USDT",
        from_="2024-01-01T00:00:00Z",
        to="2024-01-01T01:00:00Z",
        interval="1m",
    )

    print(f"Downloaded {len(bars)} bars")
