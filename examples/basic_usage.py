"""Minimal example for script and notebook workflows."""

from polaris_data import PolarisClient


with PolarisClient(api_key="pk_live_your_key") as client:
    catalog = client.catalog(source="binance")
    print("Catalog:", catalog)

    row_count = sum(
        1
        for _ in client.trades(
            source="binance",
            market="BTC-USDT",
            start=1_704_067_200_000,
            end=1_704_070_800_000,
        )
    )
    print(f"Loaded {row_count} trade rows")

    candles = client.ohlcv(
        source="binance",
        market="BTC-USDT",
        start=1_704_067_200_000,
        end=1_704_070_800_000,
        interval="1m",
    )

    print(f"Downloaded {sum(1 for _ in candles)} candle updates")
