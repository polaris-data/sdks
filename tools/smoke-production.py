#!/usr/bin/env python3
"""Exercise the production service from an installed SDK during release checks."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time

from polaris_data import PolarisClient


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authenticated", action="store_true")
    args = parser.parse_args()
    with PolarisClient() as public:
        assert public.health()["ok"]
        assert public.openapi()["openapi"].startswith("3.")
        assert "tables" in public.coverage(database="standard", table="trades")
        assert public.sql("SELECT 1")["data"] == [[1]]
    print("Public production requests passed")
    if not args.authenticated:
        return

    token = os.environ.get("POLARIS_SDK_SMOKE_API_KEY")
    candle_query = os.environ.get("POLARIS_SMOKE_CANDLE_QUERY")
    subscription = os.environ.get("POLARIS_SMOKE_STREAM_SUBSCRIPTION")
    if not (token and candle_query and subscription):
        raise SystemExit("Configure the scoped SDK smoke key and the two production query variables")
    with PolarisClient(token) as client:
        start = int(time.time() * 1000) - 3_600_000
        assert "data" in client.data("standard.trades", start=start, limit=1)
        assert "data" in client.candles(**json.loads(candle_query))
        with client.stream([json.loads(subscription)]) as pages:
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(next, pages)
                try:
                    assert pending.result(timeout=25)["cursor"]
                finally:
                    pages.close()
    print("Authenticated data, candles, and stream requests passed")


if __name__ == "__main__":
    main()
