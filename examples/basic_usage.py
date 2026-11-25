"""Discover a table, then query it with the key from POLARIS_API_KEY."""

from polaris_data import PolarisClient


with PolarisClient() as client:
    coverage = client.coverage(database="standard", table="trades")
    print(coverage["tables"])
    if client.api_key:
        page = client.data("standard.trades", limit=10)
        print(page["data"])
