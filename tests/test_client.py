from __future__ import annotations

import httpx
import pytest

from polaris_data import PolarisClient
from polaris_data.errors import RateLimitedError, UnauthorizedError


def make_client(handler, api_key: str | None = "pk_live_test") -> PolarisClient:
    transport = httpx.MockTransport(handler)
    return PolarisClient(api_key=api_key, transport=transport)


def test_exchanges_response_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/catalog/exchanges"
        return httpx.Response(200, json={"exchanges": ["binance", "hyperliquid"]})

    client = make_client(handler)
    try:
        assert client.exchanges() == ["binance", "hyperliquid"]
    finally:
        client.close()


def test_unauthorized_requires_api_key_before_request() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = make_client(handler, api_key=None)
    try:
        with pytest.raises(UnauthorizedError):
            client.dataset_download_url(
                "binance",
                "BTC-USDT",
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
            )
        assert called is False
    finally:
        client.close()


def test_rate_limited_error_maps_reset_at() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"error": "quota exceeded", "reset_at": "2026-05-01T00:00:00.000Z"},
        )

    client = make_client(handler)
    try:
        with pytest.raises(RateLimitedError) as exc_info:
            client.trades_page(
                "binance",
                "BTC-USDT",
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
            )

        assert exc_info.value.reset_at == "2026-05-01T00:00:00.000Z"
    finally:
        client.close()


def test_collect_all_trades_paginates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "data": [{"id": 1}],
                    "next_cursor": "cursor-2",
                    "has_more": True,
                },
            )

        assert cursor == "cursor-2"
        return httpx.Response(
            200,
            json={
                "data": [{"id": 2}],
                "next_cursor": None,
                "has_more": False,
            },
        )

    client = make_client(handler)
    try:
        results = client.collect_all_trades(
            "binance",
            "BTC-USDT",
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
            limit=1,
        )
        assert results == [{"id": 1}, {"id": 2}]
    finally:
        client.close()


def test_collect_events_parses_ndjson_stream() -> None:
    ndjson = b'{"timestamp": 1, "type": "trade"}\n{"timestamp": 2, "type": "trade"}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/stream"
        return httpx.Response(200, content=ndjson)

    client = make_client(handler)
    try:
        events = client.collect_events(
            "binance",
            "BTC-USDT",
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
            standard=True,
        )
        assert [event["timestamp"] for event in events] == [1, 2]
    finally:
        client.close()


def test_ohlcv_tradingview_format_returns_json() -> None:
    payload = {
        "candles": [{"time": 1704067200000000, "open": 1, "high": 2, "low": 0, "close": 1.5}],
        "volumes": [{"time": 1704067200000000, "value": 10}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/ohlcv"
        assert request.url.params.get("format") == "tradingview"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        response = client.ohlcv(
            "binance",
            "BTC-USDT",
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
            interval="1m",
            format="tradingview",
        )
        assert response == payload
    finally:
        client.close()
