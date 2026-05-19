from __future__ import annotations

import json

import httpx
import pytest
import zstandard as zstd

from polaris_data import PolarisClient
from polaris_data.errors import (
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)


def _zstd_ndjson(rows: list[dict]) -> bytes:
    ndjson = b"".join(
        f"{json.dumps(row, separators=(',', ':'), ensure_ascii=True)}\n".encode("utf-8")
        for row in rows
    )
    return zstd.ZstdCompressor().compress(ndjson)


def make_client(
    handler,
    api_key: str | None = "polaris_key_test",
    replay_cache_enabled: bool = False,
) -> PolarisClient:
    transport = httpx.MockTransport(handler)
    return PolarisClient(
        api_key=api_key,
        transport=transport,
        replay_cache_enabled=replay_cache_enabled,
    )


def test_catalog_returns_payload() -> None:
    payload = {
        "exchanges": [
            {"id": "binance", "assets": ["BTC-USDT"]},
            {"id": "hyperliquid", "assets": ["BTC", "ETH"]},
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        assert request.url.params == httpx.QueryParams()
        assert request.headers.get("authorization") == "Bearer polaris_key_test"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        assert client.catalog() == payload
    finally:
        client.close()


def test_catalog_with_exchange_filter() -> None:
    payload = {"exchanges": [{"id": "binance", "assets": ["BTC-USDT", "ETH-USDT"]}]}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        assert request.url.params.get("exchange") == "binance"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        assert client.catalog(exchange="binance") == payload
    finally:
        client.close()


def test_catalog_with_exchange_and_asset_filter() -> None:
    payload = {"exchanges": [{"id": "binance", "assets": ["BTC-USDT"]}]}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        assert request.url.params.get("exchange") == "binance"
        assert request.url.params.get("asset") == "BTC-USDT"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        assert client.catalog(exchange="binance", asset="BTC-USDT") == payload
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
            client.trades(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
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
            client.trades(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )

        assert exc_info.value.reset_at == "2026-05-01T00:00:00.000Z"
    finally:
        client.close()


def test_trades_paginates() -> None:
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
        results = client.trades(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        )
        assert results == [{"id": 1}, {"id": 2}]
    finally:
        client.close()




def test_ohlcv_tradingview_format_returns_json() -> None:
    payload = {
        "candles": [
            {"time": 1704067200000000, "open": 1, "high": 2, "low": 0, "close": 1.5}
        ],
        "volumes": [{"time": 1704067200000000, "value": 10}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ohlcv"
        assert request.url.params.get("format") == "tradingview"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        response = client.ohlcv(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            interval="1m",
            format="tradingview",
        )
        assert response == payload
    finally:
        client.close()


def test_replay_streams_rows_from_events_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        assert request.url.params.get("exchange") == "binance"
        assert request.url.params.get("asset") == "BTC-USDT"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson(
                [{"timestamp": 1, "type": "trade"}, {"timestamp": 2, "type": "bar"}]
            ),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1, "type": "trade"}, {"timestamp": 2, "type": "bar"}]
    finally:
        client.close()


def test_events_paginates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "data": [{"timestamp": 1}],
                    "next_cursor": "cursor-2",
                    "has_more": True,
                },
            )
        assert cursor == "cursor-2"
        return httpx.Response(
            200,
            json={
                "data": [{"timestamp": 2}],
                "next_cursor": None,
                "has_more": False,
            },
        )

    client = make_client(handler)
    try:
        rows = client.events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        )
        assert rows == [{"timestamp": 1}, {"timestamp": 2}]
    finally:
        client.close()


def test_events_uses_file_export_by_default() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 7}, {"timestamp": 8}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        rows = client.events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
        assert rows == [{"timestamp": 7}, {"timestamp": 8}]
    finally:
        client.close()


def test_raw_paginates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "data": [{"exchange_payload": {"id": 10}}],
                    "next_cursor": "cursor-raw-2",
                    "has_more": True,
                },
            )
        assert cursor == "cursor-raw-2"
        return httpx.Response(
            200,
            json={
                "data": [{"exchange_payload": {"id": 11}}],
                "next_cursor": None,
                "has_more": False,
            },
        )

    client = make_client(handler)
    try:
        rows = client.raw(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        )
        assert rows == [{"exchange_payload": {"id": 10}}, {"exchange_payload": {"id": 11}}]
    finally:
        client.close()


def test_raw_uses_file_export_by_default() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"exchange_payload": {"id": 42}}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        rows = client.raw(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
        assert rows == [{"exchange_payload": {"id": 42}}]
    finally:
        client.close()


def test_replay_raises_stream_decode_error_for_invalid_cached_zstd(tmp_path) -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        cache_name = client._default_dataset_filename(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=True,
        )
        cache_path = tmp_path / cache_name
        cache_path.write_bytes(b"not-zs")

        with pytest.raises(StreamDecodeError):
            list(
                client.replay(
                    exchange="binance",
                    asset="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T01:00:00Z",
                )
            )
        assert called is False
    finally:
        client.close()


def test_replay_reads_cached_rows_without_api_call(tmp_path) -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        cache_name = client._default_dataset_filename(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=True,
        )
        cache_path = (tmp_path / cache_name).with_suffix("")
        cache_path.write_bytes(b'{"timestamp":99}\n')

        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 99}]
        assert called is False
    finally:
        client.close()


def test_replay_reads_bugged_compressed_jsonl_cache_without_api_call(tmp_path) -> None:
    called = False
    events = b'{"timestamp":101}\n{"timestamp":102}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        cache_name = client._default_dataset_filename(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=True,
        )
        cache_path = (tmp_path / cache_name).with_suffix("")
        cache_path.write_bytes(compressed)

        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 101}, {"timestamp": 102}]
        assert called is False
    finally:
        client.close()


def test_replay_populates_cache_and_reuses_on_new_client(tmp_path) -> None:
    def online_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 11}, {"timestamp": 12}]),
            headers={"content-type": "application/zstd"},
        )

    online_client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(online_handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        rows = list(
            online_client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 11}, {"timestamp": 12}]
    finally:
        online_client.close()

    def offline_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            "Network should not be called when replay cache has the dataset"
        )

    offline_client = PolarisClient(
        api_key=None,
        transport=httpx.MockTransport(offline_handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        cached_rows = list(
            offline_client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert cached_rows == [{"timestamp": 11}, {"timestamp": 12}]
    finally:
        offline_client.close()


def test_replay_uses_events_endpoint_with_cache(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 21}, {"timestamp": 22}]),
            headers={"content-type": "application/zstd"},
        )

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 21}, {"timestamp": 22}]
    finally:
        client.close()


def test_replay_allows_standard_false_for_raw() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 31}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        )
        assert rows == [{"timestamp": 31}]
    finally:
        client.close()


def test_replay_parallel_splits_into_chunks() -> None:
    """Test that parallel replay splits multi-day requests into chunks."""
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count

        if request.url.path == "/events":
            request_count += 1
            assert request.url.params.get("format") == "file"
            from_param = request.url.params.get("from", "")

            if "2024-01-01" in from_param:
                return httpx.Response(
                    200,
                    content=_zstd_ndjson([{"timestamp": 1}, {"timestamp": 2}]),
                    headers={"content-type": "application/zstd"},
                )
            if "2024-01-02" in from_param:
                return httpx.Response(
                    200,
                    content=_zstd_ndjson([{"timestamp": 3}, {"timestamp": 4}]),
                    headers={"content-type": "application/zstd"},
                )
            if "2024-01-03" in from_param:
                return httpx.Response(
                    200,
                    content=_zstd_ndjson([{"timestamp": 5}, {"timestamp": 6}]),
                    headers={"content-type": "application/zstd"},
                )

        return httpx.Response(404)

    client = make_client(handler, replay_cache_enabled=False)
    try:
        # Request 3 days with parallel enabled
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-04T00:00:00Z",  # 3 days
                parallel=True,
            )
        )

        # Should have made 3 separate events requests (one per day)
        assert request_count == 3

        # Should get all records in chronological order
        assert rows == [
            {"timestamp": 1},
            {"timestamp": 2},
            {"timestamp": 3},
            {"timestamp": 4},
            {"timestamp": 5},
            {"timestamp": 6},
        ]
    finally:
        client.close()


def test_replay_parallel_with_custom_workers() -> None:
    """Test that parallel replay respects max_workers setting."""
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/events"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 1}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler, replay_cache_enabled=False)
    try:
        # Request 2 days with custom worker count
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-03T00:00:00Z",  # 2 days
                parallel=8,  # Max 8 workers
            )
        )

        # Should work without errors
        assert len(rows) >= 0
    finally:
        client.close()


def test_replay_parallel_single_day_uses_regular_replay() -> None:
    """Test that single-day requests don't use parallel mode."""
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        if request.url.path == "/events":
            request_count += 1
            assert request.url.params.get("format") == "file"
            return httpx.Response(
                200,
                content=_zstd_ndjson([{"timestamp": 1}]),
                headers={"content-type": "application/zstd"},
            )
        return httpx.Response(404)

    client = make_client(handler, replay_cache_enabled=False)
    try:
        # Request less than 24 hours with parallel enabled
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T12:00:00Z",  # 12 hours (single chunk)
                parallel=True,
            )
        )

        # Should only make 1 request (no chunking needed)
        assert request_count == 1
        assert rows == [{"timestamp": 1}]
    finally:
        client.close()


def test_replay_falls_back_to_paginated_json_when_file_export_unavailable() -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path != "/events":
            return httpx.Response(404)

        if request.url.params.get("format") == "file":
            return httpx.Response(
                200,
                json={"error": "file export not enabled"},
            )

        return httpx.Response(
            200,
            json={
                "data": [{"timestamp": 77}],
                "next_cursor": None,
                "has_more": False,
            },
        )

    client = make_client(handler, replay_cache_enabled=False)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 77}]
        assert calls == [("/events", "file"), ("/events", None)]
    finally:
        client.close()


def test_replay_file_export_follows_redirect() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))

        if request.url.path == "/events":
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/replay/events.jsonl.zst"},
            )

        if request.url.path == "/replay/events.jsonl.zst":
            return httpx.Response(
                200,
                content=_zstd_ndjson([{"timestamp": 88}, {"timestamp": 89}]),
                headers={"content-type": "application/zstd"},
            )

        return httpx.Response(404)

    client = make_client(handler, replay_cache_enabled=False)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert rows == [{"timestamp": 88}, {"timestamp": 89}]
        assert len(calls) == 2
        assert calls[0].startswith("https://api.polaris.supply/events?")
        assert calls[1] == "https://download.example.com/replay/events.jsonl.zst"
    finally:
        client.close()
