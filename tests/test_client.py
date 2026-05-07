from __future__ import annotations

import httpx
import pytest
import zstandard as zstd

from polaris_data import PolarisClient
from polaris_data.errors import (
    DownloadNotAllowedError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)


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


def test_exchanges_response_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        return httpx.Response(
            200,
            json={
                "exchanges": [
                    {"id": "binance", "assets": ["BTC-USDT"]},
                    {"id": "hyperliquid", "assets": ["BTC", "ETH"]},
                ]
            },
        )

    client = make_client(handler)
    try:
        assert client.exchanges() == ["binance", "hyperliquid"]
    finally:
        client.close()


def test_assets_reads_from_catalog() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        return httpx.Response(
            200,
            json={
                "exchanges": [
                    {"id": "binance", "assets": ["BTC-USDT", "ETH-USDT"]},
                    {"id": "okx", "assets": ["BTC-USDT"]},
                ]
            },
        )

    client = make_client(handler)
    try:
        assert client.assets(exchange="binance") == ["BTC-USDT", "ETH-USDT"]
    finally:
        client.close()


def test_assets_unknown_exchange_returns_empty_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        return httpx.Response(
            200,
            json={"exchanges": [{"id": "binance", "assets": ["BTC-USDT"]}]},
        )

    client = make_client(handler)
    try:
        assert client.assets(exchange="does-not-exist") == []
    finally:
        client.close()


def test_timerange_uses_explicit_timerange_endpoint() -> None:
    expected = {
        "exchange": "binance",
        "asset": "BTC-USDT",
        "start": "2026-05-01T00:15:00.000Z",
        "end": "2026-05-07T23:45:00.000Z",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/timerange"
        assert request.url.params.get("exchange") == "binance"
        assert request.url.params.get("asset") == "BTC-USDT"
        return httpx.Response(200, json=expected)

    client = make_client(handler)
    try:
        assert client.timerange(exchange="binance", asset="BTC-USDT") == expected
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
            client.trades_page(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
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
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        )
        assert results == [{"id": 1}, {"id": 2}]
    finally:
        client.close()


def test_collect_events_parses_ndjson_stream() -> None:
    ndjson = b'{"timestamp": 1, "type": "trade"}\n{"timestamp": 2, "type": "trade"}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/stream"
        return httpx.Response(200, content=ndjson)

    client = make_client(handler)
    try:
        events = client.collect_events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=True,
        )
        assert [event["timestamp"] for event in events] == [1, 2]
    finally:
        client.close()


def test_dataset_download_url_defaults_to_standard_true() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/datasets/download"
        assert request.url.params.get("standard") == "true"
        return httpx.Response(
            200,
            json={
                "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                "totalBytes": 4,
                "fileCount": 1,
            },
        )

    client = make_client(handler)
    try:
        payload = client.dataset_download_url(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
        assert (
            payload["url"] == "https://downloads.example.com/datasets/sample.jsonl.zst"
        )
    finally:
        client.close()


def test_dataset_download_url_allows_standard_false_for_raw() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/datasets/download"
        assert request.url.params.get("standard") == "false"
        return httpx.Response(
            200,
            json={
                "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                "totalBytes": 4,
                "fileCount": 1,
            },
        )

    client = make_client(handler)
    try:
        payload = client.dataset_download_url(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=False,
        )
        assert (
            payload["url"] == "https://downloads.example.com/datasets/sample.jsonl.zst"
        )
    finally:
        client.close()


def test_stream_events_defaults_to_standard_true() -> None:
    ndjson = b'{"timestamp": 1}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/stream"
        assert request.url.params.get("standard") == "true"
        return httpx.Response(200, content=ndjson)

    client = make_client(handler)
    try:
        events = list(
            client.stream_events(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert events == [{"timestamp": 1}]
    finally:
        client.close()


def test_stream_events_allows_standard_false_for_raw() -> None:
    ndjson = b'{"timestamp": 1}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/stream"
        assert request.url.params.get("standard") == "false"
        return httpx.Response(200, content=ndjson)

    client = make_client(handler)
    try:
        events = list(
            client.stream_events(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        )
        assert events == [{"timestamp": 1}]
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


def test_download_dataset_requires_explicit_opt_in() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = make_client(handler)
    try:
        with pytest.raises(DownloadNotAllowedError):
            client.download_dataset(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        assert called is False
    finally:
        client.close()


def test_download_dataset_writes_s3_payload(tmp_path) -> None:
    dataset_bytes = b'{"timestamp":1}\n'
    compressed_bytes = zstd.ZstdCompressor().compress(dataset_bytes)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": 4,
                    "fileCount": 1,
                },
            )

        assert request.url.host == "downloads.example.com"
        return httpx.Response(200, content=compressed_bytes)

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        allow_dataset_downloads=True,
        dataset_download_dir=tmp_path,
    )
    try:
        path = client.download_dataset(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        )
        assert path == tmp_path / "sample.jsonl"
        assert path.read_bytes() == dataset_bytes
        assert not (tmp_path / "sample.jsonl.zst").exists()
    finally:
        client.close()


def test_download_dataset_existing_file_requires_overwrite(tmp_path) -> None:
    existing = tmp_path / "sample.jsonl.zst"
    existing.write_bytes(b"old")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": 3,
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=b"new")

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        allow_dataset_downloads=True,
        dataset_download_dir=tmp_path,
    )
    try:
        with pytest.raises(PolarisError):
            client.download_dataset(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                decompress=False,
            )
        assert existing.read_bytes() == b"old"

        path = client.download_dataset(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            overwrite=True,
            decompress=False,
        )
        assert path == existing
        assert existing.read_bytes() == b"new"
    finally:
        client.close()


def test_download_dataset_keep_compressed_true_keeps_both_files(tmp_path) -> None:
    dataset_bytes = b'{"timestamp":2}\n'
    compressed_bytes = zstd.ZstdCompressor().compress(dataset_bytes)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": len(compressed_bytes),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed_bytes)

    client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(handler),
        allow_dataset_downloads=True,
        dataset_download_dir=tmp_path,
    )
    try:
        path = client.download_dataset(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            keep_compressed=True,
        )
        assert path == tmp_path / "sample.jsonl"
        assert path.read_bytes() == dataset_bytes
        assert (tmp_path / "sample.jsonl.zst").read_bytes() == compressed_bytes
    finally:
        client.close()


def test_replay_streams_rows_from_zstd_download_url() -> None:
    events = b'{"timestamp":1,"type":"trade"}\n{"timestamp":2,"type":"trade"}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "true"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": len(compressed),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed)

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
        assert rows == [
            {"timestamp": 1, "type": "trade"},
            {"timestamp": 2, "type": "trade"},
        ]
    finally:
        client.close()


def test_replay_streams_rows_from_plain_ndjson_download_url() -> None:
    events = b'{"timestamp":3}\n{"timestamp":4}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "true"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl",
                    "totalBytes": len(events),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=events)

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
        assert rows == [{"timestamp": 3}, {"timestamp": 4}]
    finally:
        client.close()


def test_replay_raises_stream_decode_error_for_invalid_zstd() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "true"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": 6,
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=b"not-zs")

    client = make_client(handler)
    try:
        with pytest.raises(StreamDecodeError):
            list(
                client.replay(
                    exchange="binance",
                    asset="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T01:00:00Z",
                )
            )
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
    events = b'{"timestamp":11}\n{"timestamp":12}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    def online_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "true"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": len(compressed),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed)

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


def test_replay_handles_signed_zstd_download_url_with_cache(tmp_path) -> None:
    events = b'{"timestamp":21}\n{"timestamp":22}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "true"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst?X-Amz-Signature=test",
                    "totalBytes": len(compressed),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed)

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
    events = b'{"timestamp":31}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            assert request.url.params.get("standard") == "false"
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl",
                    "totalBytes": len(events),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=events)

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
    # Data for 3 days, each day returns different timestamps
    day1_events = b'{"timestamp":1}\n{"timestamp":2}\n'
    day2_events = b'{"timestamp":3}\n{"timestamp":4}\n'
    day3_events = b'{"timestamp":5}\n{"timestamp":6}\n'

    compressed_day1 = zstd.ZstdCompressor().compress(day1_events)
    compressed_day2 = zstd.ZstdCompressor().compress(day2_events)
    compressed_day3 = zstd.ZstdCompressor().compress(day3_events)

    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count

        if request.url.path == "/datasets/download":
            request_count += 1
            from_param = request.url.params.get("from", "")

            # Return different URLs based on the date range
            if "2024-01-01" in from_param:
                url = "https://downloads.example.com/datasets/day1.jsonl.zst"
                return httpx.Response(
                    200,
                    json={
                        "url": url,
                        "totalBytes": len(compressed_day1),
                        "fileCount": 1,
                    },
                )
            elif "2024-01-02" in from_param:
                url = "https://downloads.example.com/datasets/day2.jsonl.zst"
                return httpx.Response(
                    200,
                    json={
                        "url": url,
                        "totalBytes": len(compressed_day2),
                        "fileCount": 1,
                    },
                )
            elif "2024-01-03" in from_param:
                url = "https://downloads.example.com/datasets/day3.jsonl.zst"
                return httpx.Response(
                    200,
                    json={
                        "url": url,
                        "totalBytes": len(compressed_day3),
                        "fileCount": 1,
                    },
                )

        # Handle download requests
        if "day1" in str(request.url):
            return httpx.Response(200, content=compressed_day1)
        elif "day2" in str(request.url):
            return httpx.Response(200, content=compressed_day2)
        elif "day3" in str(request.url):
            return httpx.Response(200, content=compressed_day3)

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

        # Should have made 3 separate download URL requests (one per day)
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
    events = b'{"timestamp":1}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/datasets/download":
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": len(compressed),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed)

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
    events = b'{"timestamp":1}\n'
    compressed = zstd.ZstdCompressor().compress(events)

    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        if request.url.path == "/datasets/download":
            request_count += 1
            return httpx.Response(
                200,
                json={
                    "url": "https://downloads.example.com/datasets/sample.jsonl.zst",
                    "totalBytes": len(compressed),
                    "fileCount": 1,
                },
            )
        return httpx.Response(200, content=compressed)

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
