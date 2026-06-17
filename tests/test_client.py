from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest
import zstandard as zstd

from polaris_data import PolarisClient
from polaris_data.errors import RateLimitedError, StreamDecodeError, UnauthorizedError

SNAPSHOT_KEY_DAY_1 = "snapshots/standard/binance/BTC-USDT/2024-01-01.jsonl.zst"
SNAPSHOT_KEY_DAY_2 = "snapshots/standard/binance/BTC-USDT/2024-01-02.jsonl.zst"


def _zstd_ndjson(rows: list[dict]) -> bytes:
    ndjson = b"".join(
        f"{json.dumps(row, separators=(',', ':'), ensure_ascii=True)}\n".encode("utf-8")
        for row in rows
    )
    return zstd.ZstdCompressor().compress(ndjson)


def make_client(
    handler,
    *,
    api_key: str | None = "polaris_key_test",
    dataset_root: Path | None = None,
    replay_cache_enabled: bool = False,
    replay_cache_dir: Path | None = None,
) -> PolarisClient:
    transport = httpx.MockTransport(handler)
    return PolarisClient(
        api_key=api_key,
        transport=transport,
        dataset_root=dataset_root,
        replay_cache_enabled=replay_cache_enabled,
        replay_cache_dir=replay_cache_dir,
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
        assert request.headers.get("authorization") == "Bearer polaris_key_test"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        assert client.catalog() == payload
    finally:
        client.close()


def test_unauthorized_raw_requires_api_key_before_request() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = make_client(handler, api_key=None)
    try:
        with pytest.raises(UnauthorizedError):
            client.raw(
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


def test_trades_use_snapshot_download_flow_by_default(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1}]},
            )
        if request.url.path == "/snapshots/download":
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/day-1-trades.jsonl.zst"},
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067200000000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 0.5},
                        },
                        {
                            "timestamp": 1704067201000000,
                            "type": "datapoint",
                            "data": {"funding": 0.01},
                        },
                        {
                            "timestamp": 1704067202000000,
                            "type": "trade",
                            "data": {"price": 101.0, "quantity": 0.25},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.trades(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [
            {
                "timestamp": 1704067200000000,
                "type": "trade",
                "data": {"price": 100.0, "quantity": 0.5},
            },
            {
                "timestamp": 1704067202000000,
                "type": "trade",
                "data": {"price": 101.0, "quantity": 0.25},
            },
        ]
    finally:
        client.close()


def test_trades_fall_back_to_events_when_snapshot_coverage_is_incomplete() -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            assert request.url.params.get("format") == "file"
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {"timestamp": 2, "type": "bar", "data": {"close": 100.5}},
                        {
                            "timestamp": 3,
                            "type": "trade",
                            "data": {"price": 101.0, "quantity": 2.0},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler)
    try:
        assert client.trades(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [
            {"timestamp": 1, "type": "trade", "data": {"price": 100.0, "quantity": 1.0}},
            {"timestamp": 3, "type": "trade", "data": {"price": 101.0, "quantity": 2.0}},
        ]
        assert calls == [("/snapshots", None), ("/events", "file")]
    finally:
        client.close()


def test_ohlcv_aggregates_from_snapshot_download_flow(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1}]})
        if request.url.path == "/snapshots/download":
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/day-1-ohlcv.jsonl.zst"},
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067205000000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": 1704067201000000,
                            "type": "trade",
                            "data": {"price": 95.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": 1704067240000000,
                            "type": "trade",
                            "data": {"price": 105.0, "quantity": 3.0},
                        },
                        {
                            "timestamp": 1704067260000000,
                            "type": "trade",
                            "data": {"price": 103.0, "quantity": 4.0},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.ohlcv(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T00:02:00Z",
            interval="1m",
        ) == [
            {
                "timestamp": 1704067200000000,
                "open": 95.0,
                "high": 105.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 6.0,
                "trades": 3,
                "interval": "1m",
            },
            {
                "timestamp": 1704067260000000,
                "open": 105.0,
                "high": 103.0,
                "low": 103.0,
                "close": 103.0,
                "volume": 4.0,
                "trades": 1,
                "interval": "1m",
            },
        ]
    finally:
        client.close()


def test_ohlcv_tradingview_format_returns_local_json(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1}]})
        if request.url.path == "/snapshots/download":
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/day-1-tv.jsonl.zst"},
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067200000000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.5},
                        }
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.ohlcv(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T00:01:00Z",
            interval="1m",
            format="tradingview",
        ) == {
            "candles": [
                {"time": 1704067200.0, "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}
            ],
            "volumes": [{"time": 1704067200.0, "value": 1.5}],
        }
    finally:
        client.close()


def test_ohlcv_rejects_stale_parquet_format() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(ValueError, match="format must be one of: None, 'tradingview'"):
            client.ohlcv(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T00:01:00Z",
                interval="1m",
                format="parquet",
            )
    finally:
        client.close()


def test_list_snapshots_paginates_across_data_and_snapshots_fields() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/snapshots"
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "data": [{"key": SNAPSHOT_KEY_DAY_1}],
                    "next_cursor": "page-2",
                },
            )
        assert cursor == "page-2"
        return httpx.Response(
            200,
            json={
                "snapshots": [{"key": SNAPSHOT_KEY_DAY_2, "filename": "2024-01-02.jsonl.zst"}],
                "next_cursor": None,
            },
        )

    client = make_client(handler)
    try:
        snapshots = client.list_snapshots(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-03T00:00:00Z",
        )
        assert [snapshot.key for snapshot in snapshots] == [
            SNAPSHOT_KEY_DAY_1,
            SNAPSHOT_KEY_DAY_2,
        ]
        assert [snapshot.filename for snapshot in snapshots] == [
            "2024-01-01.jsonl.zst",
            "2024-01-02.jsonl.zst",
        ]
    finally:
        client.close()


def test_download_snapshots_saves_files_and_materializes_daily_artifacts(tmp_path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/snapshots":
            assert request.headers.get("authorization") == "Bearer polaris_key_test"
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1}]},
            )
        if request.url.path == "/snapshots/download":
            assert request.url.params.get("key") == SNAPSHOT_KEY_DAY_1
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/day-1.jsonl.zst"},
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson([{"timestamp": 1}, {"timestamp": 2}]),
                headers={"content-type": "application/zstd"},
            )
        return httpx.Response(404)

    client = make_client(handler, dataset_root=tmp_path)
    try:
        entries = client.download_snapshots(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        )
        assert [entry.key for entry in entries] == [SNAPSHOT_KEY_DAY_1]
        assert (tmp_path / "data" / SNAPSHOT_KEY_DAY_1).exists()
        assert (tmp_path / "daily" / "binance" / "BTC-USDT" / "2024-01-01.jsonl.zst").exists()
        assert len(calls) == 3
    finally:
        client.close()


def test_list_local_snapshots_filters_by_exchange_asset_and_date(tmp_path) -> None:
    client = make_client(lambda request: httpx.Response(500), dataset_root=tmp_path)
    try:
        first = client.layout.data_path_for_key(SNAPSHOT_KEY_DAY_1)
        second = client.layout.data_path_for_key(
            "snapshots/standard/binance/ETH-USDT/2024-01-01.jsonl.zst"
        )
        first.parent.mkdir(parents=True, exist_ok=True)
        second.parent.mkdir(parents=True, exist_ok=True)
        first.write_bytes(_zstd_ndjson([{"timestamp": 1}]))
        second.write_bytes(_zstd_ndjson([{"timestamp": 2}]))

        assert len(client.list_local_snapshots()) == 2
        assert len(client.list_local_snapshots(exchange="binance", asset="BTC-USDT")) == 1
        assert len(client.list_local_snapshots(date="2024-01-01")) == 2
    finally:
        client.close()


def test_iter_local_events_filters_across_materialized_days(tmp_path) -> None:
    client = make_client(lambda request: httpx.Response(500), dataset_root=tmp_path)
    try:
        day_one = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 1),
        )
        day_two = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 2),
        )
        day_one.parent.mkdir(parents=True, exist_ok=True)
        day_two.parent.mkdir(parents=True, exist_ok=True)
        day_one.write_bytes(
            _zstd_ndjson(
                [
                    {"timestamp": 1704067200000000},
                    {"timestamp": 1704110400000000},
                ]
            )
        )
        day_two.write_bytes(_zstd_ndjson([{"timestamp": 1704153600000000}]))

        rows = list(
            client.iter_local_events(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T12:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1704110400000000}]
    finally:
        client.close()


def test_replay_reads_local_snapshot_day_files_before_network(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network should not be called when local daily files exist")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        daily_path = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 1),
        )
        daily_path.parent.mkdir(parents=True, exist_ok=True)
        daily_path.write_bytes(
            _zstd_ndjson(
                [
                    {"timestamp": 1704067200000000},
                    {"timestamp": 1704067260000000},
                ]
            )
        )

        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [
            {"timestamp": 1704067200000000},
            {"timestamp": 1704067260000000},
        ]
    finally:
        client.close()


def test_replay_uses_direct_daily_paths_without_scanning_tree(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network should not be called when local daily files exist")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        daily_path = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 1),
        )
        daily_path.parent.mkdir(parents=True, exist_ok=True)
        daily_path.write_bytes(_zstd_ndjson([{"timestamp": 1704067200000000}]))

        def fail_scan():
            raise AssertionError("daily tree scan should not be used for known replay days")

        client.layout.list_local_daily_artifacts = fail_scan  # type: ignore[method-assign]

        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1704067200000000}]
    finally:
        client.close()


def test_iter_local_events_stops_after_to_boundary_on_ordered_day_files(tmp_path) -> None:
    client = make_client(lambda request: httpx.Response(500), dataset_root=tmp_path)
    try:
        day_one = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 1),
        )
        day_two = client.layout.daily_path_for_dataset_day(
            "binance",
            "BTC-USDT",
            date(2024, 1, 2),
        )
        day_one.parent.mkdir(parents=True, exist_ok=True)
        day_two.parent.mkdir(parents=True, exist_ok=True)
        day_one.write_bytes(_zstd_ndjson([{"timestamp": 1704110400000000}]))
        day_two.write_bytes(
            zstd.ZstdCompressor().compress(
                b'{"timestamp":1704175200000000}\nnot-json\n'
            )
        )

        rows = list(
            client.iter_local_events(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T12:00:00Z",
                to="2024-01-02T06:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1704110400000000}]
    finally:
        client.close()


def test_events_use_snapshot_download_flow_by_default(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1}]})
        if request.url.path == "/snapshots/download":
            return httpx.Response(
                302,
                headers={"location": "https://download.example.com/day-1.jsonl.zst"},
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {"timestamp": 1704067200000000},
                        {"timestamp": 1704067260000000},
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.events(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        ) == [
            {"timestamp": 1704067200000000},
            {"timestamp": 1704067260000000},
        ]
    finally:
        client.close()


def test_replay_falls_back_to_events_when_snapshot_coverage_is_incomplete(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            assert request.url.params.get("format") == "file"
            return httpx.Response(
                200,
                content=_zstd_ndjson([{"timestamp": 21}, {"timestamp": 22}]),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path, replay_cache_enabled=False)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [{"timestamp": 21}, {"timestamp": 22}]
        assert calls == [("/snapshots", None), ("/events", "file")]
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
        assert client.raw(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        ) == [{"exchange_payload": {"id": 10}}, {"exchange_payload": {"id": 11}}]
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
        assert client.raw(
            exchange="binance",
            asset="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [{"exchange_payload": {"id": 42}}]
    finally:
        client.close()


def test_raw_replay_raises_stream_decode_error_for_invalid_cached_zstd(tmp_path) -> None:
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
            standard=False,
        )
        (tmp_path / cache_name).with_suffix("").write_bytes(b"not-zs")

        with pytest.raises(StreamDecodeError):
            list(
                client.replay(
                    exchange="binance",
                    asset="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T01:00:00Z",
                    standard=False,
                )
            )
        assert called is False
    finally:
        client.close()


def test_raw_replay_reads_cached_rows_without_api_call(tmp_path) -> None:
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
            standard=False,
        )
        (tmp_path / cache_name).with_suffix("").write_bytes(b'{"timestamp":99}\n')

        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        )
        assert rows == [{"timestamp": 99}]
        assert called is False
    finally:
        client.close()


def test_raw_replay_populates_cache_and_reuses_on_new_client(tmp_path) -> None:
    def online_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 31}, {"timestamp": 32}]),
            headers={"content-type": "application/zstd"},
        )

    online_client = PolarisClient(
        api_key="pk_live_test",
        transport=httpx.MockTransport(online_handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        assert list(
            online_client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        ) == [{"timestamp": 31}, {"timestamp": 32}]
    finally:
        online_client.close()

    def offline_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network should not be called when replay cache already exists")

    offline_client = PolarisClient(
        api_key=None,
        transport=httpx.MockTransport(offline_handler),
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        assert list(
            offline_client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        ) == [{"timestamp": 31}, {"timestamp": 32}]
    finally:
        offline_client.close()


def test_replay_allows_standard_false_for_raw() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params.get("format") == "file"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 41}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        assert list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                standard=False,
            )
        ) == [{"timestamp": 41}]
    finally:
        client.close()


def test_replay_parallel_keeps_legacy_raw_chunking_behavior() -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        assert request.url.path == "/raw"
        request_count += 1
        from_param = request.url.params.get("from", "")
        if "2024-01-01" in from_param:
            rows = [{"timestamp": 1}, {"timestamp": 2}]
        elif "2024-01-02" in from_param:
            rows = [{"timestamp": 3}, {"timestamp": 4}]
        else:
            rows = [{"timestamp": 5}, {"timestamp": 6}]
        return httpx.Response(
            200,
            content=_zstd_ndjson(rows),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler, replay_cache_enabled=False)
    try:
        rows = list(
            client.replay(
                exchange="binance",
                asset="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-04T00:00:00Z",
                standard=False,
                parallel=True,
            )
        )
        assert request_count == 3
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
