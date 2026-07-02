from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest
import zstandard as zstd

from polaris_data import PolarisClient
from polaris_data.errors import (
    AccessDeniedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)

SNAPSHOT_KEY_DAY_1 = "standard-binance-BTC-USDT-2024-01-01"
SNAPSHOT_KEY_DAY_2 = "standard-binance-BTC-USDT-2024-01-02"
LEGACY_SNAPSHOT_KEY_DAY_1 = "standard-binance-BTC-USDT-2024-01-01"


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


def _catalog_payload(
    *,
    source: str = "binance",
    market: str = "BTC-USDT",
    start: str,
    end: str,
    access_status: str = "open",
    public_cutoff_date: str | None = None,
    flattened: bool = True,
) -> dict:
    access: dict[str, str] = {"status": access_status}
    if public_cutoff_date is not None:
        access["public_cutoff_date"] = public_cutoff_date

    market_entry = {
        "source": source,
        "market": market,
        "start": start,
        "end": end,
        "source_type": "manifest",
        "categories": ["perp"],
        "access": access,
    }

    if flattened:
        return {
            "markets": [market_entry],
            "updatedAt": "2026-05-19T10:28:00.000Z",
        }

    return {
        "sources": [
            {
                "id": source,
                "markets": [
                    {
                        "id": market,
                        "start": start,
                        "end": end,
                        "source": "manifest",
                        "categories": ["perp"],
                        "access": access,
                    }
                ],
            }
        ],
        "updatedAt": "2026-05-19T10:28:00.000Z",
    }


def _ts(iso8601: str) -> int:
    return int(
        datetime.fromisoformat(iso8601.replace("Z", "+00:00")).timestamp()
        * 1_000_000
    )


def _hourly_snapshot_key(source: str, market: str, day: str, hour: int) -> str:
    return f"standard-{source}-{market}-{day}-{hour:02d}"


def _partial_snapshot_handler(calls: list[tuple[str, str | None]]):
    snapshot_rows = {
        _hourly_snapshot_key("binance", "BTC-USDT", "2024-01-01", 0): [
            {
                "timestamp": _ts("2024-01-01T00:00:01Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            },
            {
                "timestamp": _ts("2024-01-01T00:00:20Z"),
                "type": "trade",
                "data": {"price": 105.0, "quantity": 2.0},
            },
        ],
        _hourly_snapshot_key("binance", "BTC-USDT", "2024-01-01", 2): [
            {
                "timestamp": _ts("2024-01-01T02:00:05Z"),
                "type": "trade",
                "data": {"price": 110.0, "quantity": 3.0},
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={
                    "snapshots": [
                        {"key": key, "date": "2024-01-01", "hour": hour}
                        for key, hour in (
                            (_hourly_snapshot_key("binance", "BTC-USDT", "2024-01-01", 0), 0),
                            (_hourly_snapshot_key("binance", "BTC-USDT", "2024-01-01", 2), 2),
                        )
                    ]
                },
            )
        if request.url.path == "/download":
            key = request.url.params.get("key")
            assert key in snapshot_rows
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows[key]),
                headers={"content-type": "application/zstd"},
            )
        if request.url.path == "/events":
            raise AssertionError("standardized readers should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    return handler


def test_catalog_returns_payload() -> None:
    payload = {
        "markets": [
            {"source": "binance", "market": "BTC-USDT"},
            {"source": "hyperliquid", "market": "BTC"},
            {"source": "hyperliquid", "market": "ETH"},
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


def test_raw_infers_last_7_days_from_catalog_for_open_dataset() -> None:
    rows = [{"timestamp": _ts("2024-01-09T12:00:00Z"), "payload": "ok"}]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog":
            assert request.url.params.get("source") == "binance"
            assert request.url.params.get("market") == "BTC-USDT"
            return httpx.Response(
                200,
                json=_catalog_payload(
                    start="2024-01-01T00:00:00Z",
                    end="2024-01-10T00:00:00Z",
                ),
            )
        if request.url.path == "/raw":
            assert request.url.params.get("from") == "2024-01-03T00:00:00Z"
            assert request.url.params.get("to") == "2024-01-10T00:00:00Z"
            assert request.url.params.get("format") == "file"
            return httpx.Response(
                200,
                content=_zstd_ndjson(rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler)
    try:
        assert client.raw(source="binance", market="BTC-USDT") == rows
    finally:
        client.close()


def test_raw_infers_bounded_range_when_dataset_is_shorter_than_7_days() -> None:
    rows = [{"timestamp": _ts("2024-01-09T12:00:00Z"), "payload": "ok"}]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog":
            return httpx.Response(
                200,
                json=_catalog_payload(
                    start="2024-01-08T00:00:00Z",
                    end="2024-01-10T00:00:00Z",
                ),
            )
        if request.url.path == "/raw":
            assert request.url.params.get("from") == "2024-01-08T00:00:00Z"
            assert request.url.params.get("to") == "2024-01-10T00:00:00Z"
            return httpx.Response(
                200,
                content=_zstd_ndjson(rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler)
    try:
        assert client.raw(source="binance", market="BTC-USDT") == rows
    finally:
        client.close()


def test_events_infer_preview_cutoff_window_without_api_key(tmp_path) -> None:
    snapshot_dates = [f"2024-01-{day:02d}" for day in range(9, 16)]
    snapshot_keys = {
        f"standard-binance-BTC-USDT-{date_text}": date_text for date_text in snapshot_dates
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog":
            assert request.headers.get("authorization") is None
            return httpx.Response(
                200,
                json=_catalog_payload(
                    start="2024-01-01T00:00:00Z",
                    end="2024-01-20T00:00:00Z",
                    access_status="preview",
                    public_cutoff_date="2024-01-15",
                ),
            )
        if request.url.path == "/snapshots":
            assert request.url.params.get("from") == "2024-01-09T00:00:00Z"
            assert request.url.params.get("to") == "2024-01-16T00:00:00Z"
            return httpx.Response(
                200,
                json={
                    "snapshots": [
                        {"key": key, "date": date_text}
                        for key, date_text in snapshot_keys.items()
                    ],
                    "access": {
                        "status": "preview",
                        "public_cutoff_date": "2024-01-15",
                    },
                },
            )
        if request.url.path == "/download":
            key = request.url.params.get("key")
            assert key in snapshot_keys
            date_text = snapshot_keys[key]
            row = {
                "timestamp": _ts(f"{date_text}T12:00:00Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            }
            return httpx.Response(
                200,
                content=_zstd_ndjson([row]),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, api_key=None, dataset_root=tmp_path)
    try:
        rows = client.events(source="binance", market="BTC-USDT")
        assert [row["timestamp"] for row in rows] == [
            _ts(f"{date_text}T12:00:00Z") for date_text in snapshot_dates
        ]
    finally:
        client.close()


def test_raw_rejects_legacy_catalog_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog":
            return httpx.Response(
                200,
                json=_catalog_payload(
                    start="2024-01-01T00:00:00Z",
                    end="2024-01-10T00:00:00Z",
                    flattened=False,
                ),
            )
        if request.url.path == "/raw":
            raise AssertionError("raw endpoint should not be called for legacy catalog shape")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler)
    try:
        with pytest.raises(
            PolarisError,
            match="Catalog response did not include market metadata needed",
        ):
            client.raw(source="binance", market="BTC-USDT")
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
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
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
            source="binance",
            market="BTC-USDT",
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


def test_trades_download_404_raises_not_found_for_streaming_response(tmp_path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(404, text="snapshot missing")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(NotFoundError, match="snapshot missing"):
            client.trades(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        assert calls == ["/snapshots", "/download"]
    finally:
        client.close()


def test_trades_download_hourly_snapshots_for_partial_day_ranges(tmp_path) -> None:
    snapshot_rows = {
        "standard-binance-BTC-USDT-2024-01-01-00": [
            {
                "timestamp": _ts("2024-01-01T00:00:01Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            }
        ],
        "standard-binance-BTC-USDT-2024-01-01-01": [
            {
                "timestamp": _ts("2024-01-01T01:00:01Z"),
                "type": "trade",
                "data": {"price": 101.0, "quantity": 2.0},
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={
                    "snapshots": [
                        {
                            "key": key,
                            "date": "2024-01-01",
                            "hour": hour,
                        }
                        for hour, key in enumerate(snapshot_rows)
                    ]
                },
            )
        if request.url.path == "/download":
            key = request.url.params.get("key")
            assert key in snapshot_rows
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows[key]),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.trades(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T02:00:00Z",
        ) == [
            {
                "timestamp": _ts("2024-01-01T00:00:01Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            },
            {
                "timestamp": _ts("2024-01-01T01:00:01Z"),
                "type": "trade",
                "data": {"price": 101.0, "quantity": 2.0},
            },
        ]
    finally:
        client.close()


def test_trades_require_snapshot_coverage_and_do_not_fall_back_to_events() -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("trades() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.trades(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_trades_allow_gaps_returns_covered_rows_and_warns(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(_partial_snapshot_handler(calls), dataset_root=tmp_path)
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            rows = client.trades(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T03:00:00Z",
                allow_gaps=True,
            )
        assert [row["timestamp"] for row in rows] == [
            _ts("2024-01-01T00:00:01Z"),
            _ts("2024-01-01T00:00:20Z"),
            _ts("2024-01-01T02:00:05Z"),
        ]
        assert calls == [
            ("/snapshots", None),
            ("/download", None),
            ("/download", None),
        ]
    finally:
        client.close()


def test_ohlcv_aggregates_from_snapshot_download_flow(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
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
            source="binance",
            market="BTC-USDT",
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
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
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
            source="binance",
            market="BTC-USDT",
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


def test_ohlcv_require_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("ohlcv() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.ohlcv(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T00:02:00Z",
                interval="1m",
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_ohlcv_allow_gaps_skips_missing_hours_and_preserves_gap_open(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(_partial_snapshot_handler(calls), dataset_root=tmp_path)
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            bars = client.ohlcv(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T03:00:00Z",
                interval="1h",
                allow_gaps=True,
            )
        assert bars == [
            {
                "timestamp": _ts("2024-01-01T00:00:00Z"),
                "open": 100.0,
                "high": 105.0,
                "low": 100.0,
                "close": 105.0,
                "volume": 3.0,
                "trades": 2,
                "interval": "1h",
            },
            {
                "timestamp": _ts("2024-01-01T02:00:00Z"),
                "open": 110.0,
                "high": 110.0,
                "low": 110.0,
                "close": 110.0,
                "volume": 3.0,
                "trades": 1,
                "interval": "1h",
            },
        ]
        assert calls == [
            ("/snapshots", None),
            ("/download", None),
            ("/download", None),
        ]
    finally:
        client.close()


def test_ohlcv_rejects_stale_parquet_format() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(ValueError, match="format must be one of: None, 'tradingview'"):
            client.ohlcv(
                source="binance",
                market="BTC-USDT",
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
        assert request.url.params.get("source") == "binance"
        assert request.url.params.get("market") == "BTC-USDT"
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "data": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
                    "next_cursor": "page-2",
                },
            )
        assert cursor == "page-2"
        return httpx.Response(
            200,
            json={
                "snapshots": [{"key": SNAPSHOT_KEY_DAY_2, "date": "2024-01-02"}],
                "next_cursor": None,
            },
        )

    client = make_client(handler)
    try:
        snapshots = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-03T00:00:00Z",
        )
        assert [snapshot.key for snapshot in snapshots] == [
            SNAPSHOT_KEY_DAY_1,
            SNAPSHOT_KEY_DAY_2,
        ]
    finally:
        client.close()


def test__download_snapshots_saves_files_and_materializes_daily_artifacts(tmp_path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/snapshots":
            assert request.headers.get("authorization") == "Bearer polaris_key_test"
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
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
        entries = client._download_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        )
        assert [entry.key for entry in entries] == [SNAPSHOT_KEY_DAY_1]
        assert (
            tmp_path
            / "data"
            / "standard"
            / "binance"
            / "BTC-USDT"
            / "2024-01-01"
            / f"{SNAPSHOT_KEY_DAY_1}.jsonl.zst"
        ).exists()
        assert (tmp_path / "daily" / "binance" / "BTC-USDT" / "2024-01-01.jsonl.zst").exists()
        assert len(calls) == 3
    finally:
        client.close()


def test__list_local_snapshots_filters_by_date(tmp_path) -> None:
    client = make_client(lambda request: httpx.Response(500), dataset_root=tmp_path)
    try:
        first = client.layout.data_path_for_key(SNAPSHOT_KEY_DAY_1)
        second = client.layout.data_path_for_key(
            "standard-binance-ETH-USDT-2024-01-01"
        )
        first.parent.mkdir(parents=True, exist_ok=True)
        second.parent.mkdir(parents=True, exist_ok=True)
        first.write_bytes(_zstd_ndjson([{"timestamp": 1}]))
        second.write_bytes(_zstd_ndjson([{"timestamp": 2}]))

        assert len(client._list_local_snapshots()) == 2
        assert len(client._list_local_snapshots(date="2024-01-01")) == 2
    finally:
        client.close()


def test_replay_materializes_local_snapshot_data_files_before_network(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network should not be called when local snapshot files exist")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        snapshot_path = client.layout.data_path_for_key(SNAPSHOT_KEY_DAY_1)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_bytes(
            _zstd_ndjson(
                [
                    {"timestamp": 1704067200000000},
                    {"timestamp": 1704067260000000},
                ]
            )
        )

        rows = list(
            client.replay(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T00:02:00Z",
            )
        )

        assert rows == [
            {"timestamp": 1704067200000000},
            {"timestamp": 1704067260000000},
        ]
    finally:
        client.close()


def test_replay_ignores_legacy_flat_snapshot_data_files(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        snapshot_path = tmp_path / "data" / LEGACY_SNAPSHOT_KEY_DAY_1
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_bytes(
            _zstd_ndjson(
                [
                    {"timestamp": 1704067200000000},
                    {"timestamp": 1704067260000000},
                ]
            )
        )

        with pytest.raises(
            PolarisError,
            match="Requested replay range could not be satisfied from standardized snapshots",
        ):
            list(
                client.replay(
                    source="binance",
                    market="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T00:02:00Z",
                )
            )
    finally:
        client.close()


def test__iter_local_events_filters_across_materialized_days(tmp_path) -> None:
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
            client._iter_local_events(
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1704067200000000}]
    finally:
        client.close()


def test__iter_local_events_stops_after_to_boundary_on_ordered_day_files(tmp_path) -> None:
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
            client._iter_local_events(
                source="binance",
                market="BTC-USDT",
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
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
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
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        ) == [
            {"timestamp": 1704067200000000},
            {"timestamp": 1704067260000000},
        ]
    finally:
        client.close()


def test_events_require_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("events() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_events_allow_gaps_returns_covered_rows_and_warns(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(_partial_snapshot_handler(calls), dataset_root=tmp_path)
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            rows = client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T03:00:00Z",
                allow_gaps=True,
            )
        assert [row["timestamp"] for row in rows] == [
            _ts("2024-01-01T00:00:01Z"),
            _ts("2024-01-01T00:00:20Z"),
            _ts("2024-01-01T02:00:05Z"),
        ]
        assert calls == [
            ("/snapshots", None),
            ("/download", None),
            ("/download", None),
        ]
    finally:
        client.close()


def test_l2_snapshots_use_snapshot_download_flow_by_default(tmp_path) -> None:
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "l2_snapshot",
            "data": {
                "bids": [[100.0, 1.25], [99.5, 2.0]],
                "asks": [[100.5, 0.75], [101.0, 1.0]],
            },
        },
        {
            "timestamp": _ts("2024-01-01T00:00:01Z"),
            "type": "trade",
            "data": {"price": 100.25, "quantity": 0.5},
        },
        {
            "timestamp": _ts("2024-01-01T00:00:02Z"),
            "type": "l2_snapshot",
            "bids": [[100.1, 1.0]],
            "asks": [[100.6, 1.5]],
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.l2_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [snapshot_rows[0], snapshot_rows[2]]
    finally:
        client.close()


def test_bbo_derives_best_prices_and_quantities_from_l2_snapshots(tmp_path) -> None:
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "l2_snapshot",
            "data": {
                "bids": [[99.0, 2.0], [100.0, 1.25], ["98.5", "5.0"]],
                "asks": [[101.0, 3.0], [100.5, 0.75]],
            },
        },
        {
            "timestamp": _ts("2024-01-01T00:00:01Z"),
            "type": "l2_snapshot",
            "data": {
                "bids": [{"price": "100.1", "size": "1.5"}],
                "asks": [{"price": 100.4, "quantity": 0.25}],
            },
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.bbo(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [
            {
                "timestamp": _ts("2024-01-01T00:00:00Z"),
                "bid_price": 100.0,
                "bid_quantity": 1.25,
                "ask_price": 100.5,
                "ask_quantity": 0.75,
            },
            {
                "timestamp": _ts("2024-01-01T00:00:01Z"),
                "bid_price": 100.1,
                "bid_quantity": 1.5,
                "ask_price": 100.4,
                "ask_quantity": 0.25,
            },
        ]
    finally:
        client.close()


def test_replay_requires_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("replay() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path, replay_cache_enabled=False)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            list(
                client.replay(
                    source="binance",
                    market="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-02T00:00:00Z",
                )
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_replay_allow_gaps_returns_covered_rows_and_warns(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(
        _partial_snapshot_handler(calls),
        dataset_root=tmp_path,
        replay_cache_enabled=False,
    )
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            rows = list(
                client.replay(
                    source="binance",
                    market="BTC-USDT",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T03:00:00Z",
                    allow_gaps=True,
                )
            )
        assert [row["timestamp"] for row in rows] == [
            _ts("2024-01-01T00:00:01Z"),
            _ts("2024-01-01T00:00:20Z"),
            _ts("2024-01-01T02:00:05Z"),
        ]
        assert calls == [
            ("/snapshots", None),
            ("/download", None),
            ("/download", None),
        ]
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
            source="binance",
            market="BTC-USDT",
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
        assert request.url.params.get("source") == "binance"
        assert request.url.params.get("market") == "BTC-USDT"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"exchange_payload": {"id": 42}}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler)
    try:
        assert client.raw(
            source="binance",
            market="BTC-USDT",
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
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=False,
        )
        (tmp_path / cache_name).with_suffix("").write_bytes(b"not-zs")

        with pytest.raises(StreamDecodeError):
            list(
                client.replay(
                    source="binance",
                    market="BTC-USDT",
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
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=False,
        )
        (tmp_path / cache_name).with_suffix("").write_bytes(b'{"timestamp":99}\n')

        rows = list(
            client.replay(
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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
                source="binance",
                market="BTC-USDT",
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


# ---------------------------------------------------------------------------
# Access control (proactive checks in list_snapshots)
# ---------------------------------------------------------------------------


def test_access_open_allows_unauthenticated() -> None:
    payload = {
        "access": {"status": "open"},
        "snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key=None)
    try:
        result = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        )
        assert len(result) == 1
        assert result[0].key == SNAPSHOT_KEY_DAY_1
    finally:
        client.close()


def test_access_restricted_blocks_unauthenticated() -> None:
    payload = {
        "access": {"status": "restricted"},
        "snapshots": [],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key=None)
    try:
        with pytest.raises(AccessDeniedError) as exc_info:
            client.list_snapshots(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        assert "requires authentication" in str(exc_info.value)
        assert "docs.polaris.supply/guides/authentication" in str(exc_info.value)
    finally:
        client.close()


def test_access_restricted_passes_with_api_key() -> None:
    payload = {
        "access": {"status": "restricted"},
        "snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key="polaris_key_test")
    try:
        result = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        )
        assert len(result) == 1
    finally:
        client.close()


def test_access_preview_blocks_unauthenticated_past_cutoff() -> None:
    payload = {
        "access": {"status": "preview", "public_cutoff_date": "2024-01-15"},
        "snapshots": [],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key=None)
    try:
        with pytest.raises(AccessDeniedError) as exc_info:
            client.list_snapshots(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-20T00:00:00Z",
            )
        assert "2024-01-15" in str(exc_info.value)
        assert "docs.polaris.supply/guides/authentication" in str(exc_info.value)
    finally:
        client.close()


def test_access_preview_allows_unauthenticated_before_cutoff() -> None:
    payload = {
        "access": {"status": "preview", "public_cutoff_date": "2024-01-20"},
        "snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key=None)
    try:
        result = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-15T00:00:00Z",
        )
        assert len(result) == 1
    finally:
        client.close()


def test_access_preview_passes_with_api_key_past_cutoff() -> None:
    payload = {
        "access": {"status": "preview", "public_cutoff_date": "2020-01-01"},
        "snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key="polaris_key_test")
    try:
        result = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-10T00:00:00Z",
        )
        assert len(result) == 1
    finally:
        client.close()


def test_access_missing_field_is_tolerated() -> None:
    payload = {
        "snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}],
        "has_more": False,
        "next_cursor": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    client = make_client(handler, api_key=None)
    try:
        result = client.list_snapshots(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-02T00:00:00Z",
        )
        assert len(result) == 1
    finally:
        client.close()


def test_402_maps_to_access_denied_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            402,
            json={"error": "payment required"},
        )

    client = make_client(handler)
    try:
        with pytest.raises(AccessDeniedError) as exc_info:
            client.catalog()
        assert exc_info.value.status_code == 402
        assert "docs.polaris.supply/guides/authentication" in str(exc_info.value)
    finally:
        client.close()
