from __future__ import annotations

import json
import math
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
        "instrument": {
            "base": "BTC",
            "quote": "USDT",
            "tick_size": "0.1",
            "lot_size": "0.001",
            "min_notional": "10",
        },
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
        datetime.fromisoformat(iso8601.replace("Z", "+00:00")).timestamp() * 1_000
    )


def _ts_ms(iso8601: str) -> int:
    return int(
        datetime.fromisoformat(iso8601.replace("Z", "+00:00")).timestamp() * 1_000
    )


def _hourly_snapshot_key(source: str, market: str, day: str, hour: int) -> str:
    return f"standard-{source}-{market}-{day}-{hour:02d}"


def _snapshot_download_url(key: str) -> str:
    return f"https://download.example.com/{key}.jsonl.zst"


def _bulk_download_manifest(
    *,
    source: str,
    market: str,
    day: str,
    keys: list[str],
) -> dict:
    return {
        "source": source,
        "market": market,
        "date": day,
        "total": len(keys),
        "total_bytes": 0,
        "snapshots": [
            {
                "date": day,
                "timestamp": "000000",
                "key": key,
                "url": _snapshot_download_url(key),
                "expires_in_seconds": 86400,
            }
            for key in keys
        ],
    }


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
        if request.url.path in {"/snapshots", "/download"}:
            calls.append(
                (
                    request.url.path,
                    request.url.params.get("mode")
                    if request.url.path == "/download"
                    else request.url.params.get("format"),
                )
            )
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
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=sorted(snapshot_rows),
                ),
            )
        if request.url.host == "download.example.com":
            key = request.url.path.removeprefix("/").removesuffix(".jsonl.zst")
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
            {
                "source": "binance",
                "market": "BTC-USDT",
                "instrument": {
                    "base": "BTC",
                    "quote": "USDT",
                    "tick_size": "0.1",
                    "lot_size": "0.001",
                    "min_notional": "10",
                },
            },
            {"source": "hyperliquid", "market": "BTC"},
            {"source": "hyperliquid", "market": "ETH"},
        ],
        "updatedAt": "2026-05-19T10:28:00.000Z",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        assert request.headers.get("authorization") == "Bearer polaris_key_test"
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        result = client.catalog()
        assert result["markets"][0]["instrument"]["base"] == "BTC"
        assert result["markets"][0]["instrument"]["tick_size"] == "0.1"
        assert result["markets"][1]["instrument"] == {
            "base": None,
            "quote": None,
            "tick_size": None,
            "lot_size": None,
            "min_notional": None,
        }
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
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day=request.url.params["date"],
                    keys=[
                        key
                        for key, date_text in snapshot_keys.items()
                        if date_text == request.url.params["date"]
                    ],
                ),
            )
        if request.url.host == "download.example.com":
            key = request.url.path.removeprefix("/").removesuffix(".jsonl.zst")
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


def test_rate_limited_error_maps_reset_at(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"error": "quota exceeded", "reset_at": "2026-05-01T00:00:00.000Z"},
        )

    client = make_client(handler, dataset_root=tmp_path)
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
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067200000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 0.5},
                        },
                        {
                            "timestamp": 1704067201000,
                            "type": "datapoint",
                            "data": {"funding": 0.01},
                        },
                        {
                            "timestamp": 1704067202000,
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
                "timestamp": 1704067200000,
                "type": "trade",
                "data": {"price": 100.0, "quantity": 0.5},
            },
            {
                "timestamp": 1704067202000,
                "type": "trade",
                "data": {"price": 101.0, "quantity": 0.25},
            },
        ]
    finally:
        client.close()


def test_trades_download_404_raises_not_found_for_streaming_response(tmp_path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path in {"/snapshots", "/download"}:
            calls.append(request.url.path)
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
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
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=sorted(snapshot_rows),
                ),
            )
        if request.url.host == "download.example.com":
            key = request.url.path.removeprefix("/").removesuffix(".jsonl.zst")
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


def test_trades_select_intraday_snapshot_keys_without_hour_metadata(tmp_path) -> None:
    snapshot_rows = {
        "standard-hyperliquid-BTC-2026-07-11-010000": [
            {
                "timestamp": _ts("2026-07-11T01:05:01Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            }
        ],
        "standard-hyperliquid-BTC-2026-07-11-011000": [
            {
                "timestamp": _ts("2026-07-11T01:10:05Z"),
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
                        {"key": key, "date": "2026-07-11"} for key in snapshot_rows
                    ]
                },
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="hyperliquid",
                    market="BTC",
                    day="2026-07-11",
                    keys=sorted(snapshot_rows),
                ),
            )
        if request.url.host == "download.example.com":
            key = request.url.path.removeprefix("/").removesuffix(".jsonl.zst")
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
            source="hyperliquid",
            market="BTC",
            from_="2026-07-11T01:05:00Z",
            to="2026-07-11T01:15:00Z",
        ) == [
            {
                "timestamp": _ts("2026-07-11T01:05:01Z"),
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            },
            {
                "timestamp": _ts("2026-07-11T01:10:05Z"),
                "type": "trade",
                "data": {"price": 101.0, "quantity": 2.0},
            },
        ]
    finally:
        client.close()


def test_trades_require_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("trades() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
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
            ("/download", "json"),
        ]
    finally:
        client.close()


def test_vwap_aggregates_from_snapshot_download_flow(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": _ts("2024-01-01T00:00:00Z"),
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 10.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:01Z"),
                            "type": "trade",
                            "data": {"price": 101.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:02Z"),
                            "type": "trade",
                            "data": {"price": 99.0, "quantity": 50.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:03Z"),
                            "type": "datapoint",
                            "data": {"funding": 0.01},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.vwap(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            interval="1m",
        ) == pytest.approx(
            [
                {
                    "timestamp": _ts("2024-01-01T00:00:00Z"),
                    "vwap": 6152.0 / 62.0,
                    "volume": 62.0,
                    "quote_volume": 6152.0,
                    "trades": 3,
                }
            ]
        )
    finally:
        client.close()


def test_vwap_require_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("vwap() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.vwap(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                interval="1m",
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_vwap_validates_interval() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(
            ValueError,
            match="interval must be one of:",
        ):
            client.vwap(
                source="binance",
                market="BTC-USDT",
                interval="2m",
            )
    finally:
        client.close()


def test_vwap_allow_gaps_returns_covered_rows_and_warns(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(_partial_snapshot_handler(calls), dataset_root=tmp_path)
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            row = client.vwap(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T03:00:00Z",
                interval="1h",
                allow_gaps=True,
            )
        assert row == pytest.approx(
            [
                {
                    "timestamp": _ts("2024-01-01T00:00:00Z"),
                    "vwap": 310.0 / 3.0,
                    "volume": 3.0,
                    "quote_volume": 310.0,
                    "trades": 2,
                },
                {
                    "timestamp": _ts("2024-01-01T02:00:00Z"),
                    "vwap": 110.0,
                    "volume": 3.0,
                    "quote_volume": 330.0,
                    "trades": 1,
                },
            ]
        )
        assert calls == [
            ("/snapshots", None),
            ("/download", "json"),
        ]
    finally:
        client.close()


def test_vwap_buckets_trades_by_interval(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": _ts("2024-01-01T00:00:10Z"),
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:20Z"),
                            "type": "trade",
                            "data": {"price": 102.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:01:05Z"),
                            "type": "trade",
                            "data": {"price": 99.0, "quantity": 4.0},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.vwap(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T00:02:00Z",
            interval="1m",
        ) == pytest.approx(
            [
                {
                    "timestamp": _ts("2024-01-01T00:00:00Z"),
                    "vwap": 304.0 / 3.0,
                    "volume": 3.0,
                    "quote_volume": 304.0,
                    "trades": 2,
                },
                {
                    "timestamp": _ts("2024-01-01T00:01:00Z"),
                    "vwap": 99.0,
                    "volume": 4.0,
                    "quote_volume": 396.0,
                    "trades": 1,
                },
            ]
        )
    finally:
        client.close()


def test_volatility_aggregates_log_return_stddev_by_bucket(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": _ts("2024-01-01T00:00:20Z"),
                            "type": "trade",
                            "data": {"price": 102.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:10Z"),
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:00:40Z"),
                            "type": "trade",
                            "data": {"price": 104.0, "quantity": 3.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:01:05Z"),
                            "type": "trade",
                            "data": {"price": 99.0, "quantity": 4.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:01:25Z"),
                            "type": "trade",
                            "data": {"price": 101.0, "quantity": 5.0},
                        },
                        {
                            "timestamp": _ts("2024-01-01T00:02:10Z"),
                            "type": "trade",
                            "data": {"price": 110.0, "quantity": 6.0},
                        },
                    ]
                ),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = client.volatility(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T00:03:00Z",
            interval="1m",
        )
        assert rows == [
            {
                "timestamp": _ts("2024-01-01T00:00:00Z"),
                "volatility": rows[0]["volatility"],
                "returns": 2,
            }
        ]
        assert rows[0]["volatility"] == pytest.approx(
            abs(math.log(1.02) - math.log(104.0 / 102.0)) / math.sqrt(2.0)
        )
    finally:
        client.close()


def test_volatility_require_snapshot_coverage_and_do_not_fall_back_to_events(
    tmp_path,
) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("volatility() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.volatility(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                interval="1m",
            )
        assert calls == [("/snapshots", None)]
    finally:
        client.close()


def test_volatility_validates_interval_and_method() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(
            ValueError,
            match="interval must be one of:",
        ):
            client.volatility(
                source="binance",
                market="BTC-USDT",
                interval="2m",
            )

        with pytest.raises(ValueError, match="method must be 'log_returns'"):
            client.volatility(
                source="binance",
                market="BTC-USDT",
                interval="1m",
                method="simple_returns",
            )
    finally:
        client.close()


def test_volatility_allow_gaps_returns_covered_rows_and_warns(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []
    client = make_client(_partial_snapshot_handler(calls), dataset_root=tmp_path)
    try:
        with pytest.warns(
            UserWarning,
            match="skipped missing intervals: 2024-01-01T01:00:00Z..2024-01-01T02:00:00Z",
        ):
            rows = client.volatility(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T03:00:00Z",
                interval="1h",
                allow_gaps=True,
            )
        assert rows == []
        assert calls == [
            ("/snapshots", None),
            ("/download", "json"),
        ]
    finally:
        client.close()


def test_ohlcv_aggregates_from_snapshot_download_flow(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067205000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": 1704067201000,
                            "type": "trade",
                            "data": {"price": 95.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": 1704067240000,
                            "type": "trade",
                            "data": {"price": 105.0, "quantity": 3.0},
                        },
                        {
                            "timestamp": 1704067260000,
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
                "timestamp": 1704067200000,
                "open": 95.0,
                "high": 105.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 6.0,
                "trades": 3,
            },
            {
                "timestamp": 1704067260000,
                "open": 105.0,
                "high": 103.0,
                "low": 103.0,
                "close": 103.0,
                "volume": 4.0,
                "trades": 1,
            },
        ]
    finally:
        client.close()


def test_ohlcv_normalizes_millisecond_snapshot_timestamps(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": _ts_ms("2024-01-01T00:00:01Z"),
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": _ts_ms("2024-01-01T00:00:20Z"),
                            "type": "trade",
                            "data": {"price": 105.0, "quantity": 2.0},
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
            to="2024-01-01T00:01:00Z",
            interval="1m",
        ) == [
            {
                "timestamp": _ts("2024-01-01T00:00:00Z"),
                "open": 100.0,
                "high": 105.0,
                "low": 100.0,
                "close": 105.0,
                "volume": 3.0,
                "trades": 2,
            }
        ]
    finally:
        client.close()


def test_volume_aggregates_from_snapshot_download_flow(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067205000,
                            "type": "trade",
                            "data": {"price": 100.0, "quantity": 1.0},
                        },
                        {
                            "timestamp": 1704067201000,
                            "type": "trade",
                            "data": {"price": 95.0, "quantity": 2.0},
                        },
                        {
                            "timestamp": 1704067240000,
                            "type": "trade",
                            "data": {"price": 105.0, "quantity": 3.0},
                        },
                        {
                            "timestamp": 1704067260000,
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
        assert client.volume(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T00:02:00Z",
            interval="1m",
        ) == [
            {
                "timestamp": 1704067200000,
                "volume": 6.0,
            },
            {
                "timestamp": 1704067260000,
                "volume": 4.0,
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
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {
                            "timestamp": 1704067200000,
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


def test_volume_require_snapshot_coverage_and_do_not_fall_back_to_events(tmp_path) -> None:
    calls: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.params.get("format")))
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": []})
        if request.url.path == "/events":
            raise AssertionError("volume() should not fall back to /events")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(
            PolarisError,
            match="could not be satisfied from standardized snapshots",
        ):
            client.volume(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T00:02:00Z",
                interval="1m",
            )
        assert calls == [("/snapshots", None)]
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
            },
            {
                "timestamp": _ts("2024-01-01T02:00:00Z"),
                "open": 110.0,
                "high": 110.0,
                "low": 110.0,
                "close": 110.0,
                "volume": 3.0,
                "trades": 1,
            },
        ]
        assert calls == [
            ("/snapshots", None),
            ("/download", "json"),
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
        if request.url.path in {"/snapshots", "/download"}:
            calls.append(str(request.url))
        if request.url.path == "/snapshots":
            assert request.headers.get("authorization") == "Bearer polaris_key_test"
            return httpx.Response(
                200,
                json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]},
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
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
        assert len(calls) == 2
    finally:
        client.close()


def test__download_snapshots_preserves_dashed_market_names_in_local_paths(
    tmp_path,
) -> None:
    calls: list[str] = []
    snapshot_key = "standard-arcus-AAPL-USD-2026-07-11"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path in {"/snapshots", "/download"}:
            calls.append(str(request.url))
        if request.url.path == "/snapshots":
            return httpx.Response(
                200,
                json={
                    "snapshots": [
                        {
                            "key": snapshot_key,
                            "date": "2026-07-11",
                            "source": "arcus",
                            "market": "AAPL-USD",
                        }
                    ]
                },
            )
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="arcus",
                    market="AAPL-USD",
                    day="2026-07-11",
                    keys=[snapshot_key],
                ),
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
            source="arcus",
            market="AAPL-USD",
            from_="2026-07-11T00:00:00Z",
            to="2026-07-12T00:00:00Z",
        )
        assert [entry.key for entry in entries] == [snapshot_key]
        assert (
            tmp_path
            / "data"
            / "standard"
            / "arcus"
            / "AAPL-USD"
            / "2026-07-11"
            / f"{snapshot_key}.jsonl.zst"
        ).exists()
        assert (tmp_path / "daily" / "arcus" / "AAPL-USD" / "2026-07-11.jsonl.zst").exists()
        assert len(calls) == 2
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
                    {"timestamp": 1704067200000},
                    {"timestamp": 1704067260000},
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
            {"timestamp": 1704067200000},
            {"timestamp": 1704067260000},
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
                    {"timestamp": 1704067200000},
                    {"timestamp": 1704067260000},
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
                    {"timestamp": 1704067200000},
                    {"timestamp": 1704110400000},
                ]
            )
        )
        day_two.write_bytes(_zstd_ndjson([{"timestamp": 1704153600000}]))

        rows = list(
            client._iter_local_events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T12:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        )
        assert rows == [{"timestamp": 1704110400000}]
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
                    {"timestamp": 1704067200000},
                    {"timestamp": 1704067260000},
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
            {"timestamp": 1704067200000},
            {"timestamp": 1704067260000},
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
        daily_path.write_bytes(_zstd_ndjson([{"timestamp": 1704067200000}]))

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
        assert rows == [{"timestamp": 1704067200000}]
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
        day_one.write_bytes(_zstd_ndjson([{"timestamp": 1704110400000}]))
        day_two.write_bytes(
            zstd.ZstdCompressor().compress(
                b'{"timestamp":1704175200000}\nnot-json\n'
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
        assert rows == [{"timestamp": 1704110400000}]
    finally:
        client.close()


def test_events_use_snapshot_download_flow_by_default(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/snapshots":
            return httpx.Response(200, json={"snapshots": [{"key": SNAPSHOT_KEY_DAY_1, "date": "2024-01-01"}]})
        if request.url.path == "/download":
            return httpx.Response(
                200,
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(
                    [
                        {"timestamp": 1704067200000},
                        {"timestamp": 1704067260000},
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
            {"timestamp": 1704067200000},
            {"timestamp": 1704067260000},
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
            ("/download", "json"),
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
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
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


def test_funding_rates_filter_point_series_from_standardized_snapshots(tmp_path) -> None:
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "funding_rate", "value": 0.0001},
        },
        {
            "timestamp": _ts("2024-01-01T00:01:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "mark_price", "value": 43123.5},
        },
        {
            "timestamp": _ts("2024-01-01T00:02:00Z"),
            "type": "trade",
            "data": {"price": 43124.0, "quantity": 0.25},
        },
        {
            "timestamp": _ts("2024-01-01T00:03:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "funding_rate", "value": 0.0002},
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
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.funding_rates(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [snapshot_rows[0], snapshot_rows[3]]
    finally:
        client.close()


def test_mark_prices_filter_point_series_from_standardized_snapshots(tmp_path) -> None:
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "funding_rate", "value": 0.0001},
        },
        {
            "timestamp": _ts("2024-01-01T00:01:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "mark_price", "value": 43123.5},
        },
        {
            "timestamp": _ts("2024-01-01T00:02:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "index_price", "value": 43120.0},
        },
        {
            "timestamp": _ts("2024-01-01T00:03:00Z"),
            "type": "point",
            "source": "binance",
            "market": "BTC-USDT",
            "data": {"series": "mark_price", "value": 43124.0},
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
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        assert client.mark_prices(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [snapshot_rows[1], snapshot_rows[3]]
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
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
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


def test_depth_metrics_derive_depth_spread_and_slippage_from_l2_snapshots(
    tmp_path,
) -> None:
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "l2_snapshot",
            "data": {
                "bids": [[100.0, 2.0], [99.5, 3.0], [98.0, 4.0]],
                "asks": [[100.5, 1.0], [101.0, 2.0], [102.0, 4.0]],
            },
        },
        {
            "timestamp": _ts("2024-01-01T00:00:01Z"),
            "type": "l2_snapshot",
            "data": {
                "bids": [{"price": "100.1", "size": "0.4"}],
                "asks": [{"price": 100.4, "quantity": 0.3}],
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
                json=_bulk_download_manifest(
                    source="binance",
                    market="BTC-USDT",
                    day="2024-01-01",
                    keys=[SNAPSHOT_KEY_DAY_1],
                ),
            )
        if request.url.host == "download.example.com":
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = client.depth_metrics(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            depth_pct=0.01,
            slippage_notional=100.25,
        )
    finally:
        client.close()

    assert len(rows) == 2

    assert rows[0] == pytest.approx(
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "bid_price": 100.0,
            "ask_price": 100.5,
            "mid_price": 100.25,
            "bid_ask_spread": 0.5,
            "bid_ask_spread_bps": 49.87531172069825,
            "depth_pct": 0.01,
            "bid_depth_notional": 498.5,
            "ask_depth_notional": 302.5,
            "depth_imbalance": 0.24469413233458176,
            "slippage_notional": 100.25,
            "target_base_quantity": 1.0,
            "buy_average_price": 100.5,
            "sell_average_price": 100.0,
            "buy_slippage": 0.25,
            "sell_slippage": 0.25,
            "buy_slippage_bps": 24.937655860349125,
            "sell_slippage_bps": 24.937655860349125,
        }
    )
    assert rows[1] == pytest.approx(
        {
            "timestamp": _ts("2024-01-01T00:00:01Z"),
            "bid_price": 100.1,
            "ask_price": 100.4,
            "mid_price": 100.25,
            "bid_ask_spread": 0.30000000000001137,
            "bid_ask_spread_bps": 29.92518703241909,
            "depth_pct": 0.01,
            "bid_depth_notional": 40.04,
            "ask_depth_notional": 30.119999999999997,
            "depth_imbalance": 0.14139110604332952,
            "slippage_notional": 100.25,
            "target_base_quantity": 1.0,
            "buy_average_price": None,
            "sell_average_price": None,
            "buy_slippage": None,
            "sell_slippage": None,
            "buy_slippage_bps": None,
            "sell_slippage_bps": None,
        }
    )


def test_depth_metrics_validate_positive_inputs() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(ValueError, match="depth_pct must be greater than 0"):
            client.depth_metrics(
                source="binance",
                market="BTC-USDT",
                depth_pct=0,
            )
        with pytest.raises(
            ValueError, match="slippage_notional must be greater than 0"
        ):
            client.depth_metrics(
                source="binance",
                market="BTC-USDT",
                slippage_notional=0,
            )
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
            ("/download", "json"),
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
