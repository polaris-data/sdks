from __future__ import annotations

import json
import math
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import zstandard as zstd

from polaris_data import OrderbookBuilder, PolarisClient
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


def _raw_replay_cache_path(directory: Path) -> Path:
    return directory / (
        "binance_BTC-USDT_2024-01-01T00-00-00Z_2024-01-01T01-00-00Z_raw.jsonl"
    )


def _write_propamm_fixture(
    root: Path,
    source: str,
    fixture_name: str,
    *,
    mutate=None,
) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / fixture_name
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    if mutate is not None:
        mutate(rows)
    key = f"standard-{source}-ethereum-2024-01-01-000000"
    path = (
        root
        / "data"
        / "standard"
        / source
        / "ethereum"
        / "2024-01-01"
        / f"{key}.jsonl.zst"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_zstd_ndjson(rows))
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": 1_704_067_200_000_000,
                "end_us": 1_704_070_800_000_000,
            }
        )
    )


def _write_option_fixture(root: Path, *, mutate=None) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / "options-v2.jsonl"
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    if mutate is not None:
        mutate(rows)
    key = "standard-deribit-BTC-2024-01-01-000000"
    path = (
        root
        / "data"
        / "standard"
        / "deribit"
        / "BTC"
        / "2024-01-01"
        / f"{key}.jsonl.zst"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_zstd_ndjson(rows))
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": 1_704_067_200_000_000,
                "end_us": 1_704_070_800_000_000,
            }
        )
    )


def _write_intent_fixture(root: Path, *, mutate=None) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / "intents-v2.jsonl"
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    if mutate is not None:
        mutate(rows)
    key = "standard-uniswapx-intents-2024-01-01-000000"
    path = (
        root
        / "data"
        / "standard"
        / "uniswapx"
        / "intents"
        / "2024-01-01"
        / f"{key}.jsonl.zst"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_zstd_ndjson(rows))
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": 1_704_067_200_000_000,
                "end_us": 1_704_070_800_000_000,
            }
        )
    )


def _write_new_event_fixture(root: Path, *, mutate=None) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / "new-event-shapes-v2.jsonl"
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    if mutate is not None:
        mutate(rows)
    key = "standard-hyperliquid-BTC-2024-01-01-000000"
    path = (
        root
        / "data"
        / "standard"
        / "hyperliquid"
        / "BTC"
        / "2024-01-01"
        / f"{key}.jsonl.zst"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_zstd_ndjson(rows))
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": 1_704_067_200_000_000,
                "end_us": 1_704_070_800_000_000,
            }
        )
    )


def make_client(
    handler,
    *,
    api_key: str | None = "polaris_key_test",
    dataset_root: Path | None = None,
    replay_cache_enabled: bool = False,
    replay_cache_dir: Path | None = None,
) -> PolarisClient:
    class RequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            request = httpx.Request(
                "GET",
                f"http://{self.headers['Host']}{self.path}",
                headers=dict(self.headers),
            )
            try:
                response = handler(request)
                content = response.content.replace(
                    b"https://download.example.com",
                    f"http://{self.headers['Host']}".encode(),
                )
                self.send_response(response.status_code)
                for name, value in response.headers.items():
                    if name.lower() not in {"content-length", "transfer-encoding"}:
                        self.send_header(name, value)
                self.send_header("content-length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except Exception as exc:  # pragma: no cover - surfaced through the client
                content = f"{type(exc).__name__}: {exc}".encode()
                self.send_response(500)
                self.send_header("content-type", "text/plain")
                self.send_header("content-length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), RequestHandler)
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01),
        daemon=True,
    )
    thread.start()
    client = PolarisClient(
        api_key=api_key,
        base_url=f"http://127.0.0.1:{server.server_port}",
        dataset_root=dataset_root,
        replay_cache_enabled=replay_cache_enabled,
        replay_cache_dir=replay_cache_dir,
    )
    native_close = client.close

    def close() -> None:
        native_close()
        server.shutdown()
        server.server_close()
        thread.join()

    client.close = close
    return client


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


def _is_download_request(request: httpx.Request) -> bool:
    return request.url.host == "download.example.com" or request.url.path.endswith(
        ".jsonl.zst"
    )


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
                            (
                                _hourly_snapshot_key(
                                    "binance", "BTC-USDT", "2024-01-01", 0
                                ),
                                0,
                            ),
                            (
                                _hourly_snapshot_key(
                                    "binance", "BTC-USDT", "2024-01-01", 2
                                ),
                                2,
                            ),
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
        if _is_download_request(request):
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
                "symbol": "BTCUSDT",
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
        assert result["markets"][0]["symbol"] == "BTCUSDT"
        assert result["markets"][1]["symbol"] == "BTC"
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


def test_catalog_paginates_across_cursor_pages() -> None:
    pages = [
        {
            "markets": [
                {"source": "binance", "market": "BTC-USDT", "symbol": "BTCUSDT"},
                {"source": "binance", "market": "ETH-USDT", "symbol": "ETHUSDT"},
            ],
            "updatedAt": "2026-05-19T10:28:00.000Z",
            "has_more": True,
            "next_cursor": "cursor-token",
        },
        {
            "markets": [
                {"source": "hyperliquid", "market": "BTC", "symbol": "BTC"},
            ],
            "updatedAt": "2026-05-19T10:28:00.000Z",
            "has_more": False,
            "next_cursor": None,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/catalog"
        cursor = request.url.params.get("cursor")
        payload = pages[1] if cursor == "cursor-token" else pages[0]
        return httpx.Response(200, json=payload)

    client = make_client(handler)
    try:
        result = client.catalog()
        assert [m["market"] for m in result["markets"]] == [
            "BTC-USDT",
            "ETH-USDT",
            "BTC",
        ]
    finally:
        client.close()


def test_count_returns_catalog_totals() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/count"
        return httpx.Response(
            200,
            json={
                "updatedAt": "2026-05-19T10:28:00.000Z",
                "sources": 3,
                "markets": 42,
                "by_source": {"binance": 10, "hyperliquid": 32},
            },
        )

    client = make_client(handler)
    try:
        result = client.count()
        assert result["sources"] == 3
        assert result["markets"] == 42
        assert result["by_source"] == {"binance": 10, "hyperliquid": 32}
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
        f"standard-binance-BTC-USDT-{date_text}": date_text
        for date_text in snapshot_dates
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
        if _is_download_request(request):
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
            raise AssertionError(
                "raw endpoint should not be called for legacy catalog shape"
            )
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
            list(client.trades(
                source="binance",
                market="BTC-USDT",
                start=_ts("2024-01-01T00:00:00Z"),
                end=_ts("2024-01-01T01:00:00Z"),
            ))
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
        if _is_download_request(request):
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
        rows = list(
            client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        )
        assert [row for row in rows if row["type"] == "trade"] == [
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


def test_trade_columnar_two_pass_downloads_and_resolves_once(tmp_path) -> None:
    calls: list[str] = []
    snapshot_rows = [
        {
            "timestamp": _ts("2024-01-01T00:00:00Z"),
            "type": "trade",
            "data": {"price": 100.0, "quantity": 0.5, "trade_id": 1},
        },
        {
            "timestamp": _ts("2024-01-01T00:00:01Z"),
            "type": "trade",
            "data": {"price": 101.0, "quantity": 0.25, "trade_id": 2},
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
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
        if _is_download_request(request):
            return httpx.Response(
                200,
                content=_zstd_ndjson(snapshot_rows),
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        batches = list(
            client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
                output="batches",
                batch_size=1,
            )
        )
    finally:
        client.close()

    assert [batch.num_rows for batch in batches] == [1, 1]
    assert calls == ["/snapshots", "/download", f"/{SNAPSHOT_KEY_DAY_1}.jsonl.zst"]


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
        if _is_download_request(request):
            return httpx.Response(404, text="snapshot missing")
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        with pytest.raises(NotFoundError, match="snapshot missing"):
            client.events(
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
        if _is_download_request(request):
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
        assert list(
            client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T02:00:00Z",
            )
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
        if _is_download_request(request):
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
        assert list(
            client.events(
                source="hyperliquid",
                market="BTC",
                from_="2026-07-11T01:05:00Z",
                to="2026-07-11T01:15:00Z",
            )
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


def test_trades_require_snapshot_coverage_and_do_not_fall_back_to_events(
    tmp_path,
) -> None:
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
            client.events(
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


def test_ohlcv_rejects_stale_parquet_format() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(
            ValueError, match="format must be one of: None, 'tradingview'"
        ):
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


def test_replay_materializes_local_snapshot_data_files_before_network(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            "network should not be called when local snapshot files exist"
        )

    client = make_client(handler, dataset_root=tmp_path)
    try:
        snapshot_path = (
            tmp_path
            / "data"
            / "standard"
            / "binance"
            / "BTC-USDT"
            / "2024-01-01"
            / f"{SNAPSHOT_KEY_DAY_1}.jsonl.zst"
        )
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


def test_local_events_and_replay_convert_standard_events_without_intermediate_tree(
    tmp_path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("exact local coverage must not use the network")

    key = "standard-binance-BTC-USDT-2024-01-01-000000"
    snapshot_path = (
        tmp_path
        / "data"
        / "standard"
        / "binance"
        / "BTC-USDT"
        / "2024-01-01"
        / f"{key}.jsonl.zst"
    )
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "timestamp": 1_704_067_200_000,
            "nested": {"timestamp": 1, "type": "", "source": ""},
            "large": 2**63 + 7,
        },
        {
            "timestamp": 1_704_067_200_001,
            "source": "binance",
            "market": "BTC-USDT",
            "type": "trade",
            "data": {
                "price": 100.5,
                "quantity": 2,
                "side": "",
                "nested": {"side": ""},
            },
            "sequence": 9,
        },
        {
            "timestamp": 1_704_067_200_002,
            "source": "binance",
            "market": "BTC-USDT",
            "type": "orderbook",
            "data": {
                "bids": [{"price": 100.0, "quantity": 3.0}],
                "asks": [{"price": 101.0, "quantity": 4.0}],
            },
        },
    ]
    snapshot_path.write_bytes(_zstd_ndjson(rows))
    snapshot_path.with_name(snapshot_path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": 1_704_067_200_000_000,
                "end_us": 1_704_070_800_000_000,
            }
        )
    )

    expected = [
        {
            "timestamp": 1_704_067_200_000,
            "nested": {"timestamp": 1, "type": "", "source": ""},
            "large": 2**63 + 7,
        },
        {
            "timestamp": 1_704_067_200_001,
            "source": "binance",
            "market": "BTC-USDT",
            "type": "trade",
            "data": {
                "price": 100.5,
                "quantity": 2,
                "nested": {"side": ""},
            },
            "sequence": 9,
        },
        rows[2],
    ]
    client = make_client(handler, dataset_root=tmp_path)
    try:
        kwargs = {
            "source": "binance",
            "market": "BTC-USDT",
            "from_": "2024-01-01T00:00:00Z",
            "to": "2024-01-01T01:00:00Z",
            "materialize_orderbooks": False,
        }
        assert list(client.events(**kwargs)) == expected
        assert list(client.replay(**kwargs)) == expected
    finally:
        client.close()


def test_propamm_quote_ladders_are_typed_and_preserve_uint256_strings(tmp_path) -> None:
    _write_propamm_fixture(
        tmp_path,
        "fermiswap",
        "propamm-fermiswap-v2.jsonl",
    )
    _write_propamm_fixture(tmp_path, "metric", "propamm-metric-v2.jsonl")
    client = PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1")
    try:
        kwargs = {
            "market": "ethereum",
            "from_": "2024-01-01T00:00:00Z",
            "to": "2024-01-01T01:00:00Z",
        }
        fermi = list(
            client.propamm_quote_ladders(source="fermiswap", **kwargs)
        )
        metric = list(client.propamm_quote_ladders(source="metric", **kwargs))
    finally:
        client.close()

    assert len(fermi) == 1
    assert fermi[0]["market"] == "ethereum"
    assert fermi[0]["data"]["values"]["oracle"] is None
    assert "pool" not in fermi[0]["data"]["values"]
    assert fermi[0]["data"]["values"]["quotes"][0]["amount_in"] == str(
        2**256 - 1
    )
    assert metric[0]["data"]["values"]["pool"] == "0xpool"


@pytest.mark.parametrize("output", ["iterator", "batches"])
def test_propamm_quote_ladders_reject_malformed_matching_records(
    tmp_path, output
) -> None:
    def invalidate(rows: list[dict]) -> None:
        rows[-1]["data"]["values"]["quotes"][0]["amount_in"] = 10

    _write_propamm_fixture(
        tmp_path,
        "fermiswap",
        "propamm-fermiswap-v2.jsonl",
        mutate=invalidate,
    )
    client = PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1")
    try:
        with pytest.raises(StreamDecodeError, match="quote-ladder payload"):
            list(
                client.propamm_quote_ladders(
                    source="fermiswap",
                    market="ethereum",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T01:00:00Z",
                    output=output,
                )
            )
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


def test_replay_reads_local_snapshot_day_files_before_network(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(
            "network should not be called when local daily files exist"
        )

    client = make_client(handler, dataset_root=tmp_path)
    try:
        daily_path = (
            tmp_path / "daily" / "binance" / "BTC-USDT" / "2024-01-01.jsonl.zst"
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


def test_replay_parallel_prefetches_standard_files_in_order(tmp_path) -> None:
    client = make_client(
        lambda request: (_ for _ in ()).throw(
            AssertionError("network should not be called when local daily files exist")
        ),
        dataset_root=tmp_path,
    )
    try:
        for day, timestamp in [
            ("2024-01-01", 1704067200000),
            ("2024-01-02", 1704153600000),
            ("2024-01-03", 1704240000000),
        ]:
            path = tmp_path / "daily" / "binance" / "BTC-USDT" / f"{day}.jsonl.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(_zstd_ndjson([{"timestamp": timestamp}]))

        assert list(
            client.replay(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-04T00:00:00Z",
                parallel=True,
            )
        ) == [
            {"timestamp": 1704067200000},
            {"timestamp": 1704153600000},
            {"timestamp": 1704240000000},
        ]
    finally:
        client.close()


def test_events_use_snapshot_download_flow_by_default(tmp_path) -> None:
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
        if _is_download_request(request):
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
        assert list(
            client.events(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-02T00:00:00Z",
            )
        ) == [
            {"timestamp": 1704067200000},
            {"timestamp": 1704067260000},
        ]
    finally:
        client.close()


def test_events_require_snapshot_coverage_and_do_not_fall_back_to_events(
    tmp_path,
) -> None:
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


def test_events_defer_decode_errors_until_iteration_reaches_them(tmp_path) -> None:
    body = zstd.ZstdCompressor().compress(
        b'{"timestamp":1704067200000,"type":"trade","data":{"price":1,"quantity":1}}\n'
        b"not-json\n"
    )

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
        if _is_download_request(request):
            return httpx.Response(
                200,
                content=body,
                headers={"content-type": "application/zstd"},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = client.events(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            materialize_orderbooks=False,
        )
        assert next(rows)["timestamp"] == 1704067200000
        with pytest.raises(StreamDecodeError, match="invalid ndjson line"):
            next(rows)
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


def _l2_row(snapshot: bool) -> dict:
    row = {
        "event_id": "snapshot" if snapshot else "delta",
        "source": "hyperliquid", "market": "0G", "instrument": None,
        "collector_timestamp": 10, "exchange_timestamp": None,
        "source_capture_id": "capture", "schema_version": 1,
        "source_event_is_snapshot": snapshot,
    }
    for index in range(25):
        for side in ("bid", "ask"):
            for field in ("px", "sz"):
                row[f"{side}_{field}_{index:02}"] = None
    row["bid_px_00"] = 100.0
    row["bid_sz_00"] = 2.0
    return row


def test_l2_direct_routes_paginate_and_keep_nullable_levels(tmp_path) -> None:
    calls: list[httpx.Request] = []
    snapshot, delta = _l2_row(True), _l2_row(False)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["authorization"] == "Bearer polaris_key_test"
        if request.url.path == "/historical/l2-updates":
            second = "cursor" in request.url.params
            return httpx.Response(200, json={
                "items": [delta if second else snapshot],
                "has_more": not second,
                "next_cursor": None if second else "next",
            })
        assert request.url.path == "/historical/l2-orderbooks"
        return httpx.Response(200, json={"items": [delta], "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        updates = list(client.l2_updates(source="hyperliquid", market="0G", instrument="0G", start=10, end=10))
        books = list(client.l2_snapshots(source="hyperliquid", market="0G", start=10, end=300010))
        long_books = list(client.l2_snapshots(source="hyperliquid", market="0G", start=10, end=600010))
    finally:
        client.close()

    assert updates == [snapshot, delta]
    assert books == [delta]
    assert long_books == [delta]
    assert calls[0].url.params["instrument"] == "0G"
    assert calls[0].url.params["start"] == "10"
    assert calls[0].url.params["end"] == "10"
    assert calls[1].url.params["cursor"] == "next"
    assert calls[2].url.params["end"] == "300010"
    assert calls[3].url.params["end"] == "600010"
    assert len(calls) == 4


def test_direct_historical_rows_paginate_filter_and_keep_nullable_fields(tmp_path) -> None:
    calls: list[httpx.Request] = []
    identity = {
        "source": "deribit", "market": "BTC", "source_capture_id": "capture",
        "collector_timestamp": 10, "schema_version": 1,
    }
    trade = {**identity, "event_id": "t1", "price": 100.0, "quantity": 2.0, "side": None}
    option = {**identity, "event_id": "o1", "instrument": "BTC-29MAR24-50000-C",
              "delta": "0.431", "mark_price": None}
    funding = {**identity, "event_id": "f1", "funding_rate": None, "mark_price": "100"}
    funding_without_mark = {**identity, "event_id": "f2", "funding_rate": "0.0001", "mark_price": None}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/historical/trades":
            next_page = request.url.params.get("cursor") == "next"
            return httpx.Response(200, json={
                "items": [{**trade, "event_id": "t2" if next_page else "t1"}],
                "has_more": not next_page,
                "next_cursor": None if next_page else "next",
            })
        if request.url.path == "/historical/options-ticker":
            return httpx.Response(200, json={"items": [option], "has_more": False, "next_cursor": None})
        if request.url.path == "/historical/funding-rates":
            return httpx.Response(200, json={"items": [funding, funding_without_mark], "has_more": False, "next_cursor": None})
        raise AssertionError(f"unexpected request: {request.url}")

    client = make_client(handler, dataset_root=tmp_path)
    try:
        trades = list(client.trades(source="deribit", market="BTC", start=10, end=10))
        assert [row["event_id"] for row in trades] == ["t1", "t2"]
        assert trades[0]["side"] is None
        assert calls[0].url.params["start"] == "10"
        assert calls[0].url.params["end"] == "10"
        assert calls[0].url.params["limit"] == "1000"
        assert calls[1].url.params["cursor"] == "next"
        assert calls[0].headers["authorization"] == "Bearer polaris_key_test"
        options = list(client.option_tickers(source="deribit", market="BTC",
                                               instrument="BTC-29MAR24-50000-C"))
        assert len(options) == 1
        assert all(options[0][key] == value for key, value in option.items())
        assert calls[2].url.params["instrument"] == option["instrument"]
        funding_rows = list(client.funding_rates())
        assert len(funding_rows) == 2
        assert all(funding_rows[0][key] == value for key, value in funding.items())
        assert list(client.perpetual_tickers()) == funding_rows
        assert list(client.mark_prices()) == funding_rows[:1]
        assert "start" not in calls[3].url.params
        assert "end" not in calls[3].url.params
        with pytest.raises(ValueError, match="instrument must be non-empty"):
            client.option_tickers(instrument="")
    finally:
        client.close()


def test_direct_historical_older_range_maps_missing_authentication(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/historical/trades"
        assert request.url.params["start"] == "1704067200000"
        assert "authorization" not in request.headers
        return httpx.Response(401, json={"error": "API key required for older history"})

    client = make_client(handler, api_key=None, dataset_root=tmp_path)
    try:
        with pytest.raises(UnauthorizedError, match="API key required"):
            list(client.trades(start=1_704_067_200_000, end=1_704_067_200_000))
    finally:
        client.close()


def test_raw_channel_pages_exact_captures(tmp_path) -> None:
    calls: list[httpx.Request] = []
    capture = {
        "capture_id": "c1", "collector_timestamp": 10, "recorder_version": "v1",
        "ingested_at": 11, "additional_context": {"channel": "trades"},
        "original_json": '{ "price": 1.0 }',
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/raw/binance/trades"
        assert request.url.params["start"] == "10"
        assert request.url.params["end"] == "10"
        assert request.url.params["limit"] == "1000"
        next_page = request.url.params.get("cursor") == "next"
        return httpx.Response(200, json={
            "items": [{**capture, "capture_id": "c2" if next_page else "c1"}],
            "has_more": not next_page,
            "next_cursor": None if next_page else "next",
        })

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = list(client.raw_channel(exchange="binance", event="trades", start=10, end=10))
        assert [row["capture_id"] for row in rows] == ["c1", "c2"]
        assert rows[0]["original_json"] == '{ "price": 1.0 }'
        assert rows[0]["additional_context"] == {"channel": "trades"}
        assert calls[0].headers["authorization"] == "Bearer polaris_key_test"
        assert calls[1].url.params["cursor"] == "next"
        with pytest.raises(ValueError, match="exchange and event"):
            list(client.raw_channel(exchange="", event="trades", start=10, end=10))
    finally:
        client.close()


def test_direct_ohlcv_intent_and_quote_rows_keep_flat_observations(tmp_path) -> None:
    calls: list[httpx.Request] = []
    identity = {"source_capture_id": "capture", "collector_timestamp": 10, "schema_version": 1}
    candle = {**identity, "event_id": "c1", "source": "binance", "market": "BTC-USDT",
              "interval": "1m", "open_timestamp": 10, "open": 100.0, "high": 102.0,
              "low": 99.0, "close": 100.0, "is_closed": False}
    intent = {**identity, "event_id": "i1", "source": "uniswapx", "market": "intents",
              "intent_id": "intent-1", "input_asset_id": None}
    quote = {**identity, "event_id": "q1", "source": "propamm", "market": "ethereum",
             "instrument": "pool-1", "observation_id": "obs-1", "input_asset_id": "ETH",
             "input_chain_id": "1", "input_amount": "1000000000000000000", "input_decimals": 18,
             "output_asset_id": "USDC", "output_chain_id": "1", "output_amount": "2000000",
             "output_decimals": 6, "amount_kind": "exact_input", "block_number": 100,
             "block_hash": "0xblock", "transaction_hash": "0xtx", "transaction_index": 0,
             "router": "0xrouter", "pool": None}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/historical/ohlcv":
            second = request.url.params.get("cursor") == "next"
            return httpx.Response(200, json={"items": [{**candle, "event_id": "c2" if second else "c1"}],
                                             "has_more": not second, "next_cursor": None if second else "next"})
        rows = {"/historical/intents": [intent], "/historical/quotes": [quote]}
        return httpx.Response(200, json={"items": rows[request.url.path], "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        candles = list(client.ohlcv_rows(interval="1m", start=10, end=10))
        assert [row["event_id"] for row in candles] == ["c1", "c2"]
        assert candles[0]["open_timestamp"] == candles[1]["open_timestamp"]
        assert calls[0].url.params["interval"] == "1m"
        assert calls[1].url.params["cursor"] == "next"
        intents = list(client.intent_rows(intent_id="intent-1"))
        assert intents[0]["input_asset_id"] is None
        assert list(client.intents(intent_id="intent-1")) == intents
        assert calls[2].url.params["intent_id"] == "intent-1"
        quotes = list(client.quote_rows(observation_id="obs-1", instrument="pool-1"))
        assert quotes[0]["input_amount"] == "1000000000000000000"
        assert calls[4].url.params["observation_id"] == "obs-1"
        assert calls[4].url.params["instrument"] == "pool-1"
        assert all(request.headers["authorization"] == "Bearer polaris_key_test" for request in calls)
    finally:
        client.close()


def test_venue_candle_aggregates_use_latest_revision_and_reported_volumes(tmp_path) -> None:
    start = 1_704_067_200_000
    identity = {"source": "binance", "market": "BTC-USDT", "source_capture_id": "capture",
                "schema_version": 1}
    first = {**identity, "event_id": "open", "collector_timestamp": start + 1,
             "interval": "1m", "open_timestamp": start, "open": 100.0,
             "high": 101.0, "low": 99.0, "close": 101.0, "base_volume": 1.0,
             "quote_volume": 101.0, "trade_count": 2}
    final = {**first, "event_id": "closed", "collector_timestamp": start + 2,
             "high": 103.0, "close": 102.0, "base_volume": 2.0,
             "quote_volume": 204.0, "trade_count": 5, "is_closed": True}
    closes = [100.0, 110.0, 99.0, 108.9]
    fine = [{**identity, "event_id": f"fine-{i}", "collector_timestamp": start + i,
             "interval": "10s", "open_timestamp": start + i * 10_000,
             "open": price, "high": price, "low": price, "close": price}
            for i, price in enumerate(closes)]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/historical/ohlcv"
        assert request.url.params["source"] == "binance"
        assert request.url.params["start"] == str(start)
        assert request.url.params["end"] == str(start + 60_000)
        if request.url.params.get("interval") == "1m":
            second = request.url.params.get("cursor") == "next"
            return httpx.Response(200, json={"items": [final if second else first],
                                              "has_more": not second,
                                              "next_cursor": None if second else "next"})
        return httpx.Response(200, json={"items": fine, "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    options = {"source": "binance", "market": "BTC-USDT", "interval": "1m",
               "from_": "2024-01-01T00:00:00Z", "to": "2024-01-01T00:01:00Z"}
    try:
        assert client.ohlcv(**options) == [{"timestamp": start, "open": 100.0,
                                            "high": 103.0, "low": 99.0, "close": 102.0,
                                            "volume": 2.0, "trades": 5}]
        assert client.volume(**options) == [{"timestamp": start, "volume": 2.0}]
        assert client.vwap(**options) == [{"timestamp": start, "vwap": 102.0,
                                           "volume": 2.0, "quote_volume": 204.0, "trades": 5}]
        assert client.ohlcv(**options, format="tradingview")["candles"][0]["close"] == 102.0
        volatility = client.volatility(**options)
        assert len(volatility) == 1
        assert volatility[0]["timestamp"] == start
        assert volatility[0]["returns"] == 3
        assert volatility[0]["volatility"] > 0
    finally:
        client.close()


@pytest.mark.parametrize(
    "bad_data",
    [{}, {"funding_rate": 0.1}],
)
def test_python_rejects_malformed_perpetual_tickers(tmp_path, bad_data) -> None:
    def mutate(rows):
        rows[1]["data"] = bad_data

    _write_new_event_fixture(tmp_path, mutate=mutate)
    client = PolarisClient(base_url="http://127.0.0.1:1", dataset_root=tmp_path)
    try:
        with pytest.raises(PolarisError, match="perpetual"):
            list(
                client.events(
                    source="hyperliquid",
                    market="BTC",
                    from_="2024-01-01T00:00:00Z",
                    to="2024-01-01T01:00:00Z",
                )
            )
    finally:
        client.close()


def _book_row(timestamp: int, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> dict:
    row = _l2_row(True)
    row.update(source="binance", market="BTC-USDT", collector_timestamp=timestamp)
    for index in range(25):
        for side in ("bid", "ask"):
            row[f"{side}_px_{index:02}"] = None
            row[f"{side}_sz_{index:02}"] = None
    for side, levels in (("bid", bids), ("ask", asks)):
        for index, (price, size) in enumerate(levels):
            row[f"{side}_px_{index:02}"] = price
            row[f"{side}_sz_{index:02}"] = size
    return row


def test_bbo_uses_reconstructed_books_for_long_range_changes_and_intervals(tmp_path) -> None:
    start = _ts("2024-01-01T00:00:00Z")
    books = [
        _book_row(start + 100, [(100.0, 2.0), (99.0, 4.0)], [(101.0, 3.0)]),
        _book_row(start + 800, [(100.0, 2.0), (99.5, 4.0)], [(101.0, 3.0)]),
        _book_row(start + 1200, [(100.0, 2.0)], [(100.5, 1.5)]),
        _book_row(start + 3100, [(100.25, 1.0)], [(100.5, 1.5)]),
    ]
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/historical/l2-orderbooks"
        return httpx.Response(200, json={"items": books, "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        options = dict(source="binance", market="BTC-USDT",
                       start=start, end=start + 3_600_000)
        all_quotes = list(client.bbo(**options))
        changed = list(client.bbo(**options, changes_only=True))
        interval = list(client.bbo(**options, interval="1s"))
    finally:
        client.close()

    assert [row["timestamp"] for row in all_quotes] == [start + 100, start + 800, start + 1200, start + 3100]
    assert [row["timestamp"] for row in changed] == [start + 100, start + 1200, start + 3100]
    assert [row["timestamp"] for row in interval] == [start, start + 1000, start + 3000]
    assert interval[0]["bid_quantity"] == 2.0
    assert interval[1]["ask_price"] == 100.5
    assert all(call.url.params["start"] == str(start) for call in calls)
    assert all(call.url.params["end"] == str(start + 3_600_000) for call in calls)
    assert len(calls) == 3


def test_depth_metrics_uses_top_25_reconstructed_book_levels(tmp_path) -> None:
    start = _ts("2024-01-01T00:00:00Z")
    books = [
        _book_row(start, [(100.0, 2.0), (99.5, 3.0), (98.0, 4.0)],
                  [(100.5, 1.0), (101.0, 2.0), (102.0, 4.0)]),
        _book_row(start + 1000, [(100.1, 0.4)], [(100.4, 0.3)]),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/historical/l2-orderbooks"
        return httpx.Response(200, json={"items": books, "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = list(client.depth_metrics(
            source="binance", market="BTC-USDT",
            start=start, end=start + 3_600_000,
            depth_pct=0.01, slippage_notional=100.25,
        ))
    finally:
        client.close()

    assert len(rows) == 2
    assert rows[0] == pytest.approx({
        "timestamp": start, "bid_price": 100.0, "ask_price": 100.5,
        "mid_price": 100.25, "bid_ask_spread": 0.5,
        "bid_ask_spread_bps": 49.87531172069825, "depth_pct": 0.01,
        "bid_depth_notional": 498.5, "ask_depth_notional": 302.5,
        "depth_imbalance": 0.24469413233458176,
        "slippage_notional": 100.25, "target_base_quantity": 1.0,
        "buy_average_price": 100.5, "sell_average_price": 100.0,
        "buy_slippage": 0.25, "sell_slippage": 0.25,
        "buy_slippage_bps": 24.937655860349125,
        "sell_slippage_bps": 24.937655860349125,
    })
    assert rows[1]["buy_average_price"] is None
    assert rows[1]["sell_average_price"] is None


def test_bbo_rejects_unknown_interval() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(ValueError, match="interval must be one of"):
            client.bbo(source="lighter", market="BTC-USD", start=0, end=1, interval="2s")
    finally:
        client.close()


def test_depth_metrics_validate_positive_inputs() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(ValueError, match="depth_pct must be greater than 0"):
            client.depth_metrics(
                source="binance",
                market="BTC-USDT", start=0, end=1,
                depth_pct=0,
            )
        with pytest.raises(
            ValueError, match="slippage_notional must be greater than 0"
        ):
            client.depth_metrics(
                source="binance",
                market="BTC-USDT", start=0, end=1,
                slippage_notional=0,
            )
    finally:
        client.close()


def test_replay_requires_snapshot_coverage_and_do_not_fall_back_to_events(
    tmp_path,
) -> None:
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


def test_raw_replay_raises_stream_decode_error_for_invalid_cached_zstd(
    tmp_path,
) -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    client = make_client(
        handler,
        api_key="pk_live_test",
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        _raw_replay_cache_path(tmp_path).write_bytes(b"not-zs")

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

    client = make_client(
        handler,
        api_key="pk_live_test",
        replay_cache_enabled=True,
        replay_cache_dir=tmp_path,
    )
    try:
        _raw_replay_cache_path(tmp_path).write_bytes(b'{"timestamp":99}\n')

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

    online_client = make_client(
        online_handler,
        api_key="pk_live_test",
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
        raise AssertionError(
            "network should not be called when replay cache already exists"
        )

    offline_client = make_client(
        offline_handler,
        api_key=None,
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


def test_raw_replay_discards_partial_cache_when_iterator_is_dropped(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        return httpx.Response(
            200,
            content=_zstd_ndjson([{"timestamp": 1}, {"timestamp": 2}]),
            headers={"content-type": "application/zstd"},
        )

    client = make_client(handler, replay_cache_enabled=True, replay_cache_dir=tmp_path)
    try:
        iterator = client.replay(
            source="binance",
            market="BTC-USDT",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            standard=False,
        )
        assert next(iterator) == {"timestamp": 1}
        iterator.close()
        assert not _raw_replay_cache_path(tmp_path).exists()
        assert not list(tmp_path.glob("*.part"))
    finally:
        client.close()


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
