from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

from polaris_data import OrderbookBuilder, PolarisClient
from polaris_data.errors import (
    AccessDeniedError,
    PolarisError,
    RateLimitedError,
    UnauthorizedError,
)


def make_client(
    handler,
    *,
    api_key: str | None = "polaris_key_test",
    dataset_root: Path | None = None,
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
                content = response.content
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
    )
    native_close = client.close

    def close() -> None:
        native_close()
        server.shutdown()
        server.server_close()
        thread.join()

    client.close = close
    return client


def _ts(iso8601: str) -> int:
    return int(
        datetime.fromisoformat(iso8601.replace("Z", "+00:00")).timestamp() * 1_000
    )


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


def test_instruments_paginates_and_preserves_contract_statistics() -> None:
    calls: list[httpx.Request] = []
    first = {
        "source": "deribit", "market": "BTC", "instrument": "BTC-1OCT26-70000-C",
        "status": "active", "option_type": "call", "underlying": "BTC",
        "strike": "70000.0", "expiry_timestamp": 1790812800000,
        "statistics": {"source": "deribit", "market": "BTC", "fields": {
            "latest_price": {"value": "0.025", "observed_at": 1790800000000, "unit": "BTC"},
        }},
    }
    second = {**first, "instrument": "BTC-1OCT26-75000-C", "strike": "75000.0", "statistics": None}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/catalog/instruments"
        assert request.url.params.get("source") == "deribit"
        assert request.url.params.get("market") == "BTC"
        assert request.url.params.get("expiry") == "1790812800000"
        assert request.url.params.get("option_type") == "call"
        assert request.url.params.get("limit") == "1000"
        exact = request.url.params.get("instrument") is not None
        if exact:
            assert request.url.params.get("instrument") == first["instrument"]
            assert request.url.params.get("q") == "70000"
        else:
            assert request.url.params.get("q") is None
        later = request.url.params.get("cursor") == "contract-cursor"
        return httpx.Response(200, json={
            "updatedAt": "2026-09-30T00:00:00Z",
            "instruments": [second if later else first],
            "next_cursor": None if later or exact else "contract-cursor",
        })

    client = make_client(handler)
    try:
        result = client.instruments(
            source="deribit", market="BTC", expiry=1790812800000, option_type="call",
        )
        assert result["updatedAt"] == "2026-09-30T00:00:00Z"
        assert len(result["instruments"]) == 2
        assert result["instruments"][0]["statistics"]["fields"]["latest_price"]["value"] == "0.025"
        assert result["instruments"][1]["statistics"] is None
        exact = client.instruments(
            source="deribit", market="BTC", instrument=first["instrument"],
            expiry=1790812800000, option_type="call", q="70000",
        )
        assert [contract["instrument"] for contract in exact["instruments"]] == [first["instrument"]]
        assert len(calls) == 3
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


@pytest.mark.parametrize(
    ("api_key", "window"),
    [(None, timedelta(days=7) - timedelta(minutes=1)),
     ("polaris_key_test", timedelta(days=7))],
)
def test_raw_defaults_to_the_latest_seven_days_without_catalog(api_key, window) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params["source"] == "binance"
        assert request.url.params["market"] == "BTC-USDT"
        start = datetime.fromisoformat(request.url.params["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(request.url.params["end"].replace("Z", "+00:00"))
        assert end - start == window
        assert abs((datetime.now(timezone.utc) - end).total_seconds()) < 5
        assert request.headers.get("authorization") == (
            None if api_key is None else "Bearer polaris_key_test"
        )
        return httpx.Response(200, json={"data": [], "has_more": False, "next_cursor": None})

    client = make_client(handler, api_key=api_key)
    try:
        assert client.raw(source="binance", market="BTC-USDT") == []
    finally:
        client.close()


@pytest.mark.parametrize(
    ("from_", "to", "start", "end"),
    [
        ("2024-01-01T00:00:00Z", None, "2024-01-01T00:00:00Z", "2024-01-08T00:00:00Z"),
        (None, "2024-01-10T00:00:00Z", "2024-01-03T00:00:00Z", "2024-01-10T00:00:00Z"),
        ("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
    ],
)
def test_raw_infers_one_bound_and_accepts_inclusive_equal_bounds(from_, to, start, end) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params["start"] == start
        assert request.url.params["end"] == end
        return httpx.Response(200, json={"data": [], "has_more": False, "next_cursor": None})

    client = make_client(handler)
    try:
        assert client.raw(source="binance", market="BTC-USDT", from_=from_, to=to) == []
    finally:
        client.close()


def test_older_raw_range_maps_server_authentication_error() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        assert request.url.path == "/raw"
        assert "authorization" not in request.headers
        return httpx.Response(401, json={"error": "API key required for older raw history"})

    client = make_client(handler, api_key=None)
    try:
        with pytest.raises(UnauthorizedError):
            client.raw(
                source="binance",
                market="BTC-USDT",
                from_="2024-01-01T00:00:00Z",
                to="2024-01-01T01:00:00Z",
            )
        assert called is True
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


def test_ohlcv_rejects_old_format_option() -> None:
    client = make_client(lambda request: httpx.Response(500))
    try:
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            client.ohlcv(
                source="binance",
                market="BTC-USDT",
                start=1_704_067_200_000,
                end=1_704_067_260_000,
                interval="1m",
                format="tradingview",
            )
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


def test_events_pages_mixed_rows_and_requires_api_key(tmp_path) -> None:
    identity = {"source": "binance", "market": "BTC-USDT", "collector_timestamp": 10,
                "source_capture_id": "capture", "schema_version": 1}
    trade = {"type": "trade", "data": {**identity, "event_id": "t1", "price": 100.0, "quantity": 2.0}}
    funding = {"type": "funding_rate", "data": {**identity, "event_id": "f1", "funding_rate": None,
                                                "mark_price": "100"}}
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/events"
        assert request.headers.get("authorization") == "Bearer polaris_key_test"
        assert request.url.params["start"] == request.url.params["end"] == "10"
        assert request.url.params["types"] == "trade,funding_rate"
        assert request.url.params["instrument"] == "BTCUSDT"
        second = request.url.params.get("cursor") == "next"
        return httpx.Response(200, json={"items": [funding if second else trade],
                                         "has_more": not second,
                                         "next_cursor": None if second else "next"})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = list(client.events(start=10, end=10, types=["trade", "funding_rate"],
                                  source="binance", market="BTC-USDT", instrument="BTCUSDT"))
        assert [row["type"] for row in rows] == ["trade", "funding_rate"]
        assert rows[0]["data"]["event_id"] == "t1"
        assert rows[0]["data"]["price"] == 100.0
        assert rows[1]["data"]["funding_rate"] is None
        assert rows[1]["data"]["mark_price"] == "100"
        assert requests[1].url.params["cursor"] == "next"
        with pytest.raises(ValueError, match="start and end"):
            client.events(start=11, end=10)
    finally:
        client.close()

    anonymous = make_client(lambda _: pytest.fail("request should not be sent"), api_key="", dataset_root=tmp_path)
    try:
        with pytest.raises(UnauthorizedError):
            list(anonymous.events(start=10, end=10))
    finally:
        anonymous.close()


def test_l2_direct_routes_paginate_and_keep_nullable_levels(tmp_path) -> None:
    calls: list[httpx.Request] = []
    snapshot, delta = _l2_row(True), _l2_row(False)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["authorization"] == "Bearer polaris_key_test"
        if request.url.path == "/l2-updates":
            second = "cursor" in request.url.params
            return httpx.Response(200, json={
                "items": [delta if second else snapshot],
                "has_more": not second,
                "next_cursor": None if second else "next",
            })
        assert request.url.path == "/l2-orderbooks"
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
        if request.url.path == "/trades":
            next_page = request.url.params.get("cursor") == "next"
            return httpx.Response(200, json={
                "items": [{**trade, "event_id": "t2" if next_page else "t1"}],
                "has_more": not next_page,
                "next_cursor": None if next_page else "next",
            })
        if request.url.path == "/options-ticker":
            return httpx.Response(200, json={"items": [option], "has_more": False, "next_cursor": None})
        if request.url.path in {"/funding-rates", "/perpetual-ticker"}:
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
        assert calls[4].url.path == "/perpetual-ticker"
        assert "start" not in calls[3].url.params
        assert "end" not in calls[3].url.params
        with pytest.raises(ValueError, match="instrument must be non-empty"):
            client.option_tickers(instrument="")
    finally:
        client.close()


def test_direct_historical_older_range_maps_missing_authentication(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trades"
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
        "raw_table": "raw.binance_trades",
        "capture_id": "c1", "collector_timestamp": 10, "recorder_version": "v1",
        "ingested_at": 11, "additional_context": {"channel": "trades"},
        "original_json": '{ "price": 1.0 }',
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/raw"
        assert request.url.params["source"] == "binance"
        assert request.url.params["channel"] == "trades"
        assert request.url.params["market"] == "BTC-USDT"
        assert request.url.params["start"] == "1970-01-01T00:00:00.010Z"
        assert request.url.params["end"] == "1970-01-01T00:00:00.010Z"
        assert request.url.params["limit"] == "1000"
        next_page = request.url.params.get("cursor") == "next"
        return httpx.Response(200, json={
            "data": [{**capture, "capture_id": "c2" if next_page else "c1"}],
            "has_more": not next_page,
            "next_cursor": None if next_page else "next",
        })

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = list(client.raw_channel(exchange="binance", event="trades", start=10, end=10, market="BTC-USDT"))
        assert [row["capture_id"] for row in rows] == ["c1", "c2"]
        assert rows[0]["original_json"] == '{ "price": 1.0 }'
        assert rows[0]["raw_table"] == "raw.binance_trades"
        assert rows[0]["additional_context"] == {"channel": "trades"}
        assert calls[0].headers["authorization"] == "Bearer polaris_key_test"
        assert calls[1].url.params["cursor"] == "next"
        with pytest.raises(ValueError, match="exchange and event"):
            list(client.raw_channel(exchange="", event="trades", start=10, end=10))
    finally:
        client.close()


def test_intents_queries_direct_route_with_exact_filter(tmp_path) -> None:
    calls: list[httpx.Request] = []
    intent = {"event_id": "i1", "source": "uniswapx", "market": "intents",
              "source_capture_id": "capture", "collector_timestamp": 10,
              "schema_version": 1, "intent_id": "intent-1", "input_asset_id": None}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/intents"
        return httpx.Response(200, json={"items": [intent], "has_more": False, "next_cursor": None})

    client = make_client(handler, dataset_root=tmp_path)
    try:
        rows = list(client.intents(intent_id="intent-1"))
        assert len(rows) == 1
        assert all(rows[0][key] == value for key, value in intent.items())
        assert calls[0].url.params["intent_id"] == "intent-1"
        assert calls[0].headers["authorization"] == "Bearer polaris_key_test"
        with pytest.raises(PolarisError, match="intent_id must be non-empty"):
            list(client.intents(intent_id=" "))
    finally:
        client.close()


def test_ohlcv_returns_every_venue_candle_revision(tmp_path) -> None:
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
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ohlcv"
        assert request.url.params["source"] == "binance"
        assert request.url.params["start"] == str(start)
        assert request.url.params["end"] == str(start + 60_000)
        assert request.url.params["interval"] == "1m"
        assert request.url.params["instrument"] == "BTCUSDT"
        second = request.url.params.get("cursor") == "next"
        return httpx.Response(200, json={"items": [final if second else first],
                                          "has_more": not second,
                                          "next_cursor": None if second else "next"})

    client = make_client(handler, dataset_root=tmp_path)
    options = {"source": "binance", "market": "BTC-USDT", "instrument": "BTCUSDT",
               "interval": "1m", "start": start, "end": start + 60_000}
    try:
        rows = list(client.ohlcv(**options))
        assert [row["event_id"] for row in rows] == ["open", "closed"]
        assert rows[0]["base_volume"] == 1.0
        assert rows[1]["base_volume"] == 2.0
        assert rows[1]["close"] == 102.0
    finally:
        client.close()


def test_raw_paginates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert request.url.params["source"] == "binance"
        assert request.url.params["market"] == "BTC-USDT"
        assert request.url.params["channel"] == "trades"
        assert request.url.params["start"] == "2024-01-01T00:00:00Z"
        assert request.url.params["end"] == "2024-01-01T01:00:00Z"
        assert "format" not in request.url.params
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
            channel="trades",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
            limit=1,
        ) == [{"exchange_payload": {"id": 10}}, {"exchange_payload": {"id": 11}}]
    finally:
        client.close()


def test_raw_uses_paged_json_without_file_export() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raw"
        assert "format" not in request.url.params
        assert request.url.params.get("source") == "binance"
        assert "market" not in request.url.params
        assert "channel" not in request.url.params
        return httpx.Response(200, json={
            "data": [{"raw_table": "raw.binance_trades", "capture_id": "42"}],
            "has_more": False,
            "next_cursor": None,
        })

    client = make_client(handler)
    try:
        assert client.raw(
            source="binance",
            from_="2024-01-01T00:00:00Z",
            to="2024-01-01T01:00:00Z",
        ) == [{"raw_table": "raw.binance_trades", "capture_id": "42"}]
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------


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
