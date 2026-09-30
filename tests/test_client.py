from __future__ import annotations

import json
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
    PolarisError,
    RateLimitedError,
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
