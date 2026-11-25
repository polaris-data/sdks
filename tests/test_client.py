from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from time import sleep

import pytest

from fixture_server import EXACT, KEY, TABLE, server
from polaris_data import (
    AccessDeniedError,
    PolarisClient,
    PolarisError,
    RateLimitedError,
    StreamProtocolError,
    UnauthorizedError,
)


@pytest.fixture
def endpoint():
    with server() as fixture:
        reader = Thread(target=fixture.serve_forever, daemon=True)
        reader.start()
        try:
            yield f"http://127.0.0.1:{fixture.server_port}"
        finally:
            fixture.shutdown()
            reader.join()


def test_production_routes_and_lazy_pagination(endpoint):
    with PolarisClient(KEY, base_url=endpoint) as client:
        assert client.health() == {"ok": True}
        assert client.openapi()["openapi"] == "3.1.0"
        assert client.coverage(database="standard")["tables"][0]["row_count"] == 9_007_199_254_740_993
        assert client.sql("SELECT 1")["data"] == [[1]]
        assert client.candles("uniswap-v3", 1, "pool", "1m", 0, 60_000)["data"] == [{"close": EXACT}]
        page = client.data(TABLE, filters={"market.eq": "BTC/USDC + test"})
        assert page["next_cursor"] == "page:1+/"
        assert list(client.data_rows(TABLE)) == [
            {"id": "18446744073709551614", "price": EXACT},
            {"id": "18446744073709551615", "price": None},
        ]
        assert len(list(client.data_rows(TABLE, cursor=page["next_cursor"]))) == 1


def test_structured_errors_and_cursor_loops(endpoint, monkeypatch):
    monkeypatch.delenv("POLARIS_API_KEY", raising=False)
    with PolarisClient(base_url=endpoint) as anonymous:
        with pytest.raises(UnauthorizedError):
            anonymous.data(TABLE)
    with PolarisClient(KEY, base_url=endpoint, timeout=0.1) as client:
        with pytest.raises(AccessDeniedError) as failure:
            client.data(TABLE, filters={"scenario": "forbidden"})
        assert failure.value.status_code == 403
        assert failure.value.code == "forbidden"
        assert failure.value.resolution == "Test resolution"
        with pytest.raises(RateLimitedError) as failure:
            client.coverage(table="rate-limited")
        assert failure.value.reset_at == "60"
        with pytest.raises(PolarisError, match="Invalid Polaris response"):
            client.data(TABLE, filters={"scenario": "malformed"})
        with pytest.raises(PolarisError, match="repeated a cursor"):
            list(client.data_rows(TABLE, filters={"scenario": "repeat"}))
        with pytest.raises(PolarisError, match="timed out"):
            client.data(TABLE, filters={"scenario": "slow"})


def test_stream_reconnect_and_close(endpoint):
    with PolarisClient(KEY, base_url=endpoint) as client:
        with client.stream([{"table": TABLE}]) as stream:
            assert [page["cursor"] for page in stream] == ["stream:1", "stream:2"]
        with client.stream([{"table": TABLE, "cursor": "stream:1"}]) as stream:
            assert next(stream)["caught_up"]
        with client.stream([{"table": TABLE, "filters": {"scenario": "quiet"}}]) as stream:
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(next, stream, None)
                sleep(0.2)
                stream.close()
                assert pending.result(timeout=2) is None
        with client.stream([{"table": TABLE, "filters": {"scenario": "malformed"}}]) as stream:
            with pytest.raises(StreamProtocolError):
                next(stream)
