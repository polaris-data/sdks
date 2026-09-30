from __future__ import annotations

import importlib

import pandas as pd
import pyarrow as pa
import pytest

import polaris_data.client as client_module
from polaris_data import PolarisClient

SOURCE = "benchmark"
MARKET = "BTC-USD"
DAY = "2024-01-01"
START_MS = 1_704_067_200_000


def _query(client: PolarisClient, method: str, **kwargs):
    if method in {"trades", "funding_rates"}:
        return getattr(client, method)(
            source=SOURCE,
            market=MARKET,
            start=START_MS,
            end=START_MS + 10,
            **kwargs,
        )
    return getattr(client, method)(
        source=SOURCE,
        market=MARKET,
        from_=START_MS * 1_000,
        to=(START_MS + 10) * 1_000,
        **kwargs,
    )


def test_direct_trade_and_funding_batches_have_flat_schemas(tmp_path) -> None:
    identity = {"event_id": "e1", "source": SOURCE, "market": MARKET,
                "collector_timestamp": START_MS, "source_capture_id": "capture",
                "schema_version": 1}
    trades = [{**identity, "price": float(100 + i), "quantity": 1.0,
               "side": None if i == 0 else "buy"} for i in range(3)]
    funding = [{**identity, "funding_rate": None, "mark_price": "100"}]
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter(trades if method == "trades" else funding)
        batches = list(_query(client, "trades", output="batches", batch_size=2))
        funding_batches = list(_query(client, "funding_rates", output="batches"))
    assert [batch.num_rows for batch in batches] == [2, 1]
    table = pa.Table.from_batches(batches)
    assert table.column("price").to_pylist() == [100.0, 101.0, 102.0]
    assert table.column("side").to_pylist() == [None, "buy", "buy"]
    assert "data" not in table.schema.names
    assert funding_batches[0].column("funding_rate").to_pylist() == [None]
    assert funding_batches[0].column("mark_price").to_pylist() == ["100"]


def test_trade_dataframe_uses_flat_api_fields(tmp_path) -> None:
    rows = [{"event_id": "e1", "source": SOURCE, "market": MARKET,
             "collector_timestamp": START_MS, "source_capture_id": "capture",
             "schema_version": 1, "price": 100.0, "quantity": 1.0, "side": None}]
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter(rows)
        frame = _query(client, "trades", output="dataframe", batch_size=1)
    assert isinstance(frame, pd.DataFrame)
    assert frame["collector_timestamp"].tolist() == [START_MS]
    assert frame["price"].tolist() == [100.0]
    assert frame["side"].isna().all()
    assert "data" not in frame.columns












def test_aggregate_dataframes_have_stable_notebook_ready_schemas(tmp_path) -> None:

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: [{
            "timestamp": START_MS, "open": 100.0, "high": 101.0, "low": 99.0,
            "close": 100.0, "volume": 6.0, "trades": 3,
        }]
        ohlcv = _query(client, "ohlcv", interval="100ms", output="dataframe")

    assert isinstance(ohlcv, pd.DataFrame)
    assert ohlcv.columns.tolist() == [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trades",
    ]
    assert str(ohlcv.dtypes["timestamp"]) == "datetime64[ms, UTC]"
    assert ohlcv.index.equals(pd.RangeIndex(len(ohlcv)))
    assert str(ohlcv.dtypes["trades"]) == "uint64"


@pytest.mark.parametrize(
    ("method", "columns"),
    [
        (
            "ohlcv",
            ["timestamp", "open", "high", "low", "close", "volume", "trades"],
        ),
    ],
)
def test_empty_aggregate_dataframes_preserve_schema_and_dtypes(
    tmp_path,
    method,
    columns,
) -> None:

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: []
        frame = _query(client, method, interval="100ms", output="dataframe")

    assert frame.empty
    assert frame.columns.tolist() == columns
    assert str(frame.dtypes["timestamp"]) == "datetime64[ms, UTC]"
    assert all(
        str(frame.dtypes[column]) == "float64"
        for column in columns[1:]
        if column not in {"trades", "returns"}
    )
    assert all(
        str(frame.dtypes[column]) == "uint64"
        for column in columns
        if column in {"trades", "returns"}
    )


@pytest.mark.parametrize("method", ["ohlcv"])
def test_aggregate_output_is_validated_before_query(method) -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ValueError, match="output must be one of"):
            _query(client, method, interval="100ms", output="table")


def test_ohlcv_dataframe_rejects_tradingview_format() -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ValueError, match="only available when format is None"):
            _query(
                client,
                "ohlcv",
                interval="100ms",
                format="tradingview",
                output="dataframe",
            )


@pytest.mark.parametrize(
    "method",
    [
        "trades",
        "funding_rates",
    ],
)
def test_columnar_options_are_validated_before_query(method) -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ValueError, match="output must be one of"):
            _query(client, method, output="table")
        with pytest.raises(ValueError, match="batch_size must be greater than 0"):
            _query(client, method, output="batches", batch_size=0)
        with pytest.raises(TypeError, match="batch_size must be an integer"):
            _query(client, method, output="batches", batch_size=True)


def test_missing_optional_dependencies_fail_before_query(monkeypatch) -> None:
    original_import_module = importlib.import_module

    def missing_pyarrow(name: str, package=None):
        if name == "pyarrow":
            raise ImportError("missing")
        return original_import_module(name, package)

    monkeypatch.setattr(client_module.importlib, "import_module", missing_pyarrow)
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ImportError, match=r"polaris-data\[arrow\]"):
            _query(client, "trades", output="batches")
        with pytest.raises(ImportError, match=r"polaris-data\[dataframe\]"):
            _query(client, "trades", output="dataframe")


def test_dataframe_checks_pandas_before_query(monkeypatch) -> None:
    original_import_module = importlib.import_module

    def missing_pandas(name: str, package=None):
        if name == "pandas":
            raise ImportError("missing")
        return original_import_module(name, package)

    monkeypatch.setattr(client_module.importlib, "import_module", missing_pandas)
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ImportError, match=r"polaris-data\[dataframe\]"):
            _query(client, "trades", output="dataframe")
        with pytest.raises(ImportError, match=r"polaris-data\[dataframe\]"):
            _query(client, "ohlcv", interval="100ms", output="dataframe")


def test_empty_direct_dataframe_keeps_flat_schema() -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter(())
        frame = _query(client, "funding_rates", output="dataframe")
    assert frame.empty
    assert "funding_rate" in frame.columns
    assert "collector_timestamp" in frame.columns
