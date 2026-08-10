from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
import zstandard

import polaris_data.client as client_module
from polaris_data import PolarisClient, StreamDecodeError

SOURCE = "benchmark"
MARKET = "BTC-USD"
DAY = "2024-01-01"
START_MS = 1_704_067_200_000


def _write_fixture(root: Path, rows: list[dict]) -> Path:
    key = f"standard-{SOURCE}-{MARKET}-{DAY}-000000"
    path = root / "data" / "standard" / SOURCE / MARKET / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zstandard.open(path, "wt", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": START_MS * 1_000,
                "end_us": (START_MS + 10) * 1_000,
            }
        )
    )
    return path


def _query(client: PolarisClient, method: str, **kwargs):
    return getattr(client, method)(
        source=SOURCE,
        market=MARKET,
        from_=START_MS * 1_000,
        to=(START_MS + 10) * 1_000,
        **kwargs,
    )


def test_trade_batches_are_typed_bounded_and_preserve_dynamic_fields(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "trade",
                "data": {
                    "price": 100,
                    "quantity": 1,
                    "side": "buy",
                    "mixed": 1,
                },
            },
            {
                "timestamp": START_MS + 1,
                "type": "trade",
                "data": {
                    "price": 101,
                    "quantity": 2,
                    "side": "sell",
                    "mixed": "two",
                    "late": 7,
                },
            },
            {
                "timestamp": START_MS + 2,
                "type": "trade",
                "data": {
                    "price": 102,
                    "quantity": 3,
                    "nested": {"b": 2, "a": 1},
                },
            },
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        batches = list(_query(client, "trades", output="batches", batch_size=2))

    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    assert [batch.num_rows for batch in batches] == [2, 1]
    assert batches[0].schema == batches[1].schema
    assert batches[0].schema.names == [
        "timestamp",
        "source",
        "market",
        "price",
        "quantity",
        "side",
        "extra.late",
        "extra.mixed",
        "extra.nested",
    ]
    assert batches[0].schema.field("timestamp").type == pa.timestamp("ms", tz="UTC")
    assert pa.types.is_dictionary(batches[0].schema.field("source").type)
    assert pa.types.is_dictionary(batches[0].schema.field("side").type)
    table = pa.Table.from_batches(batches)
    assert table.column("source").to_pylist() == [SOURCE] * 3
    assert table.column("market").to_pylist() == [MARKET] * 3
    assert table.column("extra.late").to_pylist() == [None, 7, None]
    assert table.column("extra.mixed").to_pylist() == ["1", '"two"', None]
    assert table.column("extra.nested").to_pylist() == [None, None, '{"a":1,"b":2}']


def test_trade_dataframe_has_notebook_ready_dtypes(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "trade",
                "data": {"price": 100, "quantity": 1, "side": "buy"},
            },
            {
                "timestamp": START_MS + 1,
                "type": "trade",
                "data": {"price": 101, "quantity": 2},
            },
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        frame = _query(client, "trades", output="dataframe", batch_size=1)

    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == [
        "timestamp",
        "source",
        "market",
        "price",
        "quantity",
        "side",
    ]
    assert str(frame.dtypes["timestamp"]) == "datetime64[ms, UTC]"
    assert isinstance(frame.dtypes["source"], pd.CategoricalDtype)
    assert isinstance(frame.dtypes["market"], pd.CategoricalDtype)
    assert isinstance(frame.dtypes["side"], pd.CategoricalDtype)
    assert frame.index.equals(pd.RangeIndex(2))
    assert frame["price"].tolist() == [100.0, 101.0]


def test_point_series_columnar_outputs_use_endpoint_value_names(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "point",
                "data": {
                    "series": "funding_rate",
                    "value": 0.0001,
                    "interval_seconds": 28_800,
                },
            },
            {
                "timestamp": START_MS + 1,
                "type": "point",
                "data": {
                    "series": "mark_price",
                    "value": 43_123.5,
                    "estimated": True,
                },
            },
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        funding = _query(client, "funding_rates", output="dataframe")
        marks = list(_query(client, "mark_prices", output="batches"))

    assert funding.columns.tolist() == [
        "timestamp",
        "source",
        "market",
        "funding_rate",
        "extra.interval_seconds",
    ]
    assert funding["funding_rate"].tolist() == [0.0001]
    assert marks[0].schema.names == [
        "timestamp",
        "source",
        "market",
        "mark_price",
        "extra.estimated",
    ]
    assert marks[0].column("mark_price").to_pylist() == [43_123.5]


def test_bbo_and_depth_metrics_have_fixed_columnar_schemas(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "l2_snapshot",
                "data": {
                    "bids": [[100.0, 2.0], [99.5, 3.0]],
                    "asks": [[100.5, 1.0], [101.0, 2.0]],
                },
            }
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        bbo = _query(client, "bbo", output="dataframe")
        depth = list(
            _query(
                client,
                "depth_metrics",
                output="batches",
                slippage_notional=1_000_000,
            )
        )

    assert bbo.columns.tolist() == [
        "timestamp",
        "source",
        "market",
        "bid_price",
        "bid_quantity",
        "ask_price",
        "ask_quantity",
    ]
    assert bbo.loc[0, "source"] == SOURCE
    assert depth[0].schema.names[:8] == [
        "timestamp",
        "source",
        "market",
        "bid_price",
        "ask_price",
        "mid_price",
        "bid_ask_spread",
        "bid_ask_spread_bps",
    ]
    assert depth[0].column("buy_average_price").null_count == 1


def test_empty_dataframe_preserves_schema_and_dtypes(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "trade",
                "data": {"price": 100, "quantity": 1},
            }
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        frame = _query(client, "mark_prices", output="dataframe")

    assert frame.empty
    assert frame.columns.tolist() == ["timestamp", "source", "market", "mark_price"]
    assert str(frame.dtypes["timestamp"]) == "datetime64[ms, UTC]"
    assert isinstance(frame.dtypes["source"], pd.CategoricalDtype)


@pytest.mark.parametrize("method", ["trades", "funding_rates", "mark_prices", "bbo", "depth_metrics"])
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


def test_schema_change_between_inference_and_emission_is_translated(tmp_path) -> None:
    path = _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "trade",
                "data": {"price": 100, "quantity": 1, "sequence": 1},
            }
        ],
    )
    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        batches = _query(client, "trades", output="batches")
        with zstandard.open(path, "wt", encoding="utf-8") as output:
            output.write(
                json.dumps(
                    {
                        "timestamp": START_MS,
                        "type": "trade",
                        "data": {"price": 100, "quantity": 1, "sequence": True},
                    }
                )
                + "\n"
            )
        with pytest.raises(StreamDecodeError, match="columnar schema changed"):
            next(batches)
