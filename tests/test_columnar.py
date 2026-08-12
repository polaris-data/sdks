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


def test_exact_event_batches_preserve_precision_order_and_unknown_payloads(tmp_path) -> None:
    exact_timestamp = START_MS * 1_000 + 123
    rows = [
        {
            "timestamp": exact_timestamp,
            "type": "trade",
            "sequence": "7",
            "sequence_scope": "book-channel-1",
            "receive_timestamp_us": exact_timestamp + 10,
            "data": {"price": 100, "quantity": 2, "side": "buy"},
        },
        {
            "timestamp": exact_timestamp,
            "type": "orderbook_delta",
            "data": {"bids": [[99.5, 3]], "asks": []},
        },
        {
            "timestamp": exact_timestamp + 1,
            "type": "venue_specific",
            "data": {"nested": {"untouched": [1, 2, 3]}},
            "opaque": {"also": "preserved"},
        },
    ]
    _write_fixture(tmp_path, rows)

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        event_batches = list(
            _query(
                client,
                "events",
                output="batches",
                batch_size=2,
                materialize_orderbooks=False,
            )
        )
        replay_batches = list(
            client.replay(
                source=SOURCE,
                market=MARKET,
                from_=START_MS * 1_000,
                to=(START_MS + 10) * 1_000,
                output="batches",
                batch_size=1,
                materialize_orderbooks=False,
            )
        )

    assert [batch.num_rows for batch in event_batches] == [2, 1]
    table = pa.Table.from_batches(event_batches)
    replay_table = pa.Table.from_batches(replay_batches)
    assert table.combine_chunks().equals(replay_table.combine_chunks())
    assert table.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.column("timestamp").cast(pa.int64()).to_pylist() == [
        exact_timestamp,
        exact_timestamp,
        exact_timestamp + 1,
    ]
    assert table.column("collector_timestamp").cast(pa.int64()).to_pylist() == [None] * 3
    assert table.column("collector_sequence").to_pylist() == [None] * 3
    assert table.column("exchange_timestamp").cast(pa.int64()).to_pylist() == [None] * 3
    assert table.column("exchange_sequence").to_pylist() == [None] * 3
    assert table.column("replay_ordinal").to_pylist() == [0, 1, 2]
    assert table.column("source_file_ordinal").to_pylist() == [0, 0, 0]
    assert table.column("source_row_ordinal").to_pylist() == [0, 1, 2]
    assert table.column("trade_price").to_pylist() == [100.0, None, None]
    assert table.column("order_id").to_pylist() == [None, None, None]
    assert table.column("side").to_pylist() == ["buy", None, None]
    assert table.column("is_snapshot").to_pylist() == [None, None, None]
    assert table.column("bids").to_pylist() == [
        None,
        [{"price": 99.5, "quantity": 3.0}],
        None,
    ]
    assert [json.loads(value) for value in table.column("event_json").to_pylist()] == rows


def test_v2_exact_batches_use_mixed_envelope_schema_and_metadata_ordinal(tmp_path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / "schema-v2.jsonl"
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    key = f"standard-lighter-BTC-USD-{DAY}-000000"
    path = tmp_path / "data" / "standard" / "lighter" / "BTC-USD" / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zstandard.open(path, "wt", encoding="utf-8") as output:
        output.write(fixture.read_text())
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps({
            "version": 1,
            "key": key,
            "start_us": START_MS * 1_000,
            "end_us": (START_MS + 120_000) * 1_000,
        })
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        table = pa.Table.from_batches(list(client.events(
            source="lighter",
            market="BTC-USD",
            from_=START_MS * 1_000,
            to=(START_MS + 1_000) * 1_000,
            output="batches",
            materialize_orderbooks=False,
        )))
        filtered = list(client.events(
            source="lighter",
            market="BTC-USD",
            from_=(START_MS + 150) * 1_000,
            to=(START_MS + 250) * 1_000,
            materialize_orderbooks=False,
        ))
        trades = list(client.trades(
            source="lighter",
            market="BTC-USD",
            from_=START_MS * 1_000,
            to=(START_MS + 1_000) * 1_000,
        ))
        interval_bbo = list(client.bbo(
            source="lighter",
            market="BTC-USD",
            from_=START_MS * 1_000,
            to=(START_MS + 120_000) * 1_000,
            interval="1m",
        ))

    selected_rows = rows[1:7] + [rows[8]]
    assert table.num_rows == 7
    assert table.column("timestamp").cast(pa.int64()).to_pylist() == [None] * 7
    assert table.column("collector_timestamp").cast(pa.int64()).to_pylist() == [
        (START_MS + 100) * 1_000,
        (START_MS + 300) * 1_000,
        (START_MS + 200) * 1_000,
        (START_MS + 400) * 1_000,
        (START_MS + 500) * 1_000,
        (START_MS + 450) * 1_000,
        (START_MS + 550) * 1_000,
    ]
    assert table.column("collector_sequence").to_pylist() == [7, 9, 15, 21, 22, 25, 31]
    assert table.column("exchange_timestamp").cast(pa.int64()).to_pylist() == [
        (START_MS - 1_000) * 1_000,
        None,
        (START_MS - 2_000) * 1_000,
        None,
        (START_MS - 3_000) * 1_000,
        (START_MS - 4_000) * 1_000,
        (START_MS - 5_000) * 1_000,
    ]
    assert table.column("exchange_sequence").to_pylist() == [
        "book-1", None, "trade-1", None, "trade-2", "trade-3", "book-3",
    ]
    assert table.column("source_row_ordinal").to_pylist() == [1, 2, 3, 4, 5, 6, 8]
    assert table.column("order_id").to_pylist() == [None, None, None, None, "order-2", None, None]
    assert table.column("side").to_pylist() == [None, None, None, None, "buy", "sell", None]
    assert table.column("is_snapshot").to_pylist() == [True, False, None, None, None, None, False]
    assert [json.loads(value) for value in table.column("event_json").to_pylist()] == selected_rows
    assert [row["type"] for row in filtered] == ["trade"]
    assert "timestamp" not in filtered[0]
    assert filtered[0]["collector_timestamp"] == START_MS + 200
    assert trades == [rows[3], rows[5], rows[6]]
    assert [row["timestamp"] for row in interval_bbo] == [START_MS, START_MS + 60_000]
    assert [row["bid_quantity"] for row in interval_bbo] == [7.0, 6.0]


def test_shared_headerless_legacy_fixture_preserves_native_rows(tmp_path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "events" / "legacy-v1.jsonl"
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    key = f"standard-lighter-BTC-USD-{DAY}-000000"
    path = tmp_path / "data" / "standard" / "lighter" / "BTC-USD" / DAY / f"{key}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zstandard.open(path, "wt", encoding="utf-8") as output:
        output.write(fixture.read_text())
    path.with_name(path.name + ".coverage.json").write_text(json.dumps({
        "version": 1,
        "key": key,
        "start_us": START_MS * 1_000,
        "end_us": (START_MS + 1_000) * 1_000,
    }))

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        decoded = list(client.events(
            source="lighter",
            market="BTC-USD",
            from_=START_MS * 1_000,
            to=(START_MS + 1_000) * 1_000,
            materialize_orderbooks=False,
        ))

    assert decoded == rows


def test_raw_replay_rejects_columnar_output() -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ValueError, match="standardized replay"):
            client.replay(
                source=SOURCE,
                market=MARKET,
                standard=False,
                output="batches",
            )


def test_event_batch_boundaries_do_not_reset_materialized_books(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS * 1_000,
                "type": "orderbook",
                "data": {"bids": [[100, 1]], "asks": [[101, 2]]},
            },
            {
                "timestamp": START_MS * 1_000 + 1,
                "type": "orderbook_delta",
                "data": {"bids": [[100, 3]]},
            },
            {
                "timestamp": START_MS * 1_000 + 2,
                "type": "orderbook_delta",
                "data": {"asks": [[101, 0], [100.5, 4]]},
            },
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        batches = list(
            _query(
                client,
                "events",
                output="batches",
                batch_size=1,
                materialize_orderbooks=True,
            )
        )

    assert [batch.num_rows for batch in batches] == [1, 1, 1]
    table = pa.Table.from_batches(batches)
    assert table.column("replay_ordinal").to_pylist() == [0, 1, 2]
    assert table.column("bids").to_pylist()[-1] == [
        {"price": 100.0, "quantity": 3.0}
    ]
    assert table.column("asks").to_pylist()[-1] == [
        {"price": 100.5, "quantity": 4.0}
    ]
    last_event = json.loads(table.column("event_json")[-1].as_py())
    assert last_event["timestamp"] == START_MS * 1_000 + 2
    assert last_event["type"] == "orderbook"


def test_exact_event_dataframe_keeps_microsecond_timestamp(tmp_path) -> None:
    exact_timestamp = START_MS * 1_000 + 321
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": exact_timestamp,
                "type": "venue_specific",
                "data": {"value": "preserved"},
            }
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        frame = _query(
            client,
            "events",
            output="dataframe",
            materialize_orderbooks=False,
        )

    assert str(frame.dtypes["timestamp"]) == "datetime64[us, UTC]"
    assert int(frame.iloc[0]["timestamp"].value // 1_000) == exact_timestamp
    assert json.loads(frame.iloc[0]["event_json"])["data"] == {
        "value": "preserved"
    }


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
