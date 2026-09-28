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
    if method in {"trades", "funding_rates", "mark_prices"}:
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
    assert [row["timestamp"] for row in interval_bbo] == [START_MS, START_MS + 60_000]
    assert [row["bid_quantity"] for row in interval_bbo] == [7.0, 6.0]


def test_propamm_quote_ladder_exact_batches_and_dataframe_are_filtered(tmp_path) -> None:
    fixture = (
        Path(__file__).parent
        / "fixtures"
        / "events"
        / "propamm-fermiswap-v2.jsonl"
    )
    source = "fermiswap"
    market = "ethereum"
    key = f"standard-{source}-{market}-{DAY}-000000"
    path = (
        tmp_path
        / "data"
        / "standard"
        / source
        / market
        / DAY
        / f"{key}.jsonl.zst"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with zstandard.open(path, "wt", encoding="utf-8") as output:
        output.write(fixture.read_text())
    path.with_name(path.name + ".coverage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "key": key,
                "start_us": START_MS * 1_000,
                "end_us": (START_MS + 3_600_000) * 1_000,
            }
        )
    )
    kwargs = {
        "source": source,
        "market": market,
        "from_": START_MS * 1_000,
        "to": (START_MS + 3_600_000) * 1_000,
    }

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        batches = list(
            client.propamm_quote_ladders(
                **kwargs,
                output="batches",
                batch_size=1,
            )
        )
        dataframe = client.propamm_quote_ladders(**kwargs, output="dataframe")

    assert [batch.num_rows for batch in batches] == [1]
    table = pa.Table.from_batches(batches)
    assert table.column("market").to_pylist() == ["ethereum"]
    assert table.column("type").to_pylist() == ["record"]
    raw_event = json.loads(table.column("event_json")[0].as_py())
    assert "market" not in raw_event
    assert raw_event["data"]["values"]["quotes"][0]["amount_in"] == str(
        2**256 - 1
    )
    assert len(dataframe) == 1
    assert dataframe.iloc[0]["market"] == "ethereum"


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


def test_point_series_columnar_outputs_use_endpoint_value_names(tmp_path) -> None:
    row = {"event_id": "mark", "source": SOURCE, "market": MARKET,
           "collector_timestamp": START_MS, "source_capture_id": "capture",
           "schema_version": 1, "mark_price": "43123.5"}
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter([row])
        marks = list(_query(client, "mark_prices", output="batches"))

    assert "collector_timestamp" in marks[0].schema.names
    assert marks[0].column("mark_price").to_pylist() == ["43123.5"]


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
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter(())
        frame = _query(client, "mark_prices", output="dataframe")

    assert frame.empty
    assert "collector_timestamp" in frame.columns
    assert "mark_price" in frame.columns


def test_aggregate_dataframes_have_stable_notebook_ready_schemas(tmp_path) -> None:
    _write_fixture(
        tmp_path,
        [
            {
                "timestamp": START_MS,
                "type": "trade",
                "data": {"price": 100.0, "quantity": 1.0},
            },
            {
                "timestamp": START_MS + 1,
                "type": "trade",
                "data": {"price": 101.0, "quantity": 2.0},
            },
            {
                "timestamp": START_MS + 2,
                "type": "trade",
                "data": {"price": 99.0, "quantity": 3.0},
            },
        ],
    )

    with PolarisClient(dataset_root=tmp_path, base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: [{
            "timestamp": START_MS, "open": 100.0, "high": 101.0, "low": 99.0,
            "close": 100.0, "volume": 6.0, "trades": 3, "vwap": 100.0,
            "quote_volume": 600.0, "volatility": 0.1, "returns": 2,
        } if method != "volume" else {"timestamp": START_MS, "volume": 6.0}]
        ohlcv = _query(client, "ohlcv", interval="100ms", output="dataframe")
        volume = _query(client, "volume", interval="100ms", output="dataframe")
        vwap = _query(client, "vwap", interval="100ms", output="dataframe")
        volatility = _query(
            client,
            "volatility",
            interval="100ms",
            output="dataframe",
        )
        records = _query(client, "volume", interval="100ms")

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
    assert volume.columns.tolist() == ["timestamp", "volume"]
    assert vwap.columns.tolist() == [
        "timestamp",
        "vwap",
        "volume",
        "quote_volume",
        "trades",
    ]
    assert volatility.columns.tolist() == ["timestamp", "volatility", "returns"]
    for frame in [ohlcv, volume, vwap, volatility]:
        assert str(frame.dtypes["timestamp"]) == "datetime64[ms, UTC]"
        assert frame.index.equals(pd.RangeIndex(len(frame)))
    assert str(ohlcv.dtypes["trades"]) == "uint64"
    assert str(volatility.dtypes["returns"]) == "uint64"
    assert isinstance(records, list)
    assert records == [{"timestamp": START_MS, "volume": 6.0}]


@pytest.mark.parametrize(
    ("method", "columns"),
    [
        (
            "ohlcv",
            ["timestamp", "open", "high", "low", "close", "volume", "trades"],
        ),
        ("volume", ["timestamp", "volume"]),
        ("vwap", ["timestamp", "vwap", "volume", "quote_volume", "trades"]),
        ("volatility", ["timestamp", "volatility", "returns"]),
    ],
)
def test_empty_aggregate_dataframes_preserve_schema_and_dtypes(
    tmp_path,
    method,
    columns,
) -> None:
    _write_fixture(
        tmp_path,
        [{"timestamp": START_MS, "type": "point", "data": {"value": 1}}],
    )

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


@pytest.mark.parametrize("method", ["ohlcv", "volume", "vwap", "volatility"])
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
        "mark_prices",
        "propamm_quote_ladders",
        "bbo",
        "depth_metrics",
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
            _query(client, "volume", interval="100ms", output="dataframe")


def test_empty_direct_dataframe_keeps_flat_schema() -> None:
    with PolarisClient(base_url="http://127.0.0.1:1") as client:
        client._call = lambda method, *args: iter(())
        frame = _query(client, "funding_rates", output="dataframe")
    assert frame.empty
    assert "funding_rate" in frame.columns
    assert "collector_timestamp" in frame.columns
