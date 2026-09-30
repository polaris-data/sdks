"""Contract tests for the documented Python SDK surface."""

from __future__ import annotations

import importlib.util
import inspect
import json
from typing import Any, Literal, get_args, get_type_hints

import pytest
import polaris_data
from polaris_data import (
    AccessDeniedError,
    AmountKind,
    AssetAmount,
    CatalogAccess,
    CatalogCount,
    CatalogInstrument,
    CatalogMarketEntry,
    CatalogResponse,
    InstrumentsResponse,
    ObservedStatistic,
    OptionContract,
    OptionContractStatistics,
    FundingRateRow,
    IntentRow,
    OhlcvRow,
    OrderbookL2Row,
    QuoteRow,
    IntentData,
    IntentEvent,
    IntentEventV2,
    IntentQuote,
    IntentStatus,
    LegacyIntentEvent,
    LegacyOptionTickerEvent,
    LegacyPerpetualTickerEvent,
    LegacyTradeData,
    LegacyTradeEvent,
    NotFoundError,
    OptionGreeks,
    OptionTickerData,
    OptionTickerEvent,
    OptionTickerEventV2,
    OptionTickerRow,
    PerpetualTickerData,
    PerpetualTickerEvent,
    PerpetualTickerEventV2,
    OrderbookBuilder,
    PolarisClient,
    PolarisError,
    PropammQuote,
    PropammQuoteLadderData,
    PropammQuoteLadderEvent,
    PropammQuoteLadderValues,
    RawCaptureRow,
    RateLimitedError,
    RealtimeStream,
    SettlementTransaction,
    TradeDataV2,
    TradeEvent,
    TradeEventV2,
    TradeRow,
    StreamConnectionError,
    StreamDecodeError,
    StreamProtocolError,
    UnauthorizedError,
)
from polaris_data.models import JSONDict


def _parameters(callable_: object) -> list[tuple[str, inspect._ParameterKind, object]]:
    return [
        (parameter.name, parameter.kind, parameter.default)
        for parameter in inspect.signature(callable_).parameters.values()
    ]


def test_top_level_exports_are_stable() -> None:
    assert polaris_data.__all__ == [
        "AccessDeniedError",
        "AmountKind",
        "AssetAmount",
        "CatalogAccess",
        "CatalogCount",
        "CatalogInstrument",
        "CatalogMarketEntry",
        "CatalogResponse",
        "InstrumentsResponse",
        "ObservedStatistic",
        "OptionContract",
        "OptionContractStatistics",
        "FundingRateRow",
        "IntentRow",
        "OhlcvRow",
        "OrderbookL2Row",
        "QuoteRow",
        "LegacyOptionTickerEvent",
        "LegacyPerpetualTickerEvent",
        "LegacyTradeData",
        "LegacyTradeEvent",
        "IntentData",
        "IntentEvent",
        "IntentEventV2",
        "IntentQuote",
        "IntentStatus",
        "LegacyIntentEvent",
        "NotFoundError",
        "OptionGreeks",
        "OptionTickerData",
        "OptionTickerEvent",
        "OptionTickerEventV2",
        "OptionTickerRow",
        "PerpetualTickerData",
        "PerpetualTickerEvent",
        "PerpetualTickerEventV2",
        "OrderbookBuilder",
        "PolarisClient",
        "PolarisError",
        "PropammQuote",
        "PropammQuoteLadderData",
        "PropammQuoteLadderEvent",
        "PropammQuoteLadderValues",
        "RawCaptureRow",
        "RateLimitedError",
        "RealtimeStream",
        "SettlementTransaction",
        "TradeDataV2",
        "TradeEvent",
        "TradeEventV2",
        "TradeRow",
        "StreamDecodeError",
        "StreamConnectionError",
        "StreamProtocolError",
        "UnauthorizedError",
    ]


def test_client_constructor_signature_and_defaults_are_stable() -> None:
    positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert _parameters(PolarisClient) == [
        ("api_key", positional, None),
        ("base_url", positional, "https://api.polaris.supply"),
        ("timeout", positional, 30.0),
        ("dataset_root", positional, None),
        ("stream_url", positional, None),
    ]


def test_snapshot_and_replay_methods_are_removed() -> None:
    for method in ("events", "replay", "list_snapshots", "propamm_quote_ladders", "raw_replay"):
        assert not hasattr(PolarisClient, method)


def test_documented_client_method_signatures_and_defaults_are_stable() -> None:
    keyword_only = inspect.Parameter.KEYWORD_ONLY
    positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
    required = inspect.Parameter.empty

    assert _parameters(PolarisClient.health) == [("self", positional, required)]
    assert _parameters(PolarisClient.catalog) == [
        ("self", positional, required),
        ("source", positional, None),
        ("market", positional, None),
        ("q", positional, None),
    ]
    assert _parameters(PolarisClient.stream) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("markets", keyword_only, required),
        ("instrument", keyword_only, None),
        ("include_buffer", keyword_only, False),
        ("materialize_orderbooks", keyword_only, True),
    ]

    assert _parameters(PolarisClient.option_tickers) == [
        ("self", positional, required),
        ("source", keyword_only, None),
        ("market", keyword_only, None),
        ("instrument", keyword_only, None),
        ("start", keyword_only, None),
        ("end", keyword_only, None),
    ]

    assert _parameters(PolarisClient.intents) == [
        ("self", positional, required),
        ("source", keyword_only, None),
        ("market", keyword_only, None),
        ("instrument", keyword_only, None),
        ("intent_id", keyword_only, None),
        ("start", keyword_only, None),
        ("end", keyword_only, None),
    ]

    assert _parameters(PolarisClient.perpetual_tickers) == [
        ("self", positional, required),
        ("source", keyword_only, None),
        ("market", keyword_only, None),
        ("start", keyword_only, None),
        ("end", keyword_only, None),
    ]

    assert _parameters(PolarisClient.l2_snapshots) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("start", keyword_only, required),
        ("end", keyword_only, required),
        ("instrument", keyword_only, None),
    ]

    assert _parameters(PolarisClient.l2_updates) == [
        ("self", positional, required),
        ("source", keyword_only, None),
        ("market", keyword_only, None),
        ("instrument", keyword_only, None),
        ("start", keyword_only, None),
        ("end", keyword_only, None),
    ]

    direct_methods = [PolarisClient.trades, PolarisClient.funding_rates, PolarisClient.mark_prices]
    for method in direct_methods:
        assert _parameters(method) == [
            ("self", positional, required),
            ("source", keyword_only, None),
            ("market", keyword_only, None),
            ("start", keyword_only, None),
            ("end", keyword_only, None),
            ("output", keyword_only, "iterator"),
            ("batch_size", keyword_only, 65_536),
        ]

    assert _parameters(PolarisClient.bbo) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("start", keyword_only, required),
        ("end", keyword_only, required),
        ("interval", keyword_only, None),
        ("changes_only", keyword_only, False),
        ("output", keyword_only, "iterator"),
        ("batch_size", keyword_only, 65_536),
    ]

    assert _parameters(PolarisClient.raw) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("limit", keyword_only, 1000),
    ]
    assert _parameters(PolarisClient.raw_channel) == [
        ("self", positional, required),
        ("exchange", keyword_only, required),
        ("event", keyword_only, required),
        ("start", keyword_only, required),
        ("end", keyword_only, required),
    ]
    for method, extra in [
        (PolarisClient.intent_rows, "intent_id"),
        (PolarisClient.ohlcv_rows, "interval"),
        (PolarisClient.quote_rows, "observation_id"),
    ]:
        assert _parameters(method) == [
            ("self", positional, required),
            ("source", keyword_only, None),
            ("market", keyword_only, None),
            ("instrument", keyword_only, None),
            (extra, keyword_only, None),
            ("start", keyword_only, None),
            ("end", keyword_only, None),
        ]
    assert _parameters(PolarisClient.ohlcv) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("interval", keyword_only, required),
        ("format", keyword_only, None),
        ("allow_gaps", keyword_only, False),
        ("output", keyword_only, "records"),
    ]

    for method in [PolarisClient.volume, PolarisClient.vwap]:
        assert _parameters(method) == [
            ("self", positional, required),
            ("source", keyword_only, required),
            ("market", keyword_only, required),
            ("from_", keyword_only, None),
            ("to", keyword_only, None),
            ("interval", keyword_only, required),
            ("allow_gaps", keyword_only, False),
            ("output", keyword_only, "records"),
        ]

    assert _parameters(PolarisClient.volatility) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("from_", keyword_only, None),
        ("to", keyword_only, None),
        ("interval", keyword_only, required),
        ("method", keyword_only, "log_returns"),
        ("allow_gaps", keyword_only, False),
        ("output", keyword_only, "records"),
    ]
    assert _parameters(PolarisClient.depth_metrics) == [
        ("self", positional, required),
        ("source", keyword_only, required),
        ("market", keyword_only, required),
        ("start", keyword_only, required),
        ("end", keyword_only, required),
        ("depth_pct", keyword_only, 0.01),
        ("slippage_notional", keyword_only, 10_000.0),
        ("output", keyword_only, "iterator"),
        ("batch_size", keyword_only, 65_536),
    ]

def test_documented_result_annotations_and_models_are_stable() -> None:
    assert (
        inspect.signature(OrderbookBuilder.apply).return_annotation == "JSONDict | None"
    )
    assert inspect.signature(OrderbookBuilder.update).return_annotation == "bool"
    assert (
        inspect.signature(OrderbookBuilder.snapshot).return_annotation
        == "JSONDict | None"
    )
    assert inspect.signature(PolarisClient.health).return_annotation == "JSONDict"
    assert inspect.signature(PolarisClient.catalog).return_annotation == (
        "CatalogResponse | JSONDict"
    )
    assert inspect.signature(PolarisClient.count).return_annotation == "CatalogCount"
    assert inspect.signature(PolarisClient.instruments).return_annotation == "InstrumentsResponse"
    assert inspect.signature(PolarisClient.intents).return_annotation == "Iterator[IntentRow]"
    assert inspect.signature(PolarisClient.perpetual_tickers).return_annotation == (
        "Iterator[FundingRateRow]"
    )
    for method in [
        PolarisClient.l2_snapshots,
        PolarisClient.l2_updates,
    ]:
        assert inspect.signature(method).return_annotation == "Iterator[OrderbookL2Row]"
    for method in [
        PolarisClient.bbo,
        PolarisClient.depth_metrics,
    ]:
        assert inspect.signature(method).return_annotation == (
            "Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame"
        )
    assert inspect.signature(PolarisClient.trades).return_annotation == (
        "Iterator[TradeRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame"
    )
    assert inspect.signature(PolarisClient.funding_rates).return_annotation == (
        "Iterator[FundingRateRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame"
    )
    assert inspect.signature(PolarisClient.mark_prices).return_annotation == (
        "Iterator[FundingRateRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame"
    )
    assert inspect.signature(PolarisClient.option_tickers).return_annotation == "Iterator[OptionTickerRow]"
    assert inspect.signature(PolarisClient.raw_channel).return_annotation == "Iterator[RawCaptureRow]"
    assert inspect.signature(PolarisClient.ohlcv_rows).return_annotation == "Iterator[OhlcvRow]"
    assert inspect.signature(PolarisClient.intent_rows).return_annotation == "Iterator[IntentRow]"
    assert inspect.signature(PolarisClient.quote_rows).return_annotation == "Iterator[QuoteRow]"
    assert inspect.signature(PolarisClient.stream).return_annotation == "RealtimeStream"
    assert (
        inspect.signature(PolarisClient.ohlcv).return_annotation
        == "list[JSONDict] | JSONDict | pandas.DataFrame"
    )
    for method in [
        PolarisClient.volume,
        PolarisClient.vwap,
        PolarisClient.volatility,
    ]:
        assert inspect.signature(method).return_annotation == (
            "list[JSONDict] | pandas.DataFrame"
        )

    assert get_type_hints(CatalogResponse) == {
        "markets": list[CatalogMarketEntry],
        "updatedAt": str,
    }
    assert get_type_hints(InstrumentsResponse) == {
        "updatedAt": str,
        "instruments": list[OptionContract],
    }
    assert get_type_hints(CatalogCount) == {
        "updatedAt": str,
        "sources": int,
        "markets": int,
        "by_source": dict[str, int],
    }
    assert get_type_hints(CatalogMarketEntry)["symbol"] is str
    assert get_type_hints(PropammQuote) == {
        "amount_in": str,
        "amount_out": str,
    }
    assert get_type_hints(PropammQuoteLadderData)["values"] is PropammQuoteLadderValues
    event_hints = get_type_hints(PropammQuoteLadderEvent)
    assert event_hints["market"] is str
    assert event_hints["data"] is PropammQuoteLadderData
    intent_hints = get_type_hints(IntentData)
    assert intent_hints["inputs"] == list[AssetAmount]
    assert intent_hints["quote"] is IntentQuote
    assert intent_hints["transactions"] == list[SettlementTransaction]
    assert get_type_hints(IntentEventV2)["data"] is IntentData
    assert get_type_hints(IntentEventV2)["raw"] is Any
    assert get_type_hints(LegacyIntentEvent)["data"] is IntentData
    assert get_type_hints(LegacyIntentEvent)["raw"] is Any
    assert get_type_hints(TradeDataV2)["maker"] is str
    assert get_type_hints(TradeDataV2)["taker"] is str
    assert get_type_hints(TradeEventV2)["data"] is TradeDataV2
    assert get_type_hints(LegacyTradeEvent)["data"] is LegacyTradeData
    assert TradeRow.__required_keys__ == {
        "event_id", "source", "market", "collector_timestamp",
        "source_capture_id", "schema_version", "price", "quantity",
    }
    assert get_type_hints(TradeRow)["price"] is float
    assert get_type_hints(OptionTickerRow)["instrument"] is str
    assert get_type_hints(FundingRateRow)["funding_rate"] == str | None
    assert get_type_hints(RawCaptureRow)["original_json"] is str
    assert get_type_hints(OptionTickerData)["option_type"] == Literal["call", "put"]
    assert get_type_hints(PerpetualTickerData)["funding_timestamp"] is int
    assert get_type_hints(PerpetualTickerEventV2)["data"] is PerpetualTickerData
    assert get_type_hints(LegacyPerpetualTickerEvent)["data"] is PerpetualTickerData
    assert AmountKind == Literal["exact_input", "exact_output"]
    assert "settled" in get_args(IntentStatus)
    assert JSONDict == dict[str, Any]


def test_error_hierarchy_is_stable() -> None:
    for error in [
        UnauthorizedError,
        AccessDeniedError,
        NotFoundError,
        RateLimitedError,
        StreamDecodeError,
        StreamConnectionError,
        StreamProtocolError,
    ]:
        assert error.__bases__ == (PolarisError,)


def test_realtime_stream_is_closeable_and_unregisters_from_client() -> None:
    class NativeIterator:
        def __init__(self) -> None:
            self.closed = False
            self.rows = iter(
                [
                    {
                        "timestamp": 1,
                        "source": "afx",
                        "market": "AAPLUSDC",
                        "type": "trade",
                        "data": {},
                    }
                ]
            )

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.rows)

        def close(self) -> None:
            self.closed = True

    class Owner:
        def __init__(self) -> None:
            self._streams: set[RealtimeStream] = set()

        def _emit_diagnostics(self) -> None:
            pass

    owner = Owner()
    native = NativeIterator()
    stream = RealtimeStream(owner, native)  # type: ignore[arg-type]
    owner._streams.add(stream)
    assert next(stream)["market"] == "AAPLUSDC"
    stream.close()
    assert native.closed
    assert stream not in owner._streams
    with pytest.raises(StopIteration):
        next(stream)


def test_historical_generator_closes_its_native_iterator() -> None:
    class NativeIterator:
        def __init__(self) -> None:
            self.closed = False
            self.rows = iter([{"timestamp": 1}, {"timestamp": 2}])

        def __next__(self):
            return next(self.rows)

        def close(self) -> None:
            self.closed = True

    class Owner:
        def __init__(self) -> None:
            self.diagnostics = 0

        def _emit_diagnostics(self) -> None:
            self.diagnostics += 1

    owner = Owner()
    native = NativeIterator()
    rows = PolarisClient._iterate(owner, native, "events")  # type: ignore[arg-type]
    assert inspect.isgenerator(rows)
    assert next(rows) == {"timestamp": 1}
    rows.close()
    assert native.closed
    assert owner.diagnostics == 1


def test_record_batch_generator_closes_its_native_iterator() -> None:
    class NativeIterator:
        def __init__(self) -> None:
            self.closed = False
            self.rows = iter([object(), object()])

        def __next__(self):
            return next(self.rows)

        def close(self) -> None:
            self.closed = True

    class Owner:
        def __init__(self) -> None:
            self.diagnostics = 0

        def _emit_diagnostics(self) -> None:
            self.diagnostics += 1

    owner = Owner()
    native = NativeIterator()
    batches = PolarisClient._iterate_batches(  # type: ignore[arg-type]
        owner,
        native,
        "trades",
    )
    next(batches)
    batches.close()
    assert native.closed
    assert owner.diagnostics == 1


def test_realtime_native_errors_are_translated() -> None:
    connection = PolarisClient._translate_native_error(
        Exception(json.dumps({"kind": "stream_connection", "message": "offline"}))
    )
    protocol = PolarisClient._translate_native_error(
        Exception(
            json.dumps(
                {
                    "kind": "stream_protocol",
                    "code": "forbidden",
                    "message": "denied",
                }
            )
        )
    )
    assert isinstance(connection, StreamConnectionError)
    assert isinstance(protocol, StreamProtocolError)
    assert protocol.code == "forbidden"


def test_removed_legacy_layout_module_is_not_importable() -> None:
    assert importlib.util.find_spec("polaris_data.layout") is None
