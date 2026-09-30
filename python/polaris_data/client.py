"""Python compatibility facade over the native Rust Polaris SDK."""

from __future__ import annotations

import importlib
import json
import os
import warnings
from itertools import islice
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Literal, Sequence, overload

from . import _native
from .errors import (
    AccessDeniedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    StreamConnectionError,
    StreamProtocolError,
    UnauthorizedError,
)
from .models import (
    IntentEvent,
    CatalogCount,
    CatalogResponse,
    FundingRateRow,
    IntentRow,
    JSONDict,
    OptionTickerRow,
    OhlcvRow,
    OrderbookL2Row,
    PerpetualTickerEvent,
    PropammQuoteLadderEvent,
    QuoteRow,
    RawCaptureRow,
    SnapshotEntry,
    TradeRow,
)
from .utils import TimeInput, to_iso8601

if TYPE_CHECKING:
    import pandas
    import pyarrow

DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0
DEFAULT_BATCH_SIZE = 65_536
OutputFormat = Literal["iterator", "batches", "dataframe"]
AggregateOutputFormat = Literal["records", "dataframe"]

AGGREGATE_COLUMNS: dict[str, tuple[str, ...]] = {
    "ohlcv": ("timestamp", "open", "high", "low", "close", "volume", "trades"),
    "volume": ("timestamp", "volume"),
    "vwap": ("timestamp", "vwap", "volume", "quote_volume", "trades"),
    "volatility": ("timestamp", "volatility", "returns"),
}
AGGREGATE_COUNT_COLUMNS = frozenset({"trades", "returns"})


class OrderbookBuilder:
    """Reconstruct complete books from standardized orderbook events."""

    def __init__(self) -> None:
        self._native = _native.NativeOrderbookBuilder()

    def apply(self, event: JSONDict) -> JSONDict | None:
        """Apply an event, suppressing deltas until their book has a snapshot."""
        try:
            return self._native.apply(event)
        except _native.NativeError as error:
            raise PolarisClient._translate_native_error(error, "orderbook") from None

    def update(self, event: JSONDict) -> bool:
        """Update book state without constructing a complete orderbook."""
        try:
            return self._native.update(event)
        except _native.NativeError as error:
            raise PolarisClient._translate_native_error(error, "orderbook") from None

    def snapshot(self, source: str, market: str) -> JSONDict | None:
        """Return the current complete book for a source and market."""
        return self._native.snapshot(source, market)

    def clear(self) -> None:
        self._native.clear()

    def clear_book(self, source: str, market: str) -> None:
        self._native.clear_book(source, market)


class RealtimeStream(Iterator[JSONDict]):
    """Closeable iterator of normalized realtime market events."""

    def __init__(self, client: "PolarisClient", iterator: Iterator[JSONDict]) -> None:
        self._client = client
        self._iterator: Iterator[JSONDict] | None = iterator

    def __iter__(self) -> "RealtimeStream":
        return self

    def __next__(self) -> JSONDict:
        if self._iterator is None:
            raise StopIteration
        try:
            return next(self._iterator)
        except _native.NativeError as error:
            self.close()
            raise self._client._translate_native_error(error, "stream") from None
        except StopIteration:
            self.close()
            raise

    def close(self) -> None:
        if self._iterator is not None:
            close = getattr(self._iterator, "close", None)
            if close is not None:
                close()
            self._iterator = None
            self._client._streams.discard(self)
            self._client._emit_diagnostics()

    def __enter__(self) -> "RealtimeStream":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class PolarisClient:
    """High-level synchronous client backed by the shared Rust SDK."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        dataset_root: str | os.PathLike[str] | None = None,
        replay_cache_enabled: bool = True,
        replay_cache_dir: str | os.PathLike[str] | None = None,
        stream_url: str | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._native = _native.NativeClient(
            self.api_key,
            self.base_url,
            timeout,
            Path(dataset_root).expanduser() if dataset_root is not None else None,
            stream_url,
        )
        self.dataset_root = Path(self._native.dataset_root)
        self.replay_cache_enabled = replay_cache_enabled
        self.replay_cache_dir = (
            Path(replay_cache_dir).expanduser()
            if replay_cache_dir is not None
            else Path(self._native.replay_cache_dir)
        )
        self._closed = False
        self._streams: set[RealtimeStream] = set()

    def close(self) -> None:
        if not self._closed:
            for stream in list(self._streams):
                stream.close()
            self._native.close()
            self._closed = True

    def __enter__(self) -> "PolarisClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    @staticmethod
    def _time(value: TimeInput | None) -> str | None:
        return None if value is None else to_iso8601(value)

    @staticmethod
    def _validate_l2_bounds(start: int, end: int) -> None:
        for name, value in (("start", start), ("end", end)):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer Unix millisecond timestamp")
        if start < 0 or end < start:
            raise ValueError("start and end must be non-negative inclusive milliseconds with start <= end")

    def _emit_diagnostics(self) -> None:
        for message in self._native.take_diagnostics():
            warnings.warn(message, UserWarning, stacklevel=3)

    @staticmethod
    def _translate_native_error(
        error: Exception,
        operation: str | None = None,
    ) -> PolarisError:
        raw = error.args[0] if error.args else str(error)
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return PolarisError(str(error))

        message = str(payload.get("message") or "Polaris request failed")
        status_code = payload.get("status_code")
        if not isinstance(status_code, int):
            status_code = None
        body = payload.get("body")
        if not isinstance(body, str):
            body = None

        kind = payload.get("kind")
        if kind == "unauthorized":
            return UnauthorizedError(message, status_code, body)
        if kind == "access_denied":
            if "docs.polaris.supply/guides/authentication" not in message:
                message = (
                    f"{message}. Set POLARIS_API_KEY or pass api_key to PolarisClient. "
                    "See https://docs.polaris.supply/guides/authentication"
                )
            return AccessDeniedError(message, status_code, body)
        if kind == "not_found":
            return NotFoundError(message, status_code, body)
        if kind == "rate_limited":
            reset_at = payload.get("reset_at")
            return RateLimitedError(
                message,
                status_code,
                body,
                reset_at if isinstance(reset_at, str) else None,
            )
        if kind == "stream_decode":
            return StreamDecodeError(message, status_code, body)
        if kind == "stream_connection":
            return StreamConnectionError(message, status_code, body)
        if kind == "stream_protocol":
            code = payload.get("code")
            return StreamProtocolError(
                message,
                status_code,
                body,
                code if isinstance(code, str) else None,
            )
        if kind == "coverage_gap":
            label = operation or "replay"
            return PolarisError(
                f"Requested {label} range could not be satisfied from standardized snapshots"
            )
        return PolarisError(message, status_code, body)

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise PolarisError("PolarisClient is closed")
        try:
            return getattr(self._native, method)(*args, **kwargs)
        except _native.NativeError as error:
            raise self._translate_native_error(error, method) from None
        finally:
            self._emit_diagnostics()

    def _iterate(
        self,
        iterator: Iterator[JSONDict],
        operation: str | None = None,
    ) -> Iterator[JSONDict]:
        try:
            while True:
                try:
                    yield next(iterator)
                except StopIteration:
                    return
                except _native.NativeError as error:
                    raise self._translate_native_error(error, operation) from None
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()
            self._emit_diagnostics()

    def _iterate_batches(
        self,
        iterator: Iterator[Any],
        operation: str,
    ) -> Iterator[pyarrow.RecordBatch]:
        try:
            while True:
                try:
                    yield next(iterator)
                except StopIteration:
                    return
                except _native.NativeError as error:
                    raise self._translate_native_error(error, operation) from None
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()
            self._emit_diagnostics()

    @staticmethod
    def _validate_columnar_output(output: str, batch_size: int) -> None:
        if output not in {"iterator", "batches", "dataframe"}:
            raise ValueError(
                "output must be one of: 'iterator', 'batches', 'dataframe'"
            )
        if not isinstance(batch_size, int) or isinstance(batch_size, bool):
            raise TypeError("batch_size must be an integer")
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

    @staticmethod
    def _require_optional_module(name: str, extra: str) -> Any:
        try:
            return importlib.import_module(name)
        except ImportError:
            raise ImportError(
                f"{name} is required for this output; install "
                f"polaris-data[{extra}]"
            ) from None

    def _columnar_result(
        self,
        iterator: Any,
        operation: str,
        output: Literal["batches", "dataframe"],
        pyarrow_module: Any,
    ) -> Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        batches = self._iterate_batches(iterator, operation)
        if output == "batches":
            return batches

        self._require_optional_module("pandas", "dataframe")
        schema = iterator.schema
        materialized = list(batches)
        table = pyarrow_module.Table.from_batches(materialized, schema=schema)
        materialized.clear()
        return table.to_pandas(split_blocks=True, self_destruct=True)

    @staticmethod
    def _direct_row_schema(method: str, pa: Any) -> Any:
        fields = [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
            pa.field("market", pa.string(), nullable=False),
            pa.field("collector_timestamp", pa.int64(), nullable=False),
            pa.field("source_capture_id", pa.string(), nullable=False),
            pa.field("schema_version", pa.int32(), nullable=False),
        ]
        if method == "trades":
            fields += [
                pa.field("price", pa.float64(), nullable=False),
                pa.field("quantity", pa.float64(), nullable=False),
                pa.field("exchange_timestamp", pa.int64()),
                pa.field("instrument", pa.string()),
                pa.field("liquidation", pa.bool_()),
                pa.field("maker", pa.string()),
                pa.field("order_id", pa.string()),
                pa.field("side", pa.string()),
                pa.field("taker", pa.string()),
            ]
        else:
            fields += [
                pa.field("exchange_timestamp", pa.int64()),
                pa.field("funding_timestamp", pa.int64()),
            ]
            fields += [pa.field(name, pa.string()) for name in (
                "instrument", "funding_rate", "index_price", "mark_price",
                "open_interest", "predicted_funding_rate", "premium",
            )]
        return pa.schema(fields)

    def _direct_row_output(
        self,
        method: Literal["trades", "funding_rates", "mark_prices"],
        source: str | None,
        market: str | None,
        start: int | None,
        end: int | None,
        output: OutputFormat,
        batch_size: int,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_columnar_output(output, batch_size)
        for name, value in (("start", start), ("end", end)):
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise TypeError(f"{name} must be an integer Unix millisecond timestamp")
        pa = None
        schema = None
        if output != "iterator":
            if output == "dataframe":
                self._require_optional_module("pandas", "dataframe")
            pa = self._require_optional_module(
                "pyarrow", "dataframe" if output == "dataframe" else "arrow"
            )
            schema = self._direct_row_schema(method, pa)
        iterator = self._iterate(self._call(method, source, market, start, end), method)
        if output == "iterator":
            return iterator

        def batches() -> Iterator[pyarrow.RecordBatch]:
            while rows := list(islice(iterator, batch_size)):
                yield pa.RecordBatch.from_pylist(rows, schema=schema)

        if output == "batches":
            return batches()
        return pa.Table.from_batches(list(batches()), schema=schema).to_pandas()

    def health(self) -> JSONDict:
        return self._call("health")

    def stream(
        self,
        *,
        source: str,
        markets: Sequence[str],
        instrument: str | None = None,
        include_buffer: bool = False,
        materialize_orderbooks: bool = True,
    ) -> RealtimeStream:
        """Open a reconnecting realtime stream of normalized market events."""
        if instrument is not None and not instrument.strip():
            raise ValueError("instrument must be non-empty")
        iterator = self._call(
            "stream",
            source,
            list(markets),
            instrument,
            include_buffer,
            materialize_orderbooks,
        )
        stream = RealtimeStream(self, iterator)
        self._streams.add(stream)
        return stream

    def catalog(
        self,
        source: str | None = None,
        market: str | None = None,
        q: str | None = None,
    ) -> CatalogResponse | JSONDict:
        return self._call("catalog", source, market, q)

    def count(self) -> CatalogCount:
        return self._call("count")

    def list_snapshots(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> list[SnapshotEntry]:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        rows = self._call(
            "list_snapshots",
            source,
            market,
            self._time(from_),
            self._time(to),
            limit,
        )
        return [
            SnapshotEntry(
                key=row["key"],
                source=row.get("source"),
                market=row.get("market"),
                date=row.get("date"),
                start=(
                    datetime.fromisoformat(row["start"].replace("Z", "+00:00"))
                    if row.get("start")
                    else None
                ),
                end=(
                    datetime.fromisoformat(row["end"].replace("Z", "+00:00"))
                    if row.get("end")
                    else None
                ),
                hour=row.get("hour"),
            )
            for row in rows
        ]

    @overload
    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        standard: bool = True,
        allow_gaps: bool = False,
        parallel: bool | int = False,
        materialize_orderbooks: bool = True,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict]: ...

    @overload
    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        standard: Literal[True] = True,
        allow_gaps: bool = False,
        parallel: bool | int = False,
        materialize_orderbooks: bool = True,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        standard: Literal[True] = True,
        allow_gaps: bool = False,
        parallel: bool | int = False,
        materialize_orderbooks: bool = True,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        standard: bool = True,
        allow_gaps: bool = False,
        parallel: bool | int = False,
        materialize_orderbooks: bool = True,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_columnar_output(output, batch_size)
        from_text = self._time(from_)
        to_text = self._time(to)
        if parallel and (from_text is None or to_text is None):
            raise ValueError("from_ and to are required when parallel=True")

        if output != "iterator":
            if not standard:
                raise ValueError("columnar output is only available for standardized replay")
            extra = "dataframe" if output == "dataframe" else "arrow"
            pyarrow_module = self._require_optional_module("pyarrow", extra)
            if output == "dataframe":
                self._require_optional_module("pandas", "dataframe")
            iterator = self._call(
                "events_columnar",
                source,
                market,
                from_text,
                to_text,
                allow_gaps,
                materialize_orderbooks,
                batch_size,
            )
            return self._columnar_result(iterator, "replay", output, pyarrow_module)

        if standard:
            iterator = self._call(
                "replay",
                source,
                market,
                from_text,
                to_text,
                allow_gaps,
                materialize_orderbooks,
            )
        elif from_text is not None and to_text is not None:
            method = "raw_replay_chunked" if parallel else "raw_replay_cached"
            iterator = self._call(
                method,
                source,
                market,
                from_text,
                to_text,
                1000,
                self.replay_cache_enabled,
                self.replay_cache_dir,
            )
        else:
            iterator = self._call(
                "raw_replay", source, market, from_text, to_text, 1000
            )
        return self._iterate(iterator, "replay")

    @overload
    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict]: ...

    @overload
    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_columnar_output(output, batch_size)
        if output != "iterator":
            extra = "dataframe" if output == "dataframe" else "arrow"
            pyarrow_module = self._require_optional_module("pyarrow", extra)
            if output == "dataframe":
                self._require_optional_module("pandas", "dataframe")
            iterator = self._call(
                "events_columnar",
                source,
                market,
                self._time(from_),
                self._time(to),
                allow_gaps,
                materialize_orderbooks,
                batch_size,
            )
            return self._columnar_result(iterator, "events", output, pyarrow_module)
        iterator = self._call(
            "events",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
            materialize_orderbooks,
        )
        return self._iterate(iterator, "events")

    @overload
    def trades(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[TradeRow]: ...

    @overload
    def trades(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def trades(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def trades(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[TradeRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        """Read flat trade rows from the direct historical API."""
        return self._direct_row_output(
            "trades", source, market, start, end, output, batch_size
        )

    def intents(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        instrument: str | None = None,
        intent_id: str | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[IntentRow]:
        """Iterate pair-shaped intent observations from the direct API."""
        iterator = self._call("intents", source, market, instrument, intent_id, start, end)
        return self._iterate(iterator, "intents")

    def intent_rows(
        self, *, source: str | None = None, market: str | None = None,
        instrument: str | None = None, intent_id: str | None = None,
        start: int | None = None, end: int | None = None,
    ) -> Iterator[IntentRow]:
        """Read flat pair-shaped intent observations from the direct API."""
        iterator = self._call("intent_rows", source, market, instrument, intent_id, start, end)
        return self._iterate(iterator, "intent_rows")

    def ohlcv_rows(
        self, *, source: str | None = None, market: str | None = None,
        instrument: str | None = None, interval: str | None = None,
        start: int | None = None, end: int | None = None,
    ) -> Iterator[OhlcvRow]:
        """Read every venue-published candle update from the direct API."""
        iterator = self._call("ohlcv_rows", source, market, instrument, interval, start, end)
        return self._iterate(iterator, "ohlcv_rows")

    def quote_rows(
        self, *, source: str | None = None, market: str | None = None,
        instrument: str | None = None, observation_id: str | None = None,
        start: int | None = None, end: int | None = None,
    ) -> Iterator[QuoteRow]:
        """Read individual flat PropAMM quote points from the direct API."""
        iterator = self._call("quote_rows", source, market, instrument, observation_id, start, end)
        return self._iterate(iterator, "quote_rows")

    def option_tickers(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        instrument: str | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[OptionTickerRow]:
        """Read flat option ticker rows for a chain or exact contract."""
        if instrument is not None and not instrument.strip():
            raise ValueError("instrument must be non-empty")
        iterator = self._call(
            "option_tickers",
            source,
            market,
            instrument,
            start,
            end,
        )
        return self._iterate(iterator, "option_tickers")

    def perpetual_tickers(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[FundingRateRow]:
        """Iterate funding-bearing perpetual ticker observations."""
        iterator = self._call("perpetual_tickers", source, market, start, end)
        return self._iterate(iterator, "perpetual_tickers")

    def raw(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        limit: int = 1000,
    ) -> list[JSONDict]:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        return self._call(
            "raw",
            source,
            market,
            self._time(from_),
            self._time(to),
            limit,
        )

    def raw_channel(
        self,
        *,
        exchange: str,
        event: str,
        start: int,
        end: int,
    ) -> Iterator[RawCaptureRow]:
        """Iterate exact raw captures for one venue-native channel."""
        if not exchange.strip() or not event.strip() or exchange in {".", ".."} or event in {".", ".."}:
            raise ValueError("exchange and event must be non-empty channel identifiers")
        for name, value in (("start", start), ("end", end)):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer Unix millisecond timestamp")
        if start < 0 or end < start:
            raise ValueError("start and end must be non-negative inclusive milliseconds with start <= end")
        iterator = self._call("raw_channel", exchange, event, start, end)
        return self._iterate(iterator, "raw_channel")

    def l2_snapshots(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        instrument: str | None = None,
    ) -> Iterator[OrderbookL2Row]:
        """Read reconstructed top-25 books after each L2 event."""
        self._validate_l2_bounds(start, end)
        iterator = self._call("l2_snapshots", source, market, start, end, instrument)
        return self._iterate(iterator, "l2_snapshots")

    def l2_updates(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        instrument: str | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[OrderbookL2Row]:
        """Read flat source snapshots and sparse L2 deltas without reconstruction."""
        for name, value in (("start", start), ("end", end)):
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise TypeError(f"{name} must be an integer Unix millisecond timestamp")
        iterator = self._call("l2_updates", source, market, instrument, start, end)
        return self._iterate(iterator, "l2_updates")

    @overload
    def funding_rates(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[FundingRateRow]: ...

    @overload
    def funding_rates(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def funding_rates(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def funding_rates(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[FundingRateRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        """Read partial flat funding observations from the direct historical API."""
        return self._direct_row_output(
            "funding_rates", source, market, start, end, output, batch_size
        )

    @overload
    def mark_prices(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[FundingRateRow]: ...

    @overload
    def mark_prices(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def mark_prices(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def mark_prices(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[FundingRateRow] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        """Read funding observations carrying a mark price."""
        return self._direct_row_output(
            "mark_prices", source, market, start, end, output, batch_size
        )

    @overload
    def propamm_quote_ladders(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[PropammQuoteLadderEvent]: ...

    @overload
    def propamm_quote_ladders(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def propamm_quote_ladders(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def propamm_quote_ladders(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> (
        Iterator[PropammQuoteLadderEvent]
        | Iterator[pyarrow.RecordBatch]
        | pandas.DataFrame
    ):
        return self._typed_historical(
            "propamm_quote_ladders",
            source,
            market,
            from_,
            to,
            allow_gaps,
            output,
            batch_size,
        )

    @overload
    def bbo(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        interval: str | None = None,
        changes_only: bool = False,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict]: ...

    @overload
    def bbo(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        interval: str | None = None,
        changes_only: bool = False,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def bbo(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        interval: str | None = None,
        changes_only: bool = False,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def bbo(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        interval: str | None = None,
        changes_only: bool = False,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_l2_bounds(start, end)
        self._validate_columnar_output(output, batch_size)
        if output != "iterator":
            extra = "dataframe" if output == "dataframe" else "arrow"
            pyarrow_module = self._require_optional_module("pyarrow", extra)
            if output == "dataframe":
                self._require_optional_module("pandas", "dataframe")
            iterator = self._call(
                "bbo_columnar", source, market, start, end,
                interval, changes_only, batch_size,
            )
            return self._columnar_result(iterator, "bbo", output, pyarrow_module)
        iterator = self._call("bbo", source, market, start, end, interval, changes_only)
        return self._iterate(iterator, "bbo")

    def _historical(
        self,
        method: str,
        source: str,
        market: str,
        from_: TimeInput | None,
        to: TimeInput | None,
        allow_gaps: bool,
    ) -> Iterator[JSONDict]:
        iterator = self._call(
            method,
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )
        return self._iterate(iterator, method)

    def _typed_historical(
        self,
        method: Literal["funding_rates", "mark_prices", "propamm_quote_ladders"],
        source: str,
        market: str,
        from_: TimeInput | None,
        to: TimeInput | None,
        allow_gaps: bool,
        output: OutputFormat,
        batch_size: int,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_columnar_output(output, batch_size)
        if output == "iterator":
            return self._historical(method, source, market, from_, to, allow_gaps)
        extra = "dataframe" if output == "dataframe" else "arrow"
        pyarrow_module = self._require_optional_module("pyarrow", extra)
        if output == "dataframe":
            self._require_optional_module("pandas", "dataframe")
        iterator = self._call(
            f"{method}_columnar",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
            batch_size,
        )
        return self._columnar_result(iterator, method, output, pyarrow_module)

    @overload
    def ohlcv(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        format: str | None = None,
        allow_gaps: bool = False,
        output: Literal["records"] = "records",
    ) -> list[JSONDict] | JSONDict: ...

    @overload
    def ohlcv(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        format: None = None,
        allow_gaps: bool = False,
        output: Literal["dataframe"],
    ) -> pandas.DataFrame: ...

    def ohlcv(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        format: str | None = None,
        allow_gaps: bool = False,
        output: AggregateOutputFormat = "records",
    ) -> list[JSONDict] | JSONDict | pandas.DataFrame:
        if output == "dataframe" and format is not None:
            raise ValueError(
                "output='dataframe' is only available when format is None"
            )
        pandas_module = self._prepare_aggregate_output(output)
        result = self._call(
            "ohlcv",
            source,
            market,
            interval,
            self._time(from_),
            self._time(to),
            format,
            allow_gaps,
        )
        if output == "records":
            return result
        return self._aggregate_dataframe(
            result,
            AGGREGATE_COLUMNS["ohlcv"],
            pandas_module,
        )

    @overload
    def volume(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: Literal["records"] = "records",
    ) -> list[JSONDict]: ...

    @overload
    def volume(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: Literal["dataframe"],
    ) -> pandas.DataFrame: ...

    def volume(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: AggregateOutputFormat = "records",
    ) -> list[JSONDict] | pandas.DataFrame:
        return self._aggregate(
            "volume", source, market, from_, to, interval, allow_gaps, output
        )

    @overload
    def vwap(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: Literal["records"] = "records",
    ) -> list[JSONDict]: ...

    @overload
    def vwap(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: Literal["dataframe"],
    ) -> pandas.DataFrame: ...

    def vwap(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
        output: AggregateOutputFormat = "records",
    ) -> list[JSONDict] | pandas.DataFrame:
        return self._aggregate(
            "vwap", source, market, from_, to, interval, allow_gaps, output
        )

    @overload
    def volatility(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        method: str = "log_returns",
        allow_gaps: bool = False,
        output: Literal["records"] = "records",
    ) -> list[JSONDict]: ...

    @overload
    def volatility(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        method: str = "log_returns",
        allow_gaps: bool = False,
        output: Literal["dataframe"],
    ) -> pandas.DataFrame: ...

    def volatility(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        method: str = "log_returns",
        allow_gaps: bool = False,
        output: AggregateOutputFormat = "records",
    ) -> list[JSONDict] | pandas.DataFrame:
        if method != "log_returns":
            raise ValueError("method must be 'log_returns'")
        return self._aggregate(
            "volatility", source, market, from_, to, interval, allow_gaps, output
        )

    def _prepare_aggregate_output(
        self,
        output: str,
    ) -> Any | None:
        if output not in {"records", "dataframe"}:
            raise ValueError("output must be one of: 'records', 'dataframe'")
        if output == "dataframe":
            return self._require_optional_module("pandas", "dataframe")
        return None

    @staticmethod
    def _aggregate_dataframe(
        rows: list[JSONDict],
        columns: tuple[str, ...],
        pandas_module: Any,
    ) -> pandas.DataFrame:
        frame = pandas_module.DataFrame.from_records(rows, columns=columns)
        frame["timestamp"] = pandas_module.Series(
            pandas_module.to_datetime(frame["timestamp"], unit="ms", utc=True),
            dtype="datetime64[ms, UTC]",
        )
        for column in columns[1:]:
            dtype = "uint64" if column in AGGREGATE_COUNT_COLUMNS else "float64"
            frame[column] = frame[column].astype(dtype)
        return frame

    def _aggregate(
        self,
        method: str,
        source: str,
        market: str,
        from_: TimeInput | None,
        to: TimeInput | None,
        interval: str,
        allow_gaps: bool,
        output: AggregateOutputFormat,
    ) -> list[JSONDict] | pandas.DataFrame:
        pandas_module = self._prepare_aggregate_output(output)
        rows = self._call(
            method,
            source,
            market,
            interval,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )
        if output == "records":
            return rows
        return self._aggregate_dataframe(
            rows,
            AGGREGATE_COLUMNS[method],
            pandas_module,
        )

    @overload
    def depth_metrics(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        depth_pct: float = 0.01,
        slippage_notional: float = 10_000.0,
        output: Literal["iterator"] = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict]: ...

    @overload
    def depth_metrics(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        depth_pct: float = 0.01,
        slippage_notional: float = 10_000.0,
        output: Literal["batches"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pyarrow.RecordBatch]: ...

    @overload
    def depth_metrics(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        depth_pct: float = 0.01,
        slippage_notional: float = 10_000.0,
        output: Literal["dataframe"],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> pandas.DataFrame: ...

    def depth_metrics(
        self,
        *,
        source: str,
        market: str,
        start: int,
        end: int,
        depth_pct: float = 0.01,
        slippage_notional: float = 10_000.0,
        output: OutputFormat = "iterator",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[JSONDict] | Iterator[pyarrow.RecordBatch] | pandas.DataFrame:
        self._validate_l2_bounds(start, end)
        if depth_pct <= 0:
            raise ValueError("depth_pct must be greater than 0")
        if slippage_notional <= 0:
            raise ValueError("slippage_notional must be greater than 0")
        self._validate_columnar_output(output, batch_size)
        if output != "iterator":
            extra = "dataframe" if output == "dataframe" else "arrow"
            pyarrow_module = self._require_optional_module("pyarrow", extra)
            if output == "dataframe":
                self._require_optional_module("pandas", "dataframe")
            iterator = self._call(
                "depth_metrics_columnar", source, market, start, end,
                depth_pct, slippage_notional, batch_size,
            )
            return self._columnar_result(iterator, "depth_metrics", output, pyarrow_module)
        iterator = self._call(
            "depth_metrics", source, market, start, end, depth_pct, slippage_notional,
        )
        return self._iterate(iterator, "depth_metrics")
