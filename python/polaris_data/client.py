"""Python compatibility facade over the native Rust Polaris SDK."""

from __future__ import annotations

import json
import os
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

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
from .models import CatalogResponse, JSONDict, SnapshotEntry
from .utils import TimeInput, to_iso8601

DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0
DEFAULT_NETWORK_CHUNK_SIZE = 8 * 1024 * 1024


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
        dataset_download_dir: str | os.PathLike[str] | None = None,
        replay_cache_enabled: bool = True,
        replay_cache_dir: str | os.PathLike[str] | None = None,
        stream_url: str | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        explicit_root = self._resolve_explicit_root(
            dataset_root=dataset_root,
            dataset_download_dir=dataset_download_dir,
        )
        self._native = _native.NativeClient(
            self.api_key,
            self.base_url,
            timeout,
            explicit_root,
            stream_url,
        )
        self.dataset_root = Path(self._native.dataset_root)
        self.dataset_download_dir = self.dataset_root
        self.replay_cache_enabled = replay_cache_enabled
        self.replay_cache_dir = (
            Path(replay_cache_dir).expanduser()
            if replay_cache_dir is not None
            else Path(self._native.replay_cache_dir)
        )
        self._closed = False
        self._streams: set[RealtimeStream] = set()

    @staticmethod
    def _resolve_explicit_root(
        *,
        dataset_root: str | os.PathLike[str] | None,
        dataset_download_dir: str | os.PathLike[str] | None,
    ) -> Path | None:
        if dataset_root is None:
            return (
                Path(dataset_download_dir).expanduser()
                if dataset_download_dir is not None
                else None
            )

        root = Path(dataset_root).expanduser()
        if dataset_download_dir is not None:
            legacy_root = Path(dataset_download_dir).expanduser()
            if legacy_root != root:
                raise ValueError(
                    "dataset_root and dataset_download_dir must match when both are provided"
                )
        return root

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

    def health(self) -> JSONDict:
        return self._call("health")

    def stream(
        self,
        *,
        source: str,
        markets: Sequence[str],
        include_buffer: bool = False,
        materialize_orderbooks: bool = True,
    ) -> RealtimeStream:
        """Open a reconnecting realtime stream of normalized market events."""
        iterator = self._call(
            "stream",
            source,
            list(markets),
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

    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        standard: bool = True,
        allow_gaps: bool = False,
        chunk_size: int | None = None,
        timeout: float | None = None,
        parallel: bool | int = False,
        materialize_orderbooks: bool = True,
    ) -> Iterator[JSONDict]:
        del timeout
        effective_chunk_size = (
            chunk_size if chunk_size is not None else DEFAULT_NETWORK_CHUNK_SIZE
        )
        if effective_chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        from_text = self._time(from_)
        to_text = self._time(to)
        if parallel and (from_text is None or to_text is None):
            raise ValueError("from_ and to are required when parallel=True")

        if standard:
            if parallel:
                iterator = self._call(
                    "replay_chunked",
                    source,
                    market,
                    from_text,
                    to_text,
                    allow_gaps,
                    materialize_orderbooks,
                )
            else:
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

    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
    ) -> Iterator[JSONDict]:
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

    def trades(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        iterator = self._call(
            "trades",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )
        return self._iterate(iterator, "trades")

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

    def l2_snapshots(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
        materialize_orderbooks: bool = True,
    ) -> Iterator[JSONDict]:
        iterator = self._call(
            "l2_snapshots",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
            materialize_orderbooks,
        )
        return self._iterate(iterator, "l2_snapshots")

    def l2_updates(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        """Iterate raw orderbook snapshots and deltas without reconstruction."""
        iterator = self._call(
            "l2_updates",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )
        return self._iterate(iterator, "l2_updates")

    def funding_rates(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        return self._historical("funding_rates", source, market, from_, to, allow_gaps)

    def mark_prices(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        return self._historical("mark_prices", source, market, from_, to, allow_gaps)

    def bbo(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str | None = None,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        iterator = self._call(
            "bbo",
            source,
            market,
            self._time(from_),
            self._time(to),
            interval,
            allow_gaps,
        )
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
    ) -> list[JSONDict] | JSONDict:
        return self._call(
            "ohlcv",
            source,
            market,
            interval,
            self._time(from_),
            self._time(to),
            format,
            allow_gaps,
        )

    def volume(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._aggregate(
            "volume", source, market, from_, to, interval, allow_gaps
        )

    def vwap(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        interval: str,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._aggregate("vwap", source, market, from_, to, interval, allow_gaps)

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
    ) -> list[JSONDict]:
        if method != "log_returns":
            raise ValueError("method must be 'log_returns'")
        return self._aggregate(
            "volatility", source, market, from_, to, interval, allow_gaps
        )

    def _aggregate(
        self,
        method: str,
        source: str,
        market: str,
        from_: TimeInput | None,
        to: TimeInput | None,
        interval: str,
        allow_gaps: bool,
    ) -> list[JSONDict]:
        return self._call(
            method,
            source,
            market,
            interval,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )

    def depth_metrics(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        depth_pct: float = 0.01,
        slippage_notional: float = 10_000.0,
        allow_gaps: bool = False,
    ) -> Iterator[JSONDict]:
        if depth_pct <= 0:
            raise ValueError("depth_pct must be greater than 0")
        if slippage_notional <= 0:
            raise ValueError("slippage_notional must be greater than 0")
        iterator = self._call(
            "depth_metrics",
            source,
            market,
            self._time(from_),
            self._time(to),
            depth_pct,
            slippage_notional,
            allow_gaps,
        )
        return self._iterate(iterator, "depth_metrics")
