"""Python compatibility facade over the native Rust Polaris SDK."""

from __future__ import annotations

import json
import os
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from . import _native
from .errors import (
    AccessDeniedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)
from .layout import LocalDatasetLayout, resolve_dataset_root
from .models import CatalogResponse, JSONDict, SnapshotEntry
from .utils import TimeInput, chunk_timerange, to_datetime, to_iso8601

DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0
DEFAULT_NETWORK_CHUNK_SIZE = 8 * 1024 * 1024


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
    ) -> None:
        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.dataset_root = resolve_dataset_root(
            dataset_root=dataset_root,
            dataset_download_dir=dataset_download_dir,
        )
        self.dataset_download_dir = self.dataset_root
        self.layout = LocalDatasetLayout(self.dataset_root)
        self.replay_cache_enabled = replay_cache_enabled
        self.replay_cache_dir = (
            Path(replay_cache_dir).expanduser()
            if replay_cache_dir is not None
            else self.layout.cache_root / "replay"
        )
        self._native = _native.NativeClient(
            self.api_key,
            self.base_url,
            timeout,
            self.dataset_root,
        )
        self._closed = False

    def close(self) -> None:
        if not self._closed:
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

    def _iterate(self, iterator: Iterator[JSONDict]) -> Iterator[JSONDict]:
        try:
            while True:
                try:
                    yield next(iterator)
                except StopIteration:
                    return
                except _native.NativeError as error:
                    raise self._translate_native_error(error) from None
        finally:
            self._emit_diagnostics()

    def health(self) -> JSONDict:
        return self._call("health")

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
    ) -> Iterator[JSONDict]:
        del timeout
        effective_chunk_size = (
            chunk_size if chunk_size is not None else DEFAULT_NETWORK_CHUNK_SIZE
        )
        if effective_chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        if parallel:
            if from_ is None or to is None:
                raise ValueError("from_ and to are required when parallel=True")

            def parallel_iterator() -> Iterator[JSONDict]:
                for chunk_start, chunk_end in chunk_timerange(from_, to, chunk_hours=24):
                    yield from self.replay(
                        source=source,
                        market=market,
                        from_=chunk_start,
                        to=chunk_end,
                        standard=standard,
                        allow_gaps=allow_gaps,
                        chunk_size=chunk_size,
                        parallel=False,
                    )

            return parallel_iterator()

        method = "replay" if standard else "raw_replay"
        args: tuple[Any, ...] = (
            source,
            market,
            self._time(from_),
            self._time(to),
        )
        if standard:
            iterator = self._call(method, *args, allow_gaps)
        else:
            if self.replay_cache_enabled and from_ is not None and to is not None:
                return self._raw_replay_with_cache(
                    source=source,
                    market=market,
                    from_=from_,
                    to=to,
                    args=args,
                )
            iterator = self._call(method, *args, 1000)
        return self._iterate(iterator)

    @staticmethod
    def _safe_filename_fragment(value: str) -> str:
        cleaned = "".join(
            character
            if character.isalnum() or character in {"-", "_", "."}
            else "_"
            for character in value
        ).strip("_")
        return cleaned or "dataset"

    def _default_dataset_filename(
        self,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool,
    ) -> str:
        from_text = to_iso8601(from_).replace(":", "-")
        to_text = to_iso8601(to).replace(":", "-")
        mode = "standard" if standard else "raw"
        safe = self._safe_filename_fragment
        return (
            f"{safe(source)}_{safe(market)}_{safe(from_text)}_{safe(to_text)}_"
            f"{mode}.jsonl.zst"
        )

    def _raw_replay_with_cache(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        args: tuple[Any, ...],
    ) -> Iterator[JSONDict]:
        canonical = self.replay_cache_dir / self._default_dataset_filename(
            source, market, from_, to, False
        )
        candidates = [canonical.with_suffix(""), canonical]
        for candidate in candidates:
            if candidate.exists():
                try:
                    rows = _native.decode_file(candidate)
                except _native.NativeError as error:
                    raise self._translate_native_error(error, "raw replay") from None
                return iter(rows)

        iterator = self._call("raw_replay", *args, 1000)
        cache_path = canonical.with_suffix("")

        def cache_rows() -> Iterator[JSONDict]:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = cache_path.with_name(f".{cache_path.name}.part")
            try:
                with temporary.open("w", encoding="utf-8") as output:
                    for row in self._iterate(iterator):
                        output.write(json.dumps(row, separators=(",", ":")))
                        output.write("\n")
                        yield row
                temporary.replace(cache_path)
            finally:
                temporary.unlink(missing_ok=True)

        return cache_rows()

    def _download_snapshots(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        force: bool = False,
    ):
        if force:
            for entry in self.layout.list_local_snapshots():
                if entry.source == source and entry.market == market:
                    Path(entry.path).unlink(missing_ok=True)
        list(
            self.replay(
                source=source,
                market=market,
                from_=from_,
                to=to,
            )
        )
        start_day = to_datetime(from_).date()
        end_day = to_datetime(to).date()
        return [
            entry
            for entry in self.layout.list_local_snapshots()
            if entry.source == source
            and entry.market == market
            and entry.date is not None
            and start_day <= date.fromisoformat(entry.date) <= end_day
        ]

    def _list_local_snapshots(
        self,
        *,
        date: str | date | None = None,
    ):
        date_text = date.isoformat() if date is not None and not isinstance(date, str) else date
        return [
            entry
            for entry in self.layout.list_local_snapshots()
            if date_text is None or entry.date == date_text
        ]

    def _iter_local_events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
    ) -> Iterator[JSONDict]:
        return self.replay(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )

    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._call(
            "events",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )

    def trades(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._call(
            "trades",
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )

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
    ) -> list[JSONDict]:
        return self._historical(
            "l2_snapshots", source, market, from_, to, allow_gaps
        )

    def funding_rates(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._historical(
            "funding_rates", source, market, from_, to, allow_gaps
        )

    def mark_prices(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._historical(
            "mark_prices", source, market, from_, to, allow_gaps
        )

    def bbo(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
        allow_gaps: bool = False,
    ) -> list[JSONDict]:
        return self._historical("bbo", source, market, from_, to, allow_gaps)

    def _historical(
        self,
        method: str,
        source: str,
        market: str,
        from_: TimeInput | None,
        to: TimeInput | None,
        allow_gaps: bool,
    ) -> list[JSONDict]:
        return self._call(
            method,
            source,
            market,
            self._time(from_),
            self._time(to),
            allow_gaps,
        )

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
        return self._aggregate(
            "vwap", source, market, from_, to, interval, allow_gaps
        )

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
    ) -> list[JSONDict]:
        if depth_pct <= 0:
            raise ValueError("depth_pct must be greater than 0")
        if slippage_notional <= 0:
            raise ValueError("slippage_notional must be greater than 0")
        return self._call(
            "depth_metrics",
            source,
            market,
            self._time(from_),
            self._time(to),
            depth_pct,
            slippage_notional,
            allow_gaps,
        )
