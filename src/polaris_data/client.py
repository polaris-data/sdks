"""Synchronous client for the Polaris API."""

from __future__ import annotations

import dataclasses
import json
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx
import orjson
import zstandard as zstd

from .errors import (
    AccessDeniedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)
from .layout import (
    LocalDatasetLayout,
    resolve_dataset_root,
)
from .models import (
    JSONDict,
    LocalSnapshotEntry,
    SnapshotEntry,
)
from .utils import TimeInput, chunk_timerange, to_datetime, to_iso8601

DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0
DEFAULT_NETWORK_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB for network downloads
DEFAULT_FILE_CHUNK_SIZE = 1 * 1024 * 1024  # 1MB for file operations
USER_AGENT = "polaris-py/0.5.1"
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_INTERVAL_US = {
    "100ms": 100_000,
    "1s": 1_000_000,
    "10s": 10_000_000,
    "1m": 60_000_000,
    "5m": 300_000_000,
    "15m": 900_000_000,
    "1h": 3_600_000_000,
}
_VOL_SCALE = 1_000_000_000_000


def _safe_filename_fragment(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "_"
        for character in value
    ).strip("_")
    return cleaned or "dataset"


def _file_is_zstd(path: Path) -> bool:
    try:
        with path.open("rb") as file:
            return file.read(len(_ZSTD_MAGIC)) == _ZSTD_MAGIC
    except OSError:
        return False


def _datetime_to_epoch_micros(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return int(value.timestamp() * 1_000_000)


def _interval_to_us(interval: str) -> int | None:
    return _INTERVAL_US.get(interval)


def _to_tradingview_ohlcv(
    bars: list[JSONDict],
) -> JSONDict:
    candles: list[JSONDict] = []
    volumes: list[JSONDict] = []
    for bar in bars:
        timestamp = bar["timestamp"] / 1_000_000
        candles.append(
            {
                "time": timestamp,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
            }
        )
        volumes.append({"time": timestamp, "value": bar["volume"]})
    return {"candles": candles, "volumes": volumes}


class _LocalOhlcvAggregator:
    def __init__(self, interval: str) -> None:
        us = _interval_to_us(interval)
        if us is None:
            supported = ", ".join(_INTERVAL_US)
            raise ValueError(f"Unsupported interval: {interval}. Supported: {supported}")
        self._interval = interval
        self._interval_us = us
        self._bars: dict[int, dict[str, int | float | str]] = {}

    def push(self, event: JSONDict) -> None:
        if event.get("type") != "trade":
            return

        timestamp = event.get("timestamp")
        data = event.get("data")
        if not isinstance(timestamp, (int, float)) or not isinstance(data, dict):
            return

        price = data.get("price")
        quantity = data.get("quantity")
        if not isinstance(price, (int, float)) or not isinstance(quantity, (int, float)):
            return

        bucket = int(timestamp // self._interval_us) * self._interval_us
        bar = self._bars.get(bucket)
        if bar is None:
            self._bars[bucket] = {
                "timestamp": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume_scaled": round(quantity * _VOL_SCALE),
                "trades": 1,
                "interval": self._interval,
                "open_ts": int(timestamp),
                "close_ts": int(timestamp),
            }
            return

        if price > bar["high"]:
            bar["high"] = price
        if price < bar["low"]:
            bar["low"] = price
        if timestamp < bar["open_ts"]:
            bar["open"] = price
            bar["open_ts"] = int(timestamp)
        if timestamp >= bar["close_ts"]:
            bar["close"] = price
            bar["close_ts"] = int(timestamp)
        bar["volume_scaled"] += round(quantity * _VOL_SCALE)
        bar["trades"] += 1

    def finish(self) -> list[JSONDict]:
        result: list[JSONDict] = []
        for bar in self._bars.values():
            result.append(
                {
                    "timestamp": int(bar["timestamp"]),
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": int(bar["volume_scaled"]) / _VOL_SCALE,
                    "trades": int(bar["trades"]),
                    "interval": str(bar["interval"]),
                }
            )

        result.sort(key=lambda item: int(item["timestamp"]))
        for index in range(1, len(result)):
            result[index]["open"] = result[index - 1]["close"]
        return result


def _iter_utc_dates(start: datetime, end: datetime) -> list[date]:
    if start >= end:
        raise ValueError("from_ must be before to")

    current = start.date()
    last = (end - timedelta(microseconds=1)).date()
    days: list[date] = []
    while current <= last:
        days.append(current)
        current += timedelta(days=1)
    return days


class _ChunkStream(io.RawIOBase):
    """Expose an iterator of byte chunks as a readable stream."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._exhausted = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self._exhausted and not self._buffer:
            return 0

        view = memoryview(buffer).cast("B")
        target_size = len(view)
        if target_size == 0:
            return 0

        while len(self._buffer) < target_size and not self._exhausted:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._exhausted = True
                break
            if chunk:
                self._buffer.extend(chunk)

        written = min(target_size, len(self._buffer))
        if written:
            view[:written] = self._buffer[:written]
            del self._buffer[:written]
        return written


class PolarisClient:
    """High-level sync SDK client for Polaris datasets and market data."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        dataset_root: str | os.PathLike[str] | None = None,
        dataset_download_dir: str | os.PathLike[str] | None = None,
        replay_cache_enabled: bool = True,
        replay_cache_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        if transport is not None and http_client is not None:
            raise ValueError("Pass either transport or http_client, not both")

        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.dataset_root = resolve_dataset_root(
            dataset_root=dataset_root,
            dataset_download_dir=dataset_download_dir,
        )
        self.layout = LocalDatasetLayout(self.dataset_root)
        # Kept as a compatibility alias for earlier SDK releases.
        self.dataset_download_dir = self.dataset_root
        self.replay_cache_enabled = replay_cache_enabled
        self.replay_cache_dir = (
            Path(replay_cache_dir).expanduser()
            if replay_cache_dir is not None
            else self.layout.cache_root / "replay"
        )
        if self.replay_cache_enabled:
            self.replay_cache_dir.mkdir(parents=True, exist_ok=True)

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            # Try to enable HTTP/2 if available
            try:
                import h2  # noqa: F401

                use_http2 = True
            except ImportError:
                use_http2 = False

            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={"User-Agent": USER_AGENT},
                transport=transport,
                http2=use_http2,
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                ),
            )
            self._owns_client = True

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "PolarisClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _auth_headers(
        self,
        auth_required: bool,
        include_auth_if_available: bool = False,
    ) -> dict[str, str]:
        if self.api_key and (auth_required or include_auth_if_available):
            return {"Authorization": f"Bearer {self.api_key}"}

        if auth_required:
            raise UnauthorizedError("API key is required for this endpoint")

        return {}

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.is_success:
            return

        status = response.status_code
        body_text = response.text
        message = body_text or f"HTTP error {status}"
        reset_at: str | None = None

        try:
            payload = response.json()
            if isinstance(payload, dict):
                message = str(payload.get("error") or payload.get("message") or message)
                raw_reset = payload.get("reset_at")
                if isinstance(raw_reset, str):
                    reset_at = raw_reset
        except json.JSONDecodeError:
            pass

        if status == 401:
            raise UnauthorizedError(message=message, status_code=status, body=body_text)
        if status == 402:
            raise AccessDeniedError(
                message=(
                    f"{message}. This dataset requires an active subscription. "
                    "See https://docs.polaris.supply/guides/authentication"
                ),
                status_code=status,
                body=body_text,
            )
        if status == 404:
            raise NotFoundError(message=message, status_code=status, body=body_text)
        if status == 429:
            raise RateLimitedError(
                message=message,
                status_code=status,
                body=body_text,
                reset_at=reset_at,
            )

        raise PolarisError(message=message, status_code=status, body=body_text)

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        auth_required: bool = False,
        include_auth_if_available: bool = False,
    ) -> JSONDict:
        response = self._client.get(
            path,
            params=params,
            headers=self._auth_headers(auth_required, include_auth_if_available),
        )
        self._raise_for_status(response)

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise PolarisError(
                message="Response was not valid JSON",
                status_code=response.status_code,
                body=response.text,
            ) from exc

        if not isinstance(payload, dict):
            raise PolarisError(
                message="Expected JSON object response",
                status_code=response.status_code,
                body=response.text,
            )

        return payload

    def _iter_paginated_data(
        self,
        path: str,
        *,
        params: dict[str, str],
        auth_required: bool,
    ) -> Iterator[JSONDict]:
        cursor: str | None = None
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor

            payload = self._get_json(path, params=page_params, auth_required=auth_required)
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise PolarisError(f"Invalid {path} response")

            for item in data:
                if isinstance(item, dict):
                    yield item

            if not payload.get("has_more"):
                break

            next_cursor = payload.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            cursor = next_cursor

    def _parse_ndjson_line(self, line: str | bytes) -> JSONDict:
        try:
            payload = orjson.loads(line)
        except orjson.JSONDecodeError as exc:
            raise StreamDecodeError(f"Invalid NDJSON line: {exc}") from exc

        if not isinstance(payload, dict):
            raise StreamDecodeError("Expected NDJSON line to decode to an object")

        return payload

    def _iter_ndjson_lines(self, lines: Iterator[str]) -> Iterator[JSONDict]:
        for line in lines:
            raw_line = line.rstrip("\r\n")
            if not raw_line:
                continue
            yield self._parse_ndjson_line(raw_line)

    def _iter_ndjson_file(self, path: Path, chunk_size: int) -> Iterator[JSONDict]:
        with path.open("r", encoding="utf-8", buffering=chunk_size, newline="") as file:
            yield from self._iter_ndjson_lines(file)

    def _iter_ndjson_zstd_file(self, path: Path, chunk_size: int) -> Iterator[JSONDict]:
        try:
            with path.open("rb", buffering=chunk_size) as file:
                with zstd.ZstdDecompressor().stream_reader(file) as reader:
                    with io.TextIOWrapper(
                        reader,
                        encoding="utf-8",
                        newline="",
                    ) as text_reader:
                        yield from self._iter_ndjson_lines(text_reader)
        except zstd.ZstdError as exc:
            raise StreamDecodeError(f"Invalid zstd stream: {exc}") from exc

    def _iter_ndjson_zstd_stream(self, chunks: Iterator[bytes]) -> Iterator[JSONDict]:
        try:
            with zstd.ZstdDecompressor().stream_reader(_ChunkStream(chunks)) as reader:
                with io.TextIOWrapper(
                    reader,
                    encoding="utf-8",
                    newline="",
                ) as text_reader:
                    yield from self._iter_ndjson_lines(text_reader)
        except zstd.ZstdError as exc:
            raise StreamDecodeError(f"Invalid zstd stream: {exc}") from exc

    def _iter_file_export_data(
        self,
        path: str,
        *,
        params: dict[str, str],
        chunk_size: int,
        timeout: float | None,
        auth_required: bool,
    ) -> Iterator[JSONDict]:
        request_kwargs: dict[str, Any] = {
            "params": params,
            "headers": self._auth_headers(auth_required=auth_required),
            "follow_redirects": True,
        }
        if timeout is not None:
            request_kwargs["timeout"] = timeout

        with self._client.stream("GET", path, **request_kwargs) as response:
            self._raise_for_status(response)

            # If file export is not enabled server-side, endpoints may return JSON.
            content_type = response.headers.get("content-type", "").lower()
            if "application/json" in content_type:
                raise PolarisError(
                    message=f"Expected compressed file export from {path}, received JSON",
                    status_code=response.status_code,
                )

            yield from self._iter_ndjson_zstd_stream(
                response.iter_bytes(chunk_size=chunk_size)
            )

    def _iter_raw_endpoint_data(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        chunk_size: int,
        timeout: float | None,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        params = self._range_params(source, market, from_, to)
        params["format"] = "file"
        yielded_any = False

        try:
            for row in self._iter_file_export_data(
                path="raw",
                params=params,
                chunk_size=chunk_size,
                timeout=timeout,
                auth_required=True,
            ):
                yielded_any = True
                yield row
            return
        except (httpx.HTTPError, PolarisError, StreamDecodeError):
            if yielded_any:
                raise

        fallback_params = self._range_params(source, market, from_, to)
        fallback_params["limit"] = str(limit)

        yield from self._iter_paginated_data(
            "raw",
            params=fallback_params,
            auth_required=True,
        )

    def _range_params(
        self,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> dict[str, str]:
        return {
            "source": source,
            "market": market,
            "from": to_iso8601(from_),
            "to": to_iso8601(to),
        }

    def _parse_snapshot_entry(self, raw: object) -> SnapshotEntry:
        if not isinstance(raw, dict):
            raise PolarisError("Invalid /snapshots response entry")

        key = raw.get("key") or raw.get("path") or raw.get("name")
        if not isinstance(key, str) or not key:
            raise PolarisError("Snapshot entry did not include a key")

        entry_date = raw.get("date")
        if not isinstance(entry_date, str) or not entry_date:
            entry_date = None

        return SnapshotEntry(key=key, date=entry_date)

    def _iter_local_snapshot_rows(
        self,
        paths: list[Path],
        *,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
    ) -> Iterator[JSONDict]:
        start_micros = (
            _datetime_to_epoch_micros(to_datetime(from_)) if from_ is not None else None
        )
        end_micros = _datetime_to_epoch_micros(to_datetime(to)) if to is not None else None

        for path in paths:
            if _file_is_zstd(path):
                rows = self._iter_ndjson_zstd_file(path, DEFAULT_FILE_CHUNK_SIZE)
            else:
                rows = self._iter_ndjson_file(path, DEFAULT_FILE_CHUNK_SIZE)

            for row in rows:
                if start_micros is None and end_micros is None:
                    yield row
                    continue

                timestamp = row.get("timestamp")
                if not isinstance(timestamp, (int, float)):
                    continue
                if start_micros is not None and timestamp < start_micros:
                    continue
                if end_micros is not None and timestamp >= end_micros:
                    return
                yield row

    def _filter_local_snapshots(
        self,
        entries: list[LocalSnapshotEntry],
        *,
        date_filter: str | date | None = None,
    ) -> list[LocalSnapshotEntry]:
        date_text = (
            date_filter.isoformat() if isinstance(date_filter, date) else date_filter
        )
        if date_text is None:
            return entries
        return [e for e in entries if e.date == date_text]

    def _daily_artifact_paths(
        self,
        *,
        source: str,
        market: str,
        required_dates: set[date] | None = None,
    ) -> dict[date, Path]:
        if required_dates is not None:
            return {
                day: path
                for day in sorted(required_dates)
                if (path := self.layout.daily_path_for_dataset_day(source, market, day)).exists()
            }

        artifacts = self.layout.list_local_daily_artifacts()
        result: dict[date, Path] = {}
        for artifact in artifacts:
            if artifact.source != source or artifact.market != market:
                continue
            try:
                result[date.fromisoformat(artifact.date)] = Path(artifact.path)
            except ValueError:
                continue
        return result

    def _materialize_local_daily_artifacts(
        self,
        *,
        source: str,
        market: str,
        required_dates: set[date] | None = None,
        force: bool = False,
    ) -> dict[date, Path]:
        daily_paths = self._daily_artifact_paths(
            source=source,
            market=market,
            required_dates=required_dates,
        )
        snapshots = self.layout.list_local_snapshots()

        candidates: list[LocalSnapshotEntry] = []
        for snapshot in snapshots:
            if snapshot.date is None:
                continue
            try:
                snapshot_day = date.fromisoformat(snapshot.date)
            except ValueError:
                continue
            if required_dates is not None and snapshot_day not in required_dates:
                continue
            if snapshot_day in daily_paths and not force:
                continue
            candidates.append(snapshot)

        if candidates:
            with self.layout.sync_lock():
                for snapshot in candidates:
                    path = self.layout.materialize_daily_artifact(snapshot, force=force)
                    if path is None or snapshot.date is None:
                        continue
                    daily_paths[date.fromisoformat(snapshot.date)] = path

        return daily_paths

    def _download_snapshot_file(
        self,
        snapshot: SnapshotEntry,
        *,
        force_materialize: bool = False,
    ) -> LocalSnapshotEntry:
        local_path = self.layout.data_path_for_key(snapshot.key)
        temp_path = self.layout.temp_path_for_key(snapshot.key)
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if temp_path.exists():
            temp_path.unlink()

        try:
            with self._client.stream(
                "GET",
                "snapshots/download",
                params={"key": snapshot.key},
                headers=self._auth_headers(
                    auth_required=False,
                    include_auth_if_available=True,
                ),
                follow_redirects=True,
            ) as response:
                self._raise_for_status(response)

                content_type = response.headers.get("content-type", "").lower()
                if "application/json" in content_type:
                    raise PolarisError(
                        message="Expected compressed snapshot download, received JSON",
                        status_code=response.status_code,
                    )

                with temp_path.open("wb") as file:
                    for chunk in response.iter_bytes(chunk_size=DEFAULT_NETWORK_CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
                    file.flush()
                    os.fsync(file.fileno())

            os.replace(temp_path, local_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

        entry = LocalSnapshotEntry(
            key=snapshot.key,
            path=str(local_path),
            source=snapshot.source,
            market=snapshot.market,
            date=snapshot.date,
            start=None,
            end=None,
        )
        self.layout.materialize_daily_artifact(entry, force=force_materialize)
        return entry

    def _ensure_local_snapshot_entries(
        self,
        snapshots: list[SnapshotEntry],
        *,
        force: bool = False,
    ) -> list[LocalSnapshotEntry]:
        indexed = {entry.key: entry for entry in self.layout.list_local_snapshots()}
        results: list[LocalSnapshotEntry] = []

        with self.layout.sync_lock():
            for snapshot in snapshots:
                local_entry = indexed.get(snapshot.key)
                local_path = self.layout.data_path_for_key(snapshot.key)

                if local_entry is not None and local_path.exists() and not force:
                    self.layout.materialize_daily_artifact(local_entry)
                    results.append(local_entry)
                    continue

                entry = self._download_snapshot_file(
                    snapshot,
                    force_materialize=force,
                )
                indexed[entry.key] = entry
                results.append(entry)

        return results

    def _resolve_snapshot_day_files(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> list[Path] | None:
        start = to_datetime(from_)
        end = to_datetime(to)
        required_days = _iter_utc_dates(start, end)
        required_day_set = set(required_days)

        daily_paths = self._materialize_local_daily_artifacts(
            source=source,
            market=market,
            required_dates=required_day_set,
        )
        missing_days = [day for day in required_days if day not in daily_paths]
        if not missing_days:
            return [daily_paths[day] for day in required_days]

        remote_snapshots = self.list_snapshots(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )
        snapshots_by_day: dict[date, SnapshotEntry] = {}
        for snapshot in remote_snapshots:
            if snapshot.date is None:
                continue
            try:
                day = date.fromisoformat(snapshot.date)
            except ValueError:
                continue
            if day not in snapshots_by_day:
                snapshots_by_day[day] = snapshot

        missing_remote_days = [day for day in missing_days if day not in snapshots_by_day]
        if missing_remote_days:
            return None

        self._ensure_local_snapshot_entries(
            [snapshots_by_day[day] for day in missing_days],
            force=False,
        )
        daily_paths = self._materialize_local_daily_artifacts(
            source=source,
            market=market,
            required_dates=required_day_set,
        )
        if any(day not in daily_paths for day in required_days):
            return None
        return [daily_paths[day] for day in required_days]

    def _aggregate_ohlcv_from_standard_trades(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        interval: str,
    ) -> list[JSONDict]:
        day_paths = self._resolve_snapshot_day_files(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )
        if day_paths is None:
            raise PolarisError(
                "Requested OHLCV range could not be satisfied from standardized snapshots"
            )
        aggregator = _LocalOhlcvAggregator(interval)
        for row in self._iter_local_snapshot_rows(day_paths, from_=from_, to=to):
            aggregator.push(row)
        return aggregator.finish()

    def health(self) -> JSONDict:
        return self._get_json("health")

    def catalog(
        self,
        *,
        source: str | None = None,
        market: str | None = None,
    ) -> JSONDict:
        params: dict[str, str] = {}
        if source is not None:
            params["source"] = source
        if market is not None:
            params["market"] = market
        return self._get_json(
            "catalog",
            params=params or None,
            include_auth_if_available=True,
        )

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

        params = self._range_params(source, market, from_, to)
        params["limit"] = str(limit)

        snapshots: dict[str, SnapshotEntry] = {}
        cursor: str | None = None
        access_info: dict[str, Any] | None = None
        while True:
            page_params = dict(params)
            if cursor is not None:
                page_params["cursor"] = cursor

            payload = self._get_json(
                "snapshots",
                params=page_params,
                include_auth_if_available=True,
            )

            # Capture access policy from first page (present on every page).
            if access_info is None:
                access_info = payload.get("access")

            # Proactive check: surface clear errors for unauthenticated users
            # requesting data that requires authentication.
            if access_info is not None and self.api_key is None:
                status = access_info.get("status")
                if status == "restricted":
                    raise AccessDeniedError(
                        message=(
                            f"Dataset '{source}/{market}' requires authentication. "
                            "Set POLARIS_API_KEY or pass api_key to PolarisClient. "
                            "See https://docs.polaris.supply/guides/authentication"
                        )
                    )
                if status == "preview":
                    cutoff_raw = access_info.get("public_cutoff_date")
                    if isinstance(cutoff_raw, str) and cutoff_raw:
                        try:
                            cutoff_date = date.fromisoformat(cutoff_raw)
                            requested_end = to_datetime(to).date()
                            if requested_end > cutoff_date:
                                raise AccessDeniedError(
                                    message=(
                                        f"Dataset '{source}/{market}' data after "
                                        f"{cutoff_raw} requires authentication. "
                                        "Set POLARIS_API_KEY or pass api_key to "
                                        "PolarisClient. "
                                        "See https://docs.polaris.supply/guides/authentication"
                                    )
                                )
                        except (ValueError, TypeError):
                            pass  # malformed date — skip check, never block accidentally

            raw_entries = []
            data = payload.get("data")
            if isinstance(data, list):
                raw_entries.extend(data)
            snapshot_data = payload.get("snapshots")
            if isinstance(snapshot_data, list):
                raw_entries.extend(snapshot_data)

            for raw in raw_entries:
                entry = self._parse_snapshot_entry(raw)
                if entry.source is None:
                    entry = dataclasses.replace(entry, source=source)
                if entry.market is None:
                    entry = dataclasses.replace(entry, market=market)
                snapshots[entry.key] = entry

            next_cursor = payload.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            cursor = next_cursor

        return [snapshots[key] for key in sorted(snapshots)]

    def _download_snapshots(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        force: bool = False,
    ) -> list[LocalSnapshotEntry]:
        snapshots = self.list_snapshots(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )
        return self._ensure_local_snapshot_entries(snapshots, force=force)

    def _list_local_snapshots(
        self,
        *,
        date: str | date | None = None,
    ) -> list[LocalSnapshotEntry]:
        return self._filter_local_snapshots(
            self.layout.list_local_snapshots(),
            date_filter=date,
        )

    def _iter_local_events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput | None = None,
        to: TimeInput | None = None,
    ) -> Iterator[JSONDict]:
        start = to_datetime(from_) if from_ is not None else None
        end = to_datetime(to) if to is not None else None
        if start is not None and end is not None and start >= end:
            raise ValueError("from_ must be before to")

        required_dates = (
            set(_iter_utc_dates(start, end))
            if start is not None and end is not None
            else None
        )
        daily_paths = self._materialize_local_daily_artifacts(
            source=source,
            market=market,
            required_dates=required_dates,
        )

        last_inclusive = (
            (end - timedelta(microseconds=1)).date() if end is not None else None
        )
        selected_paths: list[Path] = []
        for day in sorted(daily_paths):
            if start is not None and day < start.date():
                continue
            if last_inclusive is not None and day > last_inclusive:
                continue
            selected_paths.append(daily_paths[day])

        return self._iter_local_snapshot_rows(selected_paths, from_=from_, to=to)

    def replay(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool = True,
        chunk_size: int | None = None,
        timeout: float | None = None,
        parallel: bool | int = False,
    ) -> Iterator[JSONDict]:
        # Keep chunk_size validation for compatibility with prior replay signature.
        effective_chunk_size = (
            chunk_size if chunk_size is not None else DEFAULT_NETWORK_CHUNK_SIZE
        )
        if effective_chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")

        # Handle parallel chunking
        if parallel:
            max_workers = (
                parallel
                if isinstance(parallel, int) and not isinstance(parallel, bool)
                else 4
            )
            return self._replay_parallel(
                source=source,
                market=market,
                from_=from_,
                to=to,
                standard=standard,
                chunk_size=effective_chunk_size,
                timeout=timeout,
                max_workers=max_workers,
            )

        if standard:
            day_paths = self._resolve_snapshot_day_files(
                source=source,
                market=market,
                from_=from_,
                to=to,
            )
            if day_paths is not None:
                return self._iter_local_snapshot_rows(day_paths, from_=from_, to=to)
            raise PolarisError(
                "Requested replay range could not be satisfied from standardized snapshots"
            )

        # Check if cache exists first for legacy raw replay behavior.
        if self.replay_cache_enabled:
            canonical_zst_name = self._default_dataset_filename(
                source, market, from_, to, standard
            )
            cache_zst_path = self.replay_cache_dir / canonical_zst_name
            cache_jsonl_path = cache_zst_path.with_suffix("")

            # Serve from cache if already downloaded
            if cache_jsonl_path.exists():
                if _file_is_zstd(cache_jsonl_path):
                    return self._iter_ndjson_zstd_file(
                        cache_jsonl_path, DEFAULT_FILE_CHUNK_SIZE
                    )
                return self._iter_ndjson_file(cache_jsonl_path, DEFAULT_FILE_CHUNK_SIZE)
            if cache_zst_path.exists():
                if not _file_is_zstd(cache_zst_path):
                    return self._iter_ndjson_file(
                        cache_zst_path, DEFAULT_FILE_CHUNK_SIZE
                    )
                return self._iter_ndjson_zstd_file(
                    cache_zst_path, DEFAULT_FILE_CHUNK_SIZE
                )

        source_rows = self._iter_raw_endpoint_data(
            source=source,
            market=market,
            from_=from_,
            to=to,
            chunk_size=effective_chunk_size,
            timeout=timeout,
        )

        if self.replay_cache_enabled:
            canonical_zst_name = self._default_dataset_filename(
                source, market, from_, to, standard
            )
            cache_path = (self.replay_cache_dir / canonical_zst_name).with_suffix("")
            return self._replay_with_cache(source_rows, cache_path)

        return source_rows

    def _replay_parallel(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool = True,
        chunk_size: int,
        timeout: float | None = None,
        max_workers: int = 4,
    ) -> Iterator[JSONDict]:
        """
        Download and replay data in parallel time chunks.

        Splits the time range into chunks, downloads them in parallel,
        and yields records in chronological order.
        """
        # Split time range into 24-hour chunks
        time_chunks = chunk_timerange(from_, to, chunk_hours=24)

        if len(time_chunks) == 1:
            # Only one chunk, use regular replay
            return self.replay(
                source=source,
                market=market,
                from_=from_,
                to=to,
                standard=standard,
                chunk_size=chunk_size,
                timeout=timeout,
                parallel=False,
            )

        # Download chunks in parallel and collect results
        def _parallel_iterator() -> Iterator[JSONDict]:
            # Download chunks in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all chunk downloads
                future_to_index = {}
                for idx, (chunk_start, chunk_end) in enumerate(time_chunks):
                    future = executor.submit(
                        self._download_chunk_to_list,
                        source=source,
                        market=market,
                        from_=chunk_start,
                        to=chunk_end,
                        standard=standard,
                        chunk_size=chunk_size,
                        timeout=timeout,
                    )
                    future_to_index[future] = idx

                # Collect results as they complete
                chunk_results: dict[int, list[JSONDict]] = {}
                for future in as_completed(future_to_index):
                    idx = future_to_index[future]
                    try:
                        records = future.result()
                        chunk_results[idx] = records
                    except Exception as exc:
                        raise PolarisError(
                            f"Parallel chunk {idx} download failed: {exc}"
                        ) from exc

            # Yield records in chronological order
            for idx in sorted(chunk_results.keys()):
                yield from chunk_results[idx]

        return _parallel_iterator()

    def _download_chunk_to_list(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool,
        chunk_size: int,
        timeout: float | None,
    ) -> list[JSONDict]:
        """Download a single time chunk and return all records as a list."""
        records = []
        for record in self.replay(
            source=source,
            market=market,
            from_=from_,
            to=to,
            standard=standard,
            chunk_size=chunk_size,
            timeout=timeout,
            parallel=False,  # Prevent recursive parallel calls
        ):
            records.append(record)
        return records

    def _replay_with_cache(
        self, rows: Iterator[JSONDict], cache_path: Path
    ) -> Iterator[JSONDict]:
        """Stream replay rows while caching them locally as NDJSON."""
        temp_cache_path: Path | None = None
        cache_file = None

        def _cached_iterator() -> Iterator[JSONDict]:
            nonlocal temp_cache_path, cache_file

            try:
                temp_cache_path = cache_path.with_name(f".{cache_path.name}.part")
                temp_cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_file = temp_cache_path.open("wb")

                for row in rows:
                    cache_file.write(orjson.dumps(row))
                    cache_file.write(b"\n")
                    yield row

                cache_file.close()
                cache_file = None

                os.replace(temp_cache_path, cache_path)
                temp_cache_path = None

            except Exception:
                if cache_file:
                    try:
                        cache_file.close()
                    except Exception:
                        pass
                if temp_cache_path and temp_cache_path.exists():
                    try:
                        temp_cache_path.unlink()
                    except Exception:
                        pass
                raise

        return _cached_iterator()

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
        return (
            f"{_safe_filename_fragment(source)}_"
            f"{_safe_filename_fragment(market)}_"
            f"{_safe_filename_fragment(from_text)}_"
            f"{_safe_filename_fragment(to_text)}_"
            f"{mode}.jsonl.zst"
        )

    def trades(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> list[JSONDict]:
        return list(
            self._iter_trades_data(
                source=source,
                market=market,
                from_=from_,
                to=to,
            )
        )

    def _iter_trades_data(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> Iterator[JSONDict]:
        day_paths = self._resolve_snapshot_day_files(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )
        if day_paths is None:
            raise PolarisError(
                "Requested trade range could not be satisfied from standardized snapshots"
            )
        for row in self._iter_local_snapshot_rows(day_paths, from_=from_, to=to):
            if row.get("type") == "trade":
                yield row

    def events(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> list[JSONDict]:
        return list(
            self._iter_events_data(
                source=source,
                market=market,
                from_=from_,
                to=to,
            )
        )

    def _iter_events_data(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> Iterator[JSONDict]:
        day_paths = self._resolve_snapshot_day_files(
            source=source,
            market=market,
            from_=from_,
            to=to,
        )
        if day_paths is None:
            raise PolarisError(
                "Requested event range could not be satisfied from standardized snapshots"
            )
        yield from self._iter_local_snapshot_rows(day_paths, from_=from_, to=to)

    def raw(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> list[JSONDict]:
        return list(
            self._iter_raw_data(
                source=source,
                market=market,
                from_=from_,
                to=to,
                limit=limit,
            )
        )

    def _iter_raw_data(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        yield from self._iter_raw_endpoint_data(
            source=source,
            market=market,
            from_=from_,
            to=to,
            chunk_size=DEFAULT_NETWORK_CHUNK_SIZE,
            timeout=None,
            limit=limit,
        )

    def ohlcv(
        self,
        *,
        source: str,
        market: str,
        from_: TimeInput,
        to: TimeInput,
        interval: str,
        format: str | None = None,
    ) -> list[JSONDict] | JSONDict:
        if _interval_to_us(interval) is None:
            raise ValueError(
                "interval must be one of: " + ", ".join(_INTERVAL_US)
            )

        if format is None:
            return self._aggregate_ohlcv_from_standard_trades(
                source=source,
                market=market,
                from_=from_,
                to=to,
                interval=interval,
            )

        if format != "tradingview":
            raise ValueError("format must be one of: None, 'tradingview'")

        bars = self._aggregate_ohlcv_from_standard_trades(
            source=source,
            market=market,
            from_=from_,
            to=to,
            interval=interval,
        )
        return _to_tradingview_ohlcv(bars)
