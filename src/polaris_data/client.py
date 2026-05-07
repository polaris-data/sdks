"""Synchronous client for the Polaris API."""

from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator

import httpx
import zstandard as zstd

from .errors import (
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)
from .models import (
    JSONDict,
    OhlcvParquetResponse,
)
from .utils import TimeInput, chunk_timerange, to_iso8601

DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0
DEFAULT_NETWORK_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB for network downloads
DEFAULT_FILE_CHUNK_SIZE = 1 * 1024 * 1024  # 1MB for file operations
USER_AGENT = "polaris-py/0.3.1"
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def _default_dataset_download_dir() -> Path:
    override = os.getenv("POLARIS_DATASET_DOWNLOAD_DIR")
    if override:
        return Path(override).expanduser()

    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library" / "Caches" / "polaris" / "datasets"
    if os.name == "nt":
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            return Path(local_app_data) / "polaris" / "datasets"
        return home / "AppData" / "Local" / "polaris" / "datasets"
    return home / ".cache" / "polaris" / "datasets"


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


class PolarisClient:
    """High-level sync SDK client for Polaris datasets and market data."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        dataset_download_dir: str | os.PathLike[str] | None = None,
        replay_cache_enabled: bool = True,
        replay_cache_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        if transport is not None and http_client is not None:
            raise ValueError("Pass either transport or http_client, not both")

        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.dataset_download_dir = (
            Path(dataset_download_dir).expanduser()
            if dataset_download_dir is not None
            else _default_dataset_download_dir()
        )
        self.replay_cache_enabled = replay_cache_enabled
        self.replay_cache_dir = (
            Path(replay_cache_dir).expanduser()
            if replay_cache_dir is not None
            else self.dataset_download_dir / "replay"
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
        if isinstance(line, bytes):
            decoded = line.decode("utf-8")
        else:
            decoded = line

        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise StreamDecodeError(f"Invalid NDJSON line: {exc.msg}") from exc

        if not isinstance(payload, dict):
            raise StreamDecodeError("Expected NDJSON line to decode to an object")

        return payload

    def _iter_ndjson_from_chunks(self, chunks: Iterator[bytes]) -> Iterator[JSONDict]:
        buffer = b""
        for chunk in chunks:
            if not chunk:
                continue
            buffer += chunk
            while True:
                line_end = buffer.find(b"\n")
                if line_end < 0:
                    break
                raw_line = buffer[:line_end].rstrip(b"\r")
                buffer = buffer[line_end + 1 :]
                if not raw_line:
                    continue
                yield self._parse_ndjson_line(raw_line)

        tail = buffer.rstrip(b"\r")
        if tail:
            yield self._parse_ndjson_line(tail)

    def _iter_zstd_decompressed_chunks(
        self, chunks: Iterator[bytes]
    ) -> Iterator[bytes]:
        decompressor = zstd.ZstdDecompressor().decompressobj()
        try:
            for chunk in chunks:
                if not chunk:
                    continue
                decoded = decompressor.decompress(chunk)
                if decoded:
                    yield decoded
            tail = decompressor.flush()
            if tail:
                yield tail
        except zstd.ZstdError as exc:
            raise StreamDecodeError(f"Invalid zstd stream: {exc}") from exc

    def _iter_file_chunks(self, path: Path, chunk_size: int) -> Iterator[bytes]:
        with path.open("rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def _iter_ndjson_file(self, path: Path, chunk_size: int) -> Iterator[JSONDict]:
        return self._iter_ndjson_from_chunks(self._iter_file_chunks(path, chunk_size))

    def _iter_ndjson_zstd_file(self, path: Path, chunk_size: int) -> Iterator[JSONDict]:
        decompressed_chunks = self._iter_zstd_decompressed_chunks(
            self._iter_file_chunks(path, chunk_size)
        )
        return self._iter_ndjson_from_chunks(decompressed_chunks)

    def _range_params(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> dict[str, str]:
        return {
            "exchange": exchange,
            "asset": asset,
            "from": to_iso8601(from_),
            "to": to_iso8601(to),
        }

    def health(self) -> JSONDict:
        return self._get_json("health")

    def exchanges(self) -> list[str]:
        payload = self.catalog()
        exchanges = payload.get("exchanges", [])
        if not isinstance(exchanges, list):
            raise PolarisError("Invalid catalog response")

        exchange_ids: list[str] = []
        for exchange in exchanges:
            if not isinstance(exchange, dict):
                raise PolarisError("Invalid catalog response")
            exchange_id = exchange.get("id")
            if not isinstance(exchange_id, str):
                raise PolarisError("Invalid catalog response")
            exchange_ids.append(exchange_id)

        return exchange_ids

    def assets(self, *, exchange: str) -> list[str]:
        payload = self.catalog()
        exchanges = payload.get("exchanges", [])
        if not isinstance(exchanges, list):
            raise PolarisError("Invalid catalog response")

        for item in exchanges:
            if not isinstance(item, dict):
                raise PolarisError("Invalid catalog response")
            if item.get("id") != exchange:
                continue
            assets = item.get("assets", [])
            if not isinstance(assets, list):
                raise PolarisError("Invalid catalog response")
            return [str(asset) for asset in assets]

        return []

    def timerange(self, *, exchange: str, asset: str) -> JSONDict:
        return self._get_json(
            "timerange",
            params={"exchange": exchange, "asset": asset},
        )

    def catalog(self) -> JSONDict:
        return self._get_json("catalog", include_auth_if_available=True)

    def replay(
        self,
        *,
        exchange: str,
        asset: str,
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
                exchange=exchange,
                asset=asset,
                from_=from_,
                to=to,
                standard=standard,
                chunk_size=effective_chunk_size,
                timeout=timeout,
                max_workers=max_workers,
            )

        # Check if cache exists first.
        if self.replay_cache_enabled:
            canonical_zst_name = self._default_dataset_filename(
                exchange, asset, from_, to, standard
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

        source_rows = (
            self._iter_events_data(exchange=exchange, asset=asset, from_=from_, to=to)
            if standard
            else self._iter_raw_data(exchange=exchange, asset=asset, from_=from_, to=to)
        )

        if self.replay_cache_enabled:
            canonical_zst_name = self._default_dataset_filename(
                exchange, asset, from_, to, standard
            )
            cache_path = (self.replay_cache_dir / canonical_zst_name).with_suffix("")
            return self._replay_with_cache(source_rows, cache_path)

        return source_rows

    def _replay_parallel(
        self,
        *,
        exchange: str,
        asset: str,
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
                exchange=exchange,
                asset=asset,
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
                        exchange=exchange,
                        asset=asset,
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
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool,
        chunk_size: int,
        timeout: float | None,
    ) -> list[JSONDict]:
        """Download a single time chunk and return all records as a list."""
        records = []
        for record in self.replay(
            exchange=exchange,
            asset=asset,
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
                cache_file = temp_cache_path.open("w", encoding="utf-8")

                for row in rows:
                    line = json.dumps(row, separators=(",", ":"), ensure_ascii=True)
                    cache_file.write(f"{line}\n")
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
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        standard: bool,
    ) -> str:
        from_text = to_iso8601(from_).replace(":", "-")
        to_text = to_iso8601(to).replace(":", "-")
        mode = "standard" if standard else "raw"
        return (
            f"{_safe_filename_fragment(exchange)}_"
            f"{_safe_filename_fragment(asset)}_"
            f"{_safe_filename_fragment(from_text)}_"
            f"{_safe_filename_fragment(to_text)}_"
            f"{mode}.jsonl.zst"
        )

    def trades(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> list[JSONDict]:
        return list(
            self._iter_trades_data(
                exchange=exchange,
                asset=asset,
                from_=from_,
                to=to,
                limit=limit,
            )
        )

    def _iter_trades_data(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["limit"] = str(limit)
        return self._iter_paginated_data("trades", params=params, auth_required=True)

    def events(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> list[JSONDict]:
        return list(
            self._iter_events_data(
                exchange=exchange,
                asset=asset,
                from_=from_,
                to=to,
                limit=limit,
            )
        )

    def _iter_events_data(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["limit"] = str(limit)
        return self._iter_paginated_data("events", params=params, auth_required=True)

    def raw(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> list[JSONDict]:
        return list(
            self._iter_raw_data(
                exchange=exchange,
                asset=asset,
                from_=from_,
                to=to,
                limit=limit,
            )
        )

    def _iter_raw_data(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["limit"] = str(limit)
        return self._iter_paginated_data("raw", params=params, auth_required=True)

    def ohlcv(
        self,
        *,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        interval: str,
        format: str | None = None,
    ) -> list[JSONDict] | JSONDict | OhlcvParquetResponse:
        if format is None:
            params = self._range_params(exchange, asset, from_, to)
            params["interval"] = interval
            headers = self._auth_headers(auth_required=True)
            with self._client.stream("GET", "ohlcv", params=params, headers=headers) as response:
                self._raise_for_status(response)
                return [self._parse_ndjson_line(line) for line in response.iter_lines() if line]

        if format not in {"tradingview", "parquet"}:
            raise ValueError("format must be one of: None, 'tradingview', 'parquet'")

        params = self._range_params(exchange, asset, from_, to)
        params["interval"] = interval
        params["format"] = format

        payload = self._get_json("ohlcv", params=params, auth_required=True)
        return payload
