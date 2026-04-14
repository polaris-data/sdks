"""Synchronous client for the Polaris API."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

import httpx
import zstandard as zstd

from .errors import (
    DownloadNotAllowedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)
from .models import DownloadUrlResponse, JSONDict, OhlcvParquetResponse, PaginatedResponse
from .utils import TimeInput, bool_to_query, to_iso8601

DEFAULT_BASE_URL = "https://polaris.supply/api"
DEFAULT_TIMEOUT = 30.0
USER_AGENT = "polaris-py/0.1.2"
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
        allow_dataset_downloads: bool = False,
        dataset_download_dir: str | os.PathLike[str] | None = None,
        replay_cache_enabled: bool = True,
        replay_cache_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        if transport is not None and http_client is not None:
            raise ValueError("Pass either transport or http_client, not both")

        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.allow_dataset_downloads = allow_dataset_downloads
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
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={"User-Agent": USER_AGENT},
                transport=transport,
            )
            self._owns_client = True

    @classmethod
    def new(cls, api_key: str, **kwargs: Any) -> "PolarisClient":
        """Create an authenticated client."""
        return cls(api_key=api_key, **kwargs)

    @classmethod
    def anonymous(cls, **kwargs: Any) -> "PolarisClient":
        """Create a client for open endpoints only."""
        return cls(api_key=None, **kwargs)

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

    def _iter_zstd_decompressed_chunks(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
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
        decompressed_chunks = self._iter_zstd_decompressed_chunks(self._iter_file_chunks(path, chunk_size))
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
        payload = self._get_json("catalog/exchanges")
        exchanges = payload.get("exchanges", [])
        if not isinstance(exchanges, list):
            raise PolarisError("Invalid exchanges response")
        return [str(exchange) for exchange in exchanges]

    def assets(self, exchange: str) -> list[str]:
        payload = self._get_json("catalog/assets", params={"exchange": exchange})
        assets = payload.get("assets", [])
        if not isinstance(assets, list):
            raise PolarisError("Invalid assets response")
        return [str(asset) for asset in assets]

    def timerange(self, exchange: str, asset: str) -> JSONDict:
        return self._get_json(
            "catalog/timerange",
            params={"exchange": exchange, "asset": asset},
        )

    def dataset_size(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
    ) -> JSONDict:
        return self._get_json(
            "catalog/size",
            params=self._range_params(exchange, asset, from_, to),
        )

    def catalog(self) -> JSONDict:
        return self._get_json("catalog", include_auth_if_available=True)

    def dataset_preview(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
    ) -> list[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["standard"] = bool_to_query(standard)
        payload = self._get_json("datasets/preview", params=params)
        events = payload.get("events", [])
        if not isinstance(events, list):
            raise PolarisError("Invalid dataset preview response")
        return [event for event in events if isinstance(event, dict)]

    def dataset_download_url(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
    ) -> DownloadUrlResponse:
        params = self._range_params(exchange, asset, from_, to)
        params["standard"] = bool_to_query(standard)
        payload = self._get_json("datasets/download", params=params, auth_required=True)
        return payload  # type: ignore[return-value]

    def replay(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
        chunk_size: int = 1024 * 1024,
    ) -> Iterator[JSONDict]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")

        if self.replay_cache_enabled:
            canonical_zst_name = self._default_dataset_filename(exchange, asset, from_, to, standard)
            cache_zst_path = self.replay_cache_dir / canonical_zst_name
            cache_jsonl_path = cache_zst_path.with_suffix("")

            if cache_jsonl_path.exists():
                if _file_is_zstd(cache_jsonl_path):
                    return self._iter_ndjson_zstd_file(cache_jsonl_path, chunk_size)
                return self._iter_ndjson_file(cache_jsonl_path, chunk_size)
            if cache_zst_path.exists():
                if not _file_is_zstd(cache_zst_path):
                    return self._iter_ndjson_file(cache_zst_path, chunk_size)
                return self._iter_ndjson_zstd_file(cache_zst_path, chunk_size)

        payload = self.dataset_download_url(exchange, asset, from_, to, standard=standard)
        download_url = payload.get("url")
        if not isinstance(download_url, str) or not download_url:
            raise PolarisError("Invalid dataset download response: missing url")

        if self.replay_cache_enabled:
            download_path = urlparse(download_url).path.lower()
            cache_filename = canonical_zst_name if download_path.endswith(".zst") else cache_jsonl_path.name
            cached_path = self._download_dataset_impl(
                exchange,
                asset,
                from_,
                to,
                standard=standard,
                destination=self.replay_cache_dir,
                filename=cache_filename,
                overwrite=False,
                decompress=True,
                keep_compressed=False,
                chunk_size=chunk_size,
                require_opt_in=False,
                download_url=download_url,
            )
            return self._iter_ndjson_file(cached_path, chunk_size)

        is_zst = urlparse(download_url).path.lower().endswith(".zst")

        def _iterator() -> Iterator[JSONDict]:
            with self._client.stream("GET", download_url, follow_redirects=True) as response:
                if response.is_error:
                    raise PolarisError(
                        "Dataset replay download failed",
                        status_code=response.status_code,
                        body=response.text,
                    )

                chunks = response.iter_bytes(chunk_size=chunk_size)
                source_chunks = self._iter_zstd_decompressed_chunks(chunks) if is_zst else chunks
                yield from self._iter_ndjson_from_chunks(source_chunks)

        return _iterator()

    def _dataset_filename_from_url(self, url: str) -> str | None:
        filename = Path(unquote(urlparse(url).path)).name
        if not filename:
            return None
        return filename

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

    def _download_dataset_impl(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = False,
        destination: str | os.PathLike[str] | None = None,
        filename: str | None = None,
        overwrite: bool = False,
        decompress: bool = True,
        keep_compressed: bool = False,
        chunk_size: int = 1024 * 1024,
        require_opt_in: bool,
        download_url: str | None = None,
    ) -> Path:
        if require_opt_in and not self.allow_dataset_downloads:
            raise DownloadNotAllowedError(
                "Dataset downloads are disabled. Recreate the client with "
                "allow_dataset_downloads=True to enable file downloads."
            )

        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")

        if filename is not None and Path(filename).name != filename:
            raise ValueError("filename must be a file name, not a path")

        resolved_download_url = download_url
        if resolved_download_url is None:
            payload = self.dataset_download_url(exchange, asset, from_, to, standard=standard)
            resolved_download_url = payload.get("url")

        download_url = resolved_download_url
        if not isinstance(download_url, str) or not download_url:
            raise PolarisError("Invalid dataset download response: missing url")

        target_dir = Path(destination).expanduser() if destination is not None else self.dataset_download_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        resolved_filename = filename or self._dataset_filename_from_url(download_url)
        if not resolved_filename:
            resolved_filename = self._default_dataset_filename(exchange, asset, from_, to, standard)

        target_path = target_dir / resolved_filename
        if target_path.exists() and not overwrite:
            raise PolarisError(
                f"Dataset file already exists: {target_path}. Pass overwrite=True to replace it."
            )

        decompressed_path: Path | None = None
        if decompress and target_path.suffix.lower() == ".zst":
            decompressed_path = target_path.with_suffix("")
            if decompressed_path.exists() and not overwrite:
                raise PolarisError(
                    f"Dataset file already exists: {decompressed_path}. Pass overwrite=True to replace it."
                )

        temp_path: Path | None = None
        try:
            with self._client.stream("GET", download_url, follow_redirects=True) as response:
                if response.is_error:
                    body = response.text
                    raise PolarisError(
                        "Dataset file download failed",
                        status_code=response.status_code,
                        body=body,
                    )

                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    delete=False,
                    dir=target_dir,
                    prefix=f".{target_path.name}.",
                    suffix=".part",
                ) as temp_file:
                    temp_path = Path(temp_file.name)
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        if chunk:
                            temp_file.write(chunk)

            if temp_path is None:
                raise PolarisError("Dataset file download failed before writing any data")

            os.replace(temp_path, target_path)
        except Exception:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()
            raise

        if not decompress or target_path.suffix.lower() != ".zst":
            return target_path

        if decompressed_path is None:
            raise PolarisError("Dataset decompression path resolution failed")

        decompressed_temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                delete=False,
                dir=target_dir,
                prefix=f".{decompressed_path.name}.",
                suffix=".part",
            ) as temp_file:
                decompressed_temp_path = Path(temp_file.name)
                with target_path.open("rb") as compressed_file:
                    with zstd.ZstdDecompressor().stream_reader(compressed_file) as reader:
                        while True:
                            chunk = reader.read(chunk_size)
                            if not chunk:
                                break
                            temp_file.write(chunk)

            if decompressed_temp_path is None:
                raise PolarisError("Dataset decompression failed before writing any data")

            os.replace(decompressed_temp_path, decompressed_path)
        except zstd.ZstdError as exc:
            if decompressed_temp_path is not None and decompressed_temp_path.exists():
                decompressed_temp_path.unlink()
            raise PolarisError(f"Dataset decompression failed: {exc}") from exc
        except Exception:
            if decompressed_temp_path is not None and decompressed_temp_path.exists():
                decompressed_temp_path.unlink()
            raise

        if not keep_compressed:
            target_path.unlink()

        return decompressed_path

    def download_dataset(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
        destination: str | os.PathLike[str] | None = None,
        filename: str | None = None,
        overwrite: bool = False,
        decompress: bool = True,
        keep_compressed: bool = False,
        chunk_size: int = 1024 * 1024,
    ) -> Path:
        return self._download_dataset_impl(
            exchange,
            asset,
            from_,
            to,
            standard=standard,
            destination=destination,
            filename=filename,
            overwrite=overwrite,
            decompress=decompress,
            keep_compressed=keep_compressed,
            chunk_size=chunk_size,
            require_opt_in=True,
            download_url=None,
        )

    def trades_page(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> PaginatedResponse:
        params = self._range_params(exchange, asset, from_, to)
        params["limit"] = str(limit)
        if cursor:
            params["cursor"] = cursor
        payload = self._get_json("trades", params=params, auth_required=True)
        return payload  # type: ignore[return-value]

    def iter_trades(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        limit: int = 1000,
    ) -> Iterator[JSONDict]:
        cursor: str | None = None

        while True:
            page = self.trades_page(
                exchange,
                asset,
                from_,
                to,
                limit=limit,
                cursor=cursor,
            )
            for item in page.get("data", []):
                if isinstance(item, dict):
                    yield item

            if not page.get("has_more"):
                break

            cursor = page.get("next_cursor")
            if not cursor:
                break

    def collect_all_trades(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        limit: int = 1000,
    ) -> list[JSONDict]:
        return list(self.iter_trades(exchange, asset, from_, to, limit=limit))

    def stream_events(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
    ) -> Iterator[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["standard"] = bool_to_query(standard)
        headers = self._auth_headers(auth_required=True)

        def _iterator() -> Iterator[JSONDict]:
            with self._client.stream("GET", "stream", params=params, headers=headers) as response:
                self._raise_for_status(response)
                for line in response.iter_lines():
                    if not line:
                        continue
                    yield self._parse_ndjson_line(line)

        return _iterator()

    def collect_events(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        standard: bool = True,
    ) -> list[JSONDict]:
        return list(self.stream_events(exchange, asset, from_, to, standard=standard))

    def ohlcv_preview(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        interval: str,
        limit: int | None = None,
        format: str | None = None,
    ) -> JSONDict | list[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["interval"] = interval
        if limit is not None:
            params["limit"] = str(limit)
        if format is not None:
            params["format"] = format

        payload = self._get_json("ohlcv/preview", params=params)
        if format == "tradingview":
            return payload

        bars = payload.get("bars", [])
        if not isinstance(bars, list):
            raise PolarisError("Invalid OHLCV preview response")
        return [bar for bar in bars if isinstance(bar, dict)]

    def iter_ohlcv(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        interval: str,
    ) -> Iterator[JSONDict]:
        params = self._range_params(exchange, asset, from_, to)
        params["interval"] = interval
        headers = self._auth_headers(auth_required=True)

        def _iterator() -> Iterator[JSONDict]:
            with self._client.stream("GET", "ohlcv", params=params, headers=headers) as response:
                self._raise_for_status(response)
                for line in response.iter_lines():
                    if not line:
                        continue
                    yield self._parse_ndjson_line(line)

        return _iterator()

    def ohlcv(
        self,
        exchange: str,
        asset: str,
        from_: TimeInput,
        to: TimeInput,
        *,
        interval: str,
        format: str | None = None,
    ) -> list[JSONDict] | JSONDict | OhlcvParquetResponse:
        if format is None:
            return list(self.iter_ohlcv(exchange, asset, from_, to, interval=interval))

        if format not in {"tradingview", "parquet"}:
            raise ValueError("format must be one of: None, 'tradingview', 'parquet'")

        params = self._range_params(exchange, asset, from_, to)
        params["interval"] = interval
        params["format"] = format

        payload = self._get_json("ohlcv", params=params, auth_required=True)
        return payload
