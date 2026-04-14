"""Synchronous client for the Polaris API."""

from __future__ import annotations

import os
import json
from typing import Any, Iterator

import httpx

from .errors import (
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
USER_AGENT = "polaris-py/0.1.0"


class PolarisClient:
    """High-level sync SDK client for Polaris datasets and market data."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        if transport is not None and http_client is not None:
            raise ValueError("Pass either transport or http_client, not both")

        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

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
        standard: bool = False,
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
        standard: bool = False,
    ) -> DownloadUrlResponse:
        params = self._range_params(exchange, asset, from_, to)
        params["standard"] = bool_to_query(standard)
        payload = self._get_json("datasets/download", params=params, auth_required=True)
        return payload  # type: ignore[return-value]

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
        standard: bool = False,
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
        standard: bool = False,
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
