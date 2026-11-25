"""Synchronous Python client for the Polaris API."""

from __future__ import annotations

import json
import os
from typing import Any, Iterator, Mapping, Sequence
from weakref import WeakSet

from . import _native
from .errors import (
    AccessDeniedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamConnectionError,
    StreamDecodeError,
    StreamProtocolError,
    UnauthorizedError,
)
from .models import DataTime, GeneratedClient, SubscriptionRequest, TableDataMessage

JSONDict = dict[str, Any]
DEFAULT_BASE_URL = "https://api.polaris.supply"
DEFAULT_TIMEOUT = 30.0


def _native_error(error: Exception) -> PolarisError:
    payload = json.loads(str(error))
    kind = payload["kind"]
    if kind == "stream_protocol":
        return StreamProtocolError(payload["message"], code=payload.get("code"))
    if kind == "rate_limited":
        result = RateLimitedError(
            payload["message"], payload.get("status_code"), payload.get("body"),
            reset_at=payload.get("reset_at"),
        )
    else:
        error_type = {
            "unauthorized": UnauthorizedError,
            "access_denied": AccessDeniedError,
            "not_found": NotFoundError,
            "stream_connection": StreamConnectionError,
            "stream_decode": StreamDecodeError,
        }.get(kind, PolarisError)
        result = error_type(payload["message"], payload.get("status_code"), payload.get("body"))
    result.code = payload.get("code")
    result.resolution = payload.get("resolution")
    return result


class OrderbookBuilder:
    """Reconstruct complete books from standardized orderbook events."""

    def __init__(self) -> None:
        self._native = _native.NativeOrderbookBuilder()

    def apply(self, event: JSONDict) -> JSONDict | None:
        """Apply an event, suppressing deltas until their book has a snapshot."""
        try:
            return self._native.apply(event)
        except _native.NativeError as error:
            raise _native_error(error) from None

    def update(self, event: JSONDict) -> bool:
        """Update book state without constructing a complete orderbook."""
        try:
            return self._native.update(event)
        except _native.NativeError as error:
            raise _native_error(error) from None

    def snapshot(self, source: str, market: str) -> JSONDict | None:
        """Return the current complete book for a source and market."""
        return self._native.snapshot(source, market)

    def clear(self) -> None:
        self._native.clear()

    def clear_book(self, source: str, market: str) -> None:
        self._native.clear_book(source, market)


class RealtimeStream(Iterator[TableDataMessage]):
    """A closeable iterator of complete table pages and resume cursors."""

    def __init__(self, client: PolarisClient, native: Any) -> None:
        self._client = client
        self._native = native
        self._closed = False

    def __iter__(self) -> RealtimeStream:
        return self

    def __next__(self) -> TableDataMessage:
        if self._closed:
            raise StopIteration
        try:
            return json.loads(next(self._native))
        except _native.NativeError as error:
            self.close()
            raise _native_error(error) from None
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._native.close()
            self._client._streams.discard(self)

    def __enter__(self) -> RealtimeStream:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class PolarisClient(GeneratedClient):
    """Read historical data and subscribe to realtime table pages."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        stream_url: str | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("POLARIS_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        try:
            self._native = _native.NativeClient(
                self.api_key, self.base_url, timeout, stream_url
            )
        except _native.NativeError as error:
            raise _native_error(error) from None
        self._closed = False
        self._streams: WeakSet[RealtimeStream] = WeakSet()

    def _require_open(self) -> None:
        if self._closed:
            raise PolarisError("Polaris client is closed")

    def _request(self, operation: str, query: Any) -> Any:
        self._require_open()
        try:
            return json.loads(self._native.request(operation, json.dumps(query)))
        except _native.NativeError as error:
            raise _native_error(error) from None

    def data_rows(
        self,
        table: str,
        *,
        start: DataTime | None = None,
        end: DataTime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        filters: Mapping[str, str] | None = None,
    ) -> Iterator[JSONDict]:
        """Yield rows page by page, starting at the given cursor."""
        self._require_open()
        query = dict(
            table=table, start=start, end=end, limit=limit, cursor=cursor,
            filters=dict(filters) if filters is not None else None,
        )
        native = None
        try:
            native = self._native.data_rows(json.dumps(query))
            while not self._closed:
                try:
                    yield json.loads(next(native))
                except StopIteration:
                    return
        except _native.NativeError as error:
            raise _native_error(error) from None
        finally:
            if native is not None:
                native.close()

    def stream(self, subscriptions: Sequence[SubscriptionRequest]) -> RealtimeStream:
        self._require_open()
        try:
            native = self._native.stream(json.dumps(dict(subscriptions=list(subscriptions))))
        except _native.NativeError as error:
            raise _native_error(error) from None
        stream = RealtimeStream(self, native)
        self._streams.add(stream)
        return stream

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            for stream in list(self._streams):
                stream.close()

    def __enter__(self) -> PolarisClient:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
