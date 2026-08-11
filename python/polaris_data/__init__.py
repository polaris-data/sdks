"""Python SDK for the Polaris market data API."""

from .client import OrderbookBuilder, PolarisClient, RealtimeStream
from .errors import (
    AccessDeniedError,
    DownloadNotAllowedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    StreamConnectionError,
    StreamProtocolError,
    UnauthorizedError,
)
from .models import (
    BulkDownloadManifest,
    BulkDownloadSnapshotEntry,
    CatalogAccess,
    CatalogInstrument,
    CatalogMarketEntry,
    CatalogResponse,
    LocalSnapshotEntry,
    SnapshotEntry,
)

__all__ = [
    "AccessDeniedError",
    "BulkDownloadManifest",
    "BulkDownloadSnapshotEntry",
    "CatalogAccess",
    "CatalogInstrument",
    "CatalogMarketEntry",
    "CatalogResponse",
    "NotFoundError",
    "OrderbookBuilder",
    "DownloadNotAllowedError",
    "LocalSnapshotEntry",
    "PolarisClient",
    "PolarisError",
    "RateLimitedError",
    "RealtimeStream",
    "SnapshotEntry",
    "StreamDecodeError",
    "StreamConnectionError",
    "StreamProtocolError",
    "UnauthorizedError",
]

__version__ = "0.12.0"
