"""Python SDK for the Polaris market data API."""

from .client import PolarisClient
from .errors import (
    AccessDeniedError,
    DownloadNotAllowedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
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
    "DownloadNotAllowedError",
    "LocalSnapshotEntry",
    "PolarisClient",
    "PolarisError",
    "RateLimitedError",
    "SnapshotEntry",
    "StreamDecodeError",
    "UnauthorizedError",
]

__version__ = "0.8.6"
