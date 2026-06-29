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
from .models import LocalSnapshotEntry, SnapshotEntry

__all__ = [
    "AccessDeniedError",
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

__version__ = "0.8.1"
