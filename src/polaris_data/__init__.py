"""Python SDK for the Polaris market data API."""

from .client import PolarisClient
from .errors import (
    DownloadNotAllowedError,
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)
from .models import LocalSnapshotEntry, SnapshotEntry

__all__ = [
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

__version__ = "0.5.1"
