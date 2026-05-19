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

__all__ = [
    "NotFoundError",
    "DownloadNotAllowedError",
    "PolarisClient",
    "PolarisError",
    "RateLimitedError",
    "StreamDecodeError",
    "UnauthorizedError",
]

__version__ = "0.4.1"
