"""Python SDK for the Polaris market data API."""

from .client import PolarisClient
from .errors import (
    NotFoundError,
    PolarisError,
    RateLimitedError,
    StreamDecodeError,
    UnauthorizedError,
)

__all__ = [
    "NotFoundError",
    "PolarisClient",
    "PolarisError",
    "RateLimitedError",
    "StreamDecodeError",
    "UnauthorizedError",
]

__version__ = "0.1.0"
