"""Error types raised by the Polaris SDK."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PolarisError(Exception):
    """Base SDK error for HTTP and decoding failures."""

    message: str
    status_code: int | None = None
    body: str | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.status_code is None:
            return self.message
        return f"{self.message} (status={self.status_code})"


class UnauthorizedError(PolarisError):
    """Raised for missing/invalid API credentials."""


class NotFoundError(PolarisError):
    """Raised when a requested resource does not exist."""


@dataclass
class RateLimitedError(PolarisError):
    """Raised when API quota has been exceeded."""

    reset_at: str | None = None


class StreamDecodeError(PolarisError):
    """Raised when NDJSON stream decoding fails."""


class DownloadNotAllowedError(PolarisError):
    """Raised when file downloads are disabled by client configuration."""
