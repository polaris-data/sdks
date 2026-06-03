"""Typed response structures returned by Polaris endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

JSONDict = dict[str, Any]


class PaginatedResponse(TypedDict):
    data: list[JSONDict]
    next_cursor: str | None
    has_more: bool


class DownloadUrlResponse(TypedDict):
    url: str
    totalBytes: int
    fileCount: int


class OhlcvParquetResponse(TypedDict):
    url: str
    sizeBytes: int
    barCount: int


@dataclass(frozen=True)
class SnapshotEntry:
    """Remote standardized snapshot metadata."""

    key: str
    filename: str


@dataclass(frozen=True)
class LocalSnapshotEntry:
    """Local standardized snapshot metadata."""

    key: str
    path: str
    filename: str
    exchange: str | None
    asset: str | None
    date: str | None
    start: datetime | None
    end: datetime | None
