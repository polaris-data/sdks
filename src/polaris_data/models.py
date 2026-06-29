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


@dataclass(frozen=True)
class SnapshotEntry:
    """Remote standardized snapshot metadata."""

    key: str
    source: str | None = None
    market: str | None = None
    date: str | None = None
    hour: int | None = None


@dataclass(frozen=True)
class LocalSnapshotEntry:
    """Local standardized snapshot metadata."""

    key: str
    path: str
    source: str | None
    market: str | None
    date: str | None
    start: datetime | None
    end: datetime | None
    hour: int | None = None
