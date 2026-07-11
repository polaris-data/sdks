"""Typed response structures returned by Polaris endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

JSONDict = dict[str, Any]
CatalogInstrumentValue = str | int | float | None


class CatalogInstrument(TypedDict):
    base: str | None
    quote: str | None
    tick_size: CatalogInstrumentValue
    lot_size: CatalogInstrumentValue
    min_notional: CatalogInstrumentValue


class CatalogAccess(TypedDict):
    status: str
    public_cutoff_date: str | None


class CatalogMarketEntry(TypedDict):
    source: str
    market: str
    start: str
    end: str
    source_type: str
    categories: list[str]
    access: CatalogAccess
    instrument: CatalogInstrument


class CatalogResponse(TypedDict):
    markets: list[CatalogMarketEntry]
    updatedAt: str


class PaginatedResponse(TypedDict):
    data: list[JSONDict]
    next_cursor: str | None
    has_more: bool


class DownloadUrlResponse(TypedDict):
    url: str
    totalBytes: int
    fileCount: int


class BulkDownloadSnapshotEntry(TypedDict):
    date: str
    timestamp: str
    key: str
    url: str
    expires_in_seconds: int


class BulkDownloadManifest(TypedDict):
    source: str
    market: str
    date: str
    total: int
    total_bytes: int
    snapshots: list[BulkDownloadSnapshotEntry]


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
