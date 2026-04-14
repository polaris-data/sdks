"""Typed response structures returned by Polaris endpoints."""

from __future__ import annotations

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
