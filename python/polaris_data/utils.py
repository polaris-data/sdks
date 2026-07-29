"""Utility helpers for converting SDK inputs to API query values."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Union

TimeInput = Union[str, int, float, datetime, date]


def to_iso8601(value: TimeInput) -> str:
    """Convert common time input types to API-compatible ISO 8601 strings."""
    if isinstance(value, str):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, (int, float)):
        as_dt = datetime.fromtimestamp(float(value) / 1_000_000.0, tz=timezone.utc)
        return as_dt.isoformat().replace("+00:00", "Z")

    raise TypeError(f"Unsupported time input type: {type(value)!r}")


def bool_to_query(value: bool) -> str:
    """Serialize booleans to lowercase query-string values."""
    return "true" if value else "false"


def to_datetime(value: TimeInput) -> datetime:
    """Convert TimeInput to datetime object in UTC."""
    if isinstance(value, str):
        # Parse ISO 8601 string
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1_000_000.0, tz=timezone.utc)

    raise TypeError(f"Unsupported time input type: {type(value)!r}")


def chunk_timerange(
    from_: TimeInput,
    to: TimeInput,
    chunk_hours: int = 24,
) -> list[tuple[datetime, datetime]]:
    """
    Split a time range into chunks of specified duration.

    Args:
        from_: Start time
        to: End time
        chunk_hours: Hours per chunk (default: 24 = 1 day)

    Returns:
        List of (start, end) datetime tuples for each chunk
    """
    start = to_datetime(from_)
    end = to_datetime(to)

    if start >= end:
        raise ValueError("from_ must be before to")

    chunks = []
    current = start
    delta = timedelta(hours=chunk_hours)

    while current < end:
        chunk_end = min(current + delta, end)
        chunks.append((current, chunk_end))
        current = chunk_end

    return chunks
