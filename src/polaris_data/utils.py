"""Utility helpers for converting SDK inputs to API query values."""

from __future__ import annotations

from datetime import date, datetime, timezone
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
