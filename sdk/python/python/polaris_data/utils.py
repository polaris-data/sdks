"""Utility helpers for converting SDK inputs to API query values."""

from __future__ import annotations

import importlib
from datetime import date, datetime, timezone
from itertools import islice
from typing import TYPE_CHECKING, Any, Iterable, Mapping, TypeVar, Union

if TYPE_CHECKING:
    import pandas

TimeInput = Union[str, int, float, datetime, date]
T = TypeVar("T")


def _require_pandas() -> Any:
    try:
        return importlib.import_module("pandas")
    except ImportError:
        raise ImportError(
            "pandas is required for this utility; install polaris-data[dataframe]"
        ) from None


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


def as_utc(value: Any) -> pandas.Timestamp:
    """Convert a Pandas-compatible value to a timezone-aware UTC timestamp."""
    pandas_module = _require_pandas()
    timestamp = pandas_module.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def accessible_bounds(
    market_info: Mapping[str, Any],
) -> tuple[pandas.Timestamp, pandas.Timestamp]:
    """Return the no-key catalog interval for an open or preview market."""
    start = as_utc(market_info["start"])
    end = as_utc(market_info["end"])
    access = market_info.get("access") or {}
    cutoff = access.get("public_cutoff_date")
    if access.get("status") == "preview" and cutoff:
        pandas_module = _require_pandas()
        public_day = as_utc(cutoff)
        start = max(start, public_day)
        end = min(end, public_day + pandas_module.Timedelta(days=1))
    if start >= end:
        raise ValueError(
            "Catalog metadata does not expose a no-key interval for this market"
        )
    return start, end


def bounded_rows(iterator: Iterable[T], limit: int) -> tuple[list[T], bool]:
    """Materialize at most limit rows and close a partially consumed iterator."""
    if not isinstance(limit, int) or isinstance(limit, bool):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    try:
        rows = list(islice(iterator, limit + 1))
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    truncated = len(rows) > limit
    return rows[:limit], truncated


def event_timestamp(row: Mapping[str, Any]) -> pandas.Timestamp:
    """Return the UTC timestamp from a legacy or v2 Polaris event envelope."""
    pandas_module = _require_pandas()
    value = row.get("collector_timestamp", row.get("timestamp"))
    return pandas_module.to_datetime(value, unit="ms", utc=True)
