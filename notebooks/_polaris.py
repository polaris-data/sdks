"""Shared helpers for the Polaris example notebooks.

These functions were factored out of the per-notebook preambles so each notebook
can focus on its analysis rather than SDK iterator and catalog bookkeeping.
"""

from __future__ import annotations

from itertools import islice

import pandas as pd


def as_utc(value):
    timestamp = pd.Timestamp(value)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def accessible_bounds(market_info):
    """Return the no-key catalog interval for an open or preview market."""
    start = as_utc(market_info["start"])
    end = as_utc(market_info["end"])
    access = market_info.get("access") or {}
    cutoff = access.get("public_cutoff_date")
    if access.get("status") == "preview" and cutoff:
        public_day = as_utc(cutoff)
        start = max(start, public_day)
        end = min(end, public_day + pd.Timedelta(days=1))
    if start >= end:
        raise ValueError("Catalog metadata does not expose a no-key interval for this market")
    return start, end


def bounded_rows(iterator, limit):
    """Materialize at most limit rows and close a partially consumed SDK generator."""
    rows = list(islice(iterator, limit + 1))
    truncated = len(rows) > limit
    close = getattr(iterator, "close", None)
    if close is not None:
        close()
    return rows[:limit], truncated


def event_timestamp(row):
    """Support both the legacy and v2 Polaris event envelopes."""
    value = row.get("collector_timestamp", row.get("timestamp"))
    return pd.to_datetime(value, unit="ms", utc=True)
