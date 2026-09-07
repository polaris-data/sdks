from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from polaris_data.utils import (
    accessible_bounds,
    as_utc,
    bounded_rows,
    event_timestamp,
    to_iso8601,
)


def test_to_iso8601_keeps_string() -> None:
    assert to_iso8601("2024-01-01T00:00:00Z") == "2024-01-01T00:00:00Z"


def test_to_iso8601_handles_datetime() -> None:
    dt = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert to_iso8601(dt) == "2024-01-01T00:00:00Z"


def test_to_iso8601_handles_naive_datetime_as_utc() -> None:
    dt = datetime(2024, 1, 1, 0, 0)
    assert to_iso8601(dt) == "2024-01-01T00:00:00Z"


def test_to_iso8601_handles_date() -> None:
    assert to_iso8601(date(2024, 1, 1)) == "2024-01-01T00:00:00Z"


def test_to_iso8601_handles_microseconds_epoch() -> None:
    assert to_iso8601(1_704_067_200_000_000) == "2024-01-01T00:00:00Z"


def test_as_utc_localizes_naive_and_converts_aware_values() -> None:
    assert as_utc("2024-01-01T00:00:00") == pd.Timestamp(
        "2024-01-01T00:00:00Z"
    )
    assert as_utc("2024-01-01T01:00:00+01:00") == pd.Timestamp(
        "2024-01-01T00:00:00Z"
    )


def test_accessible_bounds_returns_open_market_interval() -> None:
    start, end = accessible_bounds(
        {
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-03T00:00:00Z",
            "access": {"status": "open"},
        }
    )

    assert start == pd.Timestamp("2024-01-01T00:00:00Z")
    assert end == pd.Timestamp("2024-01-03T00:00:00Z")


def test_accessible_bounds_clips_preview_market_to_public_day() -> None:
    start, end = accessible_bounds(
        {
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-05T00:00:00Z",
            "access": {
                "status": "preview",
                "public_cutoff_date": "2024-01-03",
            },
        }
    )

    assert start == pd.Timestamp("2024-01-03T00:00:00Z")
    assert end == pd.Timestamp("2024-01-04T00:00:00Z")


def test_accessible_bounds_rejects_empty_public_interval() -> None:
    with pytest.raises(ValueError, match="does not expose a no-key interval"):
        accessible_bounds(
            {
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-02T00:00:00Z",
                "access": {
                    "status": "preview",
                    "public_cutoff_date": "2024-01-03",
                },
            }
        )


def test_bounded_rows_reports_truncation_and_closes_iterator() -> None:
    closed = False

    def rows():
        nonlocal closed
        try:
            yield from range(4)
        finally:
            closed = True

    result, truncated = bounded_rows(rows(), 2)

    assert result == [0, 1]
    assert truncated is True
    assert closed is True


def test_bounded_rows_closes_iterator_when_iteration_fails() -> None:
    closed = False

    def rows():
        nonlocal closed
        try:
            yield 1
            raise RuntimeError("broken iterator")
        finally:
            closed = True

    with pytest.raises(RuntimeError, match="broken iterator"):
        bounded_rows(rows(), 2)

    assert closed is True


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            {"timestamp": 1_704_067_200_000},
            pd.Timestamp("2024-01-01T00:00:00Z"),
        ),
        (
            {
                "timestamp": 0,
                "collector_timestamp": 1_704_067_200_123,
            },
            pd.Timestamp("2024-01-01T00:00:00.123Z"),
        ),
    ],
)
def test_event_timestamp_supports_legacy_and_v2_envelopes(row, expected) -> None:
    assert event_timestamp(row) == expected
