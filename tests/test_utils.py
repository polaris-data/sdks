from __future__ import annotations

from datetime import date, datetime, timezone

from polaris_data.utils import to_iso8601


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
