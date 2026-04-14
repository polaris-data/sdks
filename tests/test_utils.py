from __future__ import annotations

from datetime import date, datetime, timezone

from polaris_data.utils import chunk_timerange, to_iso8601


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


def test_chunk_timerange_splits_into_24_hour_chunks() -> None:
    chunks = chunk_timerange(
        "2024-01-01T00:00:00Z",
        "2024-01-04T00:00:00Z",
        chunk_hours=24,
    )

    assert len(chunks) == 3
    assert chunks[0][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert chunks[0][1] == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert chunks[1][0] == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert chunks[1][1] == datetime(2024, 1, 3, tzinfo=timezone.utc)
    assert chunks[2][0] == datetime(2024, 1, 3, tzinfo=timezone.utc)
    assert chunks[2][1] == datetime(2024, 1, 4, tzinfo=timezone.utc)


def test_chunk_timerange_handles_partial_chunks() -> None:
    chunks = chunk_timerange(
        "2024-01-01T00:00:00Z",
        "2024-01-02T12:00:00Z",
        chunk_hours=24,
    )

    assert len(chunks) == 2
    assert chunks[0][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert chunks[0][1] == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert chunks[1][0] == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert chunks[1][1] == datetime(2024, 1, 2, 12, tzinfo=timezone.utc)


def test_chunk_timerange_custom_chunk_size() -> None:
    chunks = chunk_timerange(
        "2024-01-01T00:00:00Z",
        "2024-01-01T12:00:00Z",
        chunk_hours=6,
    )

    assert len(chunks) == 2
    assert chunks[0][1] == datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
    assert chunks[1][1] == datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
