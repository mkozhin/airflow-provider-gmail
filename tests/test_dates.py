"""Tests for :func:`airflow_provider_gmail.dates.from_local_iso`.

The inverse of :func:`to_local_iso`: parses a manifest ``internal_date`` string
back into an aware :class:`datetime`. A naive (offset-less) or malformed string
must raise :class:`ValueError` right at the parse point — never a lexicographic
compare or an opaque ``TypeError`` deeper down.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_provider_gmail.dates import from_local_iso, to_local_iso

MSK = "Europe/Moscow"
UTC = "UTC"
LA = "America/Los_Angeles"


def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# -- round-trip --------------------------------------------------------------


@pytest.mark.parametrize("timezone", [MSK, UTC, LA])
def test_round_trip_preserves_moment(timezone):
    dt = datetime(2026, 7, 10, 9, 14, 22, tzinfo=ZoneInfo(timezone))
    ms = _epoch_ms(dt)
    parsed = from_local_iso(to_local_iso(ms, timezone))
    assert _epoch_ms(parsed) == ms
    assert parsed.utcoffset() is not None


def test_equal_instants_different_offsets_compare_equal():
    # Same moment written in two zones: lexicographically different, but the
    # parsed datetimes must be equal as instants.
    dt = datetime(2026, 7, 10, 9, 14, 22, tzinfo=ZoneInfo(MSK))
    ms = _epoch_ms(dt)
    a = from_local_iso(to_local_iso(ms, MSK))
    b = from_local_iso(to_local_iso(ms, UTC))
    assert to_local_iso(ms, MSK) != to_local_iso(ms, UTC)
    assert a == b


# -- Zulu (Z) suffix ---------------------------------------------------------


def test_zulu_suffix_parses_as_aware_utc():
    parsed = from_local_iso("2026-07-10T09:14:22Z")
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() is not None
    assert parsed.utcoffset().total_seconds() == 0


def test_zulu_suffix_equals_numeric_offset_spelling():
    assert from_local_iso("2026-07-10T09:14:22Z") == from_local_iso(
        "2026-07-10T09:14:22+00:00"
    )


def test_zulu_and_positive_offset_same_instant():
    # 09:00:00Z is the same moment as 12:00:00+03:00 (compared as instants,
    # not lexicographically).
    assert from_local_iso("2026-07-10T09:00:00Z") == from_local_iso(
        "2026-07-10T12:00:00+03:00"
    )


def test_zulu_suffix_with_microseconds_parses():
    parsed = from_local_iso("2026-07-10T09:14:22.123456Z")
    assert parsed.utcoffset().total_seconds() == 0
    assert parsed.microsecond == 123456


@pytest.mark.parametrize(
    "value",
    [
        "not-a-dateZ",  # malformed with a trailing Z is not masked
        "Z",  # bare Z
    ],
)
def test_malformed_zulu_string_raises_valueerror(value):
    with pytest.raises(ValueError, match="ISO 8601"):
        from_local_iso(value)


def test_lowercase_z_raises_valueerror():
    # Lowercase 'z' is not valid ISO 8601; the contract is uppercase-Z only and
    # must not be silently widened (e.g. a future rstrip("Z")/case-insensitive).
    with pytest.raises(ValueError, match="ISO 8601"):
        from_local_iso("2026-07-10T09:14:22z")


# -- naive input -------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "2026-07-10T09:14:22",
        "2026-07-10T09:14:22.123456",
        "2026-07-10 09:14:22",
    ],
)
def test_naive_string_raises_valueerror(value):
    with pytest.raises(ValueError, match="timezone-aware"):
        from_local_iso(value)


# -- malformed input ---------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "not-a-date",
        "2026-13-40T09:14:22+03:00",
        "",
        "2026-07-10T25:00:00+03:00",
    ],
)
def test_malformed_string_raises_valueerror(value):
    with pytest.raises(ValueError, match="ISO 8601"):
        from_local_iso(value)
