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
