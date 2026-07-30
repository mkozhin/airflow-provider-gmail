"""Tests for the independent helpers in :mod:`airflow_provider_gmail.dates`.

Covers :func:`from_local_iso` (the inverse of :func:`to_local_iso`: parses a
manifest ``internal_date`` string back into an aware :class:`datetime` — a
naive/offset-less or malformed string must raise :class:`ValueError` right at
the parse point, never a lexicographic compare or an opaque ``TypeError``
deeper down), and :func:`resolve_lookback_days`/:func:`resolve_overwrite` (cast
a templated or plain-literal ``lookback_days``/``overwrite`` value at runtime,
with a strict fallback — no silent default on an empty/``None``/garbage render).
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_provider_gmail import dates
from airflow_provider_gmail.dates import (
    from_local_iso,
    resolve_lookback_days,
    resolve_overwrite,
    to_local_iso,
)

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
        # A real offset PLUS a trailing Z is garbage: strip-one-Z + append
        # "+00:00" yields a double offset that must not be masked into a wrong
        # aware datetime (guards against a future rstrip("Z")/replace refactor).
        "2026-07-10T09:14:22+03:00Z",
        "2026-07-10T09:14:22+00:00Z",
    ],
)
def test_malformed_zulu_string_raises_valueerror(value):
    with pytest.raises(ValueError, match="ISO 8601"):
        from_local_iso(value)


def test_bare_zulu_error_reports_original_input():
    # The message must quote what the caller passed ("Z"), not the internal
    # Z->+00:00 normalized form.
    with pytest.raises(ValueError, match=r"got 'Z'"):
        from_local_iso("Z")


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


# -- non-str input -----------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        None,
        123,
        b"2026-07-10T09:14:22Z",
    ],
)
def test_non_str_input_raises_valueerror(value):
    # A non-str input must surface as the normalized ValueError. This is
    # guaranteed by the explicit ``isinstance(value, str)`` guard at the top of
    # ``from_local_iso`` (raised before any parsing), independently of the now
    # narrow ``except ValueError`` around the parse.
    with pytest.raises(ValueError, match="ISO 8601"):
        from_local_iso(value)


# -- internal errors are not masked as ValueError ----------------------------


@pytest.mark.parametrize("exc", [AttributeError, TypeError])
def test_internal_error_propagates_not_masked(monkeypatch, exc):
    # After narrowing the parse ``except`` to ``ValueError`` only, an unexpected
    # error from inside the try (e.g. a future typo like
    # ``datetime.fromisoformatt`` raising AttributeError, or a TypeError) must
    # PROPAGATE, not be masked as a ValueError "bad input". Rebind the
    # module-level ``datetime`` name (its stdlib class is an immutable C type, so
    # ``setattr`` on ``dates.datetime`` would fail). The value is a VALID aware
    # string so the isinstance guard and ``.endswith("Z")`` do not short-circuit
    # and execution reaches the faked ``datetime.fromisoformat``.
    class _Fake:
        @staticmethod
        def fromisoformat(value):
            raise exc("boom")

    monkeypatch.setattr(dates, "datetime", _Fake)
    with pytest.raises(exc):
        from_local_iso("2026-07-10T09:14:22+00:00")


# -- resolve_lookback_days ----------------------------------------------------


def test_resolve_lookback_days_string_digit_parses():
    assert resolve_lookback_days("14") == 14


def test_resolve_lookback_days_native_int_passes_through():
    assert resolve_lookback_days(14) == 14


def test_resolve_lookback_days_zero_is_valid():
    assert resolve_lookback_days(0) == 0
    assert resolve_lookback_days("0") == 0


def test_resolve_lookback_days_negative_string_raises():
    with pytest.raises(ValueError, match="lookback_days must be >= 0"):
        resolve_lookback_days("-1")


def test_resolve_lookback_days_negative_int_raises():
    with pytest.raises(ValueError, match="lookback_days must be >= 0"):
        resolve_lookback_days(-1)


@pytest.mark.parametrize("value", [True, False])
def test_resolve_lookback_days_native_bool_raises(value):
    # int(True) == 1 would otherwise silently turn a native-Jinja-rendered
    # `true` into a 1-day window instead of rejecting it.
    with pytest.raises(ValueError, match="must be an integer"):
        resolve_lookback_days(value)


@pytest.mark.parametrize("value", [1.9, 3.0])
def test_resolve_lookback_days_float_raises_even_when_whole(value):
    # int(1.9) == 1 would otherwise silently narrow the window; even a "whole"
    # float like 3.0 is rejected — no exception for round values.
    with pytest.raises(ValueError, match="must be an integer"):
        resolve_lookback_days(value)


@pytest.mark.parametrize("value", ["", "None", "abc", None])
def test_resolve_lookback_days_garbage_render_raises_mentioning_jinja(value):
    with pytest.raises(ValueError, match="Jinja"):
        resolve_lookback_days(value)


# -- resolve_overwrite ---------------------------------------------------------


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "1"])
def test_resolve_overwrite_true_strings(value):
    assert resolve_overwrite(value) is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE", "0"])
def test_resolve_overwrite_false_strings(value):
    assert resolve_overwrite(value) is False


@pytest.mark.parametrize("value", [True, False])
def test_resolve_overwrite_native_bool_passes_through(value):
    assert resolve_overwrite(value) is value


def test_resolve_overwrite_native_int_one_is_true():
    assert resolve_overwrite(1) is True


def test_resolve_overwrite_native_int_zero_is_false():
    assert resolve_overwrite(0) is False


def test_resolve_overwrite_native_int_other_raises():
    with pytest.raises(ValueError, match="overwrite must render to true/false"):
        resolve_overwrite(2)


@pytest.mark.parametrize("value", ["", "None", None, "yes", "garbage"])
def test_resolve_overwrite_garbage_render_raises(value):
    with pytest.raises(ValueError, match="overwrite must render to true/false"):
        resolve_overwrite(value)
