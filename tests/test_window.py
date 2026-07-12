"""Tests for :class:`Window` — the pure search-window value object.

All the subtlety of the day boundary lives here (ADR-0002 + ADR-0004), so these
tests are pure: no hook, no Gmail, no network. ``ref_day`` is always supplied as
an argument, so the window never depends on ``date.today()``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from airflow_provider_gmail.window import Window

MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")


def _midnight(y: int, m: int, d: int, tz: ZoneInfo) -> int:
    return int(datetime(y, m, d, tzinfo=tz).timestamp())


def test_after_epoch_is_moscow_midnight_not_utc():
    # 2026-07-11 10:00 UTC == 13:00 MSK, so the day is 2026-07-11 in both zones.
    ref_day = datetime(2026, 7, 11, 10, 0, tzinfo=UTC)
    w = Window.resolve(ref_day, "Europe/Moscow", lookback_days=0)

    assert w.after_epoch() == _midnight(2026, 7, 11, MSK)
    # Moscow midnight is three hours *before* UTC midnight of the same date.
    assert w.after_epoch() != _midnight(2026, 7, 11, UTC)
    assert w.before_epoch() is None


def test_day_selection_in_operator_timezone_adr0002():
    # 2026-07-11 22:00 UTC == 2026-07-12 01:00 MSK. The window day must be the
    # Moscow day (12th), not the naive UTC .date() (11th).
    ref_day = datetime(2026, 7, 11, 22, 0, tzinfo=UTC)
    w = Window.resolve(ref_day, "Europe/Moscow", lookback_days=0)

    assert w.after_epoch() == _midnight(2026, 7, 12, MSK)
    assert w.after_epoch() != _midnight(2026, 7, 11, MSK)


def test_lookback_zero_is_ref_day_midnight():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=MSK)
    w = Window.resolve(ref_day, "Europe/Moscow", lookback_days=0)
    assert w.after_epoch() == _midnight(2026, 7, 11, MSK)


def test_lookback_one_is_previous_day():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=MSK)
    w = Window.resolve(ref_day, "Europe/Moscow", lookback_days=1)
    assert w.after_epoch() == _midnight(2026, 7, 10, MSK)


def test_lookback_shifts_n_days():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=MSK)
    w = Window.resolve(ref_day, "Europe/Moscow", lookback_days=7)
    assert w.after_epoch() == _midnight(2026, 7, 4, MSK)


def test_date_from_only_has_after_no_before():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    w = Window.resolve(
        ref_day, "Europe/Moscow", lookback_days=7, date_from=date(2026, 6, 1)
    )
    assert w.after_epoch() == _midnight(2026, 6, 1, MSK)
    assert w.before_epoch() is None


def test_date_to_only_has_no_after_before_includes_whole_day():
    # ADR-0004: only date_to => no lower bound (after None); before is midnight
    # of the *next* day so date_to is included whole (Gmail before: is <).
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    w = Window.resolve(
        ref_day, "Europe/Moscow", lookback_days=7, date_to=date(2026, 6, 30)
    )
    assert w.after_epoch() is None
    assert w.before_epoch() == _midnight(2026, 7, 1, MSK)


def test_explicit_range_both_bounds():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    w = Window.resolve(
        ref_day,
        "Europe/Moscow",
        lookback_days=7,
        date_from=date(2026, 6, 1),
        date_to=date(2026, 6, 30),
    )
    assert w.after_epoch() == _midnight(2026, 6, 1, MSK)
    assert w.before_epoch() == _midnight(2026, 7, 1, MSK)


def test_explicit_range_ignores_lookback():
    # In range mode lookback_days must not leak into the bounds at all.
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    w = Window.resolve(
        ref_day, "Europe/Moscow", lookback_days=999, date_from=date(2026, 6, 1)
    )
    assert w.after_epoch() == _midnight(2026, 6, 1, MSK)


def test_window_stable_across_different_now():
    # No date.today() dependency: same ref_day => same window, regardless of
    # when resolve() is called ("different today").
    ref_day = datetime(2026, 7, 11, 23, 59, tzinfo=UTC)
    first = Window.resolve(ref_day, "Europe/Moscow", lookback_days=0)
    second = Window.resolve(ref_day, "Europe/Moscow", lookback_days=0)
    assert first.after_epoch() == second.after_epoch()


def test_non_default_timezone_shifts_midnight():
    ref_day = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    w = Window.resolve(ref_day, "UTC", lookback_days=0)
    assert w.after_epoch() == _midnight(2026, 7, 11, UTC)
