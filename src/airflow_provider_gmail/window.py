"""``Window`` — the pure value object of the Gmail search time window.

It owns *all* the epoch/timezone boundary math for the search window so the
query builder never has to reason about timezones or the day boundary:

- The calendar day is chosen in the **operator timezone** (ADR-0002), never in
  the DAG schedule zone. ``ref_day`` (a tz-aware ``datetime`` from the
  ``DagRun``, e.g. ``data_interval_end``) is converted with
  ``ref_day.astimezone(ZoneInfo(timezone)).date()`` and only then shifted back
  ``lookback_days`` calendar days. Because ``ref_day`` is an *argument* (never
  ``date.today()``), the window is stable between retries of the same task.
- The explicit ``date_from`` / ``date_to`` range (ADR-0004, manual backfill)
  wins over ``lookback_days``: ``after:`` = midnight of ``date_from`` and
  ``before:`` = midnight of ``(date_to + 1 day)`` so the ``date_to`` day is
  included whole (Gmail ``before:`` is strictly-less-than).

``after_epoch()`` may be ``None``: an explicit range with only ``date_to`` has
no lower bound, so ``after:`` must not be emitted at all (``lookback_days`` is
ignored in range mode and must not leak in). In lookback mode ``after_epoch()``
is always an ``int``. Both bounds, when present, are epoch **seconds** — the
query builder appends numeric ``after:``/``before:`` rather than
``after:YYYY/MM/DD`` (a textual date would be read in Gmail's own timezone).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def _midnight_epoch(day: date, tz: ZoneInfo) -> int:
    """Epoch seconds of midnight (00:00) of ``day`` in ``tz``."""
    return int(datetime(day.year, day.month, day.day, tzinfo=tz).timestamp())


@dataclass(frozen=True)
class Window:
    """Resolved search window: lower/upper bounds as epoch seconds (or ``None``).

    Construct via :meth:`resolve`; the two accessors are the API used by
    ``build_query``.
    """

    after: int | None
    before: int | None

    def after_epoch(self) -> int | None:
        """Lower bound (inclusive) as epoch seconds, or ``None`` if unbounded."""
        return self.after

    def before_epoch(self) -> int | None:
        """Upper bound (exclusive) as epoch seconds, or ``None`` if unbounded."""
        return self.before

    @classmethod
    def resolve(
        cls,
        ref_day: datetime,
        timezone: str,
        lookback_days: int,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> Window:
        """Compute the window from either an explicit range or ``lookback_days``.

        If *either* ``date_from`` or ``date_to`` is given, the explicit-range
        mode wins and ``lookback_days`` is ignored entirely (ADR-0004). The
        WARNING about a non-default ``lookback_days`` being ignored belongs to
        the caller resolving the window, not here (this object no longer knows
        the original ``lookback_days``).

        Otherwise the lookback mode applies: the calendar day is taken in
        ``timezone`` (ADR-0002) and shifted back ``lookback_days`` days.
        """
        tz = ZoneInfo(timezone)

        if date_from is not None or date_to is not None:
            after = _midnight_epoch(date_from, tz) if date_from is not None else None
            before = (
                _midnight_epoch(date_to + timedelta(days=1), tz)
                if date_to is not None
                else None
            )
            return cls(after=after, before=before)

        window_day = ref_day.astimezone(tz).date() - timedelta(days=lookback_days)
        return cls(after=_midnight_epoch(window_day, tz), before=None)
