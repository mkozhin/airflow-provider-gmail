"""Pure date/time helpers shared by the operators and the sensors.

This module is deliberately *neutral*: no Airflow, no S3, no Gmail imports — only
``datetime``/``zoneinfo`` and string parsing. It lives here (rather than inside
``operators/gmail.py``) so the sensors can reuse :func:`parse_date_range` and
:func:`to_local_date` without importing the operator module — the sensor→operator
coupling that would otherwise exist purely to share two pure functions.

- :func:`parse_date_range` — parse/validate the ``date_from``/``date_to`` range;
- :func:`to_local_date` / :func:`to_local_iso` — the ``dt=`` partition date and
  the manifest ``internal_date``, both over one epoch→aware-datetime conversion
  so path and manifest can never disagree.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo


def parse_date_range(
    date_from: str | None, date_to: str | None
) -> tuple[date | None, date | None]:
    """Parse and validate an explicit ``date_from``/``date_to`` range.

    Both bounds are optional ISO ``YYYY-MM-DD`` strings (typically rendered from
    ``dag_run.conf``). Returns a ``(date | None, date | None)`` pair. Raises
    :class:`ValueError` on a malformed ISO string or a reversed range
    (``date_from > date_to``).

    Shared by ``execute()`` (Task 8) and the sensor ``poke()`` (Task 11) so the
    parse never diverges between operator and sensor.
    """
    parsed_from = _parse_iso_date(date_from, "date_from")
    parsed_to = _parse_iso_date(date_to, "date_to")
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        raise ValueError(
            f"date_from ({parsed_from.isoformat()}) must be on or before "
            f"date_to ({parsed_to.isoformat()})"
        )
    return parsed_from, parsed_to


def _parse_iso_date(value: str | None, field: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"{field} must be an ISO date (YYYY-MM-DD), got {value!r}"
        ) from exc


def validate_lookback_days(lookback_days: int) -> None:
    """Reject a negative ``lookback_days`` at construction time (DAG parse).

    ``0`` means "today only"; a negative value would build an ``after:`` boundary
    in the *future*. Shared by the operator base and the sensor ``__init__`` so
    invalid config fails at DAG parse rather than on the first run/poke.
    """
    if lookback_days < 0:
        raise ValueError(
            f"lookback_days must be >= 0 (0 means 'today only'), got {lookback_days}"
        )


def validate_timezone(timezone: str) -> None:
    """Reject an unknown IANA ``timezone`` at construction time (DAG parse).

    Resolves :class:`~zoneinfo.ZoneInfo` eagerly so a typo fails at DAG parse,
    not deep inside a run/poke. The string itself is kept by the caller — the
    date helpers build the :class:`ZoneInfo` where they need it.
    """
    try:
        ZoneInfo(timezone)
    except Exception as exc:
        raise ValueError(f"unknown timezone {timezone!r}: {exc}") from exc


def _to_aware_datetime(internal_date: int, timezone: str) -> datetime:
    """Convert a Gmail ``internalDate`` to an aware datetime in ``timezone``.

    ``internal_date`` is epoch **milliseconds** as an ``int`` — the hook
    converts Gmail's ``internalDate`` string to ``int`` once, so by the time it
    reaches here it is already numeric. Both :func:`to_local_date` and
    :func:`to_local_iso` go through this single conversion so the ``dt=``
    partition date and the manifest ISO can never disagree. The zone always
    comes from the ``timezone`` argument — no hard-coded MSK.
    """
    return datetime.fromtimestamp(internal_date / 1000, tz=ZoneInfo(timezone))


def to_local_date(internal_date: int, timezone: str) -> date:
    """The calendar date of ``internal_date`` in ``timezone`` (the ``dt=`` partition)."""
    return _to_aware_datetime(internal_date, timezone).date()


def to_local_iso(internal_date: int, timezone: str) -> str:
    """ISO 8601 string of ``internal_date`` in ``timezone`` (manifest ``internal_date``)."""
    return _to_aware_datetime(internal_date, timezone).isoformat()
