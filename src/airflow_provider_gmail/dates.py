"""Pure date/time and templated-value helpers shared by the operators and the sensors.

This module is deliberately *neutral*: no Airflow, no S3, no Gmail imports — only
``datetime``/``zoneinfo`` and string parsing. It lives here (rather than inside
``operators/gmail.py``) so the sensors can reuse :func:`parse_date_range` and
:func:`to_local_date` without importing the operator module, and so both the
operators and the sensors can reuse :func:`resolve_lookback_days` (and the
operators reuse :func:`resolve_overwrite`) — the sensor→operator coupling that
would otherwise exist purely to share these pure functions.

- :func:`parse_date_range` — parse/validate the ``date_from``/``date_to`` range;
- :func:`to_local_date` / :func:`to_local_iso` — the ``dt=`` partition date and
  the manifest ``internal_date``, both over one epoch→aware-datetime conversion
  so path and manifest can never disagree.
- :func:`from_local_iso` — the inverse of :func:`to_local_iso`: parse a manifest
  ``internal_date`` string back into an aware :class:`datetime` for chronological
  comparison. Owned here because this module owns the ``internal_date`` format.
- :func:`resolve_lookback_days` / :func:`resolve_overwrite` — cast a templated
  ``lookback_days``/``overwrite`` value (rendered from ``dag_run.conf``/Jinja,
  or passed as a plain Python literal) to its runtime type, applying a strict
  fallback policy rather than a silent default.
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


def resolve_lookback_days(value: int | str | None) -> int:
    """Cast a templated (or plain) ``lookback_days`` value to a validated ``int``.

    Called at runtime (``execute()``/``poke()``), *after* Jinja rendering, so it
    handles both a plain Python ``int`` literal and a rendered template string
    the same way — unlike :func:`validate_lookback_days`, which only checked an
    already-``int`` value at ``__init__`` time.

    The fallback is strict: there is no silent default. A rendered empty
    string, ``"None"`` (the classic ``{{ dag_run.conf.get('x') }}`` trap when
    the key is missing), or a native ``None`` (the same trap under
    ``render_template_as_native_obj=True``) all raise :class:`ValueError`
    rather than falling back to the class default — the DAG author is
    responsible for a default *inside* the Jinja expression itself (e.g.
    ``{{ dag_run.conf.get('lookback_days', 7) }}``).
    """
    if isinstance(value, bool):
        # int(True) == 1 would otherwise silently turn a native-Jinja-rendered
        # `true` (render_template_as_native_obj=True) into a 1-day window.
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(check the DAG's Jinja expression renders a number, not a boolean)"
        )
    if isinstance(value, float):
        # int(1.9) == 1 would otherwise silently narrow the window instead of
        # rejecting a fractional day count; native Jinja rendering
        # (render_template_as_native_obj=True) can hand back a float from an
        # arithmetic expression in dag_run.conf.
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(a fractional value is not a valid number of days)"
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"lookback_days must be an integer, got {value!r} "
            "(check the DAG's Jinja expression renders a number)"
        ) from exc
    if parsed < 0:
        raise ValueError(f"lookback_days must be >= 0 (0 means 'today only'), got {parsed}")
    return parsed


def resolve_overwrite(value: bool | str | int | None) -> bool:
    """Cast a templated (or plain) ``overwrite`` value to a validated ``bool``.

    Called at runtime (``execute()``), *after* Jinja rendering. ``overwrite``
    was previously never validated at all when passed as a Python literal —
    this is a deliberate tightening of behavior, not a side effect of adding
    templating (see ADR-0009).

    The fallback is strict, matching :func:`resolve_lookback_days`: a rendered
    empty string, ``"None"``, or a native ``None`` all raise
    :class:`ValueError` rather than silently resolving to ``False``.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1"):
            return True
        if low in ("false", "0"):
            return False
    elif isinstance(value, int):
        # Covers native Jinja rendering (render_template_as_native_obj=True),
        # where a conf value of 0/1 arrives as a real int, not "0"/"1".
        if value in (0, 1):
            return bool(value)
    raise ValueError(
        f"overwrite must render to true/false, got {value!r} "
        "(check the DAG's Jinja expression)"
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


def from_local_iso(value: str) -> datetime:
    """Parse a manifest ``internal_date`` string into an aware :class:`datetime`.

    The inverse of :func:`to_local_iso`: this module owns the ``internal_date``
    format, so it also owns parsing it back. The resolver compares manifests by
    *moment in time* (both ``pick="all"`` and ``pick="latest"``) and must never
    compare the ISO strings lexicographically — two equal instants written with
    different UTC offsets sort wrongly as text.

    A string **without** a UTC offset (a *naive* timestamp) raises
    :class:`ValueError` right here, at the parse point, rather than slipping
    through :meth:`datetime.fromisoformat` and blowing up much later with an
    opaque ``TypeError`` when an aware and a naive datetime are compared. A
    malformed string likewise raises :class:`ValueError`.

    This is deliberately *not* wrapped in ``ManifestError`` — that exception is
    strictly about the manifest *schema* (:meth:`Manifest.from_json`), whereas a
    missing offset is a value problem surfaced by either ``pick`` mode.

    A trailing uppercase ``Z`` (ISO 8601 Zulu = UTC) is normalized to ``+00:00``
    before parsing: the project's floor is ``>=3.10``, but
    :meth:`datetime.fromisoformat` only learned to accept ``Z`` in 3.11. Our own
    :func:`to_local_iso` always writes a numeric offset (never ``Z``), yet a
    foreign or hand-written manifest may carry ``Z`` — normalizing it lets the
    resolver read both spellings of UTC on every supported Python and removes the
    misleading "not a valid ISO 8601 timestamp" rejection that Python 3.10 raises
    for what is in fact a fully aware UTC moment (3.10's
    :meth:`datetime.fromisoformat` cannot parse ``Z`` at all). Only uppercase
    ``Z`` is normalized; a lowercase ``z`` is not valid ISO 8601 and is left to
    raise :class:`ValueError`.

    Error messages report the caller's original input, not the ``Z``→``+00:00``
    normalized form.
    """
    original = value
    msg = f"internal_date must be an ISO 8601 timestamp with a UTC offset, got {value!r}"
    if not isinstance(value, str):
        raise ValueError(msg)
    try:
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(msg) from exc
    if parsed.tzinfo is None:
        raise ValueError(
            f"internal_date must be timezone-aware (carry a UTC offset), got "
            f"naive {original!r}"
        )
    return parsed
