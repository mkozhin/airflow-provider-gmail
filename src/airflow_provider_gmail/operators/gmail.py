"""``GmailAttachmentsBaseOperator`` — the abstract base operator and its helpers.

This module carries the storage-agnostic interface shared by the S3 and local
operators (ADR-0006: the storage seam stays inheritance, no ``Destination``
port) plus the pure helpers that the ``execute()`` orchestration (Task 8) and
the sensor ``poke()`` (Task 11) both reuse so the two never drift:

- :func:`parse_date_range` — parse/validate the ``date_from``/``date_to`` range;
- :func:`resolve_collisions` — sanitize names and resolve intra-message name
  collisions with a suffix, tracking the occupied set;
- :func:`to_local_date` / :func:`to_local_iso` — the ``dt=`` partition date and
  the manifest ``internal_date``, both over one epoch→aware-datetime conversion
  so path and manifest can never disagree.

The base class declares the three storage-seam methods (:meth:`_write`,
:meth:`_destination_path`, :meth:`_read_manifest`) and the label-filter policy
(:meth:`_filter_processed_label`) as abstract: a subclass that forgets any of
them fails loudly rather than silently mis-behaving. ``BaseOperatorMeta`` is
incompatible with ``abc.ABCMeta``, so "abstract" is enforced by raising
``NotImplementedError`` rather than by :mod:`abc`.

Actual ``execute()`` orchestration (the full run loop) lives in Task 8; this
module deliberately stops at the interface, the pure helpers and the
manifest-assembly helper.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from zoneinfo import ZoneInfo

from airflow.models import BaseOperator

from airflow_provider_gmail.hooks.gmail import GmailHook
from airflow_provider_gmail.manifest import FileEntry, Manifest
from airflow_provider_gmail.utils.mime import Attachment, sanitize_filename


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


def _to_aware_datetime(internal_date: str, timezone: str) -> datetime:
    """Convert a Gmail ``internalDate`` to an aware datetime in ``timezone``.

    ``internalDate`` arrives as a **string** holding epoch **milliseconds**;
    both :func:`to_local_date` and :func:`to_local_iso` go through this single
    conversion so the ``dt=`` partition date and the manifest ISO can never
    disagree. The zone always comes from the ``timezone`` argument — no
    hard-coded MSK.
    """
    epoch_ms = int(internal_date)
    return datetime.fromtimestamp(epoch_ms / 1000, tz=ZoneInfo(timezone))


def to_local_date(internal_date: str, timezone: str) -> date:
    """The calendar date of ``internal_date`` in ``timezone`` (the ``dt=`` partition)."""
    return _to_aware_datetime(internal_date, timezone).date()


def to_local_iso(internal_date: str, timezone: str) -> str:
    """ISO 8601 string of ``internal_date`` in ``timezone`` (manifest ``internal_date``)."""
    return _to_aware_datetime(internal_date, timezone).isoformat()


def resolve_collisions(
    attachments: Iterable[Attachment],
) -> list[tuple[Attachment, str]]:
    """Sanitize names and resolve intra-message collisions with a suffix.

    A single message may carry two attachments with the same name; a naive
    ``<message_id>/<filename>`` path would let the second silently overwrite the
    first. Each name is sanitized via :func:`sanitize_filename` and, on a clash,
    a ``_<n>`` suffix is inserted before the extension, incrementing to the first
    free name while tracking the set of already-taken names — so ``report.xlsx``,
    ``report.xlsx``, ``report_1.xlsx`` yield three distinct names rather than the
    second colliding with the real third.

    Returns ``(attachment, safe_name)`` pairs. The :class:`Attachment` is **not**
    mutated (it has no ``safe_name`` field — see Task 2).
    """
    occupied: set[str] = set()
    resolved: list[tuple[Attachment, str]] = []
    for index, attachment in enumerate(attachments, start=1):
        base = sanitize_filename(attachment.filename, f"attachment_{index}")
        safe = _first_free_name(base, occupied)
        occupied.add(safe)
        resolved.append((attachment, safe))
    return resolved


def _first_free_name(name: str, occupied: set[str]) -> str:
    if name not in occupied:
        return name
    stem, dot, ext = name.rpartition(".")
    if dot:
        prefix, suffix = stem, f".{ext}"
    else:
        prefix, suffix = name, ""
    counter = 1
    while True:
        candidate = f"{prefix}_{counter}{suffix}"
        if candidate not in occupied:
            return candidate
        counter += 1


class GmailAttachmentsBaseOperator(BaseOperator):
    """Abstract base for the Gmail-attachment operators.

    Holds the parameters common to every destination and the storage seam
    (:meth:`_write`, :meth:`_destination_path`, :meth:`_read_manifest`) that
    concrete subclasses implement (ADR-0006). It is abstract: the seam methods
    and :meth:`_filter_processed_label` raise :class:`NotImplementedError`, so a
    subclass that forgets one fails loudly. ``BaseOperatorMeta`` cannot be mixed
    with :class:`abc.ABCMeta`, hence the manual enforcement.

    ``date_from``/``date_to`` are **templated** and therefore parsed/validated in
    ``execute()`` after rendering (Task 8, ADR-0004), *not* in ``__init__`` —
    the reverse of ``attachment_pattern``, which is compiled here so a bad regex
    fails at DAG-parse time and is intentionally **not** templated (ADR-0005).

    The default ``lookback_days`` is ``7``; the local operator overrides it to
    ``0`` (Task 10, ADR-0001).
    """

    template_fields: Sequence[str] = (
        "query",
        "source",
        "from_email",
        "subject_contains",
        "filename_contains",
        "date_from",
        "date_to",
    )

    def __init__(
        self,
        *,
        source: str,
        gmail_conn_id: str = GmailHook.default_conn_name,
        query: str | None = None,
        from_email: str | None = None,
        subject_contains: str | None = None,
        has_attachment: bool = False,
        filename_contains: str | None = None,
        attachment_pattern: str | None = None,
        lookback_days: int = 7,
        mark_processed: bool = False,
        label_suffix: str | None = None,
        timezone: str = "Europe/Moscow",
        overwrite: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if lookback_days < 0:
            raise ValueError(
                f"lookback_days must be >= 0 (0 means 'today only'), "
                f"got {lookback_days}"
            )

        # Validate the timezone eagerly: an unknown zone must fail at DAG parse,
        # not deep inside a run. The string is kept — the helpers build ZoneInfo.
        try:
            ZoneInfo(timezone)
        except Exception as exc:
            raise ValueError(f"unknown timezone {timezone!r}: {exc}") from exc

        # Compile the pattern in __init__ so a bad regex fails at DAG parse
        # (ADR-0005). It is NOT templated: a rendered template would otherwise
        # reach re.compile as a raw ``{{ ... }}`` string.
        self._compiled_pattern: re.Pattern[str] | None = (
            re.compile(attachment_pattern) if attachment_pattern is not None else None
        )

        self.gmail_conn_id = gmail_conn_id
        self.source = source
        self.query = query
        self.from_email = from_email
        self.subject_contains = subject_contains
        self.has_attachment = has_attachment
        self.filename_contains = filename_contains
        self.attachment_pattern = attachment_pattern
        self.lookback_days = lookback_days
        self.mark_processed = mark_processed
        self.label_suffix = label_suffix
        self.timezone = timezone
        self.overwrite = overwrite
        self.date_from = date_from
        self.date_to = date_to

    # -- Storage seam (ADR-0006): implemented by concrete subclasses ---------

    def _write(self, rel_path: str, data: bytes) -> None:
        """Write ``data`` at ``rel_path`` (relative to the destination base).

        Abstract: S3 → ``load_bytes(..., replace=True)`` (Task 9), local → write
        to disk creating directories (Task 10).
        """
        raise NotImplementedError

    def _destination_path(self, rel_path: str) -> str:
        """The canonical destination path of ``rel_path`` (for the manifest / XCom).

        Abstract: S3 → the object key inside the bucket, local → the absolute
        disk path.
        """
        raise NotImplementedError

    def _read_manifest(self, rel_dir: str) -> Manifest | None:
        """Read the manifest under ``rel_dir``, or ``None`` when absent.

        ``rel_dir`` (of the form ``dt=YYYY-MM-DD/<message_id>``) is assembled by
        the base class; the subclass only reads bytes and parses them through
        :meth:`Manifest.from_json`. Returns the **whole** :class:`Manifest` (not a
        bare ``run_id``) so :func:`decide` can resolve the delivery tri-state.
        Abstract: S3 reads + parses (Task 9), local always returns ``None``
        (Task 10).
        """
        raise NotImplementedError

    def _filter_processed_label(self) -> bool:
        """Whether ``-label:`` is mixed into the search query (ADR-0001).

        Abstract on purpose so a subclass that forgets the policy fails loudly:

        - **S3 → always ``False``**: the label must not filter the search, or a
          retry after a marked-but-not-returned run would silently lose delivery
          (correctness rests on the manifest + ``run_id`` alone).
        - **local → ``self.mark_processed``**: an opt-in dedup of a wide window,
          since local has no retry-delivery contract to break.

        ``execute()`` forwards the result to ``hook.build_query(...)``;
        ``overwrite`` no longer affects the query string.
        """
        raise NotImplementedError

    # -- Manifest assembly (wired by execute() in Task 8) --------------------

    def _write_manifest(
        self,
        rel_dir: str,
        message_id: str,
        internal_date: str,
        subject: str,
        from_: str,
        files: list[FileEntry],
        run_id: str,
    ) -> Manifest:
        """Build the manifest and write it (last) under ``rel_dir``.

        Mirrors the ``execute()`` pseudocode: ``internal_date`` is converted to
        ISO in the operator timezone via :func:`to_local_iso`, then
        :meth:`Manifest.build` assembles it and ``to_json()`` is written through
        the storage seam. The full run loop that calls this lives in Task 8.
        """
        manifest = Manifest.build(
            self.source,
            message_id,
            to_local_iso(internal_date, self.timezone),
            subject,
            from_,
            files,
            run_id,
        )
        self._write(f"{rel_dir}/_manifest.json", manifest.to_json())
        return manifest
