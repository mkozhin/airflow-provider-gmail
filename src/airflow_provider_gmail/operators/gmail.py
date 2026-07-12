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

:meth:`GmailAttachmentsBaseOperator.execute` orchestrates the full run loop —
resolve the window, build the query, then for each matched message map the
:class:`Decision` tri-state (``manifest.py``, ADR-0001) onto download/deliver
actions in the strict order *attachments → manifest → label*. The dedup that
makes S3 delivery idempotent lives entirely in :func:`decide` over the
manifest's ``run_id``; the operator only maps its verdict onto actions.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from functools import cached_property
from typing import Any
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator

from airflow_provider_gmail.hooks.gmail import GmailHook, resolve_label_name
from airflow_provider_gmail.manifest import Decision, FileEntry, Manifest, decide
from airflow_provider_gmail.utils.mime import Attachment, sanitize_filename
from airflow_provider_gmail.window import Window


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

    #: The operator's *default* ``lookback_days``. Used only to decide whether to
    #: WARN that an explicit date range is overriding a **non-default**
    #: ``lookback_days`` (ADR-0004). The local operator overrides both this and
    #: the ``__init__`` default to ``0`` (Task 10).
    default_lookback_days: int = 7

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

    @cached_property
    def hook(self) -> GmailHook:
        """The :class:`GmailHook` bound to :attr:`gmail_conn_id` (built once)."""
        return GmailHook(self.gmail_conn_id)

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

    # -- Orchestration -------------------------------------------------------

    def execute(self, context: Any) -> list[str]:
        """Find, download and deliver the matching messages (see the plan pseudocode).

        The reference day and ``run_id`` come from ``context`` — never
        ``date.today()`` — so the window is stable between retries of the same
        task (a message written by a failed attempt is still found on retry and
        re-delivered from its current-``run_id`` manifest). For every matched
        message the :class:`Decision` tri-state maps onto actions in the strict
        order *attachments → manifest → label*; the manifest is always written
        **last** so its presence proves the attachments landed.

        Returns the canonical manifest paths of the messages processed **in this
        run only** (XCom). Raises :class:`AirflowSkipException` when nothing was
        delivered.
        """
        run_id = context["run_id"]
        ref_day = context["data_interval_end"]
        label_name = resolve_label_name(self.label_suffix)

        date_from, date_to = parse_date_range(self.date_from, self.date_to)
        if (date_from is not None or date_to is not None) and (
            self.lookback_days != self.default_lookback_days
        ):
            # This resolve site owns the WARNING (ADR-0004): once the Window is
            # built the hook no longer knows the original lookback_days nor
            # whether it was default for this operator (S3 7 / local 0).
            self.log.warning(
                "An explicit date_from/date_to range was given; the non-default "
                "lookback_days=%s is ignored.",
                self.lookback_days,
            )

        window = Window.resolve(
            ref_day, self.timezone, self.lookback_days, date_from, date_to
        )
        query = self.hook.build_query(
            window,
            self._filter_processed_label(),
            label_name,
            self.from_email,
            self.subject_contains,
            self.has_attachment,
            self.filename_contains,
            self.query,
        )

        delivered: list[str] = []
        to_label: list[str] = []
        for msg in self.hook.find_messages_with_attachments(
            query, self.attachment_pattern
        ):
            dt = to_local_date(msg.internal_date, self.timezone)
            rel_dir = f"dt={dt}/{msg.message_id}"
            manifest_path = self._destination_path(f"{rel_dir}/_manifest.json")

            # overwrite=True → do not read the manifest at all: a corrupt manifest
            # must not raise ManifestError during the forced re-download that
            # overwrite exists for (ADR-0001).
            manifest = None if self.overwrite else self._read_manifest(rel_dir)
            decision = decide(manifest, run_id, self.overwrite)
            # Label catch-up is orthogonal to Decision — always append.
            to_label.append(msg.message_id)

            if decision is Decision.DELIVER_ONLY:
                # Manifest of THIS run (a failed attempt): files are on storage
                # but never reached downstream → deliver the path, do not download.
                delivered.append(manifest_path)
                continue
            if decision is Decision.SKIP:
                # Manifest of a PAST run: already went through layer 2 → drop.
                self.log.info(
                    "Message %s was processed by an earlier run, skipping.",
                    msg.message_id,
                )
                continue

            # Decision.DOWNLOAD_AND_DELIVER
            files: list[FileEntry] = []
            for attachment, safe_name in resolve_collisions(msg.attachments):
                data = self.hook.download_attachment(msg.message_id, attachment)
                rel_path = f"{rel_dir}/{safe_name}"
                self._write(rel_path, data)
                files.append(
                    FileEntry(
                        name=attachment.filename,
                        size=len(data),
                        path=self._destination_path(rel_path),
                    )
                )

            # Manifest written LAST (after every attachment) — its presence is
            # the proof the message is fully on storage.
            self._write_manifest(
                rel_dir,
                msg.message_id,
                msg.internal_date,
                msg.subject,
                msg.from_,
                files,
                run_id,
            )
            delivered.append(manifest_path)

        if self.mark_processed and to_label:
            self.hook.mark_processed(to_label, label_name)

        if not delivered:
            raise AirflowSkipException("no new messages")
        return delivered


class GmailAttachmentsToS3Operator(GmailAttachmentsBaseOperator):
    """Download matching Gmail attachments and store them in S3 (or S3-compatible).

    Concrete S3 implementation of the storage seam (ADR-0006). Attachments and
    the per-message ``_manifest.json`` are written under
    ``<prefix>/dt=<dt>/<message_id>/`` inside ``bucket``; the manifest paths of
    the messages processed **in this run** are returned in XCom (ADR-0001).

    Deduplication of delivery rests on the manifest + ``run_id`` alone: a message
    whose ``_manifest.json`` carries a **past** ``run_id`` is neither re-downloaded
    nor delivered, one with the **current** ``run_id`` (a failed prior attempt of
    this run) is delivered without re-downloading, and one with no manifest is
    downloaded and delivered. Because the label is never mixed into the search
    (:meth:`_filter_processed_label` → ``False``), a marked-but-not-returned run is
    still found and delivered on retry.

    Works with any S3-compatible object store, not only AWS: point the underlying
    Amazon Connection at a custom ``endpoint_url`` via its ``extra`` (e.g. Yandex
    Object Storage, MinIO). Each attachment is loaded **fully into memory** before
    being written (Gmail caps incoming messages at 25 MB, so that is the ceiling).

    ``overwrite=True`` forces a re-download regardless of any manifest — the
    manifest is not even read, so a corrupt ``_manifest.json`` cannot break the
    recovery it exists for. It is incompatible with the storage-aware
    ``GmailAttachmentToS3Sensor`` (which would report "no work" for a message that
    already has a manifest); run overwrite backfills without that sensor.

    The ``apache-airflow-providers-amazon`` package (the ``s3`` extra) is imported
    lazily inside the methods that need it, so this module still imports — and the
    local operator still works — when the extra is not installed.
    """

    template_fields: Sequence[str] = (
        *GmailAttachmentsBaseOperator.template_fields,
        "bucket",
        "prefix",
    )

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        aws_conn_id: str = "aws_default",
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(overwrite=overwrite, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self._cached_s3_hook = None

    def _s3_hook(self):
        """The cached :class:`S3Hook`, importing the Amazon provider lazily.

        The import lives here (not at module top level) so ``operators/gmail.py``
        remains importable without the ``s3`` extra — the local operator shares
        this module (Task 9).
        """
        if self._cached_s3_hook is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self._cached_s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        return self._cached_s3_hook

    @staticmethod
    def _split_rel_dir(rel_dir: str) -> tuple[str, str]:
        """Split ``dt=<dt>/<message_id>`` into ``(dt, message_id)``."""
        dt_segment, message_id = rel_dir.split("/", 1)
        return dt_segment.removeprefix("dt="), message_id

    def _write(self, rel_path: str, data: bytes) -> None:
        """Write ``data`` to its S3 key with ``replace=True``.

        ``replace=True`` is mandatory: ``S3Hook.load_bytes`` defaults to
        ``replace=False``, which would break both a re-run after a partial write
        and ``overwrite``. The decision *whether* to write is made earlier from
        the manifest, so the byte write never needs to hesitate.
        """
        self._s3_hook().load_bytes(
            data, key=self._destination_path(rel_path), bucket_name=self.bucket, replace=True
        )

    def _destination_path(self, rel_path: str) -> str:
        """The S3 object key of ``rel_path`` via the shared :func:`s3_object_key`."""
        from airflow_provider_gmail.utils.paths import s3_object_key

        dt_dir, filename = rel_path.rsplit("/", 1)
        dt, message_id = self._split_rel_dir(dt_dir)
        return s3_object_key(self.prefix, dt, message_id, filename)

    def _read_manifest(self, rel_dir: str) -> Manifest | None:
        """Read + parse the manifest under ``rel_dir``, or ``None`` when absent.

        Uses the shared :func:`manifest_key` join (so operator and sensor agree on
        where the manifest lives) with ``check_for_key`` + ``read_key``.
        ``S3Hook.read_key`` returns ``str`` (decoded), which
        :meth:`Manifest.from_json` accepts; validation / ``ManifestError`` live in
        the manifest module, not here.
        """
        from airflow_provider_gmail.utils.paths import manifest_key

        dt, message_id = self._split_rel_dir(rel_dir)
        key = manifest_key(self.prefix, dt, message_id)
        hook = self._s3_hook()
        if not hook.check_for_key(key, bucket_name=self.bucket):
            return None
        return Manifest.from_json(hook.read_key(key, bucket_name=self.bucket))

    def _filter_processed_label(self) -> bool:
        """Never filter the S3 search by label (ADR-0001) — correctness is the manifest."""
        return False
