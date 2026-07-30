"""``GmailAttachmentsBaseOperator`` — the abstract base operator and its helpers.

This module carries the storage-agnostic interface shared by the S3 and local
operators (ADR-0006: the storage seam stays inheritance, no ``Destination``
port). The pure date helpers (:func:`parse_date_range`, :func:`to_local_date`,
:func:`to_local_iso`) live in :mod:`airflow_provider_gmail.dates` so the sensors
can reuse them without importing this operator module; they are re-exported here
for the ``execute()`` orchestration (Task 8). :func:`resolve_collisions`
(sanitize names and resolve intra-message name collisions with a suffix) stays
here — it is an operator concern, not a shared date helper.

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

import os
import re
from collections.abc import Iterable, Sequence
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator

# Re-exported so ``execute()`` and existing callers can reach them here; the
# canonical home is ``airflow_provider_gmail.dates`` (see module docstring).
from airflow_provider_gmail.dates import (
    parse_date_range,
    resolve_lookback_days,
    resolve_overwrite,
    to_local_date,
    to_local_iso,
    validate_timezone,
)
from airflow_provider_gmail.hooks.gmail import GmailHook, resolve_label_name
from airflow_provider_gmail.manifest import Decision, FileEntry, Manifest, decide
from airflow_provider_gmail.utils.mime import Attachment, sanitize_filename
from airflow_provider_gmail.utils.paths import (
    MANIFEST_FILENAME,
    message_dir,
    s3_key,
    s3_uri,
    validate_prefix,
)
from airflow_provider_gmail.window import Window

__all__ = [
    "GmailAttachmentsBaseOperator",
    "GmailAttachmentsToS3Operator",
    "GmailAttachmentsToLocalOperator",
    "parse_date_range",
    "resolve_collisions",
    "to_local_date",
    "to_local_iso",
]


def _one_line(value: str | None) -> str:
    """Collapse any whitespace run (incl. ``\\n``/``\\t``/control spaces) to single spaces.

    Used to keep a log line on one physical line: subjects and (untrusted)
    attachment filenames may carry newlines or control characters. No length
    truncation (YAGNI). An empty/``None`` value yields an empty string.
    """
    return " ".join((value or "").split())


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

    The manifest filename (:data:`MANIFEST_FILENAME`) is seeded as already-taken
    so no attachment can be assigned ``_manifest.json`` — otherwise an attachment
    sanitizing to that name would be written to the same key the per-message
    manifest is written to **last**, silently overwriting the attachment (data
    loss, ADR-0001: a delivered path must point at the actual attachment). Such an
    attachment becomes ``_manifest_1.json`` instead.

    Returns ``(attachment, safe_name)`` pairs. The :class:`Attachment` is **not**
    mutated (it has no ``safe_name`` field — see Task 2).
    """
    occupied: set[str] = {MANIFEST_FILENAME}
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
    # ``os.path.splitext`` (the same splitter as ``mime._cap_filename_length``)
    # keeps a dotfile like ``.bashrc`` whole as the stem, so its collision name
    # is ``.bashrc_1`` rather than ``_1.bashrc``.
    prefix, suffix = os.path.splitext(name)
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

    ``lookback_days`` and ``overwrite`` are likewise **templated** (ADR-0009):
    cast/validated in :meth:`_run` after rendering, via
    :func:`~airflow_provider_gmail.dates.resolve_lookback_days` /
    :func:`~airflow_provider_gmail.dates.resolve_overwrite`, the same pattern as
    ``date_from``/``date_to``. The fallback is strict — a rendered empty
    string, ``"None"``, or a native ``None`` raises :class:`ValueError` rather
    than silently falling back to the class default; the DAG author is
    responsible for a default *inside* the Jinja expression itself (e.g.
    ``{{ dag_run.conf.get('lookback_days', 7) }}``). A plain Python literal
    (no Jinja) still works exactly as before for a *valid* value; an invalid
    literal (e.g. ``lookback_days=-1``) now fails at ``execute()``/the first
    ``poke()`` instead of at ``__init__`` (DAG-parse time), because the same
    runtime path now serves both the literal and the template.

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
        "lookback_days",
        "overwrite",
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
        lookback_days: int | str = 7,
        mark_processed: bool = False,
        label_suffix: str | None = None,
        timezone: str = "Europe/Moscow",
        overwrite: bool | str = False,
        date_from: str | None = None,
        date_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # Reject an unknown timezone eagerly so a typo fails at DAG parse, not
        # deep inside a run (shared with the sensor). ``lookback_days`` is
        # templated (ADR-0009): it is cast/validated at runtime in ``_run``,
        # after rendering — not here — because it must accept a rendered
        # template string the same way it accepts a plain literal.
        validate_timezone(timezone)

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
        """The canonical destination path of ``rel_path`` (for the manifest).

        Feeds the manifest's ``files[].path`` (and the S3 sensor's key parity).
        Abstract: S3 → the object key inside the bucket, local → the absolute
        disk path. **Not** what XCom carries — that goes through
        :meth:`_xcom_path`, which diverges from this for S3 (ADR-0007).
        """
        raise NotImplementedError

    def _xcom_path(self, rel_path: str) -> str:
        """The path of ``rel_path`` as returned in XCom (a full, resolvable path).

        The storage seam's fourth method (ADR-0006/ADR-0007): XCom carries a full
        path a downstream consumer can address without knowing the bucket from a
        side channel. The base default returns :meth:`_destination_path`, so the
        local operator's XCom stays the absolute disk path; the S3 operator
        overrides it to an ``s3://<bucket>/<key>`` URI. Kept separate from
        :meth:`_destination_path` so the URI never leaks into the manifest's
        ``files[].path`` (the layer-2 contract — ADR-0001), and without any
        ``isinstance``/``hasattr`` branching in ``execute()`` (ADR-0006).
        """
        return self._destination_path(rel_path)

    def _read_manifest(self, rel_dir: str) -> Manifest | None:
        """Read the manifest under ``rel_dir``, or ``None`` when absent.

        ``rel_dir`` (of the form ``dt=YYYY-MM-DD/<message_id>``) is assembled by
        the base class; the subclass only reads and parses it (the S3 subclass
        via :meth:`Manifest.from_s3`, which wraps ``read_key`` +
        :meth:`Manifest.from_json`). Returns the **whole** :class:`Manifest` (not a
        bare ``run_id``) so :func:`decide` can resolve the delivery tri-state.
        Abstract: S3 reads + parses (Task 9), local always returns ``None``
        (Task 10).
        """
        raise NotImplementedError

    def _filter_processed_label(self) -> bool:
        """Whether processed messages are filtered out by their label id (ADR-0001).

        Abstract on purpose so a subclass that forgets the policy fails loudly:

        - **S3 → always ``False``**: the label must not filter the search, or a
          retry after a marked-but-not-returned run would silently lose delivery
          (correctness rests on the manifest + ``run_id`` alone).
        - **local → ``self.mark_processed``**: an opt-in dedup of a wide window,
          since local has no retry-delivery contract to break.

        When ``True``, ``execute()`` resolves the processed label to a ``labelId``
        via :meth:`GmailHook.find_label_id` and passes it as
        ``exclude_label_id`` to :meth:`find_messages_with_attachments`, which
        drops any message already carrying that label — the dedup is a
        ``labelIds`` comparison in code, never a ``-label:`` query term.
        ``overwrite`` does not affect it.
        """
        raise NotImplementedError

    # -- Manifest assembly (wired by execute() in Task 8) --------------------

    def _write_manifest(
        self,
        rel_dir: str,
        message_id: str,
        internal_date: int,
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
        self._write(f"{rel_dir}/{MANIFEST_FILENAME}", manifest.to_json())
        return manifest

    # -- Orchestration -------------------------------------------------------

    def execute(self, context: Any) -> list[str]:
        """Thin delegate to :meth:`_run`.

        Kept a one-liner so a storage subclass can validate its rendered config
        *before the same orchestration* by overriding ``execute`` and calling
        :meth:`_run` — NOT ``super().execute()``. A nested ``super().execute()``
        carries no ``ExecutorSafeguard`` sentinel and logs a spurious
        "... .execute cannot be called outside TaskInstance!" warning (Airflow
        2.9+). ``_run`` is not wrapped by the safeguard, so the inner call is
        silent.
        """
        return self._run(context)

    def _run(self, context: Any) -> list[str]:
        """Find, download and deliver the matching messages (see the plan pseudocode).

        The reference day and ``run_id`` come from ``context`` — never
        ``date.today()`` — so the window is stable between retries of the same
        task (a message written by a failed attempt is still found on retry and
        re-delivered from its current-``run_id`` manifest). For every matched
        message the :class:`Decision` tri-state maps onto actions in the strict
        order *attachments → manifest → label*; the manifest is always written
        **last** so its presence proves the attachments landed.

        Returns the full manifest paths of the messages processed **in this run
        only** (XCom) via :meth:`_xcom_path` — S3 ``s3://<bucket>/<key>`` URIs,
        local absolute paths (ADR-0007). Raises :class:`AirflowSkipException`
        when nothing was delivered.
        """
        run_id = context["run_id"]
        ref_day = context["data_interval_end"]
        label_name = resolve_label_name(self.label_suffix)

        # Cast/validate the templated lookback_days/overwrite here, right after
        # rendering, so the same code path handles both a plain literal and a
        # rendered Jinja string (ADR-0009). Unconditional even in explicit-range
        # mode, where Window.resolve below ends up ignoring lookback_days — a
        # garbage-rendered value still fails fast rather than being silently
        # skipped (documented trade-off, see ADR-0009).
        lookback_days = resolve_lookback_days(self.lookback_days)
        overwrite = resolve_overwrite(self.overwrite)

        date_from, date_to = parse_date_range(self.date_from, self.date_to)
        if (date_from is not None or date_to is not None) and (
            lookback_days != self.default_lookback_days
        ):
            # This resolve site owns the WARNING (ADR-0004): once the Window is
            # built the hook no longer knows the original lookback_days nor
            # whether it was default for this operator (S3 7 / local 0).
            self.log.warning(
                "An explicit date_from/date_to range was given; the non-default "
                "lookback_days=%s is ignored.",
                lookback_days,
            )

        window = Window.resolve(
            ref_day, self.timezone, lookback_days, date_from, date_to
        )
        query = self.hook.build_query(
            window,
            self.from_email,
            self.subject_contains,
            self.has_attachment,
            self.filename_contains,
            self.query,
        )

        # Processed-label dedup is applied in code over each message's labelIds,
        # not via -label: in the query (ADR-0001). Resolve the id only under the
        # subclass policy (S3 -> None; local -> when mark_processed); a lookup that
        # finds no such label yields None and simply disables the filter.
        exclude_label_id = (
            self.hook.find_label_id(label_name)
            if self._filter_processed_label()
            else None
        )

        delivered: list[str] = []
        to_label: list[str] = []
        downloaded_files = 0
        downloaded_msgs = 0
        redelivered = 0
        past_run_skipped = 0
        for msg in self.hook.find_messages_with_attachments(
            query, self._compiled_pattern, exclude_label_id
        ):
            dt = to_local_date(msg.internal_date, self.timezone)
            # Single owner of the ``dt=<date>/<message_id>`` layout: ``message_dir``
            # with an empty prefix (``join_key`` drops the empty segment). The S3
            # sensor locates the same message via ``paths.manifest_key``, so operator
            # and sensor share one layout owner and can never disagree.
            rel_dir = message_dir("", dt.isoformat(), msg.message_id)
            # XCom carries the full manifest path via ``_xcom_path`` (S3 → URI,
            # local → absolute path); the manifest's own ``files[].path`` keeps
            # going through ``_destination_path`` (ADR-0007), so the two sources
            # deliberately diverge for S3.
            manifest_xcom_path = self._xcom_path(f"{rel_dir}/{MANIFEST_FILENAME}")

            # overwrite=True → do not read the manifest at all: a corrupt manifest
            # must not raise ManifestError during the forced re-download that
            # overwrite exists for (ADR-0001).
            manifest = None if overwrite else self._read_manifest(rel_dir)
            decision = decide(manifest, run_id, overwrite)
            # Label catch-up is orthogonal to Decision — always append.
            to_label.append(msg.message_id)

            if decision is Decision.DELIVER_ONLY:
                # Manifest of THIS run (a failed attempt): files are on storage
                # but never reached downstream → deliver the path, do not download.
                delivered.append(manifest_xcom_path)
                redelivered += 1
                # Symmetric with the DOWNLOAD_AND_DELIVER line: report what is being
                # re-delivered (subject + files) from the current-run manifest.
                self.log.info(
                    "Re-delivered message %s %r: %d file(s) [%s] (no re-download) → %s",
                    msg.message_id,
                    _one_line(manifest.subject) or "(no subject)",
                    len(manifest.files),
                    ", ".join(repr(_one_line(f.name)) for f in manifest.files),
                    manifest_xcom_path,
                )
                continue
            if decision is Decision.SKIP:
                # Manifest of a PAST run: already went through layer 2 → drop.
                past_run_skipped += 1
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
            delivered.append(manifest_xcom_path)
            downloaded_msgs += 1
            downloaded_files += len(files)
            # Strictly AFTER _write_manifest: a failed write (raising above) must
            # NOT emit a false "Downloaded" line. ``from`` is deliberately not
            # logged (less sensitive mail metadata in Airflow logs).
            self.log.info(
                "Downloaded message %s %r: %d file(s) [%s] → %s",
                msg.message_id,
                _one_line(msg.subject) or "(no subject)",
                len(files),
                ", ".join(repr(_one_line(f.name)) for f in files),
                manifest_xcom_path,
            )

        if self.mark_processed and to_label:
            self.hook.mark_processed(to_label, label_name)

        # Always printed (including the skip path): a final summary of the run.
        self.log.info(
            "Gmail attachments: downloaded %d file(s) from %d message(s), "
            "re-delivered %d, past-run skipped %d.",
            downloaded_files,
            downloaded_msgs,
            redelivered,
            past_run_skipped,
        )

        if not delivered:
            raise AirflowSkipException("no new messages")
        return delivered


class GmailAttachmentsToS3Operator(GmailAttachmentsBaseOperator):
    """Download matching Gmail attachments and store them in S3 (or S3-compatible).

    Concrete S3 implementation of the storage seam (ADR-0006). Attachments and
    the per-message ``_manifest.json`` are written under
    ``<prefix>/dt=<dt>/<message_id>/`` inside ``bucket``; the full
    ``s3://<bucket>/<key>`` manifest URIs of the messages processed **in this
    run** are returned in XCom (ADR-0001/ADR-0007). The URI carries **no**
    endpoint — it addresses the object only paired with a consumer's
    ``aws_conn_id`` (the endpoint lives in the Connection). The manifest's own
    ``files[].path`` stays a bare object key (ADR-0007).

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
    already has a *past-run* manifest); run overwrite backfills without that sensor.

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

    def execute(self, context: Any) -> list[str]:
        """Validate the rendered ``prefix``, then run the base orchestration.

        ``prefix`` is a template field, so it is validated on its **rendered**
        value (parity with ``date_from``/``date_to``, ADR-0004) — not in
        ``__init__`` where a ``{{ ds }}`` template would trip the ``{``/``}``
        check. Validating at ``execute`` time — after ``pre_execute`` and
        ``on_execute_callback`` — means a late mutation of ``prefix`` is still
        caught, so produced object keys stay URL-safe by construction (ADR-0007).
        Calls :meth:`_run` (not ``super().execute()``), so the nested call does
        not trip ``ExecutorSafeguard`` (Airflow 2.9+) with a spurious warning.
        """
        validate_prefix(self.prefix)
        return self._run(context)

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

    def _write(self, rel_path: str, data: bytes) -> None:
        """Write ``data`` to its S3 key with ``replace=True``.

        ``replace=True`` is mandatory: ``S3Hook.load_bytes`` defaults to
        ``replace=False``, which would break both a re-run after a partial write
        and ``overwrite``. The decision *whether* to write is made earlier from
        the manifest, so the byte write never needs to hesitate.
        """
        self._s3_hook().load_bytes(
            data,
            key=self._destination_path(rel_path),
            bucket_name=self.bucket,
            replace=True,
        )

    def _destination_path(self, rel_path: str) -> str:
        """The S3 object key of ``rel_path``: ``<prefix>`` joined onto ``rel_path``.

        ``rel_path`` already carries the ``dt=<dt>/<message_id>/...`` layout the
        base class composed, so the key is a straight :func:`s3_key` — no
        decompose-then-recompose of the ``dt=`` segment. :func:`s3_key` guards
        the 1024-byte whole-key limit (a long ``prefix`` can push a filename that
        fits the 255-byte component cap over it), failing fast instead of letting
        S3 stall on every retry.
        """
        return s3_key(self.prefix, rel_path)

    def _xcom_path(self, rel_path: str) -> str:
        """The ``s3://<bucket>/<key>`` URI of ``rel_path`` for XCom (ADR-0007).

        Composes the *same* key as :meth:`_destination_path` (through the shared
        :func:`s3_key`) and anchors it to :attr:`bucket` via :func:`s3_uri`, so
        XCom's URI and the manifest's bare key never disagree on the object they
        point at — they differ only by the ``s3://<bucket>/`` prefix. The URI
        carries no endpoint; a consumer resolves it with its own ``aws_conn_id``.
        """
        return s3_uri(self.bucket, self.prefix, rel_path)

    def _read_manifest(self, rel_dir: str) -> Manifest | None:
        """Read + parse the manifest under ``rel_dir``, or ``None`` when absent.

        The key is ``<prefix>/<rel_dir>/_manifest.json`` via the same
        :func:`s3_key` the writes use (so operator and sensor agree on where the
        manifest lives, and the 1024-byte whole-key limit is enforced here too),
        with ``check_for_key`` + :meth:`Manifest.from_s3`. The read/parse/decode
        contract lives in the manifest module, not here: a corrupt manifest
        (invalid JSON *or* non-UTF-8 bytes) surfaces as ``ManifestError``.
        """
        key = s3_key(self.prefix, rel_dir, MANIFEST_FILENAME)
        hook = self._s3_hook()
        if not hook.check_for_key(key, bucket_name=self.bucket):
            return None
        return Manifest.from_s3(hook, self.bucket, key)

    def _filter_processed_label(self) -> bool:
        """Never filter the S3 search by label (ADR-0001) — correctness is the manifest."""
        return False


class GmailAttachmentsToLocalOperator(GmailAttachmentsBaseOperator):
    """Download matching Gmail attachments and write them to the local disk.

    Concrete local implementation of the storage seam (ADR-0006). Attachments and
    the per-message ``_manifest.json`` are written under
    ``<path>/dt=<dt>/<message_id>/``; the manifest's absolute paths of the messages
    processed **in this run** are returned in XCom. Local inherits the base
    :meth:`_xcom_path` default (``_destination_path``), so its XCom stays absolute
    disk paths — only the S3 operator diverges to ``s3://`` URIs (ADR-0007).

    **This operator is deliberately outside the S3 retry-delivery contract**
    (ADR-0001). :meth:`_read_manifest` always returns ``None``, so :func:`decide`
    always yields ``DOWNLOAD_AND_DELIVER``: every matched message is downloaded and
    delivered on every run. The local disk keeps no persistent dedup state — in the
    normal path (download → parse → a cleanup task removes the files) nothing is
    there on the next run, and if a run does repeat, overwriting is safer than
    failing. The manifest is still written, as a description of what was downloaded
    for the next step (it carries ``run_id``, even though this operator never reads
    it back).

    **Different ``lookback_days`` default from S3 (``0`` here vs ``7`` there).**
    S3 dedups delivery by manifest + ``run_id``, so a wide window is safe there; the
    local operator has no such dedup, so a wide window would re-deliver every message
    each run. The safe default is therefore ``0`` — "today only" — which suppresses
    duplicates by the *narrowness of the window* rather than by dedup (ADR-0001).
    :attr:`default_lookback_days` is overridden to ``0`` as well so the
    "explicit range overrides a non-default lookback_days" WARNING in ``execute()``
    is correct for the local default.

    Limitations (read before deploying):

    1. **One worker on one server.** The local disk is not shared between workers.
       Under CeleryExecutor with several workers, or KubernetesExecutor, the
       download task and the parse task may land on different machines and the parse
       will not find the file. In such an environment the local operator is safe
       only *within a single task*; use the S3 operator for everything else.
    2. **Idempotency is not guaranteed.** Deduplication is possible **only** via
       ``mark_processed=True``. ``lookback_days=1`` is **not** dedup — the search window is a
       time filter, not processing state; a message keeps matching every run until it
       falls out of the window. A smaller window only reduces the number of repeats,
       it does not eliminate them. With ``mark_processed=True`` and a wide window,
       processed messages are filtered out by their label id (see
       :meth:`_filter_processed_label`) as an opt-in dedup; the honest caveat is that
       a crash between setting the label and delivering downstream can, on retry,
       "lose" that message's delivery — acceptable within local's already-documented
       non-idempotency, and exactly why S3 (which has a retry-delivery contract)
       never filters by the label.
    3. **File cleanup is the DAG's job**, not the operator's. This operator never
       deletes anything; a downstream task (typically on ``all_done``) must remove
       the files, or the disk fills up.

    There is no user-facing ``overwrite`` argument: local has nothing to overwrite
    conditionally (``_read_manifest`` is always ``None`` and files are always
    overwritten). The inherited ``self.overwrite`` attribute stays ``False`` so the
    base ``execute()`` can reference it uniformly. Each attachment is loaded **fully
    into memory** before being written (Gmail caps incoming messages at 25 MB).
    """

    #: The local operator's *default* ``lookback_days`` (ADR-0001). Overridden to
    #: ``0`` from the base ``7`` so the "explicit range overrides a non-default
    #: lookback_days" WARNING in ``execute()`` fires correctly for local.
    default_lookback_days: int = 0

    template_fields: Sequence[str] = (
        *GmailAttachmentsBaseOperator.template_fields,
        "path",
    )

    def __init__(
        self,
        *,
        path: str,
        lookback_days: int = 0,
        **kwargs,
    ) -> None:
        # No public ``overwrite`` argument: it is fixed at the base default (False)
        # and never exposed here (see the class docstring).
        super().__init__(lookback_days=lookback_days, **kwargs)
        self.path = path

    def _write(self, rel_path: str, data: bytes) -> None:
        """Write ``data`` to disk, creating directories and overwriting the file.

        The file is overwritten **without checking whether it exists**: in the
        normal path a cleanup task already removed everything, and if a run did
        repeat, overwriting is safer than failing.
        """
        dest = self._destination_path(rel_path)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(data)

    def _destination_path(self, rel_path: str) -> str:
        """The absolute filesystem path of ``rel_path`` under :attr:`path`."""
        return os.path.abspath(os.path.join(self.path, rel_path))

    def _read_manifest(self, rel_dir: str) -> Manifest | None:
        """Always ``None``: local keeps no dedup state.

        With no manifest, :func:`decide` always returns
        ``Decision.DOWNLOAD_AND_DELIVER`` — every matched message is downloaded and
        delivered every run (ADR-0001).
        """
        return None

    def _filter_processed_label(self) -> bool:
        """Return :attr:`mark_processed`: local's opt-in dedup of a wide window.

        Unlike S3, local has no retry-delivery contract for the label to break, so
        filtering processed messages by their label id is allowed when
        ``mark_processed=True``. Caveat (see the class docstring): a crash between
        setting the label and delivering downstream can, on retry, lose that
        message's delivery — local makes no retry-delivery guarantee.
        """
        return self.mark_processed
