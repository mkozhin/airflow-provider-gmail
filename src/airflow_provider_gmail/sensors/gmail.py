"""``GmailAttachmentSensor`` — "is there a matching email" (Gmail only).

This sensor answers the *cheapest* of the two waiting questions: does Gmail hold
a message that matches the search filter right now? It looks **only** at Gmail
and knows nothing about storage state, so it pairs with the local operator (and
with any flow where dedup is guaranteed by ``mark_processed=True``). The
storage-aware "is there *new* work" question is a different one and is answered
by :class:`GmailAttachmentToS3Sensor` (Task 12).

Query parity with the operator is the whole point: :meth:`poke` resolves the
window and builds the query through the **same** :func:`parse_date_range`,
:class:`Window` and :meth:`GmailHook.build_query` the operator's ``execute()``
uses, so the sensor never searches a different set of messages than the operator
that follows it — otherwise the two would fire and hang out of step. There is no
second copy of the query logic here.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from functools import cached_property
from typing import Any

from airflow.sensors.base import BaseSensorOperator

from airflow_provider_gmail.dates import (
    parse_date_range,
    resolve_lookback_days,
    to_local_date,
    validate_timezone,
)
from airflow_provider_gmail.hooks.gmail import (
    GmailHook,
    MessageWithAttachments,
    resolve_label_name,
)
from airflow_provider_gmail.manifest import Manifest
from airflow_provider_gmail.utils.paths import manifest_key, validate_prefix
from airflow_provider_gmail.window import Window


class GmailAttachmentSensor(BaseSensorOperator):
    """Poke Gmail until a message matching the search filter exists.

    Knows only about Gmail: :meth:`poke` returns ``True`` as soon as
    ``hook.find_messages_with_attachments()`` is non-empty. It does **not** look
    at any storage, so with ``mark_processed=False`` it keeps firing on an
    already-processed message until that message falls out of the
    ``lookback_days`` window — the operator behind it then honestly skips. **Do
    not rely on ``lookback_days=1`` for dedup**: a processed message stays in the
    Gmail result set until the window ends, and with labels off the sensor
    re-fires on it. When you need "is there *new* work", use
    :class:`GmailAttachmentToS3Sensor` instead.

    Suited to flows where dedup is opt-in via ``mark_processed=True`` (labelled
    messages stay in Gmail's result set and the provider filters them out in
    code by comparing ``labelIds`` to the resolved processed-label id) and to
    the local operator.

    ``mode="reschedule"`` is set as the **default** in ``__init__`` (not merely
    documented): in the default ``poke`` mode the sensor would hold a worker slot
    for the whole wait (hours), and with dozens of exports that eats the pool.
    ``soft_fail`` is **not** overridden here, so Airflow's default (``False``)
    applies: a ``timeout`` **fails** the task and fires alerts. Pass
    ``soft_fail=True`` explicitly if you want a timeout to become ``skipped``
    (green DAG) instead of a failure/alert.

    The filter parameters mirror the operators exactly (``query``,
    ``from_email``, ``subject_contains``, ``has_attachment``,
    ``filename_contains``, ``attachment_pattern``, ``lookback_days``,
    ``timezone``, ``date_from``/``date_to``, ``mark_processed``, ``label_suffix``)
    so the same search is reproduced. There is no ``overwrite``: it only affects
    an operator's download/deliver decision and has no meaning for a sensor.

    ``lookback_days`` is **templated** (ADR-0009): cast/validated in
    :meth:`_find_messages` after rendering, via
    :func:`~airflow_provider_gmail.dates.resolve_lookback_days`, the same
    pattern as ``date_from``/``date_to``. The fallback is strict — a rendered
    empty string, ``"None"``, or a native ``None`` raises :class:`ValueError`
    rather than silently falling back to the class default; the DAG author is
    responsible for a default *inside* the Jinja expression itself (e.g.
    ``{{ dag_run.conf.get('lookback_days', 7) }}``). A plain Python literal (no
    Jinja) still works exactly as before for a *valid* value; an invalid
    literal (e.g. ``lookback_days=-1``) now fails at the first ``poke()``
    instead of at ``__init__`` (DAG-parse time), because the same runtime path
    now serves both the literal and the template.

    **Pairing with the local operator: match ``lookback_days``.** This sensor's
    default ``lookback_days`` is ``7``, but :class:`GmailAttachmentsToLocalOperator`
    defaults to ``0`` ("today only"). Query parity only holds when the two agree,
    so when you place this sensor in front of the local operator you **must** set
    the same ``lookback_days`` on both (typically ``lookback_days=0`` on the
    sensor) — otherwise the sensor may fire on a message the operator's narrower
    window never returns, and the DAG hangs. This holds whether ``lookback_days``
    is a plain ``int`` or a rendered template on either side.
    """

    #: The sensor's *default* ``lookback_days``, used only to decide whether to
    #: WARN that an explicit date range is overriding a **non-default**
    #: ``lookback_days`` (ADR-0004) — symmetric with the operator.
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
        date_from: str | None = None,
        date_to: str | None = None,
        mode: str = "reschedule",
        **kwargs,
    ) -> None:
        # reschedule by default (see the class docstring): the poke mode would
        # otherwise hold a worker slot for the entire (possibly multi-hour) wait.
        super().__init__(mode=mode, **kwargs)

        # Reject an unknown timezone eagerly so a typo fails at DAG parse, not
        # deep inside a run (shared with the operator). ``lookback_days`` is
        # templated (ADR-0009): it is cast/validated at runtime in
        # ``_find_messages``, after rendering — not here — because it must
        # accept a rendered template string the same way it accepts a plain
        # literal.
        validate_timezone(timezone)

        # Compile the pattern in __init__ so a bad regex fails at DAG parse
        # (ADR-0005), for parity with the operators — not on the first poke. It is
        # NOT templated (a rendered template would reach re.compile as raw text).
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
        self.date_from = date_from
        self.date_to = date_to
        # Guards the "explicit range overrides non-default lookback_days" WARNING
        # so it is logged once per instance rather than on every poke.
        self._range_warning_logged = False

    @cached_property
    def hook(self) -> GmailHook:
        """The :class:`GmailHook` bound to :attr:`gmail_conn_id` (built once)."""
        return GmailHook(self.gmail_conn_id)

    def _filter_processed_label(self) -> bool:
        """Whether processed messages are filtered out by their label id.

        Returns :attr:`mark_processed`: the base sensor dedups via the Gmail
        label (a ``labelIds`` comparison in code — :meth:`GmailHook.find_label_id`
        + ``exclude_label_id``, never a ``-label:`` query term), exactly like
        :class:`GmailAttachmentsToLocalOperator`. :class:`GmailAttachmentToS3Sensor`
        overrides this to ``False`` (it dedups by manifest, never by the label —
        ADR-0001).
        """
        return self.mark_processed

    def _find_messages(self, context: Any) -> list[MessageWithAttachments]:
        """The matching Gmail messages, found through the operator's exact chain.

        Builds the query through the *same* steps the operator's ``execute()``
        uses — ``data_interval_end`` as the stable reference day, the shared
        :func:`parse_date_range`, :meth:`Window.resolve` and
        :meth:`GmailHook.build_query` — so the sensor searches exactly the set the
        operator will. The sensor is also a window-resolve owner, so it emits the
        same WARNING when an explicit range overrides a non-default
        ``lookback_days`` (ADR-0004). :class:`GmailAttachmentToS3Sensor` reuses
        this so the storage-aware sensor never diverges from the base query.
        """
        ref_day = context["data_interval_end"]
        label_name = resolve_label_name(self.label_suffix)

        # Cast/validate the templated lookback_days here, right after rendering,
        # so the same code path handles both a plain literal and a rendered
        # Jinja string (ADR-0009). Unconditional even in explicit-range mode,
        # where Window.resolve below ends up ignoring lookback_days — a
        # garbage-rendered value still fails fast rather than being silently
        # skipped (documented trade-off, see ADR-0009).
        lookback_days = resolve_lookback_days(self.lookback_days)

        date_from, date_to = parse_date_range(self.date_from, self.date_to)
        if (
            (date_from is not None or date_to is not None)
            and (lookback_days != self.default_lookback_days)
            and not self._range_warning_logged
        ):
            # Log once per instance: poke() runs every poke_interval for hours, so
            # an unconditional warning would repeat endlessly.
            self._range_warning_logged = True
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
        # subclass policy (S3 -> None; base sensor -> when mark_processed).
        exclude_label_id = (
            self.hook.find_label_id(label_name)
            if self._filter_processed_label()
            else None
        )
        return list(
            self.hook.find_messages_with_attachments(
                query, self._compiled_pattern, exclude_label_id
            )
        )

    def poke(self, context: Any) -> bool:
        """``True`` iff Gmail holds at least one matching message."""
        return bool(self._find_messages(context))


class GmailAttachmentToS3Sensor(GmailAttachmentSensor):
    """Poke Gmail until there is *new* work to do for the S3 operator.

    Where :class:`GmailAttachmentSensor` answers "is there a matching email",
    this storage-aware subclass answers "is there *new* work": it discards every
    matched message already processed by a **different** run and returns ``True``
    only if at least one **unprocessed** message remains. It is the natural gate
    for :class:`GmailAttachmentsToS3Operator` — with ``mark_processed=False`` a
    processed message stays in the Gmail result set until the ``lookback_days``
    window ends, so the Gmail-only sensor would keep firing while the operator
    behind it does :attr:`~airflow_provider_gmail.manifest.Decision.SKIP`; this
    one does not.

    "Processed" is decided by the manifest, never by a Gmail label:
    :meth:`_filter_processed_label` is ``False`` so processed messages are not
    filtered out by their label id (symmetric with the S3 operator — ADR-0001).
    A message counts as **processed only when a manifest from a *different*
    (past) ``run_id`` exists**. A manifest carrying the *current* ``run_id`` is
    **not** "processed": it means the current run wrote the manifest but has not
    finished delivering, so the operator behind the sensor will re-run as
    :attr:`~airflow_provider_gmail.manifest.Decision.DELIVER_ONLY` (not skip),
    and the sensor keeps firing. This is exactly the **whole-run-clear
    recovery** scenario: after a run's task instances are cleared and the run is
    re-run under the *same* ``run_id``, a stale current-run manifest must not
    mask the still-pending work — so the sensor treats it as work remaining.
    Each candidate's manifest is looked up at the **same** key the operator
    writes (the shared
    :func:`~airflow_provider_gmail.utils.paths.manifest_key`), so sensor and
    operator can never disagree on where a message's manifest lives.

    A present manifest is not merely detected but **read and validated** through
    :meth:`Manifest.from_json`: a corrupt / structurally-invalid manifest raises
    :class:`~airflow_provider_gmail.manifest.ManifestError` and **fails the
    poke**, rather than silently treating the message as processed — otherwise a
    damaged manifest would block the message forever with a green sensor
    (the opposite of the ``ManifestError`` contract; see ``CONTEXT.md``).

    Each ``poke`` repeats the Gmail search and MIME parse and issues one
    ``check_for_key`` (plus a ``read_key`` on a hit) per candidate; at the
    intended ``poke_interval`` of ~30 min this cost is negligible, which is
    exactly why the cheap Gmail-only sensor stays a separate class.

    An ``overwrite`` backfill is incompatible with this sensor: it would report
    "no work" for a message that already has a *past-run* manifest, so the
    operator behind it would never run. The sensor has no ``overwrite`` parameter at all — drive
    overwrite backfills of :class:`GmailAttachmentsToS3Operator` without this
    sensor.

    The ``apache-airflow-providers-amazon`` package (the ``s3`` extra) is
    imported **lazily** inside :meth:`_s3_hook`, so the module still imports —
    and the Gmail-only :class:`GmailAttachmentSensor` still works — when the
    extra is not installed.
    """

    template_fields: Sequence[str] = (
        *GmailAttachmentSensor.template_fields,
        "bucket",
        "prefix",
    )

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self._cached_s3_hook = None

    def _s3_hook(self):
        """The cached ``S3Hook``, importing the Amazon provider lazily.

        The import lives here (not at module top level) so ``sensors/gmail.py``
        remains importable without the ``s3`` extra — the Gmail-only sensor
        shares this module.
        """
        if self._cached_s3_hook is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self._cached_s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        return self._cached_s3_hook

    def _filter_processed_label(self) -> bool:
        """Never filter the search by label (ADR-0001): new work is the manifest."""
        return False

    def _has_processed_manifest(self, msg: MessageWithAttachments, run_id: str) -> bool:
        """Whether ``msg`` was already processed by a **different** run.

        "Processed" means a manifest exists **and** it was written by another
        (past) ``run_id`` — this mirrors ADR-0001: a manifest carrying the
        *current* ``run_id`` means work still remains (the operator behind the
        sensor will do :attr:`~airflow_provider_gmail.manifest.Decision.DELIVER_ONLY`,
        not skip), so it is **not** counted as processed and this returns
        ``False``. Only a manifest from a different run returns ``True``.

        Reads and **validates** the manifest through :meth:`Manifest.from_s3`
        (so a corrupt manifest — invalid JSON *or* non-UTF-8 bytes — raises
        :class:`ManifestError` and fails the poke instead of being silently
        counted as processed), then compares ``manifest.run_id`` to the current
        ``run_id``. The key is built from the shared :func:`manifest_key`, the
        same join the S3 operator writes.
        """
        dt = to_local_date(msg.internal_date, self.timezone).isoformat()
        key = manifest_key(self.prefix, dt, msg.message_id)
        hook = self._s3_hook()
        if not hook.check_for_key(key, bucket_name=self.bucket):
            return False
        manifest = Manifest.from_s3(hook, self.bucket, key)
        return manifest.run_id != run_id

    def poke(self, context: Any) -> bool:
        """``True`` iff at least one matched message has no processed manifest.

        Reuses :meth:`GmailAttachmentSensor._find_messages` (identical query to
        the operator), then drops every candidate already processed by a
        **different** run. A candidate whose manifest carries the *current*
        ``run_id`` (``context["run_id"]``) still counts as work — that is the
        whole-run-clear recovery path (see the class docstring) — so ``poke``
        keeps firing until the operator re-writes/completes it. A corrupt
        manifest fails the poke via :class:`ManifestError` — it is never
        silently treated as processed.

        The rendered ``prefix`` is validated first (parity with the S3 operator's
        ``execute()``, ADR-0007): the sensor emits no URI and its keys do not
        break on a hostile ``prefix``, but rejecting bad config early keeps
        operator and sensor uniform so a future review need not remove it.
        """
        validate_prefix(self.prefix)
        run_id = context["run_id"]
        for msg in self._find_messages(context):
            if not self._has_processed_manifest(msg, run_id):
                return True
        return False
