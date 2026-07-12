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

from collections.abc import Sequence
from functools import cached_property
from typing import Any

from airflow.sensors.base import BaseSensorOperator

from airflow_provider_gmail.hooks.gmail import GmailHook, resolve_label_name
from airflow_provider_gmail.operators.gmail import parse_date_range
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

    Suited to flows where dedup is provided by Gmail itself
    (``mark_processed=True`` — the label removes processed mail from the result
    set) and to the local operator.

    ``mode="reschedule"`` is set as the **default** in ``__init__`` (not merely
    documented): in the default ``poke`` mode the sensor would hold a worker slot
    for the whole wait (hours), and with dozens of exports that eats the pool.
    ``soft_fail=True`` (inherited from :class:`BaseSensorOperator`) turns a
    timeout into ``skipped`` (green DAG) instead of a failure/alert.

    The filter parameters mirror the operators exactly (``query``,
    ``from_email``, ``subject_contains``, ``has_attachment``,
    ``filename_contains``, ``attachment_pattern``, ``lookback_days``,
    ``timezone``, ``date_from``/``date_to``, ``mark_processed``, ``label_suffix``,
    ``overwrite``) so the same search is reproduced.
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
        mode: str = "reschedule",
        **kwargs,
    ) -> None:
        # reschedule by default (see the class docstring): the poke mode would
        # otherwise hold a worker slot for the entire (possibly multi-hour) wait.
        super().__init__(mode=mode, **kwargs)

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

    def _filter_processed_label(self) -> bool:
        """Whether ``-label:`` is mixed into the search (pairs with the local operator).

        Returns :attr:`mark_processed`: the base sensor dedups via the Gmail
        label, exactly like :class:`GmailAttachmentsToLocalOperator`.
        :class:`GmailAttachmentToS3Sensor` overrides this to ``False`` (it dedups
        by manifest, never by the label — ADR-0001).
        """
        return self.mark_processed

    def poke(self, context: Any) -> bool:
        """``True`` iff Gmail holds at least one matching message.

        Builds the query through the *same* chain the operator's ``execute()``
        uses — ``data_interval_end`` as the stable reference day, the shared
        :func:`parse_date_range`, :meth:`Window.resolve` and
        :meth:`GmailHook.build_query` — so the sensor searches exactly the set the
        operator will. The sensor is also a window-resolve owner, so it emits the
        same WARNING when an explicit range overrides a non-default
        ``lookback_days`` (ADR-0004).
        """
        ref_day = context["data_interval_end"]
        label_name = resolve_label_name(self.label_suffix)

        date_from, date_to = parse_date_range(self.date_from, self.date_to)
        if (date_from is not None or date_to is not None) and (
            self.lookback_days != self.default_lookback_days
        ):
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
        messages = self.hook.find_messages_with_attachments(
            query, self.attachment_pattern
        )
        return bool(messages)
