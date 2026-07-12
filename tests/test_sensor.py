"""Tests for :class:`GmailAttachmentSensor` (Task 11).

The Gmail-only sensor answers "is there a matching email": ``poke`` returns
``True`` iff ``hook.find_messages_with_attachments`` is non-empty. The critical
property is **query parity** with the operator — the sensor's ``poke`` and the
local operator's ``execute`` must build an *identical* query string from
identical params. The fake hook's ``build_query`` therefore delegates to the
*real* :meth:`GmailHook.build_query`, so the parity assertion is meaningful; only
``find_messages_with_attachments`` is stubbed. No network.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow.exceptions import AirflowSkipException

from airflow_provider_gmail.hooks.gmail import GmailHook, MessageWithAttachments
from airflow_provider_gmail.operators.gmail import GmailAttachmentsToLocalOperator
from airflow_provider_gmail.sensors.gmail import GmailAttachmentSensor
from airflow_provider_gmail.utils.mime import Attachment

MSK = "Europe/Moscow"


# -- fake hook ---------------------------------------------------------------


class FakeGmailHook:
    """Fake Gmail hook; ``build_query`` delegates to the real one to stay honest.

    ``find_messages_with_attachments`` applies the ``pattern`` to the fake
    messages exactly like the real hook (``re.search`` over the filename), so the
    "message present but attachment does not match pattern" case is exercised
    honestly rather than short-circuited.
    """

    def __init__(self, messages):
        self._messages = list(messages)
        self._real = GmailHook("unused")
        self.built_query = None

    def build_query(
        self,
        window,
        filter_processed_label,
        label_name,
        from_email,
        subject_contains,
        has_attachment,
        filename_contains,
        raw_query,
    ) -> str:
        self.built_query = self._real.build_query(
            window,
            filter_processed_label,
            label_name,
            from_email,
            subject_contains,
            has_attachment,
            filename_contains,
            raw_query,
        )
        return self.built_query

    def find_messages_with_attachments(self, query, pattern):
        import re

        results = []
        for msg in self._messages:
            if pattern is None:
                matched = list(msg.attachments)
            else:
                matched = [a for a in msg.attachments if re.search(pattern, a.filename)]
            if matched:
                results.append(msg)
        return results


# -- helpers -----------------------------------------------------------------


def _attachment(filename: str) -> Attachment:
    return Attachment(
        filename=filename, mime_type="application/octet-stream", attachment_id="x", data=None
    )


def _message(message_id: str, *filenames: str, day: date = date(2026, 7, 12)):
    dt = datetime(day.year, day.month, day.day, 9, 0, 0, tzinfo=ZoneInfo(MSK))
    return MessageWithAttachments(
        message_id=message_id,
        internal_date=int(dt.timestamp() * 1000),
        subject="Отчёт",
        from_="reports@avito.ru",
        attachments=[_attachment(f) for f in filenames],
    )


def _make_sensor(**kwargs) -> GmailAttachmentSensor:
    params = {"task_id": "s", "source": "avito"}
    params.update(kwargs)
    return GmailAttachmentSensor(**params)


def _context() -> dict:
    return {
        "run_id": "scheduled__2026-07-12T06:00:00+00:00",
        "data_interval_end": datetime(2026, 7, 12, 6, 0, 0, tzinfo=ZoneInfo("UTC")),
    }


def _poke(sensor, hook) -> bool:
    sensor.hook = hook
    return sensor.poke(_context())


# -- config ------------------------------------------------------------------


def test_template_fields_match_base_operator():
    tf = set(GmailAttachmentSensor.template_fields)
    assert tf == {
        "query",
        "source",
        "from_email",
        "subject_contains",
        "filename_contains",
        "date_from",
        "date_to",
    }


def test_default_mode_is_reschedule():
    sensor = _make_sensor()
    assert sensor.mode == "reschedule"


def test_filter_processed_label_follows_mark_processed():
    assert _make_sensor(mark_processed=False)._filter_processed_label() is False
    assert _make_sensor(mark_processed=True)._filter_processed_label() is True


# -- poke --------------------------------------------------------------------


def test_poke_true_when_message_present():
    sensor = _make_sensor()
    hook = FakeGmailHook([_message("msg1", "report.xlsx")])
    assert _poke(sensor, hook) is True


def test_poke_false_when_no_message():
    sensor = _make_sensor()
    hook = FakeGmailHook([])
    assert _poke(sensor, hook) is False


def test_poke_false_when_attachment_does_not_match_pattern():
    # The message passes the search but its only attachment fails the pattern →
    # find_messages_with_attachments drops it → no matching mail.
    sensor = _make_sensor(attachment_pattern=r"^report_\d{8}\.xlsx$")
    hook = FakeGmailHook([_message("msg1", "invoice.pdf")])
    assert _poke(sensor, hook) is False


# -- query parity with the operator ------------------------------------------


def test_query_parity_with_local_operator():
    """poke() and the local operator's execute() build an identical query.

    Same window, range, timezone and the same ``_filter_processed_label()`` →
    the same ``-label:`` term. The real ``build_query`` is invoked from both, so
    the two would search the very same Gmail set.
    """
    common = {
        "source": "avito",
        "from_email": "reports@avito.ru",
        "subject_contains": "Отчёт",
        "has_attachment": True,
        "filename_contains": "report",
        "attachment_pattern": r"\.xlsx$",
        "lookback_days": 3,
        "mark_processed": True,
        "label_suffix": "avito",
        "timezone": MSK,
    }

    sensor = GmailAttachmentSensor(task_id="s", **common)
    sensor_hook = FakeGmailHook([_message("msg1", "report.xlsx")])
    sensor.hook = sensor_hook
    sensor.poke(_context())

    # The operator runs with an empty result set so execute() builds the query
    # (before the loop) but writes nothing to disk, then skips.
    op = GmailAttachmentsToLocalOperator(task_id="t", path="/data/gmail/avito", **common)
    op_hook = FakeGmailHook([])
    op.hook = op_hook
    with pytest.raises(AirflowSkipException):
        op.execute(_context())

    assert sensor_hook.built_query is not None
    assert op_hook.built_query is not None
    assert sensor_hook.built_query == op_hook.built_query
    # And the label filter really is present (mark_processed=True on both).
    assert '-label:"airflow/processed/avito"' in sensor_hook.built_query


def test_query_parity_with_explicit_range():
    """Parity holds in explicit-range mode too (both ignore lookback_days)."""
    common = {
        "source": "avito",
        "date_from": "2026-07-01",
        "date_to": "2026-07-05",
        "lookback_days": 3,
        "timezone": MSK,
    }

    sensor = GmailAttachmentSensor(task_id="s", **common)
    sensor_hook = FakeGmailHook([])
    sensor.hook = sensor_hook
    sensor.poke(_context())

    op = GmailAttachmentsToLocalOperator(task_id="t", path="/data/gmail/avito", **common)
    op_hook = FakeGmailHook([])
    op.hook = op_hook
    with pytest.raises(AirflowSkipException):
        op.execute(_context())

    assert sensor_hook.built_query == op_hook.built_query
