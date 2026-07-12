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

import re
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
        self.exclude_label_id = "<unset>"
        self.find_label_id_calls: list[str] = []
        self.label_id_to_return: str | None = None
        self.labelled_ids: set[str] = set()

    def build_query(
        self,
        window,
        from_email,
        subject_contains,
        has_attachment,
        filename_contains,
        raw_query,
    ) -> str:
        self.built_query = self._real.build_query(
            window,
            from_email,
            subject_contains,
            has_attachment,
            filename_contains,
            raw_query,
        )
        return self.built_query

    def find_label_id(self, name):
        self.find_label_id_calls.append(name)
        return self.label_id_to_return

    def find_messages_with_attachments(self, query, pattern, exclude_label_id=None):
        self.exclude_label_id = exclude_label_id
        results = []
        for msg in self._messages:
            if exclude_label_id is not None and msg.message_id in self.labelled_ids:
                continue
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


def test_default_soft_fail_is_false():
    # Contract guard (Task 9): the sensor deliberately does NOT override
    # soft_fail, so Airflow's loud default (False → timeout fails the task and
    # fires alerts) applies. A user must pass soft_fail=True explicitly to turn a
    # timeout into `skipped`. Guards against an accidental silent default change.
    assert _make_sensor().soft_fail is False


def test_filter_processed_label_follows_mark_processed():
    assert _make_sensor(mark_processed=False)._filter_processed_label() is False
    assert _make_sensor(mark_processed=True)._filter_processed_label() is True


def test_invalid_attachment_pattern_fails_at_init():
    # Parity with the operators (ADR-0005): a bad regex must fail at DAG parse
    # (construction), not on the first poke.
    with pytest.raises(re.error):
        _make_sensor(attachment_pattern="([")


def test_negative_lookback_days_fails_at_init():
    # Parity with the operator base __init__: a negative lookback_days must be
    # rejected at construction (DAG parse), not silently accepted and turned into
    # a future `after:` boundary on the first poke.
    with pytest.raises(ValueError):
        _make_sensor(lookback_days=-1)


def test_unknown_timezone_fails_at_init():
    # Parity with the operator base __init__: an unknown IANA timezone must fail
    # at construction (ZoneInfo resolved eagerly), not on the first poke.
    with pytest.raises(ValueError):
        _make_sensor(timezone="Not/AZone")


def test_sensor_has_no_overwrite_parameter():
    # overwrite only affects an operator's download/deliver decision and is
    # meaningless for a sensor; it must not be an accepted kwarg (Airflow rejects
    # the unknown arg with AirflowException) and must not be stored.
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException):
        _make_sensor(overwrite=True)
    assert not hasattr(_make_sensor(), "overwrite")


def test_range_override_warning_logged_once_across_pokes(caplog):
    # The "explicit range overrides non-default lookback_days" WARNING must fire
    # once per instance, not on every poke (poke runs for hours).
    sensor = _make_sensor(date_from="2026-07-01", lookback_days=3)
    hook = FakeGmailHook([])
    with caplog.at_level("WARNING"):
        _poke(sensor, hook)
        _poke(sensor, hook)
        _poke(sensor, hook)
    warnings = [r for r in caplog.records if "lookback_days" in r.getMessage()]
    assert len(warnings) == 1


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

    Same window, range, timezone → the same query string (which no longer
    carries any ``-label:`` term). The same ``_filter_processed_label()`` policy
    (``mark_processed=True`` on both) → both resolve the same processed label to
    the same ``labelId`` via ``find_label_id`` and pass it as ``exclude_label_id``
    to ``find_messages_with_attachments``, so the two dedup identically in code.
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
    sensor_hook.label_id_to_return = "Label_proc"
    sensor.hook = sensor_hook
    sensor.poke(_context())

    # The operator runs with an empty result set so execute() builds the query
    # (before the loop) but writes nothing to disk, then skips.
    op = GmailAttachmentsToLocalOperator(task_id="t", path="/data/gmail/avito", **common)
    op_hook = FakeGmailHook([])
    op_hook.label_id_to_return = "Label_proc"
    op.hook = op_hook
    with pytest.raises(AirflowSkipException):
        op.execute(_context())

    assert sensor_hook.built_query is not None
    assert op_hook.built_query is not None
    assert sensor_hook.built_query == op_hook.built_query
    # The label filter is applied identically in code: both resolve the same
    # processed-label name to the same id and forward it as exclude_label_id.
    assert sensor_hook.find_label_id_calls == ["airflow/processed/avito"]
    assert op_hook.find_label_id_calls == ["airflow/processed/avito"]
    assert sensor_hook.exclude_label_id == op_hook.exclude_label_id == "Label_proc"


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


# -- processed-label dedup (labelIds filter, not -label:) --------------------


def test_poke_mark_processed_true_filters_labelled_message():
    # mark_processed=True → poke resolves the processed label id and drops a
    # message already carrying it (labelIds filter in code, no -label: term), so
    # the sole labelled candidate is filtered out and poke reports "no work".
    sensor = _make_sensor(mark_processed=True, label_suffix="avito")
    hook = FakeGmailHook([_message("msg1", "report.xlsx")])
    hook.label_id_to_return = "Label_proc"
    hook.labelled_ids = {"msg1"}
    assert _poke(sensor, hook) is False
    assert hook.find_label_id_calls == ["airflow/processed/avito"]
    assert hook.exclude_label_id == "Label_proc"


def test_poke_mark_processed_true_but_label_absent_does_not_filter():
    # find_label_id → None (no such label yet) → exclude_label_id None, so an
    # unlabelled matching message still surfaces.
    sensor = _make_sensor(mark_processed=True, label_suffix="avito")
    hook = FakeGmailHook([_message("msg1", "report.xlsx")])
    hook.label_id_to_return = None
    assert _poke(sensor, hook) is True
    assert hook.find_label_id_calls == ["airflow/processed/avito"]
    assert hook.exclude_label_id is None


def test_poke_mark_processed_false_never_resolves_label():
    sensor = _make_sensor(mark_processed=False)
    hook = FakeGmailHook([_message("msg1", "report.xlsx")])
    assert _poke(sensor, hook) is True
    assert hook.find_label_id_calls == []
    assert hook.exclude_label_id is None
