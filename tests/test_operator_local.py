"""Tests for :class:`GmailAttachmentsToLocalOperator` (Task 10).

Local mode is deliberately outside the S3 retry-delivery contract (ADR-0001):
``_read_manifest`` always returns ``None``, so every matched message is
downloaded and delivered on every run, files are overwritten without an
existence check, and dedup is possible only via ``mark_processed=True``. The
Gmail hook is a fake whose ``build_query`` delegates to the *real*
:meth:`GmailHook.build_query`, so query assertions test what the operator would
truly issue. No network, no real disk beyond ``tmp_path``.
"""

from __future__ import annotations

import inspect
import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_provider_gmail.hooks.gmail import GmailHook, MessageWithAttachments
from airflow_provider_gmail.dates import to_local_date
from airflow_provider_gmail.operators.gmail import GmailAttachmentsToLocalOperator
from airflow_provider_gmail.utils.mime import Attachment

MSK = "Europe/Moscow"
CURRENT_RUN = "scheduled__2026-07-12T06:00:00+00:00"


# -- fake hook ---------------------------------------------------------------


class FakeGmailHook:
    """Fake Gmail hook; ``build_query`` delegates to the real one to stay honest."""

    def __init__(self, messages):
        self._messages = list(messages)
        self._real = GmailHook("unused")
        self.window = None
        self.built_query = None
        self.exclude_label_id = "<unset>"
        self.find_label_id_calls: list[str] = []
        self.label_id_to_return: str | None = None
        self.labelled_ids: set[str] = set()
        self.downloaded: list[tuple[str, str]] = []
        self.marked: list[tuple[list[str], str]] = []

    def build_query(
        self,
        window,
        from_email,
        subject_contains,
        has_attachment,
        filename_contains,
        raw_query,
    ) -> str:
        self.window = window
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
            results.append(msg)
        return results

    def download_attachment(self, message_id, attachment) -> bytes:
        self.downloaded.append((message_id, attachment.filename))
        return b"bytes-of-" + attachment.filename.encode()

    def mark_processed(self, message_ids, label_name) -> None:
        self.marked.append((list(message_ids), label_name))


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


def _dt(msg: MessageWithAttachments) -> str:
    return to_local_date(msg.internal_date, MSK).isoformat()


def _make_op(path, **kwargs) -> GmailAttachmentsToLocalOperator:
    params = {"task_id": "t", "source": "avito", "path": str(path)}
    params.update(kwargs)
    return GmailAttachmentsToLocalOperator(**params)


def _context(run_id: str = CURRENT_RUN) -> dict:
    return {
        "run_id": run_id,
        "data_interval_end": datetime(2026, 7, 12, 6, 0, 0, tzinfo=ZoneInfo("UTC")),
    }


def _run(op, hook, context: dict | None = None):
    op.hook = hook
    return op.execute(context or _context())


def _msg_dir(path, msg) -> str:
    return os.path.join(str(path), f"dt={_dt(msg)}", msg.message_id)


# -- config ------------------------------------------------------------------


def test_template_fields_include_path():
    tf = set(GmailAttachmentsToLocalOperator.template_fields)
    assert "path" in tf
    assert {"query", "source", "date_from", "date_to"} <= tf


def test_default_lookback_days_is_zero():
    op = _make_op("/data/gmail/avito")
    assert op.lookback_days == 0
    # base default is 7; local overrides both the __init__ default and the
    # WARNING-gating class attr.
    assert GmailAttachmentsToLocalOperator.default_lookback_days == 0
    assert op.default_lookback_days == 0


def test_overwrite_attribute_false_but_not_in_public_signature():
    op = _make_op("/data/gmail/avito")
    # The inherited attribute must exist (base execute() references it) and be False.
    assert op.overwrite is False
    # ...but it must NOT be a public __init__ argument (not "no attribute").
    params = inspect.signature(GmailAttachmentsToLocalOperator.__init__).parameters
    assert "overwrite" not in params


def test_destination_path_is_absolute(tmp_path):
    op = _make_op(tmp_path)
    dest = op._destination_path("dt=2026-07-12/msg1/a.xlsx")
    assert os.path.isabs(dest)
    assert dest.endswith(os.path.join("dt=2026-07-12", "msg1", "a.xlsx"))


def test_xcom_path_equals_destination_path_absolute(tmp_path):
    # Local inherits the base _xcom_path default (→ _destination_path), so XCom
    # stays the absolute disk path — only S3 diverges to s3:// URIs (ADR-0007).
    op = _make_op(tmp_path)
    rel = "dt=2026-07-12/msg1/_manifest.json"
    assert op._xcom_path(rel) == op._destination_path(rel)
    assert os.path.isabs(op._xcom_path(rel))
    assert not op._xcom_path(rel).startswith("s3://")


def test_read_manifest_always_none(tmp_path):
    op = _make_op(tmp_path)
    assert op._read_manifest("dt=2026-07-12/msg1") is None


def test_filter_processed_label_follows_mark_processed(tmp_path):
    assert _make_op(tmp_path, mark_processed=False)._filter_processed_label() is False
    assert _make_op(tmp_path, mark_processed=True)._filter_processed_label() is True


# -- processed-label dedup (labelIds filter, not -label:) --------------------


def test_mark_processed_true_filters_labelled_message(tmp_path):
    # mark_processed=True → execute resolves the processed label id and drops a
    # message already carrying it (labelIds filter in code, no -label: term).
    from airflow.exceptions import AirflowSkipException

    msg = _message("msg1", "report.xlsx")
    op = _make_op(tmp_path, mark_processed=True)
    hook = FakeGmailHook([msg])
    hook.label_id_to_return = "Label_proc"
    hook.labelled_ids = {"msg1"}

    with pytest.raises(AirflowSkipException):  # the only message was filtered out
        _run(op, hook)
    assert hook.find_label_id_calls == ["airflow/processed"]
    assert hook.exclude_label_id == "Label_proc"
    assert hook.downloaded == []


def test_mark_processed_true_but_label_absent_does_not_filter(tmp_path):
    # find_label_id → None (label not in the mailbox) → exclude_label_id None, so
    # the message is delivered normally.
    msg = _message("msg1", "report.xlsx")
    op = _make_op(tmp_path, mark_processed=True)
    hook = FakeGmailHook([msg])
    hook.label_id_to_return = None

    result = _run(op, hook)
    assert hook.find_label_id_calls == ["airflow/processed"]
    assert hook.exclude_label_id is None
    assert hook.downloaded == [("msg1", "report.xlsx")]
    assert result == [os.path.join(_msg_dir(tmp_path, msg), "_manifest.json")]


def test_mark_processed_false_never_resolves_label(tmp_path):
    msg = _message("msg1", "report.xlsx")
    op = _make_op(tmp_path, mark_processed=False)
    hook = FakeGmailHook([msg])
    _run(op, hook)
    assert hook.find_label_id_calls == []
    assert hook.exclude_label_id is None


# -- write + manifest --------------------------------------------------------


def test_directory_structure_and_manifest_written(tmp_path):
    msg = _message("msg1", "a.xlsx")
    op = _make_op(tmp_path)
    hook = FakeGmailHook([msg])

    result = _run(op, hook)

    msg_dir = _msg_dir(tmp_path, msg)
    attachment = os.path.join(msg_dir, "a.xlsx")
    manifest = os.path.join(msg_dir, "_manifest.json")

    assert os.path.isfile(attachment)
    with open(attachment, "rb") as fh:
        assert fh.read() == b"bytes-of-a.xlsx"
    assert os.path.isfile(manifest)

    # XCom carries the absolute manifest path of this run's message.
    assert result == [manifest]
    assert os.path.isabs(result[0])

    # The manifest records the absolute on-disk path for the attachment.
    payload = json.loads(open(manifest).read())
    assert payload["run_id"] == CURRENT_RUN
    assert payload["files"][0]["name"] == "a.xlsx"
    assert payload["files"][0]["path"] == attachment
    assert os.path.isabs(payload["files"][0]["path"])


def test_rerun_redownloads_and_overwrites_existing_file(tmp_path):
    msg = _message("msg1", "a.xlsx")
    attachment = os.path.join(_msg_dir(tmp_path, msg), "a.xlsx")

    # A prior run's stale file sits on disk.
    os.makedirs(os.path.dirname(attachment), exist_ok=True)
    with open(attachment, "wb") as fh:
        fh.write(b"stale-bytes")

    op = _make_op(tmp_path)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)

    # _read_manifest is always None → always DOWNLOAD_AND_DELIVER: re-downloaded
    # and the existing file overwritten without an existence check.
    assert hook.downloaded == [("msg1", "a.xlsx")]
    with open(attachment, "rb") as fh:
        assert fh.read() == b"bytes-of-a.xlsx"
    assert result == [os.path.join(_msg_dir(tmp_path, msg), "_manifest.json")]


def test_path_traversal_attachment_written_inside_path(tmp_path):
    # An attacker-controlled name must not escape ``path``.
    base = tmp_path / "dest"
    outside_marker = tmp_path / "evil.xlsx"
    msg = _message("msg1", "../../../tmp/evil.xlsx")

    op = _make_op(base)
    hook = FakeGmailHook([msg])
    _run(op, hook)

    # Nothing was created outside ``path``.
    assert not outside_marker.exists()
    assert not (tmp_path / "tmp").exists()

    # The file landed inside path, under the message directory, as a basename.
    written = os.path.join(_msg_dir(base, msg), "evil.xlsx")
    assert os.path.isfile(written)
    # Everything written stays under the destination base.
    for root, _dirs, files in os.walk(base):
        for name in files:
            full = os.path.abspath(os.path.join(root, name))
            assert full.startswith(os.path.abspath(str(base)) + os.sep)
