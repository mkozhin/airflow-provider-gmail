"""Tests for :class:`GmailAttachmentsToS3Operator` (Task 9).

The S3 correctness contract (ADR-0001) is exercised with an **in-memory fake
S3Hook** (no network): a message deduplicates by manifest + ``run_id`` alone, the
label never filters the search, and a run that marked-but-did-not-return is still
found and delivered on retry. The Gmail hook is a fake whose ``build_query``
delegates to the *real* :meth:`GmailHook.build_query`, so the "no ``-label:`` in
S3" assertions test the query the operator would truly issue.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow.exceptions import AirflowSkipException

from airflow_provider_gmail.hooks.gmail import GmailHook, MessageWithAttachments
from airflow_provider_gmail.manifest import Manifest, ManifestError
from airflow_provider_gmail.operators.gmail import (
    GmailAttachmentsToS3Operator,
    to_local_date,
    to_local_iso,
)
from airflow_provider_gmail.utils.mime import Attachment
from airflow_provider_gmail.utils.paths import manifest_key, s3_object_key

MSK = "Europe/Moscow"
BUCKET = "my-bucket"
PREFIX = "gmail/avito"
CURRENT_RUN = "scheduled__2026-07-12T06:00:00+00:00"
PAST_RUN = "scheduled__2026-07-11T06:00:00+00:00"


# -- fakes -------------------------------------------------------------------


class FakeS3Hook:
    """In-memory stand-in for ``S3Hook``: a ``dict`` store plus call recording.

    ``read_key`` returns ``str`` (decoded), exactly as the real hook does, so a
    test can never hide a ``str``/``bytes`` mismatch with ``Manifest.from_json``.
    """

    def __init__(self, store: dict | None = None, aws_conn_id: str | None = None):
        self.store: dict[str, bytes] = {} if store is None else store
        self.aws_conn_id = aws_conn_id
        self.load_bytes_calls: list[dict] = []

    def load_bytes(self, bytes_data, key, bucket_name=None, replace=False, **kwargs):
        self.load_bytes_calls.append(
            {"key": key, "bucket_name": bucket_name, "replace": replace}
        )
        self.store[key] = bytes_data

    def check_for_key(self, key, bucket_name=None) -> bool:
        return key in self.store

    def read_key(self, key, bucket_name=None) -> str:
        return self.store[key].decode("utf-8")


class FakeGmailHook:
    """Fake Gmail hook; ``build_query`` delegates to the real one to keep the query honest."""

    def __init__(self, messages, mark_error: Exception | None = None):
        self._messages = list(messages)
        self.mark_error = mark_error
        self._real = GmailHook("unused")
        self.window = None
        self.filter_processed_label = None
        self.built_query = None
        self.downloaded: list[tuple[str, str]] = []
        self.marked: list[tuple[list[str], str]] = []

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
        self.window = window
        self.filter_processed_label = filter_processed_label
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
        return list(self._messages)

    def download_attachment(self, message_id, attachment) -> bytes:
        self.downloaded.append((message_id, attachment.filename))
        return b"bytes-of-" + attachment.filename.encode()

    def mark_processed(self, message_ids, label_name) -> None:
        self.marked.append((list(message_ids), label_name))
        if self.mark_error is not None:
            raise self.mark_error


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


def _manifest_key(msg: MessageWithAttachments, prefix: str = PREFIX) -> str:
    return manifest_key(prefix, _dt(msg), msg.message_id)


def _file_key(msg, filename, prefix: str = PREFIX) -> str:
    return s3_object_key(prefix, _dt(msg), msg.message_id, filename)


def _seed_manifest(store: dict, msg, run_id: str, prefix: str = PREFIX) -> None:
    manifest = Manifest.build(
        "avito", msg.message_id, to_local_iso(msg.internal_date, MSK),
        msg.subject, msg.from_, [], run_id,
    )
    store[_manifest_key(msg, prefix)] = manifest.to_json()


def _make_op(store: dict, prefix: str = PREFIX, **kwargs) -> GmailAttachmentsToS3Operator:
    params = {"task_id": "t", "source": "avito", "bucket": BUCKET, "prefix": prefix}
    params.update(kwargs)
    op = GmailAttachmentsToS3Operator(**params)
    op._cached_s3_hook = FakeS3Hook(store, aws_conn_id="aws_default")
    return op


def _context(run_id: str = CURRENT_RUN) -> dict:
    return {
        "run_id": run_id,
        "data_interval_end": datetime(2026, 7, 12, 6, 0, 0, tzinfo=ZoneInfo("UTC")),
    }


def _run(op, hook, context: dict | None = None):
    op.hook = hook
    return op.execute(context or _context())


# -- template_fields / basic config ------------------------------------------


def test_template_fields_include_bucket_and_prefix():
    tf = set(GmailAttachmentsToS3Operator.template_fields)
    assert {"bucket", "prefix"} <= tf
    assert {"query", "source", "date_from", "date_to"} <= tf


def test_module_imports_without_reference_to_s3hook_at_top_level():
    # The Amazon provider must be imported lazily: constructing the operator must
    # not require touching S3Hook (only _write/_read_manifest do).
    op = GmailAttachmentsToS3Operator(
        task_id="t", source="avito", bucket=BUCKET, prefix=PREFIX
    )
    assert op._cached_s3_hook is None


# -- dedup tri-state ---------------------------------------------------------


def test_past_run_manifest_not_downloaded_not_delivered():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, PAST_RUN)
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    assert hook.downloaded == []


def test_current_run_manifest_delivered_not_downloaded():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, CURRENT_RUN)
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)
    assert result == [_manifest_key(msg)]
    assert hook.downloaded == []


def test_no_manifest_downloads_and_delivers_with_expected_key_structure():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)

    assert result == [_manifest_key(msg)]
    assert hook.downloaded == [("msg1", "a.xlsx")]
    # keys have the <prefix>/dt=<dt>/<message_id>/... structure
    assert _file_key(msg, "a.xlsx") in store
    assert _file_key(msg, "a.xlsx") == "gmail/avito/dt=2026-07-12/msg1/a.xlsx"
    assert _manifest_key(msg) in store


def test_overwrite_true_forces_download_despite_current_manifest():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, CURRENT_RUN)
    op = _make_op(store, overwrite=True)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)
    assert result == [_manifest_key(msg)]
    assert hook.downloaded == [("msg1", "a.xlsx")]  # overwrite → re-download


def test_empty_prefix_produces_no_leading_slash_keys():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store, prefix="")
    hook = FakeGmailHook([msg])
    _run(op, hook)
    for key in store:
        assert not key.startswith("/")
    assert "dt=2026-07-12/msg1/a.xlsx" in store


# -- label never filters the S3 search (ADR-0001) ---------------------------


def test_mark_processed_true_no_label_in_query():
    store: dict = {}
    op = _make_op(store, mark_processed=True)
    hook = FakeGmailHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    assert hook.filter_processed_label is False
    assert "-label:" not in hook.built_query


def test_filter_processed_label_is_always_false():
    op = _make_op({}, mark_processed=True)
    assert op._filter_processed_label() is False


# -- CRITICAL retry-delivery (ADR-0001) --------------------------------------


def test_retry_after_marked_but_not_returned_still_delivers():
    # (a) Attempt 1 with mark_processed=True writes the manifest and sets the
    # label, then the task "crashes before returning" (its XCom never lands).
    store: dict = {}
    msg = _message("msg1", "a.xlsx")

    op1 = _make_op(store, mark_processed=True)
    hook1 = FakeGmailHook([msg])
    _run(op1, hook1)
    assert hook1.marked == [(["msg1"], "airflow/processed")]  # label set
    assert "-label:" not in hook1.built_query  # yet search is NOT label-filtered

    # Retry: same run_id, same store. The message is still found (no -label:),
    # its current-run manifest → DELIVER_ONLY → path in XCom, no re-download.
    op2 = _make_op(store, mark_processed=True)
    hook2 = FakeGmailHook([msg])
    result = _run(op2, hook2)
    assert result == [_manifest_key(msg)]
    assert hook2.downloaded == []


def test_partial_batch_modify_retry_delivers_all_current_run():
    # (b) Attempt 1 downloads both messages, writes both manifests, then the
    # batchModify (label) fails partway → task fails. On retry both current-run
    # manifests are delivered; nothing is lost.
    store: dict = {}
    msg1, msg2 = _message("msg1", "a.xlsx"), _message("msg2", "b.xlsx")

    op1 = _make_op(store, mark_processed=True)
    hook1 = FakeGmailHook([msg1, msg2], mark_error=RuntimeError("batchModify failed"))
    with pytest.raises(RuntimeError):
        _run(op1, hook1)
    assert _manifest_key(msg1) in store and _manifest_key(msg2) in store

    op2 = _make_op(store, mark_processed=True)
    hook2 = FakeGmailHook([msg1, msg2])
    result = _run(op2, hook2)
    assert result == [_manifest_key(msg1), _manifest_key(msg2)]
    assert hook2.downloaded == []  # both delivered from current-run manifests


def test_retry_without_labels_behaves_the_same():
    # (c) No labels at all → identical delivery behavior.
    store: dict = {}
    msg = _message("msg1", "a.xlsx")

    op1 = _make_op(store, mark_processed=False)
    hook1 = FakeGmailHook([msg])
    _run(op1, hook1)
    assert hook1.marked == []
    assert "-label:" not in hook1.built_query

    op2 = _make_op(store, mark_processed=False)
    hook2 = FakeGmailHook([msg])
    result = _run(op2, hook2)
    assert result == [_manifest_key(msg)]
    assert hook2.downloaded == []


# -- write always replace=True -----------------------------------------------


def test_load_bytes_called_with_replace_true():
    store: dict = {}
    msg = _message("msg1", "a.xlsx", "b.xlsx")
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    _run(op, hook)
    s3 = op._cached_s3_hook
    assert s3.load_bytes_calls  # something was written
    assert all(call["replace"] is True for call in s3.load_bytes_calls)
    assert all(call["bucket_name"] == BUCKET for call in s3.load_bytes_calls)


# -- failure path: file present, manifest absent -----------------------------


def test_existing_file_without_manifest_reruns_overwrites_no_crash():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    # A prior partial attempt left the attachment but no manifest.
    store[_file_key(msg, "a.xlsx")] = b"stale-bytes"
    op = _make_op(store)
    hook = FakeGmailHook([msg])

    result = _run(op, hook)  # must overwrite + write manifest, not crash

    assert result == [_manifest_key(msg)]
    assert store[_file_key(msg, "a.xlsx")] == b"bytes-of-a.xlsx"  # overwritten
    assert _manifest_key(msg) in store


# -- _read_manifest delegates to Manifest.from_json with str body ------------


def test_read_manifest_delegates_to_from_json_with_str_body(monkeypatch):
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, CURRENT_RUN)
    op = _make_op(store)

    captured: dict = {}
    original = Manifest.from_json

    def _spy(raw):
        captured["type"] = type(raw)
        return original(raw)

    monkeypatch.setattr(Manifest, "from_json", staticmethod(_spy))

    rel_dir = f"dt={_dt(msg)}/{msg.message_id}"
    manifest = op._read_manifest(rel_dir)
    assert manifest is not None
    assert manifest.run_id == CURRENT_RUN
    assert captured["type"] is str  # read_key returned str, not bytes


def test_read_manifest_absent_returns_none():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store)
    rel_dir = f"dt={_dt(msg)}/{msg.message_id}"
    assert op._read_manifest(rel_dir) is None


# -- corrupt manifest + overwrite recovery -----------------------------------


def test_corrupt_manifest_overwrite_true_redownloads_no_manifest_error():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    store[_manifest_key(msg)] = b"{ this is not valid json"
    op = _make_op(store, overwrite=True)
    hook = FakeGmailHook([msg])

    result = _run(op, hook)  # overwrite → manifest not read → no ManifestError

    assert result == [_manifest_key(msg)]
    assert hook.downloaded == [("msg1", "a.xlsx")]
    # the corrupt manifest was overwritten by a valid one
    assert Manifest.from_json(store[_manifest_key(msg)]).run_id == CURRENT_RUN


def test_corrupt_manifest_overwrite_false_raises_manifest_error():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    store[_manifest_key(msg)] = b"{ this is not valid json"
    op = _make_op(store, overwrite=False)
    hook = FakeGmailHook([msg])
    with pytest.raises(ManifestError):
        _run(op, hook)
