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
from airflow.models.base import _sentinel

from airflow_provider_gmail.hooks.gmail import GmailHook, MessageWithAttachments
from airflow_provider_gmail.manifest import Manifest, ManifestError
from airflow_provider_gmail.dates import to_local_date, to_local_iso
from airflow_provider_gmail.operators.gmail import GmailAttachmentsToS3Operator
from airflow_provider_gmail.utils.mime import Attachment
from airflow_provider_gmail.utils.paths import join_key, manifest_key, message_dir

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
        self.built_query = None
        self.exclude_label_id = "<unset>"
        self.find_label_id_calls: list[str] = []
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
        # The S3 policy is False, so execute() must never call this. Record any
        # call so a regression (S3 resolving a processed label) is caught.
        self.find_label_id_calls.append(name)
        return "Label_should_not_be_used"

    def find_messages_with_attachments(self, query, pattern, exclude_label_id=None):
        self.exclude_label_id = exclude_label_id
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


def _manifest_uri(msg: MessageWithAttachments, prefix: str = PREFIX) -> str:
    """The full ``s3://<bucket>/<key>`` URI the operator now returns in XCom (ADR-0007)."""
    return f"s3://{BUCKET}/{_manifest_key(msg, prefix)}"


def _file_key(msg, filename, prefix: str = PREFIX) -> str:
    return join_key(message_dir(prefix, _dt(msg), msg.message_id), filename)


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
    ctx = context or _context()
    # Mirror the TaskInstance lifecycle order (pre_execute → execute) so the
    # rendered-prefix validation (now in pre_execute) runs for direct-call tests.
    op.pre_execute(ctx)
    return op.execute(ctx)


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


def test_s3_hook_lazily_constructs_real_hook_with_aws_conn_id():
    # Exercise the real lazy import path (not just the None cache): _s3_hook()
    # must build a real S3Hook bound to the configured aws_conn_id.
    op = GmailAttachmentsToS3Operator(
        task_id="t", source="avito", bucket=BUCKET, aws_conn_id="custom_aws"
    )
    hook = op._s3_hook()
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    assert isinstance(hook, S3Hook)
    assert hook.aws_conn_id == "custom_aws"
    assert op._s3_hook() is hook  # cached


# -- prefix validation (rendered value, ADR-0007) ----------------------------


def test_templated_prefix_does_not_fail_at_construction():
    # A Jinja prefix contains { } (both forbidden in a key), so validation must
    # NOT run in __init__ — only on the rendered value in pre_execute().
    op = GmailAttachmentsToS3Operator(
        task_id="t", source="avito", bucket=BUCKET, prefix="{{ ds }}"
    )
    assert op.prefix == "{{ ds }}"


def test_pre_execute_invalid_rendered_prefix_raises():
    op = _make_op({}, prefix="gmail/a#b")
    hook = FakeGmailHook([_message("msg1", "a.xlsx")])
    with pytest.raises(ValueError, match="prefix"):
        _run(op, hook)


def test_pre_execute_valid_prefix_passes():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store, prefix="gmail/avito")
    hook = FakeGmailHook([msg])
    result = _run(op, hook)
    assert result == [_manifest_uri(msg)]


def test_user_pre_execute_hook_runs_before_prefix_validation():
    # The override's super().pre_execute() must still invoke a user-supplied
    # pre_execute= hook, and it must run BEFORE validate_prefix: a spy hook fires
    # even though a hostile prefix makes validation raise right after.
    calls: list[str] = []
    op = _make_op(
        {}, prefix="gmail/a#b", pre_execute=lambda context: calls.append("hook")
    )
    op.hook = FakeGmailHook([_message("msg1", "a.xlsx")])
    with pytest.raises(ValueError, match="prefix"):
        op.pre_execute(_context())
    assert calls == ["hook"]  # hook ran (via super) before the ValueError


# -- ExecutorSafeguard: no spurious WARNING when called like TaskInstance -----


def test_execute_logs_no_warning_when_invoked_with_sentinel(caplog):
    # The whole point of the fix: dropping the execute() override that called
    # super().execute() removes the nested call that tripped ExecutorSafeguard.
    # The safeguard suppresses its WARNING only when the sentinel kwarg is present
    # (as TaskInstance passes it to the OUTER execute); a nested call loses it.
    import logging

    msg = _message("msg1", "a.xlsx")
    ctx = _context()

    # (a) negative control: a bare execute() WITHOUT the sentinel logs the WARNING.
    # Proves the safeguard is active and caplog sees it (and unit_test_mode=False;
    # under True the check is skipped and this branch would silently pass empty).
    op_a = _make_op({})
    op_a.hook = FakeGmailHook([msg])
    with caplog.at_level(logging.WARNING):
        op_a.pre_execute(ctx)
        op_a.execute(ctx)
    assert "cannot be called outside TaskInstance!" in caplog.text
    caplog.clear()  # MANDATORY: caplog.records accumulate across branches

    # (b) as TaskInstance does: pass the sentinel to execute() → no WARNING.
    op_b = _make_op({})
    op_b.hook = FakeGmailHook([msg])
    sentinel_kw = {f"{type(op_b).__name__}__sentinel": _sentinel}
    with caplog.at_level(logging.WARNING):
        op_b.pre_execute(ctx)
        op_b.execute(ctx, **sentinel_kw)
    assert "cannot be called outside TaskInstance!" not in caplog.text


# -- XCom URI vs manifest key divergence (ADR-0007) --------------------------


def test_xcom_uri_but_internal_ops_and_manifest_use_bare_keys():
    # XCom carries s3://bucket/key URIs, while the S3 writes, the object store
    # keys, and the manifest's files[].path all stay bare object keys.
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)

    # XCom → URI
    assert result == [_manifest_uri(msg)]
    assert result[0].startswith("s3://")
    # object store keyed by bare keys (no scheme)
    assert _file_key(msg, "a.xlsx") in store
    assert _manifest_key(msg) in store
    assert not any(k.startswith("s3://") for k in store)
    # manifest files[].path → bare key, not a URI
    manifest = Manifest.from_json(store[_manifest_key(msg)])
    assert [f.path for f in manifest.files] == [_file_key(msg, "a.xlsx")]


def test_xcom_path_builds_uri_from_bucket_and_destination_key():
    # _xcom_path is exactly the bucket-anchored form of _destination_path.
    op = _make_op({}, prefix="gmail/avito")
    rel = "dt=2026-07-12/msg1/_manifest.json"
    assert op._xcom_path(rel) == f"s3://{BUCKET}/{op._destination_path(rel)}"


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
    assert result == [_manifest_uri(msg)]
    assert hook.downloaded == []


def test_no_manifest_downloads_and_delivers_with_expected_key_structure():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    op = _make_op(store)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)

    assert result == [_manifest_uri(msg)]
    assert hook.downloaded == [("msg1", "a.xlsx")]
    # keys have the <prefix>/dt=<dt>/<message_id>/... structure
    assert _file_key(msg, "a.xlsx") in store
    assert _file_key(msg, "a.xlsx") == "gmail/avito/dt=2026-07-12/msg1/a.xlsx"
    assert _manifest_key(msg) in store
    # the manifest's files[].path is exactly the attachment's object key
    manifest = Manifest.from_json(store[_manifest_key(msg)])
    assert [f.path for f in manifest.files] == [_file_key(msg, "a.xlsx")]


def test_empty_attachment_yields_zero_byte_object_and_size_zero_manifest():
    # data == "" is a legitimate 0-byte attachment: it must produce a 0-byte S3
    # object and a manifest entry with size 0, not be skipped.
    store: dict = {}
    msg = _message("msg1", "empty.txt")

    class _EmptyHook(FakeGmailHook):
        def download_attachment(self, message_id, attachment) -> bytes:
            self.downloaded.append((message_id, attachment.filename))
            return b""

    op = _make_op(store)
    hook = _EmptyHook([msg])
    result = _run(op, hook)

    assert result == [_manifest_uri(msg)]
    file_key = _file_key(msg, "empty.txt")
    assert store[file_key] == b""  # 0-byte object written
    manifest = Manifest.from_json(store[_manifest_key(msg)])
    assert [(f.name, f.size) for f in manifest.files] == [("empty.txt", 0)]


def test_overwrite_true_forces_download_despite_current_manifest():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, CURRENT_RUN)
    op = _make_op(store, overwrite=True)
    hook = FakeGmailHook([msg])
    result = _run(op, hook)
    assert result == [_manifest_uri(msg)]
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


def test_mark_processed_true_never_resolves_or_filters_by_label():
    store: dict = {}
    op = _make_op(store, mark_processed=True)
    hook = FakeGmailHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    # S3 policy is False: no label id is resolved and no exclude filter applied.
    assert hook.find_label_id_calls == []
    assert hook.exclude_label_id is None


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
    # yet the search is NOT label-filtered: no id resolved, no exclude applied.
    assert hook1.find_label_id_calls == []
    assert hook1.exclude_label_id is None

    # Retry: same run_id, same store. The message is still found (not filtered by
    # label), its current-run manifest → DELIVER_ONLY → path in XCom, no re-download.
    op2 = _make_op(store, mark_processed=True)
    hook2 = FakeGmailHook([msg])
    result = _run(op2, hook2)
    assert result == [_manifest_uri(msg)]
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
    assert result == [_manifest_uri(msg1), _manifest_uri(msg2)]
    assert hook2.downloaded == []  # both delivered from current-run manifests


def test_retry_without_labels_behaves_the_same():
    # (c) No labels at all → identical delivery behavior.
    store: dict = {}
    msg = _message("msg1", "a.xlsx")

    op1 = _make_op(store, mark_processed=False)
    hook1 = FakeGmailHook([msg])
    _run(op1, hook1)
    assert hook1.marked == []
    assert hook1.exclude_label_id is None

    op2 = _make_op(store, mark_processed=False)
    hook2 = FakeGmailHook([msg])
    result = _run(op2, hook2)
    assert result == [_manifest_uri(msg)]
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

    assert result == [_manifest_uri(msg)]
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

    assert result == [_manifest_uri(msg)]
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


def test_non_utf8_manifest_raises_manifest_error():
    # A non-UTF-8 body makes read_key's .decode("utf-8") raise UnicodeDecodeError
    # after check_for_key; Manifest.from_s3 wraps it into ManifestError instead of
    # letting a bare UnicodeDecodeError leak out of execute().
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    store[_manifest_key(msg)] = b"\xff"
    op = _make_op(store, overwrite=False)
    hook = FakeGmailHook([msg])
    with pytest.raises(ManifestError):
        _run(op, hook)
