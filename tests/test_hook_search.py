"""Tests for :class:`GmailHook` search and attachment download (Task 5).

No network: the ``googleapiclient`` service is replaced by a hand-written fake
(``FakeService``) that records the ``userId``/``q``/``id`` arguments and the
``num_retries`` every ``execute`` was called with, and returns preconfigured
JSON responses. ``hook._service`` is set directly so ``get_conn()`` never runs a
refresh grant, and ``hook.user_id`` is set on the instance (it is a
``cached_property``, so plain assignment wins).
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path

import pytest

from airflow_provider_gmail.hooks.gmail import (
    GmailHook,
    MessageWithAttachments,
    _b64url_decode,
)
from airflow_provider_gmail.utils.mime import Attachment

FIXTURES = Path(__file__).parent / "fixtures" / "gmail"


# --------------------------------------------------------------------------- #
# Fake googleapiclient service
# --------------------------------------------------------------------------- #
class Recorder:
    def __init__(self):
        # Configured responses.
        self.list_responses: list[dict] = []
        self.get_responses: dict[str, dict] = {}
        self.attachment_responses: dict[str, dict] = {}
        # Recorded calls.
        self.list_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.attachment_calls: list[dict] = []
        self.num_retries_seen: list[int] = []


class FakeExecute:
    def __init__(self, recorder: Recorder, response):
        self._recorder = recorder
        self._response = response

    def execute(self, num_retries=None):
        self._recorder.num_retries_seen.append(num_retries)
        return self._response


class FakeAttachments:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def get(self, userId, messageId, id):
        self._recorder.attachment_calls.append(
            {"userId": userId, "messageId": messageId, "id": id}
        )
        return FakeExecute(self._recorder, self._recorder.attachment_responses[id])


class FakeMessages:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def list(self, userId, q, pageToken):
        self._recorder.list_calls.append(
            {"userId": userId, "q": q, "pageToken": pageToken}
        )
        return FakeExecute(self._recorder, self._recorder.list_responses.pop(0))

    def get(self, userId, id, format):
        self._recorder.get_calls.append(
            {"userId": userId, "id": id, "format": format}
        )
        return FakeExecute(self._recorder, self._recorder.get_responses[id])

    def attachments(self):
        return FakeAttachments(self._recorder)


class FakeUsers:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def messages(self):
        return FakeMessages(self._recorder)


class FakeService:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def users(self):
        return FakeUsers(self._recorder)


def _hook(recorder: Recorder, *, num_retries: int = 3, user_id: str = "me") -> GmailHook:
    hook = GmailHook(num_retries=num_retries)
    hook._service = FakeService(recorder)  # get_conn() returns this, no refresh grant
    hook.user_id = user_id  # cached_property → instance assignment wins
    return hook


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _msg(msg_id, internal_date, subject, from_, parts) -> dict:
    return {
        "id": msg_id,
        "internalDate": str(internal_date),
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "Subject", "value": subject},
                {"name": "From", "value": from_},
            ],
            "parts": parts,
        },
    }


def _att_part(filename, attachment_id, mime="application/octet-stream") -> dict:
    return {
        "mimeType": mime,
        "filename": filename,
        "body": {"attachmentId": attachment_id, "size": 10},
    }


# --------------------------------------------------------------------------- #
# search()
# --------------------------------------------------------------------------- #
def test_search_follows_pagination_across_two_pages():
    rec = Recorder()
    rec.list_responses = [
        {"messages": [{"id": "m1"}, {"id": "m2"}], "nextPageToken": "TOK"},
        {"messages": [{"id": "m3"}]},
    ]
    hook = _hook(rec)

    assert hook.search("subject:x") == ["m1", "m2", "m3"]
    assert len(rec.list_calls) == 2
    # First page has no token, second page carries the token from page one.
    assert rec.list_calls[0]["pageToken"] is None
    assert rec.list_calls[1]["pageToken"] == "TOK"


def test_search_empty_result_without_messages_key():
    rec = Recorder()
    rec.list_responses = [{}]  # empty result: no "messages" key at all
    hook = _hook(rec)

    assert hook.search("subject:none") == []


# --------------------------------------------------------------------------- #
# download_attachment()
# --------------------------------------------------------------------------- #
def test_b64url_padding_fixup_and_urlsafe_alphabet():
    # Bytes whose base64url uses the '-'/'_' alphabet and needs padding restored.
    raw = b"\xfb\xef\xff\x00\x10"
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    assert "=" not in encoded
    assert _b64url_decode(encoded) == raw


def test_download_attachment_from_inline_data():
    rec = Recorder()
    hook = _hook(rec)
    # "a,b,c" as base64 with padding.
    attachment = Attachment(
        filename="small.csv",
        mime_type="text/csv",
        attachment_id=None,
        data="YSxiLGM=",
    )
    data = hook.download_attachment("mX", attachment)
    assert data == b"a,b,c"
    assert len(data) == 5
    # Inline data means no attachments.get() round-trip.
    assert rec.attachment_calls == []


def test_download_attachment_via_attachment_id():
    rec = Recorder()
    raw = b"hello world payload"
    body = base64.urlsafe_b64encode(raw).decode().rstrip("=")  # unpadded
    rec.attachment_responses = {"ATT_1": {"data": body, "size": len(raw)}}
    hook = _hook(rec)

    attachment = Attachment(
        filename="report.xlsx",
        mime_type="application/octet-stream",
        attachment_id="ATT_1",
        data=None,
    )
    data = hook.download_attachment("mX", attachment)
    assert data == raw
    assert len(data) == len(raw)
    assert rec.attachment_calls[0] == {
        "userId": "me",
        "messageId": "mX",
        "id": "ATT_1",
    }


def test_download_attachment_prefers_attachment_id_over_empty_data():
    # A part carrying BOTH a real attachmentId AND data == "" must fetch the real
    # bytes via attachments.get — NOT write a silent 0-byte file (data loss:
    # later runs would dedup it and never re-fetch).
    rec = Recorder()
    raw = b"the real payload bytes"
    body = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    rec.attachment_responses = {"ATT_1": {"data": body, "size": len(raw)}}
    hook = _hook(rec)

    attachment = Attachment(
        filename="report.xlsx",
        mime_type="application/octet-stream",
        attachment_id="ATT_1",
        data="",  # present-but-empty inline body alongside a real attachmentId
    )
    data = hook.download_attachment("mX", attachment)
    assert data == raw
    assert len(data) > 0  # real bytes, not a 0-byte file
    assert rec.attachment_calls[0] == {
        "userId": "me",
        "messageId": "mX",
        "id": "ATT_1",
    }


def test_download_attachment_empty_file_data():
    # An empty file arrives as data == "" (not None) and must decode to b"".
    rec = Recorder()
    hook = _hook(rec)
    attachment = Attachment(
        filename="empty.txt", mime_type="text/plain", attachment_id=None, data=""
    )
    assert hook.download_attachment("mX", attachment) == b""
    assert rec.attachment_calls == []


def test_download_attachment_no_source_raises():
    # Falsy attachment_id AND data is None: neither body source is usable. Fail
    # loudly instead of silently returning b"" (which a manifest would dedup
    # permanently — data loss).
    from airflow.exceptions import AirflowException

    rec = Recorder()
    hook = _hook(rec)
    attachment = Attachment(
        filename="ghost.pdf",
        mime_type="application/pdf",
        attachment_id="",
        data=None,
    )
    with pytest.raises(AirflowException, match="no usable body source"):
        hook.download_attachment("mX", attachment)
    assert rec.attachment_calls == []


def test_download_attachment_response_without_data_key_raises_key_error():
    # A malformed attachments.get response (no "data" key) fails loudly with a
    # KeyError rather than silently returning garbage.
    rec = Recorder()
    rec.attachment_responses = {"ATT_1": {"size": 0}}  # no "data"
    hook = _hook(rec)
    attachment = Attachment(
        filename="report.xlsx",
        mime_type="application/octet-stream",
        attachment_id="ATT_1",
        data=None,
    )
    with pytest.raises(KeyError):
        hook.download_attachment("mX", attachment)


# --------------------------------------------------------------------------- #
# find_messages_with_attachments()
# --------------------------------------------------------------------------- #
def test_pattern_none_matches_all_attachments():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hello",
            "sender@example.com",
            [_att_part("a.pdf", "att_a"), _att_part("b.png", "att_b")],
        )
    }
    hook = _hook(rec)

    results = hook.find_messages_with_attachments("q", None)
    assert len(results) == 1
    assert isinstance(results[0], MessageWithAttachments)
    assert [a.filename for a in results[0].attachments] == ["a.pdf", "b.png"]


def test_pattern_filters_attachments():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hello",
            "sender@example.com",
            [
                _att_part("report.xlsx", "att_x"),
                _att_part("logo.png", "att_p"),
            ],
        )
    }
    hook = _hook(rec)

    results = hook.find_messages_with_attachments("q", r"\.xlsx$")
    assert len(results) == 1
    assert [a.filename for a in results[0].attachments] == ["report.xlsx"]


def test_body_data_attachment_without_attachment_id():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "bodydata01"}]}]
    rec.get_responses = {"bodydata01": _fixture("body_data.json")}
    hook = _hook(rec)

    results = hook.find_messages_with_attachments("q", None)
    assert len(results) == 1
    att = results[0].attachments[0]
    assert att.filename == "small.csv"
    assert att.attachment_id is None
    assert att.data == "YSxiLGM="
    # Downloadable straight from inline data, no attachments.get().
    assert hook.download_attachment("bodydata01", att) == b"a,b,c"


def test_cyrillic_subject_and_from_decoded():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "18c2f4a9b3d5e6f7"}]}]
    rec.get_responses = {"18c2f4a9b3d5e6f7": _fixture("cyrillic_filename.json")}
    hook = _hook(rec)

    results = hook.find_messages_with_attachments("q", None)
    assert len(results) == 1
    msg = results[0]
    assert msg.subject == "Отчёт за 09.07"
    assert msg.from_ == "Отдел отчётов <reports@avito.ru>"
    assert msg.internal_date == 1752041662000
    assert [a.filename for a in msg.attachments] == ["Отчёт за июль.xlsx"]


def test_missing_subject_and_from_headers_coalesce_to_empty_string():
    # A message without Subject/From headers must yield "" (not None) so the
    # manifest serializes strings, matching the layer-2 schema (never JSON null).
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    message = {
        "id": "m1",
        "internalDate": "1752000000000",
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [],  # no Subject, no From
            "parts": [_att_part("a.pdf", "att_a")],
        },
    }
    rec.get_responses = {"m1": message}
    hook = _hook(rec)

    results = hook.find_messages_with_attachments("q", None)
    assert len(results) == 1
    assert results[0].subject == ""
    assert results[0].from_ == ""
    # And it round-trips through the manifest as strings, not JSON null.
    from airflow_provider_gmail.manifest import Manifest

    payload = json.loads(
        Manifest.build(
            "avito", "m1", "2026-07-08T00:00:00", results[0].subject,
            results[0].from_, [], "run1",
        ).to_json()
    )
    assert payload["subject"] == "" and payload["from"] == ""


def test_not_our_email_logged_at_info_with_names_and_pattern(caplog):
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hello",
            "sender@example.com",
            [_att_part("a.pdf", "att_a"), _att_part("b.png", "att_b")],
        )
    }
    hook = _hook(rec)

    with caplog.at_level(logging.INFO):
        results = hook.find_messages_with_attachments("q", r"\.xlsx$")

    assert results == []
    text = caplog.text
    assert "a.pdf" in text
    assert "b.png" in text
    assert r"\.xlsx$" in text


# --------------------------------------------------------------------------- #
# userId propagation
# --------------------------------------------------------------------------- #
def test_user_id_passed_to_all_calls_default_me():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hi",
            "s@example.com",
            [_att_part("r.xlsx", "ATT")],
        )
    }
    rec.attachment_responses = {"ATT": {"data": "YSxiLGM=", "size": 5}}
    hook = _hook(rec, user_id="me")

    results = hook.find_messages_with_attachments("q", None)
    hook.download_attachment("m1", results[0].attachments[0])

    assert rec.list_calls[0]["userId"] == "me"
    assert rec.get_calls[0]["userId"] == "me"
    assert rec.attachment_calls[0]["userId"] == "me"


def test_user_id_custom_address_passed_everywhere():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hi",
            "s@example.com",
            [_att_part("r.xlsx", "ATT")],
        )
    }
    rec.attachment_responses = {"ATT": {"data": "YSxiLGM=", "size": 5}}
    hook = _hook(rec, user_id="team@example.com")

    results = hook.find_messages_with_attachments("q", None)
    hook.download_attachment("m1", results[0].attachments[0])

    assert rec.list_calls[0]["userId"] == "team@example.com"
    assert rec.get_calls[0]["userId"] == "team@example.com"
    assert rec.attachment_calls[0]["userId"] == "team@example.com"


# --------------------------------------------------------------------------- #
# retry
# --------------------------------------------------------------------------- #
def test_execute_uses_configured_num_retries():
    rec = Recorder()
    rec.list_responses = [{"messages": [{"id": "m1"}]}]
    rec.get_responses = {
        "m1": _msg(
            "m1",
            1752000000000,
            "hi",
            "s@example.com",
            [_att_part("r.xlsx", "ATT")],
        )
    }
    rec.attachment_responses = {"ATT": {"data": "YSxiLGM=", "size": 5}}
    hook = _hook(rec, num_retries=7)

    results = hook.find_messages_with_attachments("q", None)
    hook.download_attachment("m1", results[0].attachments[0])

    # Every API call went through execute(num_retries=7): list + get + attachment.
    assert rec.num_retries_seen == [7, 7, 7]
    assert all(n == 7 for n in rec.num_retries_seen)
