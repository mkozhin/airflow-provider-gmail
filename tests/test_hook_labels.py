"""Tests for :class:`GmailHook` label handling (Task 6).

No network: the ``googleapiclient`` service is replaced by a hand-written fake
that records ``labels.list``/``labels.create``/``messages.batchModify`` calls
and returns preconfigured responses (or raises a 403 ``HttpError``).
``hook._service`` is set directly so ``get_conn()`` never runs a refresh grant,
and ``hook.user_id`` is set on the instance (it is a ``cached_property``, so
plain assignment wins).
"""

from __future__ import annotations

import json

import pytest
from googleapiclient.errors import HttpError

from airflow_provider_gmail.hooks.gmail import GmailHook, GmailPermissionError


# --------------------------------------------------------------------------- #
# Fake googleapiclient service
# --------------------------------------------------------------------------- #
class FakeResp:
    def __init__(self, status: int):
        self.status = status
        self.reason = "Forbidden"


def _http_error(status: int, reason: str) -> HttpError:
    content = json.dumps(
        {"error": {"code": status, "errors": [{"reason": reason, "message": reason}]}}
    ).encode()
    return HttpError(FakeResp(status), content)


class Recorder:
    def __init__(self):
        # Configured behaviour.
        self.existing_labels: list[dict] = []
        self.create_response: dict = {"id": "Label_created"}
        self.create_error: HttpError | None = None
        self.batch_error: HttpError | None = None
        # Recorded calls.
        self.list_calls: list[dict] = []
        self.create_calls: list[dict] = []
        self.batch_calls: list[dict] = []
        self.num_retries_seen: list[int] = []


class FakeExecute:
    def __init__(self, recorder: Recorder, response, error):
        self._recorder = recorder
        self._response = response
        self._error = error

    def execute(self, num_retries=None):
        self._recorder.num_retries_seen.append(num_retries)
        if self._error is not None:
            raise self._error
        return self._response


class FakeLabels:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def list(self, userId):
        self._recorder.list_calls.append({"userId": userId})
        return FakeExecute(
            self._recorder, {"labels": self._recorder.existing_labels}, None
        )

    def create(self, userId, body):
        self._recorder.create_calls.append({"userId": userId, "body": body})
        return FakeExecute(
            self._recorder,
            self._recorder.create_response,
            self._recorder.create_error,
        )


class FakeMessages:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def batchModify(self, userId, body):
        self._recorder.batch_calls.append({"userId": userId, "body": body})
        return FakeExecute(self._recorder, "", self._recorder.batch_error)


class FakeUsers:
    def __init__(self, recorder: Recorder):
        self._recorder = recorder

    def labels(self):
        return FakeLabels(self._recorder)

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


# --------------------------------------------------------------------------- #
# get_or_create_label()
# --------------------------------------------------------------------------- #
def test_existing_label_is_not_created():
    rec = Recorder()
    rec.existing_labels = [
        {"id": "Label_1", "name": "INBOX"},
        {"id": "Label_42", "name": "airflow/processed"},
    ]
    hook = _hook(rec)

    assert hook.get_or_create_label("airflow/processed") == "Label_42"
    assert len(rec.list_calls) == 1
    assert rec.create_calls == []  # found → create never called


def test_missing_label_is_created():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    rec.create_response = {"id": "Label_new"}
    hook = _hook(rec)

    assert hook.get_or_create_label("airflow/processed") == "Label_new"
    assert len(rec.create_calls) == 1
    assert rec.create_calls[0]["body"] == {"name": "airflow/processed"}


def test_deeply_nested_label_created_with_full_name_no_parent():
    # None of the ancestors (airflow, airflow/processed) exist: a single create
    # with the full slashed name, no parent field.
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    rec.create_response = {"id": "Label_avito"}
    hook = _hook(rec)

    assert hook.get_or_create_label("airflow/processed/avito") == "Label_avito"
    assert len(rec.create_calls) == 1
    body = rec.create_calls[0]["body"]
    assert body == {"name": "airflow/processed/avito"}
    assert "parent" not in body
    assert "parentId" not in body


def test_label_result_is_cached():
    # Repeat call for the same name must not touch labels.list/labels.create again.
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    rec.create_response = {"id": "Label_new"}
    hook = _hook(rec)

    first = hook.get_or_create_label("airflow/processed")
    second = hook.get_or_create_label("airflow/processed")

    assert first == second == "Label_new"
    assert len(rec.list_calls) == 1  # not called a second time
    assert len(rec.create_calls) == 1  # not called a second time


def test_userid_passed_to_label_calls():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    hook = _hook(rec, user_id="team@example.com")

    hook.get_or_create_label("airflow/processed")
    assert rec.list_calls[0]["userId"] == "team@example.com"
    assert rec.create_calls[0]["userId"] == "team@example.com"


# --------------------------------------------------------------------------- #
# mark_processed()
# --------------------------------------------------------------------------- #
def test_mark_processed_chunks_1500_ids_into_two_batches():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_42", "name": "airflow/processed"}]
    hook = _hook(rec)

    ids = [f"m{i}" for i in range(1500)]
    hook.mark_processed(ids, "airflow/processed")

    assert len(rec.batch_calls) == 2
    assert len(rec.batch_calls[0]["body"]["ids"]) == 1000
    assert len(rec.batch_calls[1]["body"]["ids"]) == 500
    # No id lost or duplicated across the chunks.
    assert rec.batch_calls[0]["body"]["ids"] + rec.batch_calls[1]["body"]["ids"] == ids
    # Resolved labelId is attached, not the name.
    assert rec.batch_calls[0]["body"]["addLabelIds"] == ["Label_42"]


def test_mark_processed_resolves_label_and_uses_it():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    rec.create_response = {"id": "Label_new"}
    hook = _hook(rec)

    hook.mark_processed(["m1", "m2"], "airflow/processed")

    assert len(rec.batch_calls) == 1
    assert rec.batch_calls[0]["body"]["ids"] == ["m1", "m2"]
    assert rec.batch_calls[0]["body"]["addLabelIds"] == ["Label_new"]


def test_mark_processed_empty_ids_is_noop():
    rec = Recorder()
    hook = _hook(rec)

    hook.mark_processed([], "airflow/processed")
    assert rec.list_calls == []
    assert rec.batch_calls == []


def test_mark_processed_uses_retry():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_42", "name": "airflow/processed"}]
    hook = _hook(rec, num_retries=7)

    hook.mark_processed(["m1"], "airflow/processed")
    # labels.list + batchModify both went through execute(num_retries=7).
    assert all(n == 7 for n in rec.num_retries_seen)


# --------------------------------------------------------------------------- #
# 403 insufficientPermissions
# --------------------------------------------------------------------------- #
def test_batch_modify_403_raises_permission_error():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_42", "name": "airflow/processed"}]
    rec.batch_error = _http_error(403, "insufficientPermissions")
    hook = _hook(rec)

    with pytest.raises(GmailPermissionError) as exc_info:
        hook.mark_processed(["m1"], "airflow/processed")

    assert "gmail.modify" in str(exc_info.value)


def test_label_create_403_raises_permission_error():
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_1", "name": "INBOX"}]
    rec.create_error = _http_error(403, "insufficientPermissions")
    hook = _hook(rec)

    with pytest.raises(GmailPermissionError):
        hook.get_or_create_label("airflow/processed")


def test_other_403_is_not_swallowed():
    # A 403 that is not insufficientPermissions must propagate as-is (HttpError).
    rec = Recorder()
    rec.existing_labels = [{"id": "Label_42", "name": "airflow/processed"}]
    rec.batch_error = _http_error(403, "rateLimitExceeded")
    hook = _hook(rec)

    with pytest.raises(HttpError):
        hook.mark_processed(["m1"], "airflow/processed")
