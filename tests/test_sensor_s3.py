"""Tests for :class:`GmailAttachmentToS3Sensor` (Task 12).

The storage-aware sensor answers "is there *new* work": ``poke`` returns ``True``
only if at least one matched message has **no** processed manifest in S3. It uses
an **in-memory fake S3Hook** (no network) and a fake Gmail hook whose
``build_query`` delegates to the *real* :meth:`GmailHook.build_query`, so the
"no ``-label:`` in S3" behavior is honest. The manifest key the sensor looks up
is asserted to be the *same* key the S3 operator builds — both go through the
shared :func:`manifest_key`.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_provider_gmail.hooks.gmail import GmailHook, MessageWithAttachments
from airflow_provider_gmail.manifest import Manifest, ManifestError
from airflow_provider_gmail.dates import to_local_date, to_local_iso
from airflow_provider_gmail.operators.gmail import GmailAttachmentsToS3Operator
from airflow_provider_gmail.sensors.gmail import GmailAttachmentToS3Sensor
from airflow_provider_gmail.utils.mime import Attachment
from airflow_provider_gmail.utils.paths import manifest_key

MSK = "Europe/Moscow"
BUCKET = "my-bucket"
PREFIX = "gmail/avito"
RUN = "scheduled__2026-07-12T06:00:00+00:00"


# -- fakes -------------------------------------------------------------------


class FakeS3Hook:
    """In-memory stand-in for ``S3Hook``: a ``dict`` store, decoded ``read_key``."""

    def __init__(self, store: dict | None = None, aws_conn_id: str | None = None):
        self.store: dict[str, bytes] = {} if store is None else store
        self.aws_conn_id = aws_conn_id

    def check_for_key(self, key, bucket_name=None) -> bool:
        return key in self.store

    def read_key(self, key, bucket_name=None) -> str:
        return self.store[key].decode("utf-8")


class FakeGmailHook:
    """Fake Gmail hook; ``build_query`` delegates to the real one to stay honest."""

    def __init__(self, messages):
        self._messages = list(messages)
        self._real = GmailHook("unused")
        self.built_query = None
        self.exclude_label_id = "<unset>"
        self.find_label_id_calls: list[str] = []

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
        # The S3 policy is False, so poke() must never call this. Record any call
        # so a regression (the S3 sensor resolving a processed label) is caught.
        self.find_label_id_calls.append(name)
        return "Label_should_not_be_used"

    def find_messages_with_attachments(self, query, pattern, exclude_label_id=None):
        self.exclude_label_id = exclude_label_id
        return list(self._messages)


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


def _seed_manifest(store: dict, msg, run_id: str = RUN, prefix: str = PREFIX) -> None:
    manifest = Manifest.build(
        "avito", msg.message_id, to_local_iso(msg.internal_date, MSK),
        msg.subject, msg.from_, [], run_id,
    )
    store[_manifest_key(msg, prefix)] = manifest.to_json()


def _make_sensor(store: dict, prefix: str = PREFIX, **kwargs) -> GmailAttachmentToS3Sensor:
    params = {
        "task_id": "s",
        "source": "avito",
        "bucket": BUCKET,
        "prefix": prefix,
    }
    params.update(kwargs)
    sensor = GmailAttachmentToS3Sensor(**params)
    sensor._cached_s3_hook = FakeS3Hook(store, aws_conn_id="aws_default")
    return sensor


def _context() -> dict:
    return {
        "run_id": RUN,
        "data_interval_end": datetime(2026, 7, 12, 6, 0, 0, tzinfo=ZoneInfo("UTC")),
    }


def _poke(sensor, hook) -> bool:
    sensor.hook = hook
    return sensor.poke(_context())


# -- config ------------------------------------------------------------------


def test_template_fields_add_bucket_and_prefix():
    tf = set(GmailAttachmentToS3Sensor.template_fields)
    assert {"bucket", "prefix"} <= tf
    # inherits the base filter fields too
    assert {"query", "source", "date_from", "date_to"} <= tf


def test_default_mode_is_reschedule():
    sensor = _make_sensor({})
    assert sensor.mode == "reschedule"


def test_filter_processed_label_always_false():
    # even with mark_processed=True the S3 sensor never filters by label (ADR-0001)
    assert _make_sensor({}, mark_processed=True)._filter_processed_label() is False


def test_module_imports_without_touching_s3hook():
    sensor = GmailAttachmentToS3Sensor(
        task_id="s", source="avito", bucket=BUCKET, prefix=PREFIX
    )
    assert sensor._cached_s3_hook is None


def test_s3_hook_lazily_constructs_real_hook_with_aws_conn_id():
    # Exercise the real lazy import path: _s3_hook() builds a real S3Hook bound
    # to the configured aws_conn_id.
    sensor = GmailAttachmentToS3Sensor(
        task_id="s", source="avito", bucket=BUCKET, aws_conn_id="custom_aws"
    )
    hook = sensor._s3_hook()
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    assert isinstance(hook, S3Hook)
    assert hook.aws_conn_id == "custom_aws"
    assert sensor._s3_hook() is hook  # cached


# -- poke: new work vs processed ---------------------------------------------


def test_poke_true_when_message_present_and_no_manifest():
    store: dict = {}
    msg = _message("msg1", "report.xlsx")
    sensor = _make_sensor(store)
    assert _poke(sensor, FakeGmailHook([msg])) is True


def test_poke_false_when_message_present_and_valid_manifest():
    store: dict = {}
    msg = _message("msg1", "report.xlsx")
    _seed_manifest(store, msg)
    sensor = _make_sensor(store)
    assert _poke(sensor, FakeGmailHook([msg])) is False


def test_poke_false_when_no_message():
    sensor = _make_sensor({})
    assert _poke(sensor, FakeGmailHook([])) is False


def test_poke_true_when_two_messages_one_has_manifest():
    store: dict = {}
    msg1 = _message("msg1", "a.xlsx")
    msg2 = _message("msg2", "b.xlsx")
    _seed_manifest(store, msg1)  # msg1 processed, msg2 is new work
    sensor = _make_sensor(store)
    assert _poke(sensor, FakeGmailHook([msg1, msg2])) is True


def test_poke_false_when_all_messages_have_manifests():
    store: dict = {}
    msg1 = _message("msg1", "a.xlsx")
    msg2 = _message("msg2", "b.xlsx")
    _seed_manifest(store, msg1)
    _seed_manifest(store, msg2)
    sensor = _make_sensor(store)
    assert _poke(sensor, FakeGmailHook([msg1, msg2])) is False


def test_poke_manifest_of_any_run_counts_as_processed():
    # The sensor drops any message with a valid manifest regardless of run_id.
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, run_id="scheduled__2026-07-11T06:00:00+00:00")
    sensor = _make_sensor(store)
    assert _poke(sensor, FakeGmailHook([msg])) is False


# -- corrupt manifest fails the poke (does not silently stick) ---------------


def test_corrupt_manifest_raises_manifest_error():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    store[_manifest_key(msg)] = b"{ this is not valid json"
    sensor = _make_sensor(store)
    with pytest.raises(ManifestError):
        _poke(sensor, FakeGmailHook([msg]))


def test_structurally_invalid_manifest_raises_manifest_error():
    # valid JSON but missing required keys → still ManifestError, not "processed"
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    store[_manifest_key(msg)] = b'{"source": "avito"}'
    sensor = _make_sensor(store)
    with pytest.raises(ManifestError):
        _poke(sensor, FakeGmailHook([msg]))


# -- over-limit key fails fast with the same clear ValueError ----------------


def test_poke_raises_valueerror_when_manifest_key_exceeds_s3_limit():
    """A huge prefix pushing the manifest key over 1024 bytes fails fast.

    The sensor's manifest lookup goes through the same guarded key builder the
    S3 operator uses, so it raises a clear :class:`ValueError` (naming the byte
    count and the S3 limit) instead of letting an over-limit key reach S3 and
    produce a cryptic backend validation error.
    """
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    huge_prefix = "p" * 1100  # alone already over the 1024-byte whole-key cap
    sensor = _make_sensor(store, prefix=huge_prefix)
    with pytest.raises(ValueError, match="1024"):
        _poke(sensor, FakeGmailHook([msg]))


# -- key parity with the operator --------------------------------------------


def test_manifest_key_matches_operator_destination_path():
    """The key the sensor looks up equals the one the S3 operator writes.

    Both go through the shared :func:`manifest_key` join, so they can never
    disagree on where a message's manifest lives.
    """
    msg = _message("msg1", "a.xlsx")

    op = GmailAttachmentsToS3Operator(
        task_id="t", source="avito", bucket=BUCKET, prefix=PREFIX
    )
    rel_dir = f"dt={_dt(msg)}/{msg.message_id}"
    operator_key = op._destination_path(f"{rel_dir}/_manifest.json")

    sensor_key = manifest_key(PREFIX, _dt(msg), msg.message_id)

    assert sensor_key == operator_key
    assert sensor_key == "gmail/avito/dt=2026-07-12/msg1/_manifest.json"


def test_poke_does_not_filter_search_by_label():
    # The S3 sensor never filters out processed messages by their label (policy
    # False, ADR-0001): it must not resolve a label id, must pass exclude_label_id
    # None, and its query carries no -label: term.
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    sensor = _make_sensor(store, mark_processed=True)
    hook = FakeGmailHook([msg])
    _poke(sensor, hook)
    assert hook.find_label_id_calls == []
    assert hook.exclude_label_id is None
    assert "-label:" not in hook.built_query
