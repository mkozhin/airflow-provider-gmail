"""Tests for :class:`GmailResolveAttachmentsOperator` (Task 3).

The operator is a **thin** wrapper over
:func:`airflow_provider_gmail.resolve.resolve_attachments`, so these tests split
into two concerns:

- **success** — an end-to-end ``execute`` over an in-memory fake ``S3Hook``
  (reusing the resolver's read edge) returns the flat URI list that Airflow
  pushes to XCom; ``template_fields`` composition; an empty input yields ``[]``.
- **delegation** — ``manifests``/``pick``/``aws_conn_id`` reach
  ``resolve_attachments`` exactly as passed (the function is mocked), and its
  errors (unknown ``pick`` → ``ValueError``, ``ManifestError``) propagate up so
  the task fails loudly.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from airflow_provider_gmail.manifest import FileEntry, Manifest, ManifestError
from airflow_provider_gmail.operators.resolve import GmailResolveAttachmentsOperator

BUCKET = "reports-bucket"


def _mk(message_id: str, internal_date: str, file_paths: list[str]) -> Manifest:
    return Manifest.build(
        source="avito",
        message_id=message_id,
        internal_date_iso=internal_date,
        subject="Отчёт",
        from_="reports@avito.ru",
        files=[FileEntry(name="f.xlsx", size=1, path=p) for p in file_paths],
        run_id="scheduled__2026-07-10T06:00:00+00:00",
    )


def _s3_manifest_path(msg: str, dt: str = "2026-07-10") -> str:
    return f"s3://{BUCKET}/gmail/avito/dt={dt}/{msg}/_manifest.json"


def _s3_key(msg: str, filename: str, dt: str = "2026-07-10") -> str:
    return f"gmail/avito/dt={dt}/{msg}/{filename}"


@pytest.fixture
def fake_s3(monkeypatch):
    """Patch the lazily-imported ``S3Hook`` with an in-memory fake (see test_resolve)."""
    import airflow.providers.amazon.aws.hooks.s3 as s3mod

    store: dict[tuple[str, str], str] = {}
    instances: list = []

    class _FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            self.aws_conn_id = aws_conn_id
            instances.append(self)

        def read_key(self, key, bucket_name=None) -> str:
            return store[(bucket_name, key)]

    monkeypatch.setattr(s3mod, "S3Hook", _FakeS3Hook)
    return SimpleNamespace(store=store, instances=instances)


def _put(fake, msg: str, manifest: Manifest, dt: str = "2026-07-10") -> str:
    uri = _s3_manifest_path(msg, dt)
    bucket, key = uri[len("s3://") :].split("/", 1)
    fake.store[(bucket, key)] = manifest.to_json().decode("utf-8")
    return uri


# -- success ------------------------------------------------------------------


def test_execute_returns_full_uri_list(fake_s3):
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [_s3_key("MSG", "report.xlsx")])
    uri = _put(fake_s3, "MSG", manifest)
    op = GmailResolveAttachmentsOperator(
        task_id="resolve", manifests=[uri], pick="all"
    )
    # execute()'s return value is exactly what Airflow pushes to XCom.
    assert op.execute(context={}) == [f"s3://{BUCKET}/{_s3_key('MSG', 'report.xlsx')}"]


def test_execute_empty_input_returns_empty(fake_s3):
    op = GmailResolveAttachmentsOperator(task_id="resolve", manifests=[])
    assert op.execute(context={}) == []
    assert fake_s3.instances == []  # no I/O for an empty input


def test_template_fields_composition():
    assert GmailResolveAttachmentsOperator.template_fields == ("manifests", "pick")


def test_xcom_push_enabled_by_default():
    # do_xcom_push (Airflow default) pushes execute()'s return to XCom.
    op = GmailResolveAttachmentsOperator(task_id="resolve", manifests=[])
    assert op.do_xcom_push is True


# -- delegation ---------------------------------------------------------------


def test_delegates_arguments_exactly(monkeypatch):
    calls: list = []

    def _fake_resolve(manifests, pick="all", aws_conn_id="aws_default"):
        calls.append((manifests, pick, aws_conn_id))
        return ["s3://b/k"]

    monkeypatch.setattr(
        "airflow_provider_gmail.operators.resolve.resolve_attachments", _fake_resolve
    )
    op = GmailResolveAttachmentsOperator(
        task_id="resolve",
        manifests=["s3://b/m/_manifest.json"],
        pick="latest",
        aws_conn_id="reports_s3",
    )
    result = op.execute(context={})

    assert result == ["s3://b/k"]
    assert calls == [(["s3://b/m/_manifest.json"], "latest", "reports_s3")]


# -- error propagation --------------------------------------------------------


def test_unknown_pick_propagates_value_error(monkeypatch):
    # No I/O should be needed — resolve_attachments validates pick first — but even
    # via the real function an unknown pick must surface (task turns red).
    op = GmailResolveAttachmentsOperator(
        task_id="resolve", manifests=[_s3_manifest_path("MSG")], pick="invalid"
    )
    with pytest.raises(ValueError, match="pick must be one of"):
        op.execute(context={})


def test_manifest_error_propagates(fake_s3):
    uri = _s3_manifest_path("MSG")
    bucket, key = uri[len("s3://") :].split("/", 1)
    fake_s3.store[(bucket, key)] = "{ not json"
    op = GmailResolveAttachmentsOperator(task_id="resolve", manifests=[uri])
    with pytest.raises(ManifestError):
        op.execute(context={})
