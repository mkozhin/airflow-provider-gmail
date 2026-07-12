"""Example DAGs must be import-clean and structurally match their guarantees.

Import-clean alone does not prove the dedup guarantees the docs promise, so the
structural assertions below pin the load-bearing choices: the S3 daily DAG gates
on the storage-aware sensor and serializes runs; the backfill DAG forces
overwrite and deliberately has no storage-aware sensor.
"""

from __future__ import annotations

import os

import pytest
from airflow.models.dagbag import DagBag

from airflow_provider_gmail.operators.gmail import GmailAttachmentsToS3Operator
from airflow_provider_gmail.sensors.gmail import (
    GmailAttachmentSensor,
    GmailAttachmentToS3Sensor,
)

EXAMPLE_DAGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example_dags"
)


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=EXAMPLE_DAGS_DIR, include_examples=False)


def test_example_dags_import_without_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"import errors: {dagbag.import_errors}"
    # All three example DAGs are present. Use the parsed ``dags`` mapping, not
    # ``get_dag`` — the latter queries the metadata DB, which the test suite has
    # no need for (the DAGs parse without a live DB or connections).
    for dag_id in (
        "example_gmail_to_s3",
        "example_gmail_to_local",
        "example_gmail_s3_backfill",
    ):
        assert dag_id in dagbag.dags, f"missing DAG {dag_id}"


def test_s3_dag_serializes_runs_and_gates_on_storage_aware_sensor(
    dagbag: DagBag,
) -> None:
    dag = dagbag.dags["example_gmail_to_s3"]

    # max_active_runs==1 guards the check-then-act dedup against parallel runs.
    assert dag.max_active_runs == 1

    sensors = [
        t for t in dag.tasks if isinstance(t, GmailAttachmentSensor)
    ]
    assert len(sensors) == 1
    # Exactly the storage-aware sensor, not the plain Gmail-only one: the plain
    # sensor would fire again on already-processed messages (mark_processed=False).
    assert type(sensors[0]) is GmailAttachmentToS3Sensor


def test_backfill_dag_overwrites_and_has_no_storage_aware_sensor(
    dagbag: DagBag,
) -> None:
    dag = dagbag.dags["example_gmail_s3_backfill"]

    operators = [
        t for t in dag.tasks if isinstance(t, GmailAttachmentsToS3Operator)
    ]
    assert len(operators) == 1
    # overwrite=True forces re-download of messages that already have a manifest.
    assert operators[0].overwrite is True

    # No GmailAttachmentToS3Sensor: it would drop manifested messages and the
    # backfill would never start.
    assert not any(
        isinstance(t, GmailAttachmentToS3Sensor) for t in dag.tasks
    )
