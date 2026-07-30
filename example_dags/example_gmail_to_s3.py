"""Example: Gmail attachments → S3, the standard daily pull.

Flow: ``GmailAttachmentToS3Sensor`` → ``GmailAttachmentsToS3Operator`` →
``GmailResolveAttachmentsOperator`` → a stub parsing task. This is the
recommended shape for a recurring S3 export.

The resolver sits between the download and the parse: the download emits the
manifest paths (full ``s3://<bucket>/<key>`` URIs) of the messages processed in
this run, and ``GmailResolveAttachmentsOperator`` expands them into a flat list
of full attachment URIs — the recommended way for a layer-2 consumer (e.g.
``airflow-provider-tablefile``) to get files without knowing the manifest
schema. In a real DAG ``parse_attachments`` would be a ``TableFileToS3Operator``
taking ``resolve.output`` as its ``input_paths``.

Why the *storage-aware* ``GmailAttachmentToS3Sensor`` and not the plain
``GmailAttachmentSensor``:
  With ``mark_processed=False`` (the default) a message stays in the Gmail search
  results until it falls out of the ``lookback_days`` window. A plain
  ``GmailAttachmentSensor`` only looks at Gmail, so it would fire again on a
  message that was already downloaded yesterday, the operator behind it would
  honestly skip, and the whole DAG run would do nothing but burn a worker slot.
  ``GmailAttachmentToS3Sensor`` additionally drops every message whose
  ``_manifest.json`` already exists in S3, so it answers "is there *new* work",
  which is exactly what gates a delivery pipeline.
"""

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from airflow_provider_gmail.operators.gmail import GmailAttachmentsToS3Operator
from airflow_provider_gmail.operators.resolve import GmailResolveAttachmentsOperator
from airflow_provider_gmail.sensors.gmail import GmailAttachmentToS3Sensor

log = logging.getLogger(__name__)

BUCKET = "my-data-lake"
PREFIX = "gmail/avito"  # one prefix == one export (ADR-0003)


@dag(
    dag_id="example_gmail_to_s3",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
    catchup=False,
    # max_active_runs=1 is mandatory, not cosmetic. Dedup is a check-then-act:
    # the operator reads _manifest.json and only then writes. Two overlapping
    # DagRuns of the same export could both miss the manifest and both download
    # and deliver the same message downstream. "One server" is not "one active
    # run"; only max_active_runs=1 serializes the check and the act. This is a
    # DAG-level guarantee the operator cannot make on its own.
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        # Retrying is safe in S3 mode: delivery is idempotent via the manifest +
        # run_id (ADR-0001), so a retried attempt re-delivers the same path, never
        # a duplicate.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gmail", "s3", "example"],
)
def example_gmail_to_s3():
    @task
    def parse_attachments(attachment_uris):
        """Stub for the layer-2 parsing step (outside this provider).

        The resolver returns a flat list of full attachment URIs
        (``s3://<bucket>/<key>``); the TaskFlow API hands them here as
        ``attachment_uris``. A real task would be a ``TableFileToS3Operator``
        (or similar) taking this list as ``input_paths`` to parse each .xlsx and
        load rows into BigQuery/PostgreSQL/etc. — it never has to read a manifest.
        """
        log.info("would parse attachments: %s", attachment_uris)

    wait_for_email = GmailAttachmentToS3Sensor(
        task_id="wait_for_email",
        source="avito",  # free-form trace label, also the manifest "source"
        gmail_conn_id="gmail_default",  # read-only Gmail Connection
        aws_conn_id="aws_default",  # S3 Connection used to check for manifests
        bucket=BUCKET,  # where manifests already live → dedup on the sensor side
        prefix=PREFIX,  # must match the operator's prefix (same dedup namespace)
        from_email="reports@avito.ru",  # structured filter → Gmail `from:`
        subject_contains="Отчёт",  # structured filter → Gmail `subject:`
        attachment_pattern=r"^report_\d{8}\.xlsx$",  # re.search on decoded name
        lookback_days=7,  # 8 calendar days: covers weekends and short outages
        # mode="reschedule" is the built-in default: the sensor frees the worker
        # slot between pokes instead of holding it for the whole wait.
        poke_interval=30 * 60,  # re-check every 30 min
        timeout=6 * 60 * 60,  # give up after 6h
        soft_fail=True,  # timeout → skipped (green DAG), not a hard failure
    )

    download = GmailAttachmentsToS3Operator(
        task_id="download_to_s3",
        source="avito",  # goes into _manifest.json for tracing
        gmail_conn_id="gmail_default",
        aws_conn_id="aws_default",
        bucket=BUCKET,
        prefix=PREFIX,  # <bucket>/<prefix>/dt=YYYY-MM-DD/<message_id>/...
        from_email="reports@avito.ru",  # same filters as the sensor
        subject_contains="Отчёт",
        attachment_pattern=r"^report_\d{8}\.xlsx$",
        lookback_days=7,
        # mark_processed=False: correctness in S3 mode rests on the manifest +
        # run_id, NOT on Gmail labels (ADR-0001). Labels would need gmail.modify
        # and a token re-issue, and S3 never filters processed messages by label.
        mark_processed=False,
        timezone="Europe/Moscow",  # the `dt=` partition day is computed here
    )

    resolve = GmailResolveAttachmentsOperator(
        task_id="resolve",
        # download.output is the list of manifest URIs (s3://bucket/key) of the
        # messages processed this run; the resolver reads each and returns the
        # flat list of full attachment URIs. A template field, so the XComArg is
        # rendered before execute runs.
        manifests=download.output,
        # pick="all" (the default): every message's attachments, sorted
        # chronologically by internal_date (oldest message first).
        # Use pick="latest" to keep only the attachments of the single most-recent
        # message (by internal_date) when the same report arrives more than once.
        pick="all",
        aws_conn_id="aws_default",  # reads the s3:// manifests; endpoint lives here
    )

    # Sensor gates the download; the download's return value (manifest URIs) feeds
    # the resolver, and the resolver's return value (attachment URIs) flows into
    # the parse task as a TaskFlow data dependency.
    wait_for_email >> download >> resolve
    parse_attachments(resolve.output)


example_gmail_to_s3()
