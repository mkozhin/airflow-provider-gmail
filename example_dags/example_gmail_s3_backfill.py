"""Example: manual S3 backfill / replay of an explicit date range.

Trigger this DAG by hand with a config, e.g.::

    {"date_from": "2026-06-01", "date_to": "2026-06-30"}

Flow: ``GmailAttachmentsToS3Operator`` with ``overwrite=True`` and NO sensor.

Why there is no ``GmailAttachmentToS3Sensor`` here:
  The storage-aware sensor discards every message whose ``_manifest.json``
  already exists in S3 and would report "no new work" — the operator behind it
  would never start. But a backfill exists precisely to re-process messages that
  already have a manifest (a file got corrupted, a manifest went bad). So the
  replay must run WITHOUT the sensor: manually, or from a dedicated sensor-less
  DAG like this one. ``overwrite=True`` is therefore incompatible with
  ``GmailAttachmentToS3Sensor`` by construction.

Why ``date_from`` / ``date_to`` from ``dag_run.conf`` (ADR-0004):
  Without an explicit range the search window anchors on the run's reference
  "now" and looks back ``lookback_days`` — you cannot reach an old period that
  way. Supplying an explicit range makes ``after:`` = midnight ``date_from`` and
  ``before:`` = midnight ``date_to`` + 1 day, so the replay covers exactly the
  requested period and nothing else. Both parameters are templated; they default
  to empty strings so that an un-parametrized trigger fails loudly at execution
  rather than silently backfilling the wrong window.

WARNING — PAUSE the daily DAG before running this backfill over a shared prefix:
  This DAG shares ``PREFIX = "gmail/avito"`` with the daily ``example_gmail_to_s3``
  DAG. ``max_active_runs=1`` is per-DAG — it serializes replays against each other,
  but it does NOT serialize this backfill against the daily DAG over the same
  prefix. Run them concurrently and the check-then-act manifest dedup races:
  duplicate delivery, and — worse — an ``overwrite=True`` backfill can overwrite
  the ``_manifest.json`` of a failed daily attempt with a foreign ``run_id``,
  losing the daily pipeline's delivery on retry. Before starting a backfill over a
  shared prefix, PAUSE the daily DAG (and let any in-flight run finish).
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from airflow_provider_gmail.operators.gmail import GmailAttachmentsToS3Operator

BUCKET = "my-data-lake"
PREFIX = "gmail/avito"  # must match the daily DAG's prefix (same namespace)

with DAG(
    dag_id="example_gmail_s3_backfill",
    schedule=None,  # manual trigger only
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # serialize replays over the same prefix
    tags=["gmail", "s3", "backfill", "example"],
) as dag:
    replay = GmailAttachmentsToS3Operator(
        task_id="replay_to_s3",
        source="avito",
        gmail_conn_id="gmail_default",
        aws_conn_id="aws_default",
        bucket=BUCKET,
        prefix=PREFIX,
        from_email="reports@avito.ru",
        subject_contains="Отчёт",
        attachment_pattern=r"^report_\d{8}\.xlsx$",
        # overwrite=True: force re-download and re-delivery even for messages that
        # already have a manifest. With overwrite=True the manifest is not read at
        # all, so a corrupted manifest cannot block the recovery it exists for.
        overwrite=True,
        # Explicit range from the trigger config (ADR-0004). When set, lookback_days
        # is ignored. Templated → rendered from dag_run.conf at execution time.
        date_from="{{ dag_run.conf['date_from'] }}",
        date_to="{{ dag_run.conf['date_to'] }}",
        mark_processed=False,  # S3 correctness is the manifest, not labels
        timezone="Europe/Moscow",
    )
