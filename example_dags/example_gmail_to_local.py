"""Example: Gmail attachments → local disk, download → parse → cleanup.

Flow: ``GmailAttachmentsToLocalOperator`` → a stub parsing task → a cleanup task
that runs with ``trigger_rule="all_done"`` so the files are always removed, even
when parsing failed.

Worker-limit constraint (read before deploying):
  This provider is written for **one worker on one server**. The local operator
  writes to a local disk that is NOT shared between workers. Under CeleryExecutor
  with several workers, or under KubernetesExecutor, the download task and the
  parse task can land on different machines and the parse task will not find the
  files. In such an environment use the S3 operator instead; the local operator
  is only safe when download + parse + cleanup all run inside the same worker.

Non-idempotency (by design):
  The local operator keeps no dedup state: ``_read_manifest`` is always ``None``,
  so every matched message is downloaded and delivered on every run, overwriting
  any files already there. With a wide ``lookback_days`` window the same message
  is re-delivered on every run until it falls out of the window. The safe default
  is ``lookback_days=0`` (today only), where a message is handled in a single
  run. Dedup in local mode is possible ONLY via ``mark_processed=True`` (the
  label removes the message from the Gmail search) — a smaller window merely
  reduces repeats, it does not remove them.
"""

from __future__ import annotations

import shutil

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_provider_gmail.operators.gmail import GmailAttachmentsToLocalOperator

LOCAL_PATH = "/data/gmail/avito"

with DAG(
    dag_id="example_gmail_to_local",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    # Single active run: the local disk is shared state within this one worker,
    # and overlapping runs writing the same paths would race.
    max_active_runs=1,
    tags=["gmail", "local", "example"],
) as dag:
    download = GmailAttachmentsToLocalOperator(
        task_id="download_to_local",
        source="avito",  # manifest "source" / trace label
        gmail_conn_id="gmail_default",
        path=LOCAL_PATH,  # <path>/dt=YYYY-MM-DD/<message_id>/...
        from_email="reports@avito.ru",  # structured Gmail filter
        subject_contains="Отчёт",
        attachment_pattern=r"^report_\d{8}\.xlsx$",  # re.search on decoded name
        # lookback_days defaults to 0 for the local operator (today only) — the
        # safe pattern that keeps delivery inside a single run. Do not widen it
        # without mark_processed=True (see the module docstring).
        lookback_days=0,
        timezone="Europe/Moscow",
    )

    def _parse_attachments(**context):
        """Stub layer-2 parsing step. Reads manifest paths from XCom."""
        manifests = context["ti"].xcom_pull(task_ids="download_to_local")
        print(f"would parse manifests: {manifests}")

    parse = PythonOperator(
        task_id="parse_attachments",
        python_callable=_parse_attachments,
    )

    def _cleanup(**context):
        """Remove the downloaded files. Cleanup is the DAG's job, not the
        operator's — the operator does not care what happens to the files."""
        shutil.rmtree(LOCAL_PATH, ignore_errors=True)
        print(f"cleaned up {LOCAL_PATH}")

    cleanup = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=_cleanup,
        # all_done: always free the disk, even if parsing failed. Otherwise a
        # failed parse would leave files behind and eventually fill the disk.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    download >> parse >> cleanup
