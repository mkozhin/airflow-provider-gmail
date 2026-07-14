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
  provider filters out messages whose ``labelIds`` already carry the processed
  label — a comparison done in code against the resolved processed-label id; the
  label does NOT remove the message from Gmail's search results) — a smaller
  window merely reduces repeats, it does not remove them.
"""

import logging
import shutil
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from airflow_provider_gmail.operators.gmail import GmailAttachmentsToLocalOperator

log = logging.getLogger(__name__)

LOCAL_PATH = "/data/gmail/avito"


@dag(
    dag_id="example_gmail_to_local",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
    catchup=False,
    # Single active run: the local disk is shared state within this one worker,
    # and overlapping runs writing the same paths would race.
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        # A retry re-downloads and overwrites the same files (the local operator
        # keeps no dedup state); overwriting is safer than leaving a partial write.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gmail", "local", "example"],
)
def example_gmail_to_local():
    @task
    def parse_attachments(manifests):
        """Stub layer-2 parsing step. Receives the manifest paths the operator
        returned (via the TaskFlow data dependency)."""
        log.info("would parse manifests: %s", manifests)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_local_files():
        """Remove the downloaded files. Cleanup is the DAG's job, not the
        operator's — the operator does not care what happens to the files.

        ``all_done``: always free the disk, even if parsing failed. Otherwise a
        failed parse would leave files behind and eventually fill the disk.

        Note: this wipes the WHOLE base path (every ``dt=`` partition under
        ``LOCAL_PATH``), not just this run's partition. That is fine here because
        the base path is dedicated to this DAG and each run parses before cleanup;
        scope the rmtree to the run's ``dt=`` subdirectory if you share the path.
        """
        shutil.rmtree(LOCAL_PATH, ignore_errors=True)
        log.info("cleaned up %s", LOCAL_PATH)

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

    # download → parse (manifest paths flow in) → cleanup (all_done).
    parse_attachments(download.output) >> cleanup_local_files()


example_gmail_to_local()
