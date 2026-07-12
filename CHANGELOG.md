# Changelog

All notable changes to `airflow-provider-gmail` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Versions are derived from git tags via `setuptools-scm`, and the publish workflow
releases to PyPI on a version tag.

## [Unreleased]

Fixes from a deep code review. Several change delivery behavior — review the
**Changed** entries before upgrading.

### Changed

- **Inline-attachment filter now drops only inline images.** A part is dropped
  only when it is both `inline` *and* `image/*`; the previous "or it has a
  `Content-ID`" branch is gone. Inline non-images (real PDFs, xlsx) that carry a
  `Content-ID` — as Apple Mail and some gateways emit — are now kept as
  attachments instead of being silently discarded.
- **Forwarded emails (`message/rfc822`, `.eml`) are delivered as attachments.**
  A `message/rfc822` part with a filename and a body source is now yielded as a
  single `.eml` attachment and is no longer descended into, so the inner
  attachments of a forwarded message are no longer leaked as separate files under
  the outer message's `message_id`.
- **Processed-mail dedup filters by the message's `labelIds` in code**, not via a
  `-label:"..."` query term. Label IDs are deterministic (the provider
  creates/looks them up), so the comparison is reliable and unit-testable offline;
  the old query term relied on undocumented Gmail search behavior and could fail
  open (duplicate delivery). Applies to the local operator (opt-in via
  `mark_processed`) and the Gmail sensor; S3 delivery dedup is unchanged.
- **The S3 sensor now honors the manifest `run_id`.** A `_manifest.json` written
  by the *current* run means work still remains (mirrors `Decision.DELIVER_ONLY`
  in ADR-0001), so the sensor lets the operator finish delivery; "processed" now
  means a manifest from a *different* run. This fixes a stall after clearing a
  whole run.

### Fixed

- **Structured query field values with Gmail metacharacters are now quoted.** A
  value is quoted unless it matches the safe pattern `^[\w.@+-]+$` (Unicode), so
  values like `re:invoice`, `{urgent}`, or `(a)` are no longer parsed as Gmail
  operators. Plain tokens and single-word Cyrillic stay unquoted.
- **Empty `attachmentId` with no data now fails loudly.** A part whose body has
  no usable source (falsy `attachmentId` and no `data` key) is no longer treated
  as an attachment, and `download_attachment` raises `AirflowException` instead of
  silently writing a zero-byte file. A legitimately empty file (`data == ""`)
  still works.
- **Long extensionless filenames no longer collapse to empty.** Name capping and
  collision resolution use `os.path.splitext`, so a very long dotless name keeps a
  non-empty stem and dotfiles (e.g. `.bashrc`) round-trip undistorted
  (`.bashrc_1` on collision, not `_1.bashrc`).

### Documentation

- Honest `soft_fail` documentation: the Airflow default is `soft_fail=False`
  (timeout = error + alert); pass `soft_fail=True` to make a timeout a `skipped`
  (green) task. The default is unchanged.
- Backfill example and README now warn that `max_active_runs=1` is per-DAG and
  does **not** serialize a backfill against a daily DAG over the same prefix —
  pause the daily DAG before backfilling to avoid double delivery / a lost
  manifest.

## [0.1.0] - 2026-07-12

Initial release: layer 1 of the Gmail → warehouse pipeline (Gmail → S3 / local
disk). Parsing files is out of scope by design.

### Added

- `GmailHook`: read-only Gmail Connection with an in-memory refresh grant (the
  short-lived `access_token` is never stored), a required `userId` from
  `extra.user_id` (default `"me"`), the reference-only `scopes` extra field, and a
  password widget for `refresh_token`. Clear `GmailAuthError` on a
  revoked/expired token (with the "In production" hint) and `GmailPermissionError`
  on a missing `gmail.modify` scope. Built-in retry (`execute(num_retries=N)`) for
  Gmail 429/5xx.
- MIME parsing utilities: recursive attachment walk, inline-image filtering
  (inline *and* (`image/*` or a `Content-ID`)), RFC 2047 decoding of
  `Subject`/`From`, and untrusted-filename sanitization.
- Numeric `after:`/`before:` query building over a pure `Window` value object, so
  the search window is computed in the operator timezone and is stable between
  retries (reference day from `data_interval_end`, not `date.today()`).
- `GmailAttachmentsToS3Operator`: writes attachments and a per-message
  `_manifest.json` under `<prefix>/dt=<dt>/<message_id>/`, with delivery dedup keyed
  by the manifest + `run_id` (ADR-0001). `load_bytes(..., replace=True)`,
  `overwrite` for forced re-download, and lazy import of the Amazon provider.
- `GmailAttachmentsToLocalOperator`: writes to a local disk with no dedup state
  (default `lookback_days=0`), always overwriting.
- `GmailAttachmentSensor` ("is there a matching email?", Gmail only) and
  `GmailAttachmentToS3Sensor` ("is there new work?", drops messages that already
  have a manifest). Both default to `mode="reschedule"`.
- Pure modules: `Manifest`/`FileEntry`/`ManifestError`/`Decision`/`decide`
  (`manifest.py`), `Window` (`window.py`), and S3 key construction (`utils/paths.py`).
- Provider registration via the `apache_airflow_provider` entry point with a
  `gmail` connection type.
- Example DAGs (`example_dags/`): daily S3 pull, local download → parse → cleanup,
  and a sensor-less S3 backfill with `overwrite=True`.

[Unreleased]: https://github.com/mkozhin/airflow-provider-gmail/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mkozhin/airflow-provider-gmail/releases/tag/v0.1.0
