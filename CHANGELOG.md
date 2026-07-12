# Changelog

All notable changes to `airflow-provider-gmail` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Versions are derived from git tags via `setuptools-scm`, and the publish workflow
releases to PyPI on a version tag.

## [Unreleased]

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
  (inline *and* image/`Content-ID`), RFC 2047 decoding of `Subject`/`From`, and
  untrusted-filename sanitization.
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
