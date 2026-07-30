# Changelog

## [0.4.0] - 2026-07-30

### Changed

- **BREAKING: `pick="all"` now returns attachments sorted by ascending
  `internal_date`, not input order.** `resolve_attachments(manifests,
  pick="all")` / `GmailResolveAttachmentsOperator` previously flattened
  attachments in the order `manifests` was given (an artifact of Gmail
  `messages.list`/download order, not the messages' actual timing); it now
  sorts manifests by `internal_date` ascending (oldest message first) before
  flattening, so a downstream consumer sees attachments in real chronological
  order (ADR-0008). Manifests with an equal `internal_date` keep a stable
  tie-break — their relative order follows the input list. Interleaved
  duplicates of the same manifest (e.g. `[A, B, A]`) may be **regrouped** by
  the sort (e.g. `[A, A, B]`) — duplicates still pass through by count, just
  not necessarily by position. Files *within* a single manifest (`files[]`)
  are not reordered. A manifest with a malformed/naive `internal_date` can now
  raise `ValueError` (from `from_local_iso`) under `pick="all"`, which
  previously never read `internal_date` at all. `pick="latest"` is unchanged.

## [0.3.1] - 2026-07-23

### Added

- **The download operators now log at `INFO` what was downloaded/re-delivered and
  where.** On a normal successful run `GmailAttachmentsToS3Operator` and
  `GmailAttachmentsToLocalOperator` previously stayed silent about delivery; now
  each processed message emits one `INFO` line — `message_id`, subject, the
  attachment file names, and the destination manifest path (the same
  `s3://…/_manifest.json` URI or absolute path that lands in XCom) — for both
  freshly downloaded messages and messages re-delivered from a current-run
  manifest without re-download. A final summary line reports the totals
  (files/messages downloaded, re-delivered, past-run skipped). The sender
  (`from`) is deliberately **not** logged, to keep sensitive mail metadata out of
  Airflow logs. Logging only; XCom, manifests, dedup, and action order are
  unchanged.
- **`GmailResolveAttachmentsOperator` now logs at `INFO` what it resolved.** The
  operator previously stayed silent; it now emits one summary line — the input
  manifest count, the `pick` mode, and the resolved attachment count — followed
  by one line per resolved attachment path (the full `s3://…` URIs or absolute
  paths pushed to XCom). Logging only; the returned list, XCom, and error
  propagation are unchanged.

### Fixed

- **No more spurious `execute cannot be called outside TaskInstance!` warning
  on every S3 download run.** Each run of `GmailAttachmentsToS3Operator` logged
  `GmailAttachmentsToS3Operator.execute cannot be called outside TaskInstance!`
  at `WARNING` — cosmetic noise, as the task ran normally (manifests, XCom, and
  downstream were all correct). The operator overrode `execute()` only to
  validate the rendered `prefix` and then called `super().execute(context)`;
  under Airflow 2.9+ every operator's `execute` is wrapped by `ExecutorSafeguard`,
  and that nested `super().execute()` call carries no sentinel, so the safeguard
  logged the false warning. The base orchestration moved into a protected
  `_run()` method, which the S3 operator's `execute()` override now calls
  directly (instead of `super().execute()`); `ExecutorSafeguard` wraps only
  methods named `execute`, so the inner `_run()` call is silent and the warning
  no longer appears. The rendered-`prefix` validation stays **at `execute` time**
  (after `on_execute_callback`), so the URL-safe-keys guarantee (ADR-0007) is
  unchanged from 0.3.0 behavior. The operator's normal (`TaskInstance`) behavior
  is otherwise unchanged: `execute()`'s signature and return value, `prefix`
  fail-fast validation, XCom, manifests, and dedup all stay as before.

## [0.3.0] - 2026-07-22

### Changed

- **BREAKING: XCom now carries full paths, not bare object keys.** The download
  operators return the paths of the `_manifest.json` files of the messages
  processed in this run as **full** paths: `GmailAttachmentsToS3Operator` returns
  `s3://<bucket>/<key>` URIs (previously bare keys with no bucket or scheme), and
  `GmailAttachmentsToLocalOperator` continues to return absolute local paths — so
  the XCom contract is uniformly "a list of full manifest paths". A consumer no
  longer has to source the bucket out of band. The URI addresses the object only
  in tandem with the consumer's `aws_conn_id`: the endpoint lives in the
  Connection, not in the URI. The manifest schema is unchanged — `files[].path`
  stays an object key / absolute path (layer-2 contract, ADR-0001); only
  `execute()`'s return value is re-anchored (via the new `_xcom_path` seam,
  ADR-0006/ADR-0007). Update any task that reads the download's XCom and expected
  a bare key.
- **BREAKING: attachment filenames and prefixes are hardened for URL-safe keys.**
  `sanitize_filename` now replaces every character from S3's own "characters to
  avoid" set (`{ } ^ [ ] < > ~ | # %` and the backtick) plus `?` and the double
  quote with `_`, so produced object keys are URL-safe by construction and a
  third-party `s3://` URL parser works on them. For **file names**, `\` and `/`
  are unaffected (the basename step already strips them, so `a\b\c.xlsx →
  c.xlsx` is preserved). Attachments downloaded from now on get these new object
  names; `files[].name` in the manifest still stores the original attachment
  name. Separately, a rendered `prefix` — which never passes through
  `sanitize_filename` and reaches the object key verbatim — is now **rejected**
  with a `ValueError` at the top of `execute()`/`poke()` if it carries any of
  those characters, **or** a backslash (`\`), **or** an ASCII C0 control
  (`ord < 0x20`) or `DEL` (`0x7F`) — so a `prefix` such as `reports\narchive`
  can no longer produce a key a downstream `urlsplit()` would silently rewrite
  (previously such a prefix was silently allowed).

### Added

- **Manifest resolver — the official client of the `_manifest.json` contract.**
  `resolve_attachments(manifests, pick="all", aws_conn_id="aws_default")` expands
  a list of full manifest paths (the download operator's XCom) into a flat list
  of full attachment paths (`s3://<bucket>/<key>` URIs or absolute local paths) a
  layer-2 consumer (e.g. `airflow-provider-tablefile`) can open without knowing
  the manifest schema. `pick="all"` (default) returns every manifest's
  attachments in input order; `pick="latest"` returns only the attachments of the
  single most-recent manifest by `internal_date` (compared as aware moments,
  tie-broken by `message_id`). Reading a manifest is lazy: a purely-local input
  never imports the Amazon provider, and one `S3Hook` is created per call and
  reused. A broken manifest raises `ManifestError`, a missing one raises its
  natural storage error — neither is swallowed. Direct manifest reads (as the
  realcombi prod DAG does today) remain a supported contract; the resolver is the
  recommended path.
- **`GmailResolveAttachmentsOperator`** — the declarative-DAG face of
  `resolve_attachments` (template fields `manifests`, `pick`), for wiring
  `download >> resolve >> parse` without the TaskFlow API.
- **`dates.from_local_iso(value)`** — parses an `internal_date` ISO string back to
  an aware `datetime` (a naive string raises `ValueError`); used by the resolver's
  `pick="latest"`.
- **URI helpers in `utils/paths.py`** — `s3_uri(bucket, *segments)`,
  `split_s3_uri(uri)` and `is_s3_uri(uri)`, the single owner of the `s3://`
  literal in `src`.

## [0.2.0] - 2026-07-14

### Changed

- **License changed from Apache-2.0 to MIT.** Package metadata (author,
  description, and project URLs) aligned with the other providers.
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
  value is quoted unless it matches the safe pattern `^[\w.@+-]+\Z` (Unicode), so
  values like `re:invoice`, `{urgent}`, `(a)`, or a value with a trailing newline
  are no longer parsed as / smuggled past Gmail operators. Plain tokens and
  single-word Cyrillic stay unquoted.
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
- **Example DAGs modernized to the TaskFlow API** (`@dag`/`@task`), with
  `default_args` (`owner`, `retries`), `logging` instead of `print`, and stdlib
  `datetime` instead of `pendulum`.

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

[0.4.0]: https://github.com/mkozhin/airflow-provider-gmail/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/mkozhin/airflow-provider-gmail/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/mkozhin/airflow-provider-gmail/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mkozhin/airflow-provider-gmail/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mkozhin/airflow-provider-gmail/releases/tag/v0.1.0
