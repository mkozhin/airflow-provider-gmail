# airflow-provider-gmail

[English (this file)](README.md) · [Русский](README_RU.md)

*Powered by [Claude Code](https://claude.ai/code)*

An Apache Airflow provider that finds emails in Gmail by a set of conditions,
picks the attachments you want out of them, and drops the bytes into an
S3-compatible object store or onto a local disk. It targets **Airflow 2.9.1**
(`>=2.9,<3`) and **Python 3.10+**.

Dozens of independent exports are expected, each with different settings, so all
the repeating logic lives in the provider and only the per-export specifics stay
as DAG parameters.

## What it is (and is not): the three layers

The end-to-end task "Gmail → attachment → parse → warehouse" is split into three
layers by **rate of change**, not by code volume:

| Layer | What it does | Stability | Where it lives |
|---|---|---|---|
| 1 | Gmail → S3 / local disk | same for every export | **this provider** |
| 2 | Parse file → table | the most volatile | outside the provider |
| 3 | Table → BigQuery / PostgreSQL / ClickHouse / S3 | already written | standard providers |

**This provider is layer 1 only. Parsing files is explicitly out of scope.** The
provider knows about Gmail and about where to put the bytes. It knows nothing
about `.xlsx`, `.csv`, encodings, or sheet layouts. Folding parsing in would
produce an "operator that does everything" with a combinatorial explosion of
parameters.

The join point with layer 2 is the `_manifest.json` file the provider writes next
to the attachments; the operator returns the **full path** of that manifest in
XCom (an `s3://<bucket>/<key>` URI, or an absolute path in local mode). Layer 2
reads the manifest — or, more simply, lets the built-in **resolver** turn those
manifest paths into a flat list of full attachment paths — and does not care
where the file came from (see *Resolving attachments*).

### What it solves

- OAuth to Gmail **without storing the short-lived `access_token`** and without
  writing anything back to the Airflow metadata DB.
- Correct MIME parsing: Cyrillic filenames, nested `multipart`, inline images.
- In S3 mode the same email is **not** downloaded or delivered downstream twice
  **even without labels**: the manifest carrying `run_id` plus `message_id` in the
  path deduplicate delivery on their own.
- Waiting for an email (a sensor) instead of failing when the report has not
  arrived yet.

### Components

```
GmailHook                            # all the non-trivial logic
GmailAttachmentSensor                # is there a matching email? (Gmail only)
  └─ GmailAttachmentToS3Sensor       # + bucket/prefix: is there new work (an email with no past-run manifest)?
GmailAttachmentsBaseOperator         # abstract: search, select, manifest, label
  ├─ GmailAttachmentsToS3Operator    # + bucket/prefix/aws_conn_id, dedup, overwrite
  └─ GmailAttachmentsToLocalOperator # + path, no dedup, always overwrites
GmailResolveAttachmentsOperator      # manifest paths → flat list of attachment paths
resolve_attachments()                # the same, as a function for the TaskFlow API
```

The S3 operator works against any S3-compatible store (Yandex Object Storage,
MinIO, …), not only AWS — point the underlying Amazon Connection at a custom
`endpoint_url` through its `extra`.

## Setting up the Connection

The Connection is **read-only**. On every run the hook performs a refresh grant
into memory and builds a fresh Gmail service; no `access_token` is ever stored.
Writing a refreshed token back into the Connection would race between parallel
tasks, need write access to the metadata DB, and cause mysterious 401s. The price
is one extra HTTP request per task.

```
conn_type: gmail
login:     <client_id>       # shown as "Client ID"
password:  <client_secret>   # shown as "Client Secret"
extra:     {"refresh_token": "1//09fy...",
            "user_id": "me",
            "scopes": ["https://www.googleapis.com/auth/gmail.readonly"]}
```

- **`client_id` / `client_secret`** come from an OAuth client of type *installed*
  (created in the Google Cloud console for your project). They go into the
  Connection's `login` / `password`, relabeled on the form to *Client ID* /
  *Client Secret*.
- **`refresh_token`** is taken **once**, from the token file produced when you
  first walk through the OAuth consent (e.g. with `google-auth-oauthlib`). Only
  the `refresh_token` is used; the `access_token`/`expiry` from that file are
  ignored. On the Connection form `refresh_token` is a **password field** — it is
  long-lived and secret and must never be shown in clear text.
- **Why no `access_token`.** The refreshed token the hook obtains each run carries
  exactly the scopes granted at consent; a stored access token would just be a
  stale copy and a source of races. See above.
- **`user_id`** (default `"me"`) is the **required** `userId` of every Gmail API
  call; it is read from `extra.user_id`.
- **`scopes` in `extra` is reference-only / decorative.** The refreshed token
  carries exactly the scopes that were granted to the `refresh_token` at consent;
  editing `scopes` in the Connection changes nothing. The field is documentation
  for a human. The real permission check is a 403 `insufficientPermissions` at
  `batchModify` time.

Attaching labels (`mark_processed=True`) needs the `gmail.modify` scope. You
**cannot** widen the scopes of an existing `refresh_token`; you must re-issue it
by walking through the OAuth consent again with `gmail.modify` granted.

> ## ⚠️ The OAuth app MUST be "In production"
>
> If the project's consent screen is left in **"Testing"** status, Google issues
> a `refresh_token` that lives only **7 days**. The pipeline will run fine for a
> week and then **die silently** — this is the single most common trap of this
> scheme. Publish the OAuth app (status **"In production"**) before relying on it.
>
> A revoked or expired `refresh_token` surfaces as a clear `GmailAuthError` with a
> hint, not a raw `RefreshError`.

## Parameters

### Operator parameters

`GmailAttachmentsToS3Operator` and `GmailAttachmentsToLocalOperator` share the
base parameters below and each add a few of their own.

**Shared (base operator):**

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `source` | `str` | *required* | Free-form trace label; written into the manifest `source`. Not part of the path. |
| `gmail_conn_id` | `str` | `"gmail_default"` | The read-only Gmail Connection. |
| `query` | `str \| None` | `None` | Raw Gmail search string. **If set, the structured fields below are ignored** (two sources of truth are not allowed). Templated. |
| `from_email` | `str \| None` | `None` | Structured filter → Gmail `from:`. Templated. |
| `subject_contains` | `str \| None` | `None` | Structured filter → Gmail `subject:`. Templated. |
| `has_attachment` | `bool` | `False` | `True` → adds `has:attachment`. `False` adds nothing (`-has:attachment` is never emitted). |
| `filename_contains` | `str \| None` | `None` | Structured filter → Gmail `filename:` (server-side coarse narrowing only — see Gmail gotchas). Templated. |
| `attachment_pattern` | `str \| None` | `None` | `re.search` over the **decoded** filename. `None` → every non-inline attachment matches. **Not templated** (ADR-0005): a bad regex fails at DAG parse. |
| `lookback_days` | `int` | `7` (S3) / `0` (local) | Sliding `after:` window in calendar days from midnight of the reference day. `0` → today only; `7` → eight calendar days. |
| `mark_processed` | `bool` | `False` | Attach a Gmail label to processed messages (needs `gmail.modify`). Off by default → `gmail.readonly` suffices. |
| `label_suffix` | `str \| None` | `None` | `None` → label `airflow/processed`; `"avito"` → `airflow/processed/avito`. |
| `timezone` | `str` | `"Europe/Moscow"` | Zone for the window day, the `dt=` partition, and the manifest `internal_date`. |
| `date_from` / `date_to` | `str \| None` | `None` | Explicit `YYYY-MM-DD` backfill range (ADR-0004), usually from `dag_run.conf`. If either is set, `lookback_days` is ignored. Templated. |

**`GmailAttachmentsToS3Operator` adds:**

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `bucket` | `str` | *required* | Target bucket. |
| `prefix` | `str` | `""` | Base key prefix, e.g. `gmail/avito`. **One prefix = one export** (ADR-0003). Templated. |
| `aws_conn_id` | `str` | `"aws_default"` | The Amazon/S3 Connection (may point at a custom `endpoint_url`). |
| `overwrite` | `bool` | `False` | `True` → force re-download; the manifest is not even read. Incompatible with `GmailAttachmentToS3Sensor` (see Limitations). |

**`GmailAttachmentsToLocalOperator` adds:**

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `path` | `str` | *required* | Base directory, e.g. `/data/gmail/avito`. Templated. |

Note: `lookback_days` defaults to `0` here (not `7`), and there is **no public
`overwrite`** argument — the local operator always overwrites.

### Sensor parameters and which sensor to use

Both sensors mirror the operator's filter parameters exactly (`query`,
`from_email`, `subject_contains`, `has_attachment`, `filename_contains`,
`attachment_pattern`, `lookback_days`, `timezone`, `date_from`/`date_to`,
`mark_processed`, `label_suffix`, `source`, `gmail_conn_id`) so the sensor
searches the same set of messages the operator will. They also expose the
`BaseSensorOperator` knobs:

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `mode` | `str` | `"reschedule"` | **Set to `reschedule` by default in `__init__`** — in `poke` mode the sensor holds a worker slot for the whole wait (hours); with dozens of exports that eats the pool. |
| `poke_interval` | `int` | `60` | Seconds between pokes (use ~30 min in practice). |
| `timeout` | `int` | — | Give-up time. |
| `soft_fail` | `bool` | `False` | Not overridden here, so Airflow's default applies: a timeout **fails** the task and fires alerts. Pass `soft_fail=True` explicitly to make a timeout `skipped` (green DAG) instead. |

`GmailAttachmentToS3Sensor` additionally takes `bucket`, `prefix` (default `""`)
and `aws_conn_id` (default `"aws_default"`).

**Which sensor to use:**

- **`GmailAttachmentSensor`** — *"is there a matching email?"* Looks only at
  Gmail; `poke()` → `True` when the search is non-empty. Use it where dedup is
  guaranteed by the processed label (`mark_processed=True`): labelled messages
  stay in Gmail's result set, and the provider filters them out in code by
  comparing each message's `labelIds` against the processed-label id. It is also
  the **default sensor for the local operator**.
  When you pair it with the local operator, **set matching `lookback_days` on
  both** (this sensor defaults to `7`, the local operator to `0`); otherwise the
  sensor may fire on a message the operator's narrower window never returns and the
  DAG hangs. Do **not** rely on `lookback_days=1` for dedup — a processed message
  stays in the Gmail result set until the window ends, so with labels off this
  sensor re-fires and the operator behind it honestly skips.
- **`GmailAttachmentToS3Sensor`** — *"is there new work?"* Subclasses the first,
  adds `bucket`/`prefix`/`aws_conn_id`, and drops every message already processed
  by a **past run** (a `_manifest.json` in S3 from a *different* run); a manifest
  carrying the *current* `run_id` still counts as work. `True` only if at least one
  **unprocessed** message remains. It is the natural gate for `GmailAttachmentsToS3Operator` — use it for
  the standard recurring S3 export.

## Limitations

- **One worker on one server.** The provider is written for a single worker on the
  same server. `GmailAttachmentsToLocalOperator` writes to a local disk that is
  not shared between workers.
- **Local operator + multiple workers.** Under CeleryExecutor with several
  workers, or under KubernetesExecutor, the download task and the parse task can
  land on different machines and the parse will not find the file. In such an
  environment the local operator is safe only *within a single task*; use the S3
  operator for everything else.
- **Delivery dedup exists only in S3 mode**, keyed by the manifest + `run_id`. The
  local operator keeps no dedup state (`_read_manifest` is always `None`) and
  re-delivers every matched message every run — by design.
- **Labels need `gmail.modify` + a token re-issue.** You cannot widen the scope of
  an existing `refresh_token`; re-issue it through the OAuth consent with
  `gmail.modify` granted.
- **In S3 mode the label is NOT used to filter out processed messages** (ADR-0001).
  Attaching the label (`batchModify`) and pushing the return-XCom are not atomic: if
  the label were already set and the task died before returning, a label-filtered
  search would not surface the message, its current-`run_id` manifest would never
  reach the dedup decision, and a fully downloaded email would vanish silently on a
  green retry. So in S3 correctness rests on the manifest + `run_id` **alone**;
  `mark_processed=True` may still *attach* a label as an external marker, but it
  never filters processed messages out. In **local** mode the label is an opt-in
  dedup for wide windows (`mark_processed=True` drops messages whose `labelIds`
  already carry the processed label — a comparison done in code, not a `-label:`
  query term) — accepting the honest caveat that a crash between labeling and
  delivery can "lose" a message on retry, which is exactly why S3 never does this.
- **`overwrite` is incompatible with `GmailAttachmentToS3Sensor`.** The
  storage-aware sensor discards messages that already have a *past-run* manifest
  and would report "no work", so the operator behind it never runs. Drive overwrite
  backfills **without** that sensor — manually, or from a dedicated sensor-less
  backfill DAG (see `example_gmail_s3_backfill.py`).
  **PAUSE the daily DAG before backfilling over a shared prefix.** `max_active_runs=1`
  is per-DAG — it serializes a backfill DAG's own runs but does **not** serialize
  it against the daily DAG over the same prefix. Run both at once and the
  check-then-act manifest dedup races: duplicate delivery, and an `overwrite=True`
  backfill can overwrite the `_manifest.json` of a *failed* daily attempt with a
  foreign `run_id`, losing the daily pipeline's delivery on retry. Pause the daily
  DAG (and let any in-flight run finish) before starting the backfill.
- **Parallel runs require `max_active_runs=1`.** The manifest check is a
  check-then-act (`_read_manifest` → write); two overlapping `DagRun`s of one
  export could both miss the manifest and both download and deliver the same
  message. "One server" is not "one active run". The guarantee is honest: **no
  repeats on sequential runs**; parallelism is defended at the DAG level with
  `max_active_runs=1` (set and commented in the example DAGs).
- **One prefix = one export (ADR-0003).** Two exports sharing a `prefix` in one
  bucket, drawing overlapping messages from the same mailbox, silently overwrite
  each other's manifest. There is no path isolation between exports — give each
  export its own `prefix`.
- **Local default `lookback_days=0` vs S3 default `7` (ADR-0001).** S3 dedups
  delivery, so a wide window is safe there; the local operator does not, so a wide
  window re-delivers every message each run. The safe local default is therefore
  `0` (today only) — suppressing duplicates by the *narrowness of the window*
  rather than by dedup.

## Gmail gotchas

- **`filename:` is not a substring match.** Gmail tokenizes the filename by
  separators, it does not do substring search: `filename:report` finds
  `annual-report-2024.xlsx` but **not** `myreport.xlsx`. There are no regex or
  wildcards in Gmail search. `filename_contains` is only a coarse server-side
  narrowing; the precise selection is `attachment_pattern` (`re.search`).
- **`has:attachment` counts inline images.** A logo in a signature
  (`image001.png`) is an attachment too. The provider keeps only parts with a
  non-empty `filename` and drops a part **only** if it is inline *and*
  `mime_type` starts with `image/`. Inline PDFs/xlsx are kept (a `Content-ID`
  does not demote them); `attachment_pattern` guards against extras.
- **Nested labels are not hierarchical.** `airflow/processed` does **not** cover a
  message labeled only `airflow/processed/avito`. The processed-label dedup resolves
  that exact final string to its `labelId` (`find_label_id`) and compares it against
  each message's `labelIds` in code — so nesting never causes a false match, and no
  `-label:` query term is involved.
- **`attachmentId` is unstable** between requests — it is used immediately after
  `messages.get` and never stored.
- **Filenames arrive already decoded.** Gmail returns `MessagePart.filename` as a
  ready UTF-8 string (a Cyrillic `Отчёт за июль.xlsx`, not the raw
  `=?UTF-8?B?...?=` of the `Content-Disposition` header), so the provider applies
  **no** RFC 2047/2231 filename decoding — only path sanitization. This was
  confirmed against the Task 2 fixtures. Only `Subject`/`From` (read from
  `payload.headers`) are RFC 2047-decoded, since those do carry encoded-words.
- **A textual `after:` is interpreted in Gmail's timezone**, not yours, so the
  window edge drifts. The provider always emits a **numeric** `after:<epoch>` (and
  `before:<epoch>`) computed from midnight of the reference day in the operator's
  `timezone`, leaving no room for interpretation.

## The `_manifest.json` contract (layer-2 join)

The manifest is the contract with the next layer, so its schema is identical in
the plan, in `docs/gmail-pipeline-layers-2-3.md`, in `manifest.py`, and here:

```json
{"source": "avito",
 "message_id": "18c2f4a9b3d5e6f7",
 "internal_date": "2026-07-10T09:14:22+03:00",
 "subject": "Отчёт за 09.07",
 "from": "reports@avito.ru",
 "run_id": "scheduled__2026-07-10T06:00:00+00:00",
 "files": [{"name": "report.xlsx", "size": 148223,
            "path": "gmail/avito/dt=2026-07-10/18c2f4a9b3d5e6f7/report.xlsx"}]}
```

- `source` — free-form trace label, not part of the path.
- `message_id` — the opaque per-mailbox message id (`users.messages.list` → `id`),
  a hex string safe for paths. Not the RFC `Message-ID` header, not `attachmentId`.
- `internal_date` — ISO 8601 in the operator `timezone` (default `+03:00`).
- `subject` / `from` — decoded from RFC 2047 (Cyrillic subjects arrive as
  `=?UTF-8?B?...?=`).
- `run_id` — the `DagRun` that wrote the manifest. Layer 1 uses it to tell
  "delivered by a past run" (do not re-deliver) from "written by a failed attempt
  of this same run" (deliver, do not re-download). **Layer 2 ignores this field.**
- `files[].name` — the original attachment name before sanitization.
- `files[].size` — the actual number of bytes written (`len(data)` after decode).
- `files[].path` — the **canonical** destination path: for S3 the object key inside
  `bucket` (`<prefix>/dt=…`), for local the absolute disk path. There is **no**
  `s3_key` field — layer 2 must not know where the file came from.

The manifest is always written **last**, after every attachment of a message, so
its presence proves the attachments landed. A corrupt/invalid manifest raises a
loud `ManifestError` rather than being silently skipped.

## What goes to XCom

The operator returns **only the full paths of the manifests** of the messages
processed **in this run** — nothing else.

- In S3 mode each path is a full `s3://<bucket>/<key>` URI; in local mode it is an
  absolute disk path. The contract is uniformly "a list of full manifest paths",
  so a consumer never has to source the bucket out of band. (Before 0.3.0 the S3
  operator returned bare object keys — a breaking change, see the CHANGELOG.)
- **The URI still needs the consumer's `aws_conn_id`.** An `s3://bucket/key` URI
  names the object *within a store*, but the store's `endpoint_url` and
  credentials live in the Amazon Connection, not in the URI. The downstream task
  (the resolver, or your own) takes `aws_conn_id` from its own parameter — usually
  the same one — and the bucket is read straight off the URI.
- No richer structured object is introduced: a full path is exactly what a
  consumer needs, and the manifest schema (`files[].path`) is deliberately left as
  keys / absolute paths so layer 2 never learns where a file came from.

## Resolving attachments

Reading each `_manifest.json` yourself is a supported contract, but the provider
ships the **official client** of that contract so a consumer does not have to know
the manifest schema at all: it turns the download operator's XCom (a list of
manifest paths) into a **flat list of full attachment paths**.

Two faces, same logic:

- **`resolve_attachments(manifests, pick="all", aws_conn_id="aws_default")`** — a
  function, for the TaskFlow API.
- **`GmailResolveAttachmentsOperator`** (`operators.resolve`) — a thin operator
  wrapper with template fields `manifests` and `pick`, for declarative DAGs.

`pick` selects which messages contribute attachments:

- **`"all"`** (default) — every manifest's attachments, in input order.
- **`"latest"`** — only the attachments of the single most-recent manifest by
  `internal_date` (compared as aware moments, tie-broken by `message_id`). Use it
  when the same report may arrive more than once and you want only the newest.

The recommended layer-1 → layer-2 chain wires the resolver between the download
and the parse; a layer-2 operator such as `TableFileToS3Operator` then takes the
resolved paths as its `input_paths` and never touches a manifest:

```python
download = GmailAttachmentsToS3Operator(...)          # → [s3://.../_manifest.json, ...]
resolve = GmailResolveAttachmentsOperator(
    task_id="resolve", manifests=download.output, pick="latest")  # → [s3://.../report.xlsx, ...]
parse = TableFileToS3Operator(input_paths=resolve.output, ...)    # layer 2, another provider
download >> resolve >> parse
```

The resolver does **not** repair or second-guess its input: duplicate manifests
pass through as given, and a **missing** manifest (a good path but no object —
deleted, retention, or an `aws_conn_id` pointing at the wrong store) or a
**broken** one raises loudly (the storage error / `ManifestError`) rather than
silently yielding a short list. An empty input list yields an empty list; `None`
(e.g. an `xcom_pull` on the wrong `task_id`) raises `TypeError` rather than a
forever-green empty pipeline. A purely-local input never imports the Amazon
provider (the `s3` extra is not required).

## Example DAGs

See `example_dags/`:

- **`example_gmail_to_s3.py`** — the standard daily pull:
  `GmailAttachmentToS3Sensor` → `GmailAttachmentsToS3Operator` →
  `GmailResolveAttachmentsOperator` → a stub parse task, `mark_processed=False`,
  `max_active_runs=1` (with a comment on the check-then-act race). Shows the
  resolver wired between download and parse. The plain `GmailAttachmentSensor`
  must **not** be used here — with labels off it would re-fire on an
  already-processed message.
- **`example_gmail_to_local.py`** — download → parse → cleanup (`all_done`), with the
  worker-limit constraint and the non-idempotency spelled out.
- **`example_gmail_s3_backfill.py`** — a manual replay with `overwrite=True` and
  **no** `GmailAttachmentToS3Sensor` (the sensor would see the past-run manifest
  and never let the operator run), filling `date_from`/`date_to` from `dag_run.conf`
  (ADR-0004).

## Installation

```bash
pip install airflow-provider-gmail          # Gmail hook, local operator, Gmail-only sensor
pip install "airflow-provider-gmail[s3]"    # + the S3 operator and storage-aware sensor
```

The `s3` extra pulls `apache-airflow-providers-amazon`; the S3 pieces import it
lazily, so the hook, the local operator, and `GmailAttachmentSensor` work without
it.
