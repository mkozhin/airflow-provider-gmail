# AGENTS.md

This file provides guidance to coding agents (Claude Code, and others) when working with code in this repository.

## Status: implemented

The provider is fully implemented (`src/airflow_provider_gmail/`), tested
(`tests/`, 99% coverage), and CI/publish workflows are in place (`.github/workflows/`).
The design docs remain authoritative for *behavior*; consult them before changing it.

- `docs/plans/completed/20260710-airflow-provider-gmail.md` — the completed master
  implementation plan (16 numbered tasks; some split into sub-tasks, e.g. `7a`/`7b`,
  `15a`–`15c`). Historical record of what was built and why. Progress markers:
  `[x]` done, `➕` new task, `⚠️` blocker.
- `CONTEXT.md` — domain glossary (canonical terms).
- `docs/adr/000*-*.md` — architectural decisions (delivery contract, timezone, backfill, etc.). Consult before changing behavior they cover.

## Development commands

Install (the constraint pin is mandatory, else `>=2.9,<3` pulls Airflow 2.11):

```
pip install -e ".[dev,s3]" --constraint \
  https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt
```

(Match the `-3.12` suffix to the environment's Python: `3.10`/`3.11` as needed.)

- Full test suite: `pytest` (the `packaging` marker is deselected by default)
- Coverage: `pytest --cov=airflow_provider_gmail --cov-report=term-missing`
- Packaging smoke test (build → wheel → `ProvidersManager`): `pytest -m packaging`.
  First install the build tooling **unconstrained** — the Airflow 2.9.1
  constraints pin `twine==5.0.0`/`packaging==24.0`, which reject the wheel's PEP
  639 Metadata 2.4 (and `twine>=6.1` is unsatisfiable under them):
  `pip install --upgrade build "twine>=6.1" "packaging>=24.2"` (the `packaging`
  extra). Run it explicitly whenever you touch packaging.

## Domain language

Use the **canonical terms from `CONTEXT.md`** in code, tests, docstrings, and ADRs — no synonyms. When terminology is unclear, `CONTEXT.md` and the ADRs win.

## Language conventions

- README and user-facing documentation: **separate file per language**, not one bilingual document. English is primary (`README.md`), Russian alongside (`README_RU.md`); same pattern for other docs.
- Changelog: **English**.
- Internal design docs (`CONTEXT.md`, `docs/adr/`, `docs/plans/`) are currently written in Russian.

## Provider structure & publishing

This provider follows the standard layout codified by the **`airflow-pypi-provider`** skill (pyproject.toml + setuptools-scm + tag-based PyPI publish workflow + entry-point registration). Invoke it when scaffolding structure or the publish workflow.

- Target: **Python 3.10+**, Apache Airflow `>=2.9,<3` (runtime 2.9.1).
- Build backend: setuptools + **setuptools-scm** (git-tag versioning), `src/`-layout.
- Provider registration is via the `apache_airflow_provider` entry point → `airflow_provider_gmail:get_provider_info` — **there is no `provider.yaml`**. Do not add `connection-types` to `get_provider_info()` until `GmailHook` exists, or `ProvidersManager` fails to load.

## Gotchas

- **setuptools-scm resolves the version from git tags** (`fallback_version = "0.0.0"` covers a `.git`-less archive); a working tree needs at least one commit.
- Always install with the constraint pin (see *Development commands*), otherwise `>=2.9,<3` pulls Airflow 2.11.
- Tests use pytest; mock at the `googleapiclient` service level (`hook.get_conn()`), no network.
- The packaging smoke test (`python -m build` → install wheel → assert `get_provider_info()` / `ProvidersManager`) is marked `@pytest.mark.packaging` and **excluded from the default run** (slow/brittle). Run it explicitly when touching packaging.

## Domain traps (see ADRs / plan)

- OAuth app must be **"In production"** — in "Testing" status Google issues a 7-day `refresh_token` and the pipeline silently dies after a week.
- The Gmail Connection is **read-only**: the hook does a refresh-grant into memory each run; no `access_token` is stored.
- `S3Hook.load_bytes(..., replace=True)` is mandatory — the default `replace=False` breaks re-runs.
- Delivery dedup exists **only in S3 mode** (via `_manifest.json` + `run_id`) and requires the DAG's `max_active_runs=1`.
- Processed-mail filtering (opt-in `mark_processed`, local operator + Gmail sensor) drops already-labeled mail by comparing the message's `labelIds` against the label ID **in code** (`find_label_id` → `exclude_label_id`) — **not** a `-label:` query term (that relied on undocumented Gmail search behavior and could fail open). It does not affect S3 delivery dedup, which never filters by label.
- `pick="all"` sorts by `internal_date` ascending (ADR-0008) — a manifest with a
  malformed/naive `internal_date` now raises `ValueError` on `pick="all"` too
  (previously only `pick="latest"` read the field), and interleaved duplicates
  of the same manifest get regrouped by the sort, not preserved in input position.
