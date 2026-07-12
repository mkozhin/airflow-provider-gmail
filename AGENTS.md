# AGENTS.md

This file provides guidance to coding agents (Claude Code, and others) when working with code in this repository.

## Status: pre-implementation

There is **no source code, no `pyproject.toml`, no tests, no CI yet** — only design docs.
The design is authoritative; build it from the docs, don't invent structure ahead of them.

- `docs/plans/20260710-airflow-provider-gmail.md` — master implementation plan (16 numbered tasks; some split into sub-tasks, e.g. `7a`/`7b`, `15a`–`15c` — ~19 execution units). Drives the work; keep it updated as tasks land. Progress markers: `[x]` done, `➕` new task, `⚠️` blocker.
- `CONTEXT.md` — domain glossary (canonical terms).
- `docs/adr/000*-*.md` — architectural decisions (delivery contract, timezone, backfill, etc.). Consult before changing behavior they cover.

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

- **setuptools-scm needs an initial git commit** before any install/test can resolve a version (`fallback_version = "0.0.0"`).
- Install with the constraint pin, otherwise `>=2.9,<3` pulls Airflow 2.11:
  `pip install -e ".[dev,s3]" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt`
- Tests use pytest; mock at the `googleapiclient` service level (`hook.get_conn()`), no network.
- The packaging smoke test (`python -m build` → install wheel → assert `get_provider_info()` / `ProvidersManager`) is marked `@pytest.mark.packaging` and **excluded from the default run** (slow/brittle). Run it explicitly when touching packaging.

## Domain traps (see ADRs / plan)

- OAuth app must be **"In production"** — in "Testing" status Google issues a 7-day `refresh_token` and the pipeline silently dies after a week.
- The Gmail Connection is **read-only**: the hook does a refresh-grant into memory each run; no `access_token` is stored.
- `S3Hook.load_bytes(..., replace=True)` is mandatory — the default `replace=False` breaks re-runs.
- Delivery dedup exists **only in S3 mode** (via `_manifest.json` + `run_id`) and requires the DAG's `max_active_runs=1`.
