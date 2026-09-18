# SPEC 107 — BulkSource framework (SEC HTTP, raw manifest, COPY loader, worker job)

**Status:** Draft
**Task type:** service
**Date:** 2026-09-16
**Test file:** tests/test_spec_107_bulk_source_framework.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 1)

## Goal

Phase 1 loaders (Form D, 13F, insider, EDGAR, ADV, XBRL) all download a published bulk file, keep it, and load it quickly. Build that shared machinery once:
- a single polite SEC HTTP client that can stream to disk
- a release manifest that doubles as a checkpoint
- COPY-based staging with a set-based merge
- a worker job type

## Acceptance Criteria

- [ ] **`app/core/sec_http.py` `SecHttp`**
  - [ ] Sends User-Agent from `settings.sec_user_agent` (default `"Nexdata research alexiusmichael@gmail.com"`); callers can't override it.
  - [ ] Rate-limits all SEC hosts through one token bucket at 8 req/s in-process. With a session factory, it also uses the distributed `sec.gov` bucket and fails closed: raise after a 60s wait, never proceed unthrottled.
  - [ ] Retries 429/5xx/network errors with backoff. Honors `Retry-After`, capped at 60s.
  - [ ] `get_text(url)`, and `stream_to_file(url, dest)` which writes to `.part`, validates magic bytes (zip/gzip/json/html-for-data), renames atomically and returns sha256/bytes/etag/last_modified.
- [ ] **`app/core/copy_loader.py`**
  - [ ] `create_staging(conn, name, columns)` creates or truncates UNLOGGED `stg.<name>` with a `_row_num` column.
  - [ ] `copy_rows(conn, name, columns, rows) -> int` streams CSV through psycopg2 `COPY FROM STDIN`.
  - [ ] `merge_staging(conn, name, target, columns, key_columns, update_columns=None) -> (inserted, updated)` dedupes within the batch (last row wins), runs `INSERT … ON CONFLICT DO UPDATE`, and counts via `xmax = 0`.
  - [ ] `drop_staging(conn, name)`.
- [ ] **`app/ingest/bulk/base.py`**
  - [ ] `Release(release_key, url, meta)`.
  - [ ] `BulkSource` ABC: `name`, `parser_version`, `discover(http, since)`, `fetch` (default streams the URL), `load(conn, release, path) -> dict` (stage + merge), `ddl() -> list[str]`.
  - [ ] `run_source(source, since=None, max_releases=None, release_keys=None, ...)` walks `raw.source_release` statuses `discovered → fetched → loaded | failed`.
  - [ ] A loaded release is skipped.
  - [ ] A fetched release whose file exists with a matching sha256 is loaded without re-downloading.
  - [ ] Load runs in one transaction per release.
  - [ ] A failure marks the release `failed` with the error, and the run continues with the next release.
- [ ] **`app/ingest/bulk/registry.py`**: `register_bulk_source` decorator, `get_source(name)`, `list_sources()`. Auto-imports `app.ingest.bulk.*` subpackages.
- [ ] **Alembic `0004_bulk_framework`**: schemas `raw` and `stg`, plus table `raw.source_release`:
  - columns `id`, `source`, `release_key`, `url`, `status`, `local_path`, `bytes`, `sha256`, `etag`, `last_modified`, `rows_loaded` (jsonb), `parser_version`, `error`, `discovered_at`, `fetched_at`, `loaded_at`, `updated_at`
  - `UNIQUE(source, release_key)`
- [ ] **Worker**: `QueueJobType.BULK_INGEST`; executor `app/worker/executors/bulk_ingest.py` runs `run_source` via `asyncio.to_thread` so heartbeats continue. It raises if every attempted release failed, and records "warning: 0 rows loaded" in `progress_message` when nothing loaded.
- [ ] **API** `app/api/v1/bulk.py`:
  - [ ] `GET /bulk/sources`.
  - [ ] `POST /bulk/{source}/run?since=&max_releases=` queues a `bulk_ingest` job (404 for an unknown source).
  - [ ] `GET /bulk/releases?source=&status=`.
- [ ] **Settings and mounts**: `sec_user_agent`, `bulk_raw_dir` (default `data/raw`). docker-compose mounts `./data/raw` into api and worker. `rate_limiter.DISTRIBUTED_RATE_LIMITS["sec.gov"]` is 8 tokens / 8 per second.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_validate_payload_rejects_html_for_zip | Magic-byte validation |
| T2 | test_stream_to_file_atomic_and_hashed | File written, no .part left, sha256 matches, UA header sent |
| T3 | test_retry_after_is_capped | 429 with Retry-After 3600 sleeps ≤ 60 and then succeeds |
| T4 | test_failed_download_leaves_no_file | Invalid payload leaves no file and no .part |
| T5 | test_merge_sql_dedupes_and_counts | SQL has DISTINCT ON keys ORDER BY _row_num DESC, ON CONFLICT, xmax |
| T6 | test_copy_and_merge_pg | (PG) COPY 3 rows with 1 duplicate key → inserted 2; re-merge a changed row → updated 1 |
| T7 | test_run_source_loads_and_skips_loaded_pg | (PG) 2 releases load; second run does nothing |
| T8 | test_run_source_resumes_without_redownload_pg | (PG) load failure → failed; next run reuses the file (fetch not called) and loads |
| T9 | test_registry_register_and_get | Decorator registers; unknown name raises KeyError |
| T10 | test_executor_runs_in_thread_and_fails_when_all_failed | `asyncio.to_thread` used; RuntimeError when all releases failed |
| T11 | test_bulk_run_endpoint_queues_job | `submit_job` called with job_type bulk_ingest and payload; unknown source → 404 |
| T12 | test_migration_0004_chain | 0004 revises 0003 and creates `raw.source_release` |

## Rubric Checklist

_No service rubric found in memory/rubrics; generic:_
- [ ] Bounded concurrency and polite rate limiting (8 req/s, SEC fair-access UA)
- [ ] Parameterized SQL; identifiers via `safe_sql.qi`
- [ ] Idempotent re-runs
- [ ] Blocking I/O off the event loop

## Design Notes

- Loaders read only from files. Discovery is the only step that needs index pages.
- Staging tables are dropped after each release's merge to keep Cloud SQL disk small, and raw files stay on local disk.
- Payload uses the `bulk_source` key rather than `source`, so the worker's per-source in-memory limiter and timeout lookup don't apply to long bulk loads.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/sec_http.py, app/core/copy_loader.py | Create |
| app/ingest/__init__.py, app/ingest/bulk/{__init__,base,registry}.py | Create |
| alembic/versions/0004_bulk_framework.py | Create |
| app/worker/executors/bulk_ingest.py | Create |
| app/api/v1/bulk.py | Create |
| app/core/models_queue.py, app/worker/main.py, app/main.py, app/core/config.py, app/core/rate_limiter.py, docker-compose.yml | Modify |

## Feedback History

_No corrections yet._
