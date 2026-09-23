# SPEC 124 — One honest dataset status API

**Status:** Implemented (branch spec-124; live verification pending)
**Task type:** api_endpoint
**Date:** 2026-09-23
**Plan:** PLAN_087 wave 2; PLAN_085 §3 (three clocks, status taxonomy), §5 (endpoint), rev_01; review `docs/reviews/2026-09-23_daas_platform_review.md` §4 changes 5-6
**Builds on:** SPEC_123 (catalog), SPEC_126a (`core.mart_build`, `PARTIAL:`), SPEC_122 (`superseded:` releases), SPEC_121 (job write-back), SPEC_127 (roles), SPEC_128 (watchdog helpers)
**Test file:** tests/test_spec_124_dataset_status_api.py

## Goal

Five status surfaces exist and all of them are keyed by source, read run
recency as freshness, and call "no expectation" fresh. The catalog (SPEC_123)
now names every dataset; this spec answers, for each of them, in one call:

- **when did we last collect** (run clock),
- **when did upstream last publish** (publish clock, bulk sources),
- **what period does the data cover, and what should it cover by now**
  (coverage clock),

and turns that into one status from a closed vocabulary. It adds the admin
"run now" with a verdict that explains itself, and an actor on the audit row.

## Endpoints

| Route | Access | Purpose |
|---|---|---|
| `GET /api/v1/datasets/status` | any signed-in user (router `_auth`) | every catalog dataset, filters `status_public`, `kind`, `source`, `status` |
| `POST /api/v1/datasets/{key}/run` | admin only (`require_admin` on the route, and `_auth` on the router) | enqueue the dataset's producer; 409 + the verdict when refused |

The router shares the `/datasets` prefix with `/datasets/freshness`.

## Response (pydantic, `app/api/v1/dataset_status.py`)

```jsonc
{
  "generated_at": "...Z",
  "summary": {"current": 2, "awaiting_upstream": 0, "behind": 1, "stalled": 3,
              "failing": 0, "partial": 0, "blocked": 11, "dormant": 60,
              "never_run": 70, "unknown": 3},
  "count": 150, "total": 150,
  "worker": {"worker_mode": true, "live_workers": 1, "last_heartbeat_at": "..."},
  "datasets": [{
    "key": "sec_adv_schedule_d", "display_name": "...", "source": "sec",
    "kind": "filings", "grain": "...", "cadence": "monthly",
    "status_public": "internal", "producer": "bulk:sec_adv_schedule_d",
    "status": "current", "status_reason": "coverage 2026-08-31 meets expected 2026-08-31",
    "tables": [{"name": "sec_adv_filings", "exists": true, "rows": 561175,
                "rows_exact": false, "bytes": 216006656}],
    "rows_total": 561175, "rows_exact": false,
    "clocks": {"last_run_at": "...", "last_run_status": "success",
               "last_run_store": "job_queue", "last_run_duration_s": 141,
               "last_success_at": "...", "last_publish_at": "...",
               "coverage_through": "2026-08-31", "expected_through": "2026-08-31",
               "expectation_basis": "coverage", "lag_days": 0},
    "releases": {"loaded": 20, "unloaded": 0, "failed": 0, "superseded": 0,
                 "latest_release_key": "...", "last_bytes": 1234},
    "schedule": {"kind": "schedule", "schedule_id": 4, "name": "...",
                 "cron": "0 8 4 * *", "active": true,
                 "next_run_at": "...", "next_run_source": "apscheduler",
                 "last_success_at": "...", "missed_runs_30d": 0},
    "blockers": [],
    "can_run": {"allowed": true, "requires": "admin", "policy": "idempotent",
                "producer": "bulk:sec_adv_schedule_d", "run_path": "bulk_ingest",
                "blockers": [], "warnings": [{"code": "download", "message": "..."}],
                "estimated_duration_s": 141, "estimated_bytes": 1234}
  }]
}
```

## Three clocks

**Run clock** — a UNION of every trustworthy store, mapped to datasets through
the catalog's producer index (`app/catalog/job_keys.py`):

| Store | Mapped by | Contributes |
|---|---|---|
| `job_queue` | `bulk_ingest` → `bulk:<payload.bulk_source>`; `ingestion` → dispatch key from `payload.source` + `payload.config.dataset` (same resolution as `jobs._run_dispatched_job`); `site_intel` → `collector:<payload.sources[]>`; other types → `job:<type>` | every status (active rows only within 24 h) |
| `ingestion_jobs` | `dataset_key` when set, else the same resolution from `source` + `config.dataset` | `success` and `failed` only: PENDING/RUNNING rows there are the zombies PLAN_085 §8.2 found |
| `raw.source_release` | `bulk:<source>` | `loaded_at` = success; non-superseded failures = failed |
| `core.mart_build` | `pe_marts` → `job:pe_mart_build`, `entity_resolve` → `job:entity_resolve` | latest non-dry-run finished build; `busy:` refusals ignored |
| `site_intel_collection_job` | `collector:<source>` | collector history |

Statuses are compared case-insensitively (`LOWER(status)`). A `job:<type>`
run counts for every stage dataset of that type (`job:pe_mart_build#firms`,
`#funds`, ...). `success` with `error_message LIKE 'PARTIAL:%'` is `partial`.

**Publish clock** — bulk only: newest `discovered_at` of a non-superseded
release. Release counts: `loaded`, `unloaded` (not loaded, not superseded,
discovered at/after the newest loaded release), `failed` (not superseded),
`superseded` (`failed` + `superseded:`, SPEC_122 — never treated as failed).

**Coverage clock** — `coverage_through` from the catalog `coverage_sql`, all
datasets in ONE statement (scalar subqueries) under `statement_timeout`
(3 s), falling back to per-dataset `live.coverage_through` (1 s each, 5 s
overall, the rest "unavailable") only if the combined statement fails;
cached 5 minutes. Only datasets whose declared tables exist are measured. A hit in the SPEC_123 live cache is used
as is. `expected_through` = the last cadence period end (day / Sunday /
month / quarter / year) strictly before `now - slo_lag_hours`. No
`slo_lag_hours`, or an `ad_hoc` cadence, means **no expectation**.
Datasets with an SLO but no `coverage_sql` (the PE marts) fall back to
`expectation_basis = "last_success"`: the last success must be within
cadence + SLO lag. `lag_days = max(0, expected_through - coverage_through)`.

## Status taxonomy

First match wins:

| # | Status | Rule |
|---|---|---|
| 1 | `blocked` | a required API key is missing (`app.core.preflight.api_key_preflight`, formerly `jobs._check_api_key_preflight`); or the dataset is scheduled, needs a worker, and no `worker_heartbeats` row is live (5 min); or its mart's latest real build was `refused` on its inputs |
| 2 | `failing` | latest terminal run failed within 30 days |
| 3 | `partial` | latest terminal run was `PARTIAL:` within 30 days |
| 4 | `never_run` | no run evidence in any store and no rows in its tables (a legacy load with no run record is judged by the later rules) |
| 5 | `stalled` | an active schedule's own last success (batched `last_success_by_schedule`, same evidence as SPEC_128's `last_success_for_schedule`) is older than 1.5x its cadence, or it never succeeded and is older than that; batch-scheduled dispatch keys use the catalog cadence and the dataset's last success |
| 6 | `dormant` | no active schedule and not in the nightly batch |
| 7 | `behind` | bulk: a published release newer than the newest loaded one is not loaded |
| 8 | `unknown` | no expectation (no SLO / ad hoc cadence / coverage unavailable) — never `current` |
| 9 | `current` | coverage meets expectation (or, for `last_success` basis, the last success is within cadence + SLO) |
| 10 | `awaiting_upstream` | bulk, coverage below expectation, every published release loaded, and the next publish is not yet due (last publish + 1.5x cadence is in the future) |
| 11 | `behind` | otherwise |

`summary` has every status key (zero-filled).

## Schedule block

The dataset's active `ingestion_schedules` row (else its newest inactive one),
or `{"kind": "batch", "cron": "0 2 * * *"}` for the nightly batch's dispatch
keys. `next_run_at` comes from the live APScheduler job (`schedule_<id>` /
`batch_collection`) when the process-wide scheduler is running
(`next_run_source = "apscheduler"`), else the DB column (`"db"`), else none.
`missed_runs_30d` = cron fires in the last 30 days (since the schedule was
created) minus runs the schedule created in that window, floor 0; `null` when
the cadence is not a cron/frequency or for the batch.

## can_run verdict

`{allowed, requires: "admin", policy (catalog rerun), producer, run_path,
blockers[], warnings[], estimated_duration_s, estimated_bytes}`. Blockers and
warnings are `{code, message}`.

Blockers: `no_run_path` (`api:*` routers and the collection job types
people/pe/lp/fo/agentic/foot_traffic, which need per-target payloads),
`missing_api_key`, `worker_mode_off` (bulk/job/collector need the queue),
`no_live_worker` (queue mode), `already_running` (an active queue row for the
producer within 24 h, or a `running` mart build).

Warnings: `download` (bytes of the last fetched release), rerun policy notes
(`destructive`, `currency_only`, `append_only`), `mart_refused` (the last
build was refused; a rerun will be too unless inputs changed), `shared_run`
(a `job:` type rebuilds all its stage datasets), `default_config` (dispatch
key with no nightly-batch default config).

## Run

`POST /datasets/{key}/run` (admin): 404 unknown key; 409
`{"detail": ..., "can_run": verdict}` when not allowed; else enqueue through
the producer's existing path and return 202:

| Producer | Path |
|---|---|
| `bulk:<name>` | `IngestionJob(source="bulk:<name>")` + `bulk_ingest` queue job with `ingestion_job_id` (the `_run_bulk_schedule` shape, trigger `manual`) |
| `job:entity_resolve`, `job:pe_mart_build` | `IngestionJob(source="job:<type>")` + queue job (the `_run_job_schedule` shape) |
| `dispatch:<key>` | `IngestionJob(source=<base>, config={dataset, ...batch default})` + `ingestion` queue job, or `BackgroundTasks` → `run_ingestion_job` when `WORKER_MODE` is off (the `POST /jobs` shape) |
| `collector:<value>` | `site_intel` queue job `{"sources": [value]}` |

Every run writes a `collection_audit_log` row with `actor` (principal email /
name), `trigger_type="api"`, `trigger_source="/datasets/{key}/run"` and the
dataset key, producer and job ids in `config_snapshot`.

## Schema — Alembic `0014_dataset_status` (down_revision `0013_mart_build`)

- `collection_audit_log.actor VARCHAR(255) NULL`
- `ingestion_jobs.dataset_key VARCHAR(64) NULL` + index `ix_ingestion_jobs_dataset_key`

Both guarded by `to_regclass` (created lazily by `create_all` on a fresh
database, which runs after migrations) and `ADD COLUMN IF NOT EXISTS`
(metadata-only on PG 11+ for a nullable column without default, so safe on
the populated tables). The index is a plain `CREATE INDEX IF NOT EXISTS`
(ingestion_jobs is ~5k rows). No backfill: history is resolved at read time.

`dataset_key` is projected for new jobs by a `before_insert` listener on
`IngestionJob` (every enqueue path: `POST /jobs`, schedules, batch, retries,
this endpoint) when the producer maps to exactly one dataset; `job:` types
with several stage datasets stay NULL unless the run endpoint sets it.

## `/datasets/freshness`

SPEC_128 already fixed both defects (never-succeeded sources that were
attempted, scheduled or SLA'd are listed; no SLA and no schedule →
`unknown`). Not changed here; catalog datasets that were never attempted are
reported by `/datasets/status` as `never_run`.

## Performance

A constant number of statements per request, independent of the number of
job/release/schedule rows: table existence, 5 run stores, schedules,
per-schedule success, per-schedule runs, heartbeats, relation stats (≤ 12
with a warm coverage cache), plus the combined coverage statement (SET LOCAL
+ SELECT) when the 5-minute cache is cold. Only when that combined statement
fails (a coverage SQL that does not match the table on this database) does
coverage fall back to one statement per coverage dataset -- bounded by the
catalog (11 datasets have coverage SQL), deadline-capped, and cached. The
APScheduler job store is read once when the in-process scheduler runs. Row
counts are planner estimates from the one `pg_class` query
(`rows_exact=false`) unless the SPEC_123 live cache holds exact counts. T14
asserts the count is identical as rows grow and bounded cold and warm.

## Acceptance Criteria

- [x] Every catalog dataset appears; filters by status_public/kind/source/status; 422 on bad values.
- [x] Each taxonomy branch derives on PG fixtures (T3-T13).
- [x] `superseded:` releases are never failures.
- [x] Statement count is constant as datasets/jobs grow (T14).
- [x] `summary` counts every status; response validates against the pydantic model.
- [x] Run endpoint: 403 for users, 404 unknown, 409 + verdict when blocked, enqueues bulk / job / dispatch / collector, audit row with actor, `dataset_key` on the job.
- [x] Migration 0014: revision chain, guarded DDL, applies twice on PG.
- [x] `api_key_preflight` exposed; `jobs._check_api_key_preflight` still works.

## Test Plan

| ID | Test | Kind |
|---|---|---|
| T1 | expected_through per cadence; no SLO → None | unit |
| T2 | job → producer → dataset resolution (dispatch/bulk/job stages/queue/collector); listener projects dataset_key | unit |
| T3 | never_run (no evidence) and dormant (evidence, no schedule) | PG |
| T4 | blocked: missing key; no live worker for scheduled worker dataset; refused mart | PG |
| T5 | failing: latest failed; old failure is not failing | PG |
| T6 | partial: PARTIAL: success | PG |
| T7 | stalled: active schedule, own success too old; other schedule does not mask | PG |
| T8 | current + lag 0 via coverage; unknown without SLO; last_success basis; one bad coverage SQL does not blank the others | PG |
| T9 | behind: unloaded newer release; coverage below expectation with overdue publish | PG |
| T10 | awaiting_upstream | PG |
| T11 | superseded releases not failures | PG |
| T12 | case-insensitive statuses (`'FAILED'`, `'SUCCESS'`) | PG |
| T13 | schedule block: db next_run, missed runs, apscheduler source | PG |
| T14 | query count bound (event counter), constant as rows grow | PG |
| T15 | endpoint filters, summary, pydantic model | PG |
| T16 | run: auth 403, 404, 409 with verdict, enqueue per producer kind, audit actor, dataset_key | PG |
| T17 | migration 0014 chain + applies twice (guards) | unit + PG |
| T18 | batched last_success_by_schedule agrees with last_success_for_schedule | PG |

## Files

| File | Action |
|---|---|
| alembic/versions/0014_dataset_status.py | Create |
| app/core/preflight.py | Create (`api_key_preflight`) |
| app/api/v1/jobs.py | Modify (`_check_api_key_preflight` delegates) |
| app/catalog/job_keys.py | Create (job/queue/release → producer → datasets) |
| app/core/models.py | Modify (`IngestionJob.dataset_key`, `CollectionAuditLog.actor`, listener) |
| app/core/audit_service.py | Modify (`actor`) |
| app/services/data_watchdog.py | Modify (batched `last_success_by_schedule`) |
| app/services/dataset_status.py | Create (facts, derivation, verdict, enqueue) |
| app/api/v1/dataset_status.py | Create (router + pydantic models) |
| app/main.py | Modify (register router + OpenAPI tag) |
| tests/test_spec_124_dataset_status_api.py | Create |

## Out of scope

Row-count exactness beyond the SPEC_123 cache, pause/resume from the page,
SPEC_125 (the page), backfilling `dataset_key` for history, cadence profiles
(SPEC_126b).
