# SPEC 121 — Schedule and job reliability

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-23
**Plan:** PLAN_086 ("Now"), review `docs/reviews/2026-09-23_daas_platform_review.md` §4 and §5 rows 1, 5, 26
**Test file:** tests/test_spec_121_schedule_job_reliability.py
**Deadline:** before 2026-10-04 (first unattended bulk cycle; marts on 10-10)

## Goal

A scheduled job must never block its own schedule forever, and a missed fire
time must not be silently dropped.

Today a `job:<type>` schedule runs once and then freezes:

1. `_run_job_schedule` creates an `IngestionJob` (PENDING) and queues a worker
   job whose `job_table_id` points at it.
2. The worker (`worker/main.py:306`) updates only `job_queue`. `pe_marts` and
   `entity_resolve` never touch `ingestion_jobs`, so the row stays PENDING.
3. Nothing sweeps it: `cancel_stale_pending_jobs` selects unclaimed queue rows,
   `cleanup_stuck_jobs` selects RUNNING ingestion rows.
4. `run_scheduled_job` skips any schedule with a PENDING/RUNNING job, logging at
   INFO. Row 3891 (`job:pe_mart_build`) is such a zombie; the 10-10 mart run
   would be skipped.

Separately, `bulk_ingest` leaves its `IngestionJob` RUNNING when `discover()`
raises; APScheduler drops any run more than 1 s late (no `job_defaults`); api and
worker have no restart policy; the proxy image floats on `:latest`.

## Acceptance Criteria

- [ ] When a worker job linked to an `ingestion_jobs` row finishes (success,
      failure, cancel, no executor, drain timeout), the row gets the same
      outcome, `completed_at`, and on failure the error.
- [ ] The write-back never overrides a terminal row an executor already wrote
      (idempotent), and never touches rows that `job_table_id` does not truly
      reference (agentic, LP).
- [ ] A periodic sweep reconciles PENDING/RUNNING `ingestion_jobs` whose latest
      linked queue row is terminal (`success`/`failed`/`cancelled`, any case).
      Row 3891 is repaired on the first sweep after deploy.
- [ ] The sweep fails PENDING (and never-started RUNNING) rows with no queue
      row at all after 24 h.
- [ ] `bulk_ingest` always finishes its `IngestionJob`, including when
      discovery raises or the task is cancelled.
- [ ] The scheduler has `job_defaults` `{coalesce: True, max_instances: 1,
      misfire_grace_time: 3600}`; each registered schedule gets a grace scaled
      to its cadence; a run missed while the API was down is kept on
      re-registration if it is within that grace.
- [ ] A schedule skipped because its previous job is still active logs at
      WARNING; if that job is older than 2x the schedule's cadence it is
      reconciled and the schedule proceeds.
- [ ] `docker-compose.yml`: api and worker `restart: unless-stopped`, depend on
      a healthy `cloudsqlproxy`, proxy image pinned.

## What `job_table_id` points at

| Submitter | job_type | `job_table_id` → | payload `ingestion_job_id` |
|---|---|---|---|
| `scheduler_service._run_bulk_schedule` | bulk_ingest | ingestion_jobs | yes |
| `scheduler_service._run_job_schedule` | any (`job:<type>`) | ingestion_jobs | yes |
| `jobs.py` create / retry / restart / retry-all | ingestion | ingestion_jobs | yes |
| `backfill_service`, `dependency_service` | ingestion | ingestion_jobs | yes |
| `batch_service` (launch + rerun) | ingestion / agentic / site_intel ... | ingestion_jobs | yes |
| `job_splitter` | any | ingestion_jobs (or NULL) | yes (when batch) |
| `agentic_research.py` | agentic | portfolio agentic job table | no (`job_id`) |
| `lp_collection.py` | lp | LP collection job table | no |

So the link is: payload `ingestion_job_id` present **and** equal to
`job_table_id` (or `job_table_id` NULL). `job_table_id` alone is ambiguous.

## Design

### `app/core/ingestion_job_sync.py` (new)

- `linked_ingestion_job_id(job_table_id, payload)` — the rule above.
- `mirror_queue_outcome(db, ing_id, succeeded, error, started_at, completed_at)`
  — raw-SQL `UPDATE ... WHERE lower(status) IN ('pending','running','blocked')`
  so it is a no-op on terminal rows and tolerant of mis-cased status values.
  On success it advances the schedule watermark (`last_run_at`), the same rule
  as `jobs._advance_schedule_watermark`. Does not commit.
- `decide(...)` — pure decision for one ingestion row given its latest linked
  queue row. Terminal queue row → mirror, unless it finished less than `grace`
  ago (lets a retry submit its new queue row) or the ingestion row started
  *after* the queue row completed (an in-process retry re-used the id). No
  queue row → fail PENDING / never-started RUNNING after `orphan_after`;
  RUNNING with `started_at` is left to `cleanup_stuck_jobs` (per-source
  timeout). Live queue row → leave.
- `reconcile_orphaned_ingestion_jobs(db, grace_minutes=10, orphan_hours=24)` —
  the sweep: one `LEFT JOIN LATERAL` for the latest linked queue row per active
  ingestion row, then `decide` + update. Commits.
- `reconcile_ingestion_job(db, ing_id)` — same, for one row, no grace (used by
  the scheduler when a blocking job is older than 2x cadence).

### Worker (`app/worker/main.py`)

`_write_back_ingestion_job(job, succeeded, error)` in its own session after each
terminal `job_queue` commit (success, failure, cancel, no executor). Its own
session so a write-back error can never flip a successful queue job to failed,
and a dirty executor session cannot poison it. Drain-timeout cleanup: fix the
upper-case `'RUNNING','CLAIMED'` literals (statuses are stored lower-case, so it
matched nothing) and write back the rows it fails.

### Sweep hook

`cleanup_stuck_jobs` (every 30 min, already registered) runs the sweep first on
its own session, isolated by try/except. No new APScheduler job, no main.py
change.

### Scheduler

- `SCHEDULER_JOB_DEFAULTS` passed to `AsyncIOScheduler`.
- `schedule_cadence_seconds(schedule)`: frequency, or the gap between two cron
  fire times for CUSTOM.
- `schedule_misfire_grace_seconds(schedule) = clamp(cadence / 8, 5 min, 1 day)`:
  hourly 7.5 min, daily 3 h, weekly 21 h, monthly/quarterly 1 day.
- `register_schedule` passes that grace; if the job being replaced had a
  `next_run_time` in the past within the grace and the same trigger, it is
  kept, so APScheduler fires it once (coalesced) instead of losing it.
- `run_scheduled_job`: skip logs at WARNING; blocking job older than
  `ORPHAN_CADENCE_MULTIPLIER (2) x cadence` → `reconcile_ingestion_job`; if that
  resolves it, the schedule proceeds.

### bulk_ingest

`execute` wraps discovery/load in `try/finally`; `_finish_ingestion_job` always
runs with the error (or "interrupted" on cancellation).

### docker-compose

- api, worker: `restart: unless-stopped`.
- api, worker: `depends_on: cloudsqlproxy: {condition: service_healthy}`; the
  `postgres` dependency is dropped — `.env` `DATABASE_URL` points at the proxy
  (`host.docker.internal:5435`), so the local postgres is not on the data path.
  The postgres service itself stays for local-only dev.
- proxy image pinned to `2.22.1` (the version the running `:latest` reports).

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_link_rule` | ingestion vs agentic/LP payloads |
| T2 | `test_decide_*` | grace, in-process retry, no-queue orphan, live queue |
| T3 | `test_cadence_and_grace` | daily → hours, monthly cron → 1 day |
| T4 | `test_scheduler_job_defaults` | coalesce / max_instances / misfire |
| T5 | `test_register_keeps_missed_run_within_grace` | catch-up on re-registration |
| T6 | `test_compose_restart_depends_pin` | compose policy |
| T7 | `test_worker_writes_back_success` (PG) | mart job → ingestion SUCCESS + watermark |
| T8 | `test_worker_writes_back_failure` (PG) | executor raises → FAILED + error |
| T9 | `test_worker_write_back_is_idempotent` (PG) | executor-written terminal row untouched |
| T10 | `test_worker_ignores_unlinked_job_table_id` (PG) | agentic `job_table_id` collision |
| T11 | `test_sweep_repairs_zombie` (PG) | row-3891 shape, mixed-case terminal statuses |
| T12 | `test_sweep_leaves_live_and_fresh` (PG) | live queue, grace, retry re-use |
| T13 | `test_sweep_fails_queueless_orphans` (PG) | 24 h rule |
| T14 | `test_cleanup_stuck_jobs_runs_sweep` (PG) | hook wired |
| T15 | `test_schedule_unblocks_after_2x_cadence` (PG) | reconcile + proceed; young → WARNING skip |
| T16 | `test_bulk_ingest_finishes_when_discover_raises` (PG) | finally |

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/ingestion_job_sync.py | Create |
| app/worker/main.py | Modify (write-back, drain fix) |
| app/worker/executors/bulk_ingest.py | Modify (finally) |
| app/core/scheduler_service.py | Modify (job_defaults, grace, catch-up, skip handling, sweep hook) |
| docker-compose.yml | Modify (restart, depends_on, pin) |
| tests/test_spec_121_schedule_job_reliability.py | Create |

## Out of Scope

- Chaining marts on loader completion (PLAN_085 §D5).
- Tri-state partial-failure status for bulk loads.
- A catch-up pass for runs missed by more than the grace (PLAN_085 D4).
- Watchdog/alerting (SPEC_128). `/health` status casing (SPEC_128).
- Pointing `DATABASE_URL` at `cloudsqlproxy:5432` directly (`.env`, live).

## Feedback History

_No corrections yet._
