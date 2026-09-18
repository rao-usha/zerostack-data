# SPEC 106 — Job lifecycle (D25) and API keys stored in job config (D34)

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-16
**Test file:** tests/test_spec_106_job_lifecycle_and_api_key_storage.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 0)

## Goal

1. Stop creating zombie jobs. When the stale-job cleanup fails a queue job, its `ingestion_jobs` row stays pending/blocked and the batch never advances. A batch can also launch with no live worker; this is how 1,806 blocked + 131 pending ingestion jobs accumulated.
2. Stop writing API keys into `ingestion_jobs.config`. The Yelp, EIA and US Trade routers put the key into the persisted job config.

## Acceptance Criteria

- [ ] `cancel_stale_pending_jobs`:
  - marks the linked `IngestionJob` (via `job_table_id`) FAILED with the same reason and a `completed_at`
  - calls `promote_blocked_jobs` once per affected `batch_id`
- [ ] New `worker_heartbeats(worker_id PK, hostname, last_seen_at, started_at)` table. The worker poll loop upserts its row at most every 30s, including while idle. The row is deleted on graceful shutdown. It is created by Alembic migration `0002_worker_heartbeats` and by the ORM model.
- [ ] `launch_batch_collection` raises `RuntimeError` when no worker heartbeat is newer than 5 minutes, unless `force=True`. `POST /jobs/batch/launch` exposes `force`.
- [ ] The Yelp, EIA and US Trade routers no longer put `api_key` into job config.
- [ ] `_run_dispatched_job` fills `api_key` at execution time from settings when the dispatch signature expects it and config lacks it.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_stale_cleanup_fails_linked_ingestion_job | IngestionJob status FAILED with no_worker_available message |
| T2 | test_stale_cleanup_promotes_batches | `promote_blocked_jobs` is called for each distinct batch_id |
| T3 | test_worker_heartbeat_model_and_migration | Model columns exist; migration chains from 0001 and creates the table |
| T4 | test_record_worker_heartbeat_upserts | `record_heartbeat` executes an upsert with worker_id |
| T5 | test_launch_batch_refuses_without_live_worker | RuntimeError when no recent heartbeat |
| T6 | test_launch_batch_force_bypasses_worker_check | force=True proceeds |
| T7 | test_routers_do_not_store_api_key | No `"api_key": api_key` in yelp.py, eia.py, us_trade.py |
| T8 | test_dispatch_injects_api_key_from_settings | Dispatched ingest function receives api_key from settings |

## Rubric Checklist

_No bug_fix rubric in memory/rubrics; generic:_
- [ ] Root cause fixed
- [ ] Regression tests
- [ ] Existing batch/stale tests still pass
- [ ] Parameterized SQL only

## Design Notes

- `app/core/job_queue_service.py`: `record_worker_heartbeat(db, worker_id, hostname)` runs `INSERT … ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = now()`. `has_live_worker(db, within_minutes=5) -> bool`. `remove_worker_heartbeat(db, worker_id)`.
- The worker poll loop tracks `_last_hb` and calls `record_worker_heartbeat` via a fresh session when 30s have elapsed.
- API key resolution: `_resolve_runtime_api_key(base_source)` checks the explicit getters for yelp/eia/us_trade (the census survey key), then falls back to `settings.get_api_key()`, catching `KeyError`.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/models_queue.py | Add WorkerHeartbeat model |
| alembic/versions/0002_worker_heartbeats.py | Create |
| app/core/job_queue_service.py | Stale cleanup fix + heartbeat helpers |
| app/worker/main.py | Heartbeat in poll loop; remove on shutdown |
| app/core/batch_service.py | Live-worker check + force |
| app/api/v1/jobs.py | force param; runtime api_key injection |
| app/api/v1/yelp.py, eia.py, us_trade.py | Drop api_key from config |

## Feedback History

_No corrections yet._
