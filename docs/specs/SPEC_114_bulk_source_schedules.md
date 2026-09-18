# SPEC 114 — Scheduled bulk SEC loads

**Status:** Draft
**Task type:** service
**Date:** 2026-09-18
**Test file:** tests/test_spec_114_bulk_source_schedules.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 1 follow-up)

## Goal

The seven bulk SEC sources were loaded once by hand. Keep them current by running them on a schedule through the existing `ingestion_schedules` table, so they show up in the schedule admin endpoints like every other source, and keep the worker queue as the only execution path.

## Acceptance Criteria

- [ ] A schedule whose `source` starts with `bulk:` (e.g. `bulk:sec_form_d`) creates an `IngestionJob` and queues a `bulk_ingest` worker job linked by `job_table_id`, instead of going through `SOURCE_DISPATCH`.
- [ ] `schedule.config` may carry `since`, `max_releases` and `release_keys`; they are passed through to the worker payload.
- [ ] The bulk executor updates the linked `IngestionJob`: RUNNING at start, then SUCCESS with `rows_inserted`, or FAILED with the error. A run that loads nothing is SUCCESS with a "0 rows" message (releases already loaded), while a run whose attempted releases all failed is FAILED.
- [ ] `DEFAULT_BULK_SCHEDULES` defines one schedule per source with sensible cadence and staggered hours:

| Source | Cadence | Why |
|---|---|---|
| `sec_iapd_feed` | daily 05:10 | SEC keeps only the latest edition; a missed day can't be recovered |
| `sec_edgar_submissions` | daily 06:20 | submissions.zip is rebuilt nightly |
| `sec_companyfacts` | weekly Sun 07:00 | 1.2 GB download; statements change slowly |
| `sec_adv_roster` | monthly day 5, 08:00 | SEC posts monthly roster files |
| `sec_form_d` | monthly day 8, 08:30 | quarterly zips appear weeks after quarter end; loaded releases are skipped |
| `sec_insider` | monthly day 8, 09:00 | same, quarterly data sets |
| `sec_13f` | monthly day 9, 09:30 | same; holdings still limited to the newest data set |

- [ ] `install_default_bulk_schedules(db)` is idempotent: it creates missing schedules, leaves existing ones (including paused ones) untouched, and reports created vs existing.
- [ ] Endpoints: `POST /api/v1/bulk/schedules/install` and `GET /api/v1/bulk/schedules`.
- [ ] Every cron/frequency in the defaults is accepted by the scheduler's trigger builder.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_bulk_schedule_queues_worker_job | `bulk:` source → submit_job("bulk_ingest") with payload + job_table_id; no ingestion dispatch |
| T2 | test_bulk_schedule_passes_config_options | since / max_releases / release_keys reach the payload |
| T3 | test_non_bulk_schedule_unchanged | a normal source still goes through the ingestion path |
| T4 | test_paused_or_duplicate_schedule_skipped | inactive schedule and one with a job already running are skipped |
| T5 | test_executor_marks_ingestion_job_success | linked IngestionJob → SUCCESS with rows_inserted |
| T6 | test_executor_marks_ingestion_job_failed | all releases failed → RuntimeError and IngestionJob FAILED |
| T7 | test_install_defaults_idempotent | second install creates nothing; paused schedules stay paused |
| T8 | test_default_schedules_build_valid_triggers | every default maps to a valid trigger |
| T9 | test_bulk_schedule_endpoints | install/list endpoints return the expected shape |

## Rubric Checklist

_No service rubric in memory/rubrics; generic:_
- [ ] Worker queue stays the only execution path
- [ ] Idempotent install; no duplicate schedules
- [ ] Failures surface as FAILED jobs, not silent success

## Design Notes

- `bulk:` prefix keeps bulk schedules out of `SOURCE_DISPATCH` without a schema change.
- The executor writes the linked `IngestionJob` through its own short session so a long load doesn't hold a transaction open.
- **Known limitation (unchanged):** APScheduler still runs inside the API process, so schedules only fire while the API container is up. Moving the scheduler to its own leader-locked container is Phase 2 (review D26).

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/scheduler_service.py | Modify — bulk branch + DEFAULT_BULK_SCHEDULES + install_default_bulk_schedules |
| app/worker/executors/bulk_ingest.py | Modify — update the linked IngestionJob |
| app/api/v1/bulk.py | Modify — install/list schedule endpoints |
| tests/test_spec_114_bulk_source_schedules.py | Create |

## Feedback History

_No corrections yet._
