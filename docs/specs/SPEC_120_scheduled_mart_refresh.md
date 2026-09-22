# SPEC 120 — Scheduled mart refresh

**Status:** Draft
**Task type:** service
**Date:** 2026-09-22
**Test file:** tests/test_spec_120_scheduled_mart_refresh.py

## Goal

Make the PE tables refresh themselves. Today the seven SEC bulk loaders have
schedules, but **nothing rebuilds the marts on top of them** — so a month from
now `sec_adv_private_fund_filings` would hold September's filings while
`pe_funds`, `pe_firms` and `pe_people` still held August's answers, with no
signal that they had diverged.

Three gaps, all measured against the live database:

1. `ingestion_schedules` has **no row for `bulk:sec_adv_schedule_d`**. SPEC_118
   added the definition to `DEFAULT_BULK_SCHEDULES`, but rows are seeded at API
   startup and the API has been down since. The Schedule D loader — the one the
   whole fund attribution rests on — is the only loader not scheduled.
2. There is **no way to schedule a worker job that is not a bulk load**.
   `_run_bulk_schedule` only understands the `bulk:` prefix, so
   `entity_resolve` and `pe_mart_build` cannot be scheduled at all.
3. **42 of 49 schedules are dead** — inactive, and 36 of them have never run
   once. They make the real schedule list unreadable.

## Acceptance Criteria

- [ ] A schedule whose source is `job:<type>` queues that worker job type.
- [ ] An unknown job type is refused at creation, not at fire time.
- [ ] `entity_resolve` and `pe_mart_build` are scheduled, in that order, after
      the last monthly loader has landed.
- [ ] `bulk:sec_adv_schedule_d` exists and is active.
- [ ] Dead schedules are removed reversibly, and anything that ever ran is kept.
- [ ] `next_run_at` is populated for every active schedule.

## Ordering — why the marts run on the 10th

The monthly loaders finish on the 9th (`bulk:sec_13f` at 09:30). The marts read
what those loaders wrote, and each mart reads the one before it:

| time (UTC) | job | reads |
|---|---|---|
| 4th 08:00 | `bulk:sec_adv_schedule_d` | SEC |
| 5th 08:00 | `bulk:sec_adv_roster` | SEC |
| 8th 08:30 / 09:00 | `bulk:sec_form_d`, `bulk:sec_insider` | SEC |
| 9th 09:30 | `bulk:sec_13f` | SEC |
| **10th 04:00** | **`job:entity_resolve`** | 13F + ADV + Form D + EDGAR |
| **10th 06:00** | **`job:pe_mart_build`** | the entity master, then its own marts |

Two hours between them is not a dependency mechanism, it is slack: the marts
are idempotent, so a late loader costs one stale month, not a corrupt table.
Real chaining belongs to the job queue's parent/child support and is out of
scope here.

`pe_mart_build` already runs firms → ADV current state → funds → people in one
executor, so the internal ordering needs no scheduling.

## Design

`SCHEDULE_JOB_PREFIX = "job:"` alongside the existing `bulk:`.
`_run_job_schedule` submits `schedule.source[4:]` as the job type with the
schedule's `config` as payload, mirroring `_run_bulk_schedule` including the
`IngestionJob` row and `next_run_at` update.

The job type is validated against `QueueJobType` **when the schedule is
created**. A typo that only surfaces on the 10th of next month, in a job
nobody is watching, is the failure this spec exists to prevent.

## Pruning

Delete only schedules that are inactive **and** have `last_run_at IS NULL` —
never ran, so nothing depends on them. Anything that ever ran is kept even if
paused, because its `last_run_at` is the only record of when that source was
last collected. Rows are copied to `quarantine.ingestion_schedules_pruned`
first, so the list is recoverable.

Measured: 42 inactive, 36 of them never ran.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_job_prefix_queues_the_named_job_type` | `job:pe_mart_build` → a `pe_mart_build` queue row |
| T2 | `test_unknown_job_type_is_refused_at_creation` | typo caught on create, not at fire time |
| T3 | `test_bulk_prefix_is_unchanged` | the seven loaders still queue `bulk_ingest` |
| T4 | `test_mart_schedules_are_installed_and_ordered` | entity_resolve before pe_mart_build |
| T5 | `test_install_is_idempotent` | second call creates nothing |
| T6 | `test_prune_keeps_anything_that_ever_ran` | `last_run_at` set → kept, even if inactive |
| T7 | `test_prune_is_reversible` | pruned rows recoverable from quarantine |

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/scheduler_service.py | Modify (`job:` prefix, mart schedules, prune) |
| tests/test_spec_120_scheduled_mart_refresh.py | Create |

## Feedback History

_No corrections yet._
