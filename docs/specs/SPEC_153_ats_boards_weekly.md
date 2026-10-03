# SPEC 153 — Schedule the `ats_boards` lane weekly over every active board

**Status:** Implemented (schedule row live 2026-10-02, id 90; code deploy awaits `docker-compose restart api worker`)
**Task type:** service
**Date:** 2026-10-02
**Plan:** `docs/plans/PLAN_097_ats_boards_weekly.md`
**Test file:** tests/test_spec_153_ats_boards_weekly.py
**Builds on:** SPEC_151 (`app/sources/ats_boards`), SPEC_152 (old run retired; lane "not scheduled; owner call")
**Owner call:** 2026-10-02, the owner approved a WEEKLY schedule for the `ats_boards` lane.

## Goal

SPEC_152 moved the retired `job_postings:all` companies onto the gated `ats_boards` lane but left
the lane unscheduled, so hiring velocity (first_seen / closed) only moves when someone runs it by
hand. Schedule it weekly, refreshing every board the lane has already verified (`ats_board.status
= 'active'`), through the same `open_web` gate; Ashby rows (robots.txt 401) stay `refused` and are
never fetched.

## How it is triggered (measured 2026-10-02, live DB)

| Path | Before | After |
|---|---|---|
| `ingestion_schedules` | no `ats_boards` row (15 rows; `job:entity_resolve`, `job:pe_mart_build` precedents) | row "ATS boards refresh (weekly)", source `job:ingestion`, WEEKLY Tue 03:00 UTC, config `{source: ats_boards, config: {preset: active, apply: true}}`, active |
| Batch tiers | not present (SPEC_152) | unchanged: batch tiers run nightly; a weekly cadence there would need a skip window inside the source |
| Catalog `ats_boards` | cadence `ad_hoc`, status archival (no scheduled producer) | cadence `weekly`, status internal |

`job:ingestion` queues a worker `ingestion` job (`_run_job_schedule` -> `submit_job`), so the run
happens on a worker, not in the API process. The worker's `ingestion` executor reads `source` and
`config` from the payload and calls `run_ingestion_job` -> dispatch key `ats_boards`.

## Acceptance Criteria

- [x] Preset `active`: one `Company` per `ats_board` row (amended by review: per BOARD) with `status='active'`
      and `ats_type` in (greenhouse, lever); the stored token only (basis `refresh`, evidence
      names the board id), no slug discovery; aliases from `core.alias` and matching seeds.
      Ashby rows, `refused` / `not_found` / `unverified` rows and blocked seeds are never offered.
- [x] `ingest_ats_boards(preset="active")` runs `collect.run` in chunks of at most
      `discover.MAX_COMPANIES` (the per-run cap is unchanged), aggregates the reports, and raises
      when no board was fetched across all chunks.
- [x] `collect.run` runs off the event loop (`asyncio.to_thread`) so the worker heartbeat keeps
      ticking during a multi-minute refresh.
- [x] CLI `python -m app.sources.ats_boards.collect --preset active [--apply]` works (chunked).
- [x] `DEFAULT_SCHEDULES` carries the weekly template; `install_ats_boards_weekly(db)` creates the
      live row (active, owner-approved) only when missing (idempotent).
- [x] A due schedule queues a worker `ingestion` job whose payload carries `source=ats_boards` and
      the preset/apply config.
- [x] Catalog dataset `ats_boards`: cadence `weekly`, status `internal`, limitation updated.
- [x] Dry run (preset active) before the schedule goes live; the live row is inserted through the
      job path's own service function in a one-off container; before/after counts recorded.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_active_companies_from_active_boards | only active greenhouse/lever rows become companies; token basis refresh; seed aliases |
| T2 | test_active_never_offers_ashby_or_refused | ashby / refused / not_found rows and blocked seeds never reach the fetcher |
| T3 | test_active_chunks_within_cap | 27 companies -> runs of <= 25, slugs False, reports aggregated |
| T4 | test_active_raises_when_nothing_fetched | all chunks fetch 0 boards -> RuntimeError |
| T5 | test_collect_runs_off_event_loop | collect.run executes on a non-event-loop thread |
| T6 | test_weekly_template | DEFAULT_SCHEDULES template: job:ingestion, WEEKLY, config, validate_schedule_source ok |
| T7 | test_install_idempotent | install creates once (active), second call creates nothing |
| T8 | test_schedule_queues_worker_ingestion | _run_job_schedule submits job_type ingestion with source ats_boards + config |
| T9 | test_catalog_weekly_internal | catalog ats_boards cadence weekly, status internal |
| T10 | test_cli_accepts_active | CLI parser accepts --preset active |
| T11 | test_two_active_boards_one_company_both_refreshed | review: two active boards of one company are both offered and both fetched |
| T12 | test_scheduled_ingestion_job_records_real_source | review: a `job:ingestion` schedule's ingestion_jobs row carries source `ats_boards` + inner config; other `job:` types unchanged |

## Rubric Checklist

(No `service` rubric file exists in memory/rubrics; generic checklist.)
- [x] Tests written and watched failing before the code
- [x] Parameterized SQL only
- [x] No robots override; Ashby never requested; open_web gate on every request
- [x] Dry run first, before/after counts recorded
- [x] ruff clean on touched files
- [x] Session log entry

## Design Notes

- `ingest.active_companies(db)` reads `ats_board` (`status='active' AND ats_type IN
  ('greenhouse','lever')`), one `Company` per board row (review fix: it grouped by company, and
  `collect.run` stops at a company's first fetched board).
- `ingest.run_chunked(db, companies, apply, slugs)` -> one merged report; used by the job and CLI.
- `scheduler_service.ATS_BOARDS_WEEKLY` (template dict) is also in `DEFAULT_SCHEDULES`;
  `install_ats_boards_weekly(db)` = create_schedule(is_active=True) when the name is absent.
  The running API registers new rows at startup (`load_all_schedules`), so the restart that
  deploys this code also arms the schedule.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/sources/ats_boards/ingest.py | Modify | preset active, chunked runner, to_thread |
| app/sources/ats_boards/collect.py | Modify | CLI preset active |
| app/core/scheduler_service.py | Modify | weekly template + install function |
| app/core/batch_service.py | Modify | comment only (successor now scheduled) |
| app/catalog/datasets.py | Modify | `SCHEDULE_DISPATCH`, `_dormant()` reads it; cadence weekly |
| tests/test_spec_123_dataset_catalog.py | Modify | dormant test reads `SCHEDULE_DISPATCH` too |
| tests/test_spec_153_ats_boards_weekly.py | Create | T1-T10 |

## Results (2026-10-02)

- Tests: SPEC_153 10/10 watched fail, then pass; SPEC_151 + SPEC_152 + SPEC_153 82 passed / 1 skipped;
  scheduler / batch / collection-group / catalog suites: the only failure this change caused
  (`test_spec_123::test_dormant_api_sources_are_archival`, ats_boards now `internal`) fixed by reading
  `SCHEDULE_DISPATCH`; the remaining 24 failures are environmental in the one-off container (no
  docker-compose.yml / PLAN_088 evidence / read-only data/reports / unregistered routes) or the
  pre-existing `test_rights_batch_1` x2 noted in SPEC_152. ruff clean on touched files.
- DRY RUN `--preset active` (one-off `docker-compose run --rm --no-deps worker`): 27 companies,
  27 boards fetched (24 Greenhouse + 3 Lever), 54 HTTP requests, 0 Ashby requests, 2 chunks.
- Live schedule: `ingestion_schedules` 15 -> 16 rows; id 90 "ATS boards refresh (weekly)",
  `job:ingestion`, WEEKLY day 1 hour 3, active, next_run_at 2026-10-06 03:00 UTC; second install
  call created nothing. Stored lane rows unchanged by this spec (ats_board 39: 27 active / 12
  refused; ats_posting 5,934; ats_board_fetch 31) -- the first scheduled run writes.

## Review fixes (2026-10-02, same day; adversarial review of the uncommitted work)

Two defects, tests first (T11, T12 watched fail 2/2 for the intended reasons, then pass):

1. **Second board never refreshed.** `active_companies` grouped boards by company, and
   `collect.run` breaks after a company's first fetched board ("one verified board per company").
   A company with two active boards (e.g. mid-move Lever -> Greenhouse) would have had the second
   board never re-read, its postings never closed. None live today (27 active boards, 27
   companies); fixed before it can bite: one `Company` per board row.
2. **Scheduled run recorded as source `job:ingestion`.** `_run_job_schedule` wrote the
   `ingestion_jobs` row with the schedule's source (`job:ingestion`) and the wrapper config. A
   failed run's auto-retry calls `run_ingestion_job(job.source, job.config)` ->
   "Unknown source: job:ingestion" (the lane never actually retried), and the job ledger never
   showed an `ats_boards` run. Now a `job:ingestion` schedule writes the dispatched source and
   its inner config (`ats_boards`, `{preset: active, apply: true}`), exactly like batch-queued
   ingestion jobs; other `job:<type>` schedules are unchanged. The live row (id 90) needs no
   change. Note: retries of ANY ingestion job run in the API process (`process_scheduled_retries`),
   as for every batch source; `collect.run` is off the event loop there too (`asyncio.to_thread`).

Checked and fine: APScheduler `day_of_week=1` and `_calculate_next_run` (`weekday()`) both mean
Tuesday; the next fire 2026-10-06 03:00 UTC is a Tuesday; the live row is not in
`apscheduler_jobs` until the API restarts (`load_all_schedules`); `slugs=False` means only stored
tokens are tried, and `ACTIVE_BOARDS_SQL` never selects Ashby (the open_web gate would refuse it
anyway: robots.txt 401). Suites with the whole repo mounted in a one-off worker container:
scheduler / batch-schedule / catalog / SPEC_147 / 150 / 151 / 152 / 153 -> 306 passed, 37
skipped, 0 failed (the 24 failures previously reported were the one-off container lacking
non-`app/` files). ruff clean.

## Feedback History

_No corrections yet._
