# PLAN 097 — Schedule `ats_boards` weekly over every active board (SPEC_153)

**Status:** Approved via the workbench orchestrator (owner approved a weekly schedule 2026-10-02)
· **Spec:** `docs/specs/SPEC_153_ats_boards_weekly.md`

## Why

SPEC_152 retired `job_postings:all` and left its successor unscheduled ("owner call"). Hiring
velocity needs repeated fetches: `first_seen_at` / `closed_at` only move when a board is re-read.

## Choice of mechanism

Batch tiers launch every night (TIER_2 "Weekly" is a name, not a cadence; weekly-ness there lives in
per-source skip windows). `ingestion_schedules` has a true WEEKLY frequency and the `job:<type>`
prefix already queues worker jobs (`job:entity_resolve`, `job:pe_mart_build`). So: one row,
source `job:ingestion`, config `{source: ats_boards, config: {preset: active, apply: true}}`,
Tuesday 03:00 UTC. The run happens on a worker, off the API process.

## Steps

- [x] Spec + failing tests T1-T10 (watched fail: 10/10)
- [x] `ingest.py`: `ACTIVE_BOARDS_SQL`, `active_companies`, `run_chunked` (<= 25 per `collect.run`),
      preset `active`, `collect.run` via `asyncio.to_thread`
- [x] `collect.py`: CLI `--preset active`
- [x] `scheduler_service.py`: `ATS_BOARDS_WEEKLY` template (in `DEFAULT_SCHEDULES`),
      `install_ats_boards_weekly`
- [x] Catalog: `SCHEDULE_DISPATCH = {ats_boards}` feeds `_dormant()`; cadence weekly; SPEC_123's
      dormant test reads the same set
- [x] ruff; SPEC_151/152/153 + scheduler/batch/catalog suites
- [x] Dry run (one-off container) then the live schedule row via `install_ats_boards_weekly`
- [x] Session log
- [x] Review fixes (same day): T11 one Company per active board; T12 `job:ingestion` schedules
      record the dispatched source + inner config on the ingestion_jobs row (retry path). Tests
      first (2/2 fail), then pass; suites 306 passed / 37 skipped with the repo mounted; ruff clean.

## Deploy

`docker-compose restart api worker`. The API's APScheduler registers schedule rows at startup
(`load_all_schedules`), and the workers must load the new `active` preset. Restart before the first
fire (Tue 2026-10-06 03:00 UTC); a fire on the old code fails the job (`unknown preset 'active'`)
rather than fetching anything.
