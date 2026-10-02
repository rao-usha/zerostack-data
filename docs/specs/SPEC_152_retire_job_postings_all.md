# SPEC 152 — Retire the old `job_postings:all` run; move its companies onto `ats_boards`

**Status:** Implemented (live 2026-10-02; code deploy awaits `docker-compose restart api worker`)
**Task type:** bug_fix
**Date:** 2026-10-02
**Plan:** `docs/plans/PLAN_096_retire_job_postings_all.md`
**Test file:** tests/test_spec_152_retire_job_postings_all.py
**Builds on:** SPEC_151 (`app/sources/ats_boards`, commit 3629704, PLAN_095)

## Goal

Owner decisions 2026-10-02: (1) Ashby stays BLOCKED (`api.ashbyhq.com/robots.txt` answers 401;
NexData treats that as disallow-all; no override). (2) Retire the old run. The old
`job_postings:all` run (nightly 02:00 batch, tier 3) still fetches 5 Ashby boards despite that
robots answer, and its ATS detector requests company websites with no robots or terms check --
it breaks the owner's robots rule today. Retire it, keep its stored rows readable, and move its
Greenhouse / Lever companies onto the gated `ats_boards` lane with tokens verified by the lane's
own discovery.

## How the old run is triggered (measured 2026-10-02, live Cloud SQL)

| Path | State before | After |
|---|---|---|
| Nightly batch (`main.py` CronTrigger 02:00 -> `batch_service.launch_batch_collection` -> `TIERS`) | `TIER_3` carries `SourceDef("job_postings:all", {"skip_recent_hours": 600})`; jobs 4093-4299 ran nightly, "All 5 companies failed" reported success | removed from `TIER_3`; DB override `batch_source_tier_overrides(job_postings:all, enabled=false)` with the reason, effective at tonight's launch without a restart |
| `DEFAULT_COLLECTION_GROUPS["government"]` | lists `job_postings:all` | removed |
| `ingestion_schedules` | live DB: no row. `DEFAULT_SCHEDULES` template "Job Postings All Sources - Monthly" installs one via `POST /schedules/defaults` | template removed |
| Dispatch keys `job_postings:all / :company / :discover` (`POST /jobs`, worker) | run the old collector | entry points raise `OldLaneRetired` (job `failed`) with a pointer to `ats_boards` |
| `POST /job-postings/collect/{id}`, `/collect-all`, `/discover-ats/{id}` | start the old collector in-process | 410 Gone, pointer to `ats_boards`; no job row, no fetch |
| Catalog `BATCH_SCHEDULED_DISPATCH` | contains `job_postings:all` | removed -> dataset `job_postings` is archival (no scheduled producer) |

## Acceptance Criteria

- [x] `job_postings:all` is in no batch tier, no default collection group, no default schedule
      template; the catalog set no longer lists it.
- [x] Live DB: `batch_source_tier_overrides` row `job_postings:all` enabled=false, reason + date in
      `default_config`; `resolve_effective_tiers` drops it even with the old code.
- [x] `AshbyClient.fetch_jobs` refuses before any request (raises `OldLaneRetired`, names Ashby
      robots 401 and `ats_boards`).
- [x] `ATSDetector.detect` refuses before any request (website detection has no robots/terms gate).
- [x] The collector does not swallow the refusal (re-raises); `ingest_job_postings_all/_company/
      _discover` raise `OldLaneRetired`; the three POST trigger routes answer 410.
- [x] Stored rows stay: no `job_postings` / `job_posting_snapshots` / `company_ats_config` row is
      deleted; GET routes unchanged.
- [x] `board_seeds.json` carries the old run's Greenhouse / Lever companies whose board the lane
      verified (dry run 2026-10-02, gated), keyed by `industrial_company_id`, each with its
      verification evidence as the citation; Ashby companies carry `blocked` (robots 401) and are
      never offered to the fetcher; seeds load and validate.
- [x] `ats_boards` preset `migrated`: the seeded, non-blocked companies, seeded token only (no slug
      discovery), within the 25-company cap; `record_blocked` writes the blocked boards as
      `ats_board.status='refused'` (no request).
- [x] Dry run then one LIVE run over the migrated companies (one-off container); per-company board,
      postings, pay, refused/blocked reported.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_not_in_batch_tiers_groups_or_defaults | TIERS, DEFAULT_COLLECTION_GROUPS, DEFAULT_SCHEDULES, BATCH_SCHEDULED_DISPATCH have no job_postings:all |
| T2 | test_db_override_drops_it | resolve_effective_tiers with an enabled=false override drops the key |
| T3 | test_ashby_client_refuses_without_request | AshbyClient.fetch_jobs raises OldLaneRetired, no httpx client made |
| T4 | test_detector_refuses_without_request | ATSDetector.detect raises OldLaneRetired, no httpx client made |
| T5 | test_collector_reraises | collect_company on a cached ashby config / no config raises (not swallowed) |
| T6 | test_ingest_entry_points_raise | ingest_job_postings_all / _company / _discover raise OldLaneRetired |
| T7 | test_trigger_routes_gone | POST collect / collect-all / discover-ats -> 410, no job created |
| T8 | test_migrated_seeds_load | seeds validate; 25 migrated GH/Lever with evidence; no ashby among migrated |
| T9 | test_ashby_recorded_blocked | 11 Ashby seeds carry blocked robots_401; preset migrated never offers them; record_blocked upserts status refused |
| T10 | test_migrated_preset_no_slugs | ingest preset migrated runs collect.run with slugs=False over the seeded companies |

## Rubric Checklist

- [x] Tests written and watched fail before source code
- [x] Parameterized SQL only
- [x] No row deleted; stored rows readable
- [x] No robots override; no company website requested outside open_web
- [x] ruff clean on touched files

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/sources/job_postings/retired.py | Create | `OldLaneRetired` + message |
| app/sources/job_postings/ats/{ashby,detector}.py | Modify | refuse before any request |
| app/sources/job_postings/{collector,ingest}.py | Modify | re-raise; entry points raise |
| app/api/v1/job_postings.py | Modify | trigger routes 410 |
| app/core/batch_service.py, app/core/scheduler_service.py, app/catalog/datasets.py | Modify | unschedule |
| app/sources/ats_boards/{ingest,collect}.py, data/board_seeds.json | Modify | migrated preset, blocked seeds |
| tests/test_spec_152_retire_job_postings_all.py | Create | T1-T10 |
| tests/test_spec_151_ats_boards.py | Modify | T22 old-lane half now expects the retirement |

## Results

- Live DB: `batch_source_tier_overrides` before: no row; after: id 1 `job_postings:all` enabled=false,
  default_config `{retired: 2026-10-02, spec: SPEC_152, reason}`. Checked: the running (old) TIERS + this
  override -> not launched, so tonight's 02:00 batch skips it without a restart. `ingestion_schedules`
  (live): no job_postings row before or after. No queued/pending job_postings jobs.
- Verification dry runs (gated, 2026-10-02) over the old run's 28 Greenhouse/Lever configs: 22 GH + 3 Lever
  verified (GH board name == company name; Lever name in >= half the postings). Not migrated: Plaid (GH 404,
  Lever 404, Ashby slug refused at robots), Netflix (Lever 404, GH 404, Ashby refused), Ro (Lever board
  fetched but unverified: a 2-letter name can never pass the mention rule; not loosened).
- Seeds: 25 verified (with evidence) + 11 Ashby `blocked: robots_401` (Anthropic, Cursor, Linear, Mistral AI,
  Notion, OpenAI, Ramp, Rippling, Supabase, Vanta, Weights & Biases).
- `--preset migrated` DRY RUN: 25 companies, 25 boards fetched, 49 HTTP requests, 0 Ashby requests. LIVE
  (`--apply`, one-off `docker-compose run --rm --no-deps worker`): 24 fetched + Elastic `error`
  (ReadTimeout on the jobs JSON, recorded, nothing closed); one targeted follow-up (`--industrial-ids 193
  --apply`, 3 requests) fetched it. 11 blocked boards recorded `refused` with no request.
- Stored: ats_board 3 -> 39 (27 active, 12 refused), ats_posting 124 -> 5,934 (+5,810; pay 119 -> 3,238,
  +3,119; structured 1,877), ats_board_fetch 5 -> 31. Old rows unchanged: job_postings 30,949,
  company_ats_config 233, job_posting_snapshots 1,246.
- Not on the lane's ATSes (not migrated, listed): Workday 15 (ABC Supply, Airgas, ESAB, Graco, Graybar,
  Hypertherm, Pool Corp, Prudential BSN Takaful, Rockwell Automation, Sandvik, SRS Distribution, Stanley
  Black & Decker, Sullair, Terex, Visa), SmartRecruiters 3 (Alto-Shaam, Bosch, McDonald's), generic careers
  page 124, no board detected 52. No company website was requested.
- Tests: SPEC_152 12 + SPEC_151 52 pass; batch/scheduler/collection-group/catalog 123-145/148 suites pass
  except test_rights_batch_1 x2, pre-existing (SPEC_151's `ats_boards` rights entry not in that test's
  allowlist). ruff: no new findings.

## Feedback History

_No corrections yet._
