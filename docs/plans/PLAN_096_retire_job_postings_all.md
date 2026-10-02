# PLAN 096 — Retire `job_postings:all`; migrate its companies onto `ats_boards` (SPEC_152)

**Status:** Approved via the workbench orchestrator (owner decisions 2026-10-02: Ashby stays
blocked, no override; retire the old run) · **Spec:** `docs/specs/SPEC_152_retire_job_postings_all.md`

## Why

The old run still fetches Ashby boards whose robots.txt answers 401 (= disallow-all in NexData)
and its ATS detector requests company websites with no robots or terms gate. It ran nightly in
batch tier 3 and reported "success" over "All 5 companies failed".

## Steps

- [x] Map every trigger (batch tier, collection groups, schedule templates, dispatch keys, API
      routes, catalog schedule set) -- done in the spec table
- [x] Verify Greenhouse / Lever boards through the lane (dry run, gated): 23 GH + 5 Lever configs
- [x] Spec + failing tests (T1-T10), watched fail
- [x] Unschedule: TIER_3, DEFAULT_COLLECTION_GROUPS, DEFAULT_SCHEDULES, BATCH_SCHEDULED_DISPATCH;
      live DB override row (enabled=false, reason, date)
- [x] Refusals: AshbyClient + ATSDetector raise before any request; collector re-raises; ingest
      entry points raise; trigger routes 410
- [x] Seeds: verified GH/Lever companies + Ashby blocked entries; preset `migrated` (no slugs);
      `record_blocked`
- [x] ruff; SPEC_152 + SPEC_151 + catalog + batch/scheduler suites
- [x] Dry run then LIVE once (one-off `docker-compose run --rm --no-deps worker`); per-company table
- [x] Session log

## Not done here (owner calls)

- Scheduling `ats_boards` weekly -- not scheduled.
- Workday / SmartRecruiters companies (18) lose their old (ungated) collection; the lane does not
  cover those ATSes. Porting them needs a terms + robots review first.

## Deploy

DB override takes effect at tonight's 02:00 launch (the api reads overrides at launch). Code is
bind-mounted: `docker-compose restart api worker` deploys the code refusals, the 410 routes and
the tier removal (owner's call; not restarted here).
