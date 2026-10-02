# PLAN 095 — ATS job boards collected into NexData, gated, with a pay parser (SPEC_151)

**Status:** Approved via the workbench orchestrator (2026-10-02 brief: "Build Phase 4 of
PE-TARGETS-DEPTH-PLAN — job boards collected INTO NexData") · **Spec:**
`docs/specs/SPEC_151_ats_boards.md`

## Why

Workbench PE-TARGETS-DEPTH-PLAN Phase 4 (web footprint, gated), job-board half. Measured
2026-09-29/10-02: structured pay is almost empty in stored ATS rows (Greenhouse 0 of 10,643,
Ashby 0 of 2,318, SmartRecruiters 0 of 10,316, Workday 0 of 1,222, Lever 199 of 900); the old
lane is tied to `industrial_companies` (233 demo companies, FK), fetches with no robots or
terms gate, and its `generic.py` HTML scraper produced 5,550 rows with 0 locations and
18-19% navigation junk. `job_postings:all` runs nightly (02:00 batch) and reports `success`
while its own log says "All 5 companies failed".

## Steps

- [x] Terms + robots review of the three public board APIs; verdicts and citations in
      `app/entities/data/site_terms.json` (the open_web terms registry). Measured 10-02:
      greenhouse.io allowed (robots `Disallow: /embed/` only), lever.co allowed (robots
      `Allow: /`, Crawl-delay 1), ashbyhq.com terms allowed BUT `api.ashbyhq.com/robots.txt`
      answers 401 to every agent -> open_web treats 401 as disallow-all -> Ashby is NOT fetched
      (owner call; no override built)
- [x] Spec + failing tests (pay parser incl. traps, adapters, discovery verification, gated
      fetch, DB merge first_seen/last_seen/closed, generic retired, all-failed job fails)
- [x] `app/sources/ats_boards/pay.py` — pure pay parser (structured first, else text), snippet +
      confidence + parser version
- [x] `app/sources/ats_boards/adapters.py` — Greenhouse / Lever / Ashby JSON -> normalized
      postings (pure); emails/phones redacted from stored description text
- [x] `app/sources/ats_boards/discover.py` — board tokens only from held data (existing
      `company_ats_config` tokens, a held domain label, the core entity's legal name) or a cited
      seed; <= 2 slugs per company, <= 25 companies per run, a board is used only when verified
      (Greenhouse board name == company name; Lever: company name in >= half the postings)
- [x] `app/sources/ats_boards/collect.py` — every request through `app.core.open_web`
      (terms, robots, honest UA, Retry-After, >= max(crawl-delay, 2 s) per host, public
      addresses, no retries), bodies capped and truncation refused; CLI dry run by default
- [x] Migration 0018: `ats_board`, `ats_posting`, `ats_board_fetch` (new tables, no lock on
      existing ones); catalog dataset `ats_boards` with its clock
      (`max(fetched_at)` of successful fetches, basis `as_of`); rights entry
- [x] Dispatch key `ats_boards` (job path); the ingest function raises when every board failed
- [x] Retire `generic.py`: the old collector never fetches with it (config marked `retired`),
      deprecation note in the module; no rows deleted (counts reported). Fix the
      all-failed-reports-success bug in `ingest_job_postings_all`
- [x] ruff + unit tests in the api image; catalog suites green
- [x] DRY RUN for the pilot (one-off container, same image and mounts; shared api + 6 workers
      NOT restarted), then LIVE; per-company results, pay coverage before/after
- [x] Adversarial self-review: robots/terms bypass, PII, 30 real postings hand-checked for
      pay-parser false positives; fix what is found
- [x] Session log

## Deploy

Code is bind-mounted. The new lane needs nothing restarted to run from a one-off container.
The generic retirement and the all-failed fix reach the nightly `job_postings:all` batch only
after the shared workers restart (`docker-compose restart worker`), owner's call.
