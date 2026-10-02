# SPEC 151 — ATS job boards into NexData: gated fetch, pay parser, hiring clock

**Status:** Implemented (live 2026-10-02)
**Task type:** collector
**Date:** 2026-10-02
**Plan:** `docs/plans/PLAN_095_ats_boards.md`
**Test file:** tests/test_spec_151_ats_boards.py

## Goal

Workbench PE-TARGETS-DEPTH-PLAN Phase 4 (job-board half). Collect open roles for PE targets from
the official public job-board JSON endpoints (Greenhouse, Lever, Ashby) into NexData, keyed to the
core entity where resolvable, with first_seen/last_seen/closed so hiring velocity and team growth
are computable, a declared clock, and a pay parser that reads structured pay when present and
US pay-transparency text otherwise, keeping the raw snippet and a confidence. Every request goes
through `app.core.open_web` (terms review, robots.txt, honest UA, Retry-After, pacing). The old
`generic.py` HTML scraper is retired.

## Terms and robots (measured 2026-10-02)

| Host (registrable) | Endpoint used | robots.txt | Terms | Verdict |
|---|---|---|---|---|
| greenhouse.io | `boards-api.greenhouse.io/v1/boards/<token>[/jobs?content=true&pay_transparency=true]` | 200: `User-agent: * / Disallow: /embed/` | Job Board API docs: "Job Board data is publicly available, so authentication is not required for any GET endpoints." greenhouse.com/legal lists customer documents only (MSA, DPA, privacy); no clause on automated access | allowed |
| lever.co | `api.lever.co/v0/postings/<site>?mode=json` | 200: `Allow: /`, `Crawl-delay: 1` | lever/postings-api README: "This API is designed to help you create a job site." Lever Terms of Service (last updated 2023-08-25) govern customers; their "benchmarking or competitive purposes" clause is about Lever's software, not published postings | allowed |
| ashbyhq.com | `api.ashbyhq.com/posting-api/job-board/<org>?includeCompensation=true` | **401 Unauthorized to every agent** | Ashby Customer Terms of Service (2025-09-29) bind customers only; developers.ashbyhq.com documents the public posting API with no use restriction | terms allowed; **robots gate refuses** (open_web: 401/403 = disallow all). Not fetched. Owner call |

## Acceptance Criteria

- [x] Pay parser (pure, `PARSER_VERSION`): structured pay first (Greenhouse `pay_input_ranges`,
      Lever `salaryRange`, Ashby `compensation.summaryComponents`), else text. Returns min, max,
      currency, interval (`year|hour|month|week|day`), kind (`base|ote|unspecified`), source
      (`structured|text`), the raw snippet, confidence (`high|medium`); nothing (None) when unsure.
- [x] Text parser reads "$120,000 - $150,000", "$60/hr", "$120K–$150K", "between $X and Y/year",
      "$22 to $24hr", "starts at $25", "OTE $200,000", CAD/GBP/EUR markers; rejects equity %,
      revenue / budget / funding figures, "$30mm+", "$100 monthly wellbeing reimbursement",
      "401(k)", sign-on bonuses and stipends, implausible values.
- [x] Adapters map each ATS's JSON to one normalized posting shape (pure). Stored description
      text has e-mail addresses and phone numbers redacted; no applicant or recruiter field is read.
- [x] Board tokens come only from held data (non-generic `company_ats_config` tokens; a held
      domain's first label; the core entity's legal name) or a cited seed; at most 2 slugs per
      company and 25 companies per run; a board is used only when verified (Greenhouse board
      `name` equals the company name after suffix stripping; Lever: company name in at least half
      the postings) and the evidence is stored.
- [x] Every request goes through `open_web.OpenWebFetcher` with the terms registry; an unreviewed
      or refused host gets zero requests; robots refusal, Retry-After and non-JSON / truncated
      bodies are recorded outcomes, never retried.
- [x] Storage: `ats_board` (ats, token, core_entity_id, industrial_company_id, discovery basis +
      evidence, terms citation), `ats_posting` (posting fields, pay columns with provenance,
      first_seen_at / last_seen_at / closed_at / status), `ats_board_fetch` (one row per fetch:
      outcome, open count, new, closed, bytes, sha256, UA, terms citation). A re-fetch updates
      last_seen, a posting missing from a successful fetch is closed, a reappearing one reopens.
      A failed fetch never closes anything.
- [x] Dry run (default) writes nothing; `--apply` writes. Dispatch key `ats_boards` raises when
      every board failed (job is `failed`, not `success`).
- [x] Catalog dataset `ats_boards` declares its clock (`max(fetched_at)` of fetched boards,
      basis `as_of`); rights entry `ats_boards` (internal_only, scraped).
- [x] `generic.py` retired: the old collector never fetches with it (config `crawl_status =
      'retired'`), module carries a deprecation note, no rows deleted;
      `ingest_job_postings_all` raises when every company failed.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_text_ranges_real_phrasings | Bombas / Tecovas / common US phrasings parse to min/max/interval |
| T2 | test_k_suffix_and_unicode_dash | "$120K–$150K" |
| T3 | test_single_values_and_starts_at | "$28/hr", "starts at $25" |
| T4 | test_ote_kind | "OTE $200,000" -> kind ote |
| T5 | test_currency_markers | CAD / C$ / £ / € / USD suffix |
| T6 | test_traps_rejected | equity %, revenue, budgets "$30mm+", reimbursement, 401(k), sign-on bonus, funding |
| T7 | test_trap_beside_real_range | trap sentence + real range in one posting -> the range |
| T8 | test_multi_zone_ranges | several zone ranges -> overall min/max, medium confidence |
| T9 | test_structured_first | Greenhouse pay_input_ranges / Lever salaryRange / Ashby summaryComponents win over text |
| T10 | test_no_pay | nothing parsed -> None |
| T11 | test_adapters_normalize | Greenhouse / Lever / Ashby fixture JSON -> normalized postings |
| T12 | test_redaction | e-mails and phone numbers removed from stored text |
| T13 | test_slug_candidates | legal suffixes stripped, <= 2 slugs, domain label first |
| T14 | test_verify_greenhouse_name | board name must equal company name |
| T15 | test_verify_lever_mentions | company name in >= half the postings |
| T16 | test_unreviewed_host_zero_requests | no terms review -> 0 requests |
| T17 | test_robots_401_refuses_ashby | robots 401 -> robots_disallowed, API never requested |
| T18 | test_truncated_or_non_json_refused | body at the cap / not JSON -> recorded outcome, no postings |
| T19 | test_retry_after_not_retried | 429 -> recorded, host not asked again in the run |
| T20 | test_discovery_cap | > 25 companies refused |
| T21 | test_generic_never_fetched | old collector with cached generic config: no fetch, retired |
| T22 | test_all_failed_raises | ingest_job_postings_all and ingest_ats_boards raise when all failed |
| T8b | test_review_findings_2026_10_02 | hand-check findings: 'ranges from', 'Annually $', base vs variable vs OTE, SGD, CAN $, disqualifier fencing |
| T23 | test_merge_pg | PG: new / seen again / closed / reopened; failed fetch closes nothing; dry run writes nothing |

## Rubric Checklist (generic — no collector rubric file exists)

- [x] Tests written and watched fail before source code
- [x] Parameterized SQL only
- [x] No new dependency
- [x] Idempotent writes; nothing dropped silently (outcomes counted)
- [x] Public data only; robots + terms + honest UA + Retry-After for every request
- [x] No PII beyond the posting (contact details redacted)
- [x] ruff clean; catalog columns regenerated

## Design Notes

- `ats_board.status`: `active` (verified + fetched), `unverified`, `not_found`, `refused`
  (gate), `error`. Unique (ats_type, board_token).
- `ats_posting` unique (board_id, external_id). Pay columns: `pay_min, pay_max, pay_currency,
  pay_interval, pay_kind, pay_source, pay_snippet, pay_confidence, pay_parser`.
- Hiring velocity = new `first_seen_at` per period; team growth = open postings by department
  over `ats_board_fetch` history (`open_count`, `new_count`, `closed_count` per fetch).
- Body cap for board JSON: 16 MB (Tecovas Greenhouse is ~1 MB); a body at the cap is
  `truncated`, never parsed.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/sources/ats_boards/{__init__,pay,adapters,discover,collect,ingest}.py | Create | the lane |
| app/sources/ats_boards/data/board_seeds.json | Create | cited board seeds |
| app/entities/data/site_terms.json | Modify | greenhouse.io, lever.co, ashbyhq.com reviews |
| alembic/versions/0018_ats_boards.py | Create | three new tables |
| app/api/v1/jobs.py | Modify | dispatch key `ats_boards` |
| app/catalog/{datasets,rights}.py (+ generated columns) | Modify | dataset, clock, rights |
| app/sources/job_postings/{collector,ingest}.py, ats/generic.py | Modify | retire generic, all-failed raises |
| tests/test_spec_151_ats_boards.py | Create | T1-T23 |

## Results (2026-10-02, live)

- Migration 0018 applied (run_migrations, one-off container). Pilot dry run, then LIVE (`--preset pilot --apply`,
  one-off container; shared api + 6 workers not restarted): 8 companies, 33 attempts, 27 HTTP requests.
- Boards: Bombas `greenhouse:bombas` (core 136663) 11 postings, pay 11 (all text); Tecovas `greenhouse:tecovas`
  (core 133959) 113 postings, 60 locations, 53 departments, pay 108 (all text; the 5 without: 4 template
  placeholders `$[X] - $[Y]` / no `$`, 1 typo `$[18} - $[20]` left unparsed). `pay_input_ranges` empty on every
  posting even with `pay_transparency=true`. GlossGenius seed `ashby:geniusai` recorded `refused`
  (robots 401). Not found on Greenhouse / Lever under held names: Whatnot, Carbon Arc, Rockfish Data, Talent
  Source, Kimball Midwest, GlossGenius; every Ashby slug refused at robots (1 robots request, 0 API requests).
- Idempotent re-run (Bombas + Tecovas): new 0, seen 124, closed 0; 124 postings, 5 fetch rows.
- Pay coverage before -> after: pilot companies 0 postings with pay (NexData held no postings for 7 of 8;
  Kimball's 36 generic rows are nav junk) -> 119 of 124 (96%). On the 25,099 stored ATS rows (read-only
  measurement, not written): Greenhouse structured 0 -> text 4,961 of 10,643; Ashby 0 -> 208 of 2,318; companies
  with any pay 2 -> 28.
- Precision: 30 postings hand-checked (22 stored rows from 22 companies + 3 Bombas + 5 Tecovas): first pass 28/30
  (one dropped max on "ranges from", one Amazon miss); a second random 30 + a risky-snippet scan found base and
  variable ranges mixed, SGD read as USD, "CAN ... $" read as USD, and the disqualifier regex inert (a control
  byte where `\b` belonged). All fixed test-first (T8b); after the fixes the first 30 are 30/30 and the second
  30 are 30/30 on re-read.

## Owner calls left open

- Ashby: `api.ashbyhq.com/robots.txt` answers 401 to everyone; open_web treats that as disallow-all (RFC 9309
  would allow it). Genius (14 of 18 with structured pay), and likely Whatnot / Carbon Arc / Rockfish, sit behind it.
  Loosening the 401 rule for this host is the owner's call; nothing was built for it.
- The old lane (`job_postings:all`, nightly 02:00 batch) still fetches Ashby (5 configured companies) despite that
  robots answer, and its ATS detector requests company websites with no robots or terms gate. Retire or port.

## Feedback History

_No corrections yet._
