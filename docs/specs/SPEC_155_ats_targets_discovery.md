# SPEC 155 — ATS board discovery over the PE targets universe: verified links, review queue, resumable run

**Status:** Implemented (live run 1 done 2026-10-06 02:21 UTC)
**Task type:** service
**Date:** 2026-10-05
**Plan:** `docs/plans/PLAN_099_ats_targets_discovery.md`
**Test file:** tests/test_spec_155_ats_targets_discovery.py
**Builds on:** SPEC_151 (`app/sources/ats_boards`), SPEC_153 (weekly refresh of `status='active'` boards), SPEC_148 (`core.domain_link`, two-family rule)
**Evidence:** 2026-10-05 discovery pilot (300 kept targets, 48 hits hand-checked: 20 true / 21 false / 7 empty boards; session log 2026-10-05 11:16)
**Numbering:** SPEC_155 / PLAN_099 per the task ("next after SPEC_154 / PLAN_098"); 155-159 and PLAN_099 are unused (PLAN_100 claims SPEC_162+). Alembic 0020 (head is 0019_gleif).

## Goal

Find the public Greenhouse / Lever board of each kept PE target (`workbench.targets_universe`, `thesis_excluded = false`, 5,786 firms) with the pilot's verification rule R7 (precision 17/17 in-sample), link only VERIFIED boards to the firm (core entity id, CIK, workbench target id), keep everything else out of the hiring data (a `candidate` review queue with no firm link; rejected pairs recorded with their reason in a ledger), and run it as a resumable, budgeted, per-host-paced job. Verified boards become `active`, so the SPEC_153 weekly refresh picks them up. A careers domain seen on a verified board is recorded as a single-family (weak) SPEC_148 domain claim.

## Owner context

`collect.run` refuses > 25 companies ("board discovery is never run at scale"). That cap stays for the slug path of `collect.run`; this spec adds a SEPARATE preset that may run at scale because it carries the pilot's safeguards: a verification rule measured by hand, a ledger that never re-asks a finished (firm, board) pair, a request budget, the open_web gate on every request, stop-on-429 / 3x5xx / any refusal. Running it over all kept targets is the owner's request of 2026-10-05 (the pilot's "decision for you").

## Acceptance Criteria

### Name variants (pure)
- [x] Greenhouse variants, in order: `joined` (normalized full name, no spaces), `short` (generic tail words stripped) or else `hyphen` (multi-word), `inc` (joined + "inc"), up to 2 `alias` (core canonical / `core.alias` / Form D previous names, normalized, fund-like names and the literal `'None'` dropped), `first_word` (first token, >= 4 chars, multi-word names only). Lever: `joined`, `short`/`hyphen`, `alias` (no `inc`, no `first_word`: 0 true hits in the pilot). Tokens are deduped, >= 3 chars, and pass `adapters._check_token`.

### Verification (pure, rule `R7`)
- [x] Names: every name of the firm normalized (`norm.norm`); canonical names additionally strip leading "the" / "ai", and trailing generic words (data, ai, labs, holdings, ...).
- [x] Location: the firm's city (word-bounded) or state (", CA" abbreviation or full state name, word-bounded) in ANY posting's location fields. A city that is also a state name ("Washington" in DC) only counts with a state hit. "West Virginia" is not "Virginia".
- [x] VERIFIED when the board has >= 1 posting and either (a) the full name matches (board name equals a name, or a name appears in >= 50% of postings, or the board intro text names it) AND (the match was the board name, or the name is not a one-word name under 6 characters, or a location matches); or (b) a canonical name matches (same three ways) AND a location matches.
- [x] Otherwise CANDIDATE when there is a name signal: `empty_board` (0 postings, a name signal), `name_without_location`, `short_name_without_location`; REJECTED otherwise: `name_mismatch`. Every verdict carries a `score` in [0, 1] (ranking aid for the reviewer, not a probability) and its evidence: rule, board name, names and canonical names used, mention shares, location hits, posting count, short-name flag, Form D person count found in postings (count only, no names).
- [x] Replaying the pilot's 48 hand-labelled hits (`tests/fixtures/ats_pilot_hits.json`, features only, no posting text) through the verdict function gives 17 verified, 0 false.

### Storage (migration 0020, additive)
- [x] `ats_board` gains `target_id`, `verification` (`verified|candidate`), `verification_score`, `verified_at`, `careers_domain`, `careers_domain_evidence`; status CHECK gains `candidate`. Index on `target_id`.
- [x] `ats_discovery_run`: one row per run (params, status `running|done|paused|budget_exhausted|failed`, per-site checkpoint = last target_id done, metrics JSON refreshed every chunk: targets done, requests, outcomes, verdicts, reject reasons, verified boards, postings, pay, timeouts, stop reasons, heartbeat).
- [x] `ats_discovery_attempt`: one row per (target_id, ats, token), unique; variant, outcome, HTTP status, requests, verdict, reason, score, board_id, postings, evidence, run_id, time. A re-run UPDATES a row only when the previous outcome was transient.

### Linking
- [x] VERIFIED: the board row becomes `status='active'`, `verification='verified'`, linked to `core_entity_id`, `cik`, `target_id`; its postings are stored as SPEC_151 does (first_seen etc., pay with provenance, redaction) with one `ats_board_fetch` row. An existing active board linked to a DIFFERENT firm is never relinked: the attempt becomes `candidate`, reason `board_linked_to_other_firm`.
- [x] CANDIDATE: `ats_board` row `status='candidate'` with NO firm link (core_entity_id / cik / target_id NULL; the workbench links boards by core entity / CIK); the proposed firm(s) live in `discovery_evidence.proposed`. An existing active / refused row is never downgraded.
- [x] REJECTED: the ledger only (never an `ats_board` row): the pair is not retried by later runs, and the weekly refresh never sees it.
- [x] `--link-workbench`: `workbench.targets_universe.ats_board_id` set for verified targets whose value is NULL (parameterized UPDATE; skipped when the table is absent).

### The run
- [x] One thread per ATS host (greenhouse, lever), each with its own `OpenWebFetcher` (robots on every hop, terms registry, NexdataResearch UA, Retry-After, >= max(Crawl-delay, 2 s) per host) and its own DB session; targets in `target_id` order; a site stops trying variants for a firm once one verifies.
- [x] Resumable / idempotent: finished pairs in the ledger are skipped; a token known `not_found` on a site is not re-requested (recorded with `requests = 0`); a fetched board is re-used within a run for another firm (no second request). Transient outcomes (timeout `error`, 5xx) get one retry pass at the end of the site's pass and are retried by the next run.
- [x] Stop conditions per host: any `retry_after` / `host_backed_off` (429); 3 consecutive transient errors; any refused outcome (robots / terms); the request budget (`--max-requests`, per site); a stop file. Run status says why.
- [x] Dry run (default) writes NOTHING; the JSON report lists every hit with its evidence for hand checking.
- [x] Targets already linked to an active board (by core entity, CIK or target id) are skipped.

### Weekly refresh (SPEC_153) fixes needed for verified boards to join it
- [x] A `refresh` fetch never re-runs name verification (the R7-verified boards would fail the SPEC_151 board-name rule and be dropped to `unverified`).
- [x] A `refresh` fetch updates only status / last_fetched_at / last_outcome / terms citation on the board row: it never rewrites basis, evidence or the firm link (measured 2026-10-05: `discovery_basis = 'refresh'` violates `ck_ats_board_basis`, so the first scheduled run, 2026-10-06 03:00 UTC, would have raised).

### Careers domain (SPEC_148, weak)
- [x] From a verified board: the registrable domain of posting URLs when >= 50% of postings share it (`posting_url`), else a domain in posting / intro text whose first label equals or starts with a canonical name of >= 4 characters (`posting_text`); ATS / social / generic hosts dropped by `domains.domain`. Stored on `ats_board.careers_domain` with evidence.
- [x] New attach-only feed `atsboard` (family `ats_board`) feeds those domains (with the firm's CIK / EIN) into `core.source_record`, so the monthly `entity_resolve` writes them to `core.domain_link`: ONE family, therefore `weak`; `strong` only if another independent family (own_site, edgar, ...) claims the same domain for the same subject.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_variants_greenhouse_order | GH variant kinds/order, inc + first_word, alias cap 2, dedupe |
| T2 | test_variants_lever_subset | Lever: no inc, no first_word |
| T3 | test_variants_drop_none_and_funds | literal 'None', fund / SPV previous names dropped; short tokens dropped |
| T4 | test_canon_names | tail / leading-word stripping |
| T5 | test_location_rules | city word-bounded; state abbr / full name; Washington DC vs WA; West Virginia |
| T6 | test_verify_full_name_board | board name equal -> verified even for a short name without location |
| T7 | test_verify_short_name_needs_location | one-word < 6 chars matched in postings only -> candidate w/o location, verified with |
| T8 | test_verify_canonical_needs_location | canonical-only match: candidate without location, verified with a state hit |
| T9 | test_verify_empty_and_mismatch | 0 postings -> candidate empty_board / rejected name_mismatch; no signal -> rejected |
| T10 | test_score_and_evidence | score in [0,1], ordered sensibly; evidence keys; persons as a count only |
| T11 | test_pilot_replay_precision | the 48 labelled pilot hits -> 17 verified, 0 false |
| T12 | test_careers_domain | posting_url majority; text domain must match the name; generic hosts dropped |
| T13 | test_run_gated_dry_run_writes_nothing | mock transport: verified board found, db untouched, every request via gate with UA |
| T14 | test_run_stops_host_on_429 | 429 -> that host stops, run status paused, other host continues |
| T15 | test_run_stops_after_3_errors_and_retry_pass | timeouts retried once; 3 consecutive errors stop the host |
| T16 | test_run_budget | --max-requests stops the site with budget_exhausted |
| T17 | test_run_skips_ledger_and_reuses_boards | final pairs skipped; known not_found token 0 requests; board fetched once for two firms |
| T18 | test_refresh_skips_verify | refresh basis -> no name verification, postings fetched |
| T19 | test_store_refresh_keeps_basis_pg | PG: refresh store keeps basis/evidence/link; no CHECK violation |
| T20 | test_apply_pg_links_and_ledger | PG: verified -> active linked + postings + fetch row; candidate -> unlinked candidate row; rejected -> ledger only; re-run idempotent; other-firm board not relinked; run row metrics |
| T21 | test_attach_feed_atsboard | feed exists, attach-only, family ats_board, eight attach-only sources |
| T22 | test_cli_args | CLI parses --limit / --target-ids-file / --apply / --max-requests / --resume-run |
| T8b | test_canonical_state_only_needs_plain_drop | R7b: a canonical match that dropped a descriptive word needs a city hit |
| T23 | test_readjudicate_demotes_under_new_rule | PG: stored verified links re-decided from evidence; demoted -> unlinked candidate |

## Rubric Checklist (no `service` rubric file exists; generic)

- [x] Tests written and watched failing before source code
- [x] Parameterized SQL only
- [x] open_web gate on every request; Ashby never requested; no robots override
- [x] Dry run first (200 firms not in the pilot), every verified hit hand-checked, precision compared with the pilot
- [x] Idempotent writes; nothing dropped silently (every attempt has an outcome)
- [x] No PII beyond postings (existing redaction; Form D persons as a count only)
- [x] ruff clean on touched files; catalog column dictionary regenerated
- [x] Session log entry

## Design Notes

- `app/sources/ats_boards/match.py` (pure): `canon`, `firm_names`, `variants(target, ats)`, `location_hits`, `verdict(ats, target, meta, postings)`, `careers_domain(target, postings, meta)`.
- `app/sources/ats_boards/targets.py`: `Target`, `load_targets(db, ...)`, `SiteRunner` (one per host), `run(...)`, storage (`link_verified`, `record_candidate`, `record_attempt`), CLI `python -m app.sources.ats_boards.targets`.
- `collect.store` split into the board upsert and `store_postings(db, board_id, br)`; refresh path updates the board row by id.
- Ledger finality: `fetched` (any verdict) and `not_found` are final; everything else is transient and retried.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| alembic/versions/0020_ats_targets_discovery.py | Create | columns + 2 tables |
| app/sources/ats_boards/match.py | Create | variants, R7, score, careers domain |
| app/sources/ats_boards/targets.py | Create | runner, ledger, linking, CLI |
| app/sources/ats_boards/collect.py | Modify | refresh skips verify; store split; refresh keeps basis |
| app/entities/feeds.py, app/entities/resolve_core.py | Modify | attach-only feed `atsboard`, family `ats_board` |
| app/catalog/datasets.py (+ generated columns) | Modify | new tables on the ats_boards dataset |
| tests/test_spec_155_ats_targets_discovery.py, tests/fixtures/ats_pilot_hits.json | Create | T1-T22 |
| tests/test_spec_148_domain_identity.py | Modify | eight attach-only sources |

## Results (2026-10-05 / 06)

- Tests: 22 watched fail, then pass; +T8b, T23 (rule fix, below) watched fail then pass: 24/24. Neighbour suites
  (147/148/149/151/153/154/116) green against a throwaway PG; catalog suites back to baseline after the lineage
  input + usage.json / columns.generated.json regeneration. ruff clean. Migration 0020 applied live (one-off container).
- Pilot replay through `match.py` (full posting text): 17 verified, 0 false (unchanged under R7b).
- DRY RUN, 200 kept targets not in the pilot (seeded random): 962 requests / 20 min; 13 verified, all 13 correct
  on hand check; 2 candidates (Axle Labs -> Axle Informatics, correctly unlinked; Code for America, remote-only).
- LIVE run 1 (`ats_discovery_run` 1, 16:14 -> 02:21 UTC, one-off container, paused once by stop file for the rule
  fix and resumed from the ledger): 5,784 targets scanned on both sites (2 skipped: already linked to a SPEC_151
  board); 27,696 board requests (GH 17,567, Lever 10,129; plus robots), 0 x 429, 0 refusals, 63 timeouts all
  retried to a final outcome. 291 verified links (GH 239, Lever 52; 291 firms), 135 candidate boards (152 firms:
  name_without_location 93, empty_board 47, canonical_state_only 11, short_name 3, board_linked_to_other_firm 1),
  232 rejected pairs (name_mismatch). 6,716 open postings on verified boards, 4,051 with pay (1,911 structured).
  119 verified boards carry a careers domain. workbench.targets_universe.ats_board_id set for all 291.
- Spot checks during the run: 22 + 20 + 20 random verified links; one false link found (Luminate Health ->
  Luminate the LA data firm: canonical name dropped "health", state-only match) -> rule R7b (a canonical match that
  dropped a descriptive word needs the firm's city) + `--readjudicate` (1 of 41 then-verified links demoted).
- SPEC_148: the 119 careers domains reach `core.domain_link` as WEAK (single family `ats_board`) at the next
  `entity_resolve` (monthly, 2026-10-10 04:00 UTC); not triggered by hand.

## Feedback History

_No corrections yet._
