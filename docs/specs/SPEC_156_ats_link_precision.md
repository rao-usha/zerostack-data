# SPEC 156 — ATS board links: precision fixes, retraction, re-verification, pay and refresh fixes

**Status:** Implemented (live 2026-10-06: 18 links retracted, re-verified, Cockroach Labs linked)
**Task type:** service
**Date:** 2026-10-06
**Plan:** `docs/plans/PLAN_101_ats_link_precision.md`
**Test file:** tests/test_spec_156_ats_link_precision.py
**Builds on:** SPEC_155 (discovery, rule R7/R7b), SPEC_151 (pay parser, storage), SPEC_153 (weekly refresh), SPEC_148 (weak domain claims)
**Evidence:** 2026-10-06 independent review of run 1 (50 verified links hand-checked: 46 true / 3 false / 1 unsure; 6 confirmed false links of 291 + 1 likely; pay sample 28/30)
**Numbering:** SPEC_156 / PLAN_101 (SPEC_155 and PLAN_099/100 are taken; 156-159 unused). Alembic 0021.

## Goal

Raise the precision of discovery-verified board links and remove the false ones from everything that
reads them: rule R8 (names are not merged by normalisation; an ambiguous one-word name or a previous
name needs corroboration; a state code is not a city; an ambiguous canonical name needs more than one
location hit), a `rejected` board status with a reason whose postings become `retracted` (kept, never
read as open roles), re-verification of every verified link with fresh board data through the gate,
and the reviewer's other defects (Cockroach Labs seed never linkable, EIN-only careers domains,
single-404 refresh drop, four pay-parser errors).

## Acceptance Criteria

### Rule R8 (pure, `match.py`, RULE_VERSION `R8-2026-10-06`)
- [x] Names: `match.mnorm` folds like `norm.norm` but keeps every token (single letters too) and strips
  legal-form suffixes from the tail without merging names: after one suffix is stripped a foreign
  form (AG, SA, NV, BV, GmbH, ...) is kept ("TIFIN AG Inc." -> "tifin ag", "Metabase Q, Inc." ->
  "metabase q"; "Foo AG" -> "foo"). Every name / canonical name / careers-domain key uses it.
- [x] Ambiguous one-word name: when every full-name match is a single token whose document frequency
  in NexData's own name corpus (`core.entity` canonical names + Form D issuer names, normalized, the
  firm itself included) is >= 3, or unknown, the match needs corroboration: a location hit or a Form D
  person in the postings. Else CANDIDATE `ambiguous_name_without_location` (even when the board name
  is exact: Verve, Socket, Motive).
- [x] Previous name only: a full-name match on a Form D / EDGAR previous name alone needs the same
  corroboration, else CANDIDATE `previous_name_without_location` (inKind -> GoodUnited). A previous name
  equal to a CANONICAL current name ("Prismatic LLC" -> "Prismatic Software Inc.") counts as current.
- [x] A stored city that is a 2-letter code or a state abbreviation ("Ny") is not a city: it never
  matches (only the state does).
- [x] Ambiguous canonical name: a canonical match whose matched canonical name is one ambiguous token
  (df >= 3) verifies only with the full name in some posting / the intro, a Form D person, or the
  firm's city in >= 10% of postings; else CANDIDATE `ambiguous_canonical` (Axios HQ -> Axios Media).
- [x] The pilot replay (stored features, no df) keeps 0 false verified links.

### Storage (migration 0021, additive)
- [x] `ats_board.status` gains `rejected`; `verification` gains `rejected`; `ats_board.ein` (TEXT).
- [x] `ats_posting.status` gains `retracted`.

### Retraction (`targets.retract`)
- [x] A retracted board: `status='rejected'`, `verification='rejected'`, `last_outcome='retracted'`,
  firm link (core_entity_id, cik, target_id, ein) and careers domain cleared, the previous link,
  evidence and domain kept under `discovery_evidence.retracted` with the reason and time; its
  postings `open`/`closed` -> `retracted` (rows kept); its verified attempts -> `rejected` with the
  reason; `workbench.targets_universe.ats_board_id` cleared where it points at the board. Dry run
  writes nothing. Idempotent.
- [x] A `candidate` board never carries open postings: retraction of the postings of any candidate
  board (Luminate board 60) is part of the sweep.

### Re-verification (`targets.reverify`)
- [x] For every discovery-verified active board (or a given list): fetch meta + jobs through the
  open_web gate (same fetcher, pacing, stop conditions), recompute R8 for its linked target (names,
  persons, df loaded as for discovery). Verified -> evidence / score / rule version / careers domain
  refreshed, postings merged with one fetch row. Not verified -> retracted with the R8 reason.
  A fetch that is not `fetched` changes nothing (reported). A board with 0 postings today
  (`empty_board`) is kept (`keep_empty`: no openings is no evidence against the link; its open postings
  close as the weekly refresh would). Dry run (default) writes nothing.
- [x] Attempts that ended `board_linked_to_other_firm` are re-tried with the fixed same-firm check.

### Same firm (Cockroach Labs)
- [x] An active board with NO firm keys (core entity / CIK / target; an industrial_companies id is not a
  firm key) whose company name normalizes to one of the firm's names is the same firm: verification adds
  the link instead of `board_linked_to_other_firm` (board 18 carries industrial id 190 "Cockroach Labs").

### Careers domain feed
- [x] Linking stores the target's EIN on `ats_board.ein`; the `atsboard` feed passes `ein`, so an
  EIN-only firm's careers domain attaches by EIN.

### Weekly refresh
- [x] One 404 on a refresh keeps the board `active` (last_outcome `not_found`); a second consecutive
  404 sets `not_found`.

### Pay parser (`pay_v2`)
- [x] "+ Bonus" / "plus sales commission" / "(including a bonus)" after a salary range no longer
  disqualifies it; a disqualifier before the amount is overridden by a later salary cue in the same
  window ("Monthly Stipend The salary for this role is $50,000-$80,000").
- [x] An amount right after "+" or followed by variable / commission / incentive is not pay
  (Wunderkind "+ $11,500 Variable").
- [x] A range whose max is more than 4x its min is not one role's band (Tenstorrent "$100k - $500k for
  all engineers").
- [x] Two-decimal amounts in the hourly band with no interval word are hourly ("$33.17 — $44.39 USD").
- [x] Greenhouse `pay_input_ranges` in a zero-decimal currency (JPY, KRW, ...) are not divided by 100,
  and plausibility is checked in USD terms for those currencies.
- [x] `collect.reparse_pay(db, apply)` re-parses stored text pay of open postings and fixes stored
  zero-decimal structured pay; dry run reports counts.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_mnorm_keeps_tokens | Metabase Q / TIFIN AG / Foo AG / DBA / single letters |
| T2 | test_ambiguous_one_word_needs_corroboration | Verve exact board name, df 7, no location -> candidate; with location or person -> verified; rare name (df 1) still verified |
| T3 | test_previous_name_only_needs_location | inKind board -> GoodUnited (previous name) candidate; with location verified |
| T4 | test_state_code_city_ignored | city "Ny" never matches ", NY"; Orchestra Health -> candidate |
| T5 | test_ambiguous_canonical | Axios HQ: df 4, city 5% -> candidate; full name in a posting or city >= 10% -> verified |
| T6 | test_metabase_q_not_metabase | Metabase Q vs Metabase board postings -> not verified |
| T7 | test_pilot_replay_still_precise | pilot fixture: 0 false verified |
| T8 | test_same_firm_unkeyed_seed | `_same_firm` true for a keyless seed with the firm's name, false for another name |
| T9 | test_feed_passes_ein | atsboard feed SQL selects `ein` |
| T10 | test_pay_plus_bonus_and_stipend | the 5 unparsed examples parse; Wunderkind min stays 72,000 |
| T11 | test_pay_wide_range_and_hourly_and_jpy | Tenstorrent -> None; $33.17—$44.39 hourly; JPY not /100 |
| T12 | test_refresh_single_404_keeps_active_pg | PG: first 404 keeps active, second -> not_found |
| T13 | test_retract_pg | PG: board rejected, link + domain cleared and kept in evidence, postings retracted, attempts rejected, workbench link cleared, dry run writes nothing, idempotent |
| T14 | test_reverify_pg | PG + mock gate: a false link retracted, a true link refreshed (rule version R8), a failed fetch changes nothing, dry run writes nothing |
| T15 | test_link_stores_ein_and_claims_seed_pg | PG: link_verified writes ein; keyless seed claimed |
| T16 | test_reparse_pay_pg | PG: reparse fixes text pay + JPY, dry run writes nothing |
| T17 | test_cli_reverify_retract_args | CLI parses --reverify / --retract / --reason / --board-ids |

## Rubric Checklist

- [x] Tests written and watched failing before source code
- [x] Parameterized SQL only; open_web gate on every request; Ashby never requested
- [x] Dry run before every live write; nothing deleted (rejected boards and retracted postings kept)
- [x] Fresh hand check of 20 links after the fix
- [x] ruff clean on touched files; catalog column dictionary regenerated
- [x] Session log entry

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| alembic/versions/0021_ats_link_precision.py | Create | statuses, ein |
| app/sources/ats_boards/match.py | Modify | R8 |
| app/sources/ats_boards/targets.py | Modify | df loading, retract, reverify, same-firm seed, ein, CLI |
| app/sources/ats_boards/collect.py | Modify | refresh 404 tolerance, reparse_pay |
| app/sources/ats_boards/pay.py | Modify | pay_v2 fixes |
| app/entities/feeds.py | Modify | atsboard feed passes ein |
| app/catalog/* | Modify | column dictionary |
| tests/test_spec_156_ats_link_precision.py | Create | T1-T17 |
| tests/test_spec_155_ats_targets_discovery.py | Modify | T6 (MinIO) and T11 under R8 |

## Results (2026-10-06)

- Tests: T1-T17 watched fail, then pass; later additions (table "Variable based on", industrial-id seed,
  empty board kept, Prismatic previous name) each watched fail first. Lane suites 147/148/149/151/152/153/
  154/155/156: 197 passed against the throwaway PG (nexdata-qtest-pg, stopped after). Catalog suites:
  failures are the pre-existing environment ones (frontend not mounted, /app/data/reports permission,
  census patterns), none ATS. ruff clean; column dictionary --check up to date. SPEC_155 T6/T8 updated
  for R8 (MinIO needs a corpus df; rule labels R8_*).
- Migration 0021 applied live (one-off container, run_migrations; head 0021_ats_link_precision).
- Retraction (dry run, then apply): boards 178 verve, 465 axios, 386 socket, 179 onemedical,
  376 orchestra, 218 metabase, 76 motive (checked: Motive the Denver creative agency vs Motive, Inc. OKC)
  and 60 luminate (candidate, 4 stale open postings), reason `review_false_link`: 565 postings retracted.
- Re-verification (dry run 521 requests, then live 521 requests: GH 469 / Lever 52, 0 x 429, 0 stops):
  272 kept, 1 kept empty (BeatStars), 11 retracted under R8: nymbusinc, inkind, roo, transcendinc,
  counterpart, prismatic, instead, motus (ambiguous one-word, no location), futurhealth (previous name
  only), tifin (name_mismatch after mnorm), impact (ambiguous canonical, Santa Barbara in 6.5%).
  Hand view: inkind / tifin / roo / counterpart were the reviewer's "unsure"; Transcend, Impact, Nymbus,
  Prismatic, Motus, FuturHealth are probably TRUE links lost to the stricter rule (recall cost, recorded
  with reason + previous link for reinstatement). Cockroach Labs linked (board 18, workbench 1473157689).
- Before -> after: verified active links 291 -> 274; rejected boards 0 -> 19; candidate boards 135 -> 134;
  open postings on linked boards 12,695 -> 12,016; retracted postings 0 -> 712; careers domains 119 -> 112,
  all now keyed (CIK or EIN; 0 unkeyed, was 37); workbench links 294 -> 276 (0 pointing at a non-active
  board, was 1: GlossGenius -> refused ashby:geniusai).
- Fresh hand check: 20 random verified links outside the review's 50: 20/20 correct (Wilson 95% CI
  84-100%).
- Pay (pay_v2 reparse, dry run then apply): 238 of 8,475 rows changed: 143 gained (the "+ Bonus" /
  "plus commission" / stipend-cue families), Tenstorrent's 81 company-wide "$100k-$500k" rows dropped,
  Wunderkind 6295 min 11,500 -> 72,000, 4 JPY rows x100, mixed base+OTE rows fixed.
- NOT done: workers / api were not restarted (rule), so the weekly refresh (Tue 03:00 UTC) and the
  Oct 10 entity_resolve run the OLD loaded code until the owner restarts them: the data fixes hold
  (rejected boards are never refreshed, the feed reads only active verified boards), but the next refresh
  rewrites pay with pay_v1 and keeps the single-404 drop until a restart.

## Feedback History

_No corrections yet._
