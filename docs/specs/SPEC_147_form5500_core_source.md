# SPEC 147 — Form 5500 sponsors as an entity-resolver source (EIN strong, name+state weak)

**Status:** Active
**Task type:** service
**Date:** 2026-09-30
**Plan:** `docs/plans/PLAN_091_form5500_core_source.md`
**Test file:** tests/test_spec_147_form5500_core_source.py

## Goal

Measured 2026-09-30: only 744 of 53,425 Form 5500 sponsor rows (51,189 distinct EINs) link to a
core entity, because Form 5500 is not a core source at all (workbench PE-TARGETS-DEPTH-PLAN
phase 2). Feed the sponsors into `core.source_record` keyed on EIN, so the identifier-only
resolver joins them to SEC filers carrying the same EIN, and add a WEAK name+state tier that is
recorded as flagged candidates (never merged) with every contradiction written down.

## EIN-bearing sources in NexData (measured, read-only, 2026-09-30)

| Table | EIN rows | Sponsor EINs it shares |
|---|---|---|
| `sec_filers.ein` (EDGAR submissions) | 364,253 (all 9 digits) | 3,283 distinct sponsor EINs |
| ...of which already in `core.source_record` (edgar feed scope) | 83,662 | 1,567 |
| `pe_portfolio_companies.ein`, `canonical_entities.ein` | 0 filled | 0 |
| Form D, ADV, IAPD, 13F | no EIN column | — |
| 990s, PPP | not in NexData | — |

So EDGAR submissions is the only strong EIN partner. The edgar feed is scoped to PE-relevant
CIKs; 2,166 CIKs (1,849 sponsor EINs) carry a sponsor EIN but sit outside that scope.

## Acceptance Criteria

- [x] A `dol5500` feed writes one `core.source_record` per sponsor EIN (`record_key = dol5500:<ein>`),
      latest `form_year` wins, name/state/zip5 carried, `observed_at` = load time (provenance).
- [x] EIN normalized to 9 digits with leading zeros kept (an int or 7–8 digit numeric string is
      left-padded); placeholder / repeated-digit / wrong-length EINs never become keys.
- [x] The edgar feed scope adds every `sec_filers` CIK whose EIN is a sponsor EIN.
      **Amended by SPEC_149 (2026-10-01):** only when that EIN is carried by exactly one
      `sec_filers` CIK; a shared EIN fused subsidiaries, insiders and ESOPs into the sponsor.
- [x] When `workbench.dol5500_sponsor` does not exist the feeds still run; the dol5500 feed is
      reported as skipped (0 rows), never an error.
- [x] EIN is a strong key: a sponsor and an EDGAR filer sharing an EIN resolve to one entity
      (`exact_key`), with name/state disagreements recorded in `field_conflicts`, not resolved.
- [x] Name+state is a WEAK tier: `weak_name_state()` is pure, never changes components, and
      writes `core.weak_match` rows with a status: `corroborates`, `candidate`, `ambiguous`,
      `conflict` (with reasons `ein_conflict`, `strong_match_elsewhere`).
- [x] Weak candidates per sponsor are capped; the overflow is counted, not dropped silently.
- [x] Resolve metrics carry a `dol5500` block: sponsors fed, matched strong (EIN), weak by status,
      new entities containing a sponsor, conflicts; dry run reports the same numbers and writes nothing
      but the ledger row.
- [x] Re-running over an unchanged corpus writes 0 weak_match rows (idempotent).
- [x] Migration `0016_entity_weak_match` creates `core.weak_match` idempotently.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_ein9_pads_and_refuses | int/8-digit padded, hyphens stripped, placeholders/garbage refused |
| T2 | test_dol5500_row_keyed_on_ein | `_row` for dol5500: record_key, native_id = 9-digit EIN, zip5, state |
| T3 | test_dol5500_row_without_valid_ein_dropped | a sponsor with no usable EIN is not fed |
| T4 | test_weak_never_merges | plan() components identical with/without name+state twins |
| T5 | test_weak_candidate_for_unmatched_sponsor | unmatched sponsor + one SEC record same name+state -> `candidate` |
| T6 | test_weak_ein_conflict | candidate carries a different EIN -> `conflict` / `ein_conflict` |
| T7 | test_weak_corroborates_strong_match | candidate in the sponsor's own component -> `corroborates` |
| T8 | test_weak_strong_match_elsewhere | strongly matched sponsor, name twin in another component -> conflict |
| T9 | test_weak_ambiguous_and_cap | >1 identity -> `ambiguous`; over cap stored capped and counted |
| T10 | test_weak_needs_name_and_state | no state or no name -> no candidates; same name other state -> none |
| T11 | test_dol5500_metrics | per-source metric block (fed, strong matched, weak by status) |
| T12 | test_feeds_dol5500_pg | PG: feed + edgar scope expansion + latest form_year + leading zeros |
| T13 | test_feeds_without_workbench_table_pg | PG: no workbench table -> skipped, SEC feeds unchanged |
| T14 | test_resolve_dol5500_end_to_end_pg | PG: EIN join to edgar, weak rows written, entity carries EIN |
| T15 | test_resolve_dol5500_idempotent_and_dry_run_pg | PG: rerun writes 0 weak rows; dry run writes none |
| T16 | test_migration_0016_idempotent_pg | PG: migration applies twice cleanly |
| T17 | test_merge_dissolves_absorbed_entity_pg | PG: a merge forwards the absorbed entity (`_dissolve` was undefined since SPEC_116) |

## Rubric Checklist

(No `service` rubric file exists in memory/rubrics; generic checklist.)
- [x] Tests written and watched failing before the code
- [x] Parameterized SQL only; no string-built untrusted input
- [x] Dry run first, before/after counts recorded
- [x] ruff clean on touched files
- [x] Session log entry

## Design Notes

- `feeds.py`: `Feed` gains `requires` (a relation that must exist) and the run builds the feed
  list per connection (`feeds_for(conn)`), so the edgar scope includes sponsor EINs only when the
  table exists. Native id for dol5500 is the normalized EIN.
- `resolve_core.weak_name_state(records, rec_keys, comps)` — pure. Candidates are records of any
  OTHER source with identical `(name_norm, state)`. Status precedence: corroborates > conflict >
  ambiguous > candidate. `WEAK_CANDIDATE_CAP = 5` identities per sponsor.
- `resolve.py`: computes the weak tier after `plan()`, adds `metrics["dol5500"]` and
  `metrics["weak"]`; on a real run stages `core.weak_match` (merge + delete rows no longer derived),
  `candidate_entity_id` = the entity of the candidate's component (NULL when the candidate is a
  keyed singleton / keyless record).
- Weak tier merges nothing, in any case. Promotion of clean candidates is an owner call (it would
  change merge rules).
- The 5500 data is read from `workbench.dol5500_sponsor` (loaded 2026-09-01 by the workbench
  `dol_form_5500` connector from DOL's public bulk files). It is already in the shared NexData DB
  (owner rule: NexData-only data); no new fetch is added. The catalog does not model workbench
  tables, so lineage cannot see this input; recorded as a limitation.
- Sponsor singletons (EIN shared with nobody) stay keyed singletons, not entities: the resolver
  materializes components of >= 2 records. Changing that is a materialization-rule owner call.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/entities/feeds.py` | Modify | dol5500 feed, `_ein9`, zip5, edgar scope, `feeds_for` |
| `app/entities/resolve_core.py` | Modify | `weak_name_state`, `source_metrics` |
| `app/entities/resolve.py` | Modify | weak tier + metrics + `core.weak_match` writes |
| `alembic/versions/0016_entity_weak_match.py` | Create | `core.weak_match` |
| `app/catalog/datasets.py` | Modify | descriptions + `core.weak_match` table + limitation |
| `tests/test_spec_147_form5500_core_source.py` | Create | T1–T16 |

## Results (2026-09-30, job_queue 3049/3050 dry, 3051 LIVE; core.mart_build 1-3)

| | Before | After |
|---|---|---|
| core.source_record | 376,277 | 429,717 (dol5500 51,189; edgar +2,177) |
| live core.entity | 140,963 | 143,506 (+2,543 new, 0 merged, 0 split) |
| core.identifier ein / cik / crd | 77,191 / 123,511 / 24,559 | 79,718 / 126,501 / 24,563 |
| sponsor EINs in core.identifier | 744 | 3,283 (rows 754 -> 3,381) |
| core.weak_match | — | 2,628 (corroborates 1,852 / conflict 371 / candidate 384 / ambiguous 21) |

Sponsors: 3,283 matched by EIN (2,539 of them in new entities), 47,906 keyed singletons;
weak tier per sponsor: corroborates 1,278, conflict 120, candidate 215, ambiguous 5 (0 over cap).
Strong components with recorded field conflicts: state 2,086, name 862, cik 236, domain 36, ein 1.

## Feedback History

- 2026-09-30: the job ledger's `compact_summary` drops nested metric blocks, so the first dry run
  lost the dol5500/weak numbers; metrics are now flattened (`a.b` keys) and T14 asserts they survive.
- 2026-09-30: `_dissolve` was undefined on the merge path (latent since SPEC_116); fixed with T17.
