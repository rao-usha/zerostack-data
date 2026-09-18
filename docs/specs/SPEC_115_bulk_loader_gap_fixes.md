# SPEC 115 — Bulk loader gap fixes

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-18
**Test file:** tests/test_spec_115_bulk_loader_gap_fixes.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 1 follow-up)

## Goal

Close the three gaps the Phase 1 loaders reported:

1. **Reloads rewrite every row.** `merge_staging` always runs the UPDATE branch, so re-loading an unchanged release rewrites the whole table, leaving dead rows behind and burning Cloud SQL disk until vacuum.
2. **Shared 8-Ks lose companies.** One 8-K filed for several companies appears once per company in `submissions.zip`, but `sec_8k_index` is keyed on `accession_number` alone, so only one company survives.
3. **Stale `sec_financial_facts`.** 33,140 rows carry the pre-fix filing labels (D6), and `public_company_financials` still joins them for depreciation, so the view mixes corrected statements with wrong-period D&A.

## Acceptance Criteria

- [ ] `merge_staging` adds `WHERE (target.cols) IS DISTINCT FROM (EXCLUDED.cols)`, so unchanged rows are not rewritten. It still returns `(inserted, updated)`, where `updated` counts only rows that really changed.
- [ ] `build_merge_sql(..., skip_unchanged=False)` keeps the old behavior for callers that need a touch (none today).
- [ ] `sec_8k_index` is keyed on `(accession_number, cik)`; the loader merges on both. Migration `0007` rebuilds the key.
- [ ] A concept allowlist (`FACT_CONCEPTS`, the three depreciation concepts the view reads) is loaded into `sec_financial_facts` by the `sec_companyfacts` loader, keyed on (cik, fact_name, period_end_date, fiscal_year, fiscal_period, unit), with the corrected period labels.
- [ ] Migration `0007` moves the 33,140 stale fact rows into `quarantine.sec_financial_facts_pre_period_fix` before the loader repopulates them.
- [ ] `xbrl_parser.build_financial_facts(facts_data, cik, names, min_period_end)` returns fact rows for an allowlist without building every fact in the file.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_merge_sql_has_is_distinct_from_guard | SQL contains the IS DISTINCT FROM predicate; opt-out drops it |
| T2 | test_merge_skips_unchanged_rows_pg | (PG) re-merging identical rows → (0, 0) and `xmin` unchanged; a changed row → (0, 1) |
| T3 | test_merge_still_inserts_and_updates_pg | (PG) new rows insert; changed rows report as updated |
| T4 | test_8k_index_key_includes_cik | ddl() keys on (accession_number, cik) and the loader merges on both |
| T5 | test_shared_8k_keeps_every_company_pg | (PG) one accession filed by 2 CIKs → 2 rows |
| T6 | test_build_financial_facts_allowlist | Only allowlisted concepts, own-period labels, latest filed wins |
| T7 | test_companyfacts_loads_facts_pg | (PG) fixture load writes sec_financial_facts rows with the right labels; reload is idempotent |
| T8 | test_migration_0007_chain_and_sql | 0007 revises 0006; quarantines facts; rebuilds the 8-K key |

## Rubric Checklist

- [ ] Root cause fixed, not the symptom
- [ ] Regression tests, including Postgres behavior
- [ ] Reversible migration
- [ ] No behavior change for other callers of merge_staging

## Design Notes

- The IS DISTINCT FROM guard uses the update column list, so a row whose non-key columns match is left completely untouched (no new tuple version).
- `loaded_at` / `source_release_key` are part of the update list, so they would defeat the guard; both are excluded from the comparison and only set on insert or on a real change.
- Facts stay deliberately narrow: loading every us-gaap + dei fact for 3 years would add an estimated 2M rows (~1 GB), which the lean budget doesn't have. The allowlist covers exactly what the view consumes.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/copy_loader.py | Modify — skip_unchanged guard |
| app/ingest/bulk/sec_edgar_submissions/source.py | Modify — 8-K key |
| app/sources/sec/xbrl_parser.py | Modify — build_financial_facts |
| app/ingest/bulk/sec_companyfacts/source.py | Modify — load facts allowlist |
| alembic/versions/0007_bulk_gap_fixes.py | Create |
| tests/test_spec_115_bulk_loader_gap_fixes.py | Create |

## Feedback History

_No corrections yet._
