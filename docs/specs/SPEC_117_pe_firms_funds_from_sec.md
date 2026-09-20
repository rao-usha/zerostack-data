# SPEC 117 — PE firms and funds from Form ADV + Form D

**Status:** Draft
**Task type:** model
**Date:** 2026-09-20
**Test file:** tests/test_spec_117_pe_firms_funds_from_sec.py
**Plan:** docs/plans/PLAN_083_entity_master_pe_marts.md

## Goal

`pe_firms` holds 102 hand-typed firms (0 with a CRD) and `pe_funds` holds 7 rows. Populate both from the SEC data loaded in Phase 1, additively and reversibly, so the existing endpoints and reports see a real PE universe.

**Measured on the live data (2026-09-20), which sets the expectations below:**
- 7,452 advisers self-declare a PE or VC fund on Form ADV (4,571 PE, 3,251 VC; 6,480 have websites, 2,871 report AUM — ERAs do not).
- 39,141 PE/VC fund vehicles in Form D.
- GP attribution is the weak link: **0 funds** share an identifier with their manager (a fund vehicle has its own CIK), 4,855 match by name core, 1,590 via related-person lists. Roughly 15% of funds get a `firm_id`; the rest are loaded unattributed rather than guessed.

## Acceptance Criteria

- [ ] **Migration 0009**
  - partial unique indexes `pe_firms(crd_number)` and `pe_firms(cik)` where not null (the live table has no unique constraint at all today, not even on name)
  - `pe_funds.cik` + partial unique index (the Form D issuer CIK is the fund's natural key)
  - `pe_funds.firm_id` becomes nullable — most funds cannot be attributed, and NOT NULL would force inventing a parent firm
- [ ] **Firms** (`app/marts/pe_firms_sec.py`): one row per adviser whose latest ADV roster snapshot flags `Any PE Funds` or `Any VC Funds`:
  - name (business name, falling back to legal), legal_name, crd_number, sec_file_number, is_sec_registered (RIA true / ERA false), status
  - headquarters city/state/country, website, aum_usd_millions (`aum_total`/1e6)
  - firm_type and primary_strategy from the PE/VC flags ("Private Equity", "Venture Capital", or both)
  - cik from the entity master when the adviser resolves to an entity carrying one
  - `data_sources = ["SEC ADV"]`
  - crawler-only fields (sector_focus, check sizes, LinkedIn, confidence_score, founded_year, employee_count) stay NULL
- [ ] **Funds** (`app/marts/pe_funds_sec.py`): one row per PE/VC Form D issuer CIK, from its most recent filing:
  - name, cik, strategy ("Private Equity"/"Venture Capital"), vintage_year (year of first sale), first_close_date
  - target_size_usd_millions (total offering amount, NULL when the filing says indefinite), final_close_usd_millions (amount sold), sec_file_number
  - `data_source = 'SEC Form D'`
  - `firm_id` set only by the two link tiers below, otherwise NULL
- [ ] **Link tiers**, recorded per fund:
  1. `name_core` — the fund name starts with an adviser's normalized name core, and that core belongs to exactly one adviser
  2. `related_person` — a related person on the filing matches exactly one adviser's name core
  - a fund matched to two different advisers is left unlinked
- [ ] Both builds upsert on their natural keys (skip-unchanged merge), so re-runs write nothing and the 102 existing firms / 7 existing funds are never touched.
- [ ] Rollback: quarantine rules for `pe_firms.data_sources LIKE '%SEC ADV%'` and `pe_funds.data_source = 'SEC Form D'`.
- [ ] Worker job type `pe_mart_build` + executor; `POST /api/v1/pe/marts/build`, `GET /api/v1/pe/marts/stats`.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_firm_row_mapping | ADV snapshot → firm row: names, CRD, file number, AUM in millions, RIA/ERA flag, strategy from PE/VC flags |
| T2 | test_firm_universe_is_pe_or_vc_only | Advisers with only hedge/real-estate funds are excluded |
| T3 | test_fund_row_mapping | Form D offering → fund row: vintage from first sale, indefinite offering → NULL target size, amounts in millions |
| T4 | test_fund_dedup_keeps_latest_filing | Several filings for one issuer CIK → one row, latest wins |
| T5 | test_link_name_core_tier | Fund name prefixed by a unique adviser core links; ambiguous core does not |
| T6 | test_link_related_person_tier | Related-person name matching one adviser links; two advisers → unlinked |
| T7 | test_build_firms_pg | (PG) firms inserted with data_sources=["SEC ADV"]; rerun writes 0 |
| T8 | test_build_funds_pg | (PG) funds inserted, linked subset has firm_id, rest NULL; rerun writes 0 |
| T9 | test_existing_rows_untouched_pg | (PG) a pre-existing hand-entered firm/fund is unchanged by the build |
| T10 | test_migration_0009_chain | 0009 revises 0008; indexes created; firm_id nullable |
| T11 | test_quarantine_rules_cover_sec_rows | Rules exist for both data_source markers |
| T12 | test_executor_and_endpoint | Job runs the build in a thread; endpoint queues it |

## Rubric Checklist

_No model rubric in memory/rubrics; generic:_
- [ ] Additive and reversible (quarantine rules)
- [ ] Natural-key upserts; no duplicate fan-out
- [ ] Unattributed funds left NULL rather than guessed
- [ ] Parameterized SQL; identifiers via `safe_sql.qi`

## Design Notes

- `pe_firms.name` uses the **business** name (what the firm is called) and keeps the legal name alongside; the two differ for most advisers, and the existing 102 rows are brand names.
- AUM comes from `aum_total`, which ERAs leave blank — 4,581 of the 7,452 firms will have NULL AUM. That is the source's limit, not a mapping bug.
- The link tiers run in Python (the normalizer is `app.entities.norm`), not SQL, because `norm.core()` has no SQL equivalent.

## Files to Create/Modify

| File | Action |
|------|--------|
| alembic/versions/0009_pe_mart_keys.py | Create |
| app/marts/{__init__,pe_firms_sec,pe_funds_sec,links}.py | Create |
| app/worker/executors/pe_marts.py, app/api/v1/pe_marts.py | Create |
| app/core/quarantine.py, app/core/models_queue.py, app/worker/main.py, app/main.py | Modify |
| tests/test_spec_117_pe_firms_funds_from_sec.py | Create |

## Feedback History

_No corrections yet._
