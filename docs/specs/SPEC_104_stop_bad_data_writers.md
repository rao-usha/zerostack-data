# SPEC 104 — Stop the code paths that write fake or misclassified data

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-16
**Test file:** tests/test_spec_104_stop_bad_data_writers.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 0)

## Goal

SPEC_105 will quarantine the bad rows. Before that runs, remove or disable every code path that created them so they can't come back. Affected paths are confirmed in docs/reviews/2026-09-16_pe_collector_review.md (D7–D13, D8) and were re-verified 2026-09-16.

## Acceptance Criteria

- [ ] PE persister no longer writes `13f_holding` (fake portfolio companies, "13F Holdings" funds, investments) or `deal_8k_filing` (8-K false-positive deals). These items are counted as `skipped` with a warning.
- [ ] `press_release_collector` no longer emits `deal_8k_filing` items.
- [ ] No site_intel logistics collector (fmcsa, warehouse_listing, scfi, drewry, freightos, census_trade, port_throughput, air_cargo, usda_truck) substitutes sample or random data. When a source returns nothing, the collector reports an error instead.
- [ ] Good Jobs First collector has no hard-coded seed dataset and returns FAILED with a clear message.
- [ ] `sec_form_adv` sample advisers are removed. `POST /form-adv/ingest` returns 501.
- [ ] LP website collector no longer runs the regex contact extractor.
- [ ] LP runner no longer persists `13f_holding` items. Those rows had wrong CIKs and ×1000 values.
- [ ] Family-office deals collector no longer creates deals from news headlines or Form D search hits.
- [ ] `POST /pe/.../seed-demo` and `POST /pe/ecosystem/seed` return 403 unless `ALLOW_DEMO_SEED=1`.
- [ ] `PEEcosystemSeeder.purge()` deletes firms by their `data_sources` marker, never by name.
- [ ] `prediction_markets` is removed from batch TIER_1 and the `critical` default group.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_persister_skips_13f_holding | No company, fund, or investment is written; item counted as skipped |
| T2 | test_persister_skips_deal_8k_filing | No PEDeal is written; item counted as skipped |
| T3 | test_press_release_collector_emits_no_8k_items | Source no longer builds `deal_8k_filing` items |
| T4 | test_logistics_collectors_have_no_sample_fallbacks | No `_get_sample_*` attribute on any of the 9 collector classes |
| T5 | test_fmcsa_empty_api_reports_failure | Empty API response gives a FAILED result and inserts 0 rows |
| T6 | test_goodjobs_has_no_seed_data | `GJF_SEED_DATA` is gone; collect() returns FAILED |
| T7 | test_form_adv_sample_removed | No `get_sample_advisers` / `ingest_sample_data`; endpoint raises 501 |
| T8 | test_lp_website_no_regex_contacts | `_extract_contacts_from_page` / `_parse_contact_blocks` are gone |
| T9 | test_lp_runner_skips_13f_holding | `_persist_items` with `13f_holding` writes nothing |
| T10 | test_fo_deals_collector_disabled | collect() returns no items and makes no network calls |
| T11 | test_demo_seed_endpoints_forbidden_by_default | Both seed endpoints raise 403 without the env flag |
| T12 | test_ecosystem_purge_keeps_same_named_real_firm | A real firm with a seed-template name survives purge |
| T13 | test_prediction_markets_not_in_default_batch | Not in TIER_1 and not in the `critical` group |

## Rubric Checklist

_No bug_fix rubric in memory/rubrics; generic:_
- [ ] Root cause removed (the writer), not just the rows
- [ ] Regression tests
- [ ] Existing tests updated where they asserted the removed behavior (test_pe_persister 13F, test_incremental_collection critical group)

## Design Notes

- Sample and seed methods are deleted, not feature-flagged. Real data for these areas will come from Phase 1 bulk loaders.
- Logistics collectors raise inside their existing try blocks, so the existing `except` handling produces a FAILED result.
- DB-persisted `collection_groups` rows that still list `prediction_markets` are updated by the SPEC_105 migration.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/sources/pe_collection/persister.py | Remove 13F-holding and 8-K deal handlers, and the synthetic fund helper/cache |
| app/sources/pe_collection/deal_collectors/press_release_collector.py | Remove the 8-K item loop |
| app/sources/site_intel/logistics/{fmcsa,warehouse_listing,scfi,drewry,freightos,census_trade,port_throughput,air_cargo,usda_truck}_collector.py | Remove sample fallbacks |
| app/sources/site_intel/incentives/goodjobs_collector.py | Remove seed data |
| app/sources/sec_form_adv/{client,ingest}.py, app/api/v1/form_adv.py | Remove samples; return 501 |
| app/sources/lp_collection/website_source.py, runner.py | Remove regex contacts and LP 13F persistence |
| app/sources/family_office_collection/deals_source.py | Disable headline and Form D deal extraction |
| app/api/v1/pe_benchmarks.py, app/api/v1/pe_ecosystem.py, app/services/pe_ecosystem_seed.py | Demo seed gate; purge by marker |
| app/core/batch_service.py | Remove prediction_markets |
| tests/test_pe_persister.py, tests/test_incremental_collection.py | Update expectations |

## Feedback History

_No corrections yet._
