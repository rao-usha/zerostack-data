# SPEC 013 — PE Pipeline V2: Firm-Scoped Stage Tracking

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_013_pe_pipeline_v2.py

## Goal

Enhance the PE deal pipeline with firm-scoped views and granular stage tracking (Screening → DD → LOI → Closing → Won/Lost). Enables PE firms to manage their deal funnel with conversion metrics and pipeline health per firm.

## Acceptance Criteria

- [ ] `GET /pe/pipeline/{firm_id}` returns deals grouped by stage for a specific firm
- [ ] `POST /pe/pipeline/{firm_id}/deals` creates a deal linked to a firm via PEDealParticipant
- [ ] `PATCH /pe/deals/{deal_id}/stage` moves a deal between pipeline stages
- [ ] `GET /pe/pipeline/{firm_id}/insights` returns pipeline health metrics per firm
- [ ] Pipeline stages: Screening, DD, LOI, Closing, Won, Lost (plus legacy Announced/Pending/Closed)
- [ ] Seed 8-10 additional deals across all pipeline stages
- [ ] All tests pass

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_firm_deals_grouped_by_stage | Deals grouped correctly by stage |
| T2 | test_firm_deals_empty | Empty result for firm with no deals |
| T3 | test_firm_insights_conversion | Conversion metrics computed correctly |
| T4 | test_firm_insights_empty | Empty firm returns zero metrics |
| T5 | test_stage_transition_valid | Stage update works for valid transitions |
| T6 | test_pipeline_stages_comprehensive | All 6 stages represented in grouping |

## Design Notes

- Firm→Deal chain: PEFirm → PEDealParticipant.firm_id → PEDeal, plus buyer_name match
- Pipeline stages extend PEDeal.status (VARCHAR 50, no schema change needed)
- `_group_by_stage()` static method for testability
- `_compute_firm_insights()` static method: total value, stage counts, conversion rates

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_deal_pipeline.py | Modify | Add firm-scoped methods |
| app/api/v1/pe_benchmarks.py | Modify | Add 4 new endpoints |
| app/sources/pe/demo_seeder.py | Modify | Add pipeline deals with stage values |
| tests/test_spec_013_pe_pipeline_v2.py | Create | Tests for firm-scoped pipeline |
