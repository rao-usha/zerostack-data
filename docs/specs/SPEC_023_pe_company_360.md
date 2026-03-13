# SPEC 023 — PE Company 360

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-13
**Test file:** tests/test_spec_023_pe_company_360.py

## Goal

Build a unified Company 360 endpoint that aggregates all available intelligence about a PE target into a single response — financial benchmarks, exit readiness, deal score, comps, leadership, competitors, alerts, and pipeline status.

## Acceptance Criteria

- [ ] Single endpoint returns all 12 data sections
- [ ] Missing data sections return null/empty, never error
- [ ] Response includes company profile, financials, scores, leadership, pipeline
- [ ] Works for any portfolio company with seeded demo data
- [ ] Pure aggregation — no new data computation, only calls existing services

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_360_response_structure | All sections present in response |
| T2 | test_360_handles_missing_sections | Missing data → null, not error |
| T3 | test_360_scores_included | Exit + deal scores included |
| T4 | test_360_leadership_populated | Leadership list populated |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_company_360.py | Create | 360 aggregator service |
| app/api/v1/pe_benchmarks.py | Modify | Add GET /pe/companies/{company_id}/360 |
| tests/test_spec_023_pe_company_360.py | Create | Tests |
