# SPEC 006 — PE Valuation Comps & EBITDA Multiple Benchmarking

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_006_pe_valuation_comps.py

## Goal

Build a valuation comparables service that calculates EV/Revenue and EV/EBITDA multiples for a company against its peer set. Returns company multiple, peer median, P25/P75, and percentile rank to support exit pricing decisions.

## Acceptance Criteria

- [ ] `ValuationCompsService` queries pe_company_valuations + pe_company_financials
- [ ] Calculates EV/Revenue and EV/EBITDA for target company
- [ ] Builds peer set by matching industry/sub_industry
- [ ] Returns peer stats: median, P25, P75, percentile rank
- [ ] `GET /pe/valuation-comps/{company_id}` returns full comp analysis
- [ ] 10+ valuation records seeded in demo_seeder.py
- [ ] Tests cover happy path, no peers, missing financials

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_compute_multiples | EV/Revenue and EV/EBITDA calculated correctly |
| T2 | test_peer_percentile_rank | Company ranked correctly among peers |
| T3 | test_no_peers | Graceful response when no peers found |
| T4 | test_missing_financials | Handles company with no financial data |
| T5 | test_endpoint_response | API returns correct schema |
| T6 | test_peer_stats | Median, P25, P75 computed correctly |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_valuation_comps.py | Create | ValuationCompsService |
| app/api/v1/pe_benchmarks.py | Modify | Add valuation-comps endpoint |
| app/sources/pe/demo_seeder.py | Modify | Add valuation seed data |
| tests/test_spec_006_pe_valuation_comps.py | Create | Unit tests |
