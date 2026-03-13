# SPEC 026 — PE Portfolio Analytics

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_026_pe_portfolio_analytics.py

## Goal

Build firm-level portfolio analytics: performance aggregation across funds, vintage cohort analysis, sector concentration/HHI risk scoring, composite risk dashboard, and PME comparison vs public markets. Institutional-grade analytics layer.

## Acceptance Criteria

- [ ] Firm-wide performance aggregation (blended IRR, MOIC, TVPI, DPI, RVPI weighted by committed capital)
- [ ] Vintage cohort analysis with per-vintage metrics and best/worst fund identification
- [ ] Sector concentration with HHI score and risk classification
- [ ] Composite risk dashboard (sector, geographic, vintage, exit readiness, management gaps)
- [ ] PME calculation vs S&P 500 and Russell 2000 (hardcoded benchmarks)
- [ ] Benchmark comparison table (firm vs Cambridge Associates median, public indices)
- [ ] 6 API endpoints under /pe/analytics/{firm_id}/
- [ ] All pure computation — no LLM calls

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_firm_performance_aggregation | Blended IRR/MOIC across multiple funds |
| T2 | test_vintage_cohort_grouping | Funds grouped by vintage_year correctly |
| T3 | test_hhi_calculation | HHI from sector shares matches formula |
| T4 | test_hhi_classification | <1500 diversified, 1500-2500 moderate, >2500 concentrated |
| T5 | test_pme_ratio | PME >1.0 means PE outperformed |
| T6 | test_risk_dashboard_structure | All risk dimensions present |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_portfolio_analytics.py | Create | Analytics service |
| app/api/v1/pe_benchmarks.py | Modify | Add 6 analytics endpoints |
| tests/test_spec_026_pe_portfolio_analytics.py | Create | Tests |
