# SPEC 025 — PE Deal Sourcing Service

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_025_pe_deal_sourcing.py

## Goal

Build an automated deal discovery engine that reads market signals, identifies high-momentum sectors, scores candidate companies via DealScorer, and auto-creates pipeline entries at "Screening" stage. Closes the gap between market detection and deal action.

## Acceptance Criteria

- [ ] `source_deals_from_signals(db, firm_id)` — reads signals, scores candidates, creates pipeline entries
- [ ] `source_deals_from_targets(db, firm_id)` — scores all portfolio-adjacent companies, creates pipeline entries for top targets
- [ ] `get_sourcing_history(db, firm_id, days)` — recent auto-sourced deals with stats
- [ ] Deduplication: skips companies already in pipeline (by company_id)
- [ ] Only creates pipeline entries for candidates scoring B+ or above (>= 70)
- [ ] Pipeline entries tagged with source: "market_scanner" or "acquisition_scorer"
- [ ] API endpoints: POST /pe/deal-sourcing/{firm_id}/run, GET /pe/deal-sourcing/{firm_id}/history, GET /pe/deal-sourcing/{firm_id}/candidates

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_source_from_signals_creates_deals | High-score candidates become pipeline entries |
| T2 | test_source_from_signals_skips_duplicates | Existing pipeline companies not re-added |
| T3 | test_source_from_signals_score_threshold | Only B+ candidates (>=70) get pipeline entries |
| T4 | test_get_sourcing_history | Returns correct stats for firm |
| T5 | test_source_from_targets | Scores companies and creates entries |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_deal_sourcing.py | Create | Deal sourcing service |
| app/api/v1/pe_benchmarks.py | Modify | Add 3 endpoints |
| tests/test_spec_025_pe_deal_sourcing.py | Create | Tests |

## Feedback History

_No corrections yet._
