# SPEC 009 — PE Market Scanner & Intelligence Brief

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_009_pe_market_scanner.py

## Goal

Build a market scanner service that aggregates deal flow, valuation trends, and competitive dynamics across sectors to produce intelligence briefs for PE deal teams. Supports sector-level screening for momentum signals and market timing.

## Acceptance Criteria

- [ ] Service aggregates deal activity, multiples, and volume by sector
- [ ] Sector overview returns deal count, median multiples, trend, top buyers
- [ ] Intelligence brief generates narrative summary with key findings
- [ ] Market signals endpoint returns momentum indicators per sector
- [ ] `GET /pe/market-scanner/sector/{industry}` returns sector overview
- [ ] `GET /pe/market-scanner/intelligence-brief/{industry}` returns brief
- [ ] `GET /pe/market-scanner/signals` returns cross-sector signals
- [ ] Tests cover all three endpoints plus edge cases

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_sector_overview_with_deals | Returns deal stats, median, trend |
| T2 | test_sector_overview_no_deals | Empty industry returns zero counts |
| T3 | test_intelligence_brief_structure | Brief has sections, findings, recommendations |
| T4 | test_market_signals_all_sectors | Returns signals for each active sector |
| T5 | test_market_signals_momentum | Momentum computed from deal flow changes |
| T6 | test_top_buyers_ranking | Top buyers sorted by deal count |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Logging with structured context
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_market_scanner.py | Create | Market scanner service |
| app/api/v1/pe_benchmarks.py | Modify | Add 3 endpoints |
| tests/test_spec_009_pe_market_scanner.py | Create | Tests |
