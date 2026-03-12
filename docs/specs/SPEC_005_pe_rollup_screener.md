# SPEC 005 — PE Roll-Up Market Screener

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_005_pe_rollup_screener.py

## Goal

Build a roll-up market screener that combines fragmentation scores with company discovery. PE firms use this to find actual acquisition targets in fragmented industries — filtering by size, geography, ownership status, and growth signals.

## Acceptance Criteria

- [ ] `RollUpScreener` class in `app/core/pe_rollup_screener.py` queries companies by NAICS/geography
- [ ] Searches both `pe_portfolio_companies` and `industrial_companies` tables
- [ ] Enriches targets with financials from `pe_company_financials`
- [ ] Scores each target: size fit, geography match, ownership, growth signals
- [ ] `GET /pe/rollup-screener/{naics_code}` returns filtered ranked target list
- [ ] `GET /pe/rollup-screener/{naics_code}/summary` returns market overview
- [ ] 15-20 independent target companies seeded in demo_seeder.py
- [ ] All DB queries use parameterized SQL (SQLAlchemy ORM)
- [ ] Tests cover scoring, filtering, empty results, and endpoint responses

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_score_target_ideal_size | $5-50M revenue independent company scores high |
| T2 | test_score_target_pe_backed_penalty | PE-backed company scores lower |
| T3 | test_score_target_too_large | >$100M company scores low on size fit |
| T4 | test_filter_by_state | Only companies in specified state returned |
| T5 | test_filter_exclude_pe_backed | PE-backed companies excluded when flag set |
| T6 | test_empty_results | NAICS with no companies returns empty list |
| T7 | test_summary_aggregation | Market summary counts and averages correct |
| T8 | test_screener_endpoint_response | API returns correct schema |
| T9 | test_revenue_range_filter | Min/max revenue filters work |
| T10 | test_ranking_order | Targets ranked by composite score descending |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows (N/A — read-only)
- [ ] Error handling follows the error hierarchy
- [ ] Logging with structured context
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Target Scoring (0-100)
- **Size fit (35%):** Sweet spot $5-50M revenue → 100; <$2M or >$100M → 0
- **Ownership (25%):** Independent/Private → 100; VC-backed → 50; PE-backed → 20
- **Geography match (20%):** In target state → 100; adjacent → 50; other → 25
- **Growth signals (20%):** Revenue growth >10% → high; employee growth → medium

### Data Sources
- `pe_portfolio_companies` — PE-tracked companies (have naics_code, ownership_status)
- `industrial_companies` — broader company universe
- `pe_company_financials` — revenue, growth metrics

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_rollup_screener.py | Create | RollUpScreener service |
| app/api/v1/pe_benchmarks.py | Modify | Add 2 screener endpoints |
| app/sources/pe/demo_seeder.py | Modify | Add 15-20 independent target companies |
| tests/test_spec_005_pe_rollup_screener.py | Create | Unit tests |

## Feedback History

_No corrections yet._
