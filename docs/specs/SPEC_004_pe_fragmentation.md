# SPEC 004 — PE Industry Fragmentation Scorer

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_004_pe_fragmentation.py

## Goal

Build a fragmentation scoring service that leverages Census County Business Patterns (CBP) data to help PE firms identify fragmented industries ripe for roll-up acquisitions. The service scores industries 0-100 based on HHI, small business concentration, and establishment size distribution, and ranks geographic markets by fragmentation.

## Acceptance Criteria

- [ ] `FragmentationScorer` class in `app/core/pe_fragmentation.py` computes fragmentation scores 0-100
- [ ] Scorer uses existing `CBPCollector` for data fetch/cache — no duplicate Census API logic
- [ ] `GET /pe/fragmentation/{naics_code}` returns national score + top metro/county markets
- [ ] `GET /pe/fragmentation/scan` accepts multiple NAICS codes, returns ranked list
- [ ] `GET /pe/roll-up-targets/{naics_code}/{state}` finds companies in fragmented markets
- [ ] All DB queries use parameterized SQL
- [ ] Tests cover happy path, empty data, edge cases (single county, zero establishments)
- [ ] Scoring formula documented in code comments

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_compute_fragmentation_score_high | High fragmentation (many small firms) → score > 70 |
| T2 | test_compute_fragmentation_score_low | Low fragmentation (few large firms) → score < 30 |
| T3 | test_compute_fragmentation_score_empty | No data → returns None/0 gracefully |
| T4 | test_rank_markets_by_fragmentation | Counties ranked descending by fragmentation score |
| T5 | test_national_aggregation | National score aggregates county data correctly |
| T6 | test_scan_multiple_naics | Scan endpoint ranks multiple NAICS codes |
| T7 | test_single_county_edge_case | Single county still produces valid score |
| T8 | test_fragmentation_endpoint_returns_valid_response | API endpoint returns correct schema |
| T9 | test_scan_endpoint_empty_naics_list | Empty input returns 422 or empty list |
| T10 | test_roll_up_targets_filters_by_state | Roll-up targets filtered to correct state |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows (N/A — read-only scoring)
- [ ] Error handling follows the error hierarchy (`RetryableError`, `FatalError`, etc.)
- [ ] Logging with structured context (source, operation, record counts)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Scoring Formula
- **HHI component (40%):** Lower HHI → higher fragmentation score. HHI < 0.15 (unconcentrated) maps to 80-100; HHI 0.15-0.25 maps to 50-80; HHI > 0.25 maps to 0-50.
- **Small biz % component (35%):** Higher % of establishments with <50 employees → higher score
- **Avg estab size component (25%):** Smaller average establishment → higher score

### Key Functions
```python
class FragmentationScorer:
    def __init__(self, db: Session)
    async def score_industry(naics_code, year=2021, state=None) -> FragmentationResult
    async def scan_industries(naics_codes, year=2021) -> List[IndustryScanResult]
    def rank_markets(naics_code, year=2021, top_n=20) -> List[MarketRanking]
    def _compute_score(hhi, small_biz_pct, avg_size) -> float
```

### Data Flow
1. API endpoint receives NAICS code
2. `FragmentationScorer` calls `CBPCollector.collect()` to fetch/cache CBP data
3. Reads cached data via `CBPCollector.get_cached()`
4. Computes fragmentation scores from cached county-level data
5. Aggregates, ranks, and returns results

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_fragmentation.py | Create | FragmentationScorer service class |
| app/api/v1/pe_benchmarks.py | Modify | Add 3 fragmentation endpoints |
| tests/test_spec_004_pe_fragmentation.py | Create | Unit tests with mocked CBP data |

## Feedback History

_No corrections yet._
