# SPEC 018 — PE Deal Scorer

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_018_pe_deal_scorer.py

## Goal

Build a PE-specific deal scoring engine that evaluates acquisition targets on financial quality, market position, management strength, and growth trajectory. Produces a composite 0-100 score with weighted sub-scores, letter grades, and actionable rationale — enabling PE firms to prioritize their pipeline objectively.

## Acceptance Criteria

- [ ] Composite score (0-100) from 5 weighted dimensions
- [ ] Each dimension produces raw_score, weighted_score, grade, explanation
- [ ] Works with existing PE demo data (portfolio companies + financials + leadership + competitors)
- [ ] Handles missing data gracefully (scores what's available, notes data gaps)
- [ ] API endpoint `GET /pe/deal-score/{company_id}` returns full breakdown
- [ ] Pure computation functions are static and fully testable without DB

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_score_financial_quality_strong | High revenue growth + margins → high score |
| T2 | test_score_financial_quality_weak | Declining revenue + low margins → low score |
| T3 | test_score_market_position_leader | Leader with competitors → high score |
| T4 | test_score_management_completeness | Full C-suite → high score |
| T5 | test_score_growth_trajectory | Multi-year revenue CAGR → appropriate score |
| T6 | test_composite_score_weighted | Weights sum to 1.0 and composite is correct |
| T7 | test_missing_data_handling | No financials → still returns score with gaps noted |
| T8 | test_grade_assignment | Score thresholds map to correct letter grades |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Error handling follows the error hierarchy
- [ ] Logging with structured context
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

**Dimensions & Weights:**
1. Financial Quality (35%) — revenue growth, EBITDA margin, gross margin, revenue per employee
2. Market Position (20%) — competitor landscape, relative size, market position indicators
3. Management Quality (15%) — C-suite completeness, PE-appointed leaders, tenure
4. Growth Trajectory (20%) — multi-year revenue CAGR, employee growth, margin expansion
5. Deal Attractiveness (10%) — valuation multiples, industry fragmentation opportunity

**Interface:**
```python
def score_deal(db: Session, company_id: int) -> Optional[DealScoreResult]
```

**Pattern:** Follow `pe_exit_scoring.py` — dataclasses for results, static scoring functions, one public entry point.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_deal_scorer.py | Create | Deal scoring engine |
| app/api/v1/pe_benchmarks.py | Modify | Add GET /pe/deal-score/{company_id} endpoint |
| tests/test_spec_018_pe_deal_scorer.py | Create | Unit tests |

## Feedback History

_No corrections yet._
