# SPEC 028 — Deal Radar: Convergence Intelligence

**Status:** Draft
**Task type:** service
**Date:** 2026-03-30
**Test file:** tests/test_spec_028_deal_radar_convergence.py

## Goal

Build a convergence intelligence engine that queries 5 existing public data sources (EPA ECHO, IRS SOI migration, US Trade exports, Public Water Systems, IRS SOI income), computes per-region signal scores (0-100), and produces a composite convergence score that identifies geographic investment opportunities. Expose via REST API and wire to an interactive map frontend.

## Acceptance Criteria

- [ ] 3 new SQLAlchemy models: ConvergenceRegion, ConvergenceSignal, ConvergenceCluster
- [ ] 13 US macro-regions defined with state mappings (all 50 states + DC covered)
- [ ] 5 signal scorers each return 0-100 from real DB data
- [ ] Convergence formula: avg * (1 + 0.1 * signals_above_60) matches mockup logic
- [ ] Cluster classification: HOT >= 72, ACTIVE >= 58, WATCH >= 44, LOW < 44
- [ ] 7 API endpoints under /api/v1/deal-radar/
- [ ] Thesis generation via Claude API with caching
- [ ] Frontend wired to real API (replaces fake data)
- [ ] All tests pass

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_region_state_mapping_complete | All 50 states + DC mapped to exactly one region |
| T2 | test_convergence_formula_basic | Known inputs produce expected composite score |
| T3 | test_convergence_formula_no_signals_above_60 | No bonus when all signals < 60 |
| T4 | test_convergence_formula_all_signals_above_60 | Max bonus (1.5x) when all 5 above 60 |
| T5 | test_cluster_classification_hot | Score 72+ classified as HOT |
| T6 | test_cluster_classification_active | Score 58-71 classified as ACTIVE |
| T7 | test_cluster_classification_watch | Score 44-57 classified as WATCH |
| T8 | test_cluster_classification_low | Score < 44 classified as LOW |
| T9 | test_scorer_returns_0_100_range | Each scorer clamps output to [0, 100] |
| T10 | test_scorer_empty_data | Scorers return 0 when no data exists for region |
| T11 | test_grade_assignment | Scores map to correct letter grades |
| T12 | test_active_signals_list | Signals >= 60 appear in active_signals list |

## Rubric Checklist

- [ ] Service follows existing scorer pattern (FundConvictionScorer)
- [ ] Models use Base from app.core.models
- [ ] All DB queries use parameterized SQLAlchemy (no raw SQL concatenation)
- [ ] API endpoints return consistent JSON shapes
- [ ] Scores always clamped to [0, 100]
- [ ] Graceful degradation when source tables are empty
- [ ] Router registered in main.py with OpenAPI tag
- [ ] No hardcoded API keys in source code

## Design Notes

- **ConvergenceEngine(db: Session)** — main service class
  - `scan_all_regions() -> list[RegionResult]` — full scan
  - `score_region(region_id) -> RegionResult` — single region
  - `generate_thesis(region_id) -> ThesisResult` — AI thesis
- **5 private scorer methods**: `_score_epa`, `_score_irs_migration`, `_score_trade`, `_score_water`, `_score_macro`
- Each scorer queries its source table filtered by region states, computes a normalized 0-100 score
- Convergence formula from mockup: `round(avg * (1 + above_60_count * 0.1))`

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/convergence_models.py | Create | 3 SQLAlchemy models |
| app/services/convergence_engine.py | Create | Scoring engine + thesis generation |
| app/api/v1/deal_radar.py | Create | 7 REST endpoints |
| frontend/deal-radar.html | Create | Wired frontend from mockup |
| app/main.py | Modify | Register router + OpenAPI tag |
| tests/test_spec_028_deal_radar_convergence.py | Create | Unit tests |

## Feedback History

_No corrections yet._
