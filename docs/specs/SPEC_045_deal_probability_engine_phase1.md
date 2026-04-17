# SPEC 045 — Deal Probability Engine: Phase 1 Foundation

**Status:** Draft
**Task type:** service
**Date:** 2026-04-14
**Test file:** tests/test_spec_045_deal_probability_engine_phase1.py

## Goal

Establish the foundation for the Deal Probability Engine (PLAN_059): database models, company universe builder, and signal taxonomy. Phase 1 produces no scoring output yet — it creates the data structures and mapping layer that Phase 2 will populate. This is the scaffold that converts the platform from historical data to predictive alpha.

## Acceptance Criteria

- [ ] 6 new SQLAlchemy models in `app/core/probability_models.py` (txn_prob_companies, txn_prob_signals, txn_prob_scores, txn_prob_outcomes, txn_prob_alerts, sector_signal_weights)
- [ ] All tables registered with Base.metadata, created at startup
- [ ] CompanyUniverseBuilder service populates the universe from ≥3 sources (pe_portfolio_companies, industrial_companies, form_d_filers)
- [ ] Universe builder is idempotent — calling twice does not duplicate companies
- [ ] Universe builder dedupes by (normalized_name, sector) before insert
- [ ] Signal taxonomy defines exactly 12 signal types with weights summing to 1.0
- [ ] Sector weight overrides defined for Healthcare, Technology, Industrial (with sector total still summing to 1.0)
- [ ] Signal taxonomy maps each signal to either an existing scorer class or a new computer (not yet implemented — Phase 2)
- [ ] All DB queries parameterized (no raw SQL concatenation)
- [ ] No modifications to existing scorers — composition only
- [ ] All tests pass

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_all_six_tables_registered | All 6 tables exist in Base.metadata |
| T2 | test_txn_prob_company_unique_constraint | Duplicate (name, sector) inserts fail |
| T3 | test_txn_prob_signal_time_series | Multiple rows per (company, signal_type) at different timestamps allowed |
| T4 | test_txn_prob_score_probability_range | probability column accepts 0.0-1.0 values |
| T5 | test_universe_builder_dedup | Calling build_universe() twice results in same row count |
| T6 | test_universe_builder_sources | Companies populate from all 3 sources when data exists |
| T7 | test_universe_builder_empty_sources | No rows inserted when source tables empty |
| T8 | test_signal_taxonomy_has_twelve_signals | Exactly 12 signals defined |
| T9 | test_signal_taxonomy_weights_sum_to_one | Default weights sum to 1.0 (within floating-point tolerance) |
| T10 | test_sector_overrides_sum_to_one | Each sector override set sums to 1.0 |
| T11 | test_signal_taxonomy_keys | All 12 expected signal keys present |
| T12 | test_get_weights_for_sector | get_weights_for_sector returns overrides when defined, defaults otherwise |

## Rubric Checklist

- [ ] Service follows existing scorer pattern (ConvergenceEngine, FundConvictionScorer)
- [ ] Models use Base from app.core.models
- [ ] All DB queries use parameterized SQLAlchemy (no raw SQL concatenation)
- [ ] Scores always clamped to [0, 100] (future Phase 2)
- [ ] Graceful degradation when source tables are empty
- [ ] No hardcoded API keys in source code
- [ ] Tables indexed on query hot paths (company_id, signal_type, scored_at)
- [ ] Unique constraints where needed (company universe dedup)

## Design Notes

### Tables

**txn_prob_companies** — Universe of scored companies
- `id`, `company_name`, `canonical_company_id` (FK → pe_portfolio_companies.id, nullable), `sector`, `naics_code`, `hq_state`, `employee_count_est`, `revenue_est_usd`, `founded_year`, `ownership_status`, `universe_source` (pe_portfolio | industrial | form_d | manual), `is_active`, `created_at`, `updated_at`
- Unique: `(lower(company_name), sector)` for dedup
- Index: `(sector)`, `(is_active)`

**txn_prob_signals** — Time-series signal snapshots
- `id`, `company_id` (FK), `signal_type`, `score` (float 0-100), `previous_score`, `velocity`, `acceleration`, `signal_details` (JSON), `data_sources` (JSON), `confidence` (0-1), `scored_at`, `batch_id`
- Unique: `(company_id, signal_type, scored_at)`
- Index: `(company_id, signal_type)`, `(signal_type, score DESC)`, `(batch_id)`

**txn_prob_scores** — Composite probabilities per run
- `id`, `company_id` (FK), `probability` (0-1), `raw_composite_score` (0-100), `grade` (A-F), `confidence`, `sector_weights_version`, `signal_count`, `active_signal_count`, `convergence_factor`, `top_signals` (JSON), `signal_chain` (JSON), `narrative_summary` (text), `model_version`, `scored_at`, `batch_id`
- Index: `(probability DESC)`, `(company_id, scored_at DESC)`, `(grade)`

**txn_prob_outcomes** — Ground truth labels
- `id`, `company_id` (FK), `outcome_type` (acquired | ipo | secondary_sale | recap | spac_merger | no_transaction), `announced_date`, `closed_date`, `deal_value_usd`, `buyer_name`, `deal_source`, `pe_deal_id` (FK, nullable), `prediction_at_announcement`, `prediction_6mo_prior`, `prediction_12mo_prior`, `created_at`
- Index: `(company_id)`, `(outcome_type)`, `(announced_date)`

**txn_prob_alerts** — Threshold-crossing alerts
- `id`, `company_id` (FK), `alert_type` (probability_spike | grade_change | new_convergence | signal_acceleration | new_universe_entry), `severity` (high | medium | low), `title`, `description`, `probability_before`, `probability_after`, `triggering_signals` (JSON), `is_read`, `created_at`
- Index: `(is_read, created_at DESC)`, `(company_id)`

**sector_signal_weights** — Sector-specific weight overrides
- `id`, `sector`, `signal_type`, `weight`, `rationale`, `effective_date`, `version`, `created_at`
- Unique: `(sector, signal_type, version)`

### Service Interfaces

```python
class CompanyUniverseBuilder:
    def __init__(self, db: Session)
    def build_universe() -> Dict[str, int]  # {inserted, updated, skipped, total}
    def refresh_universe() -> Dict[str, int]
    def _normalize_name(name: str) -> str
    def _load_from_pe_portfolio() -> List[Dict]
    def _load_from_industrial() -> List[Dict]
    def _load_from_form_d() -> List[Dict]
```

### Signal Taxonomy

Pure-data module. `SIGNAL_TAXONOMY: Dict[str, SignalDefinition]` with 12 entries. Helper function:

```python
def get_weights_for_sector(sector: str) -> Dict[str, float]:
    """Returns signal_type → weight mapping, sector-specific if override exists."""
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/probability_models.py | Create | 6 SQLAlchemy models |
| app/services/probability_universe.py | Create | CompanyUniverseBuilder |
| app/ml/probability_signal_taxonomy.py | Create | 12-signal taxonomy + sector weights |
| app/main.py | Modify | Import probability_models to register tables |
| tests/test_spec_045_deal_probability_engine_phase1.py | Create | Unit tests (12 cases) |

## Feedback History

_No corrections yet._
