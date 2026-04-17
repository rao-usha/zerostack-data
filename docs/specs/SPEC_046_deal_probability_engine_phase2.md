# SPEC 046 — Deal Probability Engine: Phase 2 Signal Engine

**Status:** Draft
**Task type:** service
**Date:** 2026-04-14
**Test file:** tests/test_spec_046_deal_probability_engine_phase2.py

## Goal

Build the scoring pipeline that computes all 12 probability signals per company, composes them into a calibrated P(transaction in 6-12 months), and exposes the full signal chain via REST API. Phase 2 turns the Phase 1 foundation into a working product — ranked company leaderboards with explainable scores.

## Acceptance Criteria

- [ ] 6 new signal computers in `app/services/probability_signal_computers.py` — each with `.compute(company) -> SignalResult` returning score/confidence/details
- [ ] `TransactionProbabilityEngine` in `app/services/probability_engine.py` with `score_company`, `score_universe`, `get_rankings`, `get_company_detail`, `get_signal_history`
- [ ] Composite formula: `min(100, weighted_sum * (1 + above_60_count * 0.08))`
- [ ] Sigmoid probability calibration: `1 / (1 + exp(-k * (raw - x0)))`, default k=0.08 x0=55
- [ ] Velocity and acceleration computed from previous signal snapshot
- [ ] Signal chain JSON decomposes composite → per-signal contributions (explainable)
- [ ] 10 REST endpoints under `/api/v1/txn-probability/`
- [ ] Existing scorers reused via composition — zero modifications to CompanyScorer, ExitReadinessScorer, AcquisitionTargetScorer, ExecSignalScorer, CompanyDiligenceScorer
- [ ] Graceful degradation when an individual scorer fails (score=50, confidence=0)
- [ ] Router registered in `app/main.py` with OpenAPI tag

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_signal_result_dataclass | SignalResult has score, confidence, details, signal_type |
| T2 | test_composite_formula_no_convergence | Weighted sum, no above-60 signals → bonus=1.0 |
| T3 | test_composite_formula_with_convergence | 3 signals above 60 → bonus=1.24 multiplier |
| T4 | test_composite_clamped_to_100 | High-convergence never exceeds 100 |
| T5 | test_calibrate_sigmoid_range | Sigmoid output always between 0 and 1 |
| T6 | test_calibrate_sigmoid_midpoint | raw=55 returns P=0.5 (default inflection) |
| T7 | test_velocity_first_snapshot | First snapshot: velocity=0 (no previous) |
| T8 | test_velocity_computed | Subsequent snapshot: velocity = current - previous |
| T9 | test_score_company_persists_snapshots | score_company() writes txn_prob_signals + txn_prob_scores rows |
| T10 | test_signal_chain_decomposition | signal_chain JSON sums to composite score |
| T11 | test_grade_thresholds | Composite → grade mapping (A/B/C/D/F) |
| T12 | test_insider_activity_empty_data | InsiderActivityComputer returns score=50, confidence=0 when no data |
| T13 | test_founder_risk_computer | FounderRiskComputer uses founded_year + people data |
| T14 | test_macro_tailwind_computer | MacroTailwindComputer combines region convergence + sector momentum |
| T15 | test_api_score_endpoint | POST /score/{company_id} returns full signal chain |
| T16 | test_api_rankings_filters | GET /rankings with sector + min_probability filters correctly |

## Rubric Checklist

- [ ] All queries use parameterized SQLAlchemy
- [ ] Scores always clamped to [0, 100]
- [ ] Probabilities always in [0, 1]
- [ ] Graceful degradation when source tables empty
- [ ] No modifications to existing scorers
- [ ] Router registered in main.py with dependencies=_auth + OpenAPI tag
- [ ] Batch operations use per-company commits (not one big transaction)

## Design Notes

### SignalResult dataclass

```python
@dataclass
class SignalResult:
    signal_type: str
    score: float  # 0-100
    confidence: float  # 0-1
    details: Dict
    data_sources: List[str]
```

### TransactionProbabilityEngine interface

```python
class TransactionProbabilityEngine:
    def __init__(self, db: Session): ...

    def score_company(self, company_id: int, batch_id: str = None) -> Dict:
        """Score a single company. Returns {probability, raw, grade, signal_chain, ...}"""

    def score_universe(self, batch_size: int = 100) -> Dict:
        """Batch-score active companies."""

    def get_rankings(self, sector: str = None, min_probability: float = 0.0,
                     limit: int = 50, grade: str = None) -> List[Dict]: ...

    def get_company_detail(self, company_id: int) -> Dict: ...
    def get_signal_history(self, company_id: int, signal_type: str, periods: int = 12) -> List[Dict]: ...

    # Private
    def _compute_signals(self, company: TxnProbCompany) -> List[SignalResult]: ...
    def _compute_composite(self, signals: List[SignalResult], weights: Dict) -> Tuple[float, float]: ...
    def _calibrate_to_probability(self, raw: float, sector: str) -> float: ...
    def _compute_velocity_acceleration(self, company_id: int, signal_type: str, current: float) -> Tuple: ...
    def _grade_from_score(self, raw: float) -> str: ...
```

### Composite formula

```python
weighted_sum = sum(sig.score * weights[sig.signal_type] for sig in signals)
above_60_count = sum(1 for sig in signals if sig.score >= 60)
convergence_factor = 1 + (above_60_count * 0.08)
raw_composite = min(100, weighted_sum * convergence_factor)
```

### Sigmoid calibration (Phase 2 default)

```python
import math
# k=0.08, x0=55 — adjustable per sector in Phase 4
probability = 1 / (1 + math.exp(-k * (raw_composite - x0)))
```

### Grade thresholds

A: 85+, B: 70-84, C: 55-69, D: 40-54, F: <40

### API endpoints under `/api/v1/txn-probability/`

1. `POST /score/{company_id}` — score single company
2. `POST /scan` — batch-score universe
3. `GET /rankings` — top companies (query: sector, min_probability, limit, grade)
4. `GET /company/{id}` — full detail + latest signal chain
5. `GET /company/{id}/history` — time-series scores
6. `GET /company/{id}/signals` — per-signal latest + velocity
7. `GET /stats` — dashboard stats
8. `GET /sectors` — per-sector summary (avg probability, count, top)
9. `GET /alerts` — recent alerts (placeholder until Phase 3 alert engine)
10. `GET /methodology` — static methodology documentation

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/services/probability_signal_computers.py | Create | 6 new computers |
| app/services/probability_engine.py | Create | TransactionProbabilityEngine orchestrator |
| app/api/v1/transaction_probability.py | Create | 10 REST endpoints |
| app/ml/probability_signal_taxonomy.py | Modify | Fix exit_readiness score_field, diligence method name |
| app/main.py | Modify | Register router + OpenAPI tag |
| tests/test_spec_046_deal_probability_engine_phase2.py | Create | 16 unit tests |

## Feedback History

_No corrections yet._
