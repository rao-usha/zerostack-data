# SPEC 048 — Deal Probability Engine: Phase 4 Learning Loop

**Status:** Draft
**Task type:** service
**Date:** 2026-04-14
**Test file:** tests/test_spec_048_deal_probability_engine_phase4.py

## Goal

Close the learning loop. When transactions actually happen, use them as labeled training data to improve the probability engine. Four components:

1. **Outcome tracker** — scan `pe_deals` (+ SEC filings, news) for real transactions on universe companies and backfill the predictions we made 6 and 12 months prior
2. **Calibrator** — fit Platt scaling and isotonic regression so raw composites map to realistic probabilities
3. **Weight optimizer** — find per-signal weights that maximize AUC-ROC; run walk-forward backtests
4. **ML model (gated)** — LightGBM gradient-boosted model activated when ≥200 labeled samples exist; SHAP for explainability; auto-fallback to rule-based when fewer samples or LightGBM unavailable

Integration: the engine's `_calibrate_to_probability` prefers a fitted Platt calibration when available, falling back to the default sigmoid.

## Acceptance Criteria

- [ ] `OutcomeTracker.scan_for_outcomes()` populates `txn_prob_outcomes` from pe_deals; deduplicated by (company_id, announced_date, outcome_type)
- [ ] `OutcomeTracker.backfill_predictions()` writes prediction_at_announcement / 6mo / 12mo values from historical `txn_prob_scores`
- [ ] `OutcomeTracker.get_labeled_dataset()` returns pandas DataFrame with 12 signal columns + binary `outcome_within_12mo`
- [ ] `ProbabilityCalibrator.fit_platt(raw, outcomes)` returns `{k, x0}` from scipy-based logistic fit
- [ ] `ProbabilityCalibrator.fit_isotonic(raw, outcomes)` returns a monotonic step-function calibrator (PAV algorithm)
- [ ] `ProbabilityCalibrator.brier_score(predictions, outcomes)` returns float
- [ ] `SignalWeightOptimizer.optimize_weights()` returns weights summing to 1.0 via scipy.optimize
- [ ] `SignalWeightOptimizer.compute_signal_importance()` returns per-signal univariate AUC
- [ ] `SignalWeightOptimizer.run_backtest()` returns precision/recall over time
- [ ] `TransactionProbabilityModel.train()` fits LightGBM when available, raises `ModelUnavailableError` otherwise
- [ ] Engine integration: `_calibrate_to_probability` checks for fitted calibration before falling back to default sigmoid
- [ ] 4 new API endpoints: POST /outcomes/scan, GET /calibration, POST /weights/optimize, GET /model/status

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_outcome_tracker_scan_dedup | Scanning twice doesn't duplicate outcome rows |
| T2 | test_outcome_tracker_backfill_from_scores | Looks up historical txn_prob_scores snapshots |
| T3 | test_labeled_dataset_shape | Returns 12-column feature matrix + binary label |
| T4 | test_platt_fit_monotonic | Platt-fit probabilities monotonically increase in raw |
| T5 | test_platt_fit_midpoint | With balanced data, midpoint raw → P≈0.5 |
| T6 | test_isotonic_fit_monotonic | Isotonic output is non-decreasing |
| T7 | test_brier_score_correct | Perfect prediction → 0; worst prediction → 1 |
| T8 | test_weight_optimizer_sums_to_one | Optimized weights sum to 1.0 |
| T9 | test_weight_optimizer_no_negative | No weight < 0 |
| T10 | test_univariate_auc_returns_per_signal | 12 signal importance values |
| T11 | test_ml_model_unavailable_raises | TransactionProbabilityModel raises when LightGBM missing |
| T12 | test_engine_uses_calibrator_when_fitted | Engine picks up fitted calibration over default sigmoid |
| T13 | test_api_outcomes_scan | POST /outcomes/scan returns count |
| T14 | test_api_calibration_status | GET /calibration returns current calibration params |
| T15 | test_api_weights_optimize | POST /weights/optimize requires min sample count |
| T16 | test_api_model_status | GET /model/status reports enabled/disabled |

## Rubric Checklist

- [ ] No new required dependencies — sklearn/lightgbm are optional
- [ ] Graceful degradation when insufficient labeled data (returns current baseline)
- [ ] Calibration persisted in `sector_signal_weights`-style table or JSON blob (decide)
- [ ] Weight optimization respects `sum(weights)=1` constraint
- [ ] Backtest uses walk-forward, not random split
- [ ] Engine modifications are add-only (no Phase 1/2/3 regressions)

## Design Notes

### Calibration persistence

New table `txn_prob_calibrations` (or reuse JSON in `sector_signal_weights`). Columns: id, scope (`global` | sector), method (`sigmoid` | `platt` | `isotonic`), params (JSON), n_samples, brier_score, fitted_at, is_active.

### Platt scaling via scipy

```python
from scipy.optimize import minimize
def fit_platt(raw_scores, outcomes):
    def neg_log_lik(params, x, y):
        k, x0 = params
        p = 1 / (1 + np.exp(-k * (x - x0)))
        p = np.clip(p, 1e-9, 1 - 1e-9)
        return -np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))
    result = minimize(neg_log_lik, x0=[0.08, 55], args=(raw, outcomes), method='Nelder-Mead')
    return {"k": result.x[0], "x0": result.x[1]}
```

### Isotonic via PAV

```python
def pool_adjacent_violators(x, y):
    """Return sorted (x, fitted_y) pairs — non-decreasing."""
    # Standard PAV algorithm; ~30 lines.
```

### Weight optimizer

```python
def optimize_weights(feature_matrix, outcomes):
    def neg_auc(weights):
        weights = np.abs(weights)
        weights = weights / weights.sum()
        scores = feature_matrix @ weights
        return -compute_auc(scores, outcomes)
    init = np.ones(12) / 12
    result = minimize(neg_auc, init, method='Nelder-Mead')
    w = np.abs(result.x)
    return w / w.sum()
```

### Engine integration

```python
def _calibrate_to_probability(self, raw, sector):
    calibration = get_active_calibration(self.db, sector=sector)
    if calibration:
        if calibration.method == 'platt':
            return self._calibrate_sigmoid(raw, k=params['k'], x0=params['x0'])
        if calibration.method == 'isotonic':
            return apply_isotonic(raw, params['breakpoints'])
    return self._calibrate_sigmoid(raw)  # default
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/services/probability_outcome_tracker.py | Create | Outcome scan + backfill + labeled dataset |
| app/ml/probability_calibrator.py | Create | Platt + isotonic + Brier |
| app/ml/probability_weight_optimizer.py | Create | scipy-based AUC weight optimization |
| app/ml/probability_model.py | Create | LightGBM gated ML model |
| app/core/probability_models.py | Modify | Add TxnProbCalibration table |
| app/services/probability_engine.py | Modify | Use fitted calibration when available |
| app/api/v1/transaction_probability.py | Modify | 4 new endpoints |
| tests/test_spec_048_deal_probability_engine_phase4.py | Create | 16 tests |

## Feedback History

_No corrections yet._
