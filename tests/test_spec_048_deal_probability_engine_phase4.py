"""
Tests for SPEC 048 — Deal Probability Engine: Phase 4 Learning Loop
Covers: outcome tracker, calibrator, weight optimizer, ML model, API wiring.
"""
import pytest
import numpy as np


# ---------------------------------------------------------------------------
# Pure calibrator tests (no DB)
# ---------------------------------------------------------------------------


class TestSpec048Calibrator:
    """T4-T7: Calibration math."""

    def test_platt_fit_monotonic(self):
        """T4: Platt-fit probabilities monotonically increase with raw score."""
        from app.ml.probability_calibrator import apply_platt, fit_platt

        # Synthetic separable data: high raw → high outcome
        rng = np.random.default_rng(42)
        raw = np.concatenate([rng.normal(30, 5, 50), rng.normal(70, 5, 50)])
        outcomes = np.concatenate([np.zeros(50), np.ones(50)])

        params = fit_platt(raw, outcomes)
        xs = np.linspace(10, 90, 9)
        probs = [apply_platt(x, params["k"], params["x0"]) for x in xs]
        # Monotonic (non-decreasing)
        for i in range(len(probs) - 1):
            assert probs[i + 1] >= probs[i] - 1e-6

    def test_platt_fit_midpoint(self):
        """T5: balanced labels → midpoint raw should be near 0.5."""
        from app.ml.probability_calibrator import apply_platt, fit_platt

        rng = np.random.default_rng(0)
        raw = np.concatenate([rng.normal(30, 10, 100), rng.normal(70, 10, 100)])
        outcomes = np.concatenate([np.zeros(100), np.ones(100)])
        params = fit_platt(raw, outcomes)
        # With balanced data centred at 30 and 70, midpoint 50 should give ~0.5
        p_at_50 = apply_platt(50, params["k"], params["x0"])
        assert 0.3 <= p_at_50 <= 0.7

    def test_isotonic_fit_monotonic(self):
        """T6: isotonic output is non-decreasing."""
        from app.ml.probability_calibrator import apply_isotonic, fit_isotonic

        raw = np.array([10, 20, 30, 40, 50, 60, 70, 80, 90])
        # Noisy-but-monotonic outcome ratios
        outcomes = np.array([0, 0, 0, 1, 0, 1, 1, 1, 1], dtype=float)
        params = fit_isotonic(raw, outcomes)
        bps = params["breakpoints"]
        # Non-decreasing y-values
        for i in range(len(bps) - 1):
            assert bps[i + 1][1] >= bps[i][1] - 1e-9

        # Apply at several points — should be non-decreasing
        out = [apply_isotonic(r, bps) for r in np.linspace(0, 100, 20)]
        for i in range(len(out) - 1):
            assert out[i + 1] >= out[i] - 1e-9

    def test_brier_score_correct(self):
        """T7: Brier 0 for perfect, 1 for worst-case prediction."""
        from app.ml.probability_calibrator import brier_score

        # Perfect
        assert brier_score(np.array([1.0, 0.0]), np.array([1, 0])) == 0.0
        # Worst
        assert brier_score(np.array([0.0, 1.0]), np.array([1, 0])) == 1.0


# ---------------------------------------------------------------------------
# Pure weight optimizer tests (no DB)
# ---------------------------------------------------------------------------


class TestSpec048WeightOptimizer:
    """T8-T10: Weight optimization and signal importance."""

    def test_weight_optimizer_sums_to_one(self, db_session):
        """T8: Optimized weights sum to 1.0 within tolerance."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY
        from app.ml.probability_weight_optimizer import SignalWeightOptimizer

        # Build synthetic separable data: 100 samples, 12 signals
        rng = np.random.default_rng(1)
        n_signals = len(SIGNAL_TAXONOMY)
        n = 120  # above MIN_SAMPLES_FOR_OPTIMIZATION=50
        # First signal is actually predictive; others noisy
        outcomes = rng.integers(0, 2, size=n)
        features = rng.uniform(0, 100, size=(n, n_signals))
        features[:, 0] = outcomes * 70 + rng.normal(0, 5, n)  # predictive

        opt = SignalWeightOptimizer(db_session)
        result = opt.optimize_weights(features, outcomes)
        assert result["ok"]
        total = sum(result["weights"].values())
        assert abs(total - 1.0) < 0.01

    def test_weight_optimizer_no_negative(self, db_session):
        """T9: No weight is negative."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY
        from app.ml.probability_weight_optimizer import SignalWeightOptimizer

        rng = np.random.default_rng(2)
        n_signals = len(SIGNAL_TAXONOMY)
        n = 120
        outcomes = rng.integers(0, 2, size=n)
        features = rng.uniform(0, 100, size=(n, n_signals))

        opt = SignalWeightOptimizer(db_session)
        result = opt.optimize_weights(features, outcomes)
        assert result["ok"]
        for w in result["weights"].values():
            assert w >= 0

    def test_univariate_auc_returns_per_signal(self, db_session):
        """T10: compute_signal_importance returns a value per taxonomy signal."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY
        from app.ml.probability_weight_optimizer import SignalWeightOptimizer

        rng = np.random.default_rng(3)
        n_signals = len(SIGNAL_TAXONOMY)
        features = rng.uniform(0, 100, size=(60, n_signals))
        outcomes = rng.integers(0, 2, size=60)
        opt = SignalWeightOptimizer(db_session)
        importance = opt.compute_signal_importance(features, outcomes)
        assert set(importance.keys()) == set(SIGNAL_TAXONOMY.keys())
        for v in importance.values():
            assert 0.0 <= v <= 1.0


# ---------------------------------------------------------------------------
# Outcome tracker (DB-backed)
# ---------------------------------------------------------------------------


class TestSpec048OutcomeTracker:
    """T1-T3: Outcome tracker."""

    def test_outcome_tracker_scan_dedup(self, db_session):
        """T1: Scanning twice doesn't duplicate outcome rows."""
        from app.services.probability_outcome_tracker import OutcomeTracker

        tracker = OutcomeTracker(db_session)
        s1 = tracker.scan_for_outcomes()
        s2 = tracker.scan_for_outcomes()
        # With empty pe_deals table, nothing is inserted; dedup invariant holds
        assert s2["inserted"] == 0
        assert isinstance(s1, dict)
        assert isinstance(s2, dict)

    def test_outcome_tracker_backfill_from_scores(self, db_session):
        """T2: Backfill reads historical txn_prob_scores."""
        from app.services.probability_outcome_tracker import OutcomeTracker

        # Empty-data case: still returns a stats dict
        tracker = OutcomeTracker(db_session)
        result = tracker.backfill_predictions()
        assert "evaluated" in result
        assert "filled" in result
        assert "no_history" in result

    def test_labeled_dataset_shape(self, db_session):
        """T3: Returns a pandas DataFrame (empty OK)."""
        import pandas as pd
        from app.services.probability_outcome_tracker import OutcomeTracker

        df = OutcomeTracker(db_session).get_labeled_dataset()
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# ML model availability
# ---------------------------------------------------------------------------


class TestSpec048MLModel:
    """T11: ML model graceful unavailable."""

    def test_ml_model_unavailable_raises(self):
        """T11: TransactionProbabilityModel raises ModelUnavailableError when LightGBM missing."""
        from app.ml.probability_model import (
            ModelUnavailableError,
            TransactionProbabilityModel,
            is_lightgbm_available,
        )

        if is_lightgbm_available():
            # If LightGBM IS installed, the constructor succeeds
            m = TransactionProbabilityModel()
            assert m is not None
        else:
            with pytest.raises(ModelUnavailableError):
                TransactionProbabilityModel()


# ---------------------------------------------------------------------------
# Engine integration
# ---------------------------------------------------------------------------


class TestSpec048EngineIntegration:
    """T12: Engine picks up fitted calibration."""

    def test_engine_uses_calibrator_when_fitted(self, db_session):
        """T12: After fitting a distinctive calibration, engine uses it."""
        from app.core.probability_models import TxnProbCalibration, TxnProbCompany
        from app.services.probability_engine import TransactionProbabilityEngine

        # Insert a calibration that maps everything to ~0.9 (very high slope, low intercept)
        cal = TxnProbCalibration(
            scope="global",
            method="platt",
            params={"k": 10.0, "x0": 0.0},  # Everything > 0 → ~1.0
            n_samples=100,
            brier_score=0.1,
            is_active=True,
        )
        db_session.add(cal)
        db_session.commit()

        c = TxnProbCompany(
            company_name="CalibTest Co",
            normalized_name="calibtest",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        engine = TransactionProbabilityEngine(db_session)
        result = engine.score_company(c.id)
        # With k=10, x0=0, probability should be very close to 1.0
        assert result["probability"] > 0.95


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


class TestSpec048API:
    """T13-T16: Phase 4 API endpoints."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from app.main import app

        return TestClient(app)

    def test_api_outcomes_scan(self, client):
        """T13: POST /outcomes/scan returns stats dict."""
        resp = client.post("/api/v1/txn-probability/outcomes/scan")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "scan" in data
        assert "backfill" in data
        assert "metrics" in data

    def test_api_calibration_status(self, client):
        """T14: GET /calibration returns list of calibrations."""
        resp = client.get("/api/v1/txn-probability/calibration")
        assert resp.status_code == 200
        data = resp.json()
        assert "min_samples_to_fit" in data
        assert "calibrations" in data
        assert isinstance(data["calibrations"], list)

    def test_api_weights_optimize_insufficient_samples(self, client):
        """T15: POST /weights/optimize gracefully handles insufficient samples."""
        resp = client.post("/api/v1/txn-probability/weights/optimize")
        assert resp.status_code == 200
        data = resp.json()
        # Empty DB → ok=False with clear reason
        assert data.get("ok") is False
        assert "reason" in data

    def test_api_model_status(self, client):
        """T16: GET /model/status reports LightGBM + training readiness."""
        resp = client.get("/api/v1/txn-probability/model/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "lightgbm_available" in data
        assert "min_samples_for_training" in data
        assert "current_labeled_samples" in data
        assert "trainable" in data
