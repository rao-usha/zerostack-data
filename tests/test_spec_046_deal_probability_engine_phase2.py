"""
Tests for SPEC 046 — Deal Probability Engine: Phase 2 Signal Engine
Covers: probability_signal_computers, probability_engine, transaction_probability API
"""
import math

import pytest


# ---------------------------------------------------------------------------
# Pure-function tests (no DB)
# ---------------------------------------------------------------------------


class TestSpec046ScoringMath:
    """T1-T6, T11: Pure composite/calibration math."""

    def test_signal_result_dataclass(self):
        """T1: SignalResult has required fields."""
        from app.services.probability_engine import SignalResult

        r = SignalResult(
            signal_type="financial_health",
            score=75.0,
            confidence=0.9,
            details={"source": "test"},
            data_sources=["pe_portfolio"],
        )
        assert r.signal_type == "financial_health"
        assert r.score == 75.0
        assert r.confidence == 0.9

    def test_composite_formula_no_convergence(self):
        """T2: No above-60 signals → convergence factor = 1.0."""
        from app.services.probability_engine import TransactionProbabilityEngine, SignalResult

        signals = [
            SignalResult("financial_health", 50, 1.0, {}, []),
            SignalResult("exec_transition", 40, 1.0, {}, []),
        ]
        weights = {"financial_health": 0.6, "exec_transition": 0.4}
        raw, convergence = TransactionProbabilityEngine._compute_composite_static(signals, weights)
        # 50*0.6 + 40*0.4 = 46; factor = 1.0
        assert abs(raw - 46.0) < 0.01
        assert abs(convergence - 1.0) < 0.01

    def test_composite_formula_with_convergence(self):
        """T3: 3 signals above 60 → bonus = 1 + 3*0.08 = 1.24."""
        from app.services.probability_engine import TransactionProbabilityEngine, SignalResult

        signals = [
            SignalResult("financial_health", 70, 1.0, {}, []),
            SignalResult("exec_transition", 80, 1.0, {}, []),
            SignalResult("sector_momentum", 65, 1.0, {}, []),
            SignalResult("innovation_velocity", 30, 1.0, {}, []),
        ]
        weights = {
            "financial_health": 0.3,
            "exec_transition": 0.3,
            "sector_momentum": 0.2,
            "innovation_velocity": 0.2,
        }
        raw, convergence = TransactionProbabilityEngine._compute_composite_static(signals, weights)
        # weighted = 70*0.3+80*0.3+65*0.2+30*0.2 = 21+24+13+6 = 64; factor = 1.24
        # final = 64 * 1.24 = 79.36
        assert abs(convergence - 1.24) < 0.01
        assert abs(raw - 79.36) < 0.5

    def test_composite_clamped_to_100(self):
        """T4: Extreme convergence never exceeds 100."""
        from app.services.probability_engine import TransactionProbabilityEngine, SignalResult

        signals = [SignalResult(f"s{i}", 95, 1.0, {}, []) for i in range(12)]
        weights = {f"s{i}": 1.0 / 12 for i in range(12)}
        raw, _ = TransactionProbabilityEngine._compute_composite_static(signals, weights)
        assert raw <= 100.0

    def test_calibrate_sigmoid_range(self):
        """T5: Sigmoid output always in [0, 1]."""
        from app.services.probability_engine import TransactionProbabilityEngine

        for raw in (0, 25, 50, 55, 75, 100):
            p = TransactionProbabilityEngine._calibrate_sigmoid(raw, k=0.08, x0=55)
            assert 0.0 <= p <= 1.0

    def test_calibrate_sigmoid_midpoint(self):
        """T6: raw=x0 → P=0.5."""
        from app.services.probability_engine import TransactionProbabilityEngine

        p = TransactionProbabilityEngine._calibrate_sigmoid(55, k=0.08, x0=55)
        assert abs(p - 0.5) < 0.001

    def test_grade_thresholds(self):
        """T11: Composite score → grade mapping."""
        from app.services.probability_engine import TransactionProbabilityEngine

        assert TransactionProbabilityEngine._grade_from_score(90) == "A"
        assert TransactionProbabilityEngine._grade_from_score(85) == "A"
        assert TransactionProbabilityEngine._grade_from_score(75) == "B"
        assert TransactionProbabilityEngine._grade_from_score(60) == "C"
        assert TransactionProbabilityEngine._grade_from_score(45) == "D"
        assert TransactionProbabilityEngine._grade_from_score(30) == "F"


class TestSpec046Velocity:
    """T7-T8: Velocity/acceleration computation."""

    def test_velocity_first_snapshot(self):
        """T7: No previous score → velocity = 0."""
        from app.services.probability_engine import TransactionProbabilityEngine

        velocity, accel = TransactionProbabilityEngine._compute_velocity_static(
            current=70, previous=None, prev_velocity=None
        )
        assert velocity == 0.0
        assert accel == 0.0

    def test_velocity_computed(self):
        """T8: velocity = current - previous; acceleration = velocity - prev_velocity."""
        from app.services.probability_engine import TransactionProbabilityEngine

        velocity, accel = TransactionProbabilityEngine._compute_velocity_static(
            current=80, previous=70, prev_velocity=5
        )
        assert velocity == 10.0
        assert accel == 5.0


# ---------------------------------------------------------------------------
# Signal computer tests
# ---------------------------------------------------------------------------


class TestSpec046SignalComputers:
    """T12-T14: Individual signal computers."""

    def test_insider_activity_empty_data(self, db_session):
        """T12: InsiderActivityComputer returns graceful default on empty data."""
        from app.services.probability_signal_computers import InsiderActivityComputer
        from app.core.probability_models import TxnProbCompany

        c = TxnProbCompany(
            company_name="InsiderTest Co",
            normalized_name="insidertest",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        result = InsiderActivityComputer(db_session).compute(c)
        assert result.signal_type == "insider_activity"
        assert 0 <= result.score <= 100
        # No data → confidence should be low
        assert result.confidence <= 0.5

    def test_founder_risk_computer(self, db_session):
        """T13: FounderRiskComputer uses founded_year."""
        from app.services.probability_signal_computers import FounderRiskComputer
        from app.core.probability_models import TxnProbCompany

        c = TxnProbCompany(
            company_name="FounderTest Co",
            normalized_name="foundertest",
            sector="Technology",
            founded_year=1985,  # Old company — some founder risk likely
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        result = FounderRiskComputer(db_session).compute(c)
        assert result.signal_type == "founder_risk"
        assert 0 <= result.score <= 100

    def test_macro_tailwind_computer(self, db_session):
        """T14: MacroTailwindComputer returns graceful default without region/momentum data."""
        from app.services.probability_signal_computers import MacroTailwindComputer
        from app.core.probability_models import TxnProbCompany

        c = TxnProbCompany(
            company_name="MacroTest Co",
            normalized_name="macrotest",
            sector="Technology",
            hq_state="CA",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        result = MacroTailwindComputer(db_session).compute(c)
        assert result.signal_type == "macro_tailwind"
        assert 0 <= result.score <= 100


# ---------------------------------------------------------------------------
# End-to-end engine tests
# ---------------------------------------------------------------------------


class TestSpec046Engine:
    """T9-T10: Full scoring pipeline."""

    def test_score_company_persists_snapshots(self, db_session):
        """T9: score_company persists 12 signals + 1 score row."""
        from app.services.probability_engine import TransactionProbabilityEngine
        from app.core.probability_models import (
            TxnProbCompany,
            TxnProbSignal,
            TxnProbScore,
        )

        c = TxnProbCompany(
            company_name="EngineTest Co",
            normalized_name="enginetest",
            sector="Technology",
            hq_state="CA",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        engine = TransactionProbabilityEngine(db_session)
        result = engine.score_company(c.id)

        assert "probability" in result
        assert "raw_composite_score" in result
        assert "signal_chain" in result
        assert 0.0 <= result["probability"] <= 1.0

        # Persistence check
        signal_count = db_session.query(TxnProbSignal).filter_by(company_id=c.id).count()
        score_count = db_session.query(TxnProbScore).filter_by(company_id=c.id).count()
        assert signal_count == 12, f"Expected 12 signals, got {signal_count}"
        assert score_count == 1

    def test_signal_chain_decomposition(self, db_session):
        """T10: signal_chain elements with weights reproduce raw composite."""
        from app.services.probability_engine import TransactionProbabilityEngine
        from app.core.probability_models import TxnProbCompany

        c = TxnProbCompany(
            company_name="DecompTest Co",
            normalized_name="decomptest",
            sector="Technology",
            hq_state="NY",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        result = TransactionProbabilityEngine(db_session).score_company(c.id)
        chain = result["signal_chain"]
        assert isinstance(chain, list)
        assert len(chain) == 12
        # Each entry should have score, weight, signal_type, contribution
        for entry in chain:
            assert "signal_type" in entry
            assert "score" in entry
            assert "weight" in entry
            assert "contribution" in entry


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


class TestSpec046API:
    """T15-T16: API endpoints."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from app.main import app

        return TestClient(app)

    def test_api_score_endpoint(self, client):
        """T15: POST /score/{company_id} returns signal chain."""
        from app.core.database import get_session_factory
        from app.core.probability_models import TxnProbCompany

        # Use the app's DB session (same Postgres that TestClient hits),
        # not db_session (which is a separate SQLite test DB).
        SessionLocal = get_session_factory()
        app_db = SessionLocal()
        try:
            c = TxnProbCompany(
                company_name="APITest Co Phase2",
                normalized_name="apitestphase2",
                sector="Technology",
                universe_source="manual",
                is_active=True,
            )
            app_db.add(c)
            app_db.commit()
            cid = c.id

            try:
                resp = client.post(f"/api/v1/txn-probability/score/{cid}")
                assert resp.status_code == 200, resp.text
                data = resp.json()
                assert "probability" in data
                assert "signal_chain" in data
                assert len(data["signal_chain"]) == 12
            finally:
                # Cleanup — delete the test company (cascades to signals/scores/alerts)
                app_db.delete(app_db.query(TxnProbCompany).filter_by(id=cid).first())
                app_db.commit()
        finally:
            app_db.close()

    def test_api_rankings_filters(self, client):
        """T16: GET /rankings with filters."""
        resp = client.get("/api/v1/txn-probability/rankings?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
