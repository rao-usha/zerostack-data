"""
Tests for SPEC 047 — Deal Probability Engine: Phase 3 Intelligence Layer
Covers: convergence patterns, alert engine, NLQ, narrative generator, API wiring.
"""
import pytest


# ---------------------------------------------------------------------------
# Pure-function convergence tests
# ---------------------------------------------------------------------------


class TestSpec047Convergence:
    """T1-T5: Named convergence patterns."""

    def test_convergence_patterns_registry_has_four(self):
        """T1: ≥4 patterns registered."""
        from app.services.probability_convergence import CONVERGENCE_PATTERNS

        assert len(CONVERGENCE_PATTERNS) >= 4

    def test_convergence_classic_exit_setup(self):
        """T2: classic_exit_setup matches when required signals pass thresholds."""
        from app.services.probability_convergence import (
            CONVERGENCE_PATTERNS,
            match_pattern,
        )

        signals = {
            "exec_transition": 65,
            "financial_health": 72,
            "sector_momentum": 68,
        }
        assert match_pattern(CONVERGENCE_PATTERNS["classic_exit_setup"], signals)

        # One below threshold → no match
        signals["sector_momentum"] = 50
        assert not match_pattern(CONVERGENCE_PATTERNS["classic_exit_setup"], signals)

    def test_convergence_founder_transition(self):
        """T3: founder_transition pattern."""
        from app.services.probability_convergence import (
            CONVERGENCE_PATTERNS,
            match_pattern,
        )

        signals = {
            "founder_risk": 75,
            "exec_transition": 55,
            "deal_activity_signals": 45,
        }
        assert match_pattern(CONVERGENCE_PATTERNS["founder_transition"], signals)

    def test_convergence_distress_opportunity(self):
        """T4: Inverted thresholds (max) for distress signals."""
        from app.services.probability_convergence import (
            CONVERGENCE_PATTERNS,
            match_pattern,
        )

        # Weak diligence (low), insider selling (low score), and restructuring hires
        signals = {
            "diligence_health": 35,
            "insider_activity": 30,
            "hiring_velocity": 55,
        }
        assert match_pattern(CONVERGENCE_PATTERNS["distress_opportunity"], signals)

        # Healthy diligence breaks the pattern
        signals["diligence_health"] = 70
        assert not match_pattern(
            CONVERGENCE_PATTERNS["distress_opportunity"], signals
        )

    def test_convergence_no_match(self):
        """T5: detect_patterns returns empty when nothing qualifies."""
        from app.services.probability_convergence import detect_patterns

        signals = {s: 50 for s in ("exec_transition", "financial_health", "sector_momentum")}
        assert detect_patterns(signals) == []


# ---------------------------------------------------------------------------
# Alert engine tests
# ---------------------------------------------------------------------------


class TestSpec047Alerts:
    """T6-T9: Alert engine (pure-ish — inspects the generated specs)."""

    def _make_score(self, probability, grade="D"):
        """Build a minimal TxnProbScore-like object using dataclass shim."""
        from app.core.probability_models import TxnProbScore

        s = TxnProbScore(
            company_id=1,
            probability=probability,
            raw_composite_score=probability * 100,
            grade=grade,
            signal_count=12,
            active_signal_count=3,
            convergence_factor=1.0,
            top_signals=[],
            signal_chain=[],
            model_version="v1.0",
            batch_id="test",
        )
        return s

    def test_alert_probability_spike(self, db_session):
        """T6: delta > 0.15 → high-severity probability_spike alert."""
        from app.core.probability_models import TxnProbCompany
        from app.services.probability_alerts import AlertEngine

        c = TxnProbCompany(
            company_name="AlertSpike Co",
            normalized_name="alertspike",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        prev = self._make_score(0.30)
        new = self._make_score(0.55)
        new.id = None
        # Persist prev/new as real rows so FK is valid
        prev.company_id = c.id
        new.company_id = c.id
        db_session.add(prev)
        db_session.add(new)
        db_session.flush()

        alerts = AlertEngine(db_session).evaluate(
            company_id=c.id,
            prev_score=prev,
            new_score_row=new,
            new_convergences=[],
        )
        spike_alerts = [a for a in alerts if a.alert_type == "probability_spike"]
        assert len(spike_alerts) == 1
        assert spike_alerts[0].severity == "high"

    def test_alert_grade_upgrade(self, db_session):
        """T7: Grade C → B fires grade_upgrade alert."""
        from app.core.probability_models import TxnProbCompany
        from app.services.probability_alerts import AlertEngine

        c = TxnProbCompany(
            company_name="AlertGrade Co",
            normalized_name="alertgrade",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        prev = self._make_score(0.50, grade="C")
        new = self._make_score(0.52, grade="B")
        prev.company_id = c.id
        new.company_id = c.id
        db_session.add(prev)
        db_session.add(new)
        db_session.flush()

        alerts = AlertEngine(db_session).evaluate(
            company_id=c.id,
            prev_score=prev,
            new_score_row=new,
            new_convergences=[],
        )
        grade_alerts = [a for a in alerts if a.alert_type == "grade_upgrade"]
        assert len(grade_alerts) == 1

    def test_alert_new_convergence(self, db_session):
        """T8: New convergence pattern → high-severity new_convergence alert."""
        from app.core.probability_models import TxnProbCompany
        from app.services.probability_alerts import AlertEngine

        c = TxnProbCompany(
            company_name="AlertCvg Co",
            normalized_name="alertcvg",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        prev = self._make_score(0.30)
        new = self._make_score(0.32)
        prev.company_id = c.id
        new.company_id = c.id
        db_session.add(prev)
        db_session.add(new)
        db_session.flush()

        new_convergences = [
            {
                "key": "classic_exit_setup",
                "label": "Classic Exit Setup",
                "description": "...",
                "severity": "high",
                "matched_signals": {"exec_transition": 70},
            }
        ]
        alerts = AlertEngine(db_session).evaluate(
            company_id=c.id,
            prev_score=prev,
            new_score_row=new,
            new_convergences=new_convergences,
        )
        cvg_alerts = [a for a in alerts if a.alert_type == "new_convergence"]
        assert len(cvg_alerts) == 1
        assert cvg_alerts[0].severity == "high"

    def test_alert_no_spike(self, db_session):
        """T9: small delta does not fire probability_spike."""
        from app.core.probability_models import TxnProbCompany
        from app.services.probability_alerts import AlertEngine

        c = TxnProbCompany(
            company_name="AlertNoSpike Co",
            normalized_name="alertnospike",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        prev = self._make_score(0.40)
        new = self._make_score(0.45)
        prev.company_id = c.id
        new.company_id = c.id
        db_session.add(prev)
        db_session.add(new)
        db_session.flush()

        alerts = AlertEngine(db_session).evaluate(
            company_id=c.id,
            prev_score=prev,
            new_score_row=new,
            new_convergences=[],
        )
        assert not [a for a in alerts if a.alert_type == "probability_spike"]


# ---------------------------------------------------------------------------
# NLQ tests
# ---------------------------------------------------------------------------


class TestSpec047NLQ:
    """T10-T12: Natural language query."""

    def test_nlq_whitelist_rejects_bad_field(self):
        """T10: filter with unknown field is dropped."""
        from app.services.probability_nlq import validate_filter

        assert validate_filter({"field": "DROP TABLE", "op": "=", "value": "x"}) is None
        assert validate_filter({"field": "probability", "op": ">=", "value": 0.5}) is not None

    def test_nlq_keyword_fallback(self):
        """T11: keyword fallback produces reasonable filters without LLM."""
        from app.services.probability_nlq import keyword_fallback

        r1 = keyword_fallback("show me healthcare companies ready to exit")
        flds = {f["field"] for f in r1["filters"]}
        assert "sector" in flds
        assert "probability" in flds

    def test_nlq_executes_filters(self, db_session):
        """T12: filters applied via ORM return correctly-filtered rows."""
        import asyncio
        from app.core.probability_models import TxnProbCompany, TxnProbScore
        from app.services.probability_nlq import ProbabilityNLQ

        # Seed two companies + scores
        c1 = TxnProbCompany(
            company_name="NLQ Healthcare Co",
            normalized_name="nlqhealthcare",
            sector="Healthcare",
            universe_source="manual",
            is_active=True,
        )
        c2 = TxnProbCompany(
            company_name="NLQ Tech Co",
            normalized_name="nlqtech",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add_all([c1, c2])
        db_session.commit()

        db_session.add(
            TxnProbScore(
                company_id=c1.id,
                probability=0.85,
                raw_composite_score=85.0,
                grade="A",
                signal_count=12,
                active_signal_count=5,
                convergence_factor=1.0,
                top_signals=[],
                signal_chain=[],
                model_version="v1.0",
                batch_id="nlq-test",
            )
        )
        db_session.add(
            TxnProbScore(
                company_id=c2.id,
                probability=0.30,
                raw_composite_score=30.0,
                grade="F",
                signal_count=12,
                active_signal_count=0,
                convergence_factor=1.0,
                top_signals=[],
                signal_chain=[],
                model_version="v1.0",
                batch_id="nlq-test",
            )
        )
        db_session.commit()

        nlq = ProbabilityNLQ(db_session)
        # keyword_fallback picks up "healthcare" and "ready to exit" → prob>=0.7
        result = asyncio.get_event_loop().run_until_complete(
            nlq.query("healthcare companies ready to exit")
        )
        names = [r["company_name"] for r in result.results]
        assert "NLQ Healthcare Co" in names
        assert "NLQ Tech Co" not in names


# ---------------------------------------------------------------------------
# Narrative + engine integration
# ---------------------------------------------------------------------------


class TestSpec047NarrativeAndIntegration:
    """T13-T14: Narrative fallback + engine wires alerts."""

    def test_narrative_graceful_when_no_llm(self, db_session):
        """T13: narrative falls back to deterministic template when LLM fails."""
        import asyncio
        from app.core.probability_models import (
            TxnProbCompany,
            TxnProbScore,
        )
        from app.services.probability_narrative import ProbabilityNarrativeGenerator

        c = TxnProbCompany(
            company_name="Narrative Co",
            normalized_name="narrative",
            sector="Technology",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()
        db_session.add(
            TxnProbScore(
                company_id=c.id,
                probability=0.55,
                raw_composite_score=55.0,
                grade="C",
                signal_count=12,
                active_signal_count=3,
                convergence_factor=1.0,
                top_signals=[
                    {"signal_type": "financial_health", "score": 70, "contribution": 10.5}
                ],
                signal_chain=[],
                model_version="v1.0",
                batch_id="narr-test",
            )
        )
        db_session.commit()

        # Use fallback directly (no network dependency)
        ctx = ProbabilityNarrativeGenerator(db_session)._gather_company_context(c.id)
        text = ProbabilityNarrativeGenerator._fallback_narrative(ctx)
        assert "Narrative Co" in text
        assert "55" in text or "55.0" in text

    def test_engine_integrates_alerts(self, db_session):
        """T14: Scoring twice fires alerts (at minimum a new_universe_entry on 1st run)."""
        from app.core.probability_models import (
            TxnProbAlert,
            TxnProbCompany,
        )
        from app.services.probability_engine import TransactionProbabilityEngine

        c = TxnProbCompany(
            company_name="EngIntegration Co",
            normalized_name="engintegration",
            sector="Technology",
            hq_state="CA",
            universe_source="manual",
            is_active=True,
        )
        db_session.add(c)
        db_session.commit()

        engine = TransactionProbabilityEngine(db_session)
        engine.score_company(c.id)
        engine.score_company(c.id)

        alerts = db_session.query(TxnProbAlert).filter_by(company_id=c.id).all()
        # At minimum, the first-run "new_universe_entry" alert should exist
        types = {a.alert_type for a in alerts}
        assert "new_universe_entry" in types


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


class TestSpec047API:
    """T15-T16: Phase 3 API endpoints."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from app.main import app

        return TestClient(app)

    def test_api_convergences_endpoint(self, client):
        """T15: GET /convergences returns pattern registry + companies list."""
        resp = client.get("/api/v1/txn-probability/convergences")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "patterns" in data
        assert "companies" in data
        assert data["pattern_count"] >= 4

    def test_api_query_endpoint(self, client, db_session):
        """T16: POST /query with a natural-language string returns results."""
        resp = client.post(
            "/api/v1/txn-probability/query",
            json={"query": "show me all scored companies", "limit": 10},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "results" in data
        assert "explanation" in data
