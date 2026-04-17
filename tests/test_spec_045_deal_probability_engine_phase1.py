"""
Tests for SPEC 045 — Deal Probability Engine: Phase 1 Foundation
Covers: probability_models, probability_universe, probability_signal_taxonomy
"""
import pytest


class TestSpec045Models:
    """T1-T4: Database models."""

    def test_all_six_tables_registered(self):
        """T1: All 6 tables exist in Base.metadata."""
        from app.core.models import Base
        from app.core import probability_models  # noqa: F401

        expected = {
            "txn_prob_companies",
            "txn_prob_signals",
            "txn_prob_scores",
            "txn_prob_outcomes",
            "txn_prob_alerts",
            "sector_signal_weights",
        }
        registered = set(Base.metadata.tables.keys())
        missing = expected - registered
        assert not missing, f"Missing tables: {missing}"

    def test_txn_prob_company_unique_constraint(self):
        """T2: Unique constraint on (lower(company_name), sector)."""
        from app.core.probability_models import TxnProbCompany

        # Check that a unique constraint is defined
        constraints = [c for c in TxnProbCompany.__table__.constraints]
        has_unique = any(
            "name_sector" in str(c).lower() or "unique" in type(c).__name__.lower()
            for c in constraints
        )
        assert has_unique, "Expected unique constraint on name+sector"

    def test_txn_prob_signal_time_series(self):
        """T3: txn_prob_signals allows multiple rows per (company, signal_type) at different timestamps."""
        from app.core.probability_models import TxnProbSignal

        cols = {c.name for c in TxnProbSignal.__table__.columns}
        assert "scored_at" in cols, "time-series requires scored_at"
        assert "score" in cols
        assert "velocity" in cols
        assert "acceleration" in cols

    def test_txn_prob_score_probability_range(self):
        """T4: txn_prob_scores.probability column is Float type."""
        from sqlalchemy import Float
        from app.core.probability_models import TxnProbScore

        prob_col = TxnProbScore.__table__.columns["probability"]
        assert isinstance(prob_col.type, Float), f"probability must be Float, got {prob_col.type}"


class TestSpec045Universe:
    """T5-T7: CompanyUniverseBuilder."""

    def test_universe_builder_dedup(self, db_session):
        """T5: Calling build_universe() twice does not duplicate rows."""
        from app.services.probability_universe import CompanyUniverseBuilder

        builder = CompanyUniverseBuilder(db_session)
        first = builder.build_universe()
        second = builder.build_universe()
        # Total should not increase on second call (or at most match new source data)
        assert second["inserted"] == 0 or second["total"] == first["total"]

    def test_universe_builder_sources(self, db_session):
        """T6: Universe builder pulls from pe_portfolio, industrial, form_d sources."""
        from app.services.probability_universe import CompanyUniverseBuilder

        builder = CompanyUniverseBuilder(db_session)
        pe = builder._load_from_pe_portfolio()
        industrial = builder._load_from_industrial()
        form_d = builder._load_from_form_d()
        # Each loader returns a list (may be empty if source table empty)
        assert isinstance(pe, list)
        assert isinstance(industrial, list)
        assert isinstance(form_d, list)

    def test_universe_builder_empty_sources(self, db_session):
        """T7: Graceful handling when source tables are empty or missing."""
        from app.services.probability_universe import CompanyUniverseBuilder

        builder = CompanyUniverseBuilder(db_session)
        # Should not raise even if sources return empty
        result = builder.build_universe()
        assert isinstance(result, dict)
        assert "inserted" in result
        assert "total" in result


class TestSpec045SignalTaxonomy:
    """T8-T12: Signal taxonomy."""

    def test_signal_taxonomy_has_twelve_signals(self):
        """T8: Exactly 12 signals defined."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

        assert len(SIGNAL_TAXONOMY) == 12, f"Expected 12 signals, got {len(SIGNAL_TAXONOMY)}"

    def test_signal_taxonomy_weights_sum_to_one(self):
        """T9: Default weights sum to 1.0."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

        total = sum(s["default_weight"] for s in SIGNAL_TAXONOMY.values())
        assert abs(total - 1.0) < 0.001, f"Weights sum to {total}, expected 1.0"

    def test_sector_overrides_sum_to_one(self):
        """T10: Each sector override set sums to 1.0."""
        from app.ml.probability_signal_taxonomy import (
            SECTOR_WEIGHT_OVERRIDES,
            get_weights_for_sector,
        )

        for sector in SECTOR_WEIGHT_OVERRIDES.keys():
            weights = get_weights_for_sector(sector)
            total = sum(weights.values())
            assert abs(total - 1.0) < 0.001, f"{sector} weights sum to {total}"

    def test_signal_taxonomy_keys(self):
        """T11: All 12 expected signal keys present."""
        from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

        expected = {
            "financial_health",
            "exit_readiness",
            "acquisition_attractiveness",
            "exec_transition",
            "sector_momentum",
            "diligence_health",
            "insider_activity",
            "hiring_velocity",
            "deal_activity_signals",
            "innovation_velocity",
            "founder_risk",
            "macro_tailwind",
        }
        actual = set(SIGNAL_TAXONOMY.keys())
        assert actual == expected, f"Mismatch. Missing: {expected - actual}, Extra: {actual - expected}"

    def test_get_weights_for_sector(self):
        """T12: get_weights_for_sector returns overrides when defined, defaults otherwise."""
        from app.ml.probability_signal_taxonomy import (
            SIGNAL_TAXONOMY,
            get_weights_for_sector,
        )

        # Unknown sector falls back to defaults
        unknown = get_weights_for_sector("NonexistentSector")
        defaults = {k: v["default_weight"] for k, v in SIGNAL_TAXONOMY.items()}
        assert unknown == defaults

        # Known sector (Healthcare) differs from defaults for at least one signal
        healthcare = get_weights_for_sector("Healthcare")
        assert healthcare != defaults, "Healthcare overrides should differ from defaults"
