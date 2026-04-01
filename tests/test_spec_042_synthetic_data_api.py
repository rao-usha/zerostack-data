"""
Tests for SPEC_042 — Synthetic Data API
POST /api/v1/synthetic/private-financials
POST /api/v1/synthetic/macro-scenarios
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Private Company Financials Generator
# ---------------------------------------------------------------------------

class TestPrivateCompanyFinancialsGenerator:
    """Unit tests for PrivateCompanyFinancialGenerator service."""

    def _make_db(self, rows):
        """Create mock DB session that returns given rows."""
        db = MagicMock(spec=Session)
        result = MagicMock()
        result.fetchall.return_value = rows
        db.execute.return_value = result
        return db

    def test_generates_requested_count(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])  # empty peers → fallback to priors
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=10, sector="industrials", seed=42)
        assert len(result["companies"]) == 10

    def test_uses_sector_priors_when_no_peers(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=5, sector="technology", seed=42)
        assert result["peer_count"] == 0
        assert result.get("fallback") == "sector_priors"

    def test_no_nan_in_output(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=20, sector="healthcare", seed=1)
        for co in result["companies"]:
            for k, v in co.items():
                if isinstance(v, float):
                    assert v == v, f"NaN found in {k}"  # NaN != NaN

    def test_margin_constraints(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=50, seed=99)
        for co in result["companies"]:
            assert co["gross_margin_pct"] <= 100.0
            assert co["gross_margin_pct"] >= 0.0
            assert co["ebitda_margin_pct"] <= co["gross_margin_pct"] + 1  # ebitda ≤ gross (approx)
            assert co["net_margin_pct"] <= co["ebitda_margin_pct"] + 1

    def test_deterministic_with_seed(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen1 = PrivateCompanyFinancialGenerator(db)
        gen2 = PrivateCompanyFinancialGenerator(db)
        r1 = gen1.generate(n_companies=5, seed=7)
        r2 = gen2.generate(n_companies=5, seed=7)
        for c1, c2 in zip(r1["companies"], r2["companies"]):
            assert abs(c1["revenue_millions"] - c2["revenue_millions"]) < 0.01

    def test_revenue_within_bounds(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=20, revenue_min_millions=50, revenue_max_millions=200, seed=5)
        for co in result["companies"]:
            assert 50 <= co["revenue_millions"] <= 200

    def test_company_ids_unique(self):
        from app.services.synthetic.private_company_financials import PrivateCompanyFinancialGenerator
        db = self._make_db([])
        gen = PrivateCompanyFinancialGenerator(db)
        result = gen.generate(n_companies=10, seed=3)
        ids = [c["company_id"] for c in result["companies"]]
        assert len(set(ids)) == 10


# ---------------------------------------------------------------------------
# Macro Scenario Generator
# ---------------------------------------------------------------------------

class TestMacroScenarioGenerator:
    """Unit tests for MacroScenarioGenerator service."""

    def _make_fred_db(self):
        """Return mock DB that returns dummy FRED data."""
        import datetime
        db = MagicMock(spec=Session)
        rows = [
            (datetime.date(2020, 1, 1), "DFF", 1.75),
            (datetime.date(2020, 2, 1), "DFF", 1.75),
            (datetime.date(2020, 3, 1), "DFF", 0.25),
            (datetime.date(2023, 1, 1), "DFF", 4.33),
            (datetime.date(2024, 1, 1), "DFF", 5.33),
            (datetime.date(2025, 1, 1), "DFF", 3.64),
        ]
        result = MagicMock()
        result.fetchall.return_value = rows
        db.execute.return_value = result
        return db

    def test_generates_requested_scenario_count(self):
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        gen = MacroScenarioGenerator(db)
        result = gen.generate(n_scenarios=10, horizon_months=12, series=["DFF"], seed=42)
        assert len(result["scenarios"]) == 10

    def test_path_length_matches_horizon(self):
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        gen = MacroScenarioGenerator(db)
        result = gen.generate(n_scenarios=5, horizon_months=6, series=["DFF"], seed=1)
        for sc in result["scenarios"]:
            assert len(sc["paths"]["DFF"]) == 6

    def test_no_nan_in_paths(self):
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        gen = MacroScenarioGenerator(db)
        result = gen.generate(n_scenarios=5, horizon_months=12, series=["DFF"], seed=2)
        for sc in result["scenarios"]:
            for series, vals in sc["paths"].items():
                for v in vals:
                    assert v == v, f"NaN in scenario {sc['scenario_id']} series {series}"

    def test_deterministic_with_seed(self):
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        r1 = MacroScenarioGenerator(db).generate(n_scenarios=3, horizon_months=6, series=["DFF"], seed=99)
        r2 = MacroScenarioGenerator(db).generate(n_scenarios=3, horizon_months=6, series=["DFF"], seed=99)
        for s1, s2 in zip(r1["scenarios"], r2["scenarios"]):
            for v1, v2 in zip(s1["paths"]["DFF"], s2["paths"]["DFF"]):
                assert abs(v1 - v2) < 1e-9

    def test_summary_has_all_series(self):
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        gen = MacroScenarioGenerator(db)
        result = gen.generate(n_scenarios=20, horizon_months=12, series=["DFF"], seed=5)
        assert "DFF" in result["summary"]
        assert "p50_terminal" in result["summary"]["DFF"]

    def test_rate_clamp(self):
        """Interest rates should never go negative."""
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator
        db = self._make_fred_db()
        gen = MacroScenarioGenerator(db)
        result = gen.generate(n_scenarios=50, horizon_months=36, series=["DFF"], seed=0)
        for sc in result["scenarios"]:
            for v in sc["paths"]["DFF"]:
                assert v >= 0.0
                assert v <= 25.0
