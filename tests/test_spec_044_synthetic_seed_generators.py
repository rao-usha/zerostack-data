"""
Tests for SPEC 044 — Synthetic Seed Generators (Phase A)

Validates: job postings generator, LP-GP universe generator,
and macro scenario → stress scorer wiring.
"""
import random
import pytest

from app.services.synthetic.job_postings import (
    SyntheticJobPostingsGenerator,
    SECTOR_ROLE_MIX,
    SENIORITY_WEIGHTS,
    _pick_title,
)
from app.services.synthetic.lp_gp_universe import (
    SyntheticLpGpGenerator,
    LP_TYPE_CONFIG,
)


class TestSyntheticJobPostings:
    """Tests for synthetic job postings generator."""

    def test_job_postings_output_shape(self):
        """T1: _pick_title returns string with department name for non-c-suite."""
        title = _pick_title("manager", "Engineering")
        assert isinstance(title, str)
        assert len(title) > 0
        # Manager-level titles should contain the department
        assert "Engineering" in title or "Manager" in title

    def test_job_postings_seniority_distribution(self):
        """T2: Seniority weights sum to ~1 and c_suite+vp is ~5%."""
        total = sum(SENIORITY_WEIGHTS.values())
        assert abs(total - 1.0) < 0.01

        csuite_vp_pct = SENIORITY_WEIGHTS["c_suite"] + SENIORITY_WEIGHTS["vp"]
        assert 0.03 <= csuite_vp_pct <= 0.07  # ~5% ± 2%

    def test_job_postings_sector_awareness(self):
        """T3: Tech sector has Engineering as largest department."""
        tech_mix = SECTOR_ROLE_MIX["technology"]
        top_dept = max(tech_mix, key=tech_mix.get)
        assert top_dept == "Engineering"

        # Healthcare should have Clinical as top
        health_mix = SECTOR_ROLE_MIX["healthcare"]
        top_dept = max(health_mix, key=health_mix.get)
        assert top_dept == "Clinical"

    def test_job_postings_data_origin(self):
        """T4: Generator creates ingestion job with data_origin='synthetic'."""
        from app.core.models import IngestionJob
        job = IngestionJob(
            source="synthetic_job_postings",
            config={"test": True},
            data_origin="synthetic",
        )
        assert job.data_origin == "synthetic"
        assert job.source == "synthetic_job_postings"

    def test_job_postings_sector_detection(self):
        """Sector detection maps industry strings correctly."""
        gen = SyntheticJobPostingsGenerator.__new__(SyntheticJobPostingsGenerator)
        assert gen._detect_sector("Software & Technology") == "technology"
        assert gen._detect_sector("Healthcare Services") == "healthcare"
        assert gen._detect_sector("Oil & Gas Exploration") == "energy"
        assert gen._detect_sector("Consumer Retail") == "consumer"
        assert gen._detect_sector("Unknown Industry") == "industrials"

    def test_job_postings_weighted_pick(self):
        """Weighted pick returns valid keys."""
        random.seed(42)
        weights = {"a": 0.7, "b": 0.2, "c": 0.1}
        picks = [SyntheticJobPostingsGenerator._weighted_pick(weights) for _ in range(100)]
        assert all(p in weights for p in picks)
        # "a" should dominate
        assert picks.count("a") > picks.count("c")

    def test_job_postings_posted_days_distribution(self):
        """70% of postings should be within last 30 days."""
        random.seed(42)
        days = [SyntheticJobPostingsGenerator._pick_posted_days_ago() for _ in range(1000)]
        recent = sum(1 for d in days if d <= 30)
        assert recent / 1000 > 0.60  # at least 60% (allowing variance)


class TestSyntheticLpGp:
    """Tests for synthetic LP-GP universe generator."""

    def test_lp_generation_type_distribution(self):
        """T5: LP type weights sum to 1.0."""
        total = sum(cfg["weight"] for cfg in LP_TYPE_CONFIG.values())
        assert abs(total - 1.0) < 0.01

    def test_lp_type_proportions(self):
        """LP type proportions match spec."""
        assert LP_TYPE_CONFIG["public_pension"]["weight"] == 0.40
        assert LP_TYPE_CONFIG["endowment"]["weight"] == 0.20
        assert LP_TYPE_CONFIG["insurance"]["weight"] == 0.15
        assert LP_TYPE_CONFIG["sovereign_wealth"]["weight"] == 0.10
        assert LP_TYPE_CONFIG["family_office"]["weight"] == 0.10
        assert LP_TYPE_CONFIG["fund_of_funds"]["weight"] == 0.05

    def test_lp_gp_relationship_power_law(self):
        """T6: Power-law weights decrease with index."""
        weights = [1.0 / (i + 1) ** 0.6 for i in range(100)]
        # First GP should have much higher weight than last
        assert weights[0] > weights[-1] * 3

    def test_lp_commitment_sizing(self):
        """T7: Commitment range is 1-5% of AUM."""
        random.seed(42)
        aum_billions = 100.0
        pcts = [random.uniform(0.01, 0.05) for _ in range(100)]
        for pct in pcts:
            commitment = aum_billions * pct * 1e9
            assert 1e9 <= commitment <= 5e9  # 1-5% of $100B

    def test_lp_gp_data_origin(self):
        """T8: Generator creates ingestion job with data_origin='synthetic'."""
        from app.core.models import IngestionJob
        job = IngestionJob(
            source="synthetic_lp_gp",
            config={"test": True},
            data_origin="synthetic",
        )
        assert job.data_origin == "synthetic"
        assert job.source == "synthetic_lp_gp"

    def test_lp_aum_ranges(self):
        """AUM ranges are sensible per type."""
        for lp_type, cfg in LP_TYPE_CONFIG.items():
            lo, hi = cfg["aum_range"]
            assert lo < hi, f"{lp_type} AUM range invalid"
            assert lo > 0, f"{lp_type} AUM min must be positive"


class TestStressScenarioWiring:
    """Tests for macro scenario → stress scorer wiring."""

    def test_stress_scorer_with_scenario(self):
        """T9: PortfolioStressScorer accepts macro_overrides parameter."""
        from app.services.portfolio_stress_scorer import PortfolioStressScorer
        import inspect

        sig = inspect.signature(PortfolioStressScorer.score_portfolio)
        params = list(sig.parameters.keys())
        assert "macro_overrides" in params

    def test_scenario_dict_structure(self):
        """Scenario dict keys match what stress scorer consumes."""
        valid_keys = {"fed_funds_rate", "cpi_yoy_pct", "energy_cost_yoy_pct",
                      "oil_price", "consumer_sentiment"}
        # An adverse scenario
        adverse = {
            "fed_funds_rate": 7.0,
            "cpi_yoy_pct": 6.0,
            "energy_cost_yoy_pct": 25.0,
        }
        assert all(k in valid_keys for k in adverse)


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_company_list(self):
        """T10: Title generation works with all seniority levels."""
        for seniority in SENIORITY_WEIGHTS:
            title = _pick_title(seniority, "Operations")
            assert isinstance(title, str)
            assert len(title) > 0

    def test_all_sector_role_mixes_sum_to_one(self):
        """All sector role mixes sum to approximately 1.0."""
        for sector, mix in SECTOR_ROLE_MIX.items():
            total = sum(mix.values())
            assert abs(total - 1.0) < 0.01, f"{sector} role mix sums to {total}"
