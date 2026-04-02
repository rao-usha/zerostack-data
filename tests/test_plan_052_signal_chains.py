"""
PLAN_052 Signal Chain Sanity Tests

Run with: pytest tests/test_plan_052_signal_chains.py -v

Unit tests (offline, mocked DB) for all 8 signal chains.
Integration tests (requires live DB) marked with @pytest.mark.integration.

To add your own test cases, look for the "# YOUR TEST HERE" comments.
"""
import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Helper: mock DB that returns canned rows per query pattern
# ---------------------------------------------------------------------------

def _make_db(query_results: dict = None):
    """
    Create a mock DB session.

    query_results: dict mapping a keyword in the SQL to the rows to return.
    Example: {"fred_interest_rates": [(date, 3.5)], "bls_ces": [(2025, "M01", 100)]}

    If a query doesn't match any keyword, returns [].
    """
    db = MagicMock(spec=Session)
    results_map = query_results or {}

    def _execute_side_effect(sql_text, params=None):
        sql_str = str(sql_text) if not isinstance(sql_text, str) else sql_text
        result = MagicMock()
        for keyword, rows in results_map.items():
            if keyword.lower() in sql_str.lower():
                result.fetchall.return_value = rows
                result.fetchone.return_value = rows[0] if rows else None
                return result
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    db.execute.side_effect = _execute_side_effect
    db.rollback = MagicMock()
    return db


# ===========================================================================
# CHAIN 1 — Deal Environment Score
# ===========================================================================

class TestChain1DealEnvironment:
    """Deal Environment Scorer — 9 sectors, 7 factors."""

    def test_all_sectors_present(self):
        """Should return scores for all 9 configured sectors."""
        from app.services.deal_environment_scorer import SECTOR_CONFIGS
        assert len(SECTOR_CONFIGS) == 9
        expected = {"industrials", "consumer", "healthcare", "technology",
                    "real_estate", "energy", "financial", "auto_service", "logistics"}
        assert set(SECTOR_CONFIGS.keys()) == expected

    def test_sector_configs_have_energy_sensitivity(self):
        """All sectors should have 'energy' and 'fx_export' sensitivity keys."""
        from app.services.deal_environment_scorer import SECTOR_CONFIGS
        for slug, config in SECTOR_CONFIGS.items():
            assert "energy" in config["sensitivity"], f"{slug} missing 'energy' sensitivity"
            assert "fx_export" in config["sensitivity"], f"{slug} missing 'fx_export' sensitivity"

    def test_bea_industry_map_covers_all_sectors(self):
        """Every sector should map to a BEA industry ID."""
        from app.services.deal_environment_scorer import SECTOR_BEA_INDUSTRY_MAP, SECTOR_CONFIGS
        for slug in SECTOR_CONFIGS:
            assert slug in SECTOR_BEA_INDUSTRY_MAP, f"{slug} missing from BEA industry map"

    def test_scorer_handles_empty_db(self):
        """Should produce scores (with defaults) even when DB is empty."""
        from app.services.deal_environment_scorer import DealEnvironmentScorer
        db = _make_db()
        scorer = DealEnvironmentScorer(db)
        result = scorer.score_sector("healthcare")
        assert 0 <= result.score <= 100
        assert result.grade in ("A", "B", "C", "D")
        assert result.sector == "healthcare"

    def test_score_all_sectors_returns_sorted(self):
        """score_all_sectors should return 9 results sorted by score descending."""
        from app.services.deal_environment_scorer import DealEnvironmentScorer
        db = _make_db()
        scorer = DealEnvironmentScorer(db)
        results = scorer.score_all_sectors()
        assert len(results) == 9
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)

    # YOUR TEST HERE: add sector-specific assertions
    # def test_energy_scores_lower_when_gdp_negative(self):
    #     """Energy should score lower than healthcare if GDP is negative."""
    #     pass


# ===========================================================================
# CHAIN 2 — Company Diligence Composite
# ===========================================================================

class TestChain2CompanyDiligence:
    """Company Diligence Composite Scorer — 6 factors, 8 sources."""

    def test_factor_weights_sum_to_one(self):
        from app.services.company_diligence_scorer import FACTOR_WEIGHTS
        total = sum(FACTOR_WEIGHTS.values())
        assert abs(total - 1.0) < 0.01, f"Weights sum to {total}, expected 1.0"

    def test_unknown_company_gets_neutral_score(self):
        """Company with no matches should get ~50 with 0 confidence."""
        from app.services.company_diligence_scorer import CompanyDiligenceScorer
        db = _make_db()
        scorer = CompanyDiligenceScorer(db)
        result = scorer.score_company("Completely Fake Company XYZ")
        assert result.confidence == 0.0
        assert 45 <= result.score <= 55, f"Expected ~50, got {result.score}"

    def test_high_gov_contracts_flags_dependency(self):
        """Company with >$100M contracts should get red flag."""
        from app.services.company_diligence_scorer import _score_revenue_concentration
        data = {"total_awards": 500_000_000, "num_contracts": 50, "max_single": 100_000_000}
        score, reading, impact = _score_revenue_concentration(data)
        assert score == 40
        assert "high gov dependency" in reading

    def test_high_epa_penalties_flag(self):
        """Company with >$1M EPA penalties should score very low."""
        from app.services.company_diligence_scorer import _score_environmental
        data = {"total_penalties": 2_000_000, "total_violations": 50,
                "num_facilities": 10, "active_violations": 3}
        score, reading, impact = _score_environmental(data)
        assert score <= 20
        assert impact == "negative"

    def test_no_data_returns_neutral_scores(self):
        """Each factor scorer should return reasonable score when data is None."""
        from app.services.company_diligence_scorer import (
            _score_revenue_concentration, _score_environmental,
            _score_safety, _score_legal, _score_innovation, _score_growth,
        )
        assert _score_revenue_concentration(None)[0] == 100
        assert _score_environmental(None)[0] == 100
        assert _score_safety(None)[0] == 100
        assert _score_legal(None)[0] == 100
        assert _score_innovation(None)[0] == 50  # no patents = unknown, not great
        assert _score_growth(None)[0] == 50

    # YOUR TEST HERE: test specific company matching
    # def test_company_with_known_epa_record(self):
    #     """Test a company you know has EPA violations."""
    #     pass


# ===========================================================================
# CHAIN 3 — GP Pipeline Score + LP→GP Graph
# ===========================================================================

class TestChain3GPPipeline:
    """GP Pipeline Scorer — 5 signals, LP→GP graph."""

    def test_signal_weights_sum_to_one(self):
        from app.services.gp_pipeline_scorer import SIGNAL_WEIGHTS
        total = sum(SIGNAL_WEIGHTS.values())
        assert abs(total - 1.0) < 0.01

    def test_tier1_classification(self):
        """Sovereign wealth and large pensions should be tier-1."""
        from app.services.gp_pipeline_scorer import _is_tier1
        assert _is_tier1("sovereign_wealth", None) is True
        assert _is_tier1("endowment", None) is True
        assert _is_tier1("public_pension", 200) is True   # $200B pension
        assert _is_tier1("public_pension", 50) is False    # $50B pension
        assert _is_tier1("family_office", 500) is False    # FO not tier-1

    def test_gp_with_no_lps_scores_zero(self):
        """GP with no LP relationships should get 0 D."""
        from app.services.gp_pipeline_scorer import GPPipelineScorer
        db = _make_db()
        scorer = GPPipelineScorer(db)
        result = scorer.score_gp(firm_id=99999, firm_name="Nobody GP")
        assert result.score == 0
        assert result.grade == "D"

    def test_relationship_strength_calculation(self):
        """More vintages + larger commitment = higher strength."""
        from app.services.lp_gp_graph import _compute_strength
        s1 = _compute_strength(vintages=1, commitment_usd=50_000_000, trend="new")
        s2 = _compute_strength(vintages=3, commitment_usd=500_000_000, trend="growing")
        assert s2 > s1, "3 vintages + $500M + growing should beat 1 vintage + $50M + new"

    def test_strength_clamped_0_100(self):
        from app.services.lp_gp_graph import _compute_strength
        assert 0 <= _compute_strength(0, 0, "declining") <= 100
        assert 0 <= _compute_strength(10, 10_000_000_000, "growing") <= 100

    # YOUR TEST HERE: test LP overlap logic
    # def test_two_gps_sharing_lps(self):
    #     """Two GPs with the same LP set should show 100% overlap."""
    #     pass


# ===========================================================================
# CHAIN 4 — Executive Signal
# ===========================================================================

class TestChain4ExecSignals:
    """Executive Signal Scorer — transition detection."""

    def test_many_csuite_openings_flags_succession(self):
        """2+ C-suite openings should flag succession_in_progress."""
        from app.services.exec_signal_scorer import ExecSignalScorer
        db = _make_db()
        scorer = ExecSignalScorer(db)
        profile = scorer._score_company(
            company_id=1, company_name="Test Corp",
            csuite_open=3, vp_open=2, director_open=10, total_open=50,
        )
        assert "succession_in_progress" in profile.flags
        assert profile.transition_score >= 80

    def test_only_directors_is_routine(self):
        """Only director-level hiring should not flag succession."""
        from app.services.exec_signal_scorer import ExecSignalScorer
        db = _make_db()
        scorer = ExecSignalScorer(db)
        profile = scorer._score_company(
            company_id=1, company_name="Stable Corp",
            csuite_open=0, vp_open=0, director_open=2, total_open=100,
        )
        assert "succession_in_progress" not in profile.flags
        assert profile.transition_score < 50

    def test_no_hiring_scores_zero(self):
        from app.services.exec_signal_scorer import ExecSignalScorer
        db = _make_db()
        scorer = ExecSignalScorer(db)
        profile = scorer._score_company(
            company_id=1, company_name="Quiet Corp",
            csuite_open=0, vp_open=0, director_open=0, total_open=0,
        )
        assert profile.transition_score == 0
        assert profile.flags == []

    # YOUR TEST HERE: define your own transition thresholds
    # def test_vp_heavy_hiring_flags_buildup(self):
    #     """3+ VP openings with no C-suite should flag management_buildup."""
    #     pass


# ===========================================================================
# CHAIN 5 — Unified Site Score
# ===========================================================================

class TestChain5UnifiedSiteScore:
    """Unified Site Scorer — 5 factors, configurable use-case weights."""

    def test_use_case_weights_sum_to_one(self):
        from app.services.unified_site_scorer import USE_CASE_WEIGHTS
        for use_case, weights in USE_CASE_WEIGHTS.items():
            total = sum(weights.values())
            assert abs(total - 1.0) < 0.01, f"{use_case} weights sum to {total}"

    def test_all_use_cases_have_same_keys(self):
        from app.services.unified_site_scorer import USE_CASE_WEIGHTS
        keys = {"power", "climate", "workforce", "connectivity", "regulatory"}
        for use_case, weights in USE_CASE_WEIGHTS.items():
            assert set(weights.keys()) == keys, f"{use_case} has wrong keys"

    def test_datacenter_weights_power_heavy(self):
        """Datacenter should weight power highest."""
        from app.services.unified_site_scorer import USE_CASE_WEIGHTS
        dc = USE_CASE_WEIGHTS["datacenter"]
        assert dc["power"] >= 0.30
        assert dc["power"] > dc["regulatory"]

    def test_warehouse_weights_regulatory_heavy(self):
        """Warehouse should weight regulatory/incentives highest."""
        from app.services.unified_site_scorer import USE_CASE_WEIGHTS
        wh = USE_CASE_WEIGHTS["warehouse"]
        assert wh["regulatory"] >= 0.35
        assert wh["regulatory"] > wh["power"]

    def test_scorer_handles_empty_db(self):
        """Should return a score even with no data."""
        from app.services.unified_site_scorer import UnifiedSiteScorer
        db = _make_db()
        scorer = UnifiedSiteScorer(db)
        result = scorer.score_location(lat=40.0, lng=-74.0)
        assert 0 <= result.score <= 100
        assert result.use_case == "general"

    # YOUR TEST HERE: test specific locations you care about
    # def test_ashburn_va_beats_rural_alabama(self):
    #     """Ashburn VA (DC hub) should outscore a rural location for datacenter."""
    #     pass


# ===========================================================================
# CHAIN 6 — Portfolio Macro Stress
# ===========================================================================

class TestChain6PortfolioStress:
    """Portfolio Macro Stress Scorer — per-holding stress."""

    def test_industry_to_sector_mapping(self):
        """Key industries should map to correct sectors."""
        from app.services.portfolio_stress_scorer import _map_industry_to_sector
        assert _map_industry_to_sector("Software") == "technology"
        assert _map_industry_to_sector("Software - Infrastructure") == "technology"
        assert _map_industry_to_sector("Biotechnology") == "healthcare"
        assert _map_industry_to_sector("Healthcare Services") == "healthcare"
        assert _map_industry_to_sector("Oil & Gas Midstream") == "energy"
        assert _map_industry_to_sector("Oil & Gas E&P") == "energy"
        assert _map_industry_to_sector("Industrials") == "industrials"
        assert _map_industry_to_sector("Credit Services") == "financial"
        assert _map_industry_to_sector("Consumer Retail") == "consumer"
        assert _map_industry_to_sector("Integrated Freight & Logistics") == "logistics"

    def test_unknown_industry_defaults_to_industrials(self):
        from app.services.portfolio_stress_scorer import _map_industry_to_sector
        assert _map_industry_to_sector("") == "industrials"
        assert _map_industry_to_sector("Miscellaneous") == "industrials"

    def test_stress_grades(self):
        """Stress grade thresholds: A < 25, B < 50, C < 75, D >= 75."""
        from app.services.portfolio_stress_scorer import STRESS_GRADES
        # Grade D starts at 75
        assert STRESS_GRADES[0] == (75, "D")
        # Grade A is below 25
        assert STRESS_GRADES[-1] == (0, "A")

    def test_high_leverage_high_stress(self):
        """A company with extreme leverage should have high rate stress."""
        from app.services.portfolio_stress_scorer import PortfolioStressScorer
        db = _make_db({
            "pe_portfolio_companies": [(1, "Leveraged Corp", "Technology", None, "CA")],
            "pe_company_financials": [(None, 3.0, 50.0, 0.5, None)],  # 50x leverage, 0.5x coverage
        })
        scorer = PortfolioStressScorer(db)
        scorer._sector_scores = {"technology": 90}
        scorer._macro = {"fed_funds_rate": 5.0, "cpi_yoy_pct": 3.0, "energy_cost_yoy_pct": 0}
        result = scorer._score_single(1, "Leveraged Corp", "Technology", None, "CA")
        # Rate stress should be maxed out
        rate_component = result.components[0]
        assert rate_component.stress >= 80, f"Expected high rate stress, got {rate_component.stress}"

    # YOUR TEST HERE: test your own portfolio companies
    # def test_my_portfolio_company_stress(self):
    #     """Test stress for a specific company you're tracking."""
    #     pass


# ===========================================================================
# CHAIN 7 — Healthcare Practice Profiles
# ===========================================================================

class TestChain7HealthcarePractice:
    """Healthcare Practice Profile Scorer — 5-factor acquisition scoring."""

    def test_grade_thresholds(self):
        from app.services.healthcare_practice_scorer import GRADE_THRESHOLDS
        # A starts at 85
        assert GRADE_THRESHOLDS[0] == (85, "A")

    def test_physician_oversight_boosts_credibility(self):
        """Practice with physician + NPPES should score 95 on clinical."""
        from app.services.healthcare_practice_scorer import HealthcarePracticeScorer
        # Build a mock row matching _build_profile's expected tuple
        mock_row = (
            1, "Dr. Smith Med Spa", "Beverly Hills", "CA", "90210",
            4.8, 200, 90.0, "A",        # rating, reviews, acq_score, grade
            95.0, 0.9,                    # zip_score, zip_affluence
            True, 5,                      # has_physician, nppes_count
            2000000, 2, "Independent",    # revenue, locations, ownership
            3, 0.5,                       # competitors, saturation
            None, None,                   # review_velocity, rating_trend
            True, True, True, False,      # botox, fillers, laser, coolsculpting
        )
        db = _make_db()
        scorer = HealthcarePracticeScorer(db)
        profile = scorer._build_profile(mock_row)
        clinical = next(f for f in profile.factors if f.factor == "Clinical credibility")
        assert clinical.score == 95

    def test_no_physician_low_credibility(self):
        """Practice with no physician and no NPPES should score 25."""
        from app.services.healthcare_practice_scorer import HealthcarePracticeScorer
        mock_row = (
            2, "No Doc Spa", "Anytown", "TX", "75001",
            3.5, 20, 50.0, "C",
            40.0, 0.3,
            False, 0,       # no physician, no NPPES
            500000, 1, "Independent",
            5, 0.8,
            None, None,
            False, False, False, False,
        )
        db = _make_db()
        scorer = HealthcarePracticeScorer(db)
        profile = scorer._build_profile(mock_row)
        clinical = next(f for f in profile.factors if f.factor == "Clinical credibility")
        assert clinical.score == 25

    def test_multi_location_high_score(self):
        """Practice with 5+ locations should score 95 on multi-unit."""
        from app.services.healthcare_practice_scorer import HealthcarePracticeScorer
        mock_row = (
            3, "Chain Spa", "NYC", "NY", "10001",
            4.5, 500, 80.0, "B",
            80.0, 0.7,
            True, 3,
            3000000, 10, "Multi-Site",    # 10 locations
            2, 0.3,
            None, None,
            True, True, True, True,
        )
        db = _make_db()
        scorer = HealthcarePracticeScorer(db)
        profile = scorer._build_profile(mock_row)
        multi = next(f for f in profile.factors if f.factor == "Multi-unit potential")
        assert multi.score == 95

    # YOUR TEST HERE: test practices you're evaluating
    # def test_my_target_practice(self):
    #     """Score a specific practice you're considering for acquisition."""
    #     pass


# ===========================================================================
# CHAIN 8 — Roll-Up Market Attractiveness
# ===========================================================================

class TestChain8RollUpMarket:
    """Roll-Up Market Scorer — county-level scoring."""

    def test_scorer_exists(self):
        """RollupMarketScorer should be importable."""
        from app.ml.rollup_market_scorer import RollupMarketScorer
        assert RollupMarketScorer is not None

    def test_fragmentation_scorer_exists(self):
        """PE Fragmentation scorer should be importable."""
        from app.core.pe_fragmentation import FragmentationScorer
        assert FragmentationScorer is not None

    # YOUR TEST HERE: test a specific NAICS you're interested in
    # def test_dental_roll_up_rankings(self):
    #     """NAICS 621210 (dental offices) should return county rankings."""
    #     pass


# ===========================================================================
# CROSS-CHAIN CONSISTENCY
# ===========================================================================

class TestCrossChainConsistency:
    """Verify chains agree with each other."""

    def test_sector_mapping_consistency(self):
        """Chain 1 sectors should match Chain 6 industry mapper."""
        from app.services.deal_environment_scorer import SECTOR_CONFIGS
        from app.services.portfolio_stress_scorer import _map_industry_to_sector

        # Every mapped industry should resolve to a valid Chain 1 sector
        test_industries = [
            "Software", "Healthcare", "Oil & Gas", "Manufacturing",
            "Consumer Retail", "Financial Services", "Real Estate",
            "Logistics", "Automotive",
        ]
        for ind in test_industries:
            sector = _map_industry_to_sector(ind)
            assert sector in SECTOR_CONFIGS, f"'{ind}' mapped to '{sector}' which isn't in SECTOR_CONFIGS"

    def test_grade_thresholds_consistent(self):
        """Chain 1 and Chain 3 should use same grade scale."""
        from app.services.deal_environment_scorer import GRADE_THRESHOLDS as C1_GRADES
        from app.services.gp_pipeline_scorer import GRADE_THRESHOLDS as C3_GRADES
        # Both should have A=80, B=65, C=50, D=0
        assert C1_GRADES == C3_GRADES

    # YOUR TEST HERE: verify your own cross-chain expectations
    # def test_energy_stress_matches_deal_score(self):
    #     """If energy deal score is low (Chain 1), energy stress should be high (Chain 6)."""
    #     pass


# ===========================================================================
# INTEGRATION TESTS (require live DB + API)
# ===========================================================================

@pytest.mark.integration
class TestIntegrationAllChains:
    """
    Live integration tests — run against a real database.

    Run with: RUN_INTEGRATION_TESTS=true pytest tests/test_plan_052_signal_chains.py -v -k integration
    """

    @pytest.fixture
    def db(self):
        from app.core.database import get_session_factory
        session = get_session_factory()()
        yield session
        session.close()

    def test_chain1_live_scores(self, db):
        from app.services.deal_environment_scorer import DealEnvironmentScorer
        scorer = DealEnvironmentScorer(db)
        results = scorer.score_all_sectors()
        assert len(results) == 9
        # Healthcare should be top tier
        hc = next(r for r in results if r.sector == "healthcare")
        assert hc.score >= 70, f"Healthcare scored {hc.score}, expected >= 70"

    def test_chain2_live_known_company(self, db):
        from app.services.company_diligence_scorer import CompanyDiligenceScorer
        scorer = CompanyDiligenceScorer(db)
        result = scorer.score_company("Air Products")
        assert result.confidence > 0, "Should match at least one source"
        assert len(result.red_flags) > 0, "Air Products should have EPA red flags"

    def test_chain3_live_gp_scores(self, db):
        from app.services.gp_pipeline_scorer import GPPipelineScorer
        scorer = GPPipelineScorer(db)
        results = scorer.score_all_gps()
        assert len(results) > 0, "Should score at least some GPs"
        top = results[0]
        assert top.score > 50, f"Top GP scored {top.score}, expected > 50"

    def test_chain5_live_location(self, db):
        from app.services.unified_site_scorer import UnifiedSiteScorer
        scorer = UnifiedSiteScorer(db)
        result = scorer.score_location(lat=40.7128, lng=-74.006)
        assert result.score > 0
        assert result.coverage > 0, "Should have data for at least one factor"

    def test_chain6_live_portfolio(self, db):
        from app.services.portfolio_stress_scorer import PortfolioStressScorer
        scorer = PortfolioStressScorer(db)
        report = scorer.score_portfolio(firm_id=1)  # Blackstone
        assert report.holdings_scored > 0
        assert 0 <= report.portfolio_stress <= 100

    def test_chain7_live_healthcare(self, db):
        from app.services.healthcare_practice_scorer import HealthcarePracticeScorer
        scorer = HealthcarePracticeScorer(db)
        results = scorer.screen(limit=5)
        assert len(results) > 0
        top = results[0]
        assert top.acquisition_score > 50

    # YOUR INTEGRATION TEST HERE
    # def test_my_company_diligence(self, db):
    #     """Test a company you're evaluating."""
    #     from app.services.company_diligence_scorer import CompanyDiligenceScorer
    #     scorer = CompanyDiligenceScorer(db)
    #     result = scorer.score_company("YOUR COMPANY NAME")
    #     assert result.score > 0
