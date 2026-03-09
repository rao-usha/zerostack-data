"""Unit tests for Roll-Up Intelligence (scorer, CBP collector, add-on finder)."""

import pytest
from datetime import date
from unittest.mock import MagicMock, patch

from app.sources.rollup_intel.metadata import (
    NAICS_DESCRIPTIONS,
    ROLLUP_WEIGHTS,
    GRADE_THRESHOLDS,
    generate_create_census_cbp_sql,
    generate_create_rollup_scores_sql,
)
from app.sources.rollup_intel.cbp_collector import CBPCollector, _safe_int
from app.ml.rollup_market_scorer import RollupMarketScorer
from app.ml.addon_target_finder import AddonTargetFinder, STATE_ADJACENCY


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute.return_value.mappings.return_value.fetchall.return_value = []
    db.execute.return_value.scalar.return_value = 0
    return db


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------

class TestMetadata:
    def test_naics_descriptions_not_empty(self):
        assert len(NAICS_DESCRIPTIONS) >= 20

    def test_weights_sum_to_one(self):
        assert abs(sum(ROLLUP_WEIGHTS.values()) - 1.0) < 0.001

    def test_grade_thresholds_descending(self):
        thresholds = [t for t, _ in GRADE_THRESHOLDS]
        assert thresholds == sorted(thresholds, reverse=True)

    def test_cbp_ddl_contains_table(self):
        sql = generate_create_census_cbp_sql()
        assert "census_cbp" in sql
        assert "UNIQUE" in sql

    def test_scores_ddl_contains_table(self):
        sql = generate_create_rollup_scores_sql()
        assert "rollup_market_scores" in sql
        assert "overall_score" in sql


# ---------------------------------------------------------------------------
# CBPCollector tests
# ---------------------------------------------------------------------------

class TestCBPCollector:
    @patch("app.sources.rollup_intel.cbp_collector.CBPCollector._ensure_tables")
    def test_safe_int(self, _):
        assert _safe_int("123") == 123
        assert _safe_int(None) is None
        assert _safe_int("N") is None
        assert _safe_int("") is None

    @patch("app.sources.rollup_intel.cbp_collector.CBPCollector._ensure_tables")
    def test_compute_derived_basic(self, _, mock_db):
        collector = CBPCollector(mock_db)
        records = [
            {
                "establishments": 100,
                "employees": 500,
                "annual_payroll_thousands": 15000,
            }
        ]
        result = collector._compute_derived(records)
        assert result[0]["avg_employees_per_estab"] == 5.0
        assert result[0]["small_biz_pct"] is not None

    @patch("app.sources.rollup_intel.cbp_collector.CBPCollector._ensure_tables")
    def test_compute_derived_zero_estab(self, _, mock_db):
        collector = CBPCollector(mock_db)
        records = [{"establishments": 0, "employees": 0, "annual_payroll_thousands": 0}]
        result = collector._compute_derived(records)
        assert result[0]["avg_employees_per_estab"] is None

    @patch("app.sources.rollup_intel.cbp_collector.CBPCollector._ensure_tables")
    def test_compute_derived_with_size_classes(self, _, mock_db):
        collector = CBPCollector(mock_db)
        records = [{
            "establishments": 100, "employees": 500,
            "annual_payroll_thousands": 15000,
            "estab_1_4": 40, "estab_5_9": 25, "estab_10_19": 15,
            "estab_20_49": 10, "estab_50_99": 5, "estab_100_249": 3,
            "estab_250_plus": 2,
        }]
        result = collector._compute_derived(records)
        assert result[0]["small_biz_pct"] == 0.9  # (40+25+15+10)/100
        assert result[0]["hhi"] is not None
        assert result[0]["hhi"] < 1.0

    @patch("app.sources.rollup_intel.cbp_collector.CBPCollector._ensure_tables")
    def test_get_cached_empty(self, _, mock_db):
        collector = CBPCollector(mock_db)
        result = collector.get_cached("621111", year=2021)
        assert result == []


# ---------------------------------------------------------------------------
# RollupMarketScorer tests
# ---------------------------------------------------------------------------

class TestRollupMarketScorer:
    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_percentile_rank_basic(self, _):
        result = RollupMarketScorer._percentile_rank([10, 20, 30])
        assert result[0] == 0.0
        assert result[1] == 50.0
        assert result[2] == 100.0

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_percentile_rank_empty(self, _):
        assert RollupMarketScorer._percentile_rank([]) == []

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_percentile_rank_single(self, _):
        result = RollupMarketScorer._percentile_rank([42])
        assert result == [50.0]

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_get_grade(self, _):
        assert RollupMarketScorer._get_grade(85) == "A"
        assert RollupMarketScorer._get_grade(70) == "B"
        assert RollupMarketScorer._get_grade(55) == "C"
        assert RollupMarketScorer._get_grade(40) == "D"
        assert RollupMarketScorer._get_grade(10) == "F"

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_methodology_structure(self, _):
        method = RollupMarketScorer.get_methodology()
        assert "sub_scores" in method
        assert len(method["sub_scores"]) == 5
        assert "grade_thresholds" in method

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_get_rankings_empty(self, _, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        mock_db.execute.return_value.scalar.return_value = 0
        scorer = RollupMarketScorer(mock_db)
        result = scorer.get_rankings("621111")
        assert result["total_matching"] == 0

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_get_market_not_found(self, _, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchone.return_value = None
        scorer = RollupMarketScorer(mock_db)
        result = scorer.get_market("621111", "99999")
        assert "error" in result

    @patch("app.ml.rollup_market_scorer.RollupMarketScorer._ensure_tables")
    def test_score_markets_no_cbp(self, _, mock_db):
        # First call: count cache = 0, second call: empty CBP data
        mock_db.execute.return_value.scalar.return_value = 0
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        scorer = RollupMarketScorer(mock_db)
        result = scorer.score_markets("621111", force=True)
        assert "error" in result


# ---------------------------------------------------------------------------
# AddonTargetFinder tests
# ---------------------------------------------------------------------------

class TestAddonTargetFinder:
    def test_state_adjacency_structure(self):
        # Spot check
        assert "12" in STATE_ADJACENCY["01"]  # AL borders FL
        assert "48" in STATE_ADJACENCY["22"]  # LA borders TX
        assert "06" in STATE_ADJACENCY["32"]  # NV borders CA

    def test_adjacency_is_bidirectional(self):
        """If A is adjacent to B, B should be adjacent to A."""
        for state, neighbors in STATE_ADJACENCY.items():
            for neighbor in neighbors:
                assert state in STATE_ADJACENCY.get(neighbor, []), (
                    f"{state} lists {neighbor} but {neighbor} doesn't list {state}"
                )

    def test_get_target_states_radius_0(self, mock_db):
        finder = AddonTargetFinder(mock_db)
        states = finder._get_target_states("06", radius=0)
        assert states == {"06"}

    def test_get_target_states_radius_1(self, mock_db):
        finder = AddonTargetFinder(mock_db)
        states = finder._get_target_states("06", radius=1)
        assert "06" in states  # self
        assert "04" in states  # AZ
        assert "32" in states  # NV
        assert "41" in states  # OR

    def test_get_target_states_radius_2(self, mock_db):
        finder = AddonTargetFinder(mock_db)
        states = finder._get_target_states("06", radius=2)
        assert "06" in states
        # 2 hops from CA should include CO (via AZ), WA (via OR), ID (via OR/NV)
        assert "53" in states  # WA (OR neighbor)

    def test_find_targets_empty(self, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        finder = AddonTargetFinder(mock_db)
        result = finder.find_targets("621111", "06")
        assert result["total_targets"] == 0

    def test_find_targets_proximity_scoring(self, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "naics_code": "621111", "county_fips": "06037",
                "state_fips": "06", "geo_name": "Los Angeles",
                "overall_score": 85.0, "grade": "A",
                "id": 1, "score_date": date.today(), "data_year": 2021,
                "naics_description": "test", "fragmentation_score": 80,
                "market_size_score": 90, "affluence_score": 85,
                "growth_score": 70, "labor_score": 75,
                "establishment_count": 100, "hhi": 0.05,
                "small_biz_pct": 0.8, "avg_estab_size": 5.0,
                "total_employees": 500, "total_payroll_thousands": 15000,
                "avg_agi": 65000, "pct_returns_100k_plus": 0.35,
                "total_returns": 50000, "national_rank": 1,
                "state_rank": 1, "model_version": "v1.0",
            },
            {
                "naics_code": "621111", "county_fips": "04013",
                "state_fips": "04", "geo_name": "Maricopa",
                "overall_score": 75.0, "grade": "B",
                "id": 2, "score_date": date.today(), "data_year": 2021,
                "naics_description": "test", "fragmentation_score": 70,
                "market_size_score": 80, "affluence_score": 65,
                "growth_score": 60, "labor_score": 55,
                "establishment_count": 80, "hhi": 0.06,
                "small_biz_pct": 0.75, "avg_estab_size": 6.0,
                "total_employees": 400, "total_payroll_thousands": 12000,
                "avg_agi": 55000, "pct_returns_100k_plus": 0.30,
                "total_returns": 40000, "national_rank": 5,
                "state_rank": 1, "model_version": "v1.0",
            },
        ]
        # Mock the prospect count query to avoid errors
        mock_db.execute.return_value.scalar.return_value = 0

        finder = AddonTargetFinder(mock_db)
        result = finder.find_targets("621111", "06")
        assert result["total_targets"] == 2
        # LA county (same state) should have higher proximity
        la = next(t for t in result["targets"] if t["county_fips"] == "06037")
        az = next(t for t in result["targets"] if t["county_fips"] == "04013")
        assert la["proximity_score"] == 100
        assert az["proximity_score"] == 70
