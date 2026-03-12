"""
Tests for SPEC 004 — PE Industry Fragmentation Scorer.

Tests FragmentationScorer scoring logic, market ranking, multi-NAICS scan,
and API endpoint responses with mocked CBP data.
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

# ---------------------------------------------------------------------------
# Helpers — mock CBP county records
# ---------------------------------------------------------------------------

def _make_cbp_record(
    county_fips="06037",
    state_fips="06",
    geo_name="Los Angeles County, CA",
    establishments=500,
    employees=3000,
    hhi=0.05,
    small_biz_pct=0.85,
    avg_employees_per_estab=6.0,
    naics_code="621111",
    year=2021,
):
    return {
        "county_fips": county_fips,
        "state_fips": state_fips,
        "geo_name": geo_name,
        "establishments": establishments,
        "employees": employees,
        "hhi": hhi,
        "small_biz_pct": small_biz_pct,
        "avg_employees_per_estab": avg_employees_per_estab,
        "naics_code": naics_code,
        "year": year,
    }


# ---------------------------------------------------------------------------
# Scoring logic
# ---------------------------------------------------------------------------


class TestFragmentationScoring:
    """Tests for FragmentationScorer._compute_score static method."""

    def test_compute_fragmentation_score_high(self):
        """T1: High fragmentation (many small firms) → score > 70."""
        from app.core.pe_fragmentation import FragmentationScorer

        # Low HHI, high small biz %, small avg size → highly fragmented
        score = FragmentationScorer._compute_score(
            hhi=0.03, small_biz_pct=0.92, avg_size=5.0
        )
        assert score > 70, f"Expected >70 for highly fragmented market, got {score}"

    def test_compute_fragmentation_score_low(self):
        """T2: Low fragmentation (few large firms) → score < 40."""
        from app.core.pe_fragmentation import FragmentationScorer

        # High HHI, low small biz %, large avg size → concentrated
        score = FragmentationScorer._compute_score(
            hhi=0.40, small_biz_pct=0.15, avg_size=200.0
        )
        assert score < 40, f"Expected <40 for concentrated market, got {score}"

    def test_compute_fragmentation_score_empty(self):
        """T3: No data → returns 0."""
        from app.core.pe_fragmentation import FragmentationScorer

        score = FragmentationScorer._compute_score(
            hhi=None, small_biz_pct=None, avg_size=None
        )
        assert score == 0

    def test_single_county_edge_case(self):
        """T7: Single county with valid data produces valid score."""
        from app.core.pe_fragmentation import FragmentationScorer

        score = FragmentationScorer._compute_score(
            hhi=0.10, small_biz_pct=0.70, avg_size=12.0
        )
        assert 0 <= score <= 100

    def test_score_bounds(self):
        """Score is always clamped between 0 and 100."""
        from app.core.pe_fragmentation import FragmentationScorer

        # Extreme high fragmentation
        s1 = FragmentationScorer._compute_score(hhi=0.001, small_biz_pct=0.99, avg_size=1.5)
        assert 0 <= s1 <= 100

        # Extreme concentration
        s2 = FragmentationScorer._compute_score(hhi=1.0, small_biz_pct=0.0, avg_size=5000.0)
        assert 0 <= s2 <= 100

    def test_score_monotonicity_hhi(self):
        """Lower HHI should produce higher fragmentation score (all else equal)."""
        from app.core.pe_fragmentation import FragmentationScorer

        s_low = FragmentationScorer._compute_score(hhi=0.05, small_biz_pct=0.80, avg_size=10.0)
        s_high = FragmentationScorer._compute_score(hhi=0.30, small_biz_pct=0.80, avg_size=10.0)
        assert s_low > s_high


# ---------------------------------------------------------------------------
# Market ranking
# ---------------------------------------------------------------------------


class TestMarketRanking:
    """Tests for ranking markets by fragmentation."""

    def test_rank_markets_by_fragmentation(self):
        """T4: Counties ranked descending by fragmentation score."""
        from app.core.pe_fragmentation import FragmentationScorer

        records = [
            _make_cbp_record(county_fips="06037", establishments=500, hhi=0.03, small_biz_pct=0.90, avg_employees_per_estab=5.0),
            _make_cbp_record(county_fips="36061", establishments=100, hhi=0.25, small_biz_pct=0.40, avg_employees_per_estab=50.0),
            _make_cbp_record(county_fips="17031", establishments=300, hhi=0.08, small_biz_pct=0.75, avg_employees_per_estab=12.0),
        ]

        scorer = FragmentationScorer.__new__(FragmentationScorer)
        ranked = scorer._rank_markets(records)

        assert len(ranked) == 3
        # Most fragmented (lowest HHI, highest small_biz) first
        assert ranked[0]["county_fips"] == "06037"
        # Scores descending
        assert ranked[0]["score"] >= ranked[1]["score"] >= ranked[2]["score"]

    def test_national_aggregation(self):
        """T5: National score aggregates county data correctly."""
        from app.core.pe_fragmentation import FragmentationScorer

        records = [
            _make_cbp_record(county_fips="06037", establishments=500, employees=3000, hhi=0.05, small_biz_pct=0.85, avg_employees_per_estab=6.0),
            _make_cbp_record(county_fips="36061", establishments=200, employees=2000, hhi=0.10, small_biz_pct=0.70, avg_employees_per_estab=10.0),
        ]

        scorer = FragmentationScorer.__new__(FragmentationScorer)
        result = scorer._aggregate_national(records, "621111")

        assert result["naics_code"] == "621111"
        assert result["total_establishments"] == 700
        assert result["total_employees"] == 5000
        assert result["county_count"] == 2
        assert 0 <= result["national_score"] <= 100


# ---------------------------------------------------------------------------
# Multi-NAICS scan
# ---------------------------------------------------------------------------


class TestScanEndpoint:
    """Tests for multi-NAICS scan."""

    @pytest.mark.asyncio
    async def test_scan_multiple_naics(self):
        """T6: Scan ranks multiple NAICS codes by fragmentation."""
        from app.core.pe_fragmentation import FragmentationScorer

        db = MagicMock()
        scorer = FragmentationScorer(db)

        # Mock score_industry to return different scores
        async def mock_score(naics, year=2021):
            scores = {
                "621111": {"naics_code": "621111", "national_score": 85, "total_establishments": 500},
                "541330": {"naics_code": "541330", "national_score": 45, "total_establishments": 200},
                "238220": {"naics_code": "238220", "national_score": 72, "total_establishments": 800},
            }
            return scores.get(naics, {"naics_code": naics, "national_score": 0, "total_establishments": 0})

        scorer.score_industry = mock_score

        results = await scorer.scan_industries(["621111", "541330", "238220"])

        assert len(results) == 3
        # Ranked descending by score
        assert results[0]["naics_code"] == "621111"
        assert results[1]["naics_code"] == "238220"
        assert results[2]["naics_code"] == "541330"

    @pytest.mark.asyncio
    async def test_scan_endpoint_empty_naics_list(self):
        """T9: Empty NAICS list returns empty results."""
        from app.core.pe_fragmentation import FragmentationScorer

        db = MagicMock()
        scorer = FragmentationScorer(db)

        results = await scorer.scan_industries([])
        assert results == []


# ---------------------------------------------------------------------------
# API endpoint response schema
# ---------------------------------------------------------------------------


class TestFragmentationEndpoint:
    """Tests for API endpoint responses."""

    @pytest.mark.asyncio
    @patch("app.core.pe_fragmentation.FragmentationScorer")
    async def test_fragmentation_endpoint_returns_valid_response(self, MockScorer):
        """T8: API endpoint returns correct schema."""
        from app.api.v1.pe_benchmarks import get_fragmentation_score

        mock_scorer = MagicMock()
        mock_scorer.score_industry = AsyncMock(return_value={
            "naics_code": "621111",
            "naics_description": "Offices of Physicians",
            "national_score": 82,
            "national_grade": "A",
            "total_establishments": 5000,
            "total_employees": 30000,
            "county_count": 150,
            "top_markets": [],
        })
        MockScorer.return_value = mock_scorer

        # Call endpoint directly — it imports FragmentationScorer locally,
        # so we patch at source and pass the mock via dependency injection
        db = MagicMock()
        with patch("app.core.pe_fragmentation.FragmentationScorer", return_value=mock_scorer):
            result = await get_fragmentation_score("621111", 2021, 20, db)

        assert result.naics_code == "621111"
        assert result.national_score == 82
        assert result.national_grade == "A"


# ---------------------------------------------------------------------------
# Roll-up targets
# ---------------------------------------------------------------------------


class TestRollUpTargets:
    """Tests for roll-up target discovery."""

    def test_roll_up_targets_filters_by_state(self):
        """T10: Roll-up targets filtered to correct state."""
        from app.core.pe_fragmentation import FragmentationScorer

        records = [
            _make_cbp_record(county_fips="06037", state_fips="06", geo_name="Los Angeles, CA", establishments=500),
            _make_cbp_record(county_fips="06059", state_fips="06", geo_name="Orange, CA", establishments=300),
            _make_cbp_record(county_fips="36061", state_fips="36", geo_name="New York, NY", establishments=400),
        ]

        scorer = FragmentationScorer.__new__(FragmentationScorer)
        targets = scorer._filter_by_state(records, "06")

        assert len(targets) == 2
        assert all(t["state_fips"] == "06" for t in targets)
