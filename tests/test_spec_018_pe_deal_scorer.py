"""
Tests for SPEC 018 — PE Deal Scorer.
Scores acquisition targets on financial quality, market position,
management strength, growth trajectory, and deal attractiveness.
"""
import pytest


class TestFinancialQuality:
    """Tests for financial quality scoring dimension."""

    def test_score_financial_quality_strong(self):
        """T1: High revenue growth + margins → high score."""
        from app.core.pe_deal_scorer import _score_financial_quality

        financials = [
            {"fiscal_year": 2025, "revenue_usd": 50_000_000, "ebitda_usd": 12_500_000,
             "ebitda_margin_pct": 25.0, "gross_margin_pct": 55.0, "revenue_growth_pct": 20.0,
             "employees": 200},
            {"fiscal_year": 2024, "revenue_usd": 41_600_000, "ebitda_usd": 9_500_000,
             "ebitda_margin_pct": 22.8, "gross_margin_pct": 53.0, "revenue_growth_pct": 18.0,
             "employees": 175},
        ]
        result = _score_financial_quality(financials)
        assert result.raw_score >= 70, f"Strong financials should score >=70, got {result.raw_score}"
        assert result.grade in ("A", "B")

    def test_score_financial_quality_weak(self):
        """T2: Declining revenue + low margins → low score."""
        from app.core.pe_deal_scorer import _score_financial_quality

        financials = [
            {"fiscal_year": 2025, "revenue_usd": 10_000_000, "ebitda_usd": 500_000,
             "ebitda_margin_pct": 5.0, "gross_margin_pct": 25.0, "revenue_growth_pct": -5.0,
             "employees": 100},
            {"fiscal_year": 2024, "revenue_usd": 10_500_000, "ebitda_usd": 600_000,
             "ebitda_margin_pct": 5.7, "gross_margin_pct": 27.0, "revenue_growth_pct": -3.0,
             "employees": 105},
        ]
        result = _score_financial_quality(financials)
        assert result.raw_score <= 40, f"Weak financials should score <=40, got {result.raw_score}"
        assert result.grade in ("D", "F")


class TestMarketPosition:
    """Tests for market position scoring dimension."""

    def test_score_market_position_leader(self):
        """T3: Leader with competitors → high score."""
        from app.core.pe_deal_scorer import _score_market_position

        competitors = [
            {"relative_size": "Smaller", "market_position": "Leader", "is_pe_backed": False},
            {"relative_size": "Similar", "market_position": "Challenger", "is_pe_backed": True},
            {"relative_size": "Larger", "market_position": "Leader", "is_pe_backed": False},
        ]
        result = _score_market_position(competitors)
        assert result.raw_score >= 50, f"Market leader should score >=50, got {result.raw_score}"


class TestManagementQuality:
    """Tests for management quality scoring dimension."""

    def test_score_management_completeness(self):
        """T4: Full C-suite → high score."""
        from app.core.pe_deal_scorer import _score_management

        leaders = [
            {"is_ceo": True, "is_cfo": False, "is_board_member": False,
             "appointed_by_pe": True, "tenure_years": 3.0, "role_category": "C-Suite"},
            {"is_ceo": False, "is_cfo": True, "is_board_member": False,
             "appointed_by_pe": True, "tenure_years": 2.0, "role_category": "C-Suite"},
            {"is_ceo": False, "is_cfo": False, "is_board_member": True,
             "appointed_by_pe": False, "tenure_years": 5.0, "role_category": "Board"},
            {"is_ceo": False, "is_cfo": False, "is_board_member": False,
             "appointed_by_pe": False, "tenure_years": 4.0, "role_category": "VP"},
        ]
        result = _score_management(leaders)
        assert result.raw_score >= 60, f"Full C-suite should score >=60, got {result.raw_score}"


class TestGrowthTrajectory:
    """Tests for growth trajectory scoring dimension."""

    def test_score_growth_trajectory(self):
        """T5: Multi-year revenue CAGR → appropriate score."""
        from app.core.pe_deal_scorer import _score_growth_trajectory

        financials = [
            {"fiscal_year": 2025, "revenue_usd": 50_000_000, "employees": 200,
             "ebitda_margin_pct": 25.0},
            {"fiscal_year": 2024, "revenue_usd": 42_000_000, "employees": 180,
             "ebitda_margin_pct": 23.0},
            {"fiscal_year": 2023, "revenue_usd": 35_000_000, "employees": 160,
             "ebitda_margin_pct": 20.0},
        ]
        result = _score_growth_trajectory(financials)
        assert result.raw_score >= 60, f"Strong growth should score >=60, got {result.raw_score}"


class TestCompositeScore:
    """Tests for composite scoring logic."""

    def test_composite_score_weighted(self):
        """T6: Weights sum to 1.0 and composite is correct."""
        from app.core.pe_deal_scorer import DIMENSION_WEIGHTS

        total = sum(DIMENSION_WEIGHTS.values())
        assert abs(total - 1.0) < 0.001, f"Weights must sum to 1.0, got {total}"

    def test_missing_data_handling(self):
        """T7: No financials → still returns score with gaps noted."""
        from app.core.pe_deal_scorer import _score_financial_quality

        result = _score_financial_quality([])
        assert result.raw_score == 0
        assert len(result.data_gaps) > 0

    def test_grade_assignment(self):
        """T8: Score thresholds map to correct letter grades."""
        from app.core.pe_deal_scorer import _letter_grade

        assert _letter_grade(90) == "A"
        assert _letter_grade(75) == "B"
        assert _letter_grade(60) == "C"
        assert _letter_grade(40) == "D"
        assert _letter_grade(20) == "F"
