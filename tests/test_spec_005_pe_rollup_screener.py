"""
Tests for SPEC 005 — PE Roll-Up Market Screener.

Tests target scoring, filtering, market summary, and endpoint responses
with mocked company and financial data.
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock


# ---------------------------------------------------------------------------
# Helpers — mock company/financial objects
# ---------------------------------------------------------------------------

def _make_company(
    id=1,
    name="Acme Health",
    naics_code="621111",
    industry="Healthcare",
    headquarters_state="CA",
    ownership_status="Private",
    employee_count=50,
    current_pe_owner=None,
    revenue=15_000_000,
    revenue_growth_pct=12.0,
):
    """Create a mock company dict as returned by the screener's internal query."""
    return {
        "id": id,
        "name": name,
        "naics_code": naics_code,
        "industry": industry,
        "headquarters_state": headquarters_state,
        "headquarters_city": "Los Angeles",
        "ownership_status": ownership_status,
        "employee_count": employee_count,
        "current_pe_owner": current_pe_owner,
        "website": "https://example.com",
        "founded_year": 2010,
        "revenue_usd": revenue,
        "revenue_growth_pct": revenue_growth_pct,
        "ebitda_margin_pct": 18.0,
    }


# ---------------------------------------------------------------------------
# Target scoring
# ---------------------------------------------------------------------------


class TestTargetScoring:
    """Tests for RollUpScreener._score_target static method."""

    def test_score_target_ideal_size(self):
        """T1: $5-50M revenue independent company scores high."""
        from app.core.pe_rollup_screener import RollUpScreener

        target = _make_company(
            revenue=20_000_000, ownership_status="Private", headquarters_state="CA"
        )
        score = RollUpScreener._score_target(target, target_state="CA")
        assert score > 70, f"Ideal target should score >70, got {score}"

    def test_score_target_pe_backed_penalty(self):
        """T2: PE-backed company scores lower than independent."""
        from app.core.pe_rollup_screener import RollUpScreener

        independent = _make_company(ownership_status="Private", revenue=20_000_000)
        pe_backed = _make_company(ownership_status="PE-Backed", revenue=20_000_000, current_pe_owner="BigFund")

        score_ind = RollUpScreener._score_target(independent)
        score_pe = RollUpScreener._score_target(pe_backed)
        assert score_ind > score_pe, f"Independent ({score_ind}) should score higher than PE-backed ({score_pe})"

    def test_score_target_too_large(self):
        """T3: >$100M company scores low on size fit."""
        from app.core.pe_rollup_screener import RollUpScreener

        large = _make_company(revenue=200_000_000, ownership_status="Private")
        ideal = _make_company(revenue=20_000_000, ownership_status="Private")

        score_large = RollUpScreener._score_target(large)
        score_ideal = RollUpScreener._score_target(ideal)
        assert score_ideal > score_large, f"Ideal size ({score_ideal}) should beat large ({score_large})"

    def test_ranking_order(self):
        """T10: Targets ranked by composite score descending."""
        from app.core.pe_rollup_screener import RollUpScreener

        targets = [
            _make_company(id=1, revenue=200_000_000, ownership_status="PE-Backed"),  # worst
            _make_company(id=2, revenue=20_000_000, ownership_status="Private"),  # best
            _make_company(id=3, revenue=3_000_000, ownership_status="VC-Backed"),  # mid
        ]

        scored = RollUpScreener._rank_targets(targets)
        assert scored[0]["id"] == 2  # best first
        scores = [t["target_score"] for t in scored]
        assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    """Tests for screener filtering."""

    def test_filter_by_state(self):
        """T4: Only companies in specified state returned."""
        from app.core.pe_rollup_screener import RollUpScreener

        companies = [
            _make_company(id=1, headquarters_state="CA"),
            _make_company(id=2, headquarters_state="TX"),
            _make_company(id=3, headquarters_state="CA"),
        ]

        filtered = RollUpScreener._apply_filters(
            companies, state="CA"
        )
        assert len(filtered) == 2
        assert all(c["headquarters_state"] == "CA" for c in filtered)

    def test_filter_exclude_pe_backed(self):
        """T5: PE-backed companies excluded when flag set."""
        from app.core.pe_rollup_screener import RollUpScreener

        companies = [
            _make_company(id=1, ownership_status="Private"),
            _make_company(id=2, ownership_status="PE-Backed"),
            _make_company(id=3, ownership_status="VC-Backed"),
        ]

        filtered = RollUpScreener._apply_filters(
            companies, exclude_pe_backed=True
        )
        assert len(filtered) == 2
        assert all(c["ownership_status"] != "PE-Backed" for c in filtered)

    def test_empty_results(self):
        """T6: NAICS with no companies returns empty list."""
        from app.core.pe_rollup_screener import RollUpScreener

        filtered = RollUpScreener._apply_filters([], state="CA")
        assert filtered == []

    def test_revenue_range_filter(self):
        """T9: Min/max revenue filters work."""
        from app.core.pe_rollup_screener import RollUpScreener

        companies = [
            _make_company(id=1, revenue=2_000_000),
            _make_company(id=2, revenue=20_000_000),
            _make_company(id=3, revenue=80_000_000),
        ]

        filtered = RollUpScreener._apply_filters(
            companies, min_revenue=5_000_000, max_revenue=50_000_000
        )
        assert len(filtered) == 1
        assert filtered[0]["id"] == 2


# ---------------------------------------------------------------------------
# Market summary
# ---------------------------------------------------------------------------


class TestSummary:
    """Tests for market summary aggregation."""

    def test_summary_aggregation(self):
        """T7: Market summary counts and averages correct."""
        from app.core.pe_rollup_screener import RollUpScreener

        targets = [
            _make_company(id=1, revenue=10_000_000, employee_count=30),
            _make_company(id=2, revenue=20_000_000, employee_count=80),
            _make_company(id=3, revenue=50_000_000, employee_count=200),
        ]

        summary = RollUpScreener._build_summary(targets, "621111")
        assert summary["total_targets"] == 3
        assert summary["total_addressable_revenue"] == 80_000_000
        assert summary["avg_revenue"] == pytest.approx(80_000_000 / 3, rel=0.01)
        assert summary["avg_employee_count"] == pytest.approx(310 / 3, rel=0.01)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


class TestScreenerEndpoint:
    """Tests for API endpoint responses."""

    @pytest.mark.asyncio
    async def test_screener_endpoint_response(self):
        """T8: API returns correct schema."""
        from app.api.v1.pe_benchmarks import get_rollup_screener

        mock_screener = MagicMock()
        mock_screener.screen = AsyncMock(return_value={
            "naics_code": "621111",
            "naics_description": "Offices of Physicians",
            "fragmentation_score": 84.5,
            "total_targets": 5,
            "targets": [],
        })

        db = MagicMock()
        with patch("app.core.pe_rollup_screener.RollUpScreener", return_value=mock_screener):
            result = await get_rollup_screener(
                naics_code="621111", state=None, min_revenue=None,
                max_revenue=None, exclude_pe_backed=False, top_n=20, db=db,
            )

        assert result.naics_code == "621111"
        assert result.fragmentation_score == 84.5
