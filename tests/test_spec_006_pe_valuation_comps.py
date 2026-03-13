"""
Tests for SPEC 006 — PE Valuation Comps & EBITDA Multiple Benchmarking.
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from decimal import Decimal


class TestMultipleComputation:
    """Tests for ValuationCompsService multiple calculations."""

    def test_compute_multiples(self):
        """T1: EV/Revenue and EV/EBITDA calculated correctly."""
        from app.core.pe_valuation_comps import ValuationCompsService

        result = ValuationCompsService._compute_multiples(
            enterprise_value=100_000_000,
            revenue=20_000_000,
            ebitda=5_000_000,
        )
        assert result["ev_revenue"] == 5.0
        assert result["ev_ebitda"] == 20.0

    def test_compute_multiples_zero_ebitda(self):
        """EV/EBITDA should be None when EBITDA is zero or negative."""
        from app.core.pe_valuation_comps import ValuationCompsService

        result = ValuationCompsService._compute_multiples(
            enterprise_value=100_000_000,
            revenue=20_000_000,
            ebitda=0,
        )
        assert result["ev_revenue"] == 5.0
        assert result["ev_ebitda"] is None


class TestPeerStats:
    """Tests for peer set statistics."""

    def test_peer_percentile_rank(self):
        """T2: Company ranked correctly among peers."""
        from app.core.pe_valuation_comps import ValuationCompsService

        # Company at 12x EBITDA, peers at 8, 10, 14, 16
        peer_multiples = [8.0, 10.0, 14.0, 16.0]
        company_multiple = 12.0

        rank = ValuationCompsService._percentile_rank(company_multiple, peer_multiples)
        assert 40 <= rank <= 60  # should be around 50th percentile

    def test_peer_stats(self):
        """T6: Median, P25, P75 computed correctly."""
        from app.core.pe_valuation_comps import ValuationCompsService

        multiples = [8.0, 10.0, 12.0, 14.0, 16.0]
        stats = ValuationCompsService._compute_peer_stats(multiples)

        assert stats["median"] == 12.0
        assert stats["p25"] == 10.0
        assert stats["p75"] == 14.0
        assert stats["count"] == 5

    def test_no_peers(self):
        """T3: Graceful response when no peers found."""
        from app.core.pe_valuation_comps import ValuationCompsService

        stats = ValuationCompsService._compute_peer_stats([])
        assert stats["median"] is None
        assert stats["count"] == 0

    def test_missing_financials(self):
        """T4: Handles company with no financial data."""
        from app.core.pe_valuation_comps import ValuationCompsService

        result = ValuationCompsService._compute_multiples(
            enterprise_value=None, revenue=None, ebitda=None,
        )
        assert result["ev_revenue"] is None
        assert result["ev_ebitda"] is None


class TestValuationCompsEndpoint:
    """Tests for API endpoint responses."""

    @pytest.mark.asyncio
    async def test_endpoint_response(self):
        """T5: API returns correct schema."""
        from app.api.v1.pe_benchmarks import get_valuation_comps

        mock_service = MagicMock()
        mock_service.get_comps.return_value = {
            "company_id": 1,
            "company_name": "TestCo",
            "industry": "Healthcare",
            "ev_revenue": 5.0,
            "ev_ebitda": 12.0,
            "peer_ev_revenue": {"median": 4.5, "p25": 3.0, "p75": 6.0, "count": 5},
            "peer_ev_ebitda": {"median": 11.0, "p25": 9.0, "p75": 14.0, "count": 5},
            "ev_revenue_percentile": 60,
            "ev_ebitda_percentile": 55,
            "peer_companies": [],
        }

        db = MagicMock()
        with patch("app.core.pe_valuation_comps.ValuationCompsService", return_value=mock_service):
            result = await get_valuation_comps(company_id=1, db=db)

        assert result.company_name == "TestCo"
        assert result.ev_ebitda == 12.0
        assert result.peer_ev_ebitda["median"] == 11.0
