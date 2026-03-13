"""
Tests for SPEC 007 — PE Comparable Transaction Database.
"""
import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from datetime import date


class TestMarketStats:
    """Tests for market statistics computation."""

    def test_compute_market_stats_with_multiples(self):
        """Median, P25, P75, trend computed from deal multiples."""
        from app.core.pe_comparable_transactions import ComparableTransactionService

        ev_ebitda = [8.0, 10.0, 12.0, 14.0, 16.0]
        ev_revenue = [2.0, 3.0, 4.0]
        deals = []
        for m in ev_ebitda:
            d = MagicMock()
            d.enterprise_value_usd = Decimal("100000000")
            d.seller_type = "PE"
            deals.append(d)

        stats = ComparableTransactionService._compute_market_stats(
            ev_ebitda, ev_revenue, deals
        )
        assert stats["ev_ebitda_median"] == 12.0
        assert stats["ev_revenue_median"] == 3.0
        assert stats["total_deals"] == 5
        assert stats["deals_with_multiples"] == 5

    def test_compute_market_stats_empty(self):
        """Empty deal list returns None medians."""
        from app.core.pe_comparable_transactions import ComparableTransactionService

        stats = ComparableTransactionService._compute_market_stats([], [], [])
        assert stats["ev_ebitda_median"] is None
        assert stats["total_deals"] == 0

    def test_multiple_trend_expanding(self):
        """Trend = expanding when recent multiples are higher."""
        from app.core.pe_comparable_transactions import ComparableTransactionService

        # Recent (first half) higher than older (second half)
        ev_ebitda = [15.0, 14.0, 10.0, 9.0]
        deals = [MagicMock(enterprise_value_usd=Decimal("100"), seller_type="PE") for _ in ev_ebitda]

        stats = ComparableTransactionService._compute_market_stats(ev_ebitda, [], deals)
        assert stats["multiple_trend"] == "expanding"


class TestCompService:
    """Tests for the full comparable transaction service."""

    def test_no_company_returns_error(self):
        """Returns error dict when company not found."""
        from app.core.pe_comparable_transactions import ComparableTransactionService

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        svc = ComparableTransactionService(db)
        result = svc.get_comps(99999)
        assert "error" in result

    def test_deal_list_structure(self):
        """Each deal has required fields."""
        from app.core.pe_comparable_transactions import ComparableTransactionService

        deal = MagicMock()
        deal.id = 1
        deal.deal_name = "Test Exit"
        deal.deal_type = "Exit"
        deal.deal_sub_type = "Strategic Sale"
        deal.buyer_name = "Buyer Inc"
        deal.seller_name = "Seller PE"
        deal.seller_type = "PE"
        deal.enterprise_value_usd = Decimal("200000000")
        deal.ev_ebitda_multiple = Decimal("12.5")
        deal.ev_revenue_multiple = Decimal("3.5")
        deal.ltm_revenue_usd = Decimal("57000000")
        deal.ltm_ebitda_usd = Decimal("16000000")
        deal.announced_date = date(2024, 6, 1)
        deal.closed_date = date(2024, 9, 1)
        deal.status = "Closed"

        company = MagicMock()
        company.id = 1
        company.name = "TargetCo"
        company.industry = "Healthcare"
        company.sub_industry = "Urgent Care"

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = company

        svc = ComparableTransactionService(db)
        svc._find_comparable_deals = MagicMock(return_value=[deal])

        result = svc.get_comps(1)
        assert result["deal_count"] == 1
        assert result["comparable_deals"][0]["deal_name"] == "Test Exit"
        assert result["comparable_deals"][0]["ev_ebitda_multiple"] == 12.5
