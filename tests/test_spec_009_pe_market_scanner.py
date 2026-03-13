"""
Tests for SPEC 009 — PE Market Scanner & Intelligence Brief.
"""
import pytest
from unittest.mock import MagicMock
from decimal import Decimal
from datetime import date


class TestSectorOverview:
    """Tests for sector overview computation."""

    def test_sector_overview_with_deals(self):
        """T1: Returns deal stats, median multiple, trend for sector."""
        from app.core.pe_market_scanner import MarketScannerService

        deals = []
        for i, (ev, ebitda_m, rev_m, closed) in enumerate([
            (Decimal("300000000"), Decimal("12.5"), Decimal("2.8"), date(2024, 3, 1)),
            (Decimal("200000000"), Decimal("10.0"), Decimal("2.2"), date(2024, 6, 1)),
            (Decimal("450000000"), Decimal("14.0"), Decimal("3.1"), date(2024, 9, 1)),
            (Decimal("180000000"), Decimal("11.5"), Decimal("2.5"), date(2025, 1, 1)),
            (Decimal("350000000"), Decimal("13.0"), Decimal("2.9"), date(2025, 4, 1)),
        ]):
            d = MagicMock()
            d.enterprise_value_usd = ev
            d.ev_ebitda_multiple = ebitda_m
            d.ev_revenue_multiple = rev_m
            d.closed_date = closed
            d.buyer_name = f"Buyer {i}"
            d.seller_type = "PE"
            d.deal_sub_type = "Strategic Sale"
            d.status = "Closed"
            deals.append(d)

        overview = MarketScannerService._compute_sector_overview("Healthcare", deals)
        assert overview["industry"] == "Healthcare"
        assert overview["deal_count"] == 5
        assert overview["median_ev_ebitda"] == 12.5
        assert overview["total_deal_value_usd"] > 0
        assert overview["deal_type_breakdown"]["Strategic Sale"] == 5

    def test_sector_overview_no_deals(self):
        """T2: Empty industry returns zero counts."""
        from app.core.pe_market_scanner import MarketScannerService

        overview = MarketScannerService._compute_sector_overview("Unknown", [])
        assert overview["deal_count"] == 0
        assert overview["median_ev_ebitda"] is None
        assert overview["total_deal_value_usd"] == 0

    def test_top_buyers_ranking(self):
        """T6: Top buyers sorted by deal count."""
        from app.core.pe_market_scanner import MarketScannerService

        deals = []
        buyers = ["Acme Corp", "Acme Corp", "Acme Corp", "Beta Inc", "Beta Inc", "Gamma LLC"]
        for buyer in buyers:
            d = MagicMock()
            d.enterprise_value_usd = Decimal("100000000")
            d.ev_ebitda_multiple = Decimal("10.0")
            d.ev_revenue_multiple = None
            d.closed_date = date(2024, 6, 1)
            d.buyer_name = buyer
            d.seller_type = "PE"
            d.deal_sub_type = "Strategic Sale"
            d.status = "Closed"
            deals.append(d)

        overview = MarketScannerService._compute_sector_overview("Test", deals)
        top = overview["top_buyers"]
        assert top[0]["buyer_name"] == "Acme Corp"
        assert top[0]["deal_count"] == 3
        assert top[1]["buyer_name"] == "Beta Inc"


class TestIntelligenceBrief:
    """Tests for intelligence brief generation."""

    def test_intelligence_brief_structure(self):
        """T3: Brief has headline, key findings, and recommendations."""
        from app.core.pe_market_scanner import MarketScannerService

        overview = {
            "industry": "Healthcare",
            "deal_count": 7,
            "total_deal_value_usd": 2_430_000_000,
            "median_ev_ebitda": 13.2,
            "median_ev_revenue": 2.85,
            "ev_ebitda_range": {"min": 10.8, "max": 15.2},
            "deal_type_breakdown": {"Strategic Sale": 5, "Secondary Buyout": 2},
            "seller_type_breakdown": {"PE": 7},
            "top_buyers": [
                {"buyer_name": "Acadia Healthcare", "deal_count": 1},
            ],
            "yoy_deal_count_change": 0.15,
            "yoy_multiple_change": 0.08,
        }

        brief = MarketScannerService._generate_intelligence_brief(overview)
        assert "headline" in brief
        assert "key_findings" in brief
        assert len(brief["key_findings"]) >= 1
        assert "recommendations" in brief
        assert brief["industry"] == "Healthcare"


class TestMarketSignals:
    """Tests for cross-sector market signals."""

    def test_market_signals_momentum(self):
        """T5: Momentum computed from deal flow changes, returns 0-100 score."""
        from app.core.pe_market_scanner import MarketScannerService

        # Strong positive deal flow + multiple expansion = bullish
        signal = MarketScannerService._compute_momentum(
            current_deals=15, prior_deals=7,
            current_median=14.0, prior_median=10.0,
        )
        assert signal["momentum"] == "bullish"
        assert signal["momentum_score"] >= 65
        assert signal["deal_flow_change_pct"] > 0
        assert signal["multiple_change_pct"] > 0

    def test_market_signals_bearish(self):
        """Declining deal flow and multiples = bearish."""
        from app.core.pe_market_scanner import MarketScannerService

        signal = MarketScannerService._compute_momentum(
            current_deals=5, prior_deals=10,
            current_median=9.0, prior_median=12.0,
        )
        assert signal["momentum"] == "bearish"
        assert signal["momentum_score"] <= 35

    def test_momentum_score_with_sentiment(self):
        """Sentiment boosts momentum score."""
        from app.core.pe_market_scanner import MarketScannerService

        # Neutral deal flow but positive sentiment
        signal = MarketScannerService._compute_momentum(
            current_deals=10, prior_deals=10,
            current_median=12.0, prior_median=12.0,
            sentiment_score=0.8,
        )
        assert signal["momentum_score"] > 50  # boosted by sentiment
        assert signal["momentum_score"] <= 100

    def test_momentum_score_range(self):
        """Momentum score always between 0 and 100."""
        from app.core.pe_market_scanner import MarketScannerService

        # Extreme bullish
        signal = MarketScannerService._compute_momentum(
            current_deals=20, prior_deals=2,
            current_median=25.0, prior_median=5.0,
            sentiment_score=1.0,
        )
        assert 0 <= signal["momentum_score"] <= 100

        # Extreme bearish
        signal = MarketScannerService._compute_momentum(
            current_deals=1, prior_deals=20,
            current_median=5.0, prior_median=25.0,
            sentiment_score=-1.0,
        )
        assert 0 <= signal["momentum_score"] <= 100
