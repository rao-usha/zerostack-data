"""
Tests for SPEC 010 — PE Deal Pipeline Enhancements.
"""
import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from datetime import date


class TestPipelineInsights:
    """Tests for pipeline health and insights."""

    def test_pipeline_insights_with_deals(self):
        """Pipeline insights returns stage counts and total value."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deals = []
        statuses = ["Announced", "Pending", "Pending", "Closed", "Closed", "Closed"]
        for i, status in enumerate(statuses):
            d = MagicMock()
            d.id = i + 1
            d.status = status
            d.enterprise_value_usd = Decimal("100000000")
            d.deal_type = "LBO" if i % 2 == 0 else "Exit"
            d.expected_close_date = date(2025, 6, 1) if status == "Pending" else None
            deals.append(d)

        insights = DealPipelineService._compute_insights(deals)
        assert insights["total_pipeline_deals"] == 6
        assert insights["stage_breakdown"]["Pending"] == 2
        assert insights["stage_breakdown"]["Announced"] == 1
        assert insights["stage_breakdown"]["Closed"] == 3
        assert insights["total_pipeline_value_usd"] > 0

    def test_pipeline_insights_empty(self):
        """Empty pipeline returns zero counts."""
        from app.core.pe_deal_pipeline import DealPipelineService

        insights = DealPipelineService._compute_insights([])
        assert insights["total_pipeline_deals"] == 0
        assert insights["total_pipeline_value_usd"] == 0
        assert insights["stage_breakdown"] == {}

    def test_active_deal_count(self):
        """Active deals = non-Closed, non-Terminated."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deals = []
        for status in ["Announced", "Pending", "Closed", "Terminated"]:
            d = MagicMock()
            d.id = 1
            d.status = status
            d.enterprise_value_usd = Decimal("50000000")
            d.deal_type = "LBO"
            d.expected_close_date = None
            deals.append(d)

        insights = DealPipelineService._compute_insights(deals)
        assert insights["active_deals"] == 2  # Announced + Pending


class TestDealSerialization:
    """Tests for deal data serialization."""

    def test_deal_to_dict(self):
        """Deal serialization includes all required fields."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deal = MagicMock()
        deal.id = 42
        deal.deal_name = "Test Deal"
        deal.deal_type = "LBO"
        deal.deal_sub_type = "Platform"
        deal.status = "Pending"
        deal.enterprise_value_usd = Decimal("250000000")
        deal.ev_ebitda_multiple = Decimal("10.5")
        deal.ev_revenue_multiple = None
        deal.ltm_revenue_usd = Decimal("80000000")
        deal.ltm_ebitda_usd = Decimal("24000000")
        deal.buyer_name = "Summit Ridge"
        deal.seller_name = "Founder"
        deal.seller_type = "Founder"
        deal.announced_date = date(2025, 3, 1)
        deal.closed_date = None
        deal.expected_close_date = date(2025, 9, 1)
        deal.company_id = 10

        result = DealPipelineService._deal_to_dict(deal)
        assert result["id"] == 42
        assert result["deal_name"] == "Test Deal"
        assert result["status"] == "Pending"
        assert result["enterprise_value_usd"] == 250000000.0
        assert result["expected_close_date"] == "2025-09-01"
        assert result["closed_date"] is None
