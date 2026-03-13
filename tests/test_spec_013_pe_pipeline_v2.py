"""
Tests for SPEC 013 — PE Pipeline V2: Firm-Scoped Stage Tracking.
"""
import pytest
from unittest.mock import MagicMock
from decimal import Decimal
from datetime import date


class TestFirmDealsGrouped:
    """Tests for firm-scoped deal grouping by stage."""

    def test_firm_deals_grouped_by_stage(self):
        """T1: Deals grouped correctly by stage."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deals = []
        stages = ["Screening", "Screening", "DD", "LOI", "Closing", "Won", "Lost"]
        for i, stage in enumerate(stages):
            d = MagicMock()
            d.id = i + 1
            d.company_id = i + 100
            d.deal_name = f"Deal {i}"
            d.deal_type = "LBO"
            d.deal_sub_type = "Platform"
            d.status = stage
            d.enterprise_value_usd = Decimal("50000000")
            d.ev_ebitda_multiple = Decimal("10.0")
            d.ev_revenue_multiple = None
            d.ltm_revenue_usd = None
            d.ltm_ebitda_usd = None
            d.buyer_name = "Test Firm"
            d.seller_name = "Founder"
            d.seller_type = "Founder"
            d.announced_date = date(2026, 1, 1)
            d.closed_date = None
            d.expected_close_date = None
            deals.append(d)

        grouped = DealPipelineService._group_by_stage(deals)
        assert len(grouped["Screening"]) == 2
        assert len(grouped["DD"]) == 1
        assert len(grouped["LOI"]) == 1
        assert len(grouped["Closing"]) == 1
        assert len(grouped["Won"]) == 1
        assert len(grouped["Lost"]) == 1

    def test_firm_deals_empty(self):
        """T2: Empty result for firm with no deals."""
        from app.core.pe_deal_pipeline import DealPipelineService

        grouped = DealPipelineService._group_by_stage([])
        # All stages should be present but empty
        for stage in ["Screening", "DD", "LOI", "Closing", "Won", "Lost"]:
            assert grouped[stage] == []


class TestFirmInsights:
    """Tests for firm-scoped pipeline insights."""

    def test_firm_insights_conversion(self):
        """T3: Conversion metrics computed correctly."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deals = []
        # 10 deals total: 2 Screening, 2 DD, 2 LOI, 1 Closing, 2 Won, 1 Lost
        stages_evs = [
            ("Screening", Decimal("50000000")),
            ("Screening", Decimal("60000000")),
            ("DD", Decimal("80000000")),
            ("DD", Decimal("90000000")),
            ("LOI", Decimal("100000000")),
            ("LOI", Decimal("120000000")),
            ("Closing", Decimal("150000000")),
            ("Won", Decimal("200000000")),
            ("Won", Decimal("250000000")),
            ("Lost", Decimal("70000000")),
        ]
        for i, (stage, ev) in enumerate(stages_evs):
            d = MagicMock()
            d.id = i + 1
            d.status = stage
            d.enterprise_value_usd = ev
            d.deal_type = "LBO"
            d.expected_close_date = None
            deals.append(d)

        insights = DealPipelineService._compute_firm_insights(deals)
        assert insights["total_deals"] == 10
        assert insights["active_deals"] == 7  # exclude Won + Lost
        assert insights["won_deals"] == 2
        assert insights["lost_deals"] == 1
        assert insights["total_pipeline_value_usd"] > 0
        assert insights["win_rate_pct"] is not None
        assert insights["stage_breakdown"]["Screening"] == 2
        assert insights["stage_breakdown"]["Won"] == 2

    def test_firm_insights_empty(self):
        """T4: Empty firm returns zero metrics."""
        from app.core.pe_deal_pipeline import DealPipelineService

        insights = DealPipelineService._compute_firm_insights([])
        assert insights["total_deals"] == 0
        assert insights["active_deals"] == 0
        assert insights["total_pipeline_value_usd"] == 0
        assert insights["win_rate_pct"] is None


class TestStageTransition:
    """Tests for stage transitions."""

    def test_pipeline_stages_comprehensive(self):
        """T6: All 6 pipeline stages represented in grouping."""
        from app.core.pe_deal_pipeline import DealPipelineService

        deals = []
        for i, stage in enumerate(["Screening", "DD", "LOI", "Closing", "Won", "Lost"]):
            d = MagicMock()
            d.id = i + 1
            d.company_id = i + 100
            d.deal_name = f"Deal {i}"
            d.deal_type = "LBO"
            d.deal_sub_type = None
            d.status = stage
            d.enterprise_value_usd = Decimal("100000000")
            d.ev_ebitda_multiple = None
            d.ev_revenue_multiple = None
            d.ltm_revenue_usd = None
            d.ltm_ebitda_usd = None
            d.buyer_name = "Firm"
            d.seller_name = "Seller"
            d.seller_type = "Founder"
            d.announced_date = None
            d.closed_date = None
            d.expected_close_date = None
            deals.append(d)

        grouped = DealPipelineService._group_by_stage(deals)
        assert len(grouped) == 6
        for stage in ["Screening", "DD", "LOI", "Closing", "Won", "Lost"]:
            assert stage in grouped
            assert len(grouped[stage]) == 1
