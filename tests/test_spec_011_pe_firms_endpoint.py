"""
Tests for SPEC 011 — PE Firms List Endpoint.
"""
import pytest
from unittest.mock import MagicMock
from decimal import Decimal


class TestFirmsList:
    """Tests for the firms list service logic."""

    def test_build_firm_summary(self):
        """Firm summary includes id, name, fund_count, company_count."""
        from app.api.v1.pe_benchmarks import _build_firm_summary

        firm = MagicMock()
        firm.id = 42
        firm.name = "Summit Ridge Partners"
        firm.firm_type = "PE"
        firm.primary_strategy = "Buyout"
        firm.aum_usd_millions = Decimal("2500")
        firm.headquarters_city = "New York"
        firm.headquarters_state = "NY"
        firm.sector_focus = ["Healthcare", "Technology"]
        firm.status = "Active"

        result = _build_firm_summary(firm, fund_count=3, company_count=8)
        assert result["id"] == 42
        assert result["name"] == "Summit Ridge Partners"
        assert result["fund_count"] == 3
        assert result["company_count"] == 8
        assert result["firm_type"] == "PE"
        assert result["aum_usd_millions"] == 2500.0

    def test_build_firm_summary_no_aum(self):
        """Firm with no AUM returns None."""
        from app.api.v1.pe_benchmarks import _build_firm_summary

        firm = MagicMock()
        firm.id = 1
        firm.name = "Test Firm"
        firm.firm_type = None
        firm.primary_strategy = None
        firm.aum_usd_millions = None
        firm.headquarters_city = None
        firm.headquarters_state = None
        firm.sector_focus = None
        firm.status = "Active"

        result = _build_firm_summary(firm, fund_count=0, company_count=0)
        assert result["aum_usd_millions"] is None
        assert result["fund_count"] == 0
