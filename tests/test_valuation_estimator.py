"""
Unit tests for ValuationEstimator PE collector.

Tests financial context building, LLM valuation estimation,
and error handling.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.sources.pe_collection.financial_collectors.valuation_estimator import (
    ValuationEstimator,
)
from app.sources.pe_collection.types import (
    PECollectionSource,
    EntityType,
)


@pytest.fixture
def estimator():
    """Create a ValuationEstimator with mocked rate limiting."""
    est = ValuationEstimator()
    est._rate_limit = AsyncMock()
    return est


def _make_valuation_response():
    """Standard LLM valuation response."""
    return {
        "estimated_enterprise_value_usd": 500_000_000,
        "estimated_equity_value_usd": 450_000_000,
        "valuation_method": "EBITDA Multiple",
        "ev_to_revenue_multiple": 5.0,
        "ev_to_ebitda_multiple": 10.0,
        "comparable_companies": ["CompA", "CompB", "CompC"],
        "industry_median_ev_revenue": 4.5,
        "industry_median_ev_ebitda": 9.0,
        "confidence_level": "High",
        "key_assumptions": ["Stable growth", "Industry average margins"],
        "methodology_notes": "Based on EBITDA multiple with private discount.",
    }


# ============================================================================
# Property Tests
# ============================================================================

class TestValuationEstimatorProperties:

    def test_source_type(self, estimator):
        assert estimator.source_type == PECollectionSource.VALUATION_ESTIMATOR

    def test_entity_type(self, estimator):
        assert estimator.entity_type == EntityType.COMPANY


# ============================================================================
# Collect Method Tests
# ============================================================================

class TestValuationEstimatorCollect:

    @pytest.mark.asyncio
    async def test_llm_unavailable(self, estimator):
        """_get_llm_client returns None -> success=False, error message."""
        estimator._get_llm_client = MagicMock(return_value=None)

        result = await estimator.collect(entity_id=1, entity_name="TestCo")
        assert result.success is False
        assert "LLM not available" in result.error_message

    @pytest.mark.asyncio
    async def test_full_financials(self, estimator):
        """Revenue + EBITDA + growth -> LLM called, 1 valuation item returned."""
        valuation = _make_valuation_response()

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(
            entity_id=1,
            entity_name="TestCo",
            industry="Technology",
            revenue=100_000_000,
            ebitda=50_000_000,
            revenue_growth=0.15,
        )
        assert result.success is True
        assert len(result.items) == 1
        assert result.items[0].item_type == "company_valuation"
        assert result.items[0].data["estimated_enterprise_value_usd"] == 500_000_000
        assert result.items[0].confidence == "llm_extracted"
        mock_llm.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_revenue_only(self, estimator):
        """Just revenue -> context includes revenue line, LLM called."""
        valuation = _make_valuation_response()
        valuation["valuation_method"] = "Revenue Multiple"
        valuation["confidence_level"] = "Medium"

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(
            entity_id=1,
            entity_name="TestCo",
            revenue=50_000_000,
        )
        assert result.success is True
        assert len(result.items) == 1
        # Verify the prompt included revenue
        call_args = mock_llm.complete.call_args
        prompt = call_args.kwargs.get("prompt", call_args[1].get("prompt", ""))
        assert "$50,000,000" in prompt

    @pytest.mark.asyncio
    async def test_no_financial_data(self, estimator):
        """No params -> LLM prompt includes 'No financial data', LLM still called."""
        valuation = _make_valuation_response()
        valuation["confidence_level"] = "Low"

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(entity_id=1, entity_name="TestCo")
        assert result.success is True
        assert len(result.items) == 1
        # The financial context falls back to "No financial data available"
        call_args = mock_llm.complete.call_args
        prompt = call_args.kwargs.get("prompt", call_args[1].get("prompt", ""))
        assert "No financial data" in prompt
        mock_llm.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_llm_returns_null(self, estimator):
        """LLM returns None -> empty items with warning."""
        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = None
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(
            entity_id=1, entity_name="TestCo", revenue=100_000_000,
        )
        assert result.success is True
        assert len(result.items) == 0
        assert any("no result" in w.lower() for w in result.warnings)

    @pytest.mark.asyncio
    async def test_valuation_item_data(self, estimator):
        """Full LLM response -> item has all expected keys."""
        valuation = _make_valuation_response()

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(
            entity_id=42,
            entity_name="AcmeCorp",
            industry="Healthcare",
            revenue=200_000_000,
            ebitda=80_000_000,
            employee_count=500,
            revenue_growth=0.20,
        )
        item = result.items[0]
        data = item.data

        assert data["company_id"] == 42
        assert data["company_name"] == "AcmeCorp"
        assert data["industry"] == "Healthcare"
        assert data["estimated_enterprise_value_usd"] == 500_000_000
        assert data["estimated_equity_value_usd"] == 450_000_000
        assert data["valuation_method"] == "EBITDA Multiple"
        assert data["ev_to_revenue_multiple"] == 5.0
        assert data["ev_to_ebitda_multiple"] == 10.0
        assert data["comparable_companies"] == ["CompA", "CompB", "CompC"]
        assert data["confidence_level"] == "High"
        assert data["key_assumptions"] == ["Stable growth", "Industry average margins"]
        assert data["methodology_notes"] is not None
        assert data["valuation_date"] is not None

    @pytest.mark.asyncio
    async def test_input_financials_preserved(self, estimator):
        """Input revenue/ebitda/growth included in item data for reference."""
        valuation = _make_valuation_response()

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        result = await estimator.collect(
            entity_id=1,
            entity_name="TestCo",
            revenue=100_000_000,
            ebitda=40_000_000,
            employee_count=200,
            revenue_growth=0.10,
        )
        data = result.items[0].data
        assert data["input_revenue"] == 100_000_000
        assert data["input_ebitda"] == 40_000_000
        assert data["input_employee_count"] == 200
        assert data["input_revenue_growth"] == 0.10

    @pytest.mark.asyncio
    async def test_industry_defaults_to_unknown(self, estimator):
        """No industry param -> LLM prompt uses 'Unknown'."""
        valuation = _make_valuation_response()

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        await estimator.collect(entity_id=1, entity_name="TestCo")
        call_args = mock_llm.complete.call_args
        prompt = call_args.kwargs.get("prompt", call_args[1].get("prompt", ""))
        assert "Unknown" in prompt

    @pytest.mark.asyncio
    async def test_description_included(self, estimator):
        """Description kwarg -> appears in financial context."""
        valuation = _make_valuation_response()

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = valuation
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        estimator._get_llm_client = MagicMock(return_value=mock_llm)

        await estimator.collect(
            entity_id=1,
            entity_name="TestCo",
            description="Leading SaaS provider for healthcare",
        )
        call_args = mock_llm.complete.call_args
        prompt = call_args.kwargs.get("prompt", call_args[1].get("prompt", ""))
        assert "Leading SaaS provider" in prompt

    @pytest.mark.asyncio
    async def test_exception_during_collect(self, estimator):
        """Exception in outer collect -> success=False with error message."""
        # _estimate_with_llm catches LLM exceptions internally, so we
        # must raise from the outer flow to trigger the error path.
        mock_llm = MagicMock()
        mock_llm.complete = AsyncMock(return_value=MagicMock(parse_json=MagicMock(return_value=_make_valuation_response())))
        estimator._get_llm_client = MagicMock(return_value=mock_llm)
        estimator._build_financial_context = MagicMock(side_effect=RuntimeError("Unexpected error"))

        result = await estimator.collect(
            entity_id=1, entity_name="TestCo", revenue=100_000_000,
        )
        assert result.success is False
        assert "Unexpected error" in result.error_message


# ============================================================================
# Financial Context Tests
# ============================================================================

class TestFinancialContextFormatting:

    def test_revenue_formatting(self, estimator):
        """Revenue=$50M -> formatted with commas."""
        ctx = estimator._build_financial_context(revenue=50_000_000)
        assert "$50,000,000" in ctx

    def test_growth_formatting(self, estimator):
        """Growth=0.15 -> '15.0%'."""
        ctx = estimator._build_financial_context(revenue_growth=0.15)
        assert "15.0%" in ctx

    def test_employee_formatting(self, estimator):
        """Employees=500 -> '500'."""
        ctx = estimator._build_financial_context(employee_count=500)
        assert "500" in ctx

    def test_full_context(self, estimator):
        """All fields populated -> all appear in context."""
        ctx = estimator._build_financial_context(
            revenue=100_000_000,
            ebitda=30_000_000,
            employee_count=1000,
            revenue_growth=0.25,
            total_debt=20_000_000,
            total_cash=10_000_000,
            description="Enterprise SaaS company",
        )
        assert "$100,000,000" in ctx
        assert "$30,000,000" in ctx
        assert "1,000" in ctx
        assert "25.0%" in ctx
        assert "$20,000,000" in ctx
        assert "$10,000,000" in ctx
        assert "Enterprise SaaS" in ctx

    def test_no_data_context(self, estimator):
        """No fields -> fallback message."""
        ctx = estimator._build_financial_context()
        assert "No financial data" in ctx
