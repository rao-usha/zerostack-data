"""
Valuation Estimator for private PE portfolio companies.

Uses LLM to estimate private company valuations based on available
financial data, industry context, and comparable company multiples.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)

logger = logging.getLogger(__name__)

# LLM valuation prompt
VALUATION_PROMPT = """You are a private equity valuation analyst. Estimate the enterprise value for this private company based on the available data.

Company: {company_name}
Industry: {industry}
{financial_context}

Return ONLY valid JSON:
{{
  "estimated_enterprise_value_usd": number or null,
  "estimated_equity_value_usd": number or null,
  "valuation_method": "Comparable Multiples|DCF Proxy|Revenue Multiple|EBITDA Multiple|Asset-Based|Blended",
  "ev_to_revenue_multiple": number or null,
  "ev_to_ebitda_multiple": number or null,
  "comparable_companies": ["list of 2-3 public comparables used"],
  "industry_median_ev_revenue": number or null,
  "industry_median_ev_ebitda": number or null,
  "confidence_level": "Low|Medium|High",
  "key_assumptions": ["list of 2-3 key assumptions"],
  "methodology_notes": "1-2 sentence explanation of approach"
}}

Rules:
- Use industry-appropriate multiples (SaaS: 8-15x revenue; manufacturing: 6-10x EBITDA; healthcare: 12-18x EBITDA)
- If EBITDA is available, prefer EV/EBITDA; otherwise use EV/Revenue
- If no financials are available, estimate based on industry, employee count, and any available context
- Be conservative — private companies typically trade at a 15-30% discount to public comps
- confidence_level: High if EBITDA+revenue available, Medium if revenue only, Low if estimated from context"""


class ValuationEstimator(BasePECollector):
    """
    Estimates valuations for private PE portfolio companies.

    Uses LLM with financial context (revenue, EBITDA, growth,
    industry) to estimate enterprise value and appropriate multiples.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.VALUATION_ESTIMATOR

    @property
    def entity_type(self) -> EntityType:
        return EntityType.COMPANY

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._llm_client = None

    def _get_llm_client(self):
        """Lazily initialize LLM client."""
        if self._llm_client is None:
            from app.agentic.llm_client import get_llm_client

            self._llm_client = get_llm_client(model="gpt-4o-mini")
        return self._llm_client

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        industry: Optional[str] = None,
        revenue: Optional[float] = None,
        ebitda: Optional[float] = None,
        employee_count: Optional[int] = None,
        revenue_growth: Optional[float] = None,
        total_debt: Optional[float] = None,
        total_cash: Optional[float] = None,
        description: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Estimate valuation for a private portfolio company.

        Args:
            entity_id: Portfolio company ID
            entity_name: Company name
            website_url: Company website (not directly used)
            industry: Company industry/sector
            revenue: Annual revenue in USD
            ebitda: Annual EBITDA in USD
            employee_count: Number of employees
            revenue_growth: YoY revenue growth rate (decimal, e.g. 0.15)
            total_debt: Total debt in USD
            total_cash: Total cash in USD
            description: Brief company description
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        # Check LLM availability
        llm_client = self._get_llm_client()
        if not llm_client:
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message="LLM not available — cannot estimate valuation",
                started_at=started_at,
            )

        try:
            # Build financial context string
            financial_context = self._build_financial_context(
                revenue=revenue,
                ebitda=ebitda,
                employee_count=employee_count,
                revenue_growth=revenue_growth,
                total_debt=total_debt,
                total_cash=total_cash,
                description=description,
            )

            if not financial_context.strip():
                warnings.append(
                    "No financial data available — valuation will be highly speculative"
                )

            # Get LLM valuation estimate
            valuation = await self._estimate_with_llm(
                llm_client,
                entity_name,
                industry or "Unknown",
                financial_context,
            )

            if not valuation:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=["LLM valuation estimation returned no result"],
                    started_at=started_at,
                )

            # Build valuation item
            item_data = {
                "company_id": entity_id,
                "company_name": entity_name,
                "industry": industry,
                "estimated_enterprise_value_usd": valuation.get(
                    "estimated_enterprise_value_usd"
                ),
                "estimated_equity_value_usd": valuation.get(
                    "estimated_equity_value_usd"
                ),
                "valuation_method": valuation.get("valuation_method"),
                "ev_to_revenue_multiple": valuation.get("ev_to_revenue_multiple"),
                "ev_to_ebitda_multiple": valuation.get("ev_to_ebitda_multiple"),
                "comparable_companies": valuation.get("comparable_companies", []),
                "industry_median_ev_revenue": valuation.get(
                    "industry_median_ev_revenue"
                ),
                "industry_median_ev_ebitda": valuation.get("industry_median_ev_ebitda"),
                "confidence_level": valuation.get("confidence_level", "Low"),
                "key_assumptions": valuation.get("key_assumptions", []),
                "methodology_notes": valuation.get("methodology_notes"),
                # Include input financials for reference
                "input_revenue": revenue,
                "input_ebitda": ebitda,
                "input_employee_count": employee_count,
                "input_revenue_growth": revenue_growth,
                "valuation_date": datetime.utcnow().strftime("%Y-%m-%d"),
            }

            items.append(
                self._create_item(
                    item_type="company_valuation",
                    data=item_data,
                    source_url=None,
                    confidence="llm_extracted",
                )
            )

            logger.info(
                f"Estimated valuation for {entity_name}: "
                f"EV=${valuation.get('estimated_enterprise_value_usd', 'N/A')}, "
                f"method={valuation.get('valuation_method', 'N/A')}"
            )

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error estimating valuation for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    def _build_financial_context(
        self,
        revenue: Optional[float] = None,
        ebitda: Optional[float] = None,
        employee_count: Optional[int] = None,
        revenue_growth: Optional[float] = None,
        total_debt: Optional[float] = None,
        total_cash: Optional[float] = None,
        description: Optional[str] = None,
    ) -> str:
        """Build a financial context string for the LLM prompt."""
        lines = []

        if revenue is not None:
            lines.append(f"Annual Revenue: ${revenue:,.0f}")
        if ebitda is not None:
            lines.append(f"EBITDA: ${ebitda:,.0f}")
        if revenue_growth is not None:
            lines.append(f"Revenue Growth (YoY): {revenue_growth * 100:.1f}%")
        if employee_count is not None:
            lines.append(f"Employees: {employee_count:,}")
        if total_debt is not None:
            lines.append(f"Total Debt: ${total_debt:,.0f}")
        if total_cash is not None:
            lines.append(f"Cash & Equivalents: ${total_cash:,.0f}")
        if description:
            lines.append(f"Description: {description}")

        if not lines:
            return "No financial data available. Estimate based on industry averages and company context."

        return "Available Financial Data:\n" + "\n".join(lines)

    async def _estimate_with_llm(
        self,
        llm_client,
        company_name: str,
        industry: str,
        financial_context: str,
    ) -> Optional[Dict[str, Any]]:
        """Use LLM to estimate valuation."""
        prompt = VALUATION_PROMPT.format(
            company_name=company_name,
            industry=industry,
            financial_context=financial_context,
        )

        try:
            response = await llm_client.complete(
                prompt=prompt,
                system_prompt=(
                    "You are a senior private equity valuation analyst. "
                    "Provide conservative, well-reasoned valuation estimates. "
                    "Return only valid JSON."
                ),
                json_mode=True,
            )
            return response.parse_json()

        except Exception as e:
            logger.warning(f"LLM valuation estimation failed: {e}")
            return None
