"""
Public Comps Collector for PE portfolio company financials.

Pulls financial data (revenue, EBITDA, market cap, valuation multiples)
for public portfolio companies via Yahoo Finance.
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
from app.sources.pe_collection.config import settings

logger = logging.getLogger(__name__)

# Yahoo Finance endpoints
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
YAHOO_QUOTE_SUMMARY_URL = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
)
YAHOO_SUMMARY_MODULES = (
    "financialData,defaultKeyStatistics,incomeStatementHistory,"
    "balanceSheetHistory,cashflowStatementHistory,price,summaryProfile"
)


class PublicCompsCollector(BasePECollector):
    """
    Collects financial data for public portfolio companies via Yahoo Finance.

    Extracts: revenue, EBITDA, net income, balance sheet, valuation multiples,
    company profile data.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.PUBLIC_COMPS

    @property
    def entity_type(self) -> EntityType:
        return EntityType.COMPANY

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rate_limit_delay = kwargs.get("rate_limit_delay", 1.0)

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        ticker: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect financial data for a public portfolio company.

        Args:
            entity_id: Portfolio company ID
            entity_name: Company name
            website_url: Company website (used for ticker search fallback)
            ticker: Stock ticker symbol (if known)
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        # Resolve ticker if not provided
        if not ticker:
            ticker = await self._search_ticker(entity_name)
            if not ticker:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=False,
                    error_message=(
                        f"Could not resolve ticker for '{entity_name}'. "
                        "Company may be private."
                    ),
                    started_at=started_at,
                )
            warnings.append(f"Resolved ticker to '{ticker}' via search")

        try:
            # Fetch quote summary (bundles all financial modules)
            summary = await self._fetch_quote_summary(ticker)
            if not summary:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=False,
                    error_message=f"Could not fetch Yahoo Finance data for {ticker}",
                    started_at=started_at,
                )

            source_url = f"https://finance.yahoo.com/quote/{ticker}"

            # Extract financial data
            financial_item = self._extract_financials(
                summary, entity_id, entity_name, ticker, source_url
            )
            if financial_item:
                items.append(financial_item)

            # Extract valuation data
            valuation_item = self._extract_valuation(
                summary, entity_id, entity_name, ticker, source_url
            )
            if valuation_item:
                items.append(valuation_item)

            # Extract company profile update
            profile_item = self._extract_profile(
                summary, entity_id, entity_name, ticker, source_url
            )
            if profile_item:
                items.append(profile_item)

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting financials for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _search_ticker(self, company_name: str) -> Optional[str]:
        """Search Yahoo Finance for a ticker symbol by company name."""
        data = await self._fetch_json(
            YAHOO_SEARCH_URL,
            params={"q": company_name, "quotesCount": 5, "newsCount": 0},
        )
        if not data:
            return None

        quotes = data.get("quotes", [])
        for quote in quotes:
            # Prefer equity quotes
            quote_type = quote.get("quoteType", "")
            if quote_type == "EQUITY":
                return quote.get("symbol")

        # Fallback: return first result if any
        if quotes:
            return quotes[0].get("symbol")

        return None

    async def _fetch_quote_summary(
        self, ticker: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch Yahoo Finance quote summary with all financial modules."""
        url = YAHOO_QUOTE_SUMMARY_URL.format(ticker=ticker)
        data = await self._fetch_json(url, params={"modules": YAHOO_SUMMARY_MODULES})

        if not data:
            return None

        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return None

        return result[0]

    def _extract_financials(
        self,
        summary: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract income statement, balance sheet, and cash flow data."""
        fin = summary.get("financialData", {})
        stats = summary.get("defaultKeyStatistics", {})

        # Get most recent annual income statement
        income_history = (
            summary.get("incomeStatementHistory", {})
            .get("incomeStatementHistory", [])
        )
        latest_income = income_history[0] if income_history else {}

        # Get most recent balance sheet
        bs_history = (
            summary.get("balanceSheetHistory", {})
            .get("balanceSheetStatements", [])
        )
        latest_bs = bs_history[0] if bs_history else {}

        # Get most recent cash flow
        cf_history = (
            summary.get("cashflowStatementHistory", {})
            .get("cashflowStatements", [])
        )
        latest_cf = cf_history[0] if cf_history else {}

        fiscal_end = latest_income.get("endDate", {}).get("fmt")

        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            "fiscal_year_end": fiscal_end,
            # Income statement
            "revenue": _raw(fin.get("totalRevenue")),
            "ebitda": _raw(fin.get("ebitda")),
            "gross_profit": _raw(latest_income.get("grossProfit")),
            "operating_income": _raw(latest_income.get("operatingIncome")),
            "net_income": _raw(latest_income.get("netIncome")),
            # Balance sheet
            "total_assets": _raw(latest_bs.get("totalAssets")),
            "total_debt": _raw(fin.get("totalDebt")),
            "total_cash": _raw(fin.get("totalCash")),
            "total_stockholder_equity": _raw(
                latest_bs.get("totalStockholderEquity")
            ),
            # Cash flow
            "free_cash_flow": _raw(fin.get("freeCashflow")),
            "operating_cash_flow": _raw(fin.get("operatingCashflow")),
            # Margins
            "gross_margin": _raw(fin.get("grossMargins")),
            "operating_margin": _raw(fin.get("operatingMargins")),
            "profit_margin": _raw(fin.get("profitMargins")),
            # Growth
            "revenue_growth": _raw(fin.get("revenueGrowth")),
            "earnings_growth": _raw(fin.get("earningsGrowth")),
        }

        # Only return if we have some meaningful data
        if not any(v for k, v in data.items() if k not in (
            "company_id", "company_name", "ticker", "fiscal_year_end"
        )):
            return None

        return self._create_item(
            item_type="company_financial",
            data=data,
            source_url=source_url,
            confidence="high",
        )

    def _extract_valuation(
        self,
        summary: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract valuation metrics (EV, multiples)."""
        stats = summary.get("defaultKeyStatistics", {})
        fin = summary.get("financialData", {})
        price = summary.get("price", {})

        market_cap = _raw(price.get("marketCap"))
        enterprise_value = _raw(stats.get("enterpriseValue"))

        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "ev_to_revenue": _raw(stats.get("enterpriseToRevenue")),
            "ev_to_ebitda": _raw(stats.get("enterpriseToEbitda")),
            "trailing_pe": _raw(stats.get("trailingPE") or stats.get("trailingPe")),
            "forward_pe": _raw(stats.get("forwardPE") or stats.get("forwardPe")),
            "price_to_book": _raw(stats.get("priceToBook")),
            "price_to_sales": _raw(stats.get("priceToSalesTrailing12Months")),
            "beta": _raw(stats.get("beta")),
            "fifty_two_week_high": _raw(stats.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _raw(stats.get("fiftyTwoWeekLow")),
            "current_price": _raw(fin.get("currentPrice")),
        }

        if not enterprise_value and not market_cap:
            return None

        return self._create_item(
            item_type="company_valuation",
            data=data,
            source_url=source_url,
            confidence="high",
        )

    def _extract_profile(
        self,
        summary: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract company profile fields for portfolio company update."""
        profile = summary.get("summaryProfile", {})
        price = summary.get("price", {})

        if not profile:
            return None

        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            "industry": profile.get("industry"),
            "sector": profile.get("sector"),
            "description": profile.get("longBusinessSummary"),
            "employee_count": profile.get("fullTimeEmployees"),
            "headquarters_city": profile.get("city"),
            "headquarters_state": profile.get("state"),
            "headquarters_country": profile.get("country"),
            "website": profile.get("website"),
            "exchange": price.get("exchangeName"),
        }

        return self._create_item(
            item_type="company_update",
            data=data,
            source_url=source_url,
            confidence="high",
            is_new=False,
        )


def _raw(val: Any) -> Any:
    """Extract raw value from Yahoo Finance nested dict format."""
    if isinstance(val, dict):
        return val.get("raw")
    return val
