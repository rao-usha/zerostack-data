"""
Public Comps Collector for PE portfolio company financials.

Pulls financial data (revenue, EBITDA, market cap, valuation multiples)
for public portfolio companies via the yfinance package.
"""

import asyncio
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

# Yahoo Finance search endpoint (still works without auth)
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"


class PublicCompsCollector(BasePECollector):
    """
    Collects financial data for public portfolio companies via yfinance.

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
            website_url: Company website
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
            # Fetch data via yfinance (runs sync IO in thread pool)
            info = await self._fetch_yfinance_info(ticker)
            if not info:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=False,
                    error_message=f"Could not fetch Yahoo Finance data for {ticker}",
                    started_at=started_at,
                )

            self._requests_made += 1
            source_url = f"https://finance.yahoo.com/quote/{ticker}"

            # Extract financial data
            financial_item = self._extract_financials(
                info, entity_id, entity_name, ticker, source_url
            )
            if financial_item:
                items.append(financial_item)

            # Extract valuation data
            valuation_item = self._extract_valuation(
                info, entity_id, entity_name, ticker, source_url
            )
            if valuation_item:
                items.append(valuation_item)

            # Extract company profile update
            profile_item = self._extract_profile(
                info, entity_id, entity_name, ticker, source_url
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
            if quote.get("quoteType") == "EQUITY":
                return quote.get("symbol")

        if quotes:
            return quotes[0].get("symbol")

        return None

    async def _fetch_yfinance_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch ticker info via yfinance (runs in thread pool)."""
        try:
            import yfinance as yf
        except ImportError:
            logger.error("yfinance package not installed")
            return None

        def _fetch():
            t = yf.Ticker(ticker)
            return t.info

        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, _fetch)

        if not info or not info.get("symbol"):
            return None

        return info

    def _extract_financials(
        self,
        info: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract income statement, balance sheet, and cash flow data."""
        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            # Income statement
            "revenue": info.get("totalRevenue"),
            "ebitda": info.get("ebitda"),
            "gross_profit": info.get("grossProfits"),
            "operating_income": info.get("operatingIncome"),
            "net_income": info.get("netIncomeToCommon"),
            # Balance sheet
            "total_assets": info.get("totalAssets"),
            "total_debt": info.get("totalDebt"),
            "total_cash": info.get("totalCash"),
            "total_stockholder_equity": info.get("bookValue"),
            # Cash flow
            "free_cash_flow": info.get("freeCashflow"),
            "operating_cash_flow": info.get("operatingCashflow"),
            # Margins
            "gross_margin": info.get("grossMargins"),
            "operating_margin": info.get("operatingMargins"),
            "profit_margin": info.get("profitMargins"),
            # Growth
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
        }

        has_data = any(
            v is not None
            for k, v in data.items()
            if k not in ("company_id", "company_name", "ticker")
        )
        if not has_data:
            return None

        return self._create_item(
            item_type="company_financial",
            data=data,
            source_url=source_url,
            confidence="high",
        )

    def _extract_valuation(
        self,
        info: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract valuation metrics (EV, multiples)."""
        market_cap = info.get("marketCap")
        enterprise_value = info.get("enterpriseValue")

        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "ev_to_revenue": info.get("enterpriseToRevenue"),
            "ev_to_ebitda": info.get("enterpriseToEbitda"),
            "trailing_pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "price_to_book": info.get("priceToBook"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "beta": info.get("beta"),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
            "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
            "current_price": info.get("currentPrice"),
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
        info: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        ticker: str,
        source_url: str,
    ) -> Optional[PECollectedItem]:
        """Extract company profile fields for portfolio company update."""
        industry = info.get("industry")
        sector = info.get("sector")

        if not industry and not sector:
            return None

        data = {
            "company_id": entity_id,
            "company_name": entity_name,
            "ticker": ticker,
            "industry": industry,
            "sector": sector,
            "description": info.get("longBusinessSummary"),
            "employee_count": info.get("fullTimeEmployees"),
            "headquarters_city": info.get("city"),
            "headquarters_state": info.get("state"),
            "headquarters_country": info.get("country"),
            "website": info.get("website"),
            "exchange": info.get("exchange"),
        }

        return self._create_item(
            item_type="company_update",
            data=data,
            source_url=source_url,
            confidence="high",
            is_new=False,
        )
