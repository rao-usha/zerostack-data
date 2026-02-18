"""
SEC EDGAR API client for company financials.

Fetches public company filings (10-K, 10-Q) from SEC EDGAR.
"""

import logging
import httpx
from typing import Dict, Optional

logger = logging.getLogger(__name__)

SEC_EDGAR_BASE = "https://data.sec.gov"
SEC_COMPANY_SEARCH = "https://efts.sec.gov/LATEST/search-index"

class SECEdgarClient:
    """Client for SEC EDGAR public filings API."""

    def __init__(self):
        self.headers = {
            "User-Agent": "Nexdata Research contact@example.com",
            "Accept": "application/json",
        }

    async def search_company(self, company_name: str) -> Optional[Dict]:
        """
        Search for a company by name and return CIK.

        Args:
            company_name: Company name to search

        Returns:
            Dict with CIK and company info, or None if not found
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:

                # For now, use the company tickers file
                tickers_url = f"{SEC_EDGAR_BASE}/files/company_tickers.json"
                response = await client.get(tickers_url, headers=self.headers)

                if response.status_code == 200:
                    tickers = response.json()
                    # Search for company name match
                    name_lower = company_name.lower()
                    for key, data in tickers.items():
                        if name_lower in data.get("title", "").lower():
                            cik = str(data["cik_str"]).zfill(10)
                            return {
                                "cik": cik,
                                "ticker": data.get("ticker"),
                                "name": data.get("title"),
                            }
                return None
        except Exception as e:
            logger.error(f"SEC EDGAR search error for {company_name}: {e}")
            return None

    async def get_company_filings(self, cik: str) -> Optional[Dict]:
        """
        Get company filings by CIK.

        Args:
            cik: SEC CIK number (10 digits, zero-padded)

        Returns:
            Dict with filing information
        """
        try:
            cik_padded = cik.zfill(10)
            url = f"{SEC_EDGAR_BASE}/submissions/CIK{cik_padded}.json"
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self.headers)

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(
                        f"SEC EDGAR returned {response.status_code} for CIK {cik}"
                    )
                    return None
        except Exception as e:
            logger.error(f"SEC EDGAR filings error for CIK {cik}: {e}")
            return None

    async def get_company_facts(self, cik: str) -> Optional[Dict]:
        """
        Get company facts (financials) by CIK.

        Args:
            cik: SEC CIK number

        Returns:
            Dict with financial facts (revenue, assets, etc.)
        """
        try:
            cik_padded = cik.zfill(10)
            url = f"{SEC_EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik_padded}.json"

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self.headers)

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(
                        f"SEC EDGAR facts returned {response.status_code} for CIK {cik}"
                    )
                    return None
        except Exception as e:
            logger.error(f"SEC EDGAR facts error for CIK {cik}: {e}")
            return None

    def extract_financials(self, facts: Dict) -> Dict:
        """
        Extract key financials from SEC EDGAR facts.

        Args:
            facts: Raw facts from SEC EDGAR API

        Returns:
            Dict with extracted financials
        """
        result = {
            "revenue": None,
            "assets": None,
            "net_income": None,
            "filing_date": None,
        }

        if not facts:
            return result

        us_gaap = facts.get("facts", {}).get("us-gaap", {})

        # Extract revenue (multiple possible labels)
        revenue_labels = [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
        ]
        for label in revenue_labels:
            if label in us_gaap:
                units = us_gaap[label].get("units", {}).get("USD", [])
                if units:
                    # Get most recent annual value
                    annual = [u for u in units if u.get("form") == "10-K"]
                    if annual:
                        latest = max(annual, key=lambda x: x.get("end", ""))
                        result["revenue"] = latest.get("val")
                        result["filing_date"] = latest.get("filed")
                        break

        # Extract total assets
        if "Assets" in us_gaap:
            units = us_gaap["Assets"].get("units", {}).get("USD", [])
            if units:
                annual = [u for u in units if u.get("form") == "10-K"]
                if annual:
                    latest = max(annual, key=lambda x: x.get("end", ""))
                    result["assets"] = latest.get("val")

        # Extract net income
        income_labels = ["NetIncomeLoss", "ProfitLoss"]
        for label in income_labels:
            if label in us_gaap:
                units = us_gaap[label].get("units", {}).get("USD", [])
                if units:
                    annual = [u for u in units if u.get("form") == "10-K"]
                    if annual:
                        latest = max(annual, key=lambda x: x.get("end", ""))
                        result["net_income"] = latest.get("val")
                        break

        return result

    async def enrich_company(self, company_name: str) -> Dict:
        """
        Full enrichment flow for a company.

        Args:
            company_name: Company name to enrich

        Returns:
            Dict with enrichment results
        """
        result = {
            "source": "sec_edgar",
            "found": False,
            "cik": None,
            "ticker": None,
            "revenue": None,
            "assets": None,
            "net_income": None,
            "filing_date": None,
            "error": None,
        }

        # Search for company
        company_info = await self.search_company(company_name)
        if not company_info:
            result["error"] = "company_not_found"
            return result

        result["cik"] = company_info["cik"]
        result["ticker"] = company_info.get("ticker")
        result["found"] = True

        # Get financial facts
        facts = await self.get_company_facts(company_info["cik"])
        if facts:
            financials = self.extract_financials(facts)
            result.update(financials)

        return result
