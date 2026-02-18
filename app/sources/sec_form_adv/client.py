"""
SEC Form ADV Client.

Fetches investment adviser data from SEC IAPD and data files.
"""

import logging
import httpx
from typing import Dict, List, Optional, Any
import asyncio

logger = logging.getLogger(__name__)


class FormADVClient:
    """
    Client for accessing SEC Form ADV data.

    Data sources:
    - IAPD (Investment Adviser Public Disclosure): https://adviserinfo.sec.gov/
    - SEC quarterly CSV files
    """

    IAPD_API_URL = "https://api.adviserinfo.sec.gov"
    SEC_DATA_URL = "https://www.sec.gov"

    USER_AGENT = "Nexdata Data Ingestion Service (contact: support@nexdata.io)"

    # Rate limit: be conservative with SEC
    RATE_LIMIT_DELAY = 0.2  # 200ms between requests

    def __init__(self):
        self._last_request_time = 0

    async def _rate_limit(self):
        """Enforce rate limits."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json, text/csv, */*",
        }

    async def search_advisers(
        self,
        name: Optional[str] = None,
        crd: Optional[str] = None,
        state: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Search for investment advisers via IAPD API.

        Args:
            name: Adviser name to search
            crd: CRD number
            state: State code (e.g., "NY")
            page: Page number
            page_size: Results per page

        Returns:
            Search results
        """
        await self._rate_limit()

        # Build search query
        params = {
            "page": page,
            "pageSize": page_size,
        }

        if name:
            params["name"] = name
        if crd:
            params["crd"] = crd
        if state:
            params["state"] = state

        url = f"{self.IAPD_API_URL}/search/adviser"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, params=params, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"IAPD search returned {response.status_code}")
            except Exception as e:
                logger.warning(f"IAPD search failed: {e}")

        return {"hits": [], "total": 0}

    async def get_adviser_details(self, crd_number: str) -> Optional[Dict]:
        """
        Get detailed information for a specific adviser.

        Args:
            crd_number: CRD number

        Returns:
            Adviser details or None
        """
        await self._rate_limit()

        url = f"{self.IAPD_API_URL}/adviser/{crd_number}"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                logger.warning(f"Failed to get adviser {crd_number}: {e}")

        return None

    async def get_adviser_brochure_url(self, crd_number: str) -> Optional[str]:
        """
        Get URL to adviser's Form ADV Part 2 brochure PDF.

        Args:
            crd_number: CRD number

        Returns:
            URL to brochure PDF
        """
        return f"https://adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BESSION_ID={crd_number}"

    def get_sample_advisers(self) -> List[Dict]:
        """
        Return sample adviser data for testing/demo.

        In production, this would fetch from SEC quarterly files.
        """
        return [
            {
                "crd_number": "106222",
                "sec_number": "801-60166",
                "legal_name": "BlackRock Advisors, LLC",
                "dba_name": "BlackRock",
                "main_office_city": "New York",
                "main_office_state": "NY",
                "regulatory_aum": 10000000000000,  # $10T
                "discretionary_aum": 9500000000000,
                "total_employees": 16000,
                "employees_investment_advisory": 3000,
                "sec_registered": True,
                "form_of_organization": "Limited Liability Company",
                "pct_pooled_investment_vehicles": 60,
                "pct_pension_plans": 20,
                "pct_high_net_worth": 10,
                "pct_other": 10,
            },
            {
                "crd_number": "108652",
                "sec_number": "801-62786",
                "legal_name": "Vanguard Group Inc",
                "dba_name": "Vanguard",
                "main_office_city": "Malvern",
                "main_office_state": "PA",
                "regulatory_aum": 8000000000000,  # $8T
                "discretionary_aum": 7800000000000,
                "total_employees": 18000,
                "employees_investment_advisory": 2500,
                "sec_registered": True,
                "form_of_organization": "Corporation",
                "pct_pooled_investment_vehicles": 70,
                "pct_individuals": 15,
                "pct_pension_plans": 10,
                "pct_other": 5,
            },
            {
                "crd_number": "105933",
                "sec_number": "801-49947",
                "legal_name": "Fidelity Management & Research Company LLC",
                "dba_name": "Fidelity",
                "main_office_city": "Boston",
                "main_office_state": "MA",
                "regulatory_aum": 4500000000000,  # $4.5T
                "discretionary_aum": 4400000000000,
                "total_employees": 12000,
                "employees_investment_advisory": 2000,
                "sec_registered": True,
                "form_of_organization": "Limited Liability Company",
                "pct_pooled_investment_vehicles": 65,
                "pct_individuals": 20,
                "pct_pension_plans": 10,
                "pct_other": 5,
            },
            {
                "crd_number": "112170",
                "sec_number": "801-68706",
                "legal_name": "State Street Global Advisors",
                "dba_name": "SSGA",
                "main_office_city": "Boston",
                "main_office_state": "MA",
                "regulatory_aum": 3900000000000,
                "discretionary_aum": 3800000000000,
                "total_employees": 2500,
                "employees_investment_advisory": 800,
                "sec_registered": True,
                "form_of_organization": "Trust Company",
                "pct_pooled_investment_vehicles": 55,
                "pct_pension_plans": 25,
                "pct_other": 20,
            },
            {
                "crd_number": "104859",
                "sec_number": "801-35705",
                "legal_name": "JPMorgan Chase & Co",
                "dba_name": "JPMorgan Asset Management",
                "main_office_city": "New York",
                "main_office_state": "NY",
                "regulatory_aum": 3000000000000,
                "discretionary_aum": 2900000000000,
                "total_employees": 8000,
                "employees_investment_advisory": 1500,
                "sec_registered": True,
                "form_of_organization": "Corporation",
                "pct_pooled_investment_vehicles": 50,
                "pct_pension_plans": 20,
                "pct_high_net_worth": 15,
                "pct_other": 15,
            },
            {
                "crd_number": "116839",
                "sec_number": "801-71988",
                "legal_name": "Capital Group",
                "dba_name": "American Funds",
                "main_office_city": "Los Angeles",
                "main_office_state": "CA",
                "regulatory_aum": 2500000000000,
                "discretionary_aum": 2400000000000,
                "total_employees": 8500,
                "employees_investment_advisory": 1200,
                "sec_registered": True,
                "form_of_organization": "Limited Partnership",
                "pct_pooled_investment_vehicles": 75,
                "pct_individuals": 15,
                "pct_other": 10,
            },
            {
                "crd_number": "128415",
                "sec_number": "801-80289",
                "legal_name": "Bridgewater Associates, LP",
                "dba_name": "Bridgewater",
                "main_office_city": "Westport",
                "main_office_state": "CT",
                "regulatory_aum": 150000000000,  # $150B
                "discretionary_aum": 145000000000,
                "total_employees": 1500,
                "employees_investment_advisory": 400,
                "sec_registered": True,
                "form_of_organization": "Limited Partnership",
                "pct_pooled_investment_vehicles": 80,
                "pct_pension_plans": 15,
                "pct_other": 5,
            },
            {
                "crd_number": "111451",
                "sec_number": "801-67849",
                "legal_name": "Two Sigma Investments, LP",
                "dba_name": "Two Sigma",
                "main_office_city": "New York",
                "main_office_state": "NY",
                "regulatory_aum": 60000000000,  # $60B
                "discretionary_aum": 58000000000,
                "total_employees": 1800,
                "employees_investment_advisory": 300,
                "sec_registered": True,
                "form_of_organization": "Limited Partnership",
                "pct_pooled_investment_vehicles": 90,
                "pct_other": 10,
            },
            {
                "crd_number": "149553",
                "sec_number": "801-107084",
                "legal_name": "Citadel Advisors LLC",
                "dba_name": "Citadel",
                "main_office_city": "Chicago",
                "main_office_state": "IL",
                "regulatory_aum": 55000000000,
                "discretionary_aum": 54000000000,
                "total_employees": 1000,
                "employees_investment_advisory": 250,
                "sec_registered": True,
                "form_of_organization": "Limited Liability Company",
                "pct_pooled_investment_vehicles": 95,
                "pct_other": 5,
            },
            {
                "crd_number": "111122",
                "sec_number": "801-67547",
                "legal_name": "AQR Capital Management, LLC",
                "dba_name": "AQR",
                "main_office_city": "Greenwich",
                "main_office_state": "CT",
                "regulatory_aum": 100000000000,  # $100B
                "discretionary_aum": 98000000000,
                "total_employees": 1000,
                "employees_investment_advisory": 350,
                "sec_registered": True,
                "form_of_organization": "Limited Liability Company",
                "pct_pooled_investment_vehicles": 70,
                "pct_pension_plans": 20,
                "pct_other": 10,
            },
        ]
