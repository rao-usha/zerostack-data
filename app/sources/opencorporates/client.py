"""
OpenCorporates API client.

Provides access to global company registry data from 140+ jurisdictions.
"""

import os
import httpx
from typing import Optional
from datetime import datetime


class OpenCorporatesClient:
    """Client for OpenCorporates API."""

    BASE_URL = "https://api.opencorporates.com/v0.4"

    def __init__(self, api_token: str = None):
        """Initialize client with optional API token."""
        self.api_token = api_token or os.getenv("OPENCORPORATES_API_KEY")
        self.client = httpx.Client(timeout=30.0)

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """Make request to OpenCorporates API."""
        url = f"{self.BASE_URL}{endpoint}"
        params = params or {}

        if self.api_token:
            params["api_token"] = self.api_token

        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _parse_company(self, company_data: dict) -> dict:
        """Parse company data from API response."""
        company = company_data.get("company", company_data)
        return {
            "name": company.get("name"),
            "company_number": company.get("company_number"),
            "jurisdiction_code": company.get("jurisdiction_code"),
            "incorporation_date": company.get("incorporation_date"),
            "dissolution_date": company.get("dissolution_date"),
            "company_type": company.get("company_type"),
            "current_status": company.get("current_status"),
            "registry_url": company.get("registry_url"),
            "opencorporates_url": company.get("opencorporates_url"),
            "registered_address": self._parse_address(company.get("registered_address")),
            "agent_name": company.get("agent_name"),
            "agent_address": company.get("agent_address"),
            "previous_names": company.get("previous_names", []),
            "source": company.get("source", {}).get("publisher"),
            "retrieved_at": company.get("retrieved_at"),
        }

    def _parse_address(self, address_data) -> Optional[dict]:
        """Parse address from API response."""
        if not address_data:
            return None
        if isinstance(address_data, str):
            return {"full_address": address_data}
        return {
            "street": address_data.get("street_address"),
            "city": address_data.get("locality"),
            "region": address_data.get("region"),
            "postal_code": address_data.get("postal_code"),
            "country": address_data.get("country"),
            "full_address": address_data.get("full_address") or address_data.get("street_address"),
        }

    def _parse_officer(self, officer_data: dict) -> dict:
        """Parse officer data from API response."""
        officer = officer_data.get("officer", officer_data)
        return {
            "name": officer.get("name"),
            "position": officer.get("position"),
            "start_date": officer.get("start_date"),
            "end_date": officer.get("end_date"),
            "nationality": officer.get("nationality"),
            "occupation": officer.get("occupation"),
            "date_of_birth": officer.get("date_of_birth"),
            "address": officer.get("address"),
            "opencorporates_url": officer.get("opencorporates_url"),
        }

    def _parse_filing(self, filing_data: dict) -> dict:
        """Parse filing data from API response."""
        filing = filing_data.get("filing", filing_data)
        return {
            "title": filing.get("title"),
            "filing_type": filing.get("filing_type") or filing.get("filing_type_name"),
            "date": filing.get("date"),
            "description": filing.get("description"),
            "url": filing.get("url"),
            "opencorporates_url": filing.get("opencorporates_url"),
        }

    def search_companies(
        self,
        query: str,
        jurisdiction: str = None,
        company_type: str = None,
        current_status: str = None,
        page: int = 1,
        per_page: int = 30,
    ) -> dict:
        """
        Search companies by name.

        Args:
            query: Company name search query
            jurisdiction: Filter by jurisdiction code (e.g., 'us_de', 'gb')
            company_type: Filter by company type
            current_status: Filter by status (e.g., 'Active', 'Dissolved')
            page: Page number (1-indexed)
            per_page: Results per page (max 100)

        Returns:
            Dict with companies list and pagination info
        """
        params = {
            "q": query,
            "page": page,
            "per_page": min(per_page, 100),
        }

        if jurisdiction:
            params["jurisdiction_code"] = jurisdiction
        if company_type:
            params["company_type"] = company_type
        if current_status:
            params["current_status"] = current_status

        try:
            data = self._make_request("/companies/search", params)
            results = data.get("results", {})
            companies = results.get("companies", [])

            return {
                "companies": [self._parse_company(c) for c in companies],
                "total_count": results.get("total_count", 0),
                "page": results.get("page", page),
                "per_page": results.get("per_page", per_page),
                "total_pages": results.get("total_pages", 1),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {
                    "companies": [],
                    "total_count": 0,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": 0,
                }
            raise

    def get_company(self, jurisdiction: str, company_number: str) -> Optional[dict]:
        """
        Get company details by jurisdiction and company number.

        Args:
            jurisdiction: Jurisdiction code (e.g., 'us_de', 'gb')
            company_number: Company registration number

        Returns:
            Company details dict or None if not found
        """
        try:
            data = self._make_request(f"/companies/{jurisdiction}/{company_number}")
            results = data.get("results", {})
            company_data = results.get("company", {})

            if not company_data:
                return None

            parsed = self._parse_company(company_data)
            parsed["officers_count"] = len(company_data.get("officers", []))
            parsed["filings_count"] = len(company_data.get("filings", []))

            return parsed
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_company_officers(
        self,
        jurisdiction: str,
        company_number: str,
        page: int = 1,
        per_page: int = 30,
    ) -> dict:
        """
        Get officers for a company.

        Args:
            jurisdiction: Jurisdiction code
            company_number: Company registration number
            page: Page number
            per_page: Results per page

        Returns:
            Dict with officers list and count
        """
        params = {"page": page, "per_page": min(per_page, 100)}

        try:
            data = self._make_request(
                f"/companies/{jurisdiction}/{company_number}/officers",
                params
            )
            results = data.get("results", {})
            officers = results.get("officers", [])

            return {
                "officers": [self._parse_officer(o) for o in officers],
                "total_count": results.get("total_count", len(officers)),
                "page": results.get("page", page),
                "per_page": results.get("per_page", per_page),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {
                    "officers": [],
                    "total_count": 0,
                    "page": page,
                    "per_page": per_page,
                }
            raise

    def get_company_filings(
        self,
        jurisdiction: str,
        company_number: str,
        page: int = 1,
        per_page: int = 30,
    ) -> dict:
        """
        Get filings for a company.

        Args:
            jurisdiction: Jurisdiction code
            company_number: Company registration number
            page: Page number
            per_page: Results per page

        Returns:
            Dict with filings list and count
        """
        params = {"page": page, "per_page": min(per_page, 100)}

        try:
            data = self._make_request(
                f"/companies/{jurisdiction}/{company_number}/filings",
                params
            )
            results = data.get("results", {})
            filings = results.get("filings", [])

            return {
                "filings": [self._parse_filing(f) for f in filings],
                "total_count": results.get("total_count", len(filings)),
                "page": results.get("page", page),
                "per_page": results.get("per_page", per_page),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {
                    "filings": [],
                    "total_count": 0,
                    "page": page,
                    "per_page": per_page,
                }
            raise

    def search_officers(
        self,
        query: str,
        jurisdiction: str = None,
        page: int = 1,
        per_page: int = 30,
    ) -> dict:
        """
        Search officers by name.

        Args:
            query: Officer name search query
            jurisdiction: Filter by jurisdiction code
            page: Page number
            per_page: Results per page

        Returns:
            Dict with officers list and pagination info
        """
        params = {
            "q": query,
            "page": page,
            "per_page": min(per_page, 100),
        }

        if jurisdiction:
            params["jurisdiction_code"] = jurisdiction

        try:
            data = self._make_request("/officers/search", params)
            results = data.get("results", {})
            officers = results.get("officers", [])

            return {
                "officers": [self._parse_officer(o) for o in officers],
                "total_count": results.get("total_count", 0),
                "page": results.get("page", page),
                "per_page": results.get("per_page", per_page),
                "total_pages": results.get("total_pages", 1),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {
                    "officers": [],
                    "total_count": 0,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": 0,
                }
            raise

    def get_jurisdictions(self) -> list:
        """
        Get list of available jurisdictions.

        Returns:
            List of jurisdiction dicts with code and name
        """
        try:
            data = self._make_request("/jurisdictions")
            results = data.get("results", {})
            jurisdictions = results.get("jurisdictions", [])

            return [
                {
                    "code": j.get("jurisdiction", {}).get("code"),
                    "name": j.get("jurisdiction", {}).get("name"),
                    "country": j.get("jurisdiction", {}).get("country"),
                    "full_name": j.get("jurisdiction", {}).get("full_name"),
                }
                for j in jurisdictions
            ]
        except httpx.HTTPStatusError:
            # Return common jurisdictions as fallback
            return [
                {"code": "us_de", "name": "Delaware", "country": "United States"},
                {"code": "us_ca", "name": "California", "country": "United States"},
                {"code": "us_ny", "name": "New York", "country": "United States"},
                {"code": "gb", "name": "United Kingdom", "country": "United Kingdom"},
                {"code": "ie", "name": "Ireland", "country": "Ireland"},
                {"code": "de", "name": "Germany", "country": "Germany"},
                {"code": "fr", "name": "France", "country": "France"},
                {"code": "nl", "name": "Netherlands", "country": "Netherlands"},
                {"code": "sg", "name": "Singapore", "country": "Singapore"},
                {"code": "hk", "name": "Hong Kong", "country": "Hong Kong"},
            ]

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
