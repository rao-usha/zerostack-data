"""
US Census Bureau International Trade API client.

Data sources:
- Census Bureau International Trade API: https://api.census.gov/data/timeseries.html

Available datasets:
- Exports by HS code: https://api.census.gov/data/timeseries/intltrade/exports/hs
- Imports by HS code: https://api.census.gov/data/timeseries/intltrade/imports/hs
- Exports by state: https://api.census.gov/data/timeseries/intltrade/exports/statehs
- Trade by port/district: https://api.census.gov/data/timeseries/intltrade/exports/porths

Rate limits:
- No API key required for basic access
- 500 queries per IP per day without key
- With API key: unlimited (optional, from census.gov/developers)

Time coverage: 2013 - present (monthly data)
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class USTradeClient(BaseAPIClient):
    """
    HTTP client for US Census Bureau International Trade API.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "us_trade"
    BASE_URL = "https://api.census.gov/data/timeseries/intltrade"

    ENDPOINTS = {
        "exports_hs": "/exports/hs",
        "imports_hs": "/imports/hs",
        "exports_state": "/exports/statehs",
        "exports_port": "/exports/porths",
        "imports_port": "/imports/porths",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize US Trade API client.

        Args:
            api_key: Optional Census API key
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("us_trade")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=120.0,
            connect_timeout=30.0,
            rate_limit_interval=2.0  # Conservative without API key
        )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key if available."""
        if self.api_key:
            params["key"] = self.api_key
        return params

    async def get_exports_by_hs(
        self,
        year: int,
        month: Optional[int] = None,
        hs_code: Optional[str] = None,
        country: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get US export data by Harmonized System (HS) code."""
        if fields is None:
            fields = [
                "CTY_CODE", "CTY_NAME",
                "E_COMMODITY", "E_COMMODITY_LDESC",
                "ALL_VAL_MO", "ALL_VAL_YR",
                "QTY_1_MO", "QTY_1_YR",
                "UNIT_QY1"
            ]

        params = self._build_export_params(year, month, hs_code, country, fields)
        data = await self.get(
            self.ENDPOINTS["exports_hs"],
            params=params,
            resource_id=f"exports_hs:{year}"
        )
        return self._parse_census_response(data)

    async def get_imports_by_hs(
        self,
        year: int,
        month: Optional[int] = None,
        hs_code: Optional[str] = None,
        country: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get US import data by Harmonized System (HS) code."""
        if fields is None:
            fields = [
                "CTY_CODE", "CTY_NAME",
                "I_COMMODITY", "I_COMMODITY_LDESC",
                "GEN_VAL_MO", "GEN_VAL_YR",
                "CON_VAL_MO", "CON_VAL_YR",
            ]

        params = self._build_import_params(year, month, hs_code, country, fields)
        data = await self.get(
            self.ENDPOINTS["imports_hs"],
            params=params,
            resource_id=f"imports_hs:{year}"
        )
        return self._parse_census_response(data)

    async def get_exports_by_state(
        self,
        year: int,
        month: Optional[int] = None,
        state: Optional[str] = None,
        hs_code: Optional[str] = None,
        country: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get US state-level export data."""
        if fields is None:
            fields = ["STATE", "CTY_CODE", "CTY_NAME", "E_COMMODITY", "ALL_VAL_MO", "ALL_VAL_YR"]

        params = self._build_export_params(year, month, hs_code, country, fields)
        if state:
            params["STATE"] = state

        data = await self.get(
            self.ENDPOINTS["exports_state"],
            params=params,
            resource_id=f"exports_state:{year}"
        )
        return self._parse_census_response(data)

    def _build_export_params(
        self,
        year: int,
        month: Optional[int],
        hs_code: Optional[str],
        country: Optional[str],
        fields: List[str]
    ) -> Dict[str, str]:
        """Build query parameters for Census Export API."""
        params = {"get": ",".join(fields)}

        if month:
            params["time"] = f"{year}-{str(month).zfill(2)}"
        else:
            params["time"] = f"{year}-12"

        if hs_code:
            params["E_COMMODITY"] = hs_code
            if len(hs_code) == 2:
                params["COMM_LVL"] = "HS2"
            elif len(hs_code) == 4:
                params["COMM_LVL"] = "HS4"
            elif len(hs_code) == 6:
                params["COMM_LVL"] = "HS6"
            else:
                params["COMM_LVL"] = "HS10"
        else:
            params["COMM_LVL"] = "HS2"

        if country:
            params["CTY_CODE"] = country

        return params

    def _build_import_params(
        self,
        year: int,
        month: Optional[int],
        hs_code: Optional[str],
        country: Optional[str],
        fields: List[str]
    ) -> Dict[str, str]:
        """Build query parameters for Census Import API."""
        params = {"get": ",".join(fields)}

        if month:
            params["time"] = f"{year}-{str(month).zfill(2)}"
        else:
            params["time"] = f"{year}-12"

        if hs_code:
            params["I_COMMODITY"] = hs_code
            if len(hs_code) == 2:
                params["COMM_LVL"] = "HS2"
            elif len(hs_code) == 4:
                params["COMM_LVL"] = "HS4"
            elif len(hs_code) == 6:
                params["COMM_LVL"] = "HS6"
            else:
                params["COMM_LVL"] = "HS10"
        else:
            params["COMM_LVL"] = "HS2"

        if country:
            params["CTY_CODE"] = country

        return params

    def _parse_census_response(self, data: Any) -> List[Dict[str, Any]]:
        """Parse Census API response (array of arrays) to list of dicts."""
        if not data or len(data) < 2:
            return []
        headers = data[0]
        return [dict(zip(headers, row)) for row in data[1:]]


# Major HS code chapters (2-digit)
HS_CHAPTERS = {
    "27": "Mineral fuels, oils",
    "84": "Nuclear reactors, boilers, machinery",
    "85": "Electrical machinery and equipment",
    "87": "Vehicles other than railway",
    "90": "Optical, photographic instruments",
}

# Top US trading partners (Census country codes)
TOP_TRADING_PARTNERS = {
    "5700": "China",
    "2010": "Mexico",
    "1220": "Canada",
}
