"""
BEA API client with rate limiting and retry logic.

Official BEA API documentation:
https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

BEA API provides access to Bureau of Economic Analysis data:
- NIPA: National Income and Product Accounts (GDP, PCE, etc.)
- Regional: State/county/metro economic data
- International: Trade and investment data
- Industry: Input-output tables, GDP by industry

Rate limits:
- 100 requests per minute per UserID
- 100 MB data volume per minute
- API key required (free registration)
"""

import logging
from typing import Dict, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class BEAClient(BaseAPIClient):
    """
    HTTP client for BEA API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "bea"
    BASE_URL = "https://apps.bea.gov/api/data"

    # Available datasets
    DATASETS = {
        "NIPA": "National Income and Product Accounts",
        "NIUnderlyingDetail": "NIPA Underlying Detail Tables",
        "Regional": "Regional Economic Accounts",
        "International": "International Transactions and Investment",
        "IntlServTrade": "International Services Trade",
        "GDPbyIndustry": "GDP by Industry",
        "InputOutput": "Input-Output Tables",
        "UnderlyingGDPbyIndustry": "Underlying GDP by Industry",
        "ITA": "International Transactions Accounts",
        "IIP": "International Investment Position",
    }

    def __init__(
        self,
        api_key: str,
        max_concurrency: int = 3,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize BEA API client.

        Args:
            api_key: BEA API key (UserID) - required
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        if not api_key:
            raise ValueError(
                "BEA_API_KEY is required. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )

        config = get_api_config("bea")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters."""
        params["UserID"] = self.api_key
        params["ResultFormat"] = "JSON"
        return params

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for BEA-specific API errors."""
        if "BEAAPI" in data:
            results = data["BEAAPI"].get("Results", {})

            if "Error" in results:
                error = results["Error"]
                error_msg = error.get("ErrorDetail", {}).get("Description", str(error))

                logger.warning(f"BEA API error: {error_msg}")

                # Check if it's a rate limit error
                if "rate" in error_msg.lower() or "limit" in error_msg.lower():
                    return RetryableError(
                        message=f"BEA rate limit: {error_msg}",
                        source=self.SOURCE_NAME,
                        response_data=data,
                    )

                return FatalError(
                    message=f"BEA API error: {error_msg}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )

        return None

    async def get_nipa_data(
        self, table_name: str, frequency: str = "A", year: str = "ALL"
    ) -> Dict[str, Any]:
        """
        Fetch NIPA (National Income and Product Accounts) data.

        Args:
            table_name: NIPA table name (e.g., "T10101" for GDP)
            frequency: A (annual), Q (quarterly), M (monthly)
            year: Year(s) to retrieve - "ALL", single year, or comma-separated
        """
        params = {
            "method": "GetData",
            "DataSetName": "NIPA",
            "TableName": table_name,
            "Frequency": frequency,
            "Year": year,
        }
        return await self.get("", params=params, resource_id=f"NIPA:{table_name}")

    async def get_nipa_tables_list(self) -> Dict[str, Any]:
        """Get list of available NIPA tables."""
        params = {
            "method": "GetParameterValues",
            "DataSetName": "NIPA",
            "ParameterName": "TableName",
        }
        return await self.get("", params=params, resource_id="NIPA:TableList")

    async def get_regional_data(
        self,
        table_name: str,
        line_code: str = "1",
        geo_fips: str = "STATE",
        year: str = "ALL",
    ) -> Dict[str, Any]:
        """
        Fetch Regional Economic Accounts data.

        Args:
            table_name: Regional table name (e.g., "SAGDP2N" for GDP by state)
            line_code: Line code for specific measure
            geo_fips: Geographic area - "STATE", "COUNTY", "MSA", or specific FIPS
            year: Year(s) to retrieve
        """
        params = {
            "method": "GetData",
            "DataSetName": "Regional",
            "TableName": table_name,
            "LineCode": line_code,
            "GeoFips": geo_fips,
            "Year": year,
        }
        return await self.get("", params=params, resource_id=f"Regional:{table_name}")

    async def get_regional_tables_list(self) -> Dict[str, Any]:
        """Get list of available Regional tables."""
        params = {
            "method": "GetParameterValues",
            "DataSetName": "Regional",
            "ParameterName": "TableName",
        }
        return await self.get("", params=params, resource_id="Regional:TableList")

    async def get_gdp_by_industry(
        self,
        table_id: str = "1",
        frequency: str = "A",
        year: str = "ALL",
        industry: str = "ALL",
    ) -> Dict[str, Any]:
        """Fetch GDP by Industry data."""
        params = {
            "method": "GetData",
            "DataSetName": "GDPbyIndustry",
            "TableID": table_id,
            "Frequency": frequency,
            "Year": year,
            "Industry": industry,
        }
        return await self.get(
            "", params=params, resource_id=f"GDPbyIndustry:{table_id}"
        )

    async def get_international_transactions(
        self,
        indicator: str = "BalGds",
        area_or_country: str = "AllCountries",
        frequency: str = "A",
        year: str = "ALL",
    ) -> Dict[str, Any]:
        """Fetch International Transactions data."""
        params = {
            "method": "GetData",
            "DataSetName": "ITA",
            "Indicator": indicator,
            "AreaOrCountry": area_or_country,
            "Frequency": frequency,
            "Year": year,
        }
        return await self.get("", params=params, resource_id=f"ITA:{indicator}")

    async def get_input_output_data(
        self, table_id: str = "56", year: str = "ALL"
    ) -> Dict[str, Any]:
        """Fetch Input-Output Tables data."""
        params = {
            "method": "GetData",
            "DataSetName": "InputOutput",
            "TableID": table_id,
            "Year": year,
        }
        return await self.get("", params=params, resource_id=f"InputOutput:{table_id}")

    async def get_dataset_list(self) -> Dict[str, Any]:
        """Get list of all available BEA datasets."""
        params = {"method": "GetDataSetList"}
        return await self.get("", params=params, resource_id="DataSetList")

    async def get_parameter_list(self, dataset_name: str) -> Dict[str, Any]:
        """Get list of parameters for a dataset."""
        params = {
            "method": "GetParameterList",
            "DataSetName": dataset_name,
        }
        return await self.get(
            "", params=params, resource_id=f"ParameterList:{dataset_name}"
        )

    async def get_parameter_values(
        self, dataset_name: str, parameter_name: str
    ) -> Dict[str, Any]:
        """Get valid values for a specific parameter."""
        params = {
            "method": "GetParameterValues",
            "DataSetName": dataset_name,
            "ParameterName": parameter_name,
        }
        return await self.get(
            "",
            params=params,
            resource_id=f"ParameterValues:{dataset_name}:{parameter_name}",
        )


# Common NIPA table references
NIPA_TABLES = {
    "T10101": "Gross Domestic Product",
    "T10105": "Gross Domestic Product, Percent Change",
    "T10106": "Real Gross Domestic Product, Chained Dollars",
    "T10107": "Real Gross Domestic Product, Percent Change",
    "T20100": "Personal Income and Its Disposition",
    "T20200": "Personal Consumption Expenditures by Major Type of Product",
    "T30100": "Government Current Receipts and Expenditures",
    "T50100": "Saving and Investment",
    "T60100": "Corporate Profits by Industry",
    "T10109": "Implicit Price Deflators for GDP",
}

# Common Regional table references
REGIONAL_TABLES = {
    "SAGDP2N": "GDP by State (All Industries)",
    "SAGDP9N": "Real GDP by State",
    "SAINC1": "Personal Income by State",
    "SAINC4": "Personal Income and Employment by State",
    "CAINC1": "Personal Income by County",
    "CAGDP2": "GDP by County",
}
