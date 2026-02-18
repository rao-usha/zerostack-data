"""
OpenFEMA API client with rate limiting and retry logic.

Official OpenFEMA API documentation:
https://www.fema.gov/about/openfema/api

OpenFEMA provides access to FEMA disaster and emergency data:
- Disaster Declarations (1953-present)
- Public Assistance funded projects
- Hazard Mitigation Assistance projects
- NFIP flood insurance policies and claims

Rate limits:
- No official limit, but be respectful (~60 req/min recommended)
- No API key required (free public API)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class FEMAClient(BaseAPIClient):
    """
    HTTP client for OpenFEMA API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fema"
    BASE_URL = "https://www.fema.gov/api/open/v2"

    # Available datasets
    DATASETS = {
        "DisasterDeclarationsSummaries": "All federally declared disasters since 1953",
        "FemaWebDisasterSummaries": "Web-friendly disaster summaries",
        "PublicAssistanceFundedProjectsDetails": "PA funded project details",
        "HazardMitigationAssistanceProjects": "HMA mitigation projects",
        "FimaNfipPolicies": "NFIP flood insurance policies",
        "FimaNfipClaims": "NFIP flood insurance claims",
    }

    def __init__(
        self,
        max_concurrency: int = 3,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize OpenFEMA client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("fema")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    async def get_disaster_declarations(
        self,
        skip: int = 0,
        top: int = 1000,
        state: Optional[str] = None,
        year: Optional[int] = None,
        disaster_type: Optional[str] = None,
        order_by: str = "declarationDate desc",
    ) -> Dict[str, Any]:
        """
        Fetch disaster declarations from OpenFEMA.

        Args:
            skip: Number of records to skip (pagination)
            top: Number of records to return (max 1000)
            state: Filter by state code (e.g., "TX", "CA")
            year: Filter by fiscal year
            disaster_type: Filter by type (DR, EM, FM)
            order_by: Sort order
        """
        params = {
            "$skip": skip,
            "$top": min(top, 1000),
            "$orderby": order_by,
            "$inlinecount": "allpages",
        }

        filters = []
        if state:
            filters.append(f"state eq '{state}'")
        if year:
            filters.append(f"fyDeclared eq {year}")
        if disaster_type:
            filters.append(f"declarationType eq '{disaster_type}'")

        if filters:
            params["$filter"] = " and ".join(filters)

        return await self.get(
            "DisasterDeclarationsSummaries",
            params=params,
            resource_id="DisasterDeclarations",
        )

    async def get_all_disaster_declarations(
        self,
        state: Optional[str] = None,
        year: Optional[int] = None,
        disaster_type: Optional[str] = None,
        max_records: int = 50000,
    ) -> List[Dict[str, Any]]:
        """Fetch all disaster declarations with automatic pagination."""
        all_records = []
        skip = 0
        top = 1000

        while len(all_records) < max_records:
            response = await self.get_disaster_declarations(
                skip=skip, top=top, state=state, year=year, disaster_type=disaster_type
            )

            records = response.get("DisasterDeclarationsSummaries", [])
            if not records:
                break

            all_records.extend(records)
            logger.info(
                f"Fetched {len(records)} disaster records (total: {len(all_records)})"
            )

            metadata = response.get("metadata", {})
            total_count = metadata.get("count", 0)

            if len(all_records) >= total_count or len(records) < top:
                break

            skip += top

        return all_records[:max_records]

    async def get_public_assistance_projects(
        self,
        skip: int = 0,
        top: int = 1000,
        state: Optional[str] = None,
        disaster_number: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Fetch Public Assistance funded project details."""
        params = {"$skip": skip, "$top": min(top, 1000), "$inlinecount": "allpages"}

        filters = []
        if state:
            filters.append(f"state eq '{state}'")
        if disaster_number:
            filters.append(f"disasterNumber eq {disaster_number}")

        if filters:
            params["$filter"] = " and ".join(filters)

        return await self.get(
            "PublicAssistanceFundedProjectsDetails",
            params=params,
            resource_id="PublicAssistance",
        )

    async def get_hazard_mitigation_projects(
        self,
        skip: int = 0,
        top: int = 1000,
        state: Optional[str] = None,
        program_area: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch Hazard Mitigation Assistance projects."""
        # HMA Projects is available in v4
        url = "https://www.fema.gov/api/open/v4/HazardMitigationAssistanceProjects"

        params = {"$skip": skip, "$top": min(top, 1000), "$inlinecount": "allpages"}

        filters = []
        if state:
            filters.append(f"state eq '{state}'")
        if program_area:
            filters.append(f"programArea eq '{program_area}'")

        if filters:
            params["$filter"] = " and ".join(filters)

        return await self.get(url, params=params, resource_id="HazardMitigation")

    async def get_nfip_policies(
        self, skip: int = 0, top: int = 1000, state: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch NFIP flood insurance policies."""
        params = {"$skip": skip, "$top": min(top, 1000), "$inlinecount": "allpages"}

        if state:
            params["$filter"] = f"propertyState eq '{state}'"

        return await self.get(
            "FimaNfipPolicies", params=params, resource_id="NFIPPolicies"
        )

    async def get_nfip_claims(
        self,
        skip: int = 0,
        top: int = 1000,
        state: Optional[str] = None,
        year_of_loss: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Fetch NFIP flood insurance claims."""
        params = {"$skip": skip, "$top": min(top, 1000), "$inlinecount": "allpages"}

        filters = []
        if state:
            filters.append(f"state eq '{state}'")
        if year_of_loss:
            filters.append(f"yearOfLoss eq {year_of_loss}")

        if filters:
            params["$filter"] = " and ".join(filters)

        return await self.get("FimaNfipClaims", params=params, resource_id="NFIPClaims")

    async def get_web_disaster_summaries(
        self, skip: int = 0, top: int = 1000
    ) -> Dict[str, Any]:
        """Fetch web-friendly disaster summaries."""
        params = {
            "$skip": skip,
            "$top": min(top, 1000),
            "$inlinecount": "allpages",
            "$orderby": "declarationDate desc",
        }
        return await self.get(
            "FemaWebDisasterSummaries",
            params=params,
            resource_id="WebDisasterSummaries",
        )


# Disaster type codes
DISASTER_TYPES = {
    "DR": "Major Disaster Declaration",
    "EM": "Emergency Declaration",
    "FM": "Fire Management Assistance",
}

# Program areas for Hazard Mitigation
HMA_PROGRAMS = {
    "HMGP": "Hazard Mitigation Grant Program",
    "PDM": "Pre-Disaster Mitigation",
    "FMA": "Flood Mitigation Assistance",
    "RFC": "Repetitive Flood Claims",
    "SRL": "Severe Repetitive Loss",
}

# US State codes
US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "PR",
    "VI",
    "GU",
    "AS",
    "MP",
]
