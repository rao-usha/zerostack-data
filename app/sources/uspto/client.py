"""
PatentsView API client with rate limiting and retry logic.

Official PatentsView API documentation:
https://search.patentsview.org/docs/docs/Search%20API/SearchAPIReference/

PatentsView provides access to USPTO patent data:
- Granted patents (through Sept 2025)
- Pre-grant publications
- Inventors (disambiguated)
- Assignees (disambiguated)
- Classifications (CPC, IPC, USPC, WIPO)
- Citation networks

Rate limits:
- 45 requests per minute per API key
- Maximum 1,000 records per request (default 100)
- API key required (free, doesn't expire)

API key signup: https://patentsview-support.atlassian.net/servicedesk/customer/portal/1
"""

import logging
import json
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class USPTOClient(BaseAPIClient):
    """
    HTTP client for PatentsView API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "uspto"
    BASE_URL = "https://search.patentsview.org/api/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize PatentsView API client.

        Args:
            api_key: PatentsView API key (required for production use)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("uspto")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

        if not api_key:
            logger.warning(
                "USPTO PatentsView API key not provided. "
                f"Get a free key at: {config.signup_url}"
            )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with API key."""
        headers = {"Accept": "application/json", "User-Agent": "Nexdata/uspto-client"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for PatentsView-specific API errors."""
        # PatentsView returns {"error": true, "message": "..."} on errors
        if data.get("error") is True:
            error_message = data.get("message", "Unknown API error")
            logger.warning(f"PatentsView API error: {error_message}")
            return FatalError(
                message=f"PatentsView API error: {error_message}",
                source=self.SOURCE_NAME,
                response_data=data,
            )
        return None

    def _build_query(
        self,
        filters: Optional[Dict[str, Any]] = None,
        text_search: Optional[str] = None,
        text_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Build PatentsView query object.

        Args:
            filters: Field-based filters (e.g., {"patent_date": {"_gte": "2020-01-01"}})
            text_search: Full-text search string
            text_fields: Fields to search (defaults to title and abstract)

        Returns:
            Query object for PatentsView API
        """
        conditions = []

        # Add text search if provided
        if text_search:
            search_fields = text_fields or ["patent_title", "patent_abstract"]
            for field in search_fields:
                conditions.append({field: {"_text_any": text_search}})

        # Add filters
        if filters:
            for field, condition in filters.items():
                conditions.append({field: condition})

        # Combine with _and if multiple conditions
        if len(conditions) == 0:
            return {}
        elif len(conditions) == 1:
            return conditions[0]
        else:
            return {"_and": conditions}

    async def search_patents(
        self,
        query: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
        size: int = 100,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search patents using PatentsView API.

        Args:
            query: Query object (see _build_query)
            fields: Fields to return (None = defaults)
            sort: Sort order [{"field": "asc|desc"}]
            size: Number of results (max 1000)
            after: Cursor for pagination

        Returns:
            Dict containing patents and pagination info
        """
        params = {}

        if query:
            params["q"] = json.dumps(query)

        if fields:
            params["f"] = json.dumps(fields)

        if sort:
            params["s"] = json.dumps(sort)

        options = {"size": min(size, 1000)}
        if after:
            options["after"] = after
        params["o"] = json.dumps(options)

        return await self.get("patent/", params=params, resource_id="patent_search")

    async def get_patent(
        self, patent_id: str, fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get a single patent by ID.

        Args:
            patent_id: Patent number (e.g., "7861317")
            fields: Fields to return (None = defaults)

        Returns:
            Dict containing patent data
        """
        query = {"patent_id": patent_id}
        params = {"q": json.dumps(query)}

        if fields:
            params["f"] = json.dumps(fields)

        return await self.get(
            "patent/", params=params, resource_id=f"patent:{patent_id}"
        )

    async def search_inventors(
        self,
        query: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        size: int = 100,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search inventors.

        Args:
            query: Query object
            fields: Fields to return
            size: Number of results
            after: Pagination cursor

        Returns:
            Dict containing inventors
        """
        params = {}

        if query:
            params["q"] = json.dumps(query)

        if fields:
            params["f"] = json.dumps(fields)

        options = {"size": min(size, 1000)}
        if after:
            options["after"] = after
        params["o"] = json.dumps(options)

        return await self.get("inventor/", params=params, resource_id="inventor_search")

    async def get_inventor(
        self, inventor_id: str, fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get a single inventor by ID.

        Args:
            inventor_id: Inventor ID
            fields: Fields to return

        Returns:
            Dict containing inventor data
        """
        query = {"inventor_id": inventor_id}
        params = {"q": json.dumps(query)}

        if fields:
            params["f"] = json.dumps(fields)

        return await self.get(
            "inventor/", params=params, resource_id=f"inventor:{inventor_id}"
        )

    async def search_assignees(
        self,
        query: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        size: int = 100,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search assignees (companies/organizations).

        Args:
            query: Query object
            fields: Fields to return
            size: Number of results
            after: Pagination cursor

        Returns:
            Dict containing assignees
        """
        params = {}

        if query:
            params["q"] = json.dumps(query)

        if fields:
            params["f"] = json.dumps(fields)

        options = {"size": min(size, 1000)}
        if after:
            options["after"] = after
        params["o"] = json.dumps(options)

        return await self.get("assignee/", params=params, resource_id="assignee_search")

    async def get_assignee(
        self, assignee_id: str, fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get a single assignee by ID.

        Args:
            assignee_id: Assignee ID
            fields: Fields to return

        Returns:
            Dict containing assignee data
        """
        query = {"assignee_id": assignee_id}
        params = {"q": json.dumps(query)}

        if fields:
            params["f"] = json.dumps(fields)

        return await self.get(
            "assignee/", params=params, resource_id=f"assignee:{assignee_id}"
        )

    async def search_patents_by_assignee(
        self,
        assignee_name: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        size: int = 100,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search patents by assignee name.

        Args:
            assignee_name: Assignee name to search
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            size: Number of results
            after: Pagination cursor

        Returns:
            Dict containing patents
        """
        conditions = [{"assignees.assignee_organization": {"_text_any": assignee_name}}]

        if date_from:
            conditions.append({"patent_date": {"_gte": date_from}})
        if date_to:
            conditions.append({"patent_date": {"_lte": date_to}})

        query = {"_and": conditions} if len(conditions) > 1 else conditions[0]

        return await self.search_patents(query=query, size=size, after=after)

    async def search_patents_by_cpc(
        self,
        cpc_code: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        size: int = 100,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search patents by CPC classification code.

        Args:
            cpc_code: CPC code prefix (e.g., "G06N" for machine learning)
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            size: Number of results
            after: Pagination cursor

        Returns:
            Dict containing patents
        """
        conditions = [{"cpc_current.cpc_group_id": {"_begins": cpc_code}}]

        if date_from:
            conditions.append({"patent_date": {"_gte": date_from}})
        if date_to:
            conditions.append({"patent_date": {"_lte": date_to}})

        query = {"_and": conditions} if len(conditions) > 1 else conditions[0]

        return await self.search_patents(query=query, size=size, after=after)

    async def get_patent_citations(
        self, patent_id: str, size: int = 100
    ) -> Dict[str, Any]:
        """
        Get citations for a patent.

        Args:
            patent_id: Patent number
            size: Number of results

        Returns:
            Dict containing citation data
        """
        query = {"patent_id": patent_id}
        params = {"q": json.dumps(query), "o": json.dumps({"size": min(size, 1000)})}

        return await self.get(
            "patent/us_patent_citation/",
            params=params,
            resource_id=f"citations:{patent_id}",
        )


# Common CPC codes for major technology sectors
CPC_CODES = {
    "machine_learning": "G06N",
    "artificial_intelligence": "G06N3",
    "neural_networks": "G06N3/02",
    "natural_language_processing": "G06F40",
    "computer_vision": "G06V",
    "robotics": "B25J",
    "autonomous_vehicles": "B60W60",
    "blockchain": "G06Q20/38",
    "quantum_computing": "G06N10",
    "biotechnology": "C12N",
    "pharmaceuticals": "A61K",
    "medical_devices": "A61B",
    "semiconductors": "H01L",
    "telecommunications": "H04",
    "renewable_energy": "Y02E",
    "battery_technology": "H01M",
}

# Common assignees for testing
MAJOR_TECH_ASSIGNEES = [
    "Apple Inc.",
    "Microsoft Corporation",
    "Google LLC",
    "Amazon Technologies",
    "Meta Platforms",
    "Tesla Inc.",
    "NVIDIA Corporation",
    "IBM",
    "Intel Corporation",
    "Samsung Electronics",
]
