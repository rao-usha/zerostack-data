"""
Real Estate / Housing API clients with rate limiting and retry logic.

Provides clients for:
- FHFA House Price Index API
- HUD Building Permits API
- Redfin Data Downloads
- OpenStreetMap Overpass API

All clients implement:
- Bounded concurrency via semaphores
- Exponential backoff with jitter
- Retry logic for transient failures
- Respect for rate limits
"""

import asyncio
import logging
import random
import csv
import io
from typing import Dict, List, Optional, Any
import httpx

logger = logging.getLogger(__name__)


class FHFAClient:
    """
    HTTP client for FHFA House Price Index API.

    FHFA provides quarterly house price indices for:
    - National level
    - State level
    - Metropolitan Statistical Area (MSA) level
    - ZIP code level (ZIP3 aggregation)

    API Documentation:
    https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx

    Rate Limits:
    - No documented rate limits, but we use conservative defaults
    - Default: 2 concurrent requests
    """

    # FHFA data download URLs
    BASE_URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """Initialize FHFA client."""
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized FHFAClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=10.0),  # Large file downloads
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_house_price_index(
        self,
        geography_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch FHFA House Price Index data.

        Args:
            geography_type: Filter by geography type (National, State, MSA, ZIP3)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of house price index records
        """
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"Fetching FHFA HPI data (attempt {attempt+1}/{self.max_retries})"
                    )

                    # Download CSV file
                    response = await client.get(self.BASE_URL)
                    response.raise_for_status()

                    # Parse CSV
                    csv_content = response.text
                    reader = csv.DictReader(io.StringIO(csv_content))

                    records = []
                    for row in reader:
                        # Apply filters
                        if geography_type and row.get("hpi_type") != geography_type:
                            continue

                        # Parse date and apply date filters
                        record_date = row.get("date")
                        if start_date and record_date < start_date:
                            continue
                        if end_date and record_date > end_date:
                            continue

                        records.append(
                            {
                                "date": record_date,
                                "geography_type": row.get("hpi_type"),
                                "geography_id": row.get("place_id"),
                                "geography_name": row.get("place_name"),
                                "index_nsa": row.get("index_nsa"),
                                "index_sa": row.get("index_sa"),
                                "yoy_pct_change": row.get("yoy_pct_change"),
                                "qoq_pct_change": row.get("qoq_pct_change"),
                            }
                        )

                    logger.info(f"Successfully fetched {len(records)} FHFA records")
                    return records

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching FHFA data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"FHFA API error: {e}")

                except Exception as e:
                    logger.error(f"Error fetching FHFA data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception("Failed to fetch FHFA data after retries")

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (self.backoff_factor**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


class HUDClient:
    """
    HTTP client for HUD Building Permits and Housing Starts data.

    HUD provides data on:
    - Building permits issued
    - Housing starts
    - Housing completions

    Data is broken down by:
    - Single-family units
    - 2-4 unit buildings
    - 5+ unit buildings

    API Documentation:
    https://www.huduser.gov/portal/datasets/socds.html

    Rate Limits:
    - No documented rate limits
    - Default: 2 concurrent requests
    """

    # HUD State of the Cities Data Systems (SOCDS) API
    BASE_URL = "https://www.huduser.gov/hudapi/public/socds"

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """Initialize HUD client."""
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized HUDClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=True
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_permits_and_starts(
        self,
        geography_type: str = "National",
        geography_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch HUD building permits and housing starts data.

        Args:
            geography_type: Geography type (National, State, MSA, County)
            geography_id: Geography identifier (state FIPS, MSA code, etc.)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of permits/starts records
        """
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"Fetching HUD permits data (attempt {attempt+1}/{self.max_retries})"
                    )

                    # Build API URL based on geography type
                    url = f"{self.BASE_URL}/buildingpermits"

                    params = {"type": geography_type.lower()}

                    if geography_id:
                        params["geoid"] = geography_id
                    if start_date:
                        params["startdate"] = start_date
                    if end_date:
                        params["enddate"] = end_date

                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    data = response.json()

                    # Parse response into standardized format
                    records = []
                    for item in data.get("data", []):
                        records.append(
                            {
                                "date": item.get("period_end"),
                                "geography_type": geography_type,
                                "geography_id": item.get("geoid"),
                                "geography_name": item.get("name"),
                                "permits_total": item.get("permits_total"),
                                "permits_1unit": item.get("permits_1unit"),
                                "permits_2to4units": item.get("permits_2to4"),
                                "permits_5plus": item.get("permits_5plus"),
                                "starts_total": item.get("starts_total"),
                                "starts_1unit": item.get("starts_1unit"),
                                "starts_2to4units": item.get("starts_2to4"),
                                "starts_5plus": item.get("starts_5plus"),
                                "completions_total": item.get("completions_total"),
                                "completions_1unit": item.get("completions_1unit"),
                                "completions_2to4units": item.get("completions_2to4"),
                                "completions_5plus": item.get("completions_5plus"),
                            }
                        )

                    logger.info(f"Successfully fetched {len(records)} HUD records")
                    return records

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching HUD data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"HUD API error: {e}")

                except Exception as e:
                    logger.error(f"Error fetching HUD data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception("Failed to fetch HUD data after retries")

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (self.backoff_factor**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


class RedfinClient:
    """
    HTTP client for Redfin public housing data downloads.

    Redfin provides publicly available housing market data including:
    - Median sale prices
    - Inventory levels
    - Days on market
    - Sale-to-list ratios

    Data is available at multiple geographic levels:
    - ZIP codes
    - Cities
    - Neighborhoods
    - Metro areas

    Data Downloads:
    https://www.redfin.com/news/data-center/

    Rate Limits:
    - No documented API rate limits (CSV downloads)
    - Default: 2 concurrent requests
    """

    # Redfin data center download URLs
    BASE_URL = "https://redfin-public-data.s3.us-west-2.amazonaws.com"

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """Initialize Redfin client."""
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized RedfinClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=10.0),  # Large file downloads
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_redfin_data(
        self, region_type: str = "zip", property_type: str = "All Residential"
    ) -> List[Dict[str, Any]]:
        """
        Fetch Redfin housing market data.

        Args:
            region_type: Region type (zip, city, neighborhood, metro)
            property_type: Property type filter

        Returns:
            List of Redfin housing market records
        """
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"Fetching Redfin data for {region_type} (attempt {attempt+1}/{self.max_retries})"
                    )

                    # Build download URL
                    # Format: {region_type}_market_tracker.tsv000.gz
                    filename = f"{region_type}_market_tracker.tsv000.gz"
                    url = f"{self.BASE_URL}/redfin_market_tracker/{filename}"

                    response = await client.get(url)
                    response.raise_for_status()

                    # Parse TSV (tab-separated values)
                    # Note: Redfin uses gzipped TSV files
                    import gzip

                    decompressed = gzip.decompress(response.content)
                    tsv_content = decompressed.decode("utf-8")

                    reader = csv.DictReader(io.StringIO(tsv_content), delimiter="\t")

                    records = []
                    for row in reader:
                        # Apply property type filter
                        if row.get("property_type") != property_type:
                            continue

                        records.append(
                            {
                                "period_end": row.get("period_end"),
                                "region_type": row.get("region_type"),
                                "region_type_id": row.get("region_type_id"),
                                "region": row.get("region"),
                                "state_code": row.get("state_code"),
                                "property_type": row.get("property_type"),
                                "median_sale_price": row.get("median_sale_price"),
                                "median_list_price": row.get("median_list_price"),
                                "median_ppsf": row.get("median_ppsf"),
                                "homes_sold": row.get("homes_sold"),
                                "pending_sales": row.get("pending_sales"),
                                "new_listings": row.get("new_listings"),
                                "inventory": row.get("inventory"),
                                "months_of_supply": row.get("months_of_supply"),
                                "median_dom": row.get("median_dom"),
                                "avg_sale_to_list": row.get("avg_sale_to_list"),
                                "sold_above_list": row.get("sold_above_list"),
                                "price_drops": row.get("price_drops"),
                                "off_market_in_two_weeks": row.get(
                                    "off_market_in_two_weeks"
                                ),
                            }
                        )

                    logger.info(f"Successfully fetched {len(records)} Redfin records")
                    return records

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching Redfin data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"Redfin download error: {e}")

                except Exception as e:
                    logger.error(f"Error fetching Redfin data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception("Failed to fetch Redfin data after retries")

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (self.backoff_factor**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


class OSMClient:
    """
    HTTP client for OpenStreetMap Overpass API.

    OpenStreetMap provides building footprint data including:
    - Building locations (lat/lon)
    - Building types (residential, commercial, etc.)
    - Building heights and number of floors
    - Address information

    Overpass API Documentation:
    https://wiki.openstreetmap.org/wiki/Overpass_API

    Rate Limits:
    - Public Overpass API has rate limits
    - Recommended: max 2 concurrent requests
    - Large queries may timeout (3-minute timeout)
    """

    # Overpass API endpoint
    BASE_URL = "https://overpass-api.de/api/interpreter"

    DEFAULT_MAX_CONCURRENCY = 1  # Very conservative for OSM

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """Initialize OSM client."""
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized OSMClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    180.0, connect=10.0
                ),  # Overpass queries can be slow
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_buildings(
        self,
        bounding_box: tuple[float, float, float, float],
        building_type: Optional[str] = None,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch building footprints from OpenStreetMap.

        Args:
            bounding_box: (south, west, north, east) coordinates
            building_type: Filter by building type (residential, commercial, etc.)
            limit: Maximum number of results

        Returns:
            List of building records
        """
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"Fetching OSM buildings (attempt {attempt+1}/{self.max_retries})"
                    )

                    # Build Overpass QL query
                    south, west, north, east = bounding_box
                    bbox_str = f"{south},{west},{north},{east}"

                    if building_type:
                        building_filter = f'["building"="{building_type}"]'
                    else:
                        building_filter = '["building"]'

                    query = f"""
                    [out:json][timeout:180];
                    (
                      way{building_filter}({bbox_str});
                      relation{building_filter}({bbox_str});
                    );
                    out center {limit};
                    """

                    response = await client.post(self.BASE_URL, data={"data": query})
                    response.raise_for_status()

                    data = response.json()
                    elements = data.get("elements", [])

                    logger.info(f"Successfully fetched {len(elements)} OSM buildings")
                    return elements

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching OSM data: {e}")

                    # Check for rate limiting (429)
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After", 60)
                        logger.warning(f"Rate limited. Waiting {retry_after}s")
                        await asyncio.sleep(int(retry_after))
                        continue

                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"OSM API error: {e}")

                except Exception as e:
                    logger.error(f"Error fetching OSM data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception("Failed to fetch OSM data after retries")

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(2.0 * (self.backoff_factor**attempt), 120.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))
