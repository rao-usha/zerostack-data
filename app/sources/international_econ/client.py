"""
International Economic Data API clients with rate limiting and retry logic.

Supports:
- World Bank API: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
- IMF SDMX API: https://data.imf.org/
- OECD SDMX API: https://data.oecd.org/api/
- BIS Statistics API: https://www.bis.org/statistics/api.htm

All APIs are free and do not require authentication.
"""

import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)


class WorldBankClient:
    """
    HTTP client for World Bank Open Data API.

    World Bank API provides access to 1,600+ indicators for 200+ countries.

    API Documentation:
    https://datahelpdesk.worldbank.org/knowledgebase/articles/889392

    No API key required. Rate limit: reasonable use (no hard limit).
    """

    BASE_URL = "https://api.worldbank.org/v2"

    # Conservative rate limiting
    DEFAULT_MAX_CONCURRENCY = 3
    DEFAULT_MAX_REQUESTS_PER_MINUTE = 60

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize World Bank API client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Semaphore for bounded concurrency - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized WorldBankClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_countries(self) -> List[Dict[str, Any]]:
        """
        Get list of all countries with metadata.

        Returns:
            List of country records with id, name, region, income level, etc.
        """
        url = f"{self.BASE_URL}/country"
        params = {"format": "json", "per_page": 500}

        response = await self._request_with_retry(url, params, "countries")

        # World Bank API returns [metadata, data]
        if isinstance(response, list) and len(response) > 1:
            return response[1] or []
        return []

    async def get_indicators(
        self, search: Optional[str] = None, page: int = 1, per_page: int = 500
    ) -> Dict[str, Any]:
        """
        Get list of available indicators.

        Args:
            search: Optional search term
            page: Page number
            per_page: Results per page (max 500)

        Returns:
            Dict with indicators and pagination info
        """
        url = f"{self.BASE_URL}/indicator"
        params = {"format": "json", "page": page, "per_page": min(per_page, 500)}

        if search:
            params["search"] = search

        response = await self._request_with_retry(url, params, "indicators")

        if isinstance(response, list) and len(response) > 1:
            metadata = response[0] if response[0] else {}
            data = response[1] or []
            return {
                "page": metadata.get("page", 1),
                "pages": metadata.get("pages", 1),
                "total": metadata.get("total", len(data)),
                "indicators": data,
            }
        return {"indicators": [], "page": 1, "pages": 1, "total": 0}

    async def get_indicator_data(
        self,
        indicator_id: str,
        country: str = "all",
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        per_page: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Get data for a specific indicator.

        Args:
            indicator_id: World Bank indicator code (e.g., "NY.GDP.MKTP.CD")
            country: Country code or "all" (e.g., "USA", "GBR", "all")
            start_year: Start year filter
            end_year: End year filter
            per_page: Results per page (max 5000 for data queries)

        Returns:
            List of data records with country, year, value
        """
        url = f"{self.BASE_URL}/country/{country}/indicator/{indicator_id}"

        params = {"format": "json", "per_page": min(per_page, 5000)}

        if start_year and end_year:
            params["date"] = f"{start_year}:{end_year}"
        elif start_year:
            params["date"] = f"{start_year}:{datetime.now().year}"
        elif end_year:
            params["date"] = f"1960:{end_year}"

        all_data = []
        page = 1

        while True:
            params["page"] = page
            response = await self._request_with_retry(
                url, params, f"indicator:{indicator_id}:page{page}"
            )

            if isinstance(response, list) and len(response) > 1:
                metadata = response[0] if response[0] else {}
                data = response[1] or []

                all_data.extend(data)

                total_pages = metadata.get("pages", 1)
                logger.info(
                    f"Fetched page {page}/{total_pages} for indicator {indicator_id}"
                )

                if page >= total_pages:
                    break
                page += 1
            else:
                break

        return all_data

    async def get_wdi_data(
        self,
        indicators: List[str],
        countries: List[str] = None,
        start_year: int = 2010,
        end_year: int = None,
    ) -> List[Dict[str, Any]]:
        """
        Get World Development Indicators data for multiple indicators.

        Args:
            indicators: List of WDI indicator codes
            countries: List of country codes (None for all)
            start_year: Start year
            end_year: End year (None for current year)

        Returns:
            List of data records
        """
        if not end_year:
            end_year = datetime.now().year

        country_str = ";".join(countries) if countries else "all"

        all_data = []
        for indicator in indicators:
            try:
                data = await self.get_indicator_data(
                    indicator_id=indicator,
                    country=country_str,
                    start_year=start_year,
                    end_year=end_year,
                )
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} records for indicator {indicator}")
            except Exception as e:
                logger.error(f"Failed to fetch indicator {indicator}: {e}")

        return all_data

    async def _request_with_retry(
        self, url: str, params: Dict[str, Any], data_id: str
    ) -> Any:
        """
        Make HTTP GET request with exponential backoff retry.

        Args:
            url: API endpoint URL
            params: Query parameters
            data_id: Data identifier (for logging)

        Returns:
            Parsed JSON response

        Raises:
            Exception: After all retries exhausted
        """
        async with self.semaphore:  # Bounded concurrency
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"Fetching World Bank data {data_id} "
                        f"(attempt {attempt+1}/{self.max_retries})"
                    )

                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    data = response.json()
                    logger.debug(f"Successfully fetched data {data_id}")
                    return data

                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching World Bank data (attempt {attempt+1}): {e}"
                    )

                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After", "60")
                        wait_time = int(retry_after)
                        logger.warning(f"Rate limited. Waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"World Bank API HTTP error: {e.response.status_code}"
                        )

                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error fetching World Bank data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"World Bank API request failed: {str(e)}")

                except Exception as e:
                    logger.error(
                        f"Unexpected error fetching World Bank data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception(
                f"Failed to fetch World Bank data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        base_delay = 1.0
        max_delay = 60.0

        delay = min(base_delay * (self.backoff_factor**attempt), max_delay)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)

        logger.debug(f"Backing off for {delay_with_jitter:.2f}s")
        await asyncio.sleep(delay_with_jitter)


class IMFClient:
    """
    HTTP client for IMF Data API (SDMX-based).

    IMF API provides access to:
    - World Economic Outlook (WEO)
    - International Financial Statistics (IFS)
    - Balance of Payments (BOP)
    - Financial Soundness Indicators

    API Documentation: https://data.imf.org/

    No API key required.
    """

    BASE_URL = "https://dataservices.imf.org/REST/SDMX_JSON.svc"

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized IMFClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=30.0))
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_dataflows(self) -> List[Dict[str, Any]]:
        """Get list of available data flows (datasets)."""
        url = f"{self.BASE_URL}/Dataflow"

        response = await self._request_with_retry(url, {}, "dataflows")

        try:
            structures = response.get("Structure", {})
            dataflows = structures.get("Dataflows", {}).get("Dataflow", [])
            return dataflows if isinstance(dataflows, list) else [dataflows]
        except Exception as e:
            logger.error(f"Failed to parse IMF dataflows: {e}")
            return []

    async def get_data(
        self,
        database_id: str,
        dimensions: Dict[str, str],
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get data from IMF database.

        Args:
            database_id: Database ID (e.g., "IFS" for International Financial Statistics)
            dimensions: Dict of dimension filters (varies by database)
            start_period: Start period (e.g., "2010")
            end_period: End period (e.g., "2024")

        Returns:
            List of data observations
        """
        # Build dimension key string
        dim_values = ".".join(dimensions.values()) if dimensions else ""

        url = f"{self.BASE_URL}/CompactData/{database_id}/{dim_values}"

        params = {}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        response = await self._request_with_retry(url, params, f"imf:{database_id}")

        return self._parse_compact_data(response)

    def _parse_compact_data(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse IMF compact data format."""
        records = []

        try:
            datasets = response.get("CompactData", {}).get("DataSet", {})
            series_list = datasets.get("Series", [])

            if not isinstance(series_list, list):
                series_list = [series_list] if series_list else []

            for series in series_list:
                # Extract series attributes
                series_attrs = {k: v for k, v in series.items() if k.startswith("@")}

                # Extract observations
                obs_list = series.get("Obs", [])
                if not isinstance(obs_list, list):
                    obs_list = [obs_list] if obs_list else []

                for obs in obs_list:
                    record = {
                        "period": obs.get("@TIME_PERIOD"),
                        "value": obs.get("@OBS_VALUE"),
                        "status": obs.get("@OBS_STATUS"),
                    }

                    # Add series attributes
                    for key, val in series_attrs.items():
                        clean_key = key.replace("@", "").lower()
                        record[clean_key] = val

                    records.append(record)

        except Exception as e:
            logger.error(f"Failed to parse IMF compact data: {e}")

        return records

    async def get_ifs_data(
        self,
        indicator: str = "NGDP_R_XDC",
        countries: List[str] = None,
        start_year: str = "2010",
        end_year: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get International Financial Statistics data.

        Args:
            indicator: IFS indicator code
            countries: List of country codes (None for all)
            start_year: Start year
            end_year: End year

        Returns:
            List of IFS data records
        """
        if not end_year:
            end_year = str(datetime.now().year)

        country_str = "+".join(countries) if countries else ""

        dimensions = {
            "freq": "A",  # Annual
            "ref_area": country_str,
            "indicator": indicator,
        }

        return await self.get_data(
            database_id="IFS",
            dimensions=dimensions,
            start_period=start_year,
            end_period=end_year,
        )

    async def _request_with_retry(
        self, url: str, params: Dict[str, Any], data_id: str
    ) -> Any:
        """Make HTTP GET request with retry logic."""
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(f"Fetching IMF data {data_id} (attempt {attempt+1})")

                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    return response.json()

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching IMF data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"IMF API HTTP error: {e.response.status_code}")

                except httpx.RequestError as e:
                    logger.warning(f"Request error fetching IMF data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"IMF API request failed: {str(e)}")

            raise Exception(
                f"Failed to fetch IMF data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (2**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


class OECDClient:
    """
    HTTP client for OECD Data API.

    OECD API provides access to:
    - Composite Leading Indicators (CLI)
    - Key Economic Indicators (KEI) - Main Economic Indicators
    - Annual Labour Force Statistics (ALFS) - Labor Market Data
    - Balanced Trade in Services (BATIS) - Trade Statistics
    - Revenue Statistics (Tax Data)

    Uses the new OECD SDMX REST API.

    No API key required.
    """

    # New OECD SDMX REST API
    BASE_URL = "https://sdmx.oecd.org/public/rest/data"

    # Dataflow IDs for new API
    # Format: AGENCY_ID,DATAFLOW_ID,VERSION
    DATAFLOWS = {
        # Short-term Economic Statistics
        "cli": "OECD.SDD.STES,DSD_STES@DF_CLI,4.1",  # Composite Leading Indicators
        "kei": "OECD.SDD.STES,DSD_KEI@DF_KEI,4.0",  # Key Economic Indicators (MEI)
        # Labor Market
        "alfs": "OECD.SDD.TPS,DSD_ALFS@DF_SUMTAB,",  # Annual Labour Force Statistics
        # Trade Statistics
        "batis": "OECD.SDD.TPS,DSD_BATIS@DF_BATIS,",  # Balanced Trade in Services
        # Tax/Revenue Statistics
        "tax_oecd": "OECD.CTP.TPS,DSD_REV_COMP_OECD@DF_RSOECD,2.0",  # OECD Tax Revenue Comparative
    }

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized OECDClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=30.0),
                follow_redirects=True,  # Follow 301/302 redirects (OECD migrating to new endpoint)
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_data(
        self,
        dataset: str,
        filter_expression: str = "all",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get data from OECD dataset using the new SDMX REST API.

        Args:
            dataset: Dataset key (e.g., "cli" for Composite Leading Indicators)
            filter_expression: SDMX filter expression
            start_period: Start period
            end_period: End period

        Returns:
            List of data observations
        """
        # Get dataflow ID
        dataflow = self.DATAFLOWS.get(dataset.lower())

        if not dataflow:
            raise ValueError(
                f"Unknown dataset: {dataset}. Available: {list(self.DATAFLOWS.keys())}"
            )

        # New API URL format: /data/{dataflow}/{filter}
        url = f"{self.BASE_URL}/{dataflow}/{filter_expression}"

        # Don't use dimensionAtObservation=AllDimensions as it changes the structure
        # Default structure has series -> observations which is what we parse
        params = {"format": "jsondata"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        response = await self._request_with_retry(url, params, f"oecd:{dataset}")

        return self._parse_sdmx_json_new(response, dataset)

    def _parse_sdmx_json(
        self, response: Dict[str, Any], dataset: str
    ) -> List[Dict[str, Any]]:
        """Parse old OECD SDMX-JSON format (deprecated)."""
        records = []

        try:
            dataSets = response.get("dataSets", [])
            if not dataSets:
                return []

            data = dataSets[0]
            observations = data.get("observations", {})

            # Get dimension metadata
            structure = response.get("structure", {})
            dimensions = structure.get("dimensions", {}).get("observation", [])

            # Build dimension lookup
            dim_lookup = {}
            for dim in dimensions:
                dim_id = dim.get("id")
                values = {
                    str(i): v.get("id") for i, v in enumerate(dim.get("values", []))
                }
                dim_lookup[dim_id] = values

            # Parse observations
            for key, value in observations.items():
                indices = key.split(":")

                record = {"dataset": dataset, "value": value[0] if value else None}

                # Map dimension indices to values
                for i, idx in enumerate(indices):
                    if i < len(dimensions):
                        dim_id = dimensions[i].get("id", f"dim_{i}")
                        dim_values = dim_lookup.get(dim_id, {})
                        record[dim_id.lower()] = dim_values.get(idx, idx)

                records.append(record)

        except Exception as e:
            logger.error(f"Failed to parse OECD SDMX-JSON: {e}")

        return records

    def _parse_sdmx_json_new(
        self, response: Dict[str, Any], dataset: str
    ) -> List[Dict[str, Any]]:
        """Parse new OECD SDMX REST API JSON format."""
        records = []

        try:
            # New format has 'data' -> 'dataSets' structure
            data_container = response.get("data", response)
            dataSets = data_container.get("dataSets", [])

            if not dataSets:
                logger.warning("No dataSets found in OECD response")
                return []

            # Get structure info
            structures = data_container.get("structures", [])
            if not structures:
                structures = response.get("structures", [])

            # Get dimensions from structure
            # Series dimensions define the series key structure
            # Observation dimensions define the observation key structure (usually time)
            series_dimensions = []
            obs_dimensions = []

            if structures:
                struct = structures[0]
                dims_container = struct.get("dimensions", {})
                series_dimensions = dims_container.get("series", [])
                obs_dimensions = dims_container.get("observation", [])

            # Build dimension lookup for series dimensions
            series_dim_lookup = {}
            for dim in series_dimensions:
                dim_id = dim.get("id", "")
                values_list = dim.get("values", [])
                series_dim_lookup[dim_id] = {
                    str(i): v.get("id", v.get("name", str(i)))
                    for i, v in enumerate(values_list)
                }

            # Build lookup for observation dimension (usually TIME_PERIOD)
            obs_dim_lookup = {}
            for dim in obs_dimensions:
                dim_id = dim.get("id", "")
                values_list = dim.get("values", [])
                obs_dim_lookup[dim_id] = {
                    str(i): v.get("id", v.get("name", str(i)))
                    for i, v in enumerate(values_list)
                }

            # Parse each dataset
            for ds_idx, ds in enumerate(dataSets):
                series = ds.get("series", {})

                if not series:
                    continue

                # Handle both dict and object-like structures
                if isinstance(series, dict):
                    series_items = list(series.items())
                elif hasattr(series, "items"):
                    series_items = list(series.items())
                else:
                    logger.warning(f"Cannot iterate series of type: {type(series)}")
                    continue

                for series_key, series_data in series_items:
                    # Parse series key (colon-separated indices)
                    series_indices = series_key.split(":")

                    # Build base record from series dimensions
                    base_record = {"dataset": dataset}
                    for i, idx in enumerate(series_indices):
                        if i < len(series_dimensions):
                            dim_id = series_dimensions[i].get("id", f"dim_{i}")
                            dim_values = series_dim_lookup.get(dim_id, {})
                            base_record[dim_id.lower()] = dim_values.get(idx, idx)

                    # Get observations for this series
                    observations = series_data.get("observations", {})

                    for obs_key, obs_values in observations.items():
                        record = base_record.copy()
                        record["value"] = obs_values[0] if obs_values else None

                        # Map observation dimension (usually time)
                        if obs_dimensions:
                            obs_dim = obs_dimensions[0]
                            dim_id = obs_dim.get("id", "TIME_PERIOD")
                            dim_values = obs_dim_lookup.get(dim_id, {})
                            record["period"] = dim_values.get(obs_key, obs_key)
                        else:
                            record["period"] = obs_key

                        records.append(record)

            logger.info(f"Parsed {len(records)} records from OECD response")

        except Exception as e:
            logger.error(f"Failed to parse new OECD SDMX-JSON: {e}", exc_info=True)

        return records

    async def get_main_economic_indicators(
        self,
        countries: List[str] = None,
        subjects: List[str] = None,
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Composite Leading Indicators (CLI) data from OECD.

        CLI provides leading indicators for turning points in business cycles.

        Args:
            countries: List of country codes (e.g., ["USA", "GBR", "DEU"])
            subjects: List of subject codes (defaults to amplitude adjusted CLI)
            start_period: Start period (year)
            end_period: End period (year)

        Returns:
            List of CLI data records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        if not countries:
            countries = ["USA", "GBR", "DEU", "FRA", "JPN"]

        # Convert country codes to uppercase
        countries = [c.upper() for c in countries]

        # Fetch all CLI data and filter on client side
        # This is more reliable than complex server-side filters
        try:
            data = await self.get_data(
                dataset="cli",
                filter_expression="all",  # Get all data, filter locally
                start_period=start_period,
                end_period=end_period,
            )

            logger.info(f"Fetched {len(data)} total CLI records from OECD")

            # Filter to requested countries and amplitude adjusted (AA) data
            filtered_data = []
            for record in data:
                ref_area = record.get("ref_area", "")
                adjustment = record.get("adjustment", "")

                # Filter by country and adjustment type (AA = amplitude adjusted)
                if ref_area in countries and adjustment == "AA":
                    filtered_data.append(record)

            logger.info(f"Filtered to {len(filtered_data)} records for {countries}")
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to fetch OECD CLI data: {e}", exc_info=True)
            return []

    async def get_key_economic_indicators(
        self,
        countries: List[str] = None,
        subjects: List[str] = None,
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Key Economic Indicators (KEI) data from OECD - Main Economic Indicators.

        KEI includes: Industrial Production, Consumer Prices, Unemployment, etc.

        Args:
            countries: List of country codes (e.g., ["USA", "GBR", "DEU"])
            subjects: List of subject codes
            start_period: Start period (year)
            end_period: End period (year)

        Returns:
            List of KEI data records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        if not countries:
            countries = ["USA", "GBR", "DEU", "FRA", "JPN"]

        countries = [c.upper() for c in countries]

        try:
            data = await self.get_data(
                dataset="kei",
                filter_expression="all",
                start_period=start_period,
                end_period=end_period,
            )

            logger.info(f"Fetched {len(data)} total KEI records from OECD")

            # Filter to requested countries
            filtered_data = [
                record for record in data if record.get("ref_area", "") in countries
            ]

            logger.info(f"Filtered to {len(filtered_data)} records for {countries}")
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to fetch OECD KEI data: {e}", exc_info=True)
            return []

    async def get_labour_force_statistics(
        self,
        countries: List[str] = None,
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Annual Labour Force Statistics (ALFS) from OECD.

        ALFS includes: Employment, Unemployment rates, Labor force participation, etc.

        Args:
            countries: List of country codes
            start_period: Start period (year)
            end_period: End period (year)

        Returns:
            List of labor statistics records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        if not countries:
            countries = ["USA", "GBR", "DEU", "FRA", "JPN"]

        countries = [c.upper() for c in countries]

        try:
            data = await self.get_data(
                dataset="alfs",
                filter_expression="all",
                start_period=start_period,
                end_period=end_period,
            )

            logger.info(f"Fetched {len(data)} total ALFS records from OECD")

            # Filter to requested countries
            filtered_data = [
                record for record in data if record.get("ref_area", "") in countries
            ]

            logger.info(
                f"Filtered to {len(filtered_data)} labor records for {countries}"
            )
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to fetch OECD labor data: {e}", exc_info=True)
            return []

    async def get_trade_in_services(
        self,
        countries: List[str] = None,
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Balanced Trade in Services (BATIS) from OECD.

        BATIS includes bilateral trade in services data.

        Args:
            countries: List of country codes (reporting countries)
            start_period: Start period (year)
            end_period: End period (year)

        Returns:
            List of trade in services records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        if not countries:
            countries = ["USA", "GBR", "DEU", "FRA", "JPN"]

        countries = [c.upper() for c in countries]

        try:
            data = await self.get_data(
                dataset="batis",
                filter_expression="all",
                start_period=start_period,
                end_period=end_period,
            )

            logger.info(f"Fetched {len(data)} total trade records from OECD")

            # Filter to requested countries
            filtered_data = [
                record for record in data if record.get("ref_area", "") in countries
            ]

            logger.info(
                f"Filtered to {len(filtered_data)} trade records for {countries}"
            )
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to fetch OECD trade data: {e}", exc_info=True)
            return []

    async def get_tax_revenue_statistics(
        self,
        countries: List[str] = None,
        start_period: str = "2000",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Tax Revenue Statistics from OECD.

        Includes: Total tax revenue, by type (income, VAT, etc.), as % of GDP.

        Args:
            countries: List of country codes
            start_period: Start period (year)
            end_period: End period (year)

        Returns:
            List of tax revenue records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        if not countries:
            countries = ["USA", "GBR", "DEU", "FRA", "JPN"]

        countries = [c.upper() for c in countries]

        try:
            data = await self.get_data(
                dataset="tax_oecd",
                filter_expression="all",
                start_period=start_period,
                end_period=end_period,
            )

            logger.info(f"Fetched {len(data)} total tax records from OECD")

            # Filter to requested countries
            filtered_data = [
                record
                for record in data
                if record.get("cou", "") in countries
                or record.get("ref_area", "") in countries
            ]

            logger.info(f"Filtered to {len(filtered_data)} tax records for {countries}")
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to fetch OECD tax data: {e}", exc_info=True)
            return []

    async def _request_with_retry(
        self, url: str, params: Dict[str, Any], data_id: str
    ) -> Any:
        """Make HTTP GET request with retry logic."""
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(f"Fetching OECD data {data_id} (attempt {attempt+1})")

                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    return response.json()

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching OECD data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"OECD API HTTP error: {e.response.status_code}"
                        )

                except httpx.RequestError as e:
                    logger.warning(f"Request error fetching OECD data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"OECD API request failed: {str(e)}")

            raise Exception(
                f"Failed to fetch OECD data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (2**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


class BISClient:
    """
    HTTP client for Bank for International Settlements (BIS) Statistics API.

    BIS API provides access to:
    - International Banking Statistics
    - Credit Gap Indicators
    - Effective Exchange Rates
    - Property Prices
    - Debt Securities Statistics

    API Documentation: https://www.bis.org/statistics/api.htm

    No API key required.
    """

    BASE_URL = "https://stats.bis.org/api/v1"

    DEFAULT_MAX_CONCURRENCY = 2

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(f"Initialized BISClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=30.0))
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_data(
        self,
        dataset: str,
        key: str = "all",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get data from BIS dataset.

        Args:
            dataset: Dataset ID (e.g., "WS_EER" for Effective Exchange Rates)
            key: Data key filter
            start_period: Start period
            end_period: End period

        Returns:
            List of data observations
        """
        url = f"{self.BASE_URL}/data/{dataset}/{key}"

        params = {"format": "jsondata"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        response = await self._request_with_retry(url, params, f"bis:{dataset}")

        return self._parse_bis_json(response, dataset)

    def _parse_bis_json(
        self, response: Dict[str, Any], dataset: str
    ) -> List[Dict[str, Any]]:
        """Parse BIS JSON data format."""
        records = []

        try:
            dataSets = response.get("dataSets", [])
            if not dataSets:
                return []

            data = dataSets[0]
            series = data.get("series", {})

            # Get dimension metadata
            structure = response.get("structure", {})
            dimensions = structure.get("dimensions", {}).get("series", [])
            time_dimensions = structure.get("dimensions", {}).get("observation", [])

            for series_key, series_data in series.items():
                # Parse series dimensions
                series_indices = series_key.split(":")
                series_attrs = {}

                for i, idx in enumerate(series_indices):
                    if i < len(dimensions):
                        dim = dimensions[i]
                        values = dim.get("values", [])
                        if int(idx) < len(values):
                            series_attrs[dim.get("id", f"dim_{i}").lower()] = values[
                                int(idx)
                            ].get("id")

                # Parse observations
                observations = series_data.get("observations", {})
                for obs_key, obs_value in observations.items():
                    # Get time period
                    time_idx = int(obs_key)
                    time_dim = time_dimensions[0] if time_dimensions else {}
                    time_values = time_dim.get("values", [])
                    time_period = (
                        time_values[time_idx].get("id")
                        if time_idx < len(time_values)
                        else obs_key
                    )

                    record = {
                        "dataset": dataset,
                        "period": time_period,
                        "value": obs_value[0] if obs_value else None,
                        **series_attrs,
                    }
                    records.append(record)

        except Exception as e:
            logger.error(f"Failed to parse BIS JSON: {e}")

        return records

    async def get_effective_exchange_rates(
        self,
        countries: List[str] = None,
        eer_type: str = "R",  # R = Real, N = Nominal
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get Effective Exchange Rate data.

        Args:
            countries: List of country codes
            eer_type: Exchange rate type (R = Real, N = Nominal)
            start_period: Start period
            end_period: End period

        Returns:
            List of EER data records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        # Build key
        country_part = "+".join(countries) if countries else ""
        key = f"M.{eer_type}.{country_part}." if country_part else "all"

        return await self.get_data(
            dataset="WS_EER",
            key=key.rstrip(".") or "all",
            start_period=start_period,
            end_period=end_period,
        )

    async def get_property_prices(
        self,
        countries: List[str] = None,
        start_period: str = "2010",
        end_period: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get residential property price data.

        Args:
            countries: List of country codes
            start_period: Start period
            end_period: End period

        Returns:
            List of property price records
        """
        if not end_period:
            end_period = str(datetime.now().year)

        country_part = "+".join(countries) if countries else ""
        key = f"Q.{country_part}." if country_part else "all"

        return await self.get_data(
            dataset="WS_SPP",
            key=key.rstrip(".") or "all",
            start_period=start_period,
            end_period=end_period,
        )

    async def _request_with_retry(
        self, url: str, params: Dict[str, Any], data_id: str
    ) -> Any:
        """Make HTTP GET request with retry logic."""
        async with self.semaphore:
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(f"Fetching BIS data {data_id} (attempt {attempt+1})")

                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    return response.json()

                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error fetching BIS data: {e}")
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"BIS API HTTP error: {e.response.status_code}")

                except httpx.RequestError as e:
                    logger.warning(f"Request error fetching BIS data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"BIS API request failed: {str(e)}")

            raise Exception(
                f"Failed to fetch BIS data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        delay = min(1.0 * (2**attempt), 60.0)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


# Common World Bank indicators
COMMON_WDI_INDICATORS = {
    "gdp": {
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    },
    "population": {
        "SP.POP.TOTL": "Population, total",
        "SP.POP.GROW": "Population growth (annual %)",
        "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    },
    "trade": {
        "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
        "NE.EXP.GNFS.CD": "Exports of goods and services (current US$)",
        "NE.IMP.GNFS.CD": "Imports of goods and services (current US$)",
    },
    "inflation": {
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "NY.GDP.DEFL.KD.ZG": "Inflation, GDP deflator (annual %)",
    },
    "unemployment": {
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
        "SL.UEM.TOTL.NE.ZS": "Unemployment, total (national estimate)",
    },
    "poverty": {
        "SI.POV.DDAY": "Poverty headcount ratio at $2.15 a day (2017 PPP)",
        "SI.POV.GINI": "Gini index",
    },
    "health": {
        "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
        "SH.XPD.CHEX.GD.ZS": "Current health expenditure (% of GDP)",
    },
    "education": {
        "SE.XPD.TOTL.GD.ZS": "Government expenditure on education (% of GDP)",
        "SE.ADT.LITR.ZS": "Literacy rate, adult total (% of people ages 15+)",
    },
}

# Common country codes
MAJOR_ECONOMIES = ["USA", "CHN", "JPN", "DEU", "GBR", "FRA", "IND", "BRA", "ITA", "CAN"]
G7_COUNTRIES = ["USA", "GBR", "FRA", "DEU", "ITA", "CAN", "JPN"]
G20_COUNTRIES = [
    "ARG",
    "AUS",
    "BRA",
    "CAN",
    "CHN",
    "FRA",
    "DEU",
    "IND",
    "IDN",
    "ITA",
    "JPN",
    "MEX",
    "RUS",
    "SAU",
    "ZAF",
    "KOR",
    "TUR",
    "GBR",
    "USA",
]
