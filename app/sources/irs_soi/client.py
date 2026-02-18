"""
IRS Statistics of Income (SOI) HTTP client for bulk data downloads.

Data source: https://www.irs.gov/statistics/soi-tax-stats

This client handles:
- Downloading Excel/CSV files from IRS SOI
- Parsing various file formats (xlsx, xls, csv)
- Caching downloaded files to avoid re-downloads
- Bounded concurrency for multiple downloads

No API key required - all data is public domain.
"""

import asyncio
import hashlib
import io
import logging
import os
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


class IRSSOIClient:
    """
    HTTP client for IRS Statistics of Income data with bounded concurrency.

    Downloads and parses bulk Excel/CSV files from IRS SOI website.

    Datasets supported:
    - Individual Income by ZIP Code
    - Individual Income by County
    - Migration Data (county-to-county flows)
    - Business Income by ZIP Code
    """

    # Base URL for IRS SOI data
    BASE_URL = "https://www.irs.gov"

    # ZIP Code Income Data URLs by year
    # Source: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi
    ZIP_INCOME_URLS = {
        2021: "https://www.irs.gov/pub/irs-soi/21zpallagi.csv",
        2020: "https://www.irs.gov/pub/irs-soi/20zpallagi.csv",
        2019: "https://www.irs.gov/pub/irs-soi/19zpallagi.csv",
        2018: "https://www.irs.gov/pub/irs-soi/18zpallagi.csv",
        2017: "https://www.irs.gov/pub/irs-soi/17zpallagi.csv",
    }

    # County Income Data URLs by year
    # Source: https://www.irs.gov/statistics/soi-tax-stats-county-data
    COUNTY_INCOME_URLS = {
        2021: "https://www.irs.gov/pub/irs-soi/21incyallagi.csv",
        2020: "https://www.irs.gov/pub/irs-soi/20incyallagi.csv",
        2019: "https://www.irs.gov/pub/irs-soi/19incyallagi.csv",
        2018: "https://www.irs.gov/pub/irs-soi/18incyallagi.csv",
        2017: "https://www.irs.gov/pub/irs-soi/17incyallagi.csv",
    }

    # Migration Data URLs by year
    # Source: https://www.irs.gov/statistics/soi-tax-stats-migration-data
    MIGRATION_INFLOW_URLS = {
        2021: "https://www.irs.gov/pub/irs-soi/countyinflow2021.csv",
        2020: "https://www.irs.gov/pub/irs-soi/countyinflow2020.csv",
        2019: "https://www.irs.gov/pub/irs-soi/countyinflow1920.csv",
        2018: "https://www.irs.gov/pub/irs-soi/countyinflow1819.csv",
        2017: "https://www.irs.gov/pub/irs-soi/countyinflow1718.csv",
    }

    MIGRATION_OUTFLOW_URLS = {
        2021: "https://www.irs.gov/pub/irs-soi/countyoutflow2021.csv",
        2020: "https://www.irs.gov/pub/irs-soi/countyoutflow2020.csv",
        2019: "https://www.irs.gov/pub/irs-soi/countyoutflow1920.csv",
        2018: "https://www.irs.gov/pub/irs-soi/countyoutflow1819.csv",
        2017: "https://www.irs.gov/pub/irs-soi/countyoutflow1718.csv",
    }

    # Business Income by ZIP URLs
    # Source: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi
    BUSINESS_INCOME_URLS = {
        2021: "https://www.irs.gov/pub/irs-soi/21zpallnoagi.csv",
        2020: "https://www.irs.gov/pub/irs-soi/20zpallnoagi.csv",
        2019: "https://www.irs.gov/pub/irs-soi/19zpallnoagi.csv",
        2018: "https://www.irs.gov/pub/irs-soi/18zpallnoagi.csv",
        2017: "https://www.irs.gov/pub/irs-soi/17zpallnoagi.csv",
    }

    # Rate limit defaults (conservative for government site)
    DEFAULT_MAX_CONCURRENCY = 2
    DEFAULT_REQUESTS_PER_SECOND = 0.5  # 1 request every 2 seconds

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        cache_dir: Optional[str] = None,
    ):
        """
        Initialize IRS SOI client.

        Args:
            max_concurrency: Maximum concurrent downloads (bounded per RULES)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
            cache_dir: Directory to cache downloaded files (optional)
        """
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Bounded concurrency semaphore - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        # Rate limiting
        self._last_request_time: float = 0
        self._min_request_interval = 1.0 / self.DEFAULT_REQUESTS_PER_SECOND

        # Cache directory for downloaded files
        if cache_dir:
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_dir = None

        logger.info(
            f"Initialized IRSSOIClient: max_concurrency={max_concurrency}, "
            f"cache_dir={cache_dir}"
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    300.0, connect=30.0
                ),  # Long timeout for large files
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; NexdataIngestion/1.0)"
                },
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    def _get_cache_path(self, url: str) -> Optional[Path]:
        """Get cache file path for a URL."""
        if not self.cache_dir:
            return None

        # Create hash of URL for cache filename
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        filename = url.split("/")[-1]
        return self.cache_dir / f"{url_hash}_{filename}"

    def _read_from_cache(self, url: str) -> Optional[bytes]:
        """Read file from cache if exists."""
        cache_path = self._get_cache_path(url)
        if cache_path and cache_path.exists():
            logger.debug(f"Reading from cache: {cache_path}")
            return cache_path.read_bytes()
        return None

    def _write_to_cache(self, url: str, content: bytes):
        """Write file to cache."""
        cache_path = self._get_cache_path(url)
        if cache_path:
            logger.debug(f"Writing to cache: {cache_path}")
            cache_path.write_bytes(content)

    async def download_file(self, url: str, use_cache: bool = True) -> bytes:
        """
        Download a file from IRS SOI with retry logic.

        Args:
            url: URL to download
            use_cache: Whether to use cached files

        Returns:
            File content as bytes
        """
        # Check cache first
        if use_cache:
            cached = self._read_from_cache(url)
            if cached:
                logger.info(f"Using cached file for {url}")
                return cached

        async with self.semaphore:  # Bounded concurrency
            await self._rate_limit()
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"Downloading {url} (attempt {attempt + 1}/{self.max_retries})"
                    )

                    response = await client.get(url)

                    # Check for rate limiting
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", "60"))
                        logger.warning(f"Rate limited. Waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()

                    content = response.content
                    logger.info(f"Downloaded {len(content)} bytes from {url}")

                    # Cache the file
                    if use_cache:
                        self._write_to_cache(url, content)

                    return content

                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error downloading {url} (attempt {attempt + 1}): {e}"
                    )
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"IRS SOI download failed: {e.response.status_code} - "
                            f"{e.response.text[:500]}"
                        )

                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error downloading {url} (attempt {attempt + 1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"IRS SOI download failed: {str(e)}")

                except Exception as e:
                    logger.error(f"Unexpected error downloading {url}: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception(
                f"Failed to download {url} after {self.max_retries} attempts"
            )

    async def download_and_parse_csv(
        self, url: str, use_cache: bool = True, encoding: str = "latin-1"
    ) -> pd.DataFrame:
        """
        Download and parse a CSV file.

        Args:
            url: URL to download
            use_cache: Whether to use cached files
            encoding: File encoding

        Returns:
            Parsed DataFrame
        """
        content = await self.download_file(url, use_cache)

        try:
            df = pd.read_csv(io.BytesIO(content), encoding=encoding, low_memory=False)
            logger.info(f"Parsed CSV with {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error parsing CSV from {url}: {e}")
            raise

    async def download_and_parse_excel(
        self, url: str, use_cache: bool = True, sheet_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Download and parse an Excel file.

        Args:
            url: URL to download
            use_cache: Whether to use cached files
            sheet_name: Sheet to read (default: first sheet)

        Returns:
            Parsed DataFrame
        """
        content = await self.download_file(url, use_cache)

        try:
            # Determine engine based on file extension
            if url.endswith(".xlsx"):
                engine = "openpyxl"
            else:
                engine = "xlrd"

            df = pd.read_excel(
                io.BytesIO(content), sheet_name=sheet_name or 0, engine=engine
            )
            logger.info(f"Parsed Excel with {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error parsing Excel from {url}: {e}")
            raise

    # ========== ZIP Income Data ==========

    async def get_zip_income_data(
        self, year: int, use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Download and parse ZIP code income data.

        Args:
            year: Tax year
            use_cache: Whether to use cached files

        Returns:
            DataFrame with ZIP code income data
        """
        if year not in self.ZIP_INCOME_URLS:
            raise ValueError(
                f"ZIP income data not available for year {year}. "
                f"Available years: {sorted(self.ZIP_INCOME_URLS.keys())}"
            )

        url = self.ZIP_INCOME_URLS[year]
        df = await self.download_and_parse_csv(url, use_cache)

        # Add tax year column
        df["tax_year"] = year

        return df

    # ========== County Income Data ==========

    async def get_county_income_data(
        self, year: int, use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Download and parse county income data.

        Args:
            year: Tax year
            use_cache: Whether to use cached files

        Returns:
            DataFrame with county income data
        """
        if year not in self.COUNTY_INCOME_URLS:
            raise ValueError(
                f"County income data not available for year {year}. "
                f"Available years: {sorted(self.COUNTY_INCOME_URLS.keys())}"
            )

        url = self.COUNTY_INCOME_URLS[year]
        df = await self.download_and_parse_csv(url, use_cache)

        # Add tax year column
        df["tax_year"] = year

        return df

    # ========== Migration Data ==========

    async def get_migration_data(
        self, year: int, flow_type: str = "inflow", use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Download and parse county-to-county migration data.

        Args:
            year: Tax year
            flow_type: "inflow" or "outflow"
            use_cache: Whether to use cached files

        Returns:
            DataFrame with migration flow data
        """
        if flow_type == "inflow":
            urls = self.MIGRATION_INFLOW_URLS
        elif flow_type == "outflow":
            urls = self.MIGRATION_OUTFLOW_URLS
        else:
            raise ValueError(
                f"Invalid flow_type: {flow_type}. Must be 'inflow' or 'outflow'"
            )

        if year not in urls:
            raise ValueError(
                f"Migration data not available for year {year}. "
                f"Available years: {sorted(urls.keys())}"
            )

        url = urls[year]
        df = await self.download_and_parse_csv(url, use_cache)

        # Add metadata columns
        df["tax_year"] = year
        df["flow_type"] = flow_type

        return df

    # ========== Business Income Data ==========

    async def get_business_income_data(
        self, year: int, use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Download and parse business income by ZIP data.

        Args:
            year: Tax year
            use_cache: Whether to use cached files

        Returns:
            DataFrame with business income data
        """
        if year not in self.BUSINESS_INCOME_URLS:
            raise ValueError(
                f"Business income data not available for year {year}. "
                f"Available years: {sorted(self.BUSINESS_INCOME_URLS.keys())}"
            )

        url = self.BUSINESS_INCOME_URLS[year]
        df = await self.download_and_parse_csv(url, use_cache)

        # Add tax year column
        df["tax_year"] = year

        return df

    async def _backoff(self, attempt: int):
        """
        Exponential backoff with jitter.

        Args:
            attempt: Current attempt number (0-indexed)
        """
        base_delay = 1.0
        max_delay = 60.0

        delay = min(base_delay * (self.backoff_factor**attempt), max_delay)

        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)

        logger.debug(f"Backing off for {delay_with_jitter:.2f}s")
        await asyncio.sleep(delay_with_jitter)


# Available years for reference
AVAILABLE_YEARS = {
    "zip_income": sorted(IRSSOIClient.ZIP_INCOME_URLS.keys(), reverse=True),
    "county_income": sorted(IRSSOIClient.COUNTY_INCOME_URLS.keys(), reverse=True),
    "migration": sorted(IRSSOIClient.MIGRATION_INFLOW_URLS.keys(), reverse=True),
    "business_income": sorted(IRSSOIClient.BUSINESS_INCOME_URLS.keys(), reverse=True),
}

# Default year (most recent available)
DEFAULT_YEAR = max(AVAILABLE_YEARS["zip_income"])
