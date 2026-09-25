"""
CMS API client with rate limiting and retry logic.

Handles HTTP communication with CMS data sources including:
- data.cms.gov Socrata Open Data API (SODA)
- CMS bulk download endpoints
"""

import asyncio
import logging
import os
import random
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
DEFAULT_REQUESTS_PER_SECOND = 1.0


async def _shared_bucket_acquire(domain: str) -> bool:
    """Take a token from the Postgres-backed per-domain bucket shared by all
    workers (``rate_limit_bucket``; data.cms.gov = 1 req/s). Never blocks a
    request on limiter failure."""
    try:
        from app.core.database import get_session_factory
        from app.core.rate_limiter import acquire_distributed_token_with_wait

        db = get_session_factory()()
        try:
            return await acquire_distributed_token_with_wait(db, domain, max_wait=60.0)
        finally:
            db.close()
    except Exception as e:  # limiter unavailable: fall back to local pacing only
        logger.debug(f"CMS distributed rate limit skipped for {domain}: {e}")
        return True


class CMSClient:
    """
    HTTP client for CMS APIs with bounded concurrency and rate limiting.

    Responsibilities:
    - Build CMS API URLs
    - Make HTTP requests with retry/backoff
    - Respect rate limits via semaphore
    - Handle API errors gracefully
    """

    # CMS API base URLs
    SOCRATA_BASE_URL = "https://data.cms.gov/resource"
    DKAN_BASE_URL = "https://data.cms.gov/data-api/v1/dataset"
    BULK_DOWNLOAD_BASE_URL = "https://downloads.cms.gov/FILES/HCRIS"

    def __init__(
        self,
        max_concurrency: int = 4,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        requests_per_second: Optional[float] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        distributed_acquire: Optional[Callable[[str], Awaitable[bool]]] = None,
    ):
        """
        Initialize CMS API client.

        Args:
            max_concurrency: Maximum concurrent requests (bounded concurrency)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
            requests_per_second: Local pacing for every request (default
                CMS_REQUESTS_PER_SECOND env, else 1.0; 0 disables)
            transport: httpx transport override (tests)
            distributed_acquire: async ``(domain) -> bool`` token source; by
                default the shared rate_limit_bucket when WORKER_MODE=1
        """
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        if requests_per_second is None:
            requests_per_second = float(
                os.getenv("CMS_REQUESTS_PER_SECOND", DEFAULT_REQUESTS_PER_SECOND)
            )
        self.requests_per_second = requests_per_second
        self._transport = transport
        self._distributed_acquire = distributed_acquire

        # Semaphore for bounded concurrency - MANDATORY per GLOBAL RULES.
        # Every request (pages and bulk files) goes through it (SPEC_140).
        self.semaphore = asyncio.Semaphore(max_concurrency)
        # One pooled client for the life of this CMSClient: a fresh
        # AsyncClient per page meant a new TLS connection per ~75 KB page.
        self._http: Optional[httpx.AsyncClient] = None
        self._pace_lock = asyncio.Lock()
        self._last_request_at: Optional[float] = None
        self._now = time.monotonic
        self._sleep = asyncio.sleep

        logger.info(
            f"Initialized CMSClient with max_concurrency={max_concurrency}, "
            f"max_retries={max_retries}"
        )

    def build_socrata_url(
        self,
        dataset_id: str,
        limit: int = 1000,
        offset: int = 0,
        where: Optional[str] = None,
        select: Optional[str] = None,
    ) -> str:
        """
        Build Socrata Open Data API (SODA) URL.

        CMS uses Socrata for many datasets. SODA provides SQL-like query capabilities.

        Args:
            dataset_id: Socrata dataset identifier (e.g., "fs4p-t5eq")
            limit: Maximum number of records to return
            offset: Number of records to skip
            where: SoQL WHERE clause (optional)
            select: SoQL SELECT clause (optional)

        Returns:
            Full SODA API URL
        """
        url = f"{self.SOCRATA_BASE_URL}/{dataset_id}.json"
        params = [f"$limit={limit}", f"$offset={offset}"]

        if where:
            params.append(f"$where={where}")

        if select:
            params.append(f"$select={select}")

        return url + "?" + "&".join(params)

    async def fetch_socrata_data(
        self,
        dataset_id: str,
        limit: int = 1000,
        offset: int = 0,
        where: Optional[str] = None,
        select: Optional[str] = None,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from CMS Socrata Open Data API.

        Handles pagination automatically if max_records > limit.

        Args:
            dataset_id: Socrata dataset identifier
            limit: Records per page
            offset: Starting offset
            where: SoQL WHERE clause
            select: SoQL SELECT clause
            max_records: Maximum total records to fetch (None = all)

        Returns:
            List of records as dictionaries
        """
        all_records = []
        current_offset = offset

        while True:
            # Check if we've reached max_records
            if max_records and len(all_records) >= max_records:
                break

            # Adjust limit if approaching max_records
            current_limit = limit
            if max_records:
                remaining = max_records - len(all_records)
                current_limit = min(limit, remaining)

            # Fetch page
            url = self.build_socrata_url(
                dataset_id=dataset_id,
                limit=current_limit,
                offset=current_offset,
                where=where,
                select=select,
            )

            logger.info(
                f"Fetching CMS Socrata data: offset={current_offset}, limit={current_limit}"
            )

            records = await self._fetch_with_retry(url)

            if not records:
                # No more data
                break

            all_records.extend(records)

            # Check if we got fewer records than requested (last page)
            if len(records) < current_limit:
                break

            current_offset += len(records)

        logger.info(
            f"Fetched {len(all_records)} total records from Socrata dataset {dataset_id}"
        )
        return all_records

    def build_dkan_url(
        self,
        dataset_id: str,
        size: int = 1000,
        offset: int = 0,
        filters: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Build CMS DKAN API URL.

        CMS migrated from Socrata to DKAN in 2024. DKAN uses different
        pagination and filtering parameters.

        Args:
            dataset_id: DKAN dataset UUID (e.g., "92396110-2aed-4d63-a6a2-5d6207d46a29")
            size: Number of records per page
            offset: Number of records to skip
            filters: Dictionary of column filters (e.g., {"Rndrng_Prvdr_State_Abrvtn": "NY"})

        Returns:
            Full DKAN API URL
        """
        url = f"{self.DKAN_BASE_URL}/{dataset_id}/data"
        params = [f"size={size}", f"offset={offset}"]

        if filters:
            for col, value in filters.items():
                params.append(f"filter[{col}]={value}")

        return url + "?" + "&".join(params)

    async def iter_dkan_pages(
        self,
        dataset_id: str,
        size: int = 1000,
        offset: int = 0,
        filters: Optional[Dict[str, str]] = None,
        max_records: Optional[int] = None,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Yield DKAN pages one at a time, column names lower-cased to match our
        DB schema (DKAN returns CamelCase). Callers can persist each page
        before the next is fetched, so memory stays at one page (SPEC_140).
        """
        fetched = 0
        current_offset = offset

        while True:
            if max_records and fetched >= max_records:
                break

            current_size = size
            if max_records:
                current_size = min(size, max_records - fetched)

            url = self.build_dkan_url(
                dataset_id=dataset_id,
                size=current_size,
                offset=current_offset,
                filters=filters,
            )
            logger.info(
                f"Fetching CMS DKAN data: offset={current_offset}, size={current_size}"
            )
            records = await self._fetch_with_retry(url)
            if not records:
                break

            yield [{k.lower(): v for k, v in rec.items()} for rec in records]
            fetched += len(records)

            if len(records) < current_size:
                break
            current_offset += len(records)

    async def fetch_dkan_data(
        self,
        dataset_id: str,
        size: int = 1000,
        offset: int = 0,
        filters: Optional[Dict[str, str]] = None,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all DKAN pages into one list (see ``iter_dkan_pages`` to stream).

        Args:
            dataset_id: DKAN dataset UUID
            size: Records per page
            offset: Starting offset
            filters: Column filters (keys should be CamelCase as DKAN expects)
            max_records: Maximum total records to fetch (None = all)

        Returns:
            List of records with lowercased column names
        """
        all_records: List[Dict[str, Any]] = []
        async for page in self.iter_dkan_pages(
            dataset_id, size=size, offset=offset, filters=filters, max_records=max_records
        ):
            all_records.extend(page)
        logger.info(
            f"Fetched {len(all_records)} total records from DKAN dataset {dataset_id}"
        )
        return all_records

    # -- transport + rate limiting (SPEC_140) ---------------------------------

    def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=60.0,
                headers={"User-Agent": USER_AGENT},
                limits=httpx.Limits(
                    max_connections=self.max_concurrency,
                    max_keepalive_connections=self.max_concurrency,
                ),
                transport=self._transport,
            )
        return self._http

    async def _throttle(self, url: str) -> None:
        """Pace this request: local minimum interval, then the shared
        per-domain bucket so the limit holds across every worker."""
        if self.requests_per_second and self.requests_per_second > 0:
            interval = 1.0 / self.requests_per_second
            async with self._pace_lock:
                now = self._now()
                if self._last_request_at is not None:
                    wait = self._last_request_at + interval - now
                    if wait > 0:
                        await self._sleep(wait)
                self._last_request_at = self._now()

        acquire = self._distributed_acquire
        if acquire is None and os.getenv("WORKER_MODE", "0") == "1":
            acquire = _shared_bucket_acquire
        if acquire is not None:
            domain = urlparse(url).netloc
            if domain and not await acquire(domain):
                logger.warning(f"CMS distributed rate limit wait timed out for {domain}")

    async def _get(self, url: str, timeout: Optional[float] = None) -> httpx.Response:
        async with self.semaphore:
            await self._throttle(url)
            if timeout is None:
                response = await self._client().get(url)
            else:
                response = await self._client().get(url, timeout=timeout)
            response.raise_for_status()
            return response

    async def _fetch_with_retry(self, url: str) -> List[Dict[str, Any]]:
        """
        Fetch data from URL with exponential backoff retry logic.

        Args:
            url: URL to fetch

        Returns:
            List of records

        Raises:
            httpx.HTTPError: On API errors after retries exhausted
        """
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._get(url)
                return response.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    # Respect Retry-After header if present
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after:
                        wait_time = float(retry_after)
                    else:
                        # Exponential backoff with jitter
                        wait_time = (self.backoff_factor**attempt) + random.uniform(
                            0, 1
                        )

                    logger.warning(
                        f"Rate limited, waiting {wait_time:.2f}s before retry"
                    )
                    await self._sleep(wait_time)

                    if attempt < self.max_retries:
                        continue

                # Non-retryable error or max retries exceeded
                logger.error(f"HTTP error fetching data: {e}")
                raise

            except (httpx.RequestError, httpx.TimeoutException) as e:
                if attempt < self.max_retries:
                    # Exponential backoff with jitter
                    wait_time = (self.backoff_factor**attempt) + random.uniform(0, 1)
                    logger.warning(f"Request failed, retrying in {wait_time:.2f}s: {e}")
                    await self._sleep(wait_time)
                    continue

                logger.error(
                    f"Failed to fetch data after {self.max_retries} retries: {e}"
                )
                raise

        raise Exception("Failed to fetch data")

    async def fetch_bulk_file(self, file_url: str) -> bytes:
        """
        Fetch a bulk data file from CMS.

        Used for datasets that are only available as bulk downloads
        (e.g., HCRIS hospital cost reports).

        Args:
            file_url: URL to bulk data file

        Returns:
            File content as bytes
        """
        logger.info(f"Fetching bulk file: {file_url}")

        # The semaphore and rate limit are taken per attempt in _get (SPEC_140),
        # so a backoff sleep no longer holds a concurrency slot.
        for attempt in range(self.max_retries + 1):
            try:
                # Longer timeout for large files
                response = await self._get(file_url, timeout=300.0)
                logger.info(f"Downloaded {len(response.content)} bytes")
                return response.content

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = e.response.headers.get("Retry-After")
                    wait_time = (
                        float(retry_after)
                        if retry_after
                        else (self.backoff_factor**attempt) + random.uniform(0, 1)
                    )
                    logger.warning(
                        f"Rate limited, waiting {wait_time:.2f}s before retry"
                    )
                    await self._sleep(wait_time)
                    if attempt < self.max_retries:
                        continue

                logger.error(f"HTTP error fetching bulk file: {e}")
                raise

            except (httpx.RequestError, httpx.TimeoutException) as e:
                if attempt < self.max_retries:
                    wait_time = (self.backoff_factor**attempt) + random.uniform(
                        0, 1
                    )
                    logger.warning(
                        f"Request failed, retrying in {wait_time:.2f}s: {e}"
                    )
                    await self._sleep(wait_time)
                    continue

                logger.error(
                    f"Failed to fetch bulk file after {self.max_retries} retries: {e}"
                )
                raise

        raise Exception("Failed to fetch bulk file")

    async def close(self):
        """
        Clean up resources.

        Closes the pooled HTTP client.
        """
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()
