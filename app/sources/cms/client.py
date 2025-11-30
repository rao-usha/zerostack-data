"""
CMS API client with rate limiting and retry logic.

Handles HTTP communication with CMS data sources including:
- data.cms.gov Socrata Open Data API (SODA)
- CMS bulk download endpoints
"""
import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
from datetime import datetime
import httpx

logger = logging.getLogger(__name__)


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
    BULK_DOWNLOAD_BASE_URL = "https://www.cms.gov/files"
    
    def __init__(
        self,
        max_concurrency: int = 4,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize CMS API client.
        
        Args:
            max_concurrency: Maximum concurrent requests (bounded concurrency)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per GLOBAL RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
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
        select: Optional[str] = None
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
        max_records: Optional[int] = None
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
                select=select
            )
            
            logger.info(f"Fetching CMS Socrata data: offset={current_offset}, limit={current_limit}")
            
            async with self.semaphore:  # Bounded concurrency
                records = await self._fetch_with_retry(url)
            
            if not records:
                # No more data
                break
            
            all_records.extend(records)
            
            # Check if we got fewer records than requested (last page)
            if len(records) < current_limit:
                break
            
            current_offset += len(records)
        
        logger.info(f"Fetched {len(all_records)} total records from Socrata dataset {dataset_id}")
        return all_records
    
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
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.json()
            
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    # Respect Retry-After header if present
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after:
                        wait_time = float(retry_after)
                    else:
                        # Exponential backoff with jitter
                        wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                    
                    logger.warning(f"Rate limited, waiting {wait_time:.2f}s before retry")
                    await asyncio.sleep(wait_time)
                    
                    if attempt < self.max_retries:
                        continue
                
                # Non-retryable error or max retries exceeded
                logger.error(f"HTTP error fetching data: {e}")
                raise
            
            except (httpx.RequestError, httpx.TimeoutException) as e:
                if attempt < self.max_retries:
                    # Exponential backoff with jitter
                    wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Request failed, retrying in {wait_time:.2f}s: {e}")
                    await asyncio.sleep(wait_time)
                    continue
                
                logger.error(f"Failed to fetch data after {self.max_retries} retries: {e}")
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
        
        async with self.semaphore:  # Bounded concurrency
            for attempt in range(self.max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=300.0) as client:  # Longer timeout for large files
                        response = await client.get(file_url)
                        response.raise_for_status()
                        logger.info(f"Downloaded {len(response.content)} bytes")
                        return response.content
                
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After")
                        wait_time = float(retry_after) if retry_after else (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Rate limited, waiting {wait_time:.2f}s before retry")
                        await asyncio.sleep(wait_time)
                        if attempt < self.max_retries:
                            continue
                    
                    logger.error(f"HTTP error fetching bulk file: {e}")
                    raise
                
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    if attempt < self.max_retries:
                        wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Request failed, retrying in {wait_time:.2f}s: {e}")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    logger.error(f"Failed to fetch bulk file after {self.max_retries} retries: {e}")
                    raise
        
        raise Exception("Failed to fetch bulk file")
    
    async def close(self):
        """
        Clean up resources.
        
        Currently no persistent resources to clean up (httpx clients are context-managed).
        """
        pass

