"""
SEC EDGAR API client with rate limiting and retry logic.

Official SEC EDGAR API documentation:
https://www.sec.gov/edgar/sec-api-documentation

SEC EDGAR provides access to corporate filings:
- 10-K (Annual reports)
- 10-Q (Quarterly reports)
- 8-K (Current reports)
- S-1/S-3/S-4 (Registration statements)
- XBRL data

Rate limits:
- 10 requests per second per IP (strictly enforced)
- User-Agent header REQUIRED (must identify yourself)
- Rate limit documentation: https://www.sec.gov/os/accessing-edgar-data
"""
import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime
import time

logger = logging.getLogger(__name__)


class SECClient:
    """
    HTTP client for SEC EDGAR API with bounded concurrency and rate limiting.
    
    Responsibilities:
    - Make HTTP requests to SEC EDGAR API
    - Implement retry logic with exponential backoff
    - Respect rate limits via semaphore and rate limiter
    - Handle API errors gracefully
    """
    
    # SEC EDGAR API base URLs
    BASE_URL = "https://data.sec.gov"
    SUBMISSIONS_URL = f"{BASE_URL}/submissions"
    
    # Rate limit (SEC requires max 10 requests per second)
    DEFAULT_MAX_REQUESTS_PER_SECOND = 8  # Conservative (slightly under limit)
    DEFAULT_MAX_CONCURRENCY = 2
    
    # User-Agent is REQUIRED by SEC
    # SEC requests: "Your User-Agent should include contact information for you or your company"
    USER_AGENT = "Nexdata External Data Ingestion Service (contact: compliance@nexdata.com)"
    
    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_requests_per_second: float = DEFAULT_MAX_REQUESTS_PER_SECOND,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize SEC EDGAR API client.
        
        Args:
            max_concurrency: Maximum concurrent requests
            max_requests_per_second: Rate limit (SEC allows 10/sec)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.max_concurrency = max_concurrency
        self.max_requests_per_second = max_requests_per_second
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        # Rate limiting: Track request timestamps
        self.request_times: List[float] = []
        self.rate_limit_lock = asyncio.Lock()
        
        # HTTP client (will be created in async context)
        self._client: Optional[httpx.AsyncClient] = None
        
        logger.info(
            f"Initialized SECClient: "
            f"max_concurrency={max_concurrency}, "
            f"max_requests_per_second={max_requests_per_second}"
        )
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Accept": "application/json"
                }
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limit(self):
        """
        Implement rate limiting using sliding window.
        Ensures we don't exceed max_requests_per_second.
        """
        async with self.rate_limit_lock:
            now = time.time()
            
            # Remove requests older than 1 second
            self.request_times = [
                t for t in self.request_times 
                if now - t < 1.0
            ]
            
            # If we've hit the limit, wait
            if len(self.request_times) >= self.max_requests_per_second:
                # Calculate how long to wait
                oldest = self.request_times[0]
                wait_time = 1.0 - (now - oldest)
                
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.3f}s")
                    await asyncio.sleep(wait_time)
                    
                    # Clear old timestamps after waiting
                    now = time.time()
                    self.request_times = [
                        t for t in self.request_times 
                        if now - t < 1.0
                    ]
            
            # Record this request
            self.request_times.append(time.time())
    
    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """
        Fetch company submission history from SEC EDGAR.
        
        Args:
            cik: Company CIK (Central Index Key), e.g., "0000320193" for Apple
            
        Returns:
            Dict containing company information and filing history
            
        Raises:
            Exception: On API errors after retries
        """
        # Ensure CIK is 10 digits with leading zeros
        cik_padded = str(cik).zfill(10)
        
        url = f"{self.SUBMISSIONS_URL}/CIK{cik_padded}.json"
        
        return await self._request_with_retry(url, cik_padded)
    
    async def get_company_facts(self, cik: str) -> Dict[str, Any]:
        """
        Fetch company facts (XBRL data) from SEC EDGAR.
        
        Args:
            cik: Company CIK (Central Index Key)
            
        Returns:
            Dict containing XBRL facts for the company
        """
        cik_padded = str(cik).zfill(10)
        
        url = f"{self.BASE_URL}/api/xbrl/companyfacts/CIK{cik_padded}.json"
        
        return await self._request_with_retry(url, cik_padded)
    
    async def get_multiple_companies(
        self,
        ciks: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch multiple companies concurrently (with bounded concurrency).
        
        Args:
            ciks: List of CIK numbers
            
        Returns:
            Dict mapping CIK to company submission data
        """
        tasks = []
        for cik in ciks:
            task = self.get_company_submissions(cik)
            tasks.append((cik, task))
        
        results = {}
        for cik, task in tasks:
            try:
                response = await task
                results[cik] = response
            except Exception as e:
                logger.error(f"Failed to fetch CIK {cik}: {e}")
                results[cik] = None
        
        return results
    
    async def _request_with_retry(
        self,
        url: str,
        identifier: str
    ) -> Dict[str, Any]:
        """
        Make HTTP GET request with exponential backoff retry.
        
        Args:
            url: API endpoint URL
            identifier: Identifier being requested (for logging)
            
        Returns:
            Parsed JSON response
            
        Raises:
            Exception: After all retries exhausted
        """
        async with self.semaphore:  # Bounded concurrency
            # Apply rate limiting
            await self._rate_limit()
            
            client = await self._get_client()
            
            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"Fetching SEC data for {identifier} "
                        f"(attempt {attempt+1}/{self.max_retries})"
                    )
                    
                    response = await client.get(url)
                    
                    # Check for HTTP errors
                    response.raise_for_status()
                    
                    # Parse JSON response
                    data = response.json()
                    
                    # Success!
                    logger.debug(f"Successfully fetched data for {identifier}")
                    return data
                
                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching SEC data (attempt {attempt+1}): {e}"
                    )
                    
                    # Check for rate limiting (429)
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = int(retry_after)
                            logger.warning(f"Rate limited. Waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                    
                    # 404 Not Found is not retryable
                    if e.response.status_code == 404:
                        raise Exception(
                            f"SEC data not found for {identifier} (404)"
                        )
                    
                    # Retry on 5xx errors, not on 4xx
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"SEC API HTTP error: {e.response.status_code} - "
                            f"{e.response.text}"
                        )
                
                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error fetching SEC data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"SEC API request failed: {str(e)}")
                
                except Exception as e:
                    logger.error(
                        f"Unexpected error fetching SEC data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise
            
            # Should never reach here, but just in case
            raise Exception(f"Failed to fetch SEC data after {self.max_retries} attempts")
    
    async def _backoff(self, attempt: int):
        """
        Exponential backoff with jitter.
        
        Args:
            attempt: Current attempt number (0-indexed)
        """
        # Calculate backoff: base * factor^attempt + random jitter
        base_delay = 1.0
        max_delay = 60.0
        
        delay = min(
            base_delay * (self.backoff_factor ** attempt),
            max_delay
        )
        
        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)
        
        logger.debug(f"Backing off for {delay_with_jitter:.2f}s")
        await asyncio.sleep(delay_with_jitter)


# Common CIK numbers for major companies
COMMON_COMPANIES = {
    "tech": {
        "apple": "0000320193",
        "microsoft": "0000789019",
        "alphabet": "0001652044",
        "amazon": "0001018724",
        "meta": "0001326801",
        "tesla": "0001318605",
        "nvidia": "0001045810",
    },
    "finance": {
        "jpmorgan": "0000019617",
        "bank_of_america": "0000070858",
        "wells_fargo": "0000072971",
        "goldman_sachs": "0000886982",
        "morgan_stanley": "0000895421",
        "berkshire_hathaway": "0001067983",
    },
    "healthcare": {
        "johnson_johnson": "0000200406",
        "pfizer": "0000078003",
        "unitedhealth": "0000731766",
        "abbvie": "0001551152",
        "merck": "0000310158",
    },
    "energy": {
        "exxon": "0000034088",
        "chevron": "0000093410",
        "conocophillips": "0001163165",
    }
}

