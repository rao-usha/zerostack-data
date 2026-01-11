"""
Base HTTP client with unified retry logic, rate limiting, and error handling.

Provides a reusable foundation for all external API clients in the application.
Implements bounded concurrency, exponential backoff, and standardized error handling.
"""
import asyncio
import logging
import random
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable, TypeVar, Union
from datetime import datetime
import httpx

from app.core.api_errors import (
    APIError,
    RetryableError,
    RateLimitError,
    FatalError,
    AuthenticationError,
    NotFoundError,
    ValidationError,
    classify_http_error
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BaseAPIClient(ABC):
    """
    Base class for all external API clients.

    Provides unified:
    - HTTP request handling with retry logic
    - Exponential backoff with jitter
    - Rate limiting via semaphore
    - Standardized error classification
    - Connection pooling

    Subclasses should:
    - Set SOURCE_NAME and BASE_URL class attributes
    - Implement API-specific methods that call _request()
    - Override _check_api_error() for API-specific error detection
    """

    # Override in subclass
    SOURCE_NAME: str = "unknown"
    BASE_URL: str = ""

    # Default settings
    DEFAULT_MAX_CONCURRENCY: int = 2
    DEFAULT_TIMEOUT: float = 30.0
    DEFAULT_CONNECT_TIMEOUT: float = 10.0
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_BACKOFF_FACTOR: float = 2.0
    DEFAULT_MAX_BACKOFF: float = 60.0
    DEFAULT_JITTER_FACTOR: float = 0.25

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        timeout: float = DEFAULT_TIMEOUT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        rate_limit_interval: Optional[float] = None
    ):
        """
        Initialize the API client.

        Args:
            api_key: Optional API key for authentication
            max_concurrency: Maximum concurrent requests (semaphore size)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
            timeout: Request timeout in seconds
            connect_timeout: Connection timeout in seconds
            rate_limit_interval: Minimum seconds between requests (None = no limit)
        """
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.rate_limit_interval = rate_limit_interval

        # Semaphore for bounded concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # Rate limiting state
        self._last_request_time: float = 0
        self._rate_limit_lock = asyncio.Lock()

        # HTTP client (lazy initialization)
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(
            f"Initialized {self.SOURCE_NAME} client: "
            f"api_key_present={api_key is not None}, "
            f"max_concurrency={max_concurrency}, "
            f"max_retries={max_retries}"
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client with connection pooling."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=self.connect_timeout),
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=self.max_concurrency * 2,
                    max_keepalive_connections=self.max_concurrency
                )
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.debug(f"{self.SOURCE_NAME} client closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - ensures cleanup."""
        await self.close()

    async def _enforce_rate_limit(self) -> None:
        """Enforce minimum interval between requests."""
        if self.rate_limit_interval is None:
            return

        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_time
            if elapsed < self.rate_limit_interval:
                wait_time = self.rate_limit_interval - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            self._last_request_time = asyncio.get_event_loop().time()

    async def _backoff(self, attempt: int, base_delay: float = 1.0) -> None:
        """
        Exponential backoff with jitter.

        Args:
            attempt: Current attempt number (0-indexed)
            base_delay: Base delay in seconds
        """
        delay = min(
            base_delay * (self.backoff_factor ** attempt),
            self.DEFAULT_MAX_BACKOFF
        )

        # Add jitter (±25% by default)
        jitter = delay * self.DEFAULT_JITTER_FACTOR * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)

        logger.debug(f"Backing off for {delay_with_jitter:.2f}s (attempt {attempt + 1})")
        await asyncio.sleep(delay_with_jitter)

    def _check_api_error(
        self,
        data: Dict[str, Any],
        resource_id: str
    ) -> Optional[APIError]:
        """
        Check API response for source-specific errors.

        Override in subclass to handle API-specific error formats.

        Args:
            data: Parsed JSON response
            resource_id: Resource being requested (for logging)

        Returns:
            APIError if error detected, None otherwise
        """
        # Default implementation checks common patterns
        # Subclasses should override for API-specific error handling

        # Common pattern: "error" field
        if "error" in data:
            error_msg = data.get("error")
            if isinstance(error_msg, dict):
                error_msg = error_msg.get("message", str(error_msg))
            return FatalError(
                message=str(error_msg),
                source=self.SOURCE_NAME,
                response_data=data
            )

        # Common pattern: "error_code" field
        if "error_code" in data:
            error_code = data.get("error_code")
            error_message = data.get("error_message", "Unknown error")
            if error_code in [400, 404]:
                return FatalError(
                    message=f"API error {error_code}: {error_message}",
                    source=self.SOURCE_NAME,
                    status_code=error_code,
                    response_data=data
                )
            return RetryableError(
                message=f"API error {error_code}: {error_message}",
                source=self.SOURCE_NAME,
                status_code=error_code,
                response_data=data
            )

        return None

    def _build_headers(self) -> Dict[str, str]:
        """
        Build request headers.

        Override to add API-specific headers (e.g., Authorization).

        Returns:
            Dict of headers
        """
        return {
            "Accept": "application/json",
            "User-Agent": f"Nexdata/{self.SOURCE_NAME}-client"
        }

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add authentication to request parameters.

        Override to add API-specific auth (e.g., api_key param).

        Args:
            params: Request parameters

        Returns:
            Parameters with authentication added
        """
        return params

    async def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        resource_id: str = "unknown",
        extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL or path (if path, BASE_URL is prepended)
            params: Query parameters
            json_body: JSON body for POST/PUT requests
            resource_id: Identifier for logging
            extra_headers: Additional headers to include

        Returns:
            Parsed JSON response

        Raises:
            APIError: On unrecoverable errors
        """
        # Prepend BASE_URL if url is a path
        if not url.startswith("http"):
            url = f"{self.BASE_URL.rstrip('/')}/{url.lstrip('/')}"

        # Build params and headers
        params = self._add_auth_to_params(params or {})
        headers = self._build_headers()
        if extra_headers:
            headers.update(extra_headers)

        async with self.semaphore:
            await self._enforce_rate_limit()
            client = await self._get_client()

            last_error: Optional[Exception] = None

            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"[{self.SOURCE_NAME}] {method} {resource_id} "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )

                    # Make request
                    if method.upper() == "GET":
                        response = await client.get(url, params=params, headers=headers)
                    elif method.upper() == "POST":
                        response = await client.post(
                            url,
                            params=params,
                            json=json_body,
                            headers={**headers, "Content-Type": "application/json"}
                        )
                    else:
                        response = await client.request(
                            method,
                            url,
                            params=params,
                            json=json_body,
                            headers=headers
                        )

                    # Check HTTP status
                    response.raise_for_status()

                    # Parse JSON
                    data = response.json()

                    # Check for API-specific errors
                    api_error = self._check_api_error(data, resource_id)
                    if api_error:
                        if api_error.retryable and attempt < self.max_retries - 1:
                            logger.warning(
                                f"[{self.SOURCE_NAME}] Retryable API error: {api_error}"
                            )
                            await self._backoff(attempt)
                            last_error = api_error
                            continue
                        raise api_error

                    # Success!
                    logger.debug(f"[{self.SOURCE_NAME}] Successfully fetched {resource_id}")
                    return data

                except httpx.HTTPStatusError as e:
                    error = classify_http_error(
                        e.response.status_code,
                        e.response.text[:500],
                        self.SOURCE_NAME
                    )

                    if isinstance(error, RateLimitError):
                        retry_after = e.response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else error.retry_after
                        logger.warning(
                            f"[{self.SOURCE_NAME}] Rate limited. Waiting {wait_time}s"
                        )
                        await asyncio.sleep(wait_time)
                        last_error = error
                        continue

                    if error.retryable and attempt < self.max_retries - 1:
                        logger.warning(
                            f"[{self.SOURCE_NAME}] Retryable HTTP error: {error}"
                        )
                        await self._backoff(attempt)
                        last_error = error
                        continue

                    raise error

                except httpx.RequestError as e:
                    # Network errors are retryable
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"[{self.SOURCE_NAME}] Request error (attempt {attempt + 1}): {e}"
                        )
                        await self._backoff(attempt)
                        last_error = e
                        continue
                    raise RetryableError(
                        message=f"Request failed: {str(e)}",
                        source=self.SOURCE_NAME
                    )

                except (FatalError, ValidationError, AuthenticationError, NotFoundError):
                    # Don't retry fatal errors
                    raise

                except Exception as e:
                    if attempt < self.max_retries - 1:
                        logger.error(
                            f"[{self.SOURCE_NAME}] Unexpected error (attempt {attempt + 1}): {e}"
                        )
                        await self._backoff(attempt)
                        last_error = e
                        continue
                    raise APIError(
                        message=f"Unexpected error: {str(e)}",
                        source=self.SOURCE_NAME,
                        retryable=False
                    )

            # All retries exhausted
            if last_error:
                if isinstance(last_error, APIError):
                    raise last_error
                raise RetryableError(
                    message=f"Failed after {self.max_retries} attempts: {str(last_error)}",
                    source=self.SOURCE_NAME
                )

            raise APIError(
                message=f"Failed to fetch {resource_id} after {self.max_retries} attempts",
                source=self.SOURCE_NAME
            )

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        resource_id: str = "unknown"
    ) -> Dict[str, Any]:
        """
        Make GET request.

        Args:
            url: URL or path
            params: Query parameters
            resource_id: Identifier for logging

        Returns:
            Parsed JSON response
        """
        return await self._request("GET", url, params=params, resource_id=resource_id)

    async def post(
        self,
        url: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        resource_id: str = "unknown"
    ) -> Dict[str, Any]:
        """
        Make POST request.

        Args:
            url: URL or path
            json_body: JSON body
            params: Query parameters
            resource_id: Identifier for logging

        Returns:
            Parsed JSON response
        """
        return await self._request(
            "POST",
            url,
            params=params,
            json_body=json_body,
            resource_id=resource_id
        )

    async def fetch_multiple(
        self,
        items: List[T],
        fetch_func: Callable[[T], Any],
        item_id_func: Callable[[T], str] = str
    ) -> Dict[str, Any]:
        """
        Fetch multiple items concurrently (bounded by semaphore).

        Args:
            items: List of items to fetch
            fetch_func: Async function to fetch each item
            item_id_func: Function to get item ID for results dict

        Returns:
            Dict mapping item_id to result (or empty list on error)
        """
        results = {}

        async def fetch_one(item: T) -> None:
            item_id = item_id_func(item)
            try:
                result = await fetch_func(item)
                results[item_id] = result
            except Exception as e:
                logger.error(f"[{self.SOURCE_NAME}] Failed to fetch {item_id}: {e}")
                results[item_id] = []

        await asyncio.gather(*[fetch_one(item) for item in items])
        return results
