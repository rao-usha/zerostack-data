"""
Retry handler with exponential backoff and circuit breaker pattern.

Provides reusable retry logic for all agentic strategies:
- Async retry decorator with configurable parameters
- Exponential backoff with jitter
- Circuit breaker for persistent failures
- Special handling for HTTP 429 (rate limit)
"""

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set, Type, Union

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 3
    base_delay: float = 1.0  # Initial delay in seconds
    max_delay: float = 60.0  # Maximum delay between retries
    exponential_base: float = 2.0  # Multiplier for exponential backoff
    jitter: float = 0.1  # Random jitter factor (0-1)

    # Retry on these exception types
    retry_exceptions: tuple = (Exception,)

    # Don't retry on these exceptions (takes precedence)
    fatal_exceptions: tuple = ()


@dataclass
class CircuitBreakerState:
    """State tracking for circuit breaker pattern."""

    failure_count: int = 0
    last_failure_time: float = 0.0
    is_open: bool = False

    # Configuration
    failure_threshold: int = 5  # Failures before opening circuit
    reset_timeout: float = 60.0  # Seconds before trying again


class CircuitBreaker:
    """
    Circuit breaker to prevent repeated calls to failing services.

    States:
    - CLOSED: Normal operation, requests go through
    - OPEN: Failures exceeded threshold, requests fail fast
    - HALF-OPEN: Testing if service recovered
    """

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._states: Dict[str, CircuitBreakerState] = {}

    def _get_state(self, key: str) -> CircuitBreakerState:
        """Get or create state for a key."""
        if key not in self._states:
            self._states[key] = CircuitBreakerState(
                failure_threshold=self.failure_threshold,
                reset_timeout=self.reset_timeout,
            )
        return self._states[key]

    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed through the circuit."""
        state = self._get_state(key)

        if not state.is_open:
            return True

        # Check if enough time has passed to try again (half-open)
        if time.time() - state.last_failure_time >= state.reset_timeout:
            logger.info(f"Circuit breaker half-open for {key}, allowing test request")
            return True

        return False

    def record_success(self, key: str) -> None:
        """Record a successful request."""
        state = self._get_state(key)
        if state.is_open:
            logger.info(f"Circuit breaker closed for {key} after successful request")
        state.failure_count = 0
        state.is_open = False

    def record_failure(self, key: str) -> None:
        """Record a failed request."""
        state = self._get_state(key)
        state.failure_count += 1
        state.last_failure_time = time.time()

        if state.failure_count >= state.failure_threshold:
            if not state.is_open:
                logger.warning(
                    f"Circuit breaker opened for {key} after "
                    f"{state.failure_count} failures"
                )
            state.is_open = True

    def get_status(self, key: str) -> Dict[str, Any]:
        """Get current circuit breaker status."""
        state = self._get_state(key)
        return {
            "key": key,
            "is_open": state.is_open,
            "failure_count": state.failure_count,
            "last_failure_time": state.last_failure_time,
        }


# Global circuit breaker instance
_circuit_breaker = CircuitBreaker()


def get_circuit_breaker() -> CircuitBreaker:
    """Get the global circuit breaker instance."""
    return _circuit_breaker


class RetryError(Exception):
    """Raised when all retries are exhausted."""

    def __init__(
        self, message: str, attempts: int, last_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""

    def __init__(self, key: str):
        super().__init__(f"Circuit breaker is open for {key}")
        self.key = key


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """
    Calculate delay before next retry using exponential backoff with jitter.

    Args:
        attempt: Current attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay = config.base_delay * (config.exponential_base**attempt)

    # Apply maximum cap
    delay = min(delay, config.max_delay)

    # Add jitter to prevent thundering herd
    jitter_range = delay * config.jitter
    delay += random.uniform(-jitter_range, jitter_range)

    return max(0, delay)


def with_retry(
    config: Optional[RetryConfig] = None, circuit_breaker_key: Optional[str] = None
):
    """
    Decorator to add retry logic with exponential backoff.

    Args:
        config: Retry configuration (uses defaults if None)
        circuit_breaker_key: Key for circuit breaker (uses function name if None)

    Example:
        @with_retry(RetryConfig(max_retries=5, base_delay=2.0))
        async def fetch_data():
            ...
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            cb_key = circuit_breaker_key or func.__name__
            cb = get_circuit_breaker()

            # Check circuit breaker
            if not cb.is_allowed(cb_key):
                raise CircuitOpenError(cb_key)

            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    cb.record_success(cb_key)
                    return result

                except config.fatal_exceptions as e:
                    # Don't retry fatal exceptions
                    logger.warning(f"Fatal exception in {func.__name__}: {e}")
                    cb.record_failure(cb_key)
                    raise

                except config.retry_exceptions as e:
                    last_exception = e

                    # Check for HTTP 429 (rate limit)
                    retry_after = _extract_retry_after(e)

                    if attempt < config.max_retries:
                        if retry_after:
                            delay = retry_after
                            logger.warning(
                                f"Rate limited in {func.__name__}, "
                                f"waiting {delay}s (attempt {attempt + 1}/{config.max_retries + 1})"
                            )
                        else:
                            delay = calculate_delay(attempt, config)
                            logger.warning(
                                f"Retry {attempt + 1}/{config.max_retries + 1} for "
                                f"{func.__name__} after {delay:.2f}s: {e}"
                            )

                        await asyncio.sleep(delay)
                    else:
                        # All retries exhausted
                        cb.record_failure(cb_key)
                        logger.error(
                            f"All {config.max_retries + 1} attempts failed for "
                            f"{func.__name__}: {e}"
                        )

            raise RetryError(
                f"All {config.max_retries + 1} retry attempts failed",
                attempts=config.max_retries + 1,
                last_exception=last_exception,
            )

        return wrapper

    return decorator


def _extract_retry_after(exception: Exception) -> Optional[float]:
    """
    Extract Retry-After value from exception if present.

    Handles HTTP 429 responses that include Retry-After header.
    """
    # Check if exception has response attribute (httpx, requests)
    response = getattr(exception, "response", None)
    if response is None:
        return None

    # Check status code
    status_code = getattr(response, "status_code", None)
    if status_code != 429:
        return None

    # Try to get Retry-After header
    headers = getattr(response, "headers", {})
    retry_after = headers.get("Retry-After") or headers.get("retry-after")

    if retry_after:
        try:
            return float(retry_after)
        except (ValueError, TypeError):
            pass

    # Default retry delay for 429
    return 60.0


async def retry_async(
    func: Callable,
    *args,
    config: Optional[RetryConfig] = None,
    circuit_breaker_key: Optional[str] = None,
    **kwargs,
) -> Any:
    """
    Functional alternative to decorator for one-off retries.

    Args:
        func: Async function to retry
        *args: Positional arguments for func
        config: Retry configuration
        circuit_breaker_key: Circuit breaker key
        **kwargs: Keyword arguments for func

    Returns:
        Result from successful function call

    Example:
        result = await retry_async(
            fetch_data,
            url,
            config=RetryConfig(max_retries=3)
        )
    """
    if config is None:
        config = RetryConfig()

    @with_retry(config=config, circuit_breaker_key=circuit_breaker_key)
    async def wrapped():
        return await func(*args, **kwargs)

    return await wrapped()
