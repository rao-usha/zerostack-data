"""
Standardized API error classification system.

Provides a unified error hierarchy for all external API clients.
Each error type indicates whether the operation should be retried
and includes context for debugging.
"""

from typing import Optional, Dict, Any


class APIError(Exception):
    """
    Base exception for all API-related errors.

    Attributes:
        message: Human-readable error description
        source: API source name (e.g., 'fred', 'eia', 'bls')
        status_code: HTTP status code if applicable
        response_data: Raw response data for debugging
        retryable: Whether this error should trigger a retry
    """

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.source = source
        self.status_code = status_code
        self.response_data = response_data
        self.retryable = retryable

    def __str__(self) -> str:
        parts = [self.message]
        if self.source:
            parts.insert(0, f"[{self.source}]")
        if self.status_code:
            parts.append(f"(HTTP {self.status_code})")
        return " ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "source": self.source,
            "status_code": self.status_code,
            "retryable": self.retryable,
            "response_data": self.response_data,
        }


class RetryableError(APIError):
    """
    Transient errors that should trigger a retry.

    Examples:
    - HTTP 500-599 server errors
    - Network timeouts
    - Temporary API unavailability
    - Connection reset errors
    """

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            source=source,
            status_code=status_code,
            response_data=response_data,
            retryable=True,
        )


class RateLimitError(APIError):
    """
    Rate limiting error (HTTP 429 or API-specific throttling).

    These errors are retryable but require waiting before retry.
    The retry_after attribute indicates how long to wait.
    """

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        retry_after: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            source=source,
            status_code=429,
            response_data=response_data,
            retryable=True,
        )
        self.retry_after = retry_after or 60  # Default to 60 seconds


class FatalError(APIError):
    """
    Non-retryable errors that indicate a permanent problem.

    Examples:
    - Invalid API key (401)
    - Resource not found (404)
    - Invalid request parameters (400)
    - Forbidden access (403)
    """

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            source=source,
            status_code=status_code,
            response_data=response_data,
            retryable=False,
        )


class AuthenticationError(FatalError):
    """
    Authentication failed - invalid or missing API key.

    HTTP 401 or API-specific authentication errors.
    """

    def __init__(
        self,
        message: str = "Authentication failed - check API key",
        source: Optional[str] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message, source=source, status_code=401, response_data=response_data
        )


class NotFoundError(FatalError):
    """
    Requested resource not found.

    HTTP 404 or API-specific "not found" responses.
    """

    def __init__(
        self,
        message: str = "Resource not found",
        source: Optional[str] = None,
        resource_id: Optional[str] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        if resource_id:
            message = f"{message}: {resource_id}"
        super().__init__(
            message=message, source=source, status_code=404, response_data=response_data
        )
        self.resource_id = resource_id


class ValidationError(FatalError):
    """
    Request validation failed - invalid parameters.

    HTTP 400 or API-specific validation errors.
    """

    def __init__(
        self,
        message: str = "Invalid request parameters",
        source: Optional[str] = None,
        invalid_params: Optional[Dict[str, str]] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message, source=source, status_code=400, response_data=response_data
        )
        self.invalid_params = invalid_params or {}


class ConfigurationError(FatalError):
    """
    Configuration error - missing required settings.

    Raised when required API keys or settings are not configured.
    """

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        missing_config: Optional[str] = None,
    ):
        super().__init__(
            message=message, source=source, status_code=None, response_data=None
        )
        self.missing_config = missing_config


def classify_http_error(
    status_code: int, response_text: str = "", source: Optional[str] = None
) -> APIError:
    """
    Classify an HTTP error into the appropriate APIError subclass.

    Args:
        status_code: HTTP status code
        response_text: Response body text
        source: API source name

    Returns:
        Appropriate APIError subclass instance
    """
    if status_code == 429:
        return RateLimitError(
            message=f"Rate limited: {response_text[:200]}", source=source
        )
    elif status_code == 401:
        return AuthenticationError(
            message=f"Authentication failed: {response_text[:200]}", source=source
        )
    elif status_code == 403:
        return FatalError(
            message=f"Access forbidden: {response_text[:200]}",
            source=source,
            status_code=403,
        )
    elif status_code == 404:
        return NotFoundError(message=f"Not found: {response_text[:200]}", source=source)
    elif status_code == 400:
        return ValidationError(
            message=f"Bad request: {response_text[:200]}", source=source
        )
    elif 500 <= status_code < 600:
        return RetryableError(
            message=f"Server error: {response_text[:200]}",
            source=source,
            status_code=status_code,
        )
    else:
        return APIError(
            message=f"HTTP error {status_code}: {response_text[:200]}",
            source=source,
            status_code=status_code,
            retryable=False,
        )
