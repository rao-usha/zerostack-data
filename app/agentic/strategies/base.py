"""
Base strategy class for portfolio collection strategies.

All strategies must inherit from BaseStrategy and implement:
- execute(): Main collection logic
- is_applicable(): Check if strategy can be used for given investor
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.agentic.retry_handler import (
    RetryConfig,
    with_retry,
    CircuitOpenError,
    RetryError,
)

logger = logging.getLogger(__name__)


@dataclass
class StrategyResult:
    """Result from executing a collection strategy."""

    strategy_name: str
    success: bool
    companies_found: List[Dict[str, Any]] = field(default_factory=list)
    co_investors: List[Dict[str, Any]] = field(default_factory=list)

    # Metadata
    source_type: str = ""
    confidence_level: str = "medium"  # 'high', 'medium', 'low'

    # Resource usage
    requests_made: int = 0
    tokens_used: int = 0

    # Errors and reasoning
    error_message: Optional[str] = None
    reasoning: str = ""

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class InvestorContext:
    """Context about an investor for strategy planning."""

    investor_id: int
    investor_type: str  # 'lp' or 'family_office'
    investor_name: str

    # Optional metadata
    formal_name: Optional[str] = None
    lp_type: Optional[str] = (
        None  # 'public_pension', 'sovereign_wealth', 'endowment', etc.
    )
    jurisdiction: Optional[str] = None
    website_url: Optional[str] = None
    aum_usd: Optional[float] = None
    sec_crd_number: Optional[str] = None

    # For family offices
    estimated_wealth: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "investor_id": self.investor_id,
            "investor_type": self.investor_type,
            "investor_name": self.investor_name,
            "formal_name": self.formal_name,
            "lp_type": self.lp_type,
            "jurisdiction": self.jurisdiction,
            "website_url": self.website_url,
            "aum_usd": self.aum_usd,
            "sec_crd_number": self.sec_crd_number,
            "estimated_wealth": self.estimated_wealth,
        }


class BaseStrategy(ABC):
    """
    Abstract base class for portfolio collection strategies.

    Each strategy implements a different method for discovering portfolio data.
    Strategies must:
    1. Check applicability for a given investor
    2. Execute collection logic
    3. Return standardized results
    """

    # Strategy metadata (override in subclasses)
    name: str = "base_strategy"
    display_name: str = "Base Strategy"
    source_type: str = "unknown"
    default_confidence: str = "medium"

    # Rate limiting defaults (can be overridden)
    max_requests_per_second: float = 0.5  # 1 request per 2 seconds
    max_concurrent_requests: int = 1
    timeout_seconds: int = 300  # 5 minutes max

    # Retry configuration defaults
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0

    def __init__(
        self,
        max_requests_per_second: Optional[float] = None,
        max_concurrent_requests: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_base_delay: Optional[float] = None,
    ):
        """
        Initialize strategy with rate limiting and retry configuration.

        Args:
            max_requests_per_second: Override default rate limit
            max_concurrent_requests: Override default concurrency
            timeout_seconds: Override default timeout
            max_retries: Override default retry count
            retry_base_delay: Override default retry delay
        """
        if max_requests_per_second is not None:
            self.max_requests_per_second = max_requests_per_second
        if max_concurrent_requests is not None:
            self.max_concurrent_requests = max_concurrent_requests
        if timeout_seconds is not None:
            self.timeout_seconds = timeout_seconds
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_base_delay is not None:
            self.retry_base_delay = retry_base_delay

        logger.info(
            f"Initialized {self.name}: "
            f"rate={self.max_requests_per_second}/s, "
            f"concurrency={self.max_concurrent_requests}, "
            f"timeout={self.timeout_seconds}s, "
            f"retries={self.max_retries}"
        )

    def get_retry_config(self) -> RetryConfig:
        """Get retry configuration for this strategy."""
        return RetryConfig(
            max_retries=self.max_retries,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay,
        )

    @abstractmethod
    def is_applicable(self, context: InvestorContext) -> tuple[bool, str]:
        """
        Check if this strategy can be used for the given investor.

        Args:
            context: Investor context with metadata

        Returns:
            Tuple of (is_applicable, reasoning)
        """
        pass

    @abstractmethod
    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute the collection strategy.

        Args:
            context: Investor context with metadata

        Returns:
            StrategyResult with found companies and metadata
        """
        pass

    def calculate_priority(self, context: InvestorContext) -> int:
        """
        Calculate priority score for this strategy (0-10, higher = higher priority).

        Override in subclasses for strategy-specific prioritization.

        Args:
            context: Investor context

        Returns:
            Priority score (0-10)
        """
        applicable, _ = self.is_applicable(context)
        return 5 if applicable else 0

    def _create_result(
        self,
        success: bool,
        companies: Optional[List[Dict[str, Any]]] = None,
        co_investors: Optional[List[Dict[str, Any]]] = None,
        error_message: Optional[str] = None,
        reasoning: str = "",
        requests_made: int = 0,
        tokens_used: int = 0,
    ) -> StrategyResult:
        """
        Helper to create a standardized StrategyResult.

        Args:
            success: Whether the strategy succeeded
            companies: List of found companies
            co_investors: List of found co-investors
            error_message: Error message if failed
            reasoning: Agent reasoning for this execution
            requests_made: Number of HTTP requests made
            tokens_used: Number of LLM tokens used

        Returns:
            StrategyResult instance
        """
        return StrategyResult(
            strategy_name=self.name,
            success=success,
            companies_found=companies or [],
            co_investors=co_investors or [],
            source_type=self.source_type,
            confidence_level=self.default_confidence,
            requests_made=requests_made,
            tokens_used=tokens_used,
            error_message=error_message,
            reasoning=reasoning,
            completed_at=datetime.utcnow(),
        )
