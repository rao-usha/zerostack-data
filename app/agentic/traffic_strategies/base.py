"""
Base strategy class for foot traffic collection strategies.

All strategies must inherit from BaseTrafficStrategy and implement:
- execute(): Main collection logic
- is_applicable(): Check if strategy can be used for given context
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TrafficStrategyResult:
    """Result from executing a traffic collection strategy."""
    
    strategy_name: str
    success: bool
    
    # Data found
    locations_found: List[Dict[str, Any]] = field(default_factory=list)
    observations_found: List[Dict[str, Any]] = field(default_factory=list)
    metadata_enriched: Dict[str, Any] = field(default_factory=dict)
    
    # Source metadata
    source_type: str = ""
    confidence_level: str = "medium"  # 'high', 'medium', 'low'
    
    # Resource usage
    requests_made: int = 0
    cost_estimate_usd: float = 0.0
    
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
class LocationContext:
    """Context about a location or brand for strategy planning."""
    
    # Target specification
    brand_name: Optional[str] = None
    location_id: Optional[int] = None
    
    # Geographic scope
    city: Optional[str] = None
    state: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_meters: int = 5000
    
    # Date range for traffic data
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    
    # External IDs (if known)
    google_place_id: Optional[str] = None
    safegraph_placekey: Optional[str] = None
    foursquare_fsq_id: Optional[str] = None
    placer_venue_id: Optional[str] = None
    
    # Metadata
    category: Optional[str] = None  # restaurant, retail, etc.
    portfolio_company_id: Optional[int] = None  # If tracking portfolio company
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "brand_name": self.brand_name,
            "location_id": self.location_id,
            "city": self.city,
            "state": self.state,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None,
            "category": self.category,
        }


class BaseTrafficStrategy(ABC):
    """
    Abstract base class for foot traffic collection strategies.
    
    Each strategy implements a different method for collecting location or traffic data.
    Strategies must:
    1. Check applicability for a given context
    2. Execute collection logic
    3. Return standardized results
    """
    
    # Strategy metadata (override in subclasses)
    name: str = "base_traffic_strategy"
    display_name: str = "Base Traffic Strategy"
    source_type: str = "unknown"
    default_confidence: str = "medium"
    requires_api_key: bool = True
    
    # Cost estimation (per request)
    cost_per_request_usd: float = 0.0
    
    # Rate limiting defaults
    max_requests_per_second: float = 0.2  # 1 request per 5 seconds
    max_concurrent_requests: int = 1
    timeout_seconds: int = 300  # 5 minutes max
    
    def __init__(
        self,
        max_requests_per_second: Optional[float] = None,
        max_concurrent_requests: Optional[int] = None,
        timeout_seconds: Optional[int] = None
    ):
        """Initialize strategy with rate limiting configuration."""
        if max_requests_per_second is not None:
            self.max_requests_per_second = max_requests_per_second
        if max_concurrent_requests is not None:
            self.max_concurrent_requests = max_concurrent_requests
        if timeout_seconds is not None:
            self.timeout_seconds = timeout_seconds
        
        logger.info(
            f"Initialized {self.name}: "
            f"rate={self.max_requests_per_second}/s, "
            f"concurrency={self.max_concurrent_requests}"
        )
    
    @abstractmethod
    def is_applicable(self, context: LocationContext) -> tuple[bool, str]:
        """
        Check if this strategy can be used for the given context.
        
        Args:
            context: Location context with metadata
            
        Returns:
            Tuple of (is_applicable, reasoning)
        """
        pass
    
    @abstractmethod
    async def execute(self, context: LocationContext) -> TrafficStrategyResult:
        """
        Execute the collection strategy.
        
        Args:
            context: Location context with metadata
            
        Returns:
            TrafficStrategyResult with found data
        """
        pass
    
    def calculate_priority(self, context: LocationContext) -> int:
        """
        Calculate priority score for this strategy (0-10, higher = higher priority).
        
        Override in subclasses for strategy-specific prioritization.
        """
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0
        
        # Base priority on confidence level
        priority_map = {"high": 8, "medium": 5, "low": 3}
        return priority_map.get(self.default_confidence, 5)
    
    def _create_result(
        self,
        success: bool,
        locations: Optional[List[Dict[str, Any]]] = None,
        observations: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        reasoning: str = "",
        requests_made: int = 0
    ) -> TrafficStrategyResult:
        """Helper to create a standardized TrafficStrategyResult."""
        return TrafficStrategyResult(
            strategy_name=self.name,
            success=success,
            locations_found=locations or [],
            observations_found=observations or [],
            metadata_enriched=metadata or {},
            source_type=self.source_type,
            confidence_level=self.default_confidence,
            requests_made=requests_made,
            cost_estimate_usd=requests_made * self.cost_per_request_usd,
            error_message=error_message,
            reasoning=reasoning,
            completed_at=datetime.utcnow()
        )
