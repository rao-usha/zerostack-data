"""
Type definitions for the Family Office collection system.

Defines data classes for:
- Collection configuration
- Collection results and items
- Source type enumeration
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any


class FoCollectionSource(str, Enum):
    """Available data collection sources for family offices."""
    WEBSITE = "website"
    SEC_13F = "sec_13f"
    SEC_ADV = "sec_adv"
    NEWS = "news"
    CRUNCHBASE = "crunchbase"
    DEALS = "deals"


class FoCollectionMode(str, Enum):
    """Collection mode options."""
    INCREMENTAL = "incremental"  # Only collect if stale
    FULL = "full"  # Force full re-collection


@dataclass
class FoCollectionConfig:
    """
    Configuration for a family office collection job.

    Attributes:
        fo_types: Filter by FO types (single_family, multi_family)
        regions: Filter by regions (us, europe, asia, middle_east)
        sources: Which sources to collect from
        mode: Collection mode (incremental or full)
        max_age_days: Re-collect data older than this (for incremental)
        max_concurrent_fos: Maximum concurrent FO collections
        rate_limit_delay: Delay between requests in seconds
        max_retries: Maximum retry attempts per FO
    """
    fo_types: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    sources: List[FoCollectionSource] = field(
        default_factory=lambda: [FoCollectionSource.WEBSITE]
    )
    mode: FoCollectionMode = FoCollectionMode.INCREMENTAL
    max_age_days: int = 90
    max_concurrent_fos: int = 5
    rate_limit_delay: float = 2.0
    max_retries: int = 3

    # Optional FO ID filter
    fo_id: Optional[int] = None
    fo_ids: Optional[List[int]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "fo_types": self.fo_types,
            "regions": self.regions,
            "sources": [s.value for s in self.sources],
            "mode": self.mode.value,
            "max_age_days": self.max_age_days,
            "max_concurrent_fos": self.max_concurrent_fos,
            "rate_limit_delay": self.rate_limit_delay,
            "max_retries": self.max_retries,
            "fo_id": self.fo_id,
            "fo_ids": self.fo_ids,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FoCollectionConfig":
        """Create from dictionary."""
        sources = [FoCollectionSource(s) for s in data.get("sources", ["website"])]
        mode = FoCollectionMode(data.get("mode", "incremental"))
        return cls(
            fo_types=data.get("fo_types"),
            regions=data.get("regions"),
            sources=sources,
            mode=mode,
            max_age_days=data.get("max_age_days", 90),
            max_concurrent_fos=data.get("max_concurrent_fos", 5),
            rate_limit_delay=data.get("rate_limit_delay", 2.0),
            max_retries=data.get("max_retries", 3),
            fo_id=data.get("fo_id"),
            fo_ids=data.get("fo_ids"),
        )


@dataclass
class FoCollectedItem:
    """
    A single collected data item for family offices.

    Attributes:
        item_type: Type of item (contact, investment, deal, etc.)
        data: The collected data
        source_url: Where the data was found
        confidence: Confidence level (high, medium, low)
        is_new: Whether this is a new item (not update)
    """
    item_type: str  # contact, investment, deal, portfolio_company, etc.
    data: Dict[str, Any]
    source_url: Optional[str] = None
    confidence: str = "medium"
    is_new: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "item_type": self.item_type,
            "data": self.data,
            "source_url": self.source_url,
            "confidence": self.confidence,
            "is_new": self.is_new,
        }


@dataclass
class FoCollectionResult:
    """
    Result of a collection operation for a single FO/source combination.

    Attributes:
        fo_id: Family office ID
        fo_name: Family office name
        source: Collection source used
        success: Whether collection succeeded
        items: List of collected items
        error_message: Error message if failed
        warnings: List of non-fatal warnings
        requests_made: Number of HTTP requests made
        bytes_downloaded: Total bytes downloaded
        started_at: When collection started
        completed_at: When collection completed
    """
    fo_id: int
    fo_name: str
    source: FoCollectionSource
    success: bool = False
    items: List[FoCollectedItem] = field(default_factory=list)
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    requests_made: int = 0
    bytes_downloaded: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def items_found(self) -> int:
        """Total items found."""
        return len(self.items)

    @property
    def items_new(self) -> int:
        """Count of new items."""
        return sum(1 for item in self.items if item.is_new)

    @property
    def items_updated(self) -> int:
        """Count of updated items."""
        return sum(1 for item in self.items if not item.is_new)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Duration of collection in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/serialization."""
        return {
            "fo_id": self.fo_id,
            "fo_name": self.fo_name,
            "source": self.source.value,
            "success": self.success,
            "items_found": self.items_found,
            "items_new": self.items_new,
            "items_updated": self.items_updated,
            "error_message": self.error_message,
            "warnings": self.warnings,
            "requests_made": self.requests_made,
            "bytes_downloaded": self.bytes_downloaded,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class FoJobProgress:
    """
    Progress tracking for a collection job.
    """
    job_id: int
    total_fos: int = 0
    completed_fos: int = 0
    successful_fos: int = 0
    failed_fos: int = 0
    current_fo: Optional[str] = None

    @property
    def progress_pct(self) -> float:
        """Progress as percentage."""
        if self.total_fos == 0:
            return 0.0
        return (self.completed_fos / self.total_fos) * 100

    @property
    def is_complete(self) -> bool:
        """Whether job is complete."""
        return self.completed_fos >= self.total_fos


@dataclass
class FoRegistryEntry:
    """
    Entry from the expanded family office registry.

    Mirrors the structure in expanded_family_office_registry.json.
    """
    name: str
    fo_type: str  # single_family, multi_family
    principal_family: Optional[str]
    principal_name: Optional[str]
    estimated_aum_billions: Optional[float]
    region: str
    country_code: str
    city: Optional[str]
    state_province: Optional[str]
    website_url: Optional[str]
    investment_focus: Optional[List[str]]
    sectors_of_interest: Optional[List[str]]
    geographic_focus: Optional[List[str]]
    check_size_range: Optional[str]
    collection_priority: int = 5

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FoRegistryEntry":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            fo_type=data["fo_type"],
            principal_family=data.get("principal_family"),
            principal_name=data.get("principal_name"),
            estimated_aum_billions=data.get("estimated_aum_billions"),
            region=data["region"],
            country_code=data["country_code"],
            city=data.get("city"),
            state_province=data.get("state_province"),
            website_url=data.get("website_url"),
            investment_focus=data.get("investment_focus"),
            sectors_of_interest=data.get("sectors_of_interest"),
            geographic_focus=data.get("geographic_focus"),
            check_size_range=data.get("check_size_range"),
            collection_priority=data.get("collection_priority", 5),
        )
