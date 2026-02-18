"""
Type definitions for the LP collection system.

Defines data classes for:
- Collection configuration
- Collection results and items
- Source type enumeration
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any


class LpCollectionSource(str, Enum):
    """Available data collection sources."""

    WEBSITE = "website"
    SEC_ADV = "sec_adv"
    SEC_13F = "sec_13f"
    FORM_990 = "form_990"
    CAFR = "cafr"
    NEWS = "news"
    GOVERNANCE = "governance"
    PERFORMANCE = "performance"


class CollectionMode(str, Enum):
    """Collection mode options."""

    INCREMENTAL = "incremental"  # Only collect if stale
    FULL = "full"  # Force full re-collection


@dataclass
class CollectionConfig:
    """
    Configuration for a collection job.

    Attributes:
        lp_types: Filter by LP types (public_pension, sovereign_wealth, etc.)
        regions: Filter by regions (us, europe, asia, middle_east, oceania)
        sources: Which sources to collect from
        mode: Collection mode (incremental or full)
        max_age_days: Re-collect data older than this (for incremental)
        max_concurrent_lps: Maximum concurrent LP collections
        rate_limit_delay: Delay between requests in seconds
        max_retries: Maximum retry attempts per LP
    """

    lp_types: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    sources: List[LpCollectionSource] = field(
        default_factory=lambda: [LpCollectionSource.WEBSITE]
    )
    mode: CollectionMode = CollectionMode.INCREMENTAL
    max_age_days: int = 90
    max_concurrent_lps: int = 5
    rate_limit_delay: float = 2.0
    max_retries: int = 3

    # Optional LP ID filter for single LP collection
    lp_id: Optional[int] = None
    lp_ids: Optional[List[int]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "lp_types": self.lp_types,
            "regions": self.regions,
            "sources": [s.value for s in self.sources],
            "mode": self.mode.value,
            "max_age_days": self.max_age_days,
            "max_concurrent_lps": self.max_concurrent_lps,
            "rate_limit_delay": self.rate_limit_delay,
            "max_retries": self.max_retries,
            "lp_id": self.lp_id,
            "lp_ids": self.lp_ids,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CollectionConfig":
        """Create from dictionary."""
        sources = [LpCollectionSource(s) for s in data.get("sources", ["website"])]
        mode = CollectionMode(data.get("mode", "incremental"))
        return cls(
            lp_types=data.get("lp_types"),
            regions=data.get("regions"),
            sources=sources,
            mode=mode,
            max_age_days=data.get("max_age_days", 90),
            max_concurrent_lps=data.get("max_concurrent_lps", 5),
            rate_limit_delay=data.get("rate_limit_delay", 2.0),
            max_retries=data.get("max_retries", 3),
            lp_id=data.get("lp_id"),
            lp_ids=data.get("lp_ids"),
        )


@dataclass
class CollectedItem:
    """
    A single collected data item.

    Attributes:
        item_type: Type of item (contact, document, allocation, etc.)
        data: The collected data
        source_url: Where the data was found
        confidence: Confidence level (high, medium, low)
        is_new: Whether this is a new item (not update)
    """

    item_type: str  # contact, document, allocation, projection, etc.
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
class CollectionResult:
    """
    Result of a collection operation for a single LP/source combination.

    Attributes:
        lp_id: LP fund ID
        lp_name: LP fund name
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

    lp_id: int
    lp_name: str
    source: LpCollectionSource
    success: bool = False
    items: List[CollectedItem] = field(default_factory=list)
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
            "lp_id": self.lp_id,
            "lp_name": self.lp_name,
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
class JobProgress:
    """
    Progress tracking for a collection job.

    Attributes:
        job_id: Job ID in database
        total_lps: Total LPs to process
        completed_lps: LPs completed so far
        successful_lps: LPs that succeeded
        failed_lps: LPs that failed
        current_lp: Currently processing LP name
    """

    job_id: int
    total_lps: int = 0
    completed_lps: int = 0
    successful_lps: int = 0
    failed_lps: int = 0
    current_lp: Optional[str] = None

    @property
    def progress_pct(self) -> float:
        """Progress as percentage."""
        if self.total_lps == 0:
            return 0.0
        return (self.completed_lps / self.total_lps) * 100

    @property
    def is_complete(self) -> bool:
        """Whether job is complete."""
        return self.completed_lps >= self.total_lps


@dataclass
class LpRegistryEntry:
    """
    Entry from the expanded LP registry.

    Mirrors the structure in expanded_lp_registry.json.
    """

    name: str
    formal_name: str
    lp_type: str
    jurisdiction: str
    region: str
    country_code: str
    website_url: str
    aum_usd_billions: Optional[str] = None
    has_cafr: bool = False
    collection_priority: int = 5
    sec_crd_number: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LpRegistryEntry":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            formal_name=data["formal_name"],
            lp_type=data["lp_type"],
            jurisdiction=data["jurisdiction"],
            region=data["region"],
            country_code=data["country_code"],
            website_url=data["website_url"],
            aum_usd_billions=data.get("aum_usd_billions"),
            has_cafr=data.get("has_cafr", False),
            collection_priority=data.get("collection_priority", 5),
            sec_crd_number=data.get("sec_crd_number"),
        )
