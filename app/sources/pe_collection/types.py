"""
Type definitions for the PE collection system.

Defines data classes for:
- Collection configuration
- Collection results and items
- Source type enumeration
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any


class PECollectionSource(str, Enum):
    """Available data collection sources for PE intelligence."""

    # Firm Data
    SEC_ADV = "sec_adv"  # Form ADV for registered investment advisers
    FIRM_WEBSITE = "firm_website"  # PE firm website scraping
    LINKEDIN_FIRM = "linkedin_firm"  # Firm LinkedIn page

    # Portfolio Data
    SEC_13D = "sec_13d"  # Large ownership filings
    WEBSITE_PORTFOLIO = "website_portfolio"  # Portfolio page on firm websites
    CRUNCHBASE = "crunchbase"  # Crunchbase API

    # People Data
    LINKEDIN_PEOPLE = "linkedin_people"  # Individual LinkedIn profiles
    BIO_EXTRACTOR = "bio_extractor"  # LLM-powered bio parsing

    # Deal Data
    SEC_FORM_D = "sec_form_d"  # Reg D private placement filings
    PRESS_RELEASE = "press_release"  # Deal announcements

    # Financial Data
    PUBLIC_COMPS = "public_comps"  # Yahoo Finance/FMP for public comparables
    VALUATION_ESTIMATOR = "valuation_estimator"  # LLM-powered valuation

    # News
    NEWS_API = "news_api"  # NewsAPI, GDELT, etc.


class CollectionMode(str, Enum):
    """Collection mode options."""

    INCREMENTAL = "incremental"  # Only collect if stale
    FULL = "full"  # Force full re-collection


class EntityType(str, Enum):
    """Entity types that can be collected."""

    FIRM = "firm"
    FUND = "fund"
    COMPANY = "company"
    PERSON = "person"
    DEAL = "deal"


@dataclass
class PECollectionConfig:
    """
    Configuration for a PE collection job.

    Attributes:
        entity_type: Type of entity to collect (firm, company, person, deal)
        sources: Which sources to collect from
        mode: Collection mode (incremental or full)
        max_age_days: Re-collect data older than this (for incremental)
        max_concurrent: Maximum concurrent collections
        rate_limit_delay: Delay between requests in seconds
        max_retries: Maximum retry attempts
    """

    entity_type: EntityType = EntityType.FIRM
    sources: List[PECollectionSource] = field(
        default_factory=lambda: [PECollectionSource.FIRM_WEBSITE]
    )
    mode: CollectionMode = CollectionMode.INCREMENTAL
    max_age_days: int = 30
    max_concurrent: int = 5
    rate_limit_delay: float = 2.0
    max_retries: int = 3

    # Optional filters
    firm_id: Optional[int] = None
    firm_ids: Optional[List[int]] = None
    company_id: Optional[int] = None
    company_ids: Optional[List[int]] = None
    person_id: Optional[int] = None
    person_ids: Optional[List[int]] = None

    # Strategy filters
    firm_types: Optional[List[str]] = None  # PE, VC, Growth, etc.
    sectors: Optional[List[str]] = None  # Technology, Healthcare, etc.

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "entity_type": self.entity_type.value,
            "sources": [s.value for s in self.sources],
            "mode": self.mode.value,
            "max_age_days": self.max_age_days,
            "max_concurrent": self.max_concurrent,
            "rate_limit_delay": self.rate_limit_delay,
            "max_retries": self.max_retries,
            "firm_id": self.firm_id,
            "firm_ids": self.firm_ids,
            "company_id": self.company_id,
            "company_ids": self.company_ids,
            "person_id": self.person_id,
            "person_ids": self.person_ids,
            "firm_types": self.firm_types,
            "sectors": self.sectors,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PECollectionConfig":
        """Create from dictionary."""
        sources = [PECollectionSource(s) for s in data.get("sources", ["firm_website"])]
        mode = CollectionMode(data.get("mode", "incremental"))
        entity_type = EntityType(data.get("entity_type", "firm"))
        return cls(
            entity_type=entity_type,
            sources=sources,
            mode=mode,
            max_age_days=data.get("max_age_days", 30),
            max_concurrent=data.get("max_concurrent", 5),
            rate_limit_delay=data.get("rate_limit_delay", 2.0),
            max_retries=data.get("max_retries", 3),
            firm_id=data.get("firm_id"),
            firm_ids=data.get("firm_ids"),
            company_id=data.get("company_id"),
            company_ids=data.get("company_ids"),
            person_id=data.get("person_id"),
            person_ids=data.get("person_ids"),
            firm_types=data.get("firm_types"),
            sectors=data.get("sectors"),
        )


@dataclass
class PECollectedItem:
    """
    A single collected data item.

    Attributes:
        item_type: Type of item (firm, fund, company, person, deal, news, etc.)
        entity_type: The broader entity category
        data: The collected data
        source_url: Where the data was found
        confidence: Confidence level (high, medium, low)
        is_new: Whether this is a new item (not update)
    """

    item_type: str  # firm, fund, company, person, deal, valuation, news, etc.
    entity_type: EntityType
    data: Dict[str, Any]
    source_url: Optional[str] = None
    confidence: str = "medium"
    is_new: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "item_type": self.item_type,
            "entity_type": self.entity_type.value,
            "data": self.data,
            "source_url": self.source_url,
            "confidence": self.confidence,
            "is_new": self.is_new,
        }


@dataclass
class PECollectionResult:
    """
    Result of a collection operation.

    Attributes:
        entity_id: ID of the entity being collected
        entity_name: Name of the entity
        entity_type: Type of entity (firm, company, person, deal)
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

    entity_id: int
    entity_name: str
    entity_type: EntityType
    source: PECollectionSource
    success: bool = False
    items: List[PECollectedItem] = field(default_factory=list)
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
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "entity_type": self.entity_type.value,
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
class PEJobProgress:
    """
    Progress tracking for a collection job.
    """

    job_id: int
    total_entities: int = 0
    completed_entities: int = 0
    successful_entities: int = 0
    failed_entities: int = 0
    current_entity: Optional[str] = None
    current_source: Optional[str] = None

    @property
    def progress_pct(self) -> float:
        """Progress as percentage."""
        if self.total_entities == 0:
            return 0.0
        return (self.completed_entities / self.total_entities) * 100

    @property
    def is_complete(self) -> bool:
        """Whether job is complete."""
        return self.completed_entities >= self.total_entities


@dataclass
class PEFirmSeed:
    """
    Seed data entry for a PE firm.
    """

    name: str
    website: str
    cik: Optional[str] = None
    firm_type: str = "PE"
    strategy: Optional[str] = None
    aum_billions: Optional[float] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PEFirmSeed":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            website=data["website"],
            cik=data.get("cik"),
            firm_type=data.get("firm_type", "PE"),
            strategy=data.get("strategy"),
            aum_billions=data.get("aum_billions"),
        )
