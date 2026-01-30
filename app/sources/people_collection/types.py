"""
Pydantic models for people collection extraction results.

These types define the structure of data extracted from various sources
(websites, SEC filings, press releases) before being stored in the database.
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field, HttpUrl


class ExtractionConfidence(str, Enum):
    """Confidence level of extracted data."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TitleLevel(str, Enum):
    """Executive title hierarchy levels."""
    C_SUITE = "c_suite"
    PRESIDENT = "president"
    EVP = "evp"
    SVP = "svp"
    VP = "vp"
    DIRECTOR = "director"
    MANAGER = "manager"
    BOARD = "board"
    INDIVIDUAL = "individual"
    UNKNOWN = "unknown"


class ChangeType(str, Enum):
    """Types of leadership changes."""
    HIRE = "hire"
    DEPARTURE = "departure"
    PROMOTION = "promotion"
    DEMOTION = "demotion"
    LATERAL = "lateral"
    RETIREMENT = "retirement"
    BOARD_APPOINTMENT = "board_appointment"
    BOARD_DEPARTURE = "board_departure"
    INTERIM = "interim"
    DEATH = "death"


class Department(str, Enum):
    """Common corporate departments."""
    EXECUTIVE = "executive"
    FINANCE = "finance"
    OPERATIONS = "operations"
    SALES = "sales"
    MARKETING = "marketing"
    HR = "hr"
    IT = "it"
    LEGAL = "legal"
    ENGINEERING = "engineering"
    PRODUCT = "product"
    SUPPLY_CHAIN = "supply_chain"
    MANUFACTURING = "manufacturing"
    CUSTOMER_SERVICE = "customer_service"
    STRATEGY = "strategy"
    CORPORATE_DEVELOPMENT = "corporate_development"
    OTHER = "other"


class ExtractedPerson(BaseModel):
    """
    A person extracted from a source (website, SEC filing, etc.).

    This is the intermediate format before database insertion.
    """
    # Identity
    full_name: str = Field(..., description="Full name as displayed")
    first_name: Optional[str] = Field(None, description="First name if parseable")
    last_name: Optional[str] = Field(None, description="Last name if parseable")
    suffix: Optional[str] = Field(None, description="Suffix like Jr., III, PhD")

    # Role
    title: str = Field(..., description="Exact title as displayed on source")
    title_normalized: Optional[str] = Field(None, description="Standardized title (CEO, CFO, VP Sales)")
    title_level: TitleLevel = Field(TitleLevel.UNKNOWN, description="Hierarchy level")
    department: Optional[str] = Field(None, description="Department if identifiable")

    # Details
    bio: Optional[str] = Field(None, description="Biography text (1-3 sentences)")
    linkedin_url: Optional[str] = Field(None, description="LinkedIn profile URL")
    email: Optional[str] = Field(None, description="Email if visible")
    phone: Optional[str] = Field(None, description="Phone if visible")
    photo_url: Optional[str] = Field(None, description="Photo URL")

    # Hierarchy
    reports_to: Optional[str] = Field(None, description="Name of person they report to")
    is_board_member: bool = Field(False, description="Is on the board of directors")
    is_board_chair: bool = Field(False, description="Is chair of the board")
    is_executive: bool = Field(True, description="Is an executive (vs. board-only)")

    # Metadata
    confidence: ExtractionConfidence = Field(ExtractionConfidence.MEDIUM)
    source_url: Optional[str] = Field(None, description="URL where extracted from")
    extraction_notes: Optional[str] = Field(None, description="Any extraction issues or notes")

    class Config:
        use_enum_values = True


class ExtractedExperience(BaseModel):
    """Work experience extracted from a bio or profile."""
    company_name: str
    title: str
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    is_current: bool = False
    description: Optional[str] = None


class ExtractedEducation(BaseModel):
    """Education extracted from a bio or profile."""
    institution: str
    degree: Optional[str] = None
    field_of_study: Optional[str] = None
    graduation_year: Optional[int] = None


class ParsedBio(BaseModel):
    """Structured data parsed from an executive biography."""
    experience: List[ExtractedExperience] = Field(default_factory=list)
    education: List[ExtractedEducation] = Field(default_factory=list)
    board_positions: List[str] = Field(default_factory=list)
    certifications: List[str] = Field(default_factory=list)
    military_service: Optional[str] = None
    notable_achievements: List[str] = Field(default_factory=list)


class LeadershipChange(BaseModel):
    """
    A leadership change detected from news or SEC filings.
    """
    # Person
    person_name: str = Field(..., description="Full name of person involved")
    person_id: Optional[int] = Field(None, description="Database ID if matched")

    # Change details
    change_type: ChangeType = Field(..., description="Type of change")
    old_title: Optional[str] = Field(None, description="Previous title")
    new_title: Optional[str] = Field(None, description="New title")
    old_company: Optional[str] = Field(None, description="Previous company (for external hires)")

    # Dates
    announced_date: Optional[date] = Field(None, description="Date announced")
    effective_date: Optional[date] = Field(None, description="Date effective")

    # Context
    reason: Optional[str] = Field(None, description="Reason if stated")
    successor_name: Optional[str] = Field(None, description="Successor if mentioned")
    predecessor_name: Optional[str] = Field(None, description="Predecessor if mentioned")

    # Classification
    is_c_suite: bool = Field(False, description="Is C-suite level change")
    is_board: bool = Field(False, description="Is board-related change")
    significance_score: int = Field(5, ge=1, le=10, description="1-10 significance")

    # Source
    source_type: str = Field(..., description="press_release, 8k_filing, news, website_change")
    source_url: Optional[str] = Field(None)
    source_headline: Optional[str] = Field(None)

    # Metadata
    confidence: ExtractionConfidence = Field(ExtractionConfidence.MEDIUM)

    class Config:
        use_enum_values = True


class LeadershipPageResult(BaseModel):
    """Result of extracting leadership from a company page."""
    company_name: str
    page_url: str
    page_type: str = Field(..., description="leadership, team, about, board")

    people: List[ExtractedPerson] = Field(default_factory=list)

    extraction_confidence: ExtractionConfidence = Field(ExtractionConfidence.MEDIUM)
    extraction_notes: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


class SECFilingResult(BaseModel):
    """Result of parsing an SEC filing for executive data."""
    company_name: str
    cik: str
    filing_type: str  # DEF 14A, 10-K, 8-K
    filing_date: date
    filing_url: str

    # Extracted data
    executives: List[ExtractedPerson] = Field(default_factory=list)
    changes: List[LeadershipChange] = Field(default_factory=list)

    # For proxy statements
    compensation_data: Optional[dict] = None

    extraction_confidence: ExtractionConfidence = Field(ExtractionConfidence.MEDIUM)
    extracted_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


class CollectionResult(BaseModel):
    """
    Result of a collection job for a company.

    Returned by collection agents after processing.
    """
    company_id: int
    company_name: str
    source: str = Field(..., description="website, sec, news, linkedin")

    # Success metrics
    success: bool = True

    # Results
    people_found: int = 0
    people_created: int = 0
    people_updated: int = 0
    changes_detected: int = 0

    # Extracted data (before DB insertion)
    extracted_people: List[ExtractedPerson] = Field(default_factory=list)
    extracted_changes: List[LeadershipChange] = Field(default_factory=list)

    # Errors and warnings
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # Timing
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    class Config:
        use_enum_values = True


class BatchCollectionResult(BaseModel):
    """Result of collecting data for multiple companies."""
    total_companies: int
    successful: int
    failed: int

    results: List[CollectionResult] = Field(default_factory=list)

    total_people_found: int = 0
    total_people_created: int = 0
    total_changes_detected: int = 0

    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
