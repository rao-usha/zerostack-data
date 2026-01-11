"""
Pydantic models and types for the public_lp_strategies source.

These types are used for:
- Input validation when registering LPs and documents
- Structured representation of extraction results
- API request/response schemas specific to this source
"""
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator


# =============================================================================
# INPUT MODELS FOR REGISTRATION
# =============================================================================


class LpFundInput(BaseModel):
    """Input model for registering an LP fund."""
    
    name: str = Field(..., min_length=1, max_length=255, description="Short LP name (e.g., 'CalPERS')")
    formal_name: Optional[str] = Field(None, description="Full formal name")
    lp_type: str = Field(..., description="LP type: 'public_pension', 'sovereign_wealth', 'endowment'")
    jurisdiction: Optional[str] = Field(None, max_length=100, description="Jurisdiction (e.g., 'CA', 'NY')")
    website_url: Optional[str] = Field(None, description="Official website URL")
    
    @field_validator("lp_type")
    @classmethod
    def validate_lp_type(cls, v: str) -> str:
        """Validate LP type."""
        from app.sources.public_lp_strategies.config import VALID_LP_TYPES
        if v not in VALID_LP_TYPES:
            raise ValueError(f"Invalid lp_type. Must be one of: {VALID_LP_TYPES}")
        return v


class LpDocumentInput(BaseModel):
    """Input model for registering an LP document."""
    
    lp_id: int = Field(..., description="Foreign key to lp_fund.id")
    title: str = Field(..., min_length=1, description="Document title")
    document_type: str = Field(..., description="Document type (e.g., 'investment_committee_presentation')")
    program: str = Field(..., description="Program/portfolio (e.g., 'private_equity', 'total_fund')")
    
    report_period_start: Optional[date] = Field(None, description="Report period start date")
    report_period_end: Optional[date] = Field(None, description="Report period end date")
    fiscal_year: Optional[int] = Field(None, ge=1900, le=2100, description="Fiscal year")
    fiscal_quarter: Optional[str] = Field(None, description="Fiscal quarter: 'Q1', 'Q2', 'Q3', 'Q4'")
    
    source_url: str = Field(..., description="Source URL of the document")
    file_format: str = Field(..., description="File format: 'pdf', 'pptx', 'html', etc.")
    raw_file_location: Optional[str] = Field(None, description="S3 path or blob identifier")
    
    @field_validator("document_type")
    @classmethod
    def validate_document_type(cls, v: str) -> str:
        """Validate document type."""
        from app.sources.public_lp_strategies.config import VALID_DOCUMENT_TYPES
        if v not in VALID_DOCUMENT_TYPES:
            raise ValueError(f"Invalid document_type. Must be one of: {VALID_DOCUMENT_TYPES}")
        return v
    
    @field_validator("program")
    @classmethod
    def validate_program(cls, v: str) -> str:
        """Validate program."""
        from app.sources.public_lp_strategies.config import VALID_PROGRAMS
        if v not in VALID_PROGRAMS:
            raise ValueError(f"Invalid program. Must be one of: {VALID_PROGRAMS}")
        return v
    
    @field_validator("fiscal_quarter")
    @classmethod
    def validate_fiscal_quarter(cls, v: Optional[str]) -> Optional[str]:
        """Validate fiscal quarter."""
        if v is None:
            return v
        from app.sources.public_lp_strategies.config import VALID_FISCAL_QUARTERS
        if v not in VALID_FISCAL_QUARTERS:
            raise ValueError(f"Invalid fiscal_quarter. Must be one of: {VALID_FISCAL_QUARTERS}")
        return v
    
    @field_validator("file_format")
    @classmethod
    def validate_file_format(cls, v: str) -> str:
        """Validate file format."""
        from app.sources.public_lp_strategies.config import VALID_FILE_FORMATS
        if v not in VALID_FILE_FORMATS:
            raise ValueError(f"Invalid file_format. Must be one of: {VALID_FILE_FORMATS}")
        return v


class DocumentTextSectionInput(BaseModel):
    """Input model for a document text section."""
    
    section_name: Optional[str] = Field(None, description="Section name")
    page_start: Optional[int] = Field(None, ge=1, description="Starting page number")
    page_end: Optional[int] = Field(None, ge=1, description="Ending page number")
    sequence_order: int = Field(..., ge=0, description="Sequence order for sorting")
    text: str = Field(..., min_length=1, description="Extracted text content")
    embedding_vector: Optional[List[float]] = Field(None, description="Optional embedding vector")
    language: Optional[str] = Field("en", max_length=10, description="Language code")


# =============================================================================
# EXTRACTION RESULT MODELS
# =============================================================================


class AssetClassAllocationInput(BaseModel):
    """Input model for asset class allocation data."""
    
    asset_class: str = Field(..., description="Asset class name")
    target_weight_pct: Optional[float] = Field(None, description="Target allocation percentage")
    min_weight_pct: Optional[float] = Field(None, description="Minimum allocation percentage")
    max_weight_pct: Optional[float] = Field(None, description="Maximum allocation percentage")
    current_weight_pct: Optional[float] = Field(None, description="Current allocation percentage")
    benchmark_weight_pct: Optional[float] = Field(None, description="Benchmark allocation percentage")
    source_section_id: Optional[int] = Field(None, description="FK to source text section")
    
    @field_validator("asset_class")
    @classmethod
    def validate_asset_class(cls, v: str) -> str:
        """Validate asset class."""
        from app.sources.public_lp_strategies.config import VALID_ASSET_CLASSES
        if v not in VALID_ASSET_CLASSES:
            raise ValueError(f"Invalid asset_class. Must be one of: {VALID_ASSET_CLASSES}")
        return v


class AssetClassProjectionInput(BaseModel):
    """Input model for asset class projection data."""
    
    asset_class: str = Field(..., description="Asset class name")
    projection_horizon: str = Field(..., description="Projection horizon (e.g., '3_year')")
    net_flow_projection_amount: Optional[float] = Field(None, description="Net flow projection (currency)")
    commitment_plan_amount: Optional[float] = Field(None, description="Commitment plan amount")
    expected_return_pct: Optional[float] = Field(None, description="Expected return percentage")
    expected_volatility_pct: Optional[float] = Field(None, description="Expected volatility percentage")
    source_section_id: Optional[int] = Field(None, description="FK to source text section")
    
    @field_validator("asset_class")
    @classmethod
    def validate_asset_class(cls, v: str) -> str:
        """Validate asset class."""
        from app.sources.public_lp_strategies.config import VALID_ASSET_CLASSES
        if v not in VALID_ASSET_CLASSES:
            raise ValueError(f"Invalid asset_class. Must be one of: {VALID_ASSET_CLASSES}")
        return v
    
    @field_validator("projection_horizon")
    @classmethod
    def validate_horizon(cls, v: str) -> str:
        """Validate projection horizon."""
        from app.sources.public_lp_strategies.config import VALID_HORIZONS
        if v not in VALID_HORIZONS:
            raise ValueError(f"Invalid projection_horizon. Must be one of: {VALID_HORIZONS}")
        return v


class ThematicTagInput(BaseModel):
    """Input model for thematic tags."""
    
    theme: str = Field(..., description="Theme name")
    relevance_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Relevance score (0.0-1.0)")
    source_section_id: Optional[int] = Field(None, description="FK to source text section")
    
    @field_validator("theme")
    @classmethod
    def validate_theme(cls, v: str) -> str:
        """Validate theme (warn if not in known list, but allow custom themes)."""
        from app.sources.public_lp_strategies.config import VALID_THEMES
        if v not in VALID_THEMES:
            # Allow custom themes, just warn
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Using custom theme '{v}' not in standard list: {VALID_THEMES}")
        return v


class StrategySnapshotInput(BaseModel):
    """Input model for strategy snapshot data."""
    
    lp_id: int = Field(..., description="FK to lp_fund.id")
    program: str = Field(..., description="Program name")
    fiscal_year: int = Field(..., ge=1900, le=2100, description="Fiscal year")
    fiscal_quarter: str = Field(..., description="Fiscal quarter")
    strategy_date: Optional[date] = Field(None, description="Strategy date (board/IC date)")
    primary_document_id: Optional[int] = Field(None, description="FK to primary document")
    
    summary_text: Optional[str] = Field(None, description="High-level strategy summary")
    risk_positioning: Optional[str] = Field(None, description="Risk positioning")
    liquidity_profile: Optional[str] = Field(None, description="Liquidity profile")
    tilt_description: Optional[str] = Field(None, description="Tilt description")
    
    @field_validator("program")
    @classmethod
    def validate_program(cls, v: str) -> str:
        """Validate program."""
        from app.sources.public_lp_strategies.config import VALID_PROGRAMS
        if v not in VALID_PROGRAMS:
            raise ValueError(f"Invalid program. Must be one of: {VALID_PROGRAMS}")
        return v
    
    @field_validator("fiscal_quarter")
    @classmethod
    def validate_fiscal_quarter(cls, v: str) -> str:
        """Validate fiscal quarter."""
        from app.sources.public_lp_strategies.config import VALID_FISCAL_QUARTERS
        if v not in VALID_FISCAL_QUARTERS:
            raise ValueError(f"Invalid fiscal_quarter. Must be one of: {VALID_FISCAL_QUARTERS}")
        return v


class StrategyExtractionResult(BaseModel):
    """
    Complete extraction result for an LP strategy.
    
    This model encapsulates all the structured data extracted from
    an LP document, ready for ingestion into the database.
    """
    
    strategy: StrategySnapshotInput = Field(..., description="Strategy snapshot data")
    allocations: List[AssetClassAllocationInput] = Field(default_factory=list, description="Asset class allocations")
    projections: List[AssetClassProjectionInput] = Field(default_factory=list, description="Asset class projections")
    thematic_tags: List[ThematicTagInput] = Field(default_factory=list, description="Thematic tags")
    
    # Optional: manager/vehicle exposures (not always available)
    manager_exposures: List[Dict[str, Any]] = Field(default_factory=list, description="Manager/vehicle exposures")


# =============================================================================
# RESPONSE MODELS
# =============================================================================


class LpFundResponse(BaseModel):
    """Response model for LP fund data."""
    
    id: int
    name: str
    formal_name: Optional[str]
    lp_type: str
    jurisdiction: Optional[str]
    website_url: Optional[str]
    created_at: datetime
    
    model_config = {"from_attributes": True}


class LpDocumentResponse(BaseModel):
    """Response model for LP document data."""
    
    id: int
    lp_id: int
    title: str
    document_type: str
    program: str
    report_period_start: Optional[datetime]
    report_period_end: Optional[datetime]
    fiscal_year: Optional[int]
    fiscal_quarter: Optional[str]
    source_url: str
    file_format: str
    raw_file_location: Optional[str]
    ingested_at: datetime
    created_at: datetime
    
    model_config = {"from_attributes": True}


class LpStrategySnapshotResponse(BaseModel):
    """Response model for LP strategy snapshot data."""
    
    id: int
    lp_id: int
    program: str
    fiscal_year: int
    fiscal_quarter: str
    strategy_date: Optional[datetime]
    primary_document_id: Optional[int]
    summary_text: Optional[str]
    risk_positioning: Optional[str]
    liquidity_profile: Optional[str]
    tilt_description: Optional[str]
    created_at: datetime
    
    model_config = {"from_attributes": True}


# =============================================================================
# LP KEY CONTACT MODELS
# =============================================================================


class LpKeyContactInput(BaseModel):
    """
    Input model for registering a key contact at an LP fund.
    
    Only public information from official sources:
    - SEC filings (Form ADV)
    - Official LP websites (contact pages, team pages)
    - Annual reports and disclosure documents
    - Manual research with verification
    """
    
    lp_id: int = Field(..., description="Foreign key to lp_fund.id")
    full_name: str = Field(..., min_length=2, max_length=255, description="Full name of contact")
    title: Optional[str] = Field(None, description="Job title (e.g., 'Chief Investment Officer')")
    role_category: Optional[str] = Field(None, description="Standardized role: CIO, CFO, CEO, Investment Director, etc.")
    
    # Contact information (only if publicly disclosed)
    email: Optional[str] = Field(None, description="Professional email (only if publicly listed)")
    phone: Optional[str] = Field(None, description="Phone number (only if publicly listed)")
    linkedin_url: Optional[str] = Field(None, description="LinkedIn URL for manual reference (no scraping)")
    
    # Data provenance
    source_document_id: Optional[int] = Field(None, description="FK to lp_document.id if from document")
    source_type: Optional[str] = Field(None, description="Source: sec_adv, website, disclosure_doc, annual_report, manual")
    source_url: Optional[str] = Field(None, description="URL where information was found")
    confidence_level: Optional[str] = Field("medium", description="Quality indicator: high, medium, low")
    is_verified: int = Field(0, ge=0, le=1, description="1 if manually verified, 0 otherwise")
    
    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format if provided."""
        if v is None:
            return v
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError(f"Invalid email format: {v}")
        
        # Reject generic emails unless they're institutional
        generic_prefixes = ['info', 'contact', 'admin', 'webmaster', 'sales', 'support']
        email_prefix = v.split('@')[0].lower()
        if email_prefix in generic_prefixes:
            # Allow if it's a specific institutional address
            if not any(term in v.lower() for term in ['investment', 'investor', 'relations']):
                raise ValueError(f"Generic email not allowed: {v}")
        
        return v
    
    @field_validator("role_category")
    @classmethod
    def validate_role_category(cls, v: Optional[str]) -> Optional[str]:
        """Validate role category."""
        if v is None:
            return v
        valid_roles = ['CIO', 'CFO', 'CEO', 'Investment Director', 'Board Member', 
                      'Managing Director', 'IR Contact', 'Other']
        if v not in valid_roles:
            raise ValueError(f"Invalid role_category. Must be one of: {valid_roles}")
        return v
    
    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate source type."""
        if v is None:
            return v
        valid_sources = ['sec_adv', 'website', 'disclosure_doc', 'annual_report', 'manual']
        if v not in valid_sources:
            raise ValueError(f"Invalid source_type. Must be one of: {valid_sources}")
        return v
    
    @field_validator("confidence_level")
    @classmethod
    def validate_confidence_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate confidence level."""
        if v is None:
            return v
        valid_levels = ['high', 'medium', 'low']
        if v not in valid_levels:
            raise ValueError(f"Invalid confidence_level. Must be one of: {valid_levels}")
        return v


class LpKeyContactOutput(BaseModel):
    """Output model for LP key contact."""
    
    id: int
    lp_id: int
    full_name: str
    title: Optional[str]
    role_category: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    linkedin_url: Optional[str]
    source_document_id: Optional[int]
    source_type: Optional[str]
    source_url: Optional[str]
    confidence_level: Optional[str]
    is_verified: int
    collected_date: datetime
    created_at: datetime
    
    model_config = {"from_attributes": True}


class ContactExtractionResult(BaseModel):
    """Result of contact extraction from a source."""
    
    contacts_found: int
    contacts_inserted: int
    contacts_skipped: int  # Duplicates or validation failures
    errors: List[str] = []
    extraction_timestamp: datetime = Field(default_factory=datetime.utcnow)