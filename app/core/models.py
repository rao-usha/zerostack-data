"""
SQLAlchemy models for core tables.

These tables are source-agnostic and used by all data source adapters.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Enum, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base
import enum

Base = declarative_base()


class JobStatus(str, enum.Enum):
    """Job status enumeration - ONLY these values allowed."""
    PENDING = "pending"
    BLOCKED = "blocked"  # Waiting for dependencies
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class ScheduleFrequency(str, enum.Enum):
    """Schedule frequency options."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"  # For cron expressions


class IngestionJob(Base):
    """
    Tracks all ingestion runs.

    MANDATORY: Every ingestion operation MUST create and update a job record.
    """
    __tablename__ = "ingestion_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False, index=True)
    status = Column(
        Enum(JobStatus, native_enum=False, length=20),
        nullable=False,
        default=JobStatus.PENDING,
        index=True
    )
    config = Column(JSON, nullable=False)  # Job configuration (survey, year, table, etc.)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Results
    rows_inserted = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    error_details = Column(JSON, nullable=True)  # Structured error info

    # Retry tracking
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    next_retry_at = Column(DateTime, nullable=True)  # When to retry (for scheduled retries)
    parent_job_id = Column(Integer, nullable=True, index=True)  # Link to original job if this is a retry

    def __repr__(self) -> str:
        return (
            f"<IngestionJob(id={self.id}, source={self.source}, "
            f"status={self.status}, retry_count={self.retry_count}, created_at={self.created_at})>"
        )

    @property
    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.status == JobStatus.FAILED and self.retry_count < self.max_retries


class IngestionSchedule(Base):
    """
    Stores scheduled ingestion configurations.

    Allows automated data refresh on configurable schedules.
    """
    __tablename__ = "ingestion_schedules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)  # Human-readable name
    source = Column(String(50), nullable=False, index=True)  # Data source (fred, census, etc.)
    config = Column(JSON, nullable=False)  # Source-specific configuration

    # Schedule configuration
    frequency = Column(
        Enum(ScheduleFrequency, native_enum=False, length=20),
        nullable=False,
        default=ScheduleFrequency.DAILY
    )
    cron_expression = Column(String(100), nullable=True)  # For custom schedules (e.g., "0 6 * * *")
    hour = Column(Integer, nullable=True, default=6)  # Hour to run (0-23) for non-cron schedules
    day_of_week = Column(Integer, nullable=True)  # Day of week (0=Monday) for weekly
    day_of_month = Column(Integer, nullable=True)  # Day of month (1-31) for monthly

    # State
    is_active = Column(Integer, nullable=False, default=1)  # 1=active, 0=paused
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    last_job_id = Column(Integer, nullable=True)  # Last created job ID

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Metadata
    description = Column(Text, nullable=True)
    priority = Column(Integer, nullable=False, default=5)  # 1=highest, 10=lowest

    def __repr__(self) -> str:
        return (
            f"<IngestionSchedule(id={self.id}, name={self.name}, "
            f"source={self.source}, frequency={self.frequency}, active={self.is_active})>"
        )


class DatasetRegistry(Base):
    """
    Metadata registry for all ingested datasets.
    
    Each unique dataset (source + identifier) gets one entry.
    """
    __tablename__ = "dataset_registry"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False, index=True)
    dataset_id = Column(String(255), nullable=False, index=True)  # e.g., "acs5_2023_b01001"
    table_name = Column(String(255), nullable=False, unique=True)  # Actual Postgres table name
    
    # Metadata
    display_name = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    source_metadata = Column(JSON, nullable=True)  # Source-specific metadata (renamed from 'metadata' to avoid SQLAlchemy conflict)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self) -> str:
        return (
            f"<DatasetRegistry(id={self.id}, source={self.source}, "
            f"dataset_id={self.dataset_id}, table_name={self.table_name})>"
        )


class GeoJSONBoundaries(Base):
    """
    Storage for GeoJSON boundary data.
    
    Stores geographic boundaries for Census geographies.
    """
    __tablename__ = "geojson_boundaries"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String(255), nullable=False, index=True)  # Links to dataset_registry
    geo_level = Column(String(50), nullable=False, index=True)  # state, county, tract, zip
    geo_id = Column(String(50), nullable=False, index=True)  # FIPS code or other identifier
    geo_name = Column(String(255), nullable=True)  # Human-readable name
    
    # GeoJSON data (Feature)
    geojson = Column(JSON, nullable=False)  # Complete GeoJSON Feature
    
    # Bounding box for quick spatial queries
    bbox_minx = Column(String(50), nullable=True)
    bbox_miny = Column(String(50), nullable=True)
    bbox_maxx = Column(String(50), nullable=True)
    bbox_maxy = Column(String(50), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self) -> str:
        return (
            f"<GeoJSONBoundaries(id={self.id}, dataset_id={self.dataset_id}, "
            f"geo_level={self.geo_level}, geo_id={self.geo_id})>"
        )


class CensusVariableMetadata(Base):
    """
    Storage for Census variable definitions/metadata.
    
    Maps Census column names to human-readable descriptions.
    Census-specific because other sources have different metadata structures.
    """
    __tablename__ = "census_variable_metadata"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String(255), nullable=False, index=True)  # e.g., "acs5_2021_b01001"
    variable_name = Column(String(100), nullable=False, index=True)  # e.g., "B01001_001E"
    column_name = Column(String(100), nullable=False)  # e.g., "b01001_001e"
    
    # Metadata from Census API
    label = Column(Text, nullable=False)  # e.g., "Estimate!!Total:"
    concept = Column(String(500), nullable=True)  # e.g., "SEX BY AGE"
    predicate_type = Column(String(50), nullable=True)  # e.g., "int", "float", "string"
    postgres_type = Column(String(50), nullable=True)  # e.g., "INTEGER", "NUMERIC", "TEXT"
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self) -> str:
        return (
            f"<CensusVariableMetadata(dataset_id={self.dataset_id}, "
            f"variable_name={self.variable_name}, label='{self.label}')>"
        )


# =============================================================================
# PUBLIC LP (LIMITED PARTNER) STRATEGY MODELS
# =============================================================================


class LpFund(Base):
    """
    Represents a public Limited Partner (LP) fund such as CalPERS, CalSTRS, etc.
    
    These are public pension funds, sovereign wealth funds, and endowments
    that publicly disclose their investment strategies.
    """
    __tablename__ = "lp_fund"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True, index=True)  # e.g., "CalPERS"
    formal_name = Column(Text, nullable=True)  # e.g., "California Public Employees' Retirement System"
    lp_type = Column(String(100), nullable=False, index=True)  # e.g., 'public_pension', 'sovereign_wealth', 'endowment'
    jurisdiction = Column(String(100), nullable=True)  # e.g., 'CA', 'NY', 'TX'
    website_url = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self) -> str:
        return f"<LpFund(id={self.id}, name='{self.name}', lp_type='{self.lp_type}')>"


class LpDocument(Base):
    """
    Represents a publicly available strategy document from an LP.
    
    Documents include investment committee presentations, quarterly reports,
    policy statements, and pacing plans.
    """
    __tablename__ = "lp_document"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lp_id = Column(Integer, nullable=False, index=True)  # FK to lp_fund.id
    
    title = Column(Text, nullable=False)
    document_type = Column(String(100), nullable=False, index=True)
    # document_type values: 'investment_committee_presentation', 'quarterly_investment_report',
    #                       'policy_statement', 'pacing_plan'
    
    program = Column(String(100), nullable=False, index=True)
    # program values: 'total_fund', 'private_equity', 'real_estate', 'infrastructure', 'fixed_income'
    
    report_period_start = Column(DateTime, nullable=True)
    report_period_end = Column(DateTime, nullable=True)
    fiscal_year = Column(Integer, nullable=True, index=True)  # e.g., 2025
    fiscal_quarter = Column(String(10), nullable=True, index=True)  # 'Q1', 'Q2', 'Q3', 'Q4'
    
    source_url = Column(Text, nullable=False)
    file_format = Column(String(50), nullable=False)  # 'pdf', 'pptx', 'html'
    raw_file_location = Column(Text, nullable=True)  # S3 path or blob identifier
    
    # Timestamps
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_document_lp_fiscal', 'lp_id', 'fiscal_year', 'fiscal_quarter'),
        Index('idx_lp_document_program_fiscal', 'program', 'fiscal_year', 'fiscal_quarter'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpDocument(id={self.id}, lp_id={self.lp_id}, title='{self.title[:50]}...', "
            f"program='{self.program}', fiscal_year={self.fiscal_year}, fiscal_quarter='{self.fiscal_quarter}')>"
        )


class LpDocumentTextSection(Base):
    """
    Stores parsed text chunks from LP documents.
    
    Sections are extracted during document processing and can be used for:
    - Full-text search
    - NLP/LLM analysis
    - Traceability (linking extractions back to source text)
    """
    __tablename__ = "lp_document_text_section"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(Integer, nullable=False, index=True)  # FK to lp_document.id
    
    section_name = Column(Text, nullable=True)  # e.g., 'Executive Summary', 'Private Equity Strategy'
    page_start = Column(Integer, nullable=True)
    page_end = Column(Integer, nullable=True)
    sequence_order = Column(Integer, nullable=False, index=True)  # For ordering sections
    
    text = Column(Text, nullable=False)
    embedding_vector = Column(JSON, nullable=True)  # Placeholder for vector embeddings (JSONB or specialized type)
    language = Column(String(10), nullable=True, default='en')
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_text_section_doc_order', 'document_id', 'sequence_order'),
    )
    
    def __repr__(self) -> str:
        text_preview = self.text[:50] if self.text else ""
        return (
            f"<LpDocumentTextSection(id={self.id}, document_id={self.document_id}, "
            f"section_name='{self.section_name}', text='{text_preview}...')>"
        )


class LpStrategySnapshot(Base):
    """
    Normalized LP strategy at a per-LP, per-program, per-quarter level.
    
    This is the core "silver/gold" layer that represents the structured
    investment strategy extracted from documents.
    
    One row per (LP, program, fiscal_year, fiscal_quarter).
    """
    __tablename__ = "lp_strategy_snapshot"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lp_id = Column(Integer, nullable=False, index=True)  # FK to lp_fund.id
    program = Column(String(100), nullable=False, index=True)  # Same values as lp_document.program
    
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_quarter = Column(String(10), nullable=False, index=True)  # 'Q1', 'Q2', 'Q3', 'Q4'
    strategy_date = Column(DateTime, nullable=True)  # Board/IC date; fallback to report date
    
    primary_document_id = Column(Integer, nullable=True, index=True)  # FK to lp_document.id
    
    # Strategy summary fields
    summary_text = Column(Text, nullable=True)  # High-level summary
    risk_positioning = Column(String(100), nullable=True)  # e.g., 'risk_on', 'defensive', 'neutral'
    liquidity_profile = Column(Text, nullable=True)  # Short description or categorical
    tilt_description = Column(Text, nullable=True)  # e.g., 'overweight private markets, underweight public equity'
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('lp_id', 'program', 'fiscal_year', 'fiscal_quarter', 
                        name='uq_lp_strategy_snapshot'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpStrategySnapshot(id={self.id}, lp_id={self.lp_id}, program='{self.program}', "
            f"FY{self.fiscal_year}-{self.fiscal_quarter})>"
        )


class LpAssetClassTargetAllocation(Base):
    """
    Target, range, and current allocation by asset class for an LP strategy.
    
    Captures the strategic asset allocation framework including:
    - Target weights
    - Min/max ranges
    - Current actual weights
    - Benchmark weights
    """
    __tablename__ = "lp_asset_class_target_allocation"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False, index=True)  # FK to lp_strategy_snapshot.id
    
    asset_class = Column(String(100), nullable=False, index=True)
    # asset_class values: 'public_equity', 'private_equity', 'real_estate', 'fixed_income',
    #                     'infrastructure', 'cash', 'hedge_funds', 'other'
    
    # Allocation percentages (stored as decimals, e.g., 25.5 for 25.5%)
    target_weight_pct = Column(String(50), nullable=True)  # Using String for NUMERIC compatibility
    min_weight_pct = Column(String(50), nullable=True)
    max_weight_pct = Column(String(50), nullable=True)
    current_weight_pct = Column(String(50), nullable=True)
    benchmark_weight_pct = Column(String(50), nullable=True)
    
    # Traceability
    source_section_id = Column(Integer, nullable=True, index=True)  # FK to lp_document_text_section.id
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_allocation_strategy_asset', 'strategy_id', 'asset_class'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpAssetClassTargetAllocation(id={self.id}, strategy_id={self.strategy_id}, "
            f"asset_class='{self.asset_class}', target={self.target_weight_pct}%)>"
        )


class LpAssetClassProjection(Base):
    """
    Forward-looking commitments, pacing plans, and projected flows by asset class.
    
    Captures LP plans for future investment activity including:
    - Commitment plans (e.g., PE/VC commitments over next 3 years)
    - Net flow projections
    - Expected returns and volatility
    """
    __tablename__ = "lp_asset_class_projection"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False, index=True)  # FK to lp_strategy_snapshot.id
    
    asset_class = Column(String(100), nullable=False, index=True)
    projection_horizon = Column(String(50), nullable=False, index=True)  # e.g., '1_year', '3_year', '5_year'
    
    # Financial projections (stored as strings for NUMERIC compatibility with SQLite)
    net_flow_projection_amount = Column(String(50), nullable=True)  # Currency amount (e.g., USD)
    commitment_plan_amount = Column(String(50), nullable=True)  # e.g., PE commitments over horizon
    expected_return_pct = Column(String(50), nullable=True)
    expected_volatility_pct = Column(String(50), nullable=True)
    
    # Traceability
    source_section_id = Column(Integer, nullable=True, index=True)  # FK to lp_document_text_section.id
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_projection_strategy_asset_horizon', 'strategy_id', 'asset_class', 'projection_horizon'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpAssetClassProjection(id={self.id}, strategy_id={self.strategy_id}, "
            f"asset_class='{self.asset_class}', horizon='{self.projection_horizon}')>"
        )


class LpManagerOrVehicleExposure(Base):
    """
    Manager or fund-level exposures disclosed by LPs.
    
    Optional table for capturing specific manager/vehicle allocations when
    documents disclose this level of detail (often only for largest positions).
    """
    __tablename__ = "lp_manager_or_vehicle_exposure"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False, index=True)  # FK to lp_strategy_snapshot.id
    
    manager_name = Column(Text, nullable=True)
    vehicle_name = Column(Text, nullable=True)
    vehicle_type = Column(String(100), nullable=True)  # e.g., 'separate_account', 'commingled', 'co_invest'
    asset_class = Column(String(100), nullable=True, index=True)
    
    # Position details (stored as strings for NUMERIC compatibility)
    market_value_amount = Column(String(50), nullable=True)
    weight_pct = Column(String(50), nullable=True)
    
    status = Column(String(100), nullable=True)  # e.g., 'active', 'redeeming', 'new_commitment'
    geo_region = Column(String(100), nullable=True)  # e.g., 'US', 'Europe', 'Global', 'EM'
    sector_focus = Column(String(255), nullable=True)
    
    # Traceability
    source_section_id = Column(Integer, nullable=True, index=True)  # FK to lp_document_text_section.id
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self) -> str:
        return (
            f"<LpManagerOrVehicleExposure(id={self.id}, strategy_id={self.strategy_id}, "
            f"manager='{self.manager_name}', vehicle='{self.vehicle_name}')>"
        )


class LpStrategyThematicTag(Base):
    """
    Thematic tags for LP strategies (AI, energy transition, climate, etc.).
    
    Enables tracking of investment themes and priorities across LPs and time.
    Tags can be manually assigned or extracted via NLP/LLM analysis.
    """
    __tablename__ = "lp_strategy_thematic_tag"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False, index=True)  # FK to lp_strategy_snapshot.id
    
    theme = Column(String(100), nullable=False, index=True)
    # theme values: 'ai', 'energy_transition', 'climate_resilience', 'reshoring', etc.
    
    relevance_score = Column(String(50), nullable=True)  # 0.0–1.0 scale (stored as string for compatibility)
    
    # Traceability
    source_section_id = Column(Integer, nullable=True, index=True)  # FK to lp_document_text_section.id
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_tag_strategy_theme', 'strategy_id', 'theme'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpStrategyThematicTag(id={self.id}, strategy_id={self.strategy_id}, "
            f"theme='{self.theme}', relevance={self.relevance_score})>"
        )


class LpKeyContact(Base):
    """
    Key contacts at LP funds - public-facing individuals that GPs would contact.
    
    IMPORTANT: Only public information from official LP websites, annual reports,
    investment committee minutes, SEC filings. No private/restricted data.
    
    Data Collection Rules:
    - ✅ Official LP websites, SEC filings, annual reports
    - ✅ Publicly disclosed executive names and professional contact info
    - ❌ NO LinkedIn scraping (violates ToS)
    - ❌ NO authentication bypass or paywalls
    - ❌ NO personal emails (only professional/institutional)
    
    Typical roles: CIO, CFO, CEO, Investment Directors, Board Members
    """
    __tablename__ = "lp_key_contact"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lp_id = Column(Integer, nullable=False, index=True)  # FK to lp_fund.id
    
    # Personal information (public only)
    full_name = Column(Text, nullable=False)
    title = Column(Text, nullable=True)  # e.g., "Chief Investment Officer"
    
    # Role categorization (standardized)
    role_category = Column(String(100), nullable=True, index=True)
    # role_category values: 'CIO', 'CFO', 'CEO', 'Investment Director', 
    #                       'Board Member', 'Managing Director', 'IR Contact', 'Other'
    
    # Contact information (public only - from official sources)
    email = Column(Text, nullable=True)  # Only if publicly listed
    phone = Column(Text, nullable=True)  # Only if publicly listed
    
    # Professional profile (optional)
    linkedin_url = Column(Text, nullable=True)  # For manual research, no scraping
    
    # Data provenance and quality
    source_document_id = Column(Integer, nullable=True, index=True)  # FK to lp_document.id if from document
    source_type = Column(String(100), nullable=True, index=True)
    # source_type values: 'sec_adv', 'website', 'disclosure_doc', 'annual_report', 'manual'
    
    source_url = Column(Text, nullable=True)  # Where this information was found
    
    confidence_level = Column(String(50), nullable=True)  # 'high', 'medium', 'low'
    is_verified = Column(Integer, default=0)  # 0 or 1 (boolean) - manually verified
    
    # Timestamps
    collected_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_contact_role_category', 'role_category'),
        Index('idx_lp_contact_source_type', 'source_type'),
        UniqueConstraint('lp_id', 'full_name', 'email', name='uq_lp_contact_person_email'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpKeyContact(id={self.id}, lp_id={self.lp_id}, "
            f"name='{self.full_name}', role='{self.role_category}')>"
        )


# =============================================================================
# AGENTIC PORTFOLIO RESEARCH MODELS
# =============================================================================


class PortfolioCompany(Base):
    """
    Investment holdings discovered by the agentic portfolio research system.
    
    Tracks portfolio companies, investments, and deal flow for LPs and FOs.
    Data is collected from multiple sources (SEC 13F, websites, annual reports,
    press releases) and synthesized/deduplicated.
    """
    __tablename__ = "portfolio_companies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(Integer, nullable=False, index=True)
    investor_type = Column(String(50), nullable=False, index=True)  # 'lp' or 'family_office'
    
    # Company Details
    company_name = Column(Text, nullable=False, index=True)
    company_website = Column(Text, nullable=True)
    company_industry = Column(String(255), nullable=True, index=True)
    company_stage = Column(String(100), nullable=True)  # seed, series_a, growth, public, etc.
    company_location = Column(String(255), nullable=True)
    company_ticker = Column(String(20), nullable=True, index=True)  # For public equities
    company_cusip = Column(String(20), nullable=True)  # CUSIP identifier
    
    # Investment Details
    investment_type = Column(String(100), nullable=True, index=True)  # equity, PE, VC, real_estate, etc.
    investment_date = Column(DateTime, nullable=True)
    investment_amount_usd = Column(String(50), nullable=True)  # Stored as string for NUMERIC compatibility
    shares_held = Column(String(50), nullable=True)  # Number of shares (for 13F)
    market_value_usd = Column(String(50), nullable=True)  # Market value (for 13F)
    ownership_percentage = Column(String(50), nullable=True)
    current_holding = Column(Integer, default=1)  # Boolean: 1 = current, 0 = exited
    exit_date = Column(DateTime, nullable=True)
    exit_type = Column(String(100), nullable=True)  # IPO, acquisition, secondary, etc.
    
    # Data Provenance
    source_type = Column(String(100), nullable=False, index=True)
    # source_type values: 'sec_13f', 'website', 'annual_report', 'news', 
    #                     'press_release', 'portfolio_company_website'
    source_url = Column(Text, nullable=True)
    source_urls = Column(JSON, nullable=True)  # Multiple sources for merged records
    confidence_level = Column(String(50), nullable=False, default='medium')  # 'high', 'medium', 'low'
    collected_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_verified_date = Column(DateTime, nullable=True)
    
    # Agent Metadata
    collection_method = Column(String(100), default='agentic_search')
    agent_reasoning = Column(Text, nullable=True)
    collection_job_id = Column(Integer, nullable=True, index=True)  # FK to agentic_collection_jobs
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_portfolio_investor', 'investor_id', 'investor_type'),
        Index('idx_portfolio_company_name', 'company_name'),
        Index('idx_portfolio_current', 'current_holding'),
        Index('idx_portfolio_source', 'source_type'),
        UniqueConstraint('investor_id', 'investor_type', 'company_name', 'investment_date', 
                        name='uq_portfolio_company'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<PortfolioCompany(id={self.id}, investor_id={self.investor_id}, "
            f"company='{self.company_name}', source='{self.source_type}')>"
        )


class CoInvestment(Base):
    """
    Tracks co-investor relationships discovered during portfolio research.
    
    When we find that two investors participated in the same deal,
    we record the relationship here for network analysis.
    """
    __tablename__ = "co_investments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_investor_id = Column(Integer, nullable=False, index=True)
    primary_investor_type = Column(String(50), nullable=False)  # 'lp' or 'family_office'
    co_investor_name = Column(Text, nullable=False, index=True)
    co_investor_type = Column(String(100), nullable=True)  # PE firm, VC, family office, etc.
    
    # Deal details
    deal_name = Column(Text, nullable=True)  # Company name or deal identifier
    deal_date = Column(DateTime, nullable=True)
    deal_size_usd = Column(String(50), nullable=True)
    
    # Relationship strength
    co_investment_count = Column(Integer, default=1)  # Number of times they've co-invested
    
    # Data provenance
    source_type = Column(String(100), nullable=True)
    source_url = Column(Text, nullable=True)
    collected_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_coinvest_primary', 'primary_investor_id', 'primary_investor_type'),
        Index('idx_coinvest_co_investor', 'co_investor_name'),
        UniqueConstraint('primary_investor_id', 'primary_investor_type', 'co_investor_name', 'deal_name',
                        name='uq_co_investment'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<CoInvestment(id={self.id}, primary={self.primary_investor_id}, "
            f"co_investor='{self.co_investor_name}', deal='{self.deal_name}')>"
        )


class InvestorTheme(Base):
    """
    Investment themes and patterns identified from portfolio analysis.
    
    Classifies investors by their investment preferences based on
    observed portfolio patterns (sectors, geography, stage, asset class).
    """
    __tablename__ = "investor_themes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(Integer, nullable=False, index=True)
    investor_type = Column(String(50), nullable=False)  # 'lp' or 'family_office'
    
    # Theme classification
    theme_category = Column(String(100), nullable=False, index=True)
    # theme_category values: 'sector', 'geography', 'stage', 'asset_class'
    theme_value = Column(String(255), nullable=False, index=True)
    # e.g., for sector: 'climate_tech', 'healthcare', 'fintech'
    # e.g., for geography: 'us', 'europe', 'asia', 'emerging_markets'
    # e.g., for stage: 'seed', 'growth', 'buyout', 'public_equity'
    
    # Quantification
    investment_count = Column(Integer, nullable=True)  # Number of investments in this theme
    percentage_of_portfolio = Column(String(50), nullable=True)  # % of portfolio
    
    # Confidence
    confidence_level = Column(String(50), nullable=True)  # 'high', 'medium', 'low'
    evidence_sources = Column(JSON, nullable=True)  # List of source types supporting this
    
    # Timestamps
    collected_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_theme_investor', 'investor_id', 'investor_type'),
        Index('idx_theme_category_value', 'theme_category', 'theme_value'),
        UniqueConstraint('investor_id', 'investor_type', 'theme_category', 'theme_value',
                        name='uq_investor_theme'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<InvestorTheme(id={self.id}, investor_id={self.investor_id}, "
            f"category='{self.theme_category}', value='{self.theme_value}')>"
        )


class AgenticCollectionJob(Base):
    """
    Tracks agentic portfolio collection jobs with full reasoning trail.
    
    Unlike regular ingestion_jobs, this tracks:
    - Which strategies were attempted
    - Agent's reasoning for strategy selection
    - Results per strategy
    - Resource usage (API calls, tokens, cost)
    """
    __tablename__ = "agentic_collection_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(100), nullable=False, index=True)
    # job_type values: 'portfolio_discovery', 'deal_flow_update', 'co_investor_mapping', 'theme_analysis'
    
    # Target investor
    target_investor_id = Column(Integer, nullable=True, index=True)
    target_investor_type = Column(String(50), nullable=True)  # 'lp' or 'family_office'
    target_investor_name = Column(Text, nullable=True)
    
    # Job status
    status = Column(String(50), nullable=False, default='pending', index=True)
    # status values: 'pending', 'running', 'success', 'partial_success', 'failed'
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Results summary
    sources_checked = Column(Integer, nullable=True, default=0)
    sources_successful = Column(Integer, nullable=True, default=0)
    companies_found = Column(Integer, nullable=True, default=0)
    new_companies = Column(Integer, nullable=True, default=0)
    updated_companies = Column(Integer, nullable=True, default=0)
    
    # Agent decision trail
    strategies_used = Column(JSON, nullable=True)  # List of strategy names
    reasoning_log = Column(JSON, nullable=True)  # Detailed reasoning for each decision
    
    # Errors and warnings
    errors = Column(JSON, nullable=True)  # Structured error information
    warnings = Column(JSON, nullable=True)  # List of warning messages
    
    # Resource tracking
    requests_made = Column(Integer, nullable=True, default=0)
    tokens_used = Column(Integer, nullable=True, default=0)
    cost_usd = Column(String(50), nullable=True)  # Estimated cost
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_agentic_job_status', 'status'),
        Index('idx_agentic_job_target', 'target_investor_id', 'target_investor_type'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<AgenticCollectionJob(id={self.id}, type='{self.job_type}', "
            f"target='{self.target_investor_name}', status='{self.status}')>"
        )


# =============================================================================
# FOOT TRAFFIC & LOCATION INTELLIGENCE MODELS
# =============================================================================


class Location(Base):
    """
    Physical locations (stores, restaurants, offices, venues) for foot traffic tracking.
    
    Represents Points of Interest (POIs) that can be linked to portfolio companies
    or tracked independently for competitive analysis.
    """
    __tablename__ = "locations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Identifiers
    location_name = Column(Text, nullable=False, index=True)
    brand_name = Column(String(255), nullable=True, index=True)  # e.g., "Starbucks" for "Starbucks #1234"
    chain_id = Column(Integer, nullable=True, index=True)  # FK to private_companies.id
    
    # Location Details
    street_address = Column(Text, nullable=True)
    city = Column(String(255), nullable=True, index=True)
    state = Column(String(50), nullable=True, index=True)
    postal_code = Column(String(20), nullable=True, index=True)
    country = Column(String(100), nullable=True, default='United States')
    latitude = Column(String(50), nullable=True)  # Stored as string for precision
    longitude = Column(String(50), nullable=True)
    
    # POI Metadata
    category = Column(String(100), nullable=True, index=True)  # restaurant, retail, office, venue
    subcategory = Column(String(100), nullable=True)  # coffee_shop, fast_food, clothing_store
    
    hours_of_operation = Column(JSON, nullable=True)  # {"Monday": {"open": "0800", "close": "2000"}, ...}
    phone = Column(String(50), nullable=True)
    website = Column(Text, nullable=True)
    
    # External IDs (for API mapping)
    google_place_id = Column(String(255), nullable=True, unique=True)
    safegraph_placekey = Column(String(255), nullable=True, unique=True)
    foursquare_fsq_id = Column(String(255), nullable=True)
    placer_venue_id = Column(String(255), nullable=True)
    
    # Status
    is_active = Column(Integer, nullable=False, default=1)  # 1 = active, 0 = closed
    opened_date = Column(DateTime, nullable=True)
    closed_date = Column(DateTime, nullable=True)
    
    # Linkage to portfolio tracking
    portfolio_company_id = Column(Integer, nullable=True, index=True)  # FK to portfolio_companies.id
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_locations_brand', 'brand_name'),
        Index('idx_locations_city_state', 'city', 'state'),
        Index('idx_locations_category', 'category'),
        Index('idx_locations_chain', 'chain_id'),
        Index('idx_locations_coords', 'latitude', 'longitude'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<Location(id={self.id}, name='{self.location_name}', "
            f"brand='{self.brand_name}', city='{self.city}')>"
        )


class FootTrafficObservation(Base):
    """
    Time-series foot traffic data for locations.
    
    Captures visitor counts, patterns, and engagement metrics from multiple sources.
    Supports daily, weekly, and monthly aggregation levels.
    """
    __tablename__ = "foot_traffic_observations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, nullable=False, index=True)  # FK to locations.id
    
    # Time Period
    observation_date = Column(DateTime, nullable=False, index=True)
    observation_period = Column(String(20), nullable=False, index=True)  # daily, weekly, monthly, hourly
    
    # Traffic Metrics
    visit_count = Column(Integer, nullable=True)  # Absolute visitor count (if available)
    visitor_count = Column(Integer, nullable=True)  # Unique visitors (if available)
    visit_count_relative = Column(Integer, nullable=True)  # 0-100 scale (Google Popular Times)
    
    # Dwell & Engagement
    median_dwell_minutes = Column(String(50), nullable=True)  # NUMERIC stored as string
    avg_dwell_minutes = Column(String(50), nullable=True)
    
    # Hourly Breakdown (for daily observations)
    hourly_traffic = Column(JSON, nullable=True)  # {"00": 10, "01": 5, "02": 3, ..., "23": 15}
    
    # Day of Week Patterns (for weekly observations)
    daily_traffic = Column(JSON, nullable=True)  # {"Mon": 850, "Tue": 920, ..., "Sun": 650}
    
    # Visitor Demographics (if available from SafeGraph/Placer)
    visitor_demographics = Column(JSON, nullable=True)
    # {"age_ranges": {"18-24": 0.15, ...}, "median_income": 75000, "home_distance_mi": {...}}
    
    # Data Provenance
    source_type = Column(String(50), nullable=False, index=True)  # google, safegraph, placer, city_data, foursquare
    source_confidence = Column(String(20), nullable=True)  # high, medium, low
    
    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('location_id', 'observation_date', 'observation_period', 'source_type',
                        name='uq_traffic_observation'),
        Index('idx_traffic_location', 'location_id'),
        Index('idx_traffic_date', 'observation_date'),
        Index('idx_traffic_source', 'source_type'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<FootTrafficObservation(id={self.id}, location_id={self.location_id}, "
            f"date={self.observation_date}, visits={self.visit_count})>"
        )


class LocationMetadata(Base):
    """
    Extended metadata for locations including trade area analysis and competitive data.
    
    Stores enrichment data from Placer.ai, SafeGraph, and other sources.
    """
    __tablename__ = "location_metadata"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, nullable=False, index=True)  # FK to locations.id
    
    # Business Info
    square_footage = Column(Integer, nullable=True)
    employee_count_estimate = Column(Integer, nullable=True)
    parking_spots = Column(Integer, nullable=True)
    
    # Trade Area (from Placer.ai)
    trade_area_5min_population = Column(Integer, nullable=True)
    trade_area_5min_median_income = Column(String(50), nullable=True)
    trade_area_10min_population = Column(Integer, nullable=True)
    trade_area_10min_median_income = Column(String(50), nullable=True)
    
    # Competitive Set
    nearby_competitors = Column(JSON, nullable=True)
    # [{"name": "Panera #123", "distance_mi": 0.5, "category": "fast_casual"}, ...]
    
    # Ratings & Reviews
    google_rating = Column(String(10), nullable=True)  # e.g., "4.5"
    google_review_count = Column(Integer, nullable=True)
    yelp_rating = Column(String(10), nullable=True)
    yelp_review_count = Column(Integer, nullable=True)
    
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_metadata_location', 'location_id'),
    )
    
    def __repr__(self) -> str:
        return f"<LocationMetadata(id={self.id}, location_id={self.location_id})>"


class FootTrafficCollectionJob(Base):
    """
    Tracks foot traffic collection jobs with full agent reasoning trail.
    
    Supports multiple job types:
    - discover_locations: Find locations for a brand
    - collect_traffic: Gather foot traffic data for locations
    - enrich_metadata: Add trade area and competitive data
    """
    __tablename__ = "foot_traffic_collection_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False, index=True)
    # job_type values: 'discover_locations', 'collect_traffic', 'enrich_metadata', 'analyze_trends'
    
    # Target specification
    target_brand = Column(String(255), nullable=True, index=True)
    target_location_id = Column(Integer, nullable=True, index=True)
    geographic_scope = Column(Text, nullable=True)  # city, state, national, specific_address
    
    # Job status
    status = Column(String(50), nullable=False, default='pending', index=True)
    # status values: 'pending', 'running', 'success', 'partial_success', 'failed'
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Results
    locations_found = Column(Integer, nullable=True, default=0)
    locations_enriched = Column(Integer, nullable=True, default=0)
    observations_collected = Column(Integer, nullable=True, default=0)
    sources_checked = Column(JSON, nullable=True)  # List of source names checked
    
    # Agent Reasoning
    reasoning_log = Column(JSON, nullable=True)  # Detailed reasoning for each decision
    
    # Errors and Warnings
    errors = Column(JSON, nullable=True)
    warnings = Column(JSON, nullable=True)
    
    # Resource tracking
    requests_made = Column(Integer, nullable=True, default=0)
    cost_usd = Column(String(50), nullable=True)  # Estimated API cost
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_ft_job_status', 'status'),
        Index('idx_ft_job_brand', 'target_brand'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<FootTrafficCollectionJob(id={self.id}, type='{self.job_type}', "
            f"brand='{self.target_brand}', status='{self.status}')>"
        )


# =============================================================================
# PREDICTION MARKET INTELLIGENCE MODELS
# =============================================================================


class PredictionMarket(Base):
    """
    Prediction market metadata from Kalshi, Polymarket, PredictIt.
    
    Tracks market details, categories, and resolution status.
    Markets are uniquely identified by (source, market_id).
    """
    __tablename__ = "prediction_markets"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source & Identifiers
    source = Column(String(50), nullable=False, index=True)  # kalshi, polymarket, predictit
    market_id = Column(String(255), nullable=False, index=True)  # platform-specific ID
    market_url = Column(Text, nullable=True)  # direct link to market
    
    # Market Details
    question = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=True, index=True)  # economics, politics, sports, crypto, business
    subcategory = Column(String(100), nullable=True, index=True)  # fed_rates, presidential_election, nfl, etc.
    
    # Market Type
    outcome_type = Column(String(50), nullable=True)  # binary, multiple_choice, scalar
    possible_outcomes = Column(JSON, nullable=True)  # for multiple choice markets
    
    # Timing
    created_date = Column(DateTime, nullable=True)
    close_date = Column(DateTime, nullable=True, index=True)  # when market resolves
    resolved_date = Column(DateTime, nullable=True)
    
    # Resolution
    resolved_outcome = Column(String(255), nullable=True)  # actual outcome when market closes
    
    # Status
    is_active = Column(Integer, nullable=False, default=1, index=True)  # 1 = active, 0 = resolved/closed
    is_featured = Column(Integer, nullable=False, default=0)  # high-profile market
    
    # Latest Values (for quick queries without joining observations)
    last_yes_probability = Column(String(20), nullable=True)  # 0.00 to 1.00
    last_volume_usd = Column(String(50), nullable=True)
    last_updated = Column(DateTime, nullable=True)
    
    # Metadata
    first_observed = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('source', 'market_id', name='uq_prediction_market'),
        Index('idx_pm_source', 'source'),
        Index('idx_pm_category', 'category'),
        Index('idx_pm_close_date', 'close_date'),
        Index('idx_pm_active', 'is_active'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<PredictionMarket(id={self.id}, source='{self.source}', "
            f"question='{self.question[:50]}...', prob={self.last_yes_probability})>"
        )


class MarketObservation(Base):
    """
    Time-series probability and volume data for prediction markets.
    
    Captures snapshots of market state at regular intervals for trend analysis.
    """
    __tablename__ = "market_observations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, nullable=False, index=True)  # FK to prediction_markets.id
    
    # Observation Time
    observation_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    
    # Probabilities (0.00 to 1.00)
    yes_probability = Column(String(20), nullable=False)  # stored as string for precision
    no_probability = Column(String(20), nullable=True)  # can be derived (1 - yes) for binary
    
    # For Multiple Choice Markets
    outcome_probabilities = Column(JSON, nullable=True)  # {"outcome_1": "0.45", "outcome_2": "0.35", ...}
    
    # Market Activity
    volume_usd = Column(String(50), nullable=True)  # total volume
    volume_24h_usd = Column(String(50), nullable=True)  # 24h volume
    liquidity_usd = Column(String(50), nullable=True)  # open interest / liquidity
    trade_count = Column(Integer, nullable=True)  # number of trades (if available)
    
    # Price Movement (calculated)
    probability_change_1h = Column(String(20), nullable=True)  # change from 1 hour ago
    probability_change_24h = Column(String(20), nullable=True)  # change from 24 hours ago
    probability_change_7d = Column(String(20), nullable=True)  # change from 7 days ago
    
    # Data Quality
    data_source = Column(String(50), nullable=False, default='api')  # api, browser, manual
    confidence_score = Column(String(10), nullable=True)  # 0-1, how confident in data quality
    
    __table_args__ = (
        Index('idx_mo_market', 'market_id'),
        Index('idx_mo_timestamp', 'observation_timestamp'),
        Index('idx_mo_market_time', 'market_id', 'observation_timestamp'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<MarketObservation(id={self.id}, market_id={self.market_id}, "
            f"prob={self.yes_probability}, time={self.observation_timestamp})>"
        )


class MarketCategory(Base):
    """
    Classification system for prediction markets.
    
    Links market categories to relevant sectors and companies,
    and defines alert thresholds for monitoring.
    """
    __tablename__ = "market_categories"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    category_name = Column(String(100), nullable=False, unique=True, index=True)
    parent_category = Column(String(100), nullable=True)  # for hierarchical categories
    display_name = Column(String(255), nullable=True)  # human-readable name
    
    # Relevance
    relevant_sectors = Column(JSON, nullable=True)  # which sectors this affects
    relevant_companies = Column(JSON, nullable=True)  # array of company IDs
    impact_level = Column(String(20), nullable=True)  # high, medium, low
    
    # Monitoring
    monitoring_priority = Column(Integer, nullable=True)  # 1-5 (5 = highest)
    alert_threshold = Column(String(10), nullable=True)  # probability change threshold (e.g., "0.10")
    
    description = Column(Text, nullable=True)
    
    def __repr__(self) -> str:
        return f"<MarketCategory(name='{self.category_name}', priority={self.monitoring_priority})>"


class MarketAlert(Base):
    """
    Alerts for significant probability shifts in prediction markets.
    
    Generated when probability changes exceed configured thresholds.
    """
    __tablename__ = "market_alerts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, nullable=False, index=True)  # FK to prediction_markets.id
    
    # Alert Details
    alert_type = Column(String(50), nullable=False, index=True)
    # alert_type values: 'probability_spike', 'probability_drop', 'volume_surge', 'new_market', 'market_resolved'
    alert_severity = Column(String(20), nullable=False, index=True)  # critical, high, medium, low
    
    # Trigger Conditions
    triggered_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    probability_before = Column(String(20), nullable=True)
    probability_after = Column(String(20), nullable=True)
    probability_change = Column(String(20), nullable=True)
    time_period = Column(String(20), nullable=True)  # 1h, 24h, 7d
    
    # Context
    alert_message = Column(Text, nullable=True)
    affected_sectors = Column(JSON, nullable=True)
    affected_companies = Column(JSON, nullable=True)
    
    # Status
    is_acknowledged = Column(Integer, nullable=False, default=0)  # 0 or 1
    acknowledged_at = Column(DateTime, nullable=True)
    acknowledged_by = Column(String(100), nullable=True)
    
    __table_args__ = (
        Index('idx_alert_market', 'market_id'),
        Index('idx_alert_triggered', 'triggered_at'),
        Index('idx_alert_severity', 'alert_severity'),
        Index('idx_alert_acknowledged', 'is_acknowledged'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<MarketAlert(id={self.id}, market_id={self.market_id}, "
            f"type='{self.alert_type}', severity='{self.alert_severity}')>"
        )


class PredictionMarketJob(Base):
    """
    Tracks prediction market monitoring jobs.
    
    Each job monitors one or more platforms and captures:
    - Markets checked and updated
    - New markets discovered
    - Alerts generated
    """
    __tablename__ = "prediction_market_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False, index=True)
    # job_type values: 'monitor_all', 'monitor_kalshi', 'monitor_polymarket', 
    #                  'monitor_predictit', 'analyze_trends', 'generate_alerts'
    
    # Target specification
    target_platforms = Column(JSON, nullable=True)  # ['kalshi', 'polymarket', 'predictit']
    target_categories = Column(JSON, nullable=True)  # ['economics', 'politics', 'sports']
    
    # Job status
    status = Column(String(50), nullable=False, default='pending', index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Results
    markets_checked = Column(Integer, nullable=True, default=0)
    markets_updated = Column(Integer, nullable=True, default=0)
    new_markets_found = Column(Integer, nullable=True, default=0)
    observations_stored = Column(Integer, nullable=True, default=0)
    alerts_generated = Column(Integer, nullable=True, default=0)
    
    # Agent Reasoning
    reasoning_log = Column(JSON, nullable=True)
    
    # Errors
    errors = Column(JSON, nullable=True)
    warnings = Column(JSON, nullable=True)
    
    # Resource tracking
    requests_made = Column(Integer, nullable=True, default=0)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_pm_job_status', 'status'),
        Index('idx_pm_job_type', 'job_type'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<PredictionMarketJob(id={self.id}, type='{self.job_type}', "
            f"status='{self.status}', markets_updated={self.markets_updated})>"
        )


# =============================================================================
# WEBHOOK NOTIFICATION MODELS
# =============================================================================


class WebhookEventType(str, enum.Enum):
    """Types of events that can trigger webhooks."""
    JOB_FAILED = "job_failed"
    JOB_SUCCESS = "job_success"
    ALERT_HIGH_FAILURE_RATE = "alert_high_failure_rate"
    ALERT_STUCK_JOB = "alert_stuck_job"
    ALERT_DATA_STALENESS = "alert_data_staleness"
    SCHEDULE_TRIGGERED = "schedule_triggered"
    CLEANUP_COMPLETED = "cleanup_completed"


class Webhook(Base):
    """
    Webhook configuration for notifications.

    Webhooks can be configured to send HTTP POST requests to external
    services (Slack, Discord, custom endpoints) when events occur.
    """
    __tablename__ = "webhooks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    url = Column(String(2048), nullable=False)  # Webhook endpoint URL

    # Event configuration
    event_types = Column(JSON, nullable=False)  # List of WebhookEventType values
    source_filter = Column(String(50), nullable=True)  # Optional: only trigger for specific source

    # Authentication (optional)
    secret = Column(String(255), nullable=True)  # For HMAC signature verification
    headers = Column(JSON, nullable=True)  # Custom headers to include

    # State
    is_active = Column(Integer, nullable=False, default=1)  # 1=active, 0=disabled

    # Statistics
    total_sent = Column(Integer, nullable=False, default=0)
    total_failed = Column(Integer, nullable=False, default=0)
    last_sent_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<Webhook(id={self.id}, name='{self.name}', "
            f"active={self.is_active}, sent={self.total_sent})>"
        )


class WebhookDelivery(Base):
    """
    Log of webhook delivery attempts.

    Tracks each webhook notification sent, including success/failure status.
    """
    __tablename__ = "webhook_deliveries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(Integer, nullable=False, index=True)

    # Event details
    event_type = Column(String(50), nullable=False, index=True)
    event_data = Column(JSON, nullable=False)  # The payload sent

    # Delivery status
    status = Column(String(20), nullable=False, index=True)  # success, failed, pending
    response_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)

    # Timing
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    delivered_at = Column(DateTime, nullable=True)

    # Retry tracking
    attempt_number = Column(Integer, nullable=False, default=1)

    __table_args__ = (
        Index('idx_webhook_delivery_webhook', 'webhook_id'),
        Index('idx_webhook_delivery_status', 'status'),
        Index('idx_webhook_delivery_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<WebhookDelivery(id={self.id}, webhook_id={self.webhook_id}, "
            f"event='{self.event_type}', status='{self.status}')>"
        )


# =============================================================================
# JOB DEPENDENCY MODELS
# =============================================================================


class DependencyCondition(str, enum.Enum):
    """Condition for when a dependency is satisfied."""
    ON_SUCCESS = "on_success"  # Parent must succeed
    ON_COMPLETE = "on_complete"  # Parent must complete (success or failure)
    ON_FAILURE = "on_failure"  # Parent must fail (for error handling jobs)


class JobDependency(Base):
    """
    Defines dependencies between ingestion jobs.

    Supports DAG-style workflows where jobs can depend on other jobs.
    A job will not start until all its dependencies are satisfied.
    """
    __tablename__ = "job_dependencies"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # The job that has the dependency
    job_id = Column(Integer, nullable=False, index=True)

    # The job that must complete first
    depends_on_job_id = Column(Integer, nullable=False, index=True)

    # When is the dependency satisfied?
    condition = Column(
        Enum(DependencyCondition, native_enum=False, length=20),
        nullable=False,
        default=DependencyCondition.ON_SUCCESS
    )

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_job_dep_job', 'job_id'),
        Index('idx_job_dep_parent', 'depends_on_job_id'),
        UniqueConstraint('job_id', 'depends_on_job_id', name='uq_job_dependency'),
    )

    def __repr__(self) -> str:
        return (
            f"<JobDependency(job_id={self.job_id}, depends_on={self.depends_on_job_id}, "
            f"condition='{self.condition}')>"
        )


class JobChain(Base):
    """
    Named job chain (workflow) definition.

    Chains are reusable workflow templates that define a sequence of jobs
    with their dependencies. Creating a chain instance creates all jobs
    and dependencies automatically.
    """
    __tablename__ = "job_chains"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)

    # Chain definition (JSON array of job configurations)
    # Format: [{"source": "fred", "config": {...}, "depends_on": [0]}, ...]
    # depends_on uses array indices
    chain_definition = Column(JSON, nullable=False)

    # State
    is_active = Column(Integer, nullable=False, default=1)

    # Statistics
    times_executed = Column(Integer, nullable=False, default=0)
    last_executed_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<JobChain(id={self.id}, name='{self.name}', "
            f"active={self.is_active}, executions={self.times_executed})>"
        )


class JobChainExecution(Base):
    """
    Tracks execution of a job chain.

    Each time a chain is executed, this records all the jobs created
    and tracks overall chain status.
    """
    __tablename__ = "job_chain_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chain_id = Column(Integer, nullable=False, index=True)

    # Status
    status = Column(String(20), nullable=False, default='running', index=True)
    # status values: 'running', 'success', 'partial_success', 'failed'

    # Job tracking
    job_ids = Column(JSON, nullable=False)  # Array of job IDs in this execution
    total_jobs = Column(Integer, nullable=False, default=0)
    completed_jobs = Column(Integer, nullable=False, default=0)
    successful_jobs = Column(Integer, nullable=False, default=0)
    failed_jobs = Column(Integer, nullable=False, default=0)

    # Timing
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index('idx_chain_exec_chain', 'chain_id'),
        Index('idx_chain_exec_status', 'status'),
    )

    def __repr__(self) -> str:
        return (
            f"<JobChainExecution(id={self.id}, chain_id={self.chain_id}, "
            f"status='{self.status}', progress={self.completed_jobs}/{self.total_jobs})>"
        )


# =============================================================================
# PER-SOURCE RATE LIMIT MODELS
# =============================================================================


class SourceRateLimit(Base):
    """
    Configurable rate limits per data source.

    Stores rate limit configuration for each external API source.
    Supports token bucket algorithm with burst capacity.
    """
    __tablename__ = "source_rate_limits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False, unique=True, index=True)

    # Rate limit configuration
    requests_per_second = Column(String(20), nullable=False, default="1.0")  # Tokens added per second
    burst_capacity = Column(Integer, nullable=False, default=10)  # Maximum tokens (burst size)
    concurrent_limit = Column(Integer, nullable=False, default=5)  # Max concurrent requests

    # Current state (for distributed rate limiting)
    current_tokens = Column(String(20), nullable=True)  # Current token count
    last_refill_at = Column(DateTime, nullable=True)  # Last token refill time

    # Statistics
    total_requests = Column(Integer, nullable=False, default=0)
    total_throttled = Column(Integer, nullable=False, default=0)
    last_request_at = Column(DateTime, nullable=True)

    # Configuration
    is_enabled = Column(Integer, nullable=False, default=1)  # 1=enabled, 0=disabled
    description = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<SourceRateLimit(source='{self.source}', "
            f"rps={self.requests_per_second}, burst={self.burst_capacity})>"
        )


# =============================================================================
# DATA QUALITY RULES ENGINE MODELS
# =============================================================================


class RuleType(str, enum.Enum):
    """Types of data quality validation rules."""
    RANGE = "range"  # Value must be within min/max
    NOT_NULL = "not_null"  # Value must not be null
    UNIQUE = "unique"  # Values must be unique
    REGEX = "regex"  # Value must match pattern
    FRESHNESS = "freshness"  # Data must be recent
    ROW_COUNT = "row_count"  # Minimum/maximum rows
    CUSTOM_SQL = "custom_sql"  # Custom SQL condition
    ENUM = "enum"  # Value must be in allowed list
    COMPARISON = "comparison"  # Compare two columns


class RuleSeverity(str, enum.Enum):
    """Severity levels for rule violations."""
    ERROR = "error"  # Critical - fails validation
    WARNING = "warning"  # Non-critical - logged but doesn't fail
    INFO = "info"  # Informational only


class DataQualityRule(Base):
    """
    Configurable data quality validation rules.

    Rules can be applied to specific sources, datasets, or columns.
    Supports various validation types with configurable thresholds.
    """
    __tablename__ = "data_quality_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)

    # Targeting
    source = Column(String(50), nullable=True, index=True)  # null = all sources
    dataset_pattern = Column(String(255), nullable=True)  # Regex pattern for dataset/table names
    column_name = Column(String(100), nullable=True)  # Specific column (null = table-level)

    # Rule configuration
    rule_type = Column(
        Enum(RuleType, native_enum=False, length=20),
        nullable=False
    )
    severity = Column(
        Enum(RuleSeverity, native_enum=False, length=20),
        nullable=False,
        default=RuleSeverity.ERROR
    )

    # Rule parameters (JSON for flexibility)
    # Examples:
    # RANGE: {"min": 0, "max": 100}
    # NOT_NULL: {}
    # REGEX: {"pattern": "^[A-Z]{2}$"}
    # FRESHNESS: {"max_age_hours": 24}
    # ROW_COUNT: {"min": 1, "max": 1000000}
    # ENUM: {"allowed": ["A", "B", "C"]}
    # COMPARISON: {"operator": ">", "compare_column": "other_col"}
    # CUSTOM_SQL: {"condition": "column_a > column_b"}
    parameters = Column(JSON, nullable=False, default={})

    # State
    is_enabled = Column(Integer, nullable=False, default=1)
    priority = Column(Integer, nullable=False, default=5)  # 1=highest, 10=lowest

    # Statistics
    times_evaluated = Column(Integer, nullable=False, default=0)
    times_passed = Column(Integer, nullable=False, default=0)
    times_failed = Column(Integer, nullable=False, default=0)
    last_evaluated_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_dq_rule_source', 'source'),
        Index('idx_dq_rule_type', 'rule_type'),
        Index('idx_dq_rule_enabled', 'is_enabled'),
    )

    def __repr__(self) -> str:
        return (
            f"<DataQualityRule(id={self.id}, name='{self.name}', "
            f"type='{self.rule_type}', severity='{self.severity}')>"
        )


class DataQualityResult(Base):
    """
    Results of data quality rule evaluations.

    Stores the outcome of each rule check for audit and reporting.
    """
    __tablename__ = "data_quality_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=True, index=True)  # Associated ingestion job

    # Context
    source = Column(String(50), nullable=False, index=True)
    dataset_name = Column(String(255), nullable=True)
    column_name = Column(String(100), nullable=True)

    # Result
    passed = Column(Integer, nullable=False)  # 1=passed, 0=failed
    severity = Column(
        Enum(RuleSeverity, native_enum=False, length=20),
        nullable=False
    )

    # Details
    message = Column(Text, nullable=True)  # Human-readable result
    actual_value = Column(Text, nullable=True)  # What was found
    expected_value = Column(Text, nullable=True)  # What was expected
    sample_failures = Column(JSON, nullable=True)  # Sample of failing rows/values

    # Metrics
    rows_checked = Column(Integer, nullable=True)
    rows_passed = Column(Integer, nullable=True)
    rows_failed = Column(Integer, nullable=True)
    execution_time_ms = Column(Integer, nullable=True)

    # Timestamps
    evaluated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_dq_result_rule', 'rule_id'),
        Index('idx_dq_result_job', 'job_id'),
        Index('idx_dq_result_passed', 'passed'),
        Index('idx_dq_result_evaluated', 'evaluated_at'),
    )

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"<DataQualityResult(id={self.id}, rule_id={self.rule_id}, "
            f"status={status}, severity='{self.severity}')>"
        )


class DataQualityReport(Base):
    """
    Aggregated data quality report for a job or time period.

    Summarizes rule evaluations for easy monitoring and alerting.
    """
    __tablename__ = "data_quality_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, nullable=True, index=True)  # Associated job (null for scheduled reports)

    # Scope
    source = Column(String(50), nullable=True, index=True)
    report_type = Column(String(50), nullable=False, default='job')  # job, daily, weekly

    # Summary
    total_rules = Column(Integer, nullable=False, default=0)
    rules_passed = Column(Integer, nullable=False, default=0)
    rules_failed = Column(Integer, nullable=False, default=0)
    rules_warned = Column(Integer, nullable=False, default=0)

    # By severity
    errors = Column(Integer, nullable=False, default=0)
    warnings = Column(Integer, nullable=False, default=0)
    info = Column(Integer, nullable=False, default=0)

    # Overall status
    overall_status = Column(String(20), nullable=False, default='pending')
    # Values: 'passed', 'failed', 'warning', 'pending'

    # Details
    failed_rules = Column(JSON, nullable=True)  # List of failed rule names/IDs
    execution_time_ms = Column(Integer, nullable=True)

    # Timestamps
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index('idx_dq_report_job', 'job_id'),
        Index('idx_dq_report_status', 'overall_status'),
        Index('idx_dq_report_started', 'started_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<DataQualityReport(id={self.id}, job_id={self.job_id}, "
            f"status='{self.overall_status}', passed={self.rules_passed}/{self.total_rules})>"
        )


# =============================================================================
# BULK INGESTION TEMPLATE MODELS
# =============================================================================


class TemplateCategory(str, enum.Enum):
    """Categories for organizing templates."""
    DEMOGRAPHICS = "demographics"
    ECONOMIC = "economic"
    FINANCIAL = "financial"
    ENERGY = "energy"
    HEALTHCARE = "healthcare"
    REAL_ESTATE = "real_estate"
    TRADE = "trade"
    CUSTOM = "custom"


class IngestionTemplate(Base):
    """
    Reusable bulk ingestion templates.

    Templates define multiple jobs to run together with optional parameters.
    Supports variable substitution for customizable ingestion patterns.
    """
    __tablename__ = "ingestion_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)

    # Categorization
    category = Column(
        Enum(TemplateCategory, native_enum=False, length=20),
        nullable=False,
        default=TemplateCategory.CUSTOM
    )
    tags = Column(JSON, nullable=True)  # ["census", "state-level", "demographics"]

    # Template definition
    # Format: [{"source": "census", "config": {"year": "{{year}}", ...}}, ...]
    # Variables use {{variable_name}} syntax
    jobs_definition = Column(JSON, nullable=False)

    # Variable schema (for validation and documentation)
    # Format: {"year": {"type": "integer", "default": 2023, "description": "..."}, ...}
    variables = Column(JSON, nullable=True)

    # Execution settings
    use_chain = Column(Integer, nullable=False, default=0)  # 1 = create as job chain
    parallel_execution = Column(Integer, nullable=False, default=1)  # 1 = run jobs in parallel

    # State
    is_builtin = Column(Integer, nullable=False, default=0)  # 1 = system template
    is_enabled = Column(Integer, nullable=False, default=1)

    # Statistics
    times_executed = Column(Integer, nullable=False, default=0)
    last_executed_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_template_category', 'category'),
        Index('idx_template_enabled', 'is_enabled'),
    )

    def __repr__(self) -> str:
        return (
            f"<IngestionTemplate(id={self.id}, name='{self.name}', "
            f"category='{self.category}', jobs={len(self.jobs_definition or [])})>"
        )


class TemplateExecution(Base):
    """
    Tracks execution of ingestion templates.

    Records each time a template is run with the parameters used.
    """
    __tablename__ = "template_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    template_id = Column(Integer, nullable=False, index=True)
    template_name = Column(String(255), nullable=False)  # Denormalized for history

    # Execution parameters
    parameters = Column(JSON, nullable=True)  # Variables used for this execution

    # Status
    status = Column(String(20), nullable=False, default='running', index=True)
    # Values: 'running', 'success', 'partial_success', 'failed'

    # Job tracking
    job_ids = Column(JSON, nullable=False)  # Array of created job IDs
    chain_id = Column(Integer, nullable=True)  # If executed as chain
    total_jobs = Column(Integer, nullable=False, default=0)
    completed_jobs = Column(Integer, nullable=False, default=0)
    successful_jobs = Column(Integer, nullable=False, default=0)
    failed_jobs = Column(Integer, nullable=False, default=0)

    # Timing
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    # Error tracking
    errors = Column(JSON, nullable=True)

    __table_args__ = (
        Index('idx_template_exec_template', 'template_id'),
        Index('idx_template_exec_status', 'status'),
        Index('idx_template_exec_started', 'started_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<TemplateExecution(id={self.id}, template='{self.template_name}', "
            f"status='{self.status}', progress={self.completed_jobs}/{self.total_jobs})>"
        )
