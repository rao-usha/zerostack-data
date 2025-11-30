"""
SQLAlchemy models for core tables.

These tables are source-agnostic and used by all data source adapters.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Enum, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()


class JobStatus(str, enum.Enum):
    """Job status enumeration - ONLY these values allowed."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


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
    
    def __repr__(self) -> str:
        return (
            f"<IngestionJob(id={self.id}, source={self.source}, "
            f"status={self.status}, created_at={self.created_at})>"
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
    investment committee minutes, and LinkedIn. No private/restricted data.
    
    Typical roles: CIO, Head of Private Equity, Managing Director - Alternatives,
    Senior Investment Officer, etc.
    """
    __tablename__ = "lp_key_contact"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lp_id = Column(Integer, nullable=False, index=True)  # FK to lp_fund.id
    
    # Personal information (public only)
    full_name = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)  # e.g., "Chief Investment Officer"
    department = Column(String(255), nullable=True)  # e.g., "Private Equity", "Alternatives"
    
    # Contact information (public only - from official sources)
    email = Column(String(255), nullable=True)  # Only if publicly listed
    phone = Column(String(50), nullable=True)  # Only if publicly listed
    office_location = Column(String(255), nullable=True)  # e.g., "Sacramento, CA"
    
    # Professional profile
    linkedin_url = Column(Text, nullable=True)
    bio_summary = Column(Text, nullable=True)  # Brief background if available
    years_at_fund = Column(Integer, nullable=True)
    
    # Categorization
    contact_type = Column(String(100), nullable=False, index=True)
    # contact_type values: 'cio', 'head_of_pe', 'head_of_alternatives', 'senior_investment_officer',
    #                      'managing_director', 'investment_committee_member', 'ir_contact', 'operations'
    
    seniority_level = Column(String(50), nullable=True)  # 'c_suite', 'senior', 'mid', 'junior'
    is_decision_maker = Column(Integer, default=0)  # 0 or 1 (boolean) - for investment decisions
    
    # Data quality
    last_verified = Column(DateTime, nullable=True)
    source_url = Column(Text, nullable=True)  # Where this information was found
    confidence_score = Column(String(50), nullable=True)  # 0.0-1.0 (data quality indicator)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_lp_contact_lp_type', 'lp_id', 'contact_type'),
        UniqueConstraint('lp_id', 'full_name', 'title', name='uq_lp_contact'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LpKeyContact(id={self.id}, lp_id={self.lp_id}, "
            f"name='{self.full_name}', title='{self.title}')>"
        )

