"""
Site Intelligence Platform - Types and Pydantic Models.

Defines enums, configuration models, and result schemas used across all collectors.
"""
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# =============================================================================
# ENUMS
# =============================================================================

class SiteIntelDomain(str, Enum):
    """Site intelligence data domains."""
    POWER = "power"
    TELECOM = "telecom"
    TRANSPORT = "transport"
    LABOR = "labor"
    RISK = "risk"
    INCENTIVES = "incentives"
    LOGISTICS = "logistics"
    SCORING = "scoring"


class SiteIntelSource(str, Enum):
    """Data sources for site intelligence."""
    # Power
    EIA = "eia"
    NREL = "nrel"
    HIFLD = "hifld"
    ISO_PJM = "iso_pjm"
    ISO_CAISO = "iso_caiso"
    ISO_ERCOT = "iso_ercot"
    ISO_MISO = "iso_miso"
    ISO_SPP = "iso_spp"
    ISO_NYISO = "iso_nyiso"
    ISO_ISONE = "iso_isone"

    # Telecom
    FCC = "fcc"
    PEERINGDB = "peeringdb"
    TELEGEOGRAPHY = "telegeography"
    RIPE_ATLAS = "ripe_atlas"

    # Transport
    BTS = "bts"  # Combined BTS transport data
    BTS_NTAD = "bts_ntad"
    FRA = "fra"
    USACE = "usace"
    FAA = "faa"
    FHWA = "fhwa"

    # Labor
    BLS = "bls"  # Combined BLS labor data
    BLS_OES = "bls_oes"
    BLS_QCEW = "bls_qcew"
    CENSUS_LEHD = "census_lehd"
    CENSUS_ACS = "census_acs"

    # Risk
    FEMA = "fema"  # Combined FEMA risk data
    FEMA_NFHL = "fema_nfhl"
    FEMA_NRI = "fema_nri"  # National Risk Index
    USGS_EARTHQUAKE = "usgs_earthquake"
    NOAA_CLIMATE = "noaa_climate"
    EPA_ENVIROFACTS = "epa_envirofacts"
    USFWS_NWI = "usfws_nwi"

    # Incentives
    CDFI_OZ = "cdfi_oz"
    FTZ_BOARD = "ftz_board"
    GOOD_JOBS_FIRST = "good_jobs_first"
    STATE_EDO = "state_edo"

    # Logistics
    FREIGHTOS = "freightos"
    USDA_AMS = "usda_ams"


class CollectionStatus(str, Enum):
    """Status of a collection job."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"  # Some items failed
    FAILED = "failed"


class UseCase(str, Enum):
    """Site selection use cases for scoring."""
    DATA_CENTER = "data_center"
    WAREHOUSE = "warehouse"
    MANUFACTURING = "manufacturing"
    OFFICE = "office"
    MIXED_USE = "mixed_use"


# =============================================================================
# CONFIGURATION MODELS
# =============================================================================

class CollectionConfig(BaseModel):
    """Configuration for a collection job."""
    domain: SiteIntelDomain
    source: SiteIntelSource
    job_type: str = "full_sync"  # full_sync, incremental, single_item

    # Geographic filters
    states: Optional[List[str]] = None
    counties: Optional[List[str]] = None
    bbox: Optional[Dict[str, float]] = None  # min_lat, max_lat, min_lng, max_lng

    # Time filters
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    year: Optional[int] = None

    # Pagination
    limit: Optional[int] = None
    offset: Optional[int] = 0

    # Source-specific options
    options: Optional[Dict[str, Any]] = None


class GeoPoint(BaseModel):
    """Geographic point."""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)


class BoundingBox(BaseModel):
    """Geographic bounding box."""
    min_lat: float = Field(..., ge=-90, le=90)
    max_lat: float = Field(..., ge=-90, le=90)
    min_lng: float = Field(..., ge=-180, le=180)
    max_lng: float = Field(..., ge=-180, le=180)


class NearbyQuery(BaseModel):
    """Query for nearby features."""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    radius_miles: float = Field(default=25, gt=0, le=500)
    limit: int = Field(default=50, gt=0, le=500)


# =============================================================================
# RESULT MODELS
# =============================================================================

class CollectionResult(BaseModel):
    """Result of a collection job."""
    status: CollectionStatus
    domain: SiteIntelDomain
    source: SiteIntelSource

    # Counts
    total_items: int = 0
    processed_items: int = 0
    inserted_items: int = 0
    updated_items: int = 0
    failed_items: int = 0

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Error handling
    errors: Optional[List[Dict[str, Any]]] = None
    error_message: Optional[str] = None

    # Data sample (for verification)
    sample_records: Optional[List[Dict[str, Any]]] = None


class CollectionProgress(BaseModel):
    """Progress update during collection."""
    job_id: int
    status: CollectionStatus
    processed_items: int
    total_items: int
    progress_pct: float
    current_step: Optional[str] = None
    errors_so_far: int = 0


# =============================================================================
# SITE SCORING MODELS
# =============================================================================

class ScoringFactorWeight(BaseModel):
    """Weight configuration for a scoring factor."""
    factor_name: str
    weight: float = Field(..., ge=-1.0, le=1.0)  # Negative for risks
    enabled: bool = True


class ScoringConfig(BaseModel):
    """Configuration for site scoring."""
    use_case: UseCase
    factor_weights: List[ScoringFactorWeight]
    normalize_scores: bool = True


class FactorScore(BaseModel):
    """Individual factor score."""
    factor_name: str
    raw_value: Optional[float] = None
    normalized_score: float = Field(..., ge=0, le=100)
    weight: float
    weighted_score: float
    details: Optional[Dict[str, Any]] = None


class SiteScoreResult(BaseModel):
    """Complete site score result."""
    latitude: float
    longitude: float
    use_case: UseCase
    overall_score: float = Field(..., ge=0, le=100)
    factor_scores: List[FactorScore]
    computed_at: datetime
    valid_until: Optional[datetime] = None

    # Location context
    state: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None

    # Summary by domain
    domain_scores: Optional[Dict[str, float]] = None


class SiteComparisonResult(BaseModel):
    """Result of comparing multiple sites."""
    sites: List[SiteScoreResult]
    best_overall: Optional[SiteScoreResult] = None
    best_by_factor: Optional[Dict[str, SiteScoreResult]] = None
    comparison_matrix: Optional[Dict[str, Dict[str, float]]] = None


# =============================================================================
# API REQUEST/RESPONSE MODELS
# =============================================================================

class SiteScoreRequest(BaseModel):
    """Request to score a site."""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    use_case: UseCase = UseCase.DATA_CENTER
    factors: Optional[List[str]] = None  # If None, use all factors
    custom_weights: Optional[Dict[str, float]] = None


class SiteCompareRequest(BaseModel):
    """Request to compare multiple sites."""
    locations: List[Dict[str, Any]]  # [{"name": "...", "lat": ..., "lng": ...}]
    use_case: UseCase = UseCase.DATA_CENTER
    factors: Optional[List[str]] = None


class SiteSearchRequest(BaseModel):
    """Request to search for sites matching criteria."""
    use_case: UseCase = UseCase.DATA_CENTER
    region: Optional[Dict[str, Any]] = None  # {"states": [...]} or {"bbox": {...}}
    requirements: Optional[Dict[str, Any]] = None  # {"min_acreage": 50, "rail_required": true}
    sort_by: str = "overall_score"
    limit: int = Field(default=20, gt=0, le=100)


# =============================================================================
# INFRASTRUCTURE DETAIL MODELS
# =============================================================================

class PowerPlantSummary(BaseModel):
    """Summary of a power plant."""
    id: int
    eia_plant_id: Optional[str]
    name: str
    latitude: float
    longitude: float
    state: str
    primary_fuel: str
    nameplate_capacity_mw: Optional[float]
    distance_miles: Optional[float] = None


class SubstationSummary(BaseModel):
    """Summary of a substation."""
    id: int
    name: Optional[str]
    latitude: float
    longitude: float
    state: str
    max_voltage_kv: Optional[float]
    distance_miles: Optional[float] = None


class DataCenterSummary(BaseModel):
    """Summary of a data center facility."""
    id: int
    name: str
    operator: Optional[str]
    city: Optional[str]
    state: Optional[str]
    latitude: float
    longitude: float
    network_count: Optional[int]
    distance_miles: Optional[float] = None


class IntermodalTerminalSummary(BaseModel):
    """Summary of an intermodal terminal."""
    id: int
    name: str
    railroad: Optional[str]
    city: Optional[str]
    state: str
    latitude: float
    longitude: float
    annual_lifts: Optional[int]
    distance_miles: Optional[float] = None


class PortSummary(BaseModel):
    """Summary of a port."""
    id: int
    port_code: str
    name: str
    state: str
    latitude: float
    longitude: float
    has_container_terminal: bool
    distance_miles: Optional[float] = None


class AirportSummary(BaseModel):
    """Summary of an airport."""
    id: int
    faa_code: Optional[str]
    name: str
    city: Optional[str]
    state: str
    latitude: float
    longitude: float
    has_cargo_facility: bool
    distance_miles: Optional[float] = None
