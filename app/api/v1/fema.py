"""
OpenFEMA API endpoints.

Provides HTTP endpoints for ingesting FEMA disaster and emergency data:
- Disaster Declarations (1953-present)
- Public Assistance funded projects
- Hazard Mitigation Assistance projects
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from enum import Enum

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fema"])


# ========== Enums ==========

class DisasterType(str, Enum):
    MAJOR_DISASTER = "DR"
    EMERGENCY = "EM"
    FIRE_MANAGEMENT = "FM"


class HMAProgram(str, Enum):
    HMGP = "HMGP"  # Hazard Mitigation Grant Program
    PDM = "PDM"    # Pre-Disaster Mitigation
    FMA = "FMA"    # Flood Mitigation Assistance
    RFC = "RFC"    # Repetitive Flood Claims


# ========== Request Models ==========

class DisasterDeclarationsIngestRequest(BaseModel):
    """Request model for disaster declarations ingestion."""
    state: Optional[str] = Field(
        None,
        description="Filter by state code (e.g., 'TX', 'CA', 'FL')",
        examples=["TX"],
        max_length=2
    )
    year: Optional[int] = Field(
        None,
        description="Filter by fiscal year declared",
        examples=[2023],
        ge=1953,
        le=2030
    )
    disaster_type: Optional[DisasterType] = Field(
        None,
        description="Filter by disaster type: DR (Major), EM (Emergency), FM (Fire)"
    )
    max_records: int = Field(
        default=50000,
        description="Maximum records to fetch",
        ge=1,
        le=100000
    )


class PAProjectsIngestRequest(BaseModel):
    """Request model for Public Assistance projects ingestion."""
    state: Optional[str] = Field(
        None,
        description="Filter by state code",
        examples=["FL"],
        max_length=2
    )
    disaster_number: Optional[int] = Field(
        None,
        description="Filter by specific disaster number",
        examples=[4673]
    )
    max_records: int = Field(
        default=50000,
        description="Maximum records to fetch",
        ge=1,
        le=100000
    )


class HMAProjectsIngestRequest(BaseModel):
    """Request model for Hazard Mitigation projects ingestion."""
    state: Optional[str] = Field(
        None,
        description="Filter by state code",
        examples=["CA"],
        max_length=2
    )
    program_area: Optional[HMAProgram] = Field(
        None,
        description="Filter by program: HMGP, PDM, FMA, RFC"
    )
    max_records: int = Field(
        default=50000,
        description="Maximum records to fetch",
        ge=1,
        le=100000
    )


# ========== Endpoints ==========

@router.post("/fema/disasters/ingest")
async def ingest_disaster_declarations(
    request: DisasterDeclarationsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FEMA disaster declarations data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data includes:**
    - All federally declared disasters since 1953
    - Major Disasters (DR), Emergencies (EM), Fire Management (FM)
    - Program eligibility (Individual Assistance, Public Assistance, Hazard Mitigation)
    - Geographic designations (state, county, FIPS codes)
    
    **No API key required** - free public OpenFEMA API
    
    **Examples:**
    - Get all Texas disasters: `state="TX"`
    - Get 2023 disasters only: `year=2023`
    - Get major disasters only: `disaster_type="DR"`
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fema",
        config={
            "dataset": "disaster_declarations",
            "state": request.state,
            "year": request.year,
            "disaster_type": request.disaster_type.value if request.disaster_type else None,
            "max_records": request.max_records,
        },
        message="FEMA disaster declarations ingestion job created",
    )


@router.post("/fema/public-assistance/ingest")
async def ingest_pa_projects(
    request: PAProjectsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FEMA Public Assistance funded projects.
    
    **Data includes:**
    - Project details and descriptions
    - Funding amounts (total obligated, federal share)
    - Damage categories
    - Applicant information
    
    **No API key required** - free public OpenFEMA API
    
    **Note:** This is a large dataset. Use filters for faster ingestion.
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fema",
        config={
            "dataset": "pa_projects",
            "state": request.state,
            "disaster_number": request.disaster_number,
            "max_records": request.max_records,
        },
        message="FEMA Public Assistance projects ingestion job created",
    )


@router.post("/fema/hazard-mitigation/ingest")
async def ingest_hma_projects(
    request: HMAProjectsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FEMA Hazard Mitigation Assistance projects.
    
    **Programs included:**
    - **HMGP**: Hazard Mitigation Grant Program (post-disaster)
    - **PDM**: Pre-Disaster Mitigation
    - **FMA**: Flood Mitigation Assistance
    - **RFC**: Repetitive Flood Claims
    
    **Data includes:**
    - Project identifiers and types
    - Funding amounts
    - Project status
    - Subgrantee information
    
    **No API key required** - free public OpenFEMA API
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fema",
        config={
            "dataset": "hma_projects",
            "state": request.state,
            "program_area": request.program_area.value if request.program_area else None,
            "max_records": request.max_records,
        },
        message="FEMA Hazard Mitigation projects ingestion job created",
    )


@router.get("/fema/datasets")
async def list_fema_datasets():
    """
    List available OpenFEMA datasets and their descriptions.
    """
    return {
        "datasets": [
            {
                "id": "disaster_declarations",
                "name": "Disaster Declarations Summaries",
                "description": "All federally declared disasters since 1953",
                "endpoint": "/fema/disasters/ingest",
                "filters": ["state", "year", "disaster_type"],
                "record_count": "~65,000+"
            },
            {
                "id": "pa_projects",
                "name": "Public Assistance Funded Projects",
                "description": "PA funded project details and funding amounts",
                "endpoint": "/fema/public-assistance/ingest",
                "filters": ["state", "disaster_number"],
                "record_count": "~1,000,000+"
            },
            {
                "id": "hma_projects",
                "name": "Hazard Mitigation Assistance Projects",
                "description": "HMGP, PDM, FMA mitigation projects",
                "endpoint": "/fema/hazard-mitigation/ingest",
                "filters": ["state", "program_area"],
                "record_count": "~50,000+"
            }
        ],
        "disaster_types": {
            "DR": "Major Disaster Declaration",
            "EM": "Emergency Declaration",
            "FM": "Fire Management Assistance"
        },
        "hma_programs": {
            "HMGP": "Hazard Mitigation Grant Program",
            "PDM": "Pre-Disaster Mitigation",
            "FMA": "Flood Mitigation Assistance",
            "RFC": "Repetitive Flood Claims"
        },
        "api_info": {
            "api_key_required": False,
            "documentation": "https://www.fema.gov/about/openfema/api",
            "rate_limit": "Be respectful (~60 req/min recommended)"
        }
    }


