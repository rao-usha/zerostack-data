"""
FCC Broadband & Telecom API endpoints.

Provides HTTP endpoints for ingesting FCC broadband data:
- Broadband coverage by state/county
- Provider availability
- Technology deployment data
- Digital divide metrics

No API key required - public FCC data.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from enum import Enum

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.fcc_broadband import ingest
from app.sources.fcc_broadband.client import US_STATES, STATE_FIPS, TECHNOLOGY_CODES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["FCC Broadband & Telecom"])


# ========== Enums for validation ==========

class SpeedTier(str, Enum):
    SUB_BROADBAND = "sub_broadband"
    BASIC_BROADBAND = "basic_broadband"
    HIGH_SPEED = "high_speed"
    GIGABIT = "gigabit"


class TechnologyType(str, Enum):
    DSL = "10"
    CABLE_DOCSIS_30 = "40"
    CABLE_DOCSIS_31 = "41"
    FIBER = "50"
    SATELLITE = "60"
    FIXED_WIRELESS = "70"
    FIXED_WIRELESS_LICENSED = "71"


# ========== Request Models ==========

class StateIngestRequest(BaseModel):
    """Request model for state-level broadband ingestion."""
    state_codes: List[str] = Field(
        ...,
        description="List of 2-letter state codes (e.g., ['CA', 'NY', 'TX'])",
        examples=[["CA", "NY", "TX"]],
        min_length=1,
        max_length=52
    )
    include_summary: bool = Field(
        default=True,
        description="Generate summary statistics (provider counts, coverage %)"
    )


class AllStatesIngestRequest(BaseModel):
    """Request model for all-states ingestion."""
    include_summary: bool = Field(
        default=True,
        description="Generate summary statistics for each state"
    )


class CountyIngestRequest(BaseModel):
    """Request model for county-level broadband ingestion."""
    county_fips_codes: List[str] = Field(
        ...,
        description="List of 5-digit county FIPS codes (e.g., ['06001', '36061'])",
        examples=[["06001", "36061"]],
        min_length=1
    )
    include_summary: bool = Field(
        default=True,
        description="Generate summary statistics"
    )


# ========== Endpoints ==========

@router.post("/fcc-broadband/state/ingest")
async def ingest_state_broadband(
    request: StateIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FCC broadband coverage data for specific states.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data includes:**
    - Provider availability by state
    - Technology types (Fiber, Cable, DSL, Fixed Wireless, Satellite)
    - Advertised download/upload speeds
    - Service type (residential vs business)
    
    **No API key required** - FCC public data
    
    **Example:**
    ```json
    {
        "state_codes": ["CA", "NY", "TX"],
        "include_summary": true
    }
    ```
    
    **Rate Limits:** Be respectful (~60 requests/min)
    """
    try:
        # Validate state codes
        invalid_states = [s for s in request.state_codes if s.upper() not in US_STATES]
        if invalid_states:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid state codes: {invalid_states}. Use 2-letter codes like CA, NY, TX."
            )
        
        job_config = {
            "dataset": "broadband_coverage",
            "state_codes": [s.upper() for s in request.state_codes],
            "include_summary": request.include_summary,
            "state_count": len(request.state_codes)
        }
        
        job = IngestionJob(
            source="fcc_broadband",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_state_ingestion,
            job.id,
            [s.upper() for s in request.state_codes],
            request.include_summary
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"FCC broadband ingestion job created for {len(request.state_codes)} state(s)",
            "states": [s.upper() for s in request.state_codes],
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create FCC state ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fcc-broadband/all-states/ingest")
async def ingest_all_states_broadband(
    request: AllStatesIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FCC broadband coverage for ALL 50 states + DC.
    
    ⚠️ **This is a large operation** that may take 30-60 minutes.
    
    **Data includes:**
    - Complete U.S. broadband coverage map
    - All providers serving each state
    - Technology deployment by state
    - Coverage statistics and digital divide metrics
    
    **No API key required** - FCC public data
    
    **Expected Results:**
    - ~3,100 counties × ~10 providers/county = ~31,000+ coverage records
    - 51 state-level summary records
    """
    try:
        job_config = {
            "dataset": "broadband_coverage",
            "scope": "all_states",
            "state_count": 51,
            "include_summary": request.include_summary
        }
        
        job = IngestionJob(
            source="fcc_broadband",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_all_states_ingestion,
            job.id,
            request.include_summary
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "FCC broadband ingestion job created for ALL 50 states + DC",
            "warning": "This operation may take 30-60 minutes",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create FCC all-states job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fcc-broadband/county/ingest")
async def ingest_county_broadband(
    request: CountyIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FCC broadband coverage for specific counties.
    
    **Data includes:**
    - Provider availability by county
    - Technology types and speeds
    - Coverage statistics
    
    **County FIPS format:** 5-digit code (state FIPS + county FIPS)
    - Example: "06001" = Alameda County, California (06 = CA, 001 = Alameda)
    - Example: "36061" = New York County, New York (Manhattan)
    
    **No API key required** - FCC public data
    """
    try:
        # Validate FIPS codes format
        invalid_fips = [f for f in request.county_fips_codes if len(f) != 5 or not f.isdigit()]
        if invalid_fips:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid county FIPS codes: {invalid_fips}. Must be 5 digits."
            )
        
        job_config = {
            "dataset": "broadband_coverage",
            "geography": "county",
            "county_fips_codes": request.county_fips_codes,
            "include_summary": request.include_summary
        }
        
        job = IngestionJob(
            source="fcc_broadband",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_county_ingestion,
            job.id,
            request.county_fips_codes,
            request.include_summary
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"FCC county broadband ingestion job created for {len(request.county_fips_codes)} county(ies)",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create FCC county job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ========== Reference Endpoints ==========

@router.get("/fcc-broadband/reference/states")
async def get_state_codes():
    """
    Get all U.S. state codes and FIPS mappings.
    
    Returns all 50 states + DC with their 2-letter codes and FIPS codes.
    """
    state_info = []
    for code in US_STATES:
        fips = STATE_FIPS.get(code, "")
        from app.sources.fcc_broadband.metadata import STATE_NAMES
        name = STATE_NAMES.get(fips, code)
        state_info.append({
            "code": code,
            "fips": fips,
            "name": name
        })
    
    return {
        "count": len(state_info),
        "states": state_info
    }


@router.get("/fcc-broadband/reference/technologies")
async def get_technology_types():
    """
    Get FCC broadband technology type codes and descriptions.
    
    These codes are used in FCC Form 477 filings to classify
    the type of broadband technology deployed.
    """
    return {
        "technologies": [
            {"code": code, "name": name}
            for code, name in TECHNOLOGY_CODES.items()
        ],
        "categories": {
            "wireline": {
                "dsl": ["10", "11", "12", "20"],
                "cable": ["40", "41", "42"],
                "fiber": ["50"],
                "other_copper": ["30"]
            },
            "wireless": {
                "fixed_wireless": ["70", "71", "72"],
                "satellite": ["60"]
            }
        }
    }


@router.get("/fcc-broadband/reference/speed-tiers")
async def get_speed_tiers():
    """
    Get FCC broadband speed tier classifications.
    
    FCC defines broadband as 25 Mbps download / 3 Mbps upload (as of 2024).
    """
    return {
        "fcc_broadband_definition": {
            "download_mbps": 25,
            "upload_mbps": 3,
            "note": "FCC minimum broadband threshold (2024)"
        },
        "speed_tiers": [
            {
                "tier": "sub_broadband",
                "download_range": "< 25 Mbps",
                "description": "Below FCC broadband definition"
            },
            {
                "tier": "basic_broadband",
                "download_range": "25 - 100 Mbps",
                "description": "Meets FCC minimum"
            },
            {
                "tier": "high_speed",
                "download_range": "100 - 1000 Mbps",
                "description": "High-speed broadband"
            },
            {
                "tier": "gigabit",
                "download_range": "1000+ Mbps",
                "description": "Gigabit fiber-class speeds"
            }
        ],
        "proposed_update": {
            "note": "FCC proposed updating definition to 100/20 Mbps",
            "status": "Under consideration"
        }
    }


@router.get("/fcc-broadband/datasets")
async def list_fcc_datasets():
    """
    List available FCC broadband datasets and their descriptions.
    """
    return {
        "datasets": [
            {
                "id": "broadband_coverage",
                "name": "Broadband Coverage by Provider",
                "description": "Detailed provider availability with technology and speeds",
                "endpoint": "/fcc-broadband/state/ingest",
                "geography_levels": ["state", "county"],
                "source": "FCC National Broadband Map"
            },
            {
                "id": "broadband_summary",
                "name": "Broadband Summary Statistics",
                "description": "Aggregated stats: provider count, coverage %, technology availability",
                "generated_from": "broadband_coverage",
                "use_cases": ["Digital divide analysis", "Policy research"]
            }
        ],
        "use_cases": [
            "Digital divide analysis (which areas lack broadband?)",
            "ISP competition analysis (monopoly vs competitive markets)",
            "Real estate investment (broadband = property value)",
            "Policy analysis (universal broadband initiatives)",
            "Network infrastructure planning"
        ],
        "api_info": {
            "api_key_required": False,
            "documentation": "https://broadbandmap.fcc.gov",
            "rate_limit": "Be respectful (~60 req/min recommended)",
            "data_license": "Public domain (U.S. government)"
        }
    }


# ========== Background Task Functions ==========

async def _run_state_ingestion(
    job_id: int,
    state_codes: List[str],
    include_summary: bool
):
    """Run FCC state broadband ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_multiple_states(
            db=db,
            job_id=job_id,
            state_codes=state_codes,
            include_summary=include_summary
        )
    except Exception as e:
        logger.error(f"Background FCC state ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_all_states_ingestion(
    job_id: int,
    include_summary: bool
):
    """Run FCC all-states broadband ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_all_states(
            db=db,
            job_id=job_id,
            include_summary=include_summary
        )
    except Exception as e:
        logger.error(f"Background FCC all-states ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_county_ingestion(
    job_id: int,
    county_fips_codes: List[str],
    include_summary: bool
):
    """Run FCC county broadband ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.models import IngestionJob, JobStatus
    from datetime import datetime
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        total_rows = 0
        failed_counties = []
        
        for county_fips in county_fips_codes:
            try:
                # Create a sub-job ID for each county (using same parent job)
                result = await ingest.ingest_county_coverage(
                    db=db,
                    job_id=job_id,  # Reuse parent job
                    county_fips=county_fips,
                    include_summary=include_summary
                )
                total_rows += result.get("coverage_rows_inserted", 0)
                total_rows += result.get("summary_rows_inserted", 0)
            except Exception as e:
                logger.error(f"Failed county {county_fips}: {e}")
                failed_counties.append(county_fips)
        
        # Update final job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            if failed_counties:
                job.error_message = f"Failed counties: {failed_counties}"
            db.commit()
    
    except Exception as e:
        logger.error(f"Background FCC county ingestion failed: {e}", exc_info=True)
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()
