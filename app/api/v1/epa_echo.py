"""
EPA ECHO API endpoints.

Provides HTTP endpoints for ingesting and querying EPA ECHO
(Enforcement and Compliance History Online) facility data.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)

router = APIRouter(tags=["epa_echo"])


# =============================================================================
# Request / Response Models
# =============================================================================


class EPAECHOIngestRequest(BaseModel):
    """Request model for EPA ECHO ingestion."""

    state: Optional[str] = Field(
        None,
        description="Two-letter state code (e.g., 'TX', 'CA'). "
        "If not provided, ingests all 50 states + DC.",
        min_length=2,
        max_length=2,
    )
    naics: Optional[str] = Field(
        None, description="NAICS code filter (e.g., '325' for chemicals)"
    )
    sic: Optional[str] = Field(
        None, description="SIC code filter"
    )
    zip_code: Optional[str] = Field(
        None, description="ZIP code filter"
    )
    media: Optional[str] = Field(
        None,
        description="Media program filter: AIR, WATER, RCRA, SDWA, or ALL",
    )


class EPAECHOSearchResponse(BaseModel):
    """Response model for facility search."""

    facilities: List[dict]
    total: int
    page: int
    per_page: int


class EPAECHOFacilityResponse(BaseModel):
    """Response model for single facility detail."""

    facility: Optional[dict]


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/epa-echo/ingest")
async def ingest_epa_echo(
    request: EPAECHOIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest EPA ECHO facility compliance data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **state**: Two-letter state code (e.g., "TX"). Omit to ingest all states.
    - **naics**: NAICS code filter (e.g., "325" for chemical manufacturing)
    - **sic**: SIC code filter
    - **zip_code**: ZIP code filter
    - **media**: Media program (AIR, WATER, RCRA, SDWA, ALL)

    **Note:** No API key required. EPA ECHO is publicly accessible.
    Full national ingestion (all states) may take 30-60 minutes.
    """
    from app.sources.epa_echo import metadata

    # Validate media if provided
    if request.media:
        media_upper = request.media.upper()
        if media_upper not in metadata.VALID_MEDIA_PROGRAMS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid media program: {request.media}. "
                    f"Must be one of: {', '.join(metadata.VALID_MEDIA_PROGRAMS)}"
                ),
            )

    # Validate state if provided
    if request.state:
        from app.sources.epa_echo.client import US_STATES

        if request.state.upper() not in US_STATES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid state code: {request.state}",
            )

    # Create ingestion job
    job_config = {
        "state": request.state,
        "naics": request.naics,
        "sic": request.sic,
        "zip_code": request.zip_code,
        "media": request.media,
    }

    job = IngestionJob(
        source="epa_echo", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_epa_echo_ingestion,
        job.id,
        request.state,
        request.naics,
        request.sic,
        request.zip_code,
        request.media,
    )

    mode = f"state={request.state}" if request.state else "all states"
    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"EPA ECHO ingestion job created ({mode})",
        "config": job_config,
    }


@router.get("/epa-echo/search")
async def search_epa_echo_facilities(
    state: Optional[str] = Query(None, description="State code filter"),
    zip_code: Optional[str] = Query(None, description="ZIP code filter"),
    city: Optional[str] = Query(None, description="City name filter"),
    county: Optional[str] = Query(None, description="County name filter"),
    facility_name: Optional[str] = Query(
        None, description="Facility name (partial match)"
    ),
    compliance_status: Optional[str] = Query(
        None, description="Compliance status filter"
    ),
    naics: Optional[str] = Query(None, description="NAICS code filter"),
    media: Optional[str] = Query(None, description="Media program filter"),
    min_violations: Optional[int] = Query(
        None, description="Minimum violation count"
    ),
    min_penalties: Optional[float] = Query(
        None, description="Minimum penalty amount"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored EPA ECHO facility data.

    Query the ingested epa_echo_facilities table with flexible filters.
    Data must be ingested first via POST /epa-echo/ingest.

    **Filters:**
    - state, zip_code, city, county: Geographic filters
    - facility_name: Partial match on facility name
    - compliance_status: Filter by compliance status
    - naics: Filter by NAICS code (uses JSONB containment)
    - media: Filter by media program (uses JSONB containment)
    - min_violations: Facilities with at least N violations
    - min_penalties: Facilities with penalties >= amount
    """
    try:
        conditions = []
        params = {}

        if state:
            conditions.append("state = :state")
            params["state"] = state.upper()
        if zip_code:
            conditions.append("zip_code = :zip_code")
            params["zip_code"] = zip_code
        if city:
            conditions.append("LOWER(city) LIKE :city")
            params["city"] = f"%{city.lower()}%"
        if county:
            conditions.append("LOWER(county) LIKE :county")
            params["county"] = f"%{county.lower()}%"
        if facility_name:
            conditions.append("LOWER(facility_name) LIKE :facility_name")
            params["facility_name"] = f"%{facility_name.lower()}%"
        if compliance_status:
            conditions.append("compliance_status = :compliance_status")
            params["compliance_status"] = compliance_status
        if naics:
            conditions.append("naics_codes @> :naics::jsonb")
            params["naics"] = f'["{naics}"]'
        if media:
            conditions.append("media_programs @> :media::jsonb")
            params["media"] = f'["{media.upper()}"]'
        if min_violations is not None:
            conditions.append("violation_count >= :min_violations")
            params["min_violations"] = min_violations
        if min_penalties is not None:
            conditions.append("penalty_amount >= :min_penalties")
            params["min_penalties"] = min_penalties

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM epa_echo_facilities WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM epa_echo_facilities "
            f"WHERE {where_clause} "
            f"ORDER BY facility_name "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        facilities = [dict(row) for row in rows]

        return {
            "facilities": facilities,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "epa_echo_facilities table not found. "
                    "Run POST /epa-echo/ingest first."
                ),
            )
        logger.error(f"EPA ECHO search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/epa-echo/facility/{facility_id}")
async def get_epa_echo_facility(
    facility_id: str,
    db: Session = Depends(get_db),
):
    """
    Get detailed information for a specific EPA ECHO facility.

    Returns the locally stored facility record by facility_id (registry_id).

    **Parameters:**
    - **facility_id**: EPA facility registry ID
    """
    try:
        sql = text(
            "SELECT * FROM epa_echo_facilities WHERE facility_id = :fid"
        )
        row = db.execute(sql, {"fid": facility_id}).mappings().first()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Facility {facility_id} not found",
            )

        return {"facility": dict(row)}

    except HTTPException:
        raise
    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "epa_echo_facilities table not found. "
                    "Run POST /epa-echo/ingest first."
                ),
            )
        logger.error(f"EPA ECHO facility lookup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/epa-echo/stats")
async def get_epa_echo_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested EPA ECHO data.

    Returns counts by state, media program, and compliance status.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM epa_echo_facilities")
        total = db.execute(total_sql).scalar() or 0

        # Count by state
        state_sql = text(
            "SELECT state, COUNT(*) as cnt "
            "FROM epa_echo_facilities "
            "GROUP BY state ORDER BY cnt DESC"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        # Count by compliance status
        status_sql = text(
            "SELECT compliance_status, COUNT(*) as cnt "
            "FROM epa_echo_facilities "
            "WHERE compliance_status IS NOT NULL AND compliance_status != '' "
            "GROUP BY compliance_status ORDER BY cnt DESC"
        )
        status_rows = db.execute(status_sql).all()
        by_status = {row[0]: row[1] for row in status_rows}

        # Top violators
        violators_sql = text(
            "SELECT facility_id, facility_name, state, violation_count, penalty_amount "
            "FROM epa_echo_facilities "
            "WHERE violation_count > 0 "
            "ORDER BY violation_count DESC LIMIT 20"
        )
        violator_rows = db.execute(violators_sql).mappings().all()
        top_violators = [dict(row) for row in violator_rows]

        return {
            "total_facilities": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "by_compliance_status": by_status,
            "top_violators": top_violators,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "epa_echo_facilities table not found. "
                    "Run POST /epa-echo/ingest first."
                ),
            )
        logger.error(f"EPA ECHO stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_epa_echo_ingestion(
    job_id: int,
    state: Optional[str],
    naics: Optional[str],
    sic: Optional[str],
    zip_code: Optional[str],
    media: Optional[str],
):
    """Run EPA ECHO ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.epa_echo import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        if state:
            await ingest.ingest_epa_echo_state(
                db=db,
                job_id=job_id,
                state=state,
                naics=naics,
                sic=sic,
                zip_code=zip_code,
                media=media,
            )
        else:
            # Full national ingestion
            await ingest.ingest_epa_echo_all_states(
                db=db,
                job_id=job_id,
                naics=naics,
                media=media,
            )
    except Exception as e:
        logger.error(
            f"Background EPA ECHO ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
