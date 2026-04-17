"""
CMS Hospital Provider Data API endpoints.

Provides HTTP endpoints for ingesting and querying CMS hospital
quality ratings and provider information.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)

router = APIRouter(tags=["cms_hospitals"])


# =============================================================================
# Request / Response Models
# =============================================================================


class CmsHospitalIngestRequest(BaseModel):
    """Request model for CMS hospital ingestion."""

    max_pages: int = Field(
        50,
        description="Maximum number of pages to fetch (safety limit)",
        ge=1,
        le=200,
    )
    page_size: int = Field(
        500,
        description="Number of records per page",
        ge=100,
        le=500,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/cms-hospitals/ingest")
async def ingest_cms_hospitals(
    request: CmsHospitalIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest CMS hospital provider quality data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Note:** No API key required. CMS Provider Data is publicly accessible.
    Full ingestion typically completes in 5-15 minutes.
    """
    # Create ingestion job
    job_config = {
        "max_pages": request.max_pages,
        "page_size": request.page_size,
    }

    job = IngestionJob(
        source="cms_hospitals", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_cms_hospital_ingestion,
        job.id,
        request.max_pages,
        request.page_size,
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": "CMS hospital ingestion job created",
        "config": job_config,
    }


@router.get("/cms-hospitals/search")
async def search_cms_hospitals(
    state: Optional[str] = Query(None, description="State code filter"),
    city: Optional[str] = Query(None, description="City name filter"),
    county: Optional[str] = Query(None, description="County name filter"),
    facility_name: Optional[str] = Query(
        None, description="Hospital name (partial match)"
    ),
    hospital_type: Optional[str] = Query(
        None, description="Hospital type filter"
    ),
    ownership: Optional[str] = Query(
        None, description="Hospital ownership filter (partial match)"
    ),
    emergency_services: Optional[bool] = Query(
        None, description="Filter by emergency services availability"
    ),
    min_rating: Optional[int] = Query(
        None, description="Minimum overall quality rating (1-5)", ge=1, le=5
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored CMS hospital provider data.

    Query the ingested cms_hospitals table with flexible filters.
    Data must be ingested first via POST /cms-hospitals/ingest.
    """
    try:
        conditions = []
        params = {}

        if state:
            conditions.append("state = :state")
            params["state"] = state.upper()
        if city:
            conditions.append("LOWER(city) LIKE :city")
            params["city"] = f"%{city.lower()}%"
        if county:
            conditions.append("LOWER(county) LIKE :county")
            params["county"] = f"%{county.lower()}%"
        if facility_name:
            conditions.append("LOWER(facility_name) LIKE :facility_name")
            params["facility_name"] = f"%{facility_name.lower()}%"
        if hospital_type:
            conditions.append("LOWER(hospital_type) LIKE :hospital_type")
            params["hospital_type"] = f"%{hospital_type.lower()}%"
        if ownership:
            conditions.append("LOWER(ownership) LIKE :ownership")
            params["ownership"] = f"%{ownership.lower()}%"
        if emergency_services is not None:
            conditions.append("emergency_services = :emergency_services")
            params["emergency_services"] = emergency_services
        if min_rating is not None:
            conditions.append("overall_rating >= :min_rating")
            params["min_rating"] = min_rating

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM cms_hospitals WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM cms_hospitals "
            f"WHERE {where_clause} "
            f"ORDER BY facility_name "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        hospitals = [dict(row) for row in rows]

        return {
            "hospitals": hospitals,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "cms_hospitals table not found. "
                    "Run POST /cms-hospitals/ingest first."
                ),
            )
        logger.error(f"CMS hospital search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cms-hospitals/stats")
async def get_cms_hospital_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested CMS hospital data.

    Returns counts by state, hospital type, ownership, and rating distribution.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM cms_hospitals")
        total = db.execute(total_sql).scalar() or 0

        # Count by state
        state_sql = text(
            "SELECT state, COUNT(*) as cnt "
            "FROM cms_hospitals "
            "WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY cnt DESC"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        # Count by hospital type
        type_sql = text(
            "SELECT hospital_type, COUNT(*) as cnt "
            "FROM cms_hospitals "
            "WHERE hospital_type IS NOT NULL "
            "GROUP BY hospital_type ORDER BY cnt DESC"
        )
        type_rows = db.execute(type_sql).all()
        by_type = {row[0]: row[1] for row in type_rows}

        # Rating distribution
        rating_sql = text(
            "SELECT overall_rating, COUNT(*) as cnt "
            "FROM cms_hospitals "
            "WHERE overall_rating IS NOT NULL "
            "GROUP BY overall_rating ORDER BY overall_rating"
        )
        rating_rows = db.execute(rating_sql).all()
        by_rating = {row[0]: row[1] for row in rating_rows}

        return {
            "total_hospitals": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "by_hospital_type": by_type,
            "by_overall_rating": by_rating,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "cms_hospitals table not found. "
                    "Run POST /cms-hospitals/ingest first."
                ),
            )
        logger.error(f"CMS hospital stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_cms_hospital_ingestion(
    job_id: int,
    max_pages: int,
    page_size: int,
):
    """Run CMS hospital ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.cms_hospitals import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_cms_hospitals(
            db=db,
            job_id=job_id,
            max_pages=max_pages,
            page_size=page_size,
        )
    except Exception as e:
        logger.error(
            f"Background CMS hospital ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
