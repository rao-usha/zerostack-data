"""
EPA GHGRP API endpoints.

Provides HTTP endpoints for ingesting and querying EPA Greenhouse Gas
Reporting Program (GHGRP) facility emissions data.
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

router = APIRouter(tags=["epa_ghg"])


# =============================================================================
# Request / Response Models
# =============================================================================


class EpaGhgIngestRequest(BaseModel):
    """Request model for EPA GHGRP ingestion."""

    max_pages: int = Field(
        200,
        description="Maximum number of pages to fetch (safety limit)",
        ge=1,
        le=1000,
    )
    page_size: int = Field(
        1000,
        description="Number of rows per page",
        ge=100,
        le=1000,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/epa-ghg/ingest")
async def ingest_epa_ghg(
    request: EpaGhgIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest EPA GHGRP facility greenhouse gas emissions data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Note:** No API key required. EPA Envirofacts is publicly accessible.
    Full ingestion may take 10-30 minutes depending on data volume.
    """
    # Create ingestion job
    job_config = {
        "max_pages": request.max_pages,
        "page_size": request.page_size,
    }

    job = IngestionJob(
        source="epa_ghg", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_epa_ghg_ingestion,
        job.id,
        request.max_pages,
        request.page_size,
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": "EPA GHGRP ingestion job created",
        "config": job_config,
    }


@router.get("/epa-ghg/search")
async def search_epa_ghg_facilities(
    state: Optional[str] = Query(None, description="State code filter"),
    city: Optional[str] = Query(None, description="City name filter"),
    facility_name: Optional[str] = Query(
        None, description="Facility name (partial match)"
    ),
    industry_type: Optional[str] = Query(
        None, description="Industry type / NAICS filter"
    ),
    reporting_year: Optional[int] = Query(
        None, description="Reporting year filter"
    ),
    min_emissions: Optional[float] = Query(
        None, description="Minimum total reported emissions"
    ),
    parent_company: Optional[str] = Query(
        None, description="Parent company (partial match)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored EPA GHGRP facility emissions data.

    Query the ingested epa_ghg_emissions table with flexible filters.
    Data must be ingested first via POST /epa-ghg/ingest.
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
        if facility_name:
            conditions.append("LOWER(facility_name) LIKE :facility_name")
            params["facility_name"] = f"%{facility_name.lower()}%"
        if industry_type:
            conditions.append("LOWER(industry_type) LIKE :industry_type")
            params["industry_type"] = f"%{industry_type.lower()}%"
        if reporting_year is not None:
            conditions.append("reporting_year = :reporting_year")
            params["reporting_year"] = reporting_year
        if min_emissions is not None:
            conditions.append("total_reported_emissions >= :min_emissions")
            params["min_emissions"] = min_emissions
        if parent_company:
            conditions.append("LOWER(parent_company) LIKE :parent_company")
            params["parent_company"] = f"%{parent_company.lower()}%"

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM epa_ghg_emissions WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM epa_ghg_emissions "
            f"WHERE {where_clause} "
            f"ORDER BY total_reported_emissions DESC NULLS LAST "
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
                    "epa_ghg_emissions table not found. "
                    "Run POST /epa-ghg/ingest first."
                ),
            )
        logger.error(f"EPA GHGRP search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/epa-ghg/stats")
async def get_epa_ghg_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested EPA GHGRP data.

    Returns counts by state, top emitters, and emissions by year.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM epa_ghg_emissions")
        total = db.execute(total_sql).scalar() or 0

        # Count by state
        state_sql = text(
            "SELECT state, COUNT(*) as cnt, "
            "SUM(total_reported_emissions) as total_emissions "
            "FROM epa_ghg_emissions "
            "WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY total_emissions DESC NULLS LAST"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {
            row[0]: {"count": row[1], "total_emissions": float(row[2]) if row[2] else 0}
            for row in state_rows
        }

        # Top emitters
        top_sql = text(
            "SELECT facility_id, facility_name, state, "
            "total_reported_emissions, reporting_year, parent_company "
            "FROM epa_ghg_emissions "
            "WHERE total_reported_emissions IS NOT NULL "
            "ORDER BY total_reported_emissions DESC LIMIT 20"
        )
        top_rows = db.execute(top_sql).mappings().all()
        top_emitters = [dict(row) for row in top_rows]

        return {
            "total_records": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "top_emitters": top_emitters,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "epa_ghg_emissions table not found. "
                    "Run POST /epa-ghg/ingest first."
                ),
            )
        logger.error(f"EPA GHGRP stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_epa_ghg_ingestion(
    job_id: int,
    max_pages: int,
    page_size: int,
):
    """Run EPA GHGRP ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.epa_ghg import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_epa_ghg(
            db=db,
            job_id=job_id,
            max_pages=max_pages,
            page_size=page_size,
        )
    except Exception as e:
        logger.error(
            f"Background EPA GHGRP ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
