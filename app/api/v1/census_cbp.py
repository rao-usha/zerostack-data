"""
Census County Business Patterns (CBP) API endpoints.

Provides HTTP endpoints for ingesting and querying Census CBP
establishment, employment, and payroll data by state and NAICS industry.
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

router = APIRouter(tags=["census_cbp"])


# =============================================================================
# Request / Response Models
# =============================================================================


class CensusCBPIngestRequest(BaseModel):
    """Request model for Census CBP ingestion."""

    year: int = Field(
        2022,
        description="Data year (e.g., 2022)",
        ge=2000,
        le=2030,
    )
    state_fips: Optional[str] = Field(
        None,
        description="State FIPS code (e.g., '06' for California). "
        "If not provided, ingests all states.",
    )
    naics_code: Optional[str] = Field(
        None,
        description="NAICS code filter (e.g., '31-33' for manufacturing). "
        "If not provided, ingests all industries.",
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/census-cbp/ingest")
async def ingest_census_cbp(
    request: CensusCBPIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census County Business Patterns data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **year**: Data year (default: 2022)
    - **state_fips**: State FIPS code (e.g., "06"). Omit to ingest all states.
    - **naics_code**: NAICS code filter. Omit for all industries.

    **Note:** No API key required. Census CBP data is publicly accessible.
    Set CENSUS_SURVEY_API_KEY for higher rate limits.
    """
    # Create ingestion job
    job_config = {
        "year": request.year,
        "state_fips": request.state_fips,
        "naics_code": request.naics_code,
    }

    job = IngestionJob(
        source="census_cbp", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_census_cbp_ingestion,
        job.id,
        request.year,
        request.state_fips,
        request.naics_code,
    )

    mode = f"state_fips={request.state_fips}" if request.state_fips else "all states"
    naics_info = f", naics={request.naics_code}" if request.naics_code else ""
    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"Census CBP ingestion job created ({mode}{naics_info}, year={request.year})",
        "config": job_config,
    }


@router.get("/census-cbp/search")
async def search_census_cbp(
    state_fips: Optional[str] = Query(None, description="State FIPS code filter"),
    state_abbr: Optional[str] = Query(None, description="State abbreviation filter"),
    naics_code: Optional[str] = Query(None, description="NAICS code filter"),
    naics_description: Optional[str] = Query(
        None, description="NAICS description (partial match)"
    ),
    year: Optional[int] = Query(None, description="Year filter"),
    min_establishments: Optional[int] = Query(
        None, description="Minimum establishment count"
    ),
    min_employees: Optional[int] = Query(
        None, description="Minimum employee count"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored Census CBP data.

    Query the ingested census_business_patterns table with flexible filters.
    Data must be ingested first via POST /census-cbp/ingest.
    """
    try:
        conditions = []
        params = {}

        if state_fips:
            conditions.append("state_fips = :state_fips")
            params["state_fips"] = state_fips
        if state_abbr:
            conditions.append("state_abbr = :state_abbr")
            params["state_abbr"] = state_abbr.upper()
        if naics_code:
            conditions.append("naics_code = :naics_code")
            params["naics_code"] = naics_code
        if naics_description:
            conditions.append("LOWER(naics_description) LIKE :naics_description")
            params["naics_description"] = f"%{naics_description.lower()}%"
        if year is not None:
            conditions.append("year = :year")
            params["year"] = year
        if min_establishments is not None:
            conditions.append("establishments >= :min_establishments")
            params["min_establishments"] = min_establishments
        if min_employees is not None:
            conditions.append("employees >= :min_employees")
            params["min_employees"] = min_employees

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM census_business_patterns WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM census_business_patterns "
            f"WHERE {where_clause} "
            f"ORDER BY state_fips, naics_code "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        records = [dict(row) for row in rows]

        return {
            "records": records,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "census_business_patterns table not found. "
                    "Run POST /census-cbp/ingest first."
                ),
            )
        logger.error(f"Census CBP search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/census-cbp/stats")
async def get_census_cbp_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested Census CBP data.

    Returns counts by state, top industries, and year coverage.
    """
    try:
        total_sql = text("SELECT COUNT(*) FROM census_business_patterns")
        total = db.execute(total_sql).scalar() or 0

        state_sql = text(
            "SELECT state_abbr, COUNT(*) as cnt "
            "FROM census_business_patterns "
            "WHERE state_abbr IS NOT NULL "
            "GROUP BY state_abbr ORDER BY state_abbr"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        top_industries_sql = text(
            "SELECT naics_code, naics_description, "
            "SUM(establishments) as total_estab, "
            "SUM(employees) as total_emp "
            "FROM census_business_patterns "
            "WHERE naics_code != '' AND employees IS NOT NULL "
            "GROUP BY naics_code, naics_description "
            "ORDER BY total_emp DESC NULLS LAST "
            "LIMIT 20"
        )
        industry_rows = db.execute(top_industries_sql).mappings().all()
        top_industries = [dict(row) for row in industry_rows]

        year_sql = text(
            "SELECT DISTINCT year FROM census_business_patterns ORDER BY year"
        )
        year_rows = db.execute(year_sql).all()
        years = [row[0] for row in year_rows]

        return {
            "total_records": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "top_industries": top_industries,
            "years": years,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "census_business_patterns table not found. "
                    "Run POST /census-cbp/ingest first."
                ),
            )
        logger.error(f"Census CBP stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_census_cbp_ingestion(
    job_id: int,
    year: int,
    state_fips: Optional[str],
    naics_code: Optional[str],
):
    """Run Census CBP ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.census_cbp import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_census_cbp(
            db=db,
            job_id=job_id,
            year=year,
            state_fips=state_fips,
            naics_code=naics_code,
        )
    except Exception as e:
        logger.error(
            f"Background Census CBP ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
