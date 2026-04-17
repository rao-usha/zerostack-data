"""
Census Business Formation Statistics (BFS) API endpoints.

Provides HTTP endpoints for ingesting and querying Census BFS
business application data by state and time period.
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

router = APIRouter(tags=["census_bfs"])


# =============================================================================
# Request / Response Models
# =============================================================================


class CensusBFSIngestRequest(BaseModel):
    """Request model for Census BFS ingestion."""

    time_from: str = Field(
        "2020",
        description="Start year for time series (e.g., '2020')",
    )
    state_fips: Optional[str] = Field(
        None,
        description="State FIPS code (e.g., '06' for California). "
        "If not provided, ingests all states.",
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/census-bfs/ingest")
async def ingest_census_bfs(
    request: CensusBFSIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census Business Formation Statistics data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **time_from**: Start year for time series (default: "2020")
    - **state_fips**: State FIPS code (e.g., "06"). Omit to ingest all states.

    **Note:** No API key required. Census BFS data is publicly accessible.
    Set CENSUS_SURVEY_API_KEY for higher rate limits.
    """
    # Create ingestion job
    job_config = {
        "time_from": request.time_from,
        "state_fips": request.state_fips,
    }

    job = IngestionJob(
        source="census_bfs", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_census_bfs_ingestion,
        job.id,
        request.time_from,
        request.state_fips,
    )

    mode = f"state_fips={request.state_fips}" if request.state_fips else "all states"
    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"Census BFS ingestion job created ({mode}, from {request.time_from})",
        "config": job_config,
    }


@router.get("/census-bfs/search")
async def search_census_bfs(
    state_fips: Optional[str] = Query(None, description="State FIPS code filter"),
    state_abbr: Optional[str] = Query(None, description="State abbreviation filter"),
    time_period: Optional[str] = Query(None, description="Time period filter"),
    min_applications: Optional[int] = Query(
        None, description="Minimum total business applications"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored Census BFS data.

    Query the ingested census_bfs table with flexible filters.
    Data must be ingested first via POST /census-bfs/ingest.
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
        if time_period:
            conditions.append("time_period = :time_period")
            params["time_period"] = time_period
        if min_applications is not None:
            conditions.append("business_applications >= :min_applications")
            params["min_applications"] = min_applications

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM census_bfs WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM census_bfs "
            f"WHERE {where_clause} "
            f"ORDER BY state_fips, time_period "
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
                    "census_bfs table not found. "
                    "Run POST /census-bfs/ingest first."
                ),
            )
        logger.error(f"Census BFS search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/census-bfs/stats")
async def get_census_bfs_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested Census BFS data.

    Returns counts by state and time period coverage.
    """
    try:
        total_sql = text("SELECT COUNT(*) FROM census_bfs")
        total = db.execute(total_sql).scalar() or 0

        state_sql = text(
            "SELECT state_abbr, COUNT(*) as cnt "
            "FROM census_bfs "
            "WHERE state_abbr IS NOT NULL "
            "GROUP BY state_abbr ORDER BY state_abbr"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        time_sql = text(
            "SELECT MIN(time_period), MAX(time_period) FROM census_bfs"
        )
        time_row = db.execute(time_sql).first()
        time_range = {
            "earliest": time_row[0] if time_row else None,
            "latest": time_row[1] if time_row else None,
        }

        return {
            "total_records": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "time_range": time_range,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "census_bfs table not found. "
                    "Run POST /census-bfs/ingest first."
                ),
            )
        logger.error(f"Census BFS stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_census_bfs_ingestion(
    job_id: int,
    time_from: str,
    state_fips: Optional[str],
):
    """Run Census BFS ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.census_bfs import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_census_bfs(
            db=db,
            job_id=job_id,
            time_from=time_from,
            state_fips=state_fips,
        )
    except Exception as e:
        logger.error(
            f"Background Census BFS ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
