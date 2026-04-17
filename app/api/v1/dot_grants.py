"""
DOT Infrastructure Grants API endpoints.

Provides HTTP endpoints for ingesting and querying Department of
Transportation grant spending data from USAspending.gov.
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

router = APIRouter(tags=["dot_grants"])


# =============================================================================
# Request / Response Models
# =============================================================================


class DotGrantsIngestRequest(BaseModel):
    """Request model for DOT grants ingestion."""

    agency: str = Field(
        "Department of Transportation",
        description="Top-tier awarding agency name",
    )
    start_year: int = Field(
        2021,
        description="First fiscal year to fetch",
        ge=2000,
        le=2030,
    )
    end_year: int = Field(
        2026,
        description="Last fiscal year to fetch (inclusive)",
        ge=2000,
        le=2030,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/dot-grants/ingest")
async def ingest_dot_grants(
    request: DotGrantsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest DOT infrastructure grant spending by state.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    Fetches state-level spending data from USAspending.gov for each
    fiscal year in the specified range.

    **Note:** No API key required. USAspending is publicly accessible.
    Ingestion typically completes in 2-5 minutes.
    """
    if request.start_year > request.end_year:
        raise HTTPException(
            status_code=400,
            detail="start_year must be <= end_year",
        )

    # Create ingestion job
    job_config = {
        "agency": request.agency,
        "start_year": request.start_year,
        "end_year": request.end_year,
    }

    job = IngestionJob(
        source="dot_grants", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_dot_grants_ingestion,
        job.id,
        request.agency,
        request.start_year,
        request.end_year,
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": (
            f"DOT grants ingestion job created "
            f"({request.agency}, FY{request.start_year}-{request.end_year})"
        ),
        "config": job_config,
    }


@router.get("/dot-grants/search")
async def search_dot_grants(
    state: Optional[str] = Query(None, description="State code filter"),
    agency: Optional[str] = Query(None, description="Agency name filter"),
    fiscal_year: Optional[int] = Query(None, description="Fiscal year filter"),
    min_amount: Optional[float] = Query(
        None, description="Minimum aggregated amount"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored DOT infrastructure grant data.

    Query the ingested dot_infra_grants table with flexible filters.
    Data must be ingested first via POST /dot-grants/ingest.
    """
    try:
        conditions = []
        params = {}

        if state:
            conditions.append("state = :state")
            params["state"] = state.upper()
        if agency:
            conditions.append("LOWER(agency) LIKE :agency")
            params["agency"] = f"%{agency.lower()}%"
        if fiscal_year is not None:
            conditions.append("fiscal_year = :fiscal_year")
            params["fiscal_year"] = fiscal_year
        if min_amount is not None:
            conditions.append("aggregated_amount >= :min_amount")
            params["min_amount"] = min_amount

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM dot_infra_grants WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM dot_infra_grants "
            f"WHERE {where_clause} "
            f"ORDER BY aggregated_amount DESC NULLS LAST "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        grants = [dict(row) for row in rows]

        return {
            "grants": grants,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "dot_infra_grants table not found. "
                    "Run POST /dot-grants/ingest first."
                ),
            )
        logger.error(f"DOT grants search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dot-grants/stats")
async def get_dot_grants_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested DOT grant data.

    Returns totals by state, by year, and top-funded states.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM dot_infra_grants")
        total = db.execute(total_sql).scalar() or 0

        # Total spending
        spend_sql = text(
            "SELECT SUM(aggregated_amount) FROM dot_infra_grants"
        )
        total_spending = db.execute(spend_sql).scalar() or 0

        # By state (total across years)
        state_sql = text(
            "SELECT state, SUM(aggregated_amount) as total_amount, "
            "SUM(transaction_count) as total_transactions "
            "FROM dot_infra_grants "
            "WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY total_amount DESC NULLS LAST"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {
            row[0]: {
                "total_amount": float(row[1]) if row[1] else 0,
                "total_transactions": int(row[2]) if row[2] else 0,
            }
            for row in state_rows
        }

        # By year
        year_sql = text(
            "SELECT fiscal_year, SUM(aggregated_amount) as total_amount, "
            "COUNT(*) as state_count "
            "FROM dot_infra_grants "
            "WHERE fiscal_year IS NOT NULL "
            "GROUP BY fiscal_year ORDER BY fiscal_year"
        )
        year_rows = db.execute(year_sql).all()
        by_year = {
            row[0]: {
                "total_amount": float(row[1]) if row[1] else 0,
                "state_count": row[2],
            }
            for row in year_rows
        }

        return {
            "total_records": total,
            "total_spending": float(total_spending),
            "states_covered": len(by_state),
            "by_state": by_state,
            "by_fiscal_year": by_year,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "dot_infra_grants table not found. "
                    "Run POST /dot-grants/ingest first."
                ),
            )
        logger.error(f"DOT grants stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_dot_grants_ingestion(
    job_id: int,
    agency: str,
    start_year: int,
    end_year: int,
):
    """Run DOT grants ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.dot_grants import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_dot_grants(
            db=db,
            job_id=job_id,
            agency=agency,
            start_year=start_year,
            end_year=end_year,
        )
    except Exception as e:
        logger.error(
            f"Background DOT grants ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
