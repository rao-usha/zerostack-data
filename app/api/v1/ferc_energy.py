"""
FERC Energy Filings API endpoints.

Provides HTTP endpoints for ingesting and querying state-level
electricity data via the EIA API (FERC proxy).
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

router = APIRouter(tags=["ferc_energy"])


# =============================================================================
# Request / Response Models
# =============================================================================


class FercEnergyIngestRequest(BaseModel):
    """Request model for FERC energy ingestion."""

    period: Optional[str] = Field(
        None,
        description="Year filter (e.g., '2022'). If not provided, ingests all available years.",
    )
    state: Optional[str] = Field(
        None,
        description="Two-letter state code filter (e.g., 'TX'). "
        "If not provided, ingests all states.",
        min_length=2,
        max_length=2,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/ferc-energy/ingest")
async def ingest_ferc_energy(
    request: FercEnergyIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest FERC energy state electricity profile data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **period**: Year filter (e.g., "2022"). Omit to ingest all years.
    - **state**: Two-letter state code (e.g., "TX"). Omit to ingest all states.

    **Note:** Requires EIA_API_KEY environment variable.
    Uses EIA electricity state profiles as a reliable proxy for FERC data.
    """
    job_config = {
        "period": request.period,
        "state": request.state,
    }

    job = IngestionJob(
        source="ferc_energy", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    background_tasks.add_task(
        _run_ferc_energy_ingestion,
        job.id,
        request.period,
        request.state,
    )

    mode_parts = []
    if request.period:
        mode_parts.append(f"period={request.period}")
    if request.state:
        mode_parts.append(f"state={request.state}")
    mode = ", ".join(mode_parts) if mode_parts else "all states/years"

    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"FERC energy ingestion job created ({mode})",
        "config": job_config,
    }


@router.get("/ferc-energy/search")
async def search_ferc_energy(
    state: Optional[str] = Query(None, description="State code filter"),
    period: Optional[str] = Query(None, description="Year/period filter"),
    min_generation: Optional[float] = Query(
        None, description="Minimum total generation (MWh)"
    ),
    max_price: Optional[float] = Query(
        None, description="Maximum average retail price (cents/kWh)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored FERC energy data.

    Query the ingested ferc_energy_filings table with flexible filters.
    Data must be ingested first via POST /ferc-energy/ingest.
    """
    try:
        conditions = []
        params = {}

        if state:
            conditions.append("state = :state")
            params["state"] = state.upper()
        if period:
            conditions.append("period = :period")
            params["period"] = period
        if min_generation is not None:
            conditions.append("total_generation_mwh >= :min_generation")
            params["min_generation"] = min_generation
        if max_price is not None:
            conditions.append("avg_retail_price_cents <= :max_price")
            params["max_price"] = max_price

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM ferc_energy_filings WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM ferc_energy_filings "
            f"WHERE {where_clause} "
            f"ORDER BY total_generation_mwh DESC NULLS LAST "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        filings = [dict(row) for row in rows]

        return {
            "filings": filings,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "ferc_energy_filings table not found. "
                    "Run POST /ferc-energy/ingest first."
                ),
            )
        logger.error(f"FERC energy search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ferc-energy/stats")
async def get_ferc_energy_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested FERC energy data.

    Returns counts by state, top generators, and price comparisons.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM ferc_energy_filings")
        total = db.execute(total_sql).scalar() or 0

        # Count by state
        state_sql = text(
            "SELECT state, COUNT(*) as cnt "
            "FROM ferc_energy_filings "
            "GROUP BY state ORDER BY cnt DESC"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        # Available periods
        period_sql = text(
            "SELECT DISTINCT period FROM ferc_energy_filings ORDER BY period DESC"
        )
        period_rows = db.execute(period_sql).all()
        periods = [row[0] for row in period_rows]

        # Top states by generation (latest period)
        top_gen_sql = text(
            "SELECT state, period, total_generation_mwh, "
            "avg_retail_price_cents, num_utilities "
            "FROM ferc_energy_filings "
            "WHERE total_generation_mwh IS NOT NULL "
            "ORDER BY total_generation_mwh DESC LIMIT 20"
        )
        top_rows = db.execute(top_gen_sql).mappings().all()
        top_generators = [dict(row) for row in top_rows]

        return {
            "total_records": total,
            "states_covered": len(by_state),
            "periods_available": periods,
            "by_state": by_state,
            "top_generators": top_generators,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "ferc_energy_filings table not found. "
                    "Run POST /ferc-energy/ingest first."
                ),
            )
        logger.error(f"FERC energy stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_ferc_energy_ingestion(
    job_id: int,
    period: Optional[str],
    state: Optional[str],
):
    """Run FERC energy ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.ferc_energy import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_ferc_energy(
            db=db,
            job_id=job_id,
            period=period,
            state=state,
        )
    except Exception as e:
        logger.error(
            f"Background FERC energy ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
