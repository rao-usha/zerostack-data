"""
FFIEC Bank Call Reports API endpoints.

Provides HTTP endpoints for ingesting and querying bank financial data
via the FDIC BankFind Suite API.
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

router = APIRouter(tags=["ffiec_banks"])


# =============================================================================
# Request / Response Models
# =============================================================================


class FfiecBankIngestRequest(BaseModel):
    """Request model for FFIEC bank ingestion."""

    report_date: str = Field(
        "20231231",
        description="Report date in YYYYMMDD format (e.g., '20231231' for Q4 2023)",
    )
    state: Optional[str] = Field(
        None,
        description="State name filter (e.g., 'Texas', 'California'). "
        "If not provided, ingests all states.",
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/ffiec-banks/ingest")
async def ingest_ffiec_banks(
    request: FfiecBankIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest FFIEC bank call report financial data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **report_date**: Report date in YYYYMMDD format (default: "20231231")
    - **state**: State name filter (e.g., "Texas"). Omit to ingest all states.

    **Note:** No API key required. Uses the FDIC BankFind Suite API.
    Full ingestion may take several minutes depending on the number of banks.
    """
    # Create ingestion job
    job_config = {
        "report_date": request.report_date,
        "state": request.state,
    }

    job = IngestionJob(
        source="ffiec_banks", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run ingestion in background
    background_tasks.add_task(
        _run_ffiec_bank_ingestion,
        job.id,
        request.report_date,
        request.state,
    )

    mode = f"state={request.state}" if request.state else "all states"
    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"FFIEC bank ingestion job created ({mode}, date={request.report_date})",
        "config": job_config,
    }


@router.get("/ffiec-banks/search")
async def search_ffiec_banks(
    state: Optional[str] = Query(None, description="State name filter"),
    city: Optional[str] = Query(None, description="City name filter"),
    institution_name: Optional[str] = Query(
        None, description="Institution name (partial match)"
    ),
    report_date: Optional[str] = Query(None, description="Report date filter"),
    min_assets: Optional[float] = Query(
        None, description="Minimum total assets (thousands)"
    ),
    max_assets: Optional[float] = Query(
        None, description="Maximum total assets (thousands)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored FFIEC bank financial data.

    Query the ingested ffiec_bank_calls table with flexible filters.
    Data must be ingested first via POST /ffiec-banks/ingest.
    """
    try:
        conditions = []
        params = {}

        if state:
            conditions.append("LOWER(state) LIKE :state")
            params["state"] = f"%{state.lower()}%"
        if city:
            conditions.append("LOWER(city) LIKE :city")
            params["city"] = f"%{city.lower()}%"
        if institution_name:
            conditions.append("LOWER(institution_name) LIKE :institution_name")
            params["institution_name"] = f"%{institution_name.lower()}%"
        if report_date:
            conditions.append("report_date = :report_date")
            params["report_date"] = report_date
        if min_assets is not None:
            conditions.append("total_assets >= :min_assets")
            params["min_assets"] = min_assets
        if max_assets is not None:
            conditions.append("total_assets <= :max_assets")
            params["max_assets"] = max_assets

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM ffiec_bank_calls WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM ffiec_bank_calls "
            f"WHERE {where_clause} "
            f"ORDER BY total_assets DESC NULLS LAST "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        banks = [dict(row) for row in rows]

        return {
            "banks": banks,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "ffiec_bank_calls table not found. "
                    "Run POST /ffiec-banks/ingest first."
                ),
            )
        logger.error(f"FFIEC bank search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ffiec-banks/stats")
async def get_ffiec_bank_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested FFIEC bank data.

    Returns counts by state, top banks by assets, and aggregate figures.
    """
    try:
        # Total count
        total_sql = text("SELECT COUNT(*) FROM ffiec_bank_calls")
        total = db.execute(total_sql).scalar() or 0

        # Count by state
        state_sql = text(
            "SELECT state, COUNT(*) as cnt "
            "FROM ffiec_bank_calls "
            "GROUP BY state ORDER BY cnt DESC"
        )
        state_rows = db.execute(state_sql).all()
        by_state = {row[0]: row[1] for row in state_rows}

        # Top banks by total assets
        top_sql = text(
            "SELECT cert_id, institution_name, state, total_assets, total_deposits "
            "FROM ffiec_bank_calls "
            "WHERE total_assets IS NOT NULL "
            "ORDER BY total_assets DESC LIMIT 20"
        )
        top_rows = db.execute(top_sql).mappings().all()
        top_banks = [dict(row) for row in top_rows]

        return {
            "total_banks": total,
            "states_covered": len(by_state),
            "by_state": by_state,
            "top_banks_by_assets": top_banks,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "ffiec_bank_calls table not found. "
                    "Run POST /ffiec-banks/ingest first."
                ),
            )
        logger.error(f"FFIEC bank stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_ffiec_bank_ingestion(
    job_id: int,
    report_date: str,
    state: Optional[str],
):
    """Run FFIEC bank ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.ffiec_banks import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_ffiec_banks(
            db=db,
            job_id=job_id,
            report_date=report_date,
            state=state,
        )
    except Exception as e:
        logger.error(
            f"Background FFIEC bank ingestion failed: {e}", exc_info=True
        )
    finally:
        db.close()
