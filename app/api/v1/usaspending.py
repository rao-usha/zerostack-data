"""
USAspending.gov API endpoints.

Provides HTTP endpoints for ingesting and querying federal award spending data.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.job_helpers import create_and_dispatch_job
from app.sources.usaspending import metadata
from app.sources.usaspending.client import (
    NAICS_CODES_OF_INTEREST,
    AWARD_TYPE_CODES,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["USAspending"])


class USASpendingIngestRequest(BaseModel):
    """Request model for USAspending ingestion."""

    naics_codes: Optional[List[str]] = Field(
        None,
        description=(
            "List of NAICS codes to search (defaults to all codes of interest). "
            "Examples: 621 (Healthcare), 622 (Hospitals), 518210 (Data Processing), "
            "517311 (Telecom), 54 (Professional Services), 238 (Construction)"
        ),
    )
    states: Optional[List[str]] = Field(
        None,
        description="List of state codes to filter by (e.g., ['TX', 'CA'])",
    )
    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format (defaults to 3 years ago)",
    )
    end_date: Optional[str] = Field(
        None,
        description="End date in YYYY-MM-DD format (defaults to today)",
    )
    award_type_codes: Optional[List[str]] = Field(
        None,
        description=(
            "Award type codes. Contracts: A,B,C,D. Grants: 02,03,04,05. "
            "Defaults to contracts."
        ),
    )
    min_amount: Optional[float] = Field(
        None,
        description="Minimum award amount in USD",
    )
    max_pages: Optional[int] = Field(
        None,
        description="Maximum number of pages to fetch (100 results/page). "
        "Useful for testing or limiting large queries.",
    )


@router.post("/usaspending/ingest")
async def ingest_usaspending_data(
    request: USASpendingIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest federal award data from USAspending.gov.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Data Includes:**
    - Contract and grant award details
    - Recipient name and UEI
    - Award amounts and total obligations
    - NAICS code classification
    - Awarding agency
    - Place of performance (city, state, zip)
    - Period of performance dates

    **NAICS Codes of Interest:**
    - 621: Healthcare (Ambulatory)
    - 622: Hospitals
    - 518210: Data Processing / Colocation
    - 517311: Telecommunications
    - 54: Professional Services
    - 238: Specialty Construction

    **Source:** USAspending.gov POST /search/spending_by_award/

    **API Key:** Not required
    """
    # Validate date formats if provided
    if request.start_date and not metadata.validate_date_format(request.start_date):
        raise HTTPException(
            status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD"
        )
    if request.end_date and not metadata.validate_date_format(request.end_date):
        raise HTTPException(
            status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD"
        )

    # Validate NAICS codes if provided
    if request.naics_codes:
        for code in request.naics_codes:
            if not code.strip():
                raise HTTPException(
                    status_code=400, detail="NAICS codes cannot be empty strings"
                )

    # Validate state codes if provided
    if request.states:
        for state in request.states:
            if len(state) != 2:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid state code: {state}. Use 2-letter abbreviation.",
                )

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="usaspending",
        config={
            "naics_codes": request.naics_codes,
            "states": request.states,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "award_type_codes": request.award_type_codes,
            "min_amount": request.min_amount,
            "max_pages": request.max_pages,
        },
        message="USAspending award ingestion job created",
    )


@router.get("/usaspending/search")
async def search_usaspending_awards(
    db: Session = Depends(get_db),
    naics_code: Optional[str] = Query(
        None, description="Filter by NAICS code (exact or prefix match)"
    ),
    state: Optional[str] = Query(
        None, description="Filter by state code (e.g., TX)"
    ),
    recipient: Optional[str] = Query(
        None, description="Filter by recipient name (case-insensitive partial match)"
    ),
    min_amount: Optional[float] = Query(
        None, description="Minimum award amount"
    ),
    limit: int = Query(50, ge=1, le=500, description="Results per page"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    sort_by: str = Query(
        "award_amount", description="Column to sort by (default: award_amount)"
    ),
    sort_order: str = Query(
        "desc", description="Sort order: asc or desc"
    ),
):
    """
    Search locally stored USAspending award data.

    Query the `usaspending_awards` table in the local database.
    Data must be ingested first via POST /usaspending/ingest.

    Returns awards matching the specified filters.
    """
    try:
        table_name = metadata.TABLE_NAME

        # Check if table exists
        check_sql = text(
            "SELECT EXISTS ("
            "SELECT FROM information_schema.tables "
            "WHERE table_name = :table_name"
            ")"
        )
        exists = db.execute(check_sql, {"table_name": table_name}).scalar()

        if not exists:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Table {table_name} does not exist. "
                    "Run POST /usaspending/ingest first."
                ),
            )

        # Build query
        conditions = []
        params = {"limit_val": limit, "offset_val": offset}

        if naics_code:
            conditions.append("naics_code LIKE :naics_code")
            params["naics_code"] = f"{naics_code}%"

        if state:
            conditions.append("place_of_performance_state = :state")
            params["state"] = state.upper()

        if recipient:
            conditions.append("recipient_name ILIKE :recipient")
            params["recipient"] = f"%{recipient}%"

        if min_amount is not None:
            conditions.append("award_amount >= :min_amount")
            params["min_amount"] = min_amount

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Validate sort column
        valid_sort_columns = {
            "award_amount", "total_obligation", "award_id",
            "recipient_name", "naics_code", "place_of_performance_state",
            "period_of_performance_start", "period_of_performance_end",
            "ingested_at",
        }
        if sort_by not in valid_sort_columns:
            sort_by = "award_amount"

        order = "DESC" if sort_order.lower() == "desc" else "ASC"

        # Get total count
        count_sql = text(
            f"SELECT COUNT(*) FROM {table_name} {where_clause}"
        )
        total_count = db.execute(count_sql, params).scalar()

        # Get results
        query_sql = text(
            f"SELECT * FROM {table_name} {where_clause} "
            f"ORDER BY {sort_by} {order} NULLS LAST "
            f"LIMIT :limit_val OFFSET :offset_val"
        )
        result = db.execute(query_sql, params)

        rows = [dict(row._mapping) for row in result]

        # Serialize dates and decimals
        for row in rows:
            for key, value in row.items():
                if hasattr(value, "isoformat"):
                    row[key] = value.isoformat()
                elif hasattr(value, "__float__"):
                    row[key] = float(value)

        return {
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "results": rows,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to search USAspending data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usaspending/reference/naics-codes")
async def get_naics_codes_of_interest():
    """
    Get NAICS codes of interest tracked by Nexdata.

    These are the default NAICS codes used when ingesting USAspending data
    without specifying explicit codes.
    """
    return {
        "naics_codes": [
            {"code": code, "description": desc}
            for code, desc in NAICS_CODES_OF_INTEREST.items()
        ]
    }


@router.get("/usaspending/reference/award-types")
async def get_award_type_codes():
    """
    Get available award type codes for USAspending queries.

    Returns award type code groups that can be used in the
    award_type_codes parameter of the ingest endpoint.
    """
    return {"award_type_codes": AWARD_TYPE_CODES}


@router.get("/usaspending/stats")
async def get_usaspending_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested USAspending data.

    Returns counts, totals, and breakdowns by NAICS code and state.
    """
    try:
        table_name = metadata.TABLE_NAME

        # Check if table exists
        check_sql = text(
            "SELECT EXISTS ("
            "SELECT FROM information_schema.tables "
            "WHERE table_name = :table_name"
            ")"
        )
        exists = db.execute(check_sql, {"table_name": table_name}).scalar()

        if not exists:
            return {
                "total_awards": 0,
                "message": "No data ingested yet. Run POST /usaspending/ingest first.",
            }

        # Total count and sum
        summary_sql = text(
            f"SELECT COUNT(*) as total_awards, "
            f"COALESCE(SUM(award_amount), 0) as total_amount, "
            f"COALESCE(AVG(award_amount), 0) as avg_amount, "
            f"MIN(period_of_performance_start) as earliest_start, "
            f"MAX(period_of_performance_end) as latest_end "
            f"FROM {table_name}"
        )
        summary = dict(db.execute(summary_sql).mappings().first())

        # By NAICS code
        naics_sql = text(
            f"SELECT naics_code, naics_description, "
            f"COUNT(*) as count, "
            f"COALESCE(SUM(award_amount), 0) as total_amount "
            f"FROM {table_name} "
            f"WHERE naics_code IS NOT NULL "
            f"GROUP BY naics_code, naics_description "
            f"ORDER BY total_amount DESC "
            f"LIMIT 20"
        )
        naics_breakdown = [
            dict(row._mapping) for row in db.execute(naics_sql)
        ]

        # By state
        state_sql = text(
            f"SELECT place_of_performance_state as state, "
            f"COUNT(*) as count, "
            f"COALESCE(SUM(award_amount), 0) as total_amount "
            f"FROM {table_name} "
            f"WHERE place_of_performance_state IS NOT NULL "
            f"GROUP BY place_of_performance_state "
            f"ORDER BY total_amount DESC "
            f"LIMIT 20"
        )
        state_breakdown = [
            dict(row._mapping) for row in db.execute(state_sql)
        ]

        # Serialize
        for key, value in summary.items():
            if hasattr(value, "isoformat"):
                summary[key] = value.isoformat()
            elif hasattr(value, "__float__"):
                summary[key] = float(value)

        for breakdown in [naics_breakdown, state_breakdown]:
            for row in breakdown:
                for key, value in row.items():
                    if hasattr(value, "__float__"):
                        row[key] = float(value)

        return {
            **summary,
            "by_naics": naics_breakdown,
            "by_state": state_breakdown,
        }

    except Exception as e:
        logger.error(f"Failed to get USAspending stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
