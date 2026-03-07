"""
CourtListener Bankruptcy Dockets API endpoints.

Provides access to federal bankruptcy court docket data from
CourtListener (Free Law Project). Includes Chapter 7, 11, and 13
bankruptcy filings, case information, and court details.

No API key required for basic access (~100 req/min).
Optional auth token for higher rate limits.

API documentation: https://www.courtlistener.com/api/rest-info/
"""

import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/courtlistener", tags=["CourtListener Bankruptcy"])


# =============================================================================
# REQUEST MODELS
# =============================================================================


class CourtListenerIngestRequest(BaseModel):
    """Request model for CourtListener bankruptcy docket ingestion."""

    query: Optional[str] = Field(
        default=None,
        description="Search query (company name, party, case details)",
    )
    court: Optional[str] = Field(
        default=None,
        description="Bankruptcy court ID (e.g., 'nysb' for NY Southern, 'deb' for Delaware)",
    )
    filed_after: Optional[str] = Field(
        default=None,
        description="Filter cases filed after this date (YYYY-MM-DD)",
    )
    filed_before: Optional[str] = Field(
        default=None,
        description="Filter cases filed before this date (YYYY-MM-DD)",
    )
    max_pages: int = Field(
        default=20,
        description="Maximum pages to fetch (20 results per page)",
        ge=1,
        le=100,
    )


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================


@router.post("/ingest")
async def ingest_courtlistener_dockets(
    request: CourtListenerIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Search and ingest bankruptcy dockets from CourtListener.

    Searches the CourtListener API for bankruptcy filings matching
    the specified criteria and stores results in the
    `courtlistener_dockets` table.

    **No API key required** (rate limited to ~100 req/min).

    **Common Bankruptcy Courts:**
    - `deb` - Delaware Bankruptcy (popular for corporate filings)
    - `nysb` - New York Southern Bankruptcy
    - `txsb` - Texas Southern Bankruptcy
    - `ilnb` - Illinois Northern Bankruptcy

    **Example Request:**
    ```json
    {
        "query": "Chapter 11",
        "court": "deb",
        "filed_after": "2024-01-01",
        "max_pages": 10
    }
    ```
    """
    try:
        if not request.query and not request.court:
            raise HTTPException(
                status_code=400,
                detail="At least one filter is required: query or court",
            )

        return create_and_dispatch_job(
            db,
            background_tasks,
            source="courtlistener",
            config={
                "dataset": "bankruptcy_dockets",
                "query": request.query,
                "court": request.court,
                "filed_after": request.filed_after,
                "filed_before": request.filed_before,
                "max_pages": request.max_pages,
            },
            message="CourtListener bankruptcy docket ingestion job created",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create CourtListener job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SEARCH ENDPOINTS
# =============================================================================


@router.get("/search")
async def search_courtlistener_dockets(
    case_name: Optional[str] = Query(
        default=None, description="Search by case name (partial match)"
    ),
    court_id: Optional[str] = Query(
        default=None, description="Filter by court ID (e.g., 'deb')"
    ),
    chapter: Optional[str] = Query(
        default=None, description="Filter by bankruptcy chapter (7, 11, 13)"
    ),
    filed_after: Optional[str] = Query(
        default=None, description="Cases filed after this date (YYYY-MM-DD)"
    ),
    filed_before: Optional[str] = Query(
        default=None, description="Cases filed before this date (YYYY-MM-DD)"
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search bankruptcy dockets in the local database.

    Queries previously ingested docket records. Supports filtering
    by case name, court, chapter, and filing date range.

    **Example:** `/api/v1/courtlistener/search?chapter=11&court_id=deb&limit=20`
    """
    try:
        conditions = []
        params = {"limit": limit, "offset": offset}

        if case_name:
            conditions.append("case_name ILIKE :case_name")
            params["case_name"] = f"%{case_name}%"
        if court_id:
            conditions.append("court_id = :court_id")
            params["court_id"] = court_id
        if chapter:
            conditions.append("chapter = :chapter")
            params["chapter"] = chapter
        if filed_after:
            conditions.append("date_filed >= :filed_after")
            params["filed_after"] = filed_after
        if filed_before:
            conditions.append("date_filed <= :filed_before")
            params["filed_before"] = filed_before

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = f"SELECT COUNT(*) FROM courtlistener_dockets WHERE {where_clause}"
        total = db.execute(text(count_sql), params).scalar() or 0

        # Data query
        data_sql = f"""
            SELECT docket_id, case_name, case_number, court_id, court_name,
                   date_filed, date_terminated, chapter, nature_of_suit,
                   assigned_to, source_url
            FROM courtlistener_dockets
            WHERE {where_clause}
            ORDER BY date_filed DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
        rows = db.execute(text(data_sql), params).fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": [dict(row._mapping) for row in rows],
        }

    except Exception as e:
        if "does not exist" in str(e):
            return {"total": 0, "limit": limit, "offset": offset, "results": []}
        logger.error(f"CourtListener search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/courts")
async def list_bankruptcy_courts():
    """
    List all recognized bankruptcy court IDs.

    Returns a reference list of court IDs that can be used as
    filters in the search and ingest endpoints.
    """
    from app.sources.courtlistener.client import BANKRUPTCY_COURTS

    return {
        "total_courts": len(BANKRUPTCY_COURTS),
        "courts": BANKRUPTCY_COURTS,
        "popular_courts": {
            "deb": "Delaware Bankruptcy Court",
            "nysb": "New York Southern Bankruptcy Court",
            "txsb": "Texas Southern Bankruptcy Court",
            "ilnb": "Illinois Northern Bankruptcy Court",
            "cacb": "California Central Bankruptcy Court",
            "flsb": "Florida Southern Bankruptcy Court",
        },
    }


@router.get("/stats")
async def get_courtlistener_stats(db: Session = Depends(get_db)):
    """
    Get summary statistics for ingested CourtListener dockets.
    """
    try:
        stats_sql = """
            SELECT
                COUNT(*) as total_dockets,
                COUNT(DISTINCT court_id) as courts_represented,
                COUNT(CASE WHEN chapter = '7' THEN 1 END) as chapter_7_count,
                COUNT(CASE WHEN chapter = '11' THEN 1 END) as chapter_11_count,
                COUNT(CASE WHEN chapter = '13' THEN 1 END) as chapter_13_count,
                MIN(date_filed) as earliest_filing,
                MAX(date_filed) as latest_filing
            FROM courtlistener_dockets
        """
        result = db.execute(text(stats_sql)).fetchone()
        if not result:
            return {"total_dockets": 0}

        return dict(result._mapping)

    except Exception as e:
        if "does not exist" in str(e):
            return {"total_dockets": 0, "message": "Table not yet created. Run ingestion first."}
        logger.error(f"CourtListener stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
