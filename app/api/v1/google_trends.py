"""
Google Trends API endpoints.

Provides HTTP endpoints for ingesting and querying Google Trends
search interest and trending data.
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

router = APIRouter(tags=["google_trends"])


# =============================================================================
# Request / Response Models
# =============================================================================


class GoogleTrendsDailyIngestRequest(BaseModel):
    """Request model for Google Trends daily trends ingestion."""

    geo: str = Field(
        "US",
        description="Geographic region code (e.g., 'US', 'GB', 'DE')",
    )
    date: Optional[str] = Field(
        None,
        description="Date in YYYYMMDD format (defaults to today)",
    )


class GoogleTrendsRegionIngestRequest(BaseModel):
    """Request model for Google Trends interest by region ingestion."""

    keyword: str = Field(
        ...,
        description="Search keyword to analyze (e.g., 'artificial intelligence')",
    )
    geo: str = Field(
        "US",
        description="Geographic region code (e.g., 'US')",
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/google-trends/ingest/daily")
async def ingest_google_trends_daily(
    request: GoogleTrendsDailyIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest daily trending searches from Google Trends.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **geo**: Geographic region code (default: "US")
    - **date**: Date in YYYYMMDD format (defaults to today)

    **Note:** No API key required, but Google Trends aggressively rate-limits
    automated access. Ingestion may fail with 429 errors.
    """
    job_config = {
        "geo": request.geo,
        "date": request.date,
        "mode": "daily_trends",
    }

    job = IngestionJob(
        source="google_trends", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    background_tasks.add_task(
        _run_google_trends_daily_ingestion,
        job.id,
        request.geo,
        request.date,
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"Google Trends daily ingestion job created (geo={request.geo})",
        "config": job_config,
    }


@router.post("/google-trends/ingest/region")
async def ingest_google_trends_region(
    request: GoogleTrendsRegionIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest interest by region for a keyword from Google Trends.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Parameters:**
    - **keyword**: Search keyword to analyze
    - **geo**: Geographic region code (default: "US")

    **Note:** Google Trends requires session tokens for region data.
    This may return empty results. Consider using pytrends as alternative.
    """
    job_config = {
        "keyword": request.keyword,
        "geo": request.geo,
        "mode": "interest_by_region",
    }

    job = IngestionJob(
        source="google_trends", status=JobStatus.PENDING, config=job_config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    background_tasks.add_task(
        _run_google_trends_region_ingestion,
        job.id,
        request.keyword,
        request.geo,
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": (
            f"Google Trends region ingestion job created "
            f"(keyword={request.keyword}, geo={request.geo})"
        ),
        "config": job_config,
    }


@router.get("/google-trends/search")
async def search_google_trends(
    keyword: Optional[str] = Query(None, description="Keyword filter (partial match)"),
    geo: Optional[str] = Query(None, description="Geographic region filter"),
    state: Optional[str] = Query(None, description="State filter"),
    date: Optional[str] = Query(None, description="Date filter"),
    min_interest: Optional[int] = Query(
        None, description="Minimum interest score"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search locally stored Google Trends data.

    Query the ingested google_trends table with flexible filters.
    Data must be ingested first via POST /google-trends/ingest/*.
    """
    try:
        conditions = []
        params = {}

        if keyword:
            conditions.append("LOWER(keyword) LIKE :keyword")
            params["keyword"] = f"%{keyword.lower()}%"
        if geo:
            conditions.append("geo = :geo")
            params["geo"] = geo.upper()
        if state:
            conditions.append("LOWER(state) LIKE :state")
            params["state"] = f"%{state.lower()}%"
        if date:
            conditions.append("date = :date")
            params["date"] = date
        if min_interest is not None:
            conditions.append("interest_score >= :min_interest")
            params["min_interest"] = min_interest

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count query
        count_sql = text(
            f"SELECT COUNT(*) FROM google_trends WHERE {where_clause}"
        )
        total = db.execute(count_sql, params).scalar() or 0

        # Data query with pagination
        offset = (page - 1) * per_page
        params["limit"] = per_page
        params["offset"] = offset

        data_sql = text(
            f"SELECT * FROM google_trends "
            f"WHERE {where_clause} "
            f"ORDER BY interest_score DESC NULLS LAST, date DESC "
            f"LIMIT :limit OFFSET :offset"
        )
        rows = db.execute(data_sql, params).mappings().all()
        trends = [dict(row) for row in rows]

        return {
            "trends": trends,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "google_trends table not found. "
                    "Run POST /google-trends/ingest/daily first."
                ),
            )
        logger.error(f"Google Trends search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/google-trends/stats")
async def get_google_trends_stats(
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for ingested Google Trends data.

    Returns counts by keyword, geo, and top trending terms.
    """
    try:
        total_sql = text("SELECT COUNT(*) FROM google_trends")
        total = db.execute(total_sql).scalar() or 0

        # Top keywords by count
        keyword_sql = text(
            "SELECT keyword, COUNT(*) as cnt "
            "FROM google_trends "
            "GROUP BY keyword ORDER BY cnt DESC LIMIT 20"
        )
        keyword_rows = db.execute(keyword_sql).all()
        top_keywords = {row[0]: row[1] for row in keyword_rows}

        # By geo
        geo_sql = text(
            "SELECT geo, COUNT(*) as cnt "
            "FROM google_trends "
            "GROUP BY geo ORDER BY cnt DESC"
        )
        geo_rows = db.execute(geo_sql).all()
        by_geo = {row[0]: row[1] for row in geo_rows}

        # Top interest scores
        top_sql = text(
            "SELECT keyword, geo, date, interest_score "
            "FROM google_trends "
            "WHERE interest_score IS NOT NULL "
            "ORDER BY interest_score DESC LIMIT 20"
        )
        top_rows = db.execute(top_sql).mappings().all()
        top_trending = [dict(row) for row in top_rows]

        return {
            "total_records": total,
            "top_keywords": top_keywords,
            "by_geo": by_geo,
            "top_trending": top_trending,
        }

    except Exception as e:
        if "does not exist" in str(e):
            raise HTTPException(
                status_code=404,
                detail=(
                    "google_trends table not found. "
                    "Run POST /google-trends/ingest/daily first."
                ),
            )
        logger.error(f"Google Trends stats failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_google_trends_daily_ingestion(
    job_id: int,
    geo: str,
    date: Optional[str],
):
    """Run Google Trends daily ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.google_trends import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_google_trends_daily(
            db=db,
            job_id=job_id,
            geo=geo,
            date=date,
        )
    except Exception as e:
        logger.error(
            f"Background Google Trends daily ingestion failed: {e}",
            exc_info=True,
        )
    finally:
        db.close()


async def _run_google_trends_region_ingestion(
    job_id: int,
    keyword: str,
    geo: str,
):
    """Run Google Trends region ingestion in background."""
    from app.core.database import get_session_factory
    from app.sources.google_trends import ingest

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_google_trends_region(
            db=db,
            job_id=job_id,
            keyword=keyword,
            geo=geo,
        )
    except Exception as e:
        logger.error(
            f"Background Google Trends region ingestion failed: {e}",
            exc_info=True,
        )
    finally:
        db.close()
