"""
App Stores API endpoints.

Provides access to iOS and Android app data, rankings, and portfolios.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.app_stores.client import AppStoreClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/app-stores", tags=["App Stores"])


class IngestRequest(BaseModel):
    company_name: Optional[str] = Field(None, description="Scope to a specific company")
    search_query: Optional[str] = Field(None, description="Search iTunes for new apps")
    limit: Optional[int] = Field(None, ge=1, le=200, description="Max apps to process")


class AppLinkRequest(BaseModel):
    company_name: str
    app_id: str
    store: str = "ios"
    relationship: str = "owner"


@router.post("/ingest")
async def ingest_app_data(
    request: IngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Trigger app store data ingestion.

    Updates tracked apps and optionally searches for new ones.
    """
    from app.sources.app_stores.ingest import AppStoreIngestor

    job = IngestionJob(source="app_stores", status=JobStatus.PENDING, config={})
    db.add(job)
    db.commit()
    db.refresh(job)

    async def _run(job_id, company_name, search_query, limit):
        ingestor = AppStoreIngestor(db)
        await ingestor.run(job_id, company_name, search_query, limit)

    background_tasks.add_task(
        _run, job.id, request.company_name, request.search_query, request.limit
    )

    return {"status": "started", "job_id": job.id}


@router.get("/search")
async def search_ios_apps(
    q: str = Query(..., min_length=1, description="Search query"),
    country: str = Query("us", max_length=2),
    limit: int = Query(25, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Search the iTunes App Store for apps."""
    client = AppStoreClient(db)
    results = await client.search_ios_apps(q, country=country, limit=limit)
    return {"results": results, "count": len(results)}


@router.get("/app/{app_id}")
def get_app(app_id: str, store: str = Query("ios"), db: Session = Depends(get_db)):
    """Get stored app data."""
    client = AppStoreClient(db)
    result = client.get_app(app_id, store=store)
    if not result:
        raise HTTPException(status_code=404, detail=f"App '{app_id}' not found")
    return result


@router.get("/app/{app_id}/ratings")
def get_rating_history(
    app_id: str,
    store: str = Query("ios"),
    limit: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Get historical rating data for an app."""
    client = AppStoreClient(db)
    return client.get_rating_history(app_id, store=store, limit=limit)


@router.get("/app/{app_id}/rankings")
def get_ranking_history(
    app_id: str,
    store: str = Query("ios"),
    rank_type: Optional[str] = Query(None),
    limit: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Get historical ranking data for an app."""
    client = AppStoreClient(db)
    return client.get_ranking_history(
        app_id, store=store, rank_type=rank_type, limit=limit
    )


@router.post("/link")
def link_app_to_company(request: AppLinkRequest, db: Session = Depends(get_db)):
    """Link an app to a company."""
    client = AppStoreClient(db)
    return client.link_app_to_company(
        request.company_name, request.app_id, request.store, request.relationship
    )


@router.get("/company/{company_name}/apps")
def get_company_apps(company_name: str, db: Session = Depends(get_db)):
    """Get all apps linked to a company."""
    client = AppStoreClient(db)
    return client.get_company_apps(company_name)


@router.get("/developer/{developer_name}")
def search_by_developer(developer_name: str, db: Session = Depends(get_db)):
    """Search apps by developer name."""
    client = AppStoreClient(db)
    return client.search_apps_by_developer(developer_name)


@router.get("/top")
def get_top_apps(
    store: str = Query("ios"),
    category: Optional[str] = Query(None),
    sort_by: str = Query("rating_count"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get top-ranked apps."""
    client = AppStoreClient(db)
    return client.get_top_apps(
        store=store, category=category, sort_by=sort_by, limit=limit
    )


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get app store database statistics."""
    client = AppStoreClient(db)
    return client.get_stats()
