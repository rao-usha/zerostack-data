"""
App Store Rankings API endpoints.

Provides access to iOS App Store and Google Play app metrics.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional, List

from app.core.database import get_db
from app.sources.app_stores.client import AppStoreClient

router = APIRouter(prefix="/apps", tags=["App Store Rankings"])


# Request/Response Models
class AndroidAppData(BaseModel):
    """Android app data for manual entry."""

    app_id: str
    app_name: str
    bundle_id: Optional[str] = None
    developer_name: Optional[str] = None
    developer_id: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = 0
    currency: Optional[str] = "USD"
    current_rating: Optional[float] = Field(None, ge=0, le=5)
    rating_count: Optional[int] = None
    current_version: Optional[str] = None
    release_date: Optional[str] = None
    last_updated: Optional[str] = None
    minimum_os_version: Optional[str] = None
    content_rating: Optional[str] = None
    app_icon_url: Optional[str] = None
    app_url: Optional[str] = None
    in_app_purchases: Optional[bool] = False


class RankingRecord(BaseModel):
    """Ranking record for manual entry."""

    app_id: str
    store: str = "ios"
    rank_position: int = Field(..., ge=1)
    rank_type: str = Field("top_free", description="top_free, top_paid, top_grossing")
    category: Optional[str] = None
    country: str = "us"


class CompanyAppLink(BaseModel):
    """Link app to company."""

    company_name: str
    app_id: str
    store: str = "ios"
    relationship: str = Field("owner", description="owner, subsidiary, acquired")


class AppCompareRequest(BaseModel):
    """Request for comparing apps."""

    apps: List[dict] = Field(..., description="List of {app_id, store}")


@router.get("/search")
async def search_ios_apps(
    q: str = Query(..., description="Search query"),
    country: str = Query("us", description="Country code"),
    limit: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Search iOS App Store using iTunes API.

    Returns matching apps with ratings, pricing, and metadata.
    No API key required - uses public iTunes Search API.
    """
    client = AppStoreClient(db)
    results = await client.search_ios_apps(q, country=country, limit=limit)

    return {
        "query": q,
        "country": country,
        "count": len(results),
        "results": results,
    }


@router.get("/ios/{app_id}")
async def get_ios_app(
    app_id: str,
    country: str = Query("us", description="Country code"),
    refresh: bool = Query(False, description="Force refresh from iTunes API"),
    db: Session = Depends(get_db),
):
    """
    Get iOS app details by App ID.

    Fetches from iTunes API and caches in database.
    Use refresh=true to force update from iTunes.
    """
    client = AppStoreClient(db)

    # Check cache first unless refresh requested
    if not refresh:
        cached = client.get_app(app_id, "ios")
        if cached:
            cached["source"] = "cache"
            return cached

    # Fetch from iTunes API
    app = await client.lookup_ios_app(app_id, country=country)

    if not app:
        raise HTTPException(status_code=404, detail=f"iOS app {app_id} not found")

    app["source"] = "itunes_api"
    return app


@router.get("/android/{app_id}")
def get_android_app(app_id: str, db: Session = Depends(get_db)):
    """
    Get Android app from database.

    Note: Google Play data must be added via POST /apps/android endpoint.
    """
    client = AppStoreClient(db)
    app = client.get_app(app_id, "android")

    if not app:
        raise HTTPException(status_code=404, detail=f"Android app {app_id} not found")

    return app


@router.post("/android")
def add_android_app(data: AndroidAppData, db: Session = Depends(get_db)):
    """
    Add or update Android app data.

    Google Play doesn't have a free public API, so data must be
    entered manually or via bulk import.
    """
    client = AppStoreClient(db)
    result = client.upsert_android_app(data.model_dump(exclude_none=True))

    if not result:
        raise HTTPException(status_code=500, detail="Failed to save app")

    return result


@router.get("/{app_id}/ratings")
def get_rating_history(
    app_id: str,
    store: str = Query("ios"),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get rating history for an app.

    Shows rating changes over time as tracked in database.
    """
    client = AppStoreClient(db)
    history = client.get_rating_history(app_id, store=store, limit=limit)

    return {
        "app_id": app_id,
        "store": store,
        "history": history,
    }


@router.get("/{app_id}/rankings")
def get_ranking_history(
    app_id: str,
    store: str = Query("ios"),
    rank_type: Optional[str] = Query(None, description="Filter by rank type"),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get ranking history for an app.

    Shows position changes over time in various charts.
    """
    client = AppStoreClient(db)
    history = client.get_ranking_history(
        app_id, store=store, rank_type=rank_type, limit=limit
    )

    return {
        "app_id": app_id,
        "store": store,
        "history": history,
    }


@router.post("/rankings")
def record_ranking(data: RankingRecord, db: Session = Depends(get_db)):
    """
    Record app ranking position.

    Use this to track chart positions over time.
    """
    client = AppStoreClient(db)
    client.record_ranking(
        app_id=data.app_id,
        store=data.store,
        rank_position=data.rank_position,
        rank_type=data.rank_type,
        category=data.category,
        country=data.country,
    )

    return {
        "status": "recorded",
        "app_id": data.app_id,
        "store": data.store,
        "rank_position": data.rank_position,
        "rank_type": data.rank_type,
    }


@router.post("/company/link")
def link_app_to_company(data: CompanyAppLink, db: Session = Depends(get_db)):
    """
    Link an app to a company.

    Associates apps with portfolio companies for tracking.
    """
    client = AppStoreClient(db)
    result = client.link_app_to_company(
        company_name=data.company_name,
        app_id=data.app_id,
        store=data.store,
        relationship=data.relationship,
    )

    return result


@router.get("/company/{company_name}")
def get_company_apps(company_name: str, db: Session = Depends(get_db)):
    """
    Get all apps linked to a company.

    Returns the company's mobile app portfolio.
    """
    client = AppStoreClient(db)
    result = client.get_company_apps(company_name)

    return result


@router.get("/developer/{developer_name}")
def search_by_developer(developer_name: str, db: Session = Depends(get_db)):
    """
    Search apps by developer name.

    Searches cached apps in database.
    """
    client = AppStoreClient(db)
    apps = client.search_apps_by_developer(developer_name)

    return {
        "developer": developer_name,
        "app_count": len(apps),
        "apps": apps,
    }


@router.get("/top")
def get_top_apps(
    store: str = Query("ios"),
    category: Optional[str] = Query(None, description="Filter by category"),
    sort_by: str = Query("rating_count", pattern="^(rating_count|current_rating)$"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get top apps from database.

    Returns apps sorted by rating count or rating value.
    """
    client = AppStoreClient(db)
    apps = client.get_top_apps(
        store=store,
        category=category,
        sort_by=sort_by,
        limit=limit,
    )

    return {
        "store": store,
        "category": category,
        "sort_by": sort_by,
        "count": len(apps),
        "apps": apps,
    }


@router.post("/compare")
def compare_apps(request: AppCompareRequest, db: Session = Depends(get_db)):
    """
    Compare multiple apps side by side.

    Accepts list of {app_id, store} objects.
    """
    if len(request.apps) < 2:
        raise HTTPException(
            status_code=400, detail="At least 2 apps required for comparison"
        )

    if len(request.apps) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 apps per comparison")

    client = AppStoreClient(db)
    result = client.compare_apps(request.apps)

    return result


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """
    Get app database statistics.

    Returns counts and aggregates for tracked apps.
    """
    client = AppStoreClient(db)
    return client.get_stats()


@router.delete("/{app_id}")
def delete_app(app_id: str, store: str = Query("ios"), db: Session = Depends(get_db)):
    """
    Delete an app from the database.
    """
    from sqlalchemy import text

    # Check if app exists
    check_query = text("""
        SELECT id FROM app_store_apps
        WHERE app_id = :app_id AND store = :store
    """)
    result = db.execute(check_query, {"app_id": app_id, "store": store})
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"App {app_id} ({store}) not found")

    # Delete app and related data
    delete_ratings = text("""
        DELETE FROM app_store_rating_history
        WHERE app_id = :app_id AND store = :store
    """)
    delete_rankings = text("""
        DELETE FROM app_store_rankings
        WHERE app_id = :app_id AND store = :store
    """)
    delete_links = text("""
        DELETE FROM company_app_portfolios
        WHERE app_id = :app_id AND store = :store
    """)
    delete_app = text("""
        DELETE FROM app_store_apps
        WHERE app_id = :app_id AND store = :store
    """)

    db.execute(delete_ratings, {"app_id": app_id, "store": store})
    db.execute(delete_rankings, {"app_id": app_id, "store": store})
    db.execute(delete_links, {"app_id": app_id, "store": store})
    db.execute(delete_app, {"app_id": app_id, "store": store})
    db.commit()

    return {"status": "deleted", "app_id": app_id, "store": store}
