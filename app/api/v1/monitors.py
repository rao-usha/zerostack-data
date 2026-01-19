"""
News Monitor API endpoints.

Provides endpoints for:
- Watch list management (add, list, remove, update)
- Personalized news feed
- Digest generation
- Breaking alerts
- Statistics
"""

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.agents.news_monitor import NewsMonitor

router = APIRouter(prefix="/monitors/news", tags=["News Monitor"])


# ============================================================================
# Request/Response Models
# ============================================================================

class WatchItemCreate(BaseModel):
    """Request to add a watch item."""
    watch_type: str = Field(..., description="Type: company, investor, sector, keyword")
    watch_value: str = Field(..., description="Value to watch")
    event_types: Optional[List[str]] = Field(None, description="Filter by event types")
    min_relevance: float = Field(0.5, ge=0, le=1, description="Minimum relevance score")
    alert_enabled: bool = Field(True, description="Enable breaking alerts")
    digest_enabled: bool = Field(True, description="Include in digests")


class WatchItemUpdate(BaseModel):
    """Request to update a watch item."""
    event_types: Optional[List[str]] = None
    min_relevance: Optional[float] = Field(None, ge=0, le=1)
    alert_enabled: Optional[bool] = None
    digest_enabled: Optional[bool] = None


class WatchItemResponse(BaseModel):
    """Watch item response."""
    id: int
    watch_type: str
    watch_value: str
    event_types: Optional[List[str]]
    min_relevance: float
    alert_enabled: bool
    digest_enabled: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class MatchedWatch(BaseModel):
    """Matched watch info in feed response."""
    id: int
    type: str
    value: str


class FeedItem(BaseModel):
    """News feed item."""
    id: int
    title: str
    url: Optional[str]
    source: Optional[str]
    published_at: Optional[datetime]
    matched_watch: MatchedWatch
    relevance_score: float
    impact_score: float
    sentiment: float
    event_type: Optional[str]
    summary: Optional[str]
    is_breaking: bool
    is_read: bool


class FeedResponse(BaseModel):
    """News feed response."""
    items: List[FeedItem]
    total: int
    unread: int


class DigestHighlight(BaseModel):
    """Digest highlight item."""
    title: str
    impact: str
    summary: Optional[str]


class DigestResponse(BaseModel):
    """Digest response."""
    period: str
    date: str
    summary: Optional[str]
    highlights: List[DigestHighlight]
    by_category: dict
    sentiment_summary: dict
    generated_at: Optional[datetime]


class AlertItem(BaseModel):
    """Breaking news alert."""
    id: int
    title: str
    impact_score: float
    event_type: Optional[str]
    matched_watches: List[str]
    summary: Optional[str]
    published_at: Optional[datetime]
    acknowledged: bool


class AlertsResponse(BaseModel):
    """Breaking alerts response."""
    alerts: List[AlertItem]
    unacknowledged: int


class StatsResponse(BaseModel):
    """Monitoring statistics."""
    watch_items: int
    matches_today: int
    matches_this_week: int
    unread: int
    pending_alerts: int
    top_sources: List[dict]
    top_event_types: List[dict]


# ============================================================================
# Watch List Management
# ============================================================================

@router.post("/watch", response_model=WatchItemResponse)
def add_watch(
    request: WatchItemCreate,
    db: Session = Depends(get_db)
):
    """
    Add item to watch list.

    Watch types:
    - company: Watch a specific company (e.g., "Stripe")
    - investor: Watch an investor (e.g., "Sequoia Capital")
    - sector: Watch a sector (e.g., "fintech")
    - keyword: Watch a keyword (e.g., "acquisition")
    """
    monitor = NewsMonitor(db)

    try:
        watch = monitor.add_watch(
            watch_type=request.watch_type,
            watch_value=request.watch_value,
            event_types=request.event_types,
            min_relevance=request.min_relevance,
            alert_enabled=request.alert_enabled,
            digest_enabled=request.digest_enabled
        )

        return WatchItemResponse(
            id=watch.id,
            watch_type=watch.watch_type,
            watch_value=watch.watch_value,
            event_types=watch.event_types,
            min_relevance=watch.min_relevance,
            alert_enabled=watch.alert_enabled,
            digest_enabled=watch.digest_enabled,
            created_at=watch.created_at,
            updated_at=watch.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/watch", response_model=List[WatchItemResponse])
def list_watches(db: Session = Depends(get_db)):
    """List all watch items."""
    monitor = NewsMonitor(db)
    watches = monitor.list_watches()

    return [
        WatchItemResponse(
            id=w.id,
            watch_type=w.watch_type,
            watch_value=w.watch_value,
            event_types=w.event_types,
            min_relevance=w.min_relevance,
            alert_enabled=w.alert_enabled,
            digest_enabled=w.digest_enabled,
            created_at=w.created_at,
            updated_at=w.updated_at
        )
        for w in watches
    ]


@router.delete("/watch/{watch_id}")
def remove_watch(watch_id: int, db: Session = Depends(get_db)):
    """Remove item from watch list."""
    monitor = NewsMonitor(db)

    if not monitor.remove_watch(watch_id):
        raise HTTPException(status_code=404, detail="Watch item not found")

    return {"status": "removed", "id": watch_id}


@router.patch("/watch/{watch_id}", response_model=WatchItemResponse)
def update_watch(
    watch_id: int,
    request: WatchItemUpdate,
    db: Session = Depends(get_db)
):
    """Update watch item settings."""
    monitor = NewsMonitor(db)

    watch = monitor.update_watch(
        watch_id=watch_id,
        event_types=request.event_types,
        min_relevance=request.min_relevance,
        alert_enabled=request.alert_enabled,
        digest_enabled=request.digest_enabled
    )

    if not watch:
        raise HTTPException(status_code=404, detail="Watch item not found")

    return WatchItemResponse(
        id=watch.id,
        watch_type=watch.watch_type,
        watch_value=watch.watch_value,
        event_types=watch.event_types,
        min_relevance=watch.min_relevance,
        alert_enabled=watch.alert_enabled,
        digest_enabled=watch.digest_enabled,
        created_at=watch.created_at,
        updated_at=watch.updated_at
    )


# ============================================================================
# Personalized Feed
# ============================================================================

@router.get("/feed", response_model=FeedResponse)
def get_feed(
    days: int = Query(7, ge=1, le=90, description="Time range in days"),
    min_relevance: float = Query(0.5, ge=0, le=1, description="Minimum relevance score"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(50, ge=1, le=200, description="Max items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    """
    Get personalized news feed matched to watch list.

    Returns news items sorted by relevance, with matching watch item info.
    """
    monitor = NewsMonitor(db)

    result = monitor.get_personalized_feed(
        days=days,
        min_relevance=min_relevance,
        event_type=event_type,
        limit=limit,
        offset=offset
    )

    items = []
    for match in result["items"]:
        # Parse published_at if it's a string
        published_at = match.get("published_at")
        if published_at and isinstance(published_at, str):
            try:
                published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            except ValueError:
                published_at = None

        items.append(FeedItem(
            id=match["id"],
            title=match["title"],
            url=match.get("url"),
            source=match.get("source"),
            published_at=published_at,
            matched_watch=MatchedWatch(
                id=match["matched_watch"]["id"],
                type=match["matched_watch"]["type"],
                value=match["matched_watch"]["value"]
            ),
            relevance_score=match.get("relevance_score", 0),
            impact_score=match.get("impact_score", 0),
            sentiment=match.get("sentiment", 0),
            event_type=match.get("event_type"),
            summary=match.get("summary"),
            is_breaking=match.get("is_breaking", False),
            is_read=match.get("is_read", False)
        ))

    return FeedResponse(
        items=items,
        total=result["total"],
        unread=result["unread"]
    )


@router.post("/feed/{match_id}/read")
def mark_as_read(match_id: int, db: Session = Depends(get_db)):
    """Mark a news item as read."""
    monitor = NewsMonitor(db)

    count = monitor.mark_as_read([match_id])
    if count == 0:
        raise HTTPException(status_code=404, detail="News match not found")

    return {"status": "marked_read", "id": match_id}


# ============================================================================
# Digest Generation
# ============================================================================

@router.get("/digest", response_model=DigestResponse)
def get_digest(
    period: str = Query("daily", regex="^(daily|weekly)$", description="Digest period"),
    date_str: Optional[str] = Query(None, alias="date", description="Date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get AI-generated news digest.

    Returns a summary of news activity with highlights and statistics.
    """
    monitor = NewsMonitor(db)

    # Parse date
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        target_date = date.today()

    digest = monitor.get_digest(period, target_date)

    if not digest:
        # Generate on demand if not exists
        digest = monitor.generate_digest(period, target_date)

    # Parse highlights
    highlights = []
    if digest.highlights:
        for h in digest.highlights:
            highlights.append(DigestHighlight(
                title=h.get("title", ""),
                impact=h.get("impact", "medium"),
                summary=h.get("summary")
            ))

    # Parse stats
    stats = digest.stats or {}

    return DigestResponse(
        period=digest.period_type,
        date=target_date.isoformat(),
        summary=digest.summary,
        highlights=highlights,
        by_category=stats.get("by_category", {}),
        sentiment_summary=stats.get("sentiment_summary", {}),
        generated_at=digest.generated_at
    )


@router.post("/digest/generate", response_model=DigestResponse)
def regenerate_digest(
    period: str = Query("daily", regex="^(daily|weekly)$"),
    date_str: Optional[str] = Query(None, alias="date"),
    db: Session = Depends(get_db)
):
    """Force regenerate digest for a period."""
    monitor = NewsMonitor(db)

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")
    else:
        target_date = date.today()

    digest = monitor.generate_digest(period, target_date)

    highlights = []
    if digest.highlights:
        for h in digest.highlights:
            highlights.append(DigestHighlight(
                title=h.get("title", ""),
                impact=h.get("impact", "medium"),
                summary=h.get("summary")
            ))

    stats = digest.stats or {}

    return DigestResponse(
        period=digest.period_type,
        date=target_date.isoformat(),
        summary=digest.summary,
        highlights=highlights,
        by_category=stats.get("by_category", {}),
        sentiment_summary=stats.get("sentiment_summary", {}),
        generated_at=digest.generated_at
    )


# ============================================================================
# Breaking Alerts
# ============================================================================

@router.get("/alerts", response_model=AlertsResponse)
def get_alerts(
    acknowledged: Optional[bool] = Query(None, description="Filter by acknowledged status"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get breaking/high-impact news alerts.

    Breaking alerts are news items with impact_score >= 0.8.
    """
    monitor = NewsMonitor(db)

    result = monitor.get_breaking_alerts(acknowledged=acknowledged, limit=limit)

    alerts = []
    for alert in result["alerts"]:
        # Parse published_at if it's a string
        published_at = alert.get("published_at")
        if published_at and isinstance(published_at, str):
            try:
                published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            except ValueError:
                published_at = None

        # Get matched watch value
        matched_watch = alert.get("matched_watch", {})
        watch_values = [matched_watch.get("value")] if matched_watch.get("value") else []

        alerts.append(AlertItem(
            id=alert["id"],
            title=alert["title"],
            impact_score=alert.get("impact_score", 0),
            event_type=alert.get("event_type"),
            matched_watches=watch_values,
            summary=alert.get("summary"),
            published_at=published_at,
            acknowledged=alert.get("acknowledged", False)
        ))

    return AlertsResponse(
        alerts=alerts,
        unacknowledged=result["unacknowledged"]
    )


@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    """Mark alert as acknowledged."""
    monitor = NewsMonitor(db)

    if not monitor.acknowledge_alert(alert_id):
        raise HTTPException(status_code=404, detail="Alert not found")

    return {"status": "acknowledged", "id": alert_id}


# ============================================================================
# Statistics
# ============================================================================

@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get news monitoring statistics."""
    monitor = NewsMonitor(db)
    stats = monitor.get_stats()

    return StatsResponse(
        watch_items=stats["watch_items"],
        matches_today=stats["matches_today"],
        matches_this_week=stats["matches_this_week"],
        unread=stats["unread"],
        pending_alerts=stats["pending_alerts"],
        top_sources=stats["top_sources"],
        top_event_types=stats["top_event_types"]
    )


# ============================================================================
# News Processing (Internal/Admin)
# ============================================================================

@router.post("/process")
def process_news(db: Session = Depends(get_db)):
    """
    Process new news items against watch list.

    This endpoint triggers matching of recent news against all watch items.
    Typically called by a background job or scheduler.
    """
    monitor = NewsMonitor(db)
    result = monitor.process_recent_news()

    return {
        "status": "processed",
        "news_processed": result["news_processed"],
        "matches_created": result["matches_created"],
        "alerts_triggered": result["alerts_triggered"]
    }
