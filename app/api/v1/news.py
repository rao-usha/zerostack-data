"""
News & Event Feed API Endpoints.

T24: Aggregates news from SEC EDGAR, Google News, and other sources
to provide a unified news feed for investors and portfolio companies.
"""

import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.news.aggregator import NewsAggregator, NewsFilters

router = APIRouter(prefix="/news", tags=["News"])


@router.get("/feed")
def get_news_feed(
    event_type: Optional[str] = Query(None, description="Filter: filing, funding, acquisition, ipo, news"),
    filing_type: Optional[str] = Query(None, description="Filter: 13F, 13D, 8-K, 10-K, Form D"),
    source: Optional[str] = Query(None, description="Filter: sec_edgar, google_news"),
    company: Optional[str] = Query(None, description="Filter by company name"),
    days: int = Query(7, ge=1, le=90, description="News from last N days"),
    limit: int = Query(50, ge=1, le=100, description="Max items to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
):
    """
    Get aggregated news feed with filters.

    Returns news items from all sources, sorted by publish date.
    Supports filtering by event type, source, company, and time range.
    """
    aggregator = NewsAggregator(db)

    filters = NewsFilters(
        event_type=event_type,
        filing_type=filing_type,
        source=source,
        company_name=company,
        days=days,
        limit=limit,
        offset=offset,
    )

    return aggregator.get_feed(filters)


@router.get("/company/{name}")
def get_company_news(
    name: str,
    days: int = Query(30, ge=1, le=90, description="News from last N days"),
    limit: int = Query(20, ge=1, le=100, description="Max items to return"),
    db: Session = Depends(get_db),
):
    """
    Get news for a specific company.

    Returns news items mentioning the company name.
    """
    aggregator = NewsAggregator(db)
    items = aggregator.get_company_news(
        company_name=name,
        days=days,
        limit=limit,
    )

    return {
        "company": name,
        "items": items,
        "total": len(items),
    }


@router.get("/investor/{investor_id}")
def get_investor_news(
    investor_id: int,
    investor_type: str = Query("lp", description="Investor type: lp or family_office"),
    days: int = Query(30, ge=1, le=90, description="News from last N days"),
    limit: int = Query(20, ge=1, le=100, description="Max items to return"),
    db: Session = Depends(get_db),
):
    """
    Get news for a specific investor.

    Returns news items tagged with the investor.
    """
    aggregator = NewsAggregator(db)
    items = aggregator.get_investor_news(
        investor_id=investor_id,
        investor_type=investor_type,
        days=days,
        limit=limit,
    )

    return {
        "investor_id": investor_id,
        "investor_type": investor_type,
        "items": items,
        "total": len(items),
    }


@router.get("/filings")
def get_sec_filings(
    filing_type: Optional[str] = Query(None, description="Filter: 13F, 13D, 8-K, 10-K, 10-Q, Form D"),
    days: int = Query(7, ge=1, le=90, description="Filings from last N days"),
    limit: int = Query(50, ge=1, le=100, description="Max items to return"),
    db: Session = Depends(get_db),
):
    """
    Get SEC filing feed.

    Returns recent SEC filings filtered by type.
    """
    aggregator = NewsAggregator(db)
    return aggregator.get_filings(
        filing_type=filing_type,
        days=days,
        limit=limit,
    )


@router.post("/refresh")
async def refresh_news(
    source: Optional[str] = Query(None, description="Specific source to refresh (sec_edgar, google_news)"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Trigger news refresh from sources.

    Fetches latest news from SEC EDGAR and Google News.
    Can optionally refresh a specific source only.
    """
    aggregator = NewsAggregator(db)

    if source:
        result = await aggregator.refresh_source(source)
    else:
        result = await aggregator.refresh_all()

    return result


@router.get("/sources")
def get_news_sources(
    db: Session = Depends(get_db),
):
    """
    List available news sources.

    Returns source names with item counts and last refresh times.
    """
    aggregator = NewsAggregator(db)
    sources = aggregator.get_sources()

    return {
        "sources": sources,
        "total": len(sources),
    }
