"""
DUNL (S&P Global Data Unlocked) API endpoints.

Provides ingestion triggers and query endpoints for DUNL reference data:
- Currencies, Ports, Units of Measure, UOM Conversions, Holiday Calendars
- RSS feed for dataset change tracking
- Aggregate stats across all DUNL tables
"""

import logging
import xml.etree.ElementTree as ET
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dunl"])


# ========== Request Models ==========


class CalendarIngestRequest(BaseModel):
    years: List[int] = Field(
        default=[2024, 2025, 2026],
        description="Years to ingest calendar data for",
    )


# ========== Ingestion Triggers (POST) ==========


@router.post("/dunl/currencies/ingest")
async def ingest_currencies(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Ingest all DUNL currency definitions (~208 records)."""
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="dunl:currencies",
        config={"dataset": "currencies"},
        message="DUNL currencies ingestion job created",
    )


@router.post("/dunl/ports/ingest")
async def ingest_ports(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Ingest all DUNL port definitions (~301 records)."""
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="dunl:ports",
        config={"dataset": "ports"},
        message="DUNL ports ingestion job created",
    )


@router.post("/dunl/uom/ingest")
async def ingest_uom(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Ingest DUNL units of measure (~210) and conversions (~635)."""
    # Dispatch two jobs: UOM definitions + conversions
    uom_result = create_and_dispatch_job(
        db,
        background_tasks,
        source="dunl:uom",
        config={"dataset": "uom"},
        message="DUNL UOM ingestion job created",
    )
    conv_result = create_and_dispatch_job(
        db,
        background_tasks,
        source="dunl:uom_conversions",
        config={"dataset": "uom_conversions"},
        message="DUNL UOM conversions ingestion job created",
    )
    return {
        "status": "accepted",
        "message": "DUNL UOM + conversions ingestion jobs created",
        "jobs": [uom_result, conv_result],
    }


@router.post("/dunl/calendars/ingest")
async def ingest_calendars(
    request: CalendarIngestRequest = CalendarIngestRequest(),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """Ingest DUNL holiday calendars for specified years."""
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="dunl:calendars",
        config={"dataset": "calendars", "years": request.years},
        message=f"DUNL calendars ingestion job created for years {request.years}",
    )


@router.post("/dunl/ingest-all")
async def ingest_all(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Ingest all DUNL datasets: currencies, ports, UOM, conversions, calendars."""
    jobs = []
    for source_key, label in [
        ("dunl:currencies", "currencies"),
        ("dunl:ports", "ports"),
        ("dunl:uom", "uom"),
        ("dunl:uom_conversions", "uom_conversions"),
        ("dunl:calendars", "calendars"),
    ]:
        config = {"dataset": label}
        if label == "calendars":
            config["years"] = [2024, 2025, 2026]
        result = create_and_dispatch_job(
            db,
            background_tasks,
            source=source_key,
            config=config,
            message=f"DUNL {label} ingestion job created",
        )
        jobs.append(result)

    return {
        "status": "accepted",
        "message": "DUNL full ingestion started (5 jobs)",
        "jobs": jobs,
    }


# ========== Query Endpoints (GET) ==========


@router.get("/dunl/currencies")
async def list_currencies(
    search: Optional[str] = Query(None, description="Search by code or name"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List DUNL currencies with optional search."""
    where = ""
    params = {"limit": limit, "offset": offset}
    if search:
        where = "WHERE currency_code ILIKE :q OR currency_name ILIKE :q"
        params["q"] = f"%{search}%"

    sql = f"""
        SELECT currency_code, currency_name, dunl_uri, country_region_ref, ingested_at
        FROM dunl_currencies {where}
        ORDER BY currency_code
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), params).mappings().all()
    count_sql = f"SELECT COUNT(*) FROM dunl_currencies {where}"
    total = db.execute(text(count_sql), params).scalar()
    return {"total": total, "data": [dict(r) for r in rows]}


@router.get("/dunl/ports")
async def list_ports(
    search: Optional[str] = Query(None, description="Search by symbol or name"),
    location: Optional[str] = Query(None, description="Filter by location/region"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List DUNL ports with optional search and location filter."""
    conditions = []
    params = {"limit": limit, "offset": offset}
    if search:
        conditions.append("(symbol ILIKE :q OR port_name ILIKE :q)")
        params["q"] = f"%{search}%"
    if location:
        conditions.append("location ILIKE :loc")
        params["loc"] = f"%{location}%"

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT symbol, port_name, location, dunl_uri, ingested_at
        FROM dunl_ports {where}
        ORDER BY symbol
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), params).mappings().all()
    count_sql = f"SELECT COUNT(*) FROM dunl_ports {where}"
    total = db.execute(text(count_sql), params).scalar()
    return {"total": total, "data": [dict(r) for r in rows]}


@router.get("/dunl/uom")
async def list_uom(
    search: Optional[str] = Query(None, description="Search by code or description"),
    uom_type: Optional[str] = Query(None, description="Filter by UOM type"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List DUNL units of measure."""
    conditions = []
    params = {"limit": limit, "offset": offset}
    if search:
        conditions.append("(uom_code ILIKE :q OR uom_name ILIKE :q OR description ILIKE :q)")
        params["q"] = f"%{search}%"
    if uom_type:
        conditions.append("uom_type ILIKE :utype")
        params["utype"] = f"%{uom_type}%"

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT uom_code, uom_name, description, uom_type, dunl_uri, ingested_at
        FROM dunl_uom {where}
        ORDER BY uom_code
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), params).mappings().all()
    count_sql = f"SELECT COUNT(*) FROM dunl_uom {where}"
    total = db.execute(text(count_sql), params).scalar()
    return {"total": total, "data": [dict(r) for r in rows]}


@router.get("/dunl/uom/conversions")
async def list_uom_conversions(
    from_uom: Optional[str] = Query(None, description="Filter by source UOM"),
    to_uom: Optional[str] = Query(None, description="Filter by target UOM"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List DUNL UOM conversion factors."""
    conditions = []
    params = {"limit": limit, "offset": offset}
    if from_uom:
        conditions.append("from_uom ILIKE :from_u")
        params["from_u"] = f"%{from_uom}%"
    if to_uom:
        conditions.append("to_uom ILIKE :to_u")
        params["to_u"] = f"%{to_uom}%"

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT conversion_code, from_uom, to_uom, factor, description, dunl_uri, ingested_at
        FROM dunl_uom_conversions {where}
        ORDER BY conversion_code
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), params).mappings().all()
    count_sql = f"SELECT COUNT(*) FROM dunl_uom_conversions {where}"
    total = db.execute(text(count_sql), params).scalar()
    return {"total": total, "data": [dict(r) for r in rows]}


@router.get("/dunl/calendars")
async def list_calendars(
    year: Optional[int] = Query(None, description="Filter by year"),
    commodity: Optional[str] = Query(None, description="Filter by commodity"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List DUNL holiday calendar events."""
    conditions = []
    params = {"limit": limit, "offset": offset}
    if year:
        conditions.append("year = :year")
        params["year"] = year
    if commodity:
        conditions.append("commodity ILIKE :comm")
        params["comm"] = f"%{commodity}%"

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT year, commodity, event_date, publication_affected, publication_comments,
               service_affected, service_comments, dunl_uri, ingested_at
        FROM dunl_calendars {where}
        ORDER BY year DESC, event_date
        LIMIT :limit OFFSET :offset
    """
    rows = db.execute(text(sql), params).mappings().all()
    count_sql = f"SELECT COUNT(*) FROM dunl_calendars {where}"
    total = db.execute(text(count_sql), params).scalar()
    return {"total": total, "data": [dict(r) for r in rows]}


@router.get("/dunl/stats")
async def dunl_stats(db: Session = Depends(get_db)):
    """Aggregate record counts across all DUNL tables."""
    tables = [
        ("dunl_currencies", "Currencies"),
        ("dunl_ports", "Ports"),
        ("dunl_uom", "Units of Measure"),
        ("dunl_uom_conversions", "UOM Conversions"),
        ("dunl_calendars", "Holiday Calendars"),
    ]
    stats = {}
    total = 0
    for tbl, label in tables:
        try:
            count = db.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
        except Exception:
            count = 0
        stats[label] = count
        total += count

    return {"source": "dunl", "total_records": total, "tables": stats}


@router.get("/dunl/changes")
async def dunl_changes():
    """Parse DUNL RSS feed for recent dataset changes."""
    from app.sources.dunl.client import DunlClient

    client = DunlClient()
    try:
        rss_data = await client.fetch_rss_feed()
        if not rss_data or not isinstance(rss_data, str):
            return {"changes": [], "message": "No RSS data available"}

        items = []
        try:
            root = ET.fromstring(rss_data)
            for item in root.iter("item"):
                entry = {
                    "title": item.findtext("title", ""),
                    "link": item.findtext("link", ""),
                    "description": item.findtext("description", ""),
                    "pubDate": item.findtext("pubDate", ""),
                }
                items.append(entry)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse DUNL RSS feed: {e}")
            return {"changes": [], "error": f"RSS parse error: {e}"}

        return {"changes": items, "count": len(items)}
    finally:
        await client.close()
