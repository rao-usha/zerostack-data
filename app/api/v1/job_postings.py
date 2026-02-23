"""
Job Posting Intelligence API endpoints.

Provides endpoints for:
- Job posting collection triggers (single company, all, ATS discovery)
- Querying postings with filters
- Trend snapshots
- ATS configuration listing
- Aggregate statistics
- ATS company seeding
- Skills extraction & backfill
"""

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.job_postings.metadata import (
    generate_create_job_postings_sql,
    generate_create_company_ats_config_sql,
    generate_create_job_posting_snapshots_sql,
    generate_create_job_posting_alerts_sql,
)
from app.sources.job_postings.skills_extractor import extract_skills

logger = logging.getLogger(__name__)

_tables_created = False


def _ensure_tables(db: Session):
    """Create job posting tables if they don't exist (idempotent)."""
    global _tables_created
    if _tables_created:
        return
    try:
        db.execute(text(generate_create_job_postings_sql()))
        db.execute(text(generate_create_company_ats_config_sql()))
        db.execute(text(generate_create_job_posting_snapshots_sql()))
        db.execute(text(generate_create_job_posting_alerts_sql()))
        db.commit()
        _tables_created = True
    except Exception:
        db.rollback()
        _tables_created = True  # avoid retry loops; table may already exist

router = APIRouter(prefix="/job-postings", tags=["job_postings"])


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================


class CollectCompanyRequest(BaseModel):
    force_rediscover: bool = Field(False, description="Force re-detect ATS even if cached")


class CollectAllRequest(BaseModel):
    limit: Optional[int] = Field(None, description="Max companies to process")
    skip_recent_hours: int = Field(24, description="Skip companies crawled within N hours")


class SeedATSCompanyEntry(BaseModel):
    name: str
    website: Optional[str] = None
    ats_type: str
    board_token: str


class SeedATSCompaniesRequest(BaseModel):
    companies: List[SeedATSCompanyEntry]


class BackfillSkillsRequest(BaseModel):
    company_id: Optional[int] = Field(None, description="Limit to a single company")
    limit: int = Field(1000, description="Max postings to process")


# =============================================================================
# BACKGROUND TASK HELPERS
# =============================================================================


async def _run_collect_company(db_factory, job_id: int, company_id: int, force_rediscover: bool):
    db = db_factory()
    try:
        from app.sources.job_postings.ingest import ingest_job_postings_company
        await ingest_job_postings_company(db, job_id, company_id=company_id, force_rediscover=force_rediscover)
    except Exception as e:
        logger.error(f"Background job posting collection failed: {e}", exc_info=True)
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)[:1000]
            db.commit()
    finally:
        db.close()


async def _run_collect_all(db_factory, job_id: int, limit: Optional[int], skip_recent_hours: int):
    db = db_factory()
    try:
        from app.sources.job_postings.ingest import ingest_job_postings_all
        await ingest_job_postings_all(db, job_id, limit=limit, skip_recent_hours=skip_recent_hours)
    except Exception as e:
        logger.error(f"Background bulk job posting collection failed: {e}", exc_info=True)
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)[:1000]
            db.commit()
    finally:
        db.close()


async def _run_discover_ats(db_factory, job_id: int, company_id: int):
    db = db_factory()
    try:
        from app.sources.job_postings.ingest import ingest_job_postings_discover
        await ingest_job_postings_discover(db, job_id, company_id=company_id)
    except Exception as e:
        logger.error(f"Background ATS discovery failed: {e}", exc_info=True)
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)[:1000]
            db.commit()
    finally:
        db.close()


def _create_job(db: Session, source: str, config: dict) -> IngestionJob:
    job = IngestionJob(
        source=source,
        status=JobStatus.PENDING,
        config=config,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


# =============================================================================
# POST TRIGGERS
# =============================================================================


@router.post("/collect/{company_id}")
async def collect_company(
    company_id: int,
    body: CollectCompanyRequest = CollectCompanyRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """Collect job postings for a single company."""
    job = _create_job(db, "job_postings", {"company_id": company_id, "force_rediscover": body.force_rediscover})

    from app.core.database import get_session_factory
    db_factory = get_session_factory()

    background_tasks.add_task(
        _run_collect_company, db_factory, job.id, company_id, body.force_rediscover
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"Job posting collection started for company {company_id}",
    }


@router.post("/collect-all")
async def collect_all(
    body: CollectAllRequest = CollectAllRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """Collect job postings for all companies with websites."""
    job = _create_job(db, "job_postings", {"limit": body.limit, "skip_recent_hours": body.skip_recent_hours})

    from app.core.database import get_session_factory
    db_factory = get_session_factory()

    background_tasks.add_task(
        _run_collect_all, db_factory, job.id, body.limit, body.skip_recent_hours
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": "Bulk job posting collection started",
    }


@router.post("/discover-ats/{company_id}")
async def discover_ats(
    company_id: int,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """Discover ATS type for a company (no job collection)."""
    job = _create_job(db, "job_postings", {"company_id": company_id})

    from app.core.database import get_session_factory
    db_factory = get_session_factory()

    background_tasks.add_task(_run_discover_ats, db_factory, job.id, company_id)

    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"ATS discovery started for company {company_id}",
    }


# =============================================================================
# ALERTS
# =============================================================================


@router.get("/alerts")
def list_alerts(
    company_id: Optional[int] = Query(None),
    alert_type: Optional[str] = Query(None, description="hiring_surge, hiring_freeze, department_surge, department_decline"),
    severity: Optional[str] = Query(None, description="low, medium, high"),
    days: int = Query(30, ge=1, le=365),
    acknowledged: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """List job posting alerts with filters."""
    _ensure_tables(db)
    conditions = ["a.snapshot_date >= CURRENT_DATE - :days"]
    params: dict = {"days": days, "lim": limit}

    if company_id is not None:
        conditions.append("a.company_id = :company_id")
        params["company_id"] = company_id
    if alert_type:
        conditions.append("a.alert_type = :alert_type")
        params["alert_type"] = alert_type
    if severity:
        conditions.append("a.severity = :severity")
        params["severity"] = severity
    if acknowledged is not None:
        conditions.append("a.acknowledged = :ack")
        params["ack"] = acknowledged

    where = "WHERE " + " AND ".join(conditions)

    rows = db.execute(
        text(f"""
            SELECT a.id, a.company_id, ic.name, a.alert_type, a.severity,
                   a.snapshot_date, a.current_total, a.previous_total,
                   a.change_pct, a.change_abs, a.department, a.details,
                   a.acknowledged, a.created_at
            FROM job_posting_alerts a
            LEFT JOIN industrial_companies ic ON ic.id = a.company_id
            {where}
            ORDER BY a.created_at DESC
            LIMIT :lim
        """),
        params,
    ).fetchall()

    alerts = []
    for r in rows:
        alerts.append({
            "id": r[0], "company_id": r[1], "company_name": r[2],
            "alert_type": r[3], "severity": r[4],
            "snapshot_date": str(r[5]),
            "current_total": r[6], "previous_total": r[7],
            "change_pct": float(r[8]) if r[8] is not None else None,
            "change_abs": r[9],
            "department": r[10], "details": r[11],
            "acknowledged": r[12],
            "created_at": str(r[13]) if r[13] else None,
        })

    return {"total": len(alerts), "alerts": alerts}


@router.get("/alerts/summary")
def alerts_summary(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Aggregate alert counts by type and severity for the last N days."""
    _ensure_tables(db)

    rows = db.execute(
        text("""
            SELECT alert_type, severity, COUNT(*) as cnt
            FROM job_posting_alerts
            WHERE snapshot_date >= CURRENT_DATE - :days
            GROUP BY alert_type, severity
            ORDER BY cnt DESC
        """),
        {"days": days},
    ).fetchall()

    by_type: Dict[str, int] = {}
    by_severity: Dict[str, int] = {}
    breakdown = []
    for r in rows:
        by_type[r[0]] = by_type.get(r[0], 0) + r[2]
        by_severity[r[1]] = by_severity.get(r[1], 0) + r[2]
        breakdown.append({"alert_type": r[0], "severity": r[1], "count": r[2]})

    total = sum(by_type.values())

    return {
        "days": days,
        "total_alerts": total,
        "by_type": by_type,
        "by_severity": by_severity,
        "breakdown": breakdown,
    }


@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: int,
    db: Session = Depends(get_db),
):
    """Mark an alert as acknowledged."""
    _ensure_tables(db)
    result = db.execute(
        text("UPDATE job_posting_alerts SET acknowledged = TRUE WHERE id = :id RETURNING id"),
        {"id": alert_id},
    ).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    db.commit()
    return {"alert_id": alert_id, "acknowledged": True}


# =============================================================================
# GET QUERIES
# =============================================================================


@router.get("/")
def list_postings(
    company_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None, description="open or closed"),
    department: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    seniority: Optional[str] = Query(None),
    employment_type: Optional[str] = Query(None),
    ats_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None, description="Search title text"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List job postings with filters."""
    _ensure_tables(db)
    conditions = []
    params: dict = {"lim": limit, "off": offset}

    if company_id:
        conditions.append("jp.company_id = :company_id")
        params["company_id"] = company_id
    if status:
        conditions.append("jp.status = :status")
        params["status"] = status
    if department:
        conditions.append("jp.department ILIKE :dept")
        params["dept"] = f"%{department}%"
    if location:
        conditions.append("jp.location ILIKE :loc")
        params["loc"] = f"%{location}%"
    if seniority:
        conditions.append("jp.seniority_level = :seniority")
        params["seniority"] = seniority
    if employment_type:
        conditions.append("jp.employment_type = :emp_type")
        params["emp_type"] = employment_type
    if ats_type:
        conditions.append("jp.ats_type = :ats_type")
        params["ats_type"] = ats_type
    if search:
        conditions.append("jp.title ILIKE :search")
        params["search"] = f"%{search}%"

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Count
    count_row = db.execute(
        text(f"SELECT COUNT(*) FROM job_postings jp {where}"), params
    ).fetchone()
    total = count_row[0] if count_row else 0

    rows = db.execute(
        text(f"""
            SELECT jp.id, jp.company_id, ic.name,
                   jp.external_job_id, jp.title, jp.title_normalized,
                   jp.department, jp.team, jp.location,
                   jp.employment_type, jp.workplace_type, jp.seniority_level,
                   jp.salary_min, jp.salary_max, jp.salary_currency, jp.salary_interval,
                   jp.source_url, jp.ats_type, jp.status,
                   jp.first_seen_at, jp.last_seen_at, jp.closed_at, jp.posted_date,
                   jp.requirements
            FROM job_postings jp
            LEFT JOIN industrial_companies ic ON ic.id = jp.company_id
            {where}
            ORDER BY jp.last_seen_at DESC
            LIMIT :lim OFFSET :off
        """),
        params,
    ).fetchall()

    postings = []
    for r in rows:
        postings.append({
            "id": r[0], "company_id": r[1], "company_name": r[2],
            "external_job_id": r[3], "title": r[4], "title_normalized": r[5],
            "department": r[6], "team": r[7], "location": r[8],
            "employment_type": r[9], "workplace_type": r[10], "seniority_level": r[11],
            "salary_min": float(r[12]) if r[12] else None,
            "salary_max": float(r[13]) if r[13] else None,
            "salary_currency": r[14], "salary_interval": r[15],
            "source_url": r[16], "ats_type": r[17], "status": r[18],
            "first_seen_at": str(r[19]) if r[19] else None,
            "last_seen_at": str(r[20]) if r[20] else None,
            "closed_at": str(r[21]) if r[21] else None,
            "posted_date": str(r[22]) if r[22] else None,
            "requirements": r[23],
        })

    return {"total": total, "limit": limit, "offset": offset, "postings": postings}


@router.get("/company/{company_id}")
def company_postings(
    company_id: int,
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Get all postings for a company."""
    _ensure_tables(db)
    conditions = ["jp.company_id = :cid"]
    params: dict = {"cid": company_id, "lim": limit}

    if status:
        conditions.append("jp.status = :status")
        params["status"] = status

    where = "WHERE " + " AND ".join(conditions)

    rows = db.execute(
        text(f"""
            SELECT jp.id, jp.external_job_id, jp.title, jp.title_normalized,
                   jp.department, jp.team, jp.location,
                   jp.employment_type, jp.workplace_type, jp.seniority_level,
                   jp.salary_min, jp.salary_max, jp.source_url, jp.ats_type,
                   jp.status, jp.first_seen_at, jp.last_seen_at, jp.posted_date
            FROM job_postings jp
            {where}
            ORDER BY jp.first_seen_at DESC
            LIMIT :lim
        """),
        params,
    ).fetchall()

    postings = []
    for r in rows:
        postings.append({
            "id": r[0], "external_job_id": r[1], "title": r[2], "title_normalized": r[3],
            "department": r[4], "team": r[5], "location": r[6],
            "employment_type": r[7], "workplace_type": r[8], "seniority_level": r[9],
            "salary_min": float(r[10]) if r[10] else None,
            "salary_max": float(r[11]) if r[11] else None,
            "source_url": r[12], "ats_type": r[13], "status": r[14],
            "first_seen_at": str(r[15]) if r[15] else None,
            "last_seen_at": str(r[16]) if r[16] else None,
            "posted_date": str(r[17]) if r[17] else None,
        })

    return {"company_id": company_id, "total": len(postings), "postings": postings}


@router.get("/company/{company_id}/trends")
def company_trends(
    company_id: int,
    days: int = Query(90, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Get snapshot time series with WoW/MoM deltas and a trend summary."""
    _ensure_tables(db)
    rows = db.execute(
        text("""
            SELECT
                snapshot_date, total_open, new_postings, closed_postings,
                by_department, by_location, by_seniority, by_employment_type,
                total_open - LAG(total_open, 7) OVER w as wow_delta,
                total_open - LAG(total_open, 30) OVER w as mom_delta,
                CASE WHEN LAG(total_open, 7) OVER w > 0
                     THEN ROUND(100.0 * (total_open - LAG(total_open, 7) OVER w)
                          / LAG(total_open, 7) OVER w, 1) END as wow_pct,
                CASE WHEN LAG(total_open, 30) OVER w > 0
                     THEN ROUND(100.0 * (total_open - LAG(total_open, 30) OVER w)
                          / LAG(total_open, 30) OVER w, 1) END as mom_pct,
                ROUND(AVG(total_open) OVER (
                    ORDER BY snapshot_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 1) as avg_7d
            FROM job_posting_snapshots
            WHERE company_id = :cid AND snapshot_date >= CURRENT_DATE - :days
            WINDOW w AS (ORDER BY snapshot_date)
            ORDER BY snapshot_date
        """),
        {"cid": company_id, "days": days},
    ).fetchall()

    snapshots = []
    for r in rows:
        snapshots.append({
            "date": str(r[0]),
            "total_open": r[1],
            "new_postings": r[2],
            "closed_postings": r[3],
            "by_department": r[4],
            "by_location": r[5],
            "by_seniority": r[6],
            "by_employment_type": r[7],
            "wow_delta": r[8],
            "mom_delta": r[9],
            "wow_pct": float(r[10]) if r[10] is not None else None,
            "mom_pct": float(r[11]) if r[11] is not None else None,
            "avg_7d": float(r[12]) if r[12] is not None else None,
        })

    # Build summary from the latest snapshot
    summary = None
    if snapshots:
        latest = snapshots[-1]
        totals = [s["total_open"] for s in snapshots if s["total_open"] is not None]
        # Trend direction: compare last 7d avg vs prior 7d avg
        trend_direction = "stable"
        if len(totals) >= 14:
            recent_avg = sum(totals[-7:]) / 7
            prior_avg = sum(totals[-14:-7]) / 7
            if prior_avg > 0:
                pct_diff = (recent_avg - prior_avg) / prior_avg * 100
                if pct_diff > 5:
                    trend_direction = "growing"
                elif pct_diff < -5:
                    trend_direction = "declining"

        summary = {
            "latest_total": latest["total_open"],
            "wow_change": {
                "delta": latest.get("wow_delta"),
                "pct": latest.get("wow_pct"),
            },
            "mom_change": {
                "delta": latest.get("mom_delta"),
                "pct": latest.get("mom_pct"),
            },
            "trend_direction": trend_direction,
            "peak_total": max(totals) if totals else None,
            "trough_total": min(totals) if totals else None,
        }

    return {
        "company_id": company_id,
        "days": days,
        "summary": summary,
        "snapshots": snapshots,
    }


@router.get("/ats-configs")
def list_ats_configs(
    ats_type: Optional[str] = Query(None),
    crawl_status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """List all discovered ATS configurations."""
    _ensure_tables(db)
    conditions = []
    params: dict = {"lim": limit}

    if ats_type:
        conditions.append("cac.ats_type = :ats_type")
        params["ats_type"] = ats_type
    if crawl_status:
        conditions.append("cac.crawl_status = :crawl_status")
        params["crawl_status"] = crawl_status

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = db.execute(
        text(f"""
            SELECT cac.id, cac.company_id, ic.name,
                   cac.ats_type, cac.board_token, cac.careers_url, cac.api_url,
                   cac.last_crawled_at, cac.last_successful_crawl,
                   cac.total_postings, cac.crawl_status, cac.error_message
            FROM company_ats_config cac
            LEFT JOIN industrial_companies ic ON ic.id = cac.company_id
            {where}
            ORDER BY cac.total_postings DESC NULLS LAST
            LIMIT :lim
        """),
        params,
    ).fetchall()

    configs = []
    for r in rows:
        configs.append({
            "id": r[0], "company_id": r[1], "company_name": r[2],
            "ats_type": r[3], "board_token": r[4], "careers_url": r[5],
            "api_url": r[6],
            "last_crawled_at": str(r[7]) if r[7] else None,
            "last_successful_crawl": str(r[8]) if r[8] else None,
            "total_postings": r[9], "crawl_status": r[10], "error_message": r[11],
        })

    return {"total": len(configs), "configs": configs}


@router.get("/snapshots")
def list_snapshots(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Get latest snapshots across companies."""
    _ensure_tables(db)
    conditions = []
    params: dict = {"lim": limit}

    if date_from:
        conditions.append("s.snapshot_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("s.snapshot_date <= :date_to")
        params["date_to"] = date_to

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = db.execute(
        text(f"""
            SELECT s.company_id, ic.name, s.snapshot_date,
                   s.total_open, s.new_postings, s.closed_postings,
                   s.by_department, s.by_location, s.by_seniority, s.by_employment_type
            FROM job_posting_snapshots s
            LEFT JOIN industrial_companies ic ON ic.id = s.company_id
            {where}
            ORDER BY s.snapshot_date DESC, s.total_open DESC
            LIMIT :lim
        """),
        params,
    ).fetchall()

    snapshots = []
    for r in rows:
        snapshots.append({
            "company_id": r[0], "company_name": r[1],
            "snapshot_date": str(r[2]),
            "total_open": r[3], "new_postings": r[4], "closed_postings": r[5],
            "by_department": r[6], "by_location": r[7],
            "by_seniority": r[8], "by_employment_type": r[9],
        })

    return {"total": len(snapshots), "snapshots": snapshots}


# =============================================================================
# MARKET TRENDS
# =============================================================================


@router.get("/trends/market")
def market_trends(
    days: int = Query(90, ge=1, le=365),
    min_postings: int = Query(10, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Cross-company market trends — top growing/declining, market aggregate."""
    _ensure_tables(db)

    # Top growing and declining companies (latest vs 7 days ago)
    ranked = db.execute(
        text("""
            WITH latest AS (
                SELECT DISTINCT ON (company_id)
                    company_id, snapshot_date, total_open, by_department
                FROM job_posting_snapshots
                WHERE snapshot_date >= CURRENT_DATE - :days
                ORDER BY company_id, snapshot_date DESC
            ),
            week_ago AS (
                SELECT DISTINCT ON (company_id)
                    company_id, total_open
                FROM job_posting_snapshots
                WHERE snapshot_date <= CURRENT_DATE - 7
                  AND snapshot_date >= CURRENT_DATE - :days
                ORDER BY company_id, snapshot_date DESC
            ),
            changes AS (
                SELECT
                    l.company_id,
                    l.total_open as current_total,
                    w.total_open as previous_total,
                    l.total_open - w.total_open as wow_delta,
                    CASE WHEN w.total_open > 0
                         THEN ROUND(100.0 * (l.total_open - w.total_open) / w.total_open, 1)
                    END as wow_pct,
                    l.by_department
                FROM latest l
                JOIN week_ago w ON w.company_id = l.company_id
                WHERE l.total_open >= :min_postings OR w.total_open >= :min_postings
            )
            SELECT c.company_id, ic.name, c.current_total, c.previous_total,
                   c.wow_delta, c.wow_pct, c.by_department
            FROM changes c
            LEFT JOIN industrial_companies ic ON ic.id = c.company_id
            ORDER BY c.wow_pct DESC NULLS LAST
        """),
        {"days": days, "min_postings": min_postings},
    ).fetchall()

    all_companies = []
    for r in ranked:
        all_companies.append({
            "company_id": r[0], "company_name": r[1],
            "current_total": r[2], "previous_total": r[3],
            "wow_delta": r[4],
            "wow_pct": float(r[5]) if r[5] is not None else None,
            "by_department": r[6],
        })

    top_growing = [c for c in all_companies if (c["wow_delta"] or 0) > 0][:limit]
    top_declining = sorted(
        [c for c in all_companies if (c["wow_delta"] or 0) < 0],
        key=lambda x: x["wow_pct"] or 0,
    )[:limit]

    # Market aggregate — daily sum of total_open across all companies
    agg_rows = db.execute(
        text("""
            SELECT snapshot_date, SUM(total_open) as market_total,
                   COUNT(DISTINCT company_id) as company_count
            FROM job_posting_snapshots
            WHERE snapshot_date >= CURRENT_DATE - :days
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """),
        {"days": days},
    ).fetchall()

    market_aggregate = [
        {
            "date": str(r[0]),
            "total_open": r[1],
            "company_count": r[2],
        }
        for r in agg_rows
    ]

    # Department aggregate across all companies (latest snapshots)
    dept_agg: Dict[str, int] = {}
    for c in all_companies:
        depts = c.get("by_department")
        if isinstance(depts, dict):
            for dept, count in depts.items():
                dept_agg[dept] = dept_agg.get(dept, 0) + (count if isinstance(count, int) else 0)

    by_department_aggregate = sorted(
        [{"department": k, "total": v} for k, v in dept_agg.items()],
        key=lambda x: -x["total"],
    )[:30]

    return {
        "days": days,
        "top_growing": top_growing,
        "top_declining": top_declining,
        "market_aggregate": market_aggregate,
        "by_department_aggregate": by_department_aggregate,
    }


@router.get("/stats")
def posting_stats(db: Session = Depends(get_db)):
    """Aggregate statistics across all job postings."""
    _ensure_tables(db)
    stats = db.execute(
        text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'open') as total_open,
                COUNT(*) FILTER (WHERE status = 'closed') as total_closed,
                COUNT(DISTINCT company_id) as companies_with_postings
            FROM job_postings
        """)
    ).fetchone()

    by_ats = db.execute(
        text("""
            SELECT ats_type, COUNT(*) FROM job_postings
            WHERE status = 'open'
            GROUP BY ats_type ORDER BY COUNT(*) DESC
        """)
    ).fetchall()

    top_hiring = db.execute(
        text("""
            SELECT ic.name, COUNT(*) as open_roles
            FROM job_postings jp
            JOIN industrial_companies ic ON ic.id = jp.company_id
            WHERE jp.status = 'open'
            GROUP BY ic.name
            ORDER BY open_roles DESC
            LIMIT 20
        """)
    ).fetchall()

    by_seniority = db.execute(
        text("""
            SELECT COALESCE(seniority_level, 'unknown'), COUNT(*)
            FROM job_postings WHERE status = 'open'
            GROUP BY seniority_level ORDER BY COUNT(*) DESC
        """)
    ).fetchall()

    return {
        "total_postings": stats[0] if stats else 0,
        "total_open": stats[1] if stats else 0,
        "total_closed": stats[2] if stats else 0,
        "companies_with_postings": stats[3] if stats else 0,
        "by_ats_type": {r[0]: r[1] for r in by_ats},
        "by_seniority": {r[0]: r[1] for r in by_seniority},
        "top_hiring_companies": [{"company": r[0], "open_roles": r[1]} for r in top_hiring],
    }


# =============================================================================
# ATS COMPANY SEEDING
# =============================================================================

API_URL_TEMPLATES = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
    "smartrecruiters": "https://api.smartrecruiters.com/v1/companies/{token}/postings",
}


@router.post("/seed-ats-companies")
def seed_ats_companies(
    body: SeedATSCompaniesRequest,
    db: Session = Depends(get_db),
):
    """Seed well-known ATS companies into industrial_companies + company_ats_config.

    Pre-populates board tokens to skip ATS detection during collection.
    """
    _ensure_tables(db)
    seeded = 0
    skipped = 0
    company_ids = []

    for entry in body.companies:
        try:
            # Upsert industrial_companies
            result = db.execute(
                text("""
                    INSERT INTO industrial_companies (name, website)
                    VALUES (:name, :website)
                    ON CONFLICT (name) DO UPDATE SET
                        website = COALESCE(EXCLUDED.website, industrial_companies.website)
                    RETURNING id
                """),
                {"name": entry.name, "website": entry.website},
            )
            row = result.fetchone()
            if not row:
                skipped += 1
                continue
            company_id = row[0]
            company_ids.append(company_id)

            # Build API URL
            api_url = API_URL_TEMPLATES.get(entry.ats_type, "").format(token=entry.board_token) if entry.ats_type in API_URL_TEMPLATES else None

            # Upsert company_ats_config
            db.execute(
                text("""
                    INSERT INTO company_ats_config (
                        company_id, ats_type, board_token, api_url,
                        crawl_status, updated_at
                    ) VALUES (
                        :cid, :ats_type, :token, :api_url,
                        'pending', NOW()
                    )
                    ON CONFLICT (company_id) DO UPDATE SET
                        ats_type = EXCLUDED.ats_type,
                        board_token = EXCLUDED.board_token,
                        api_url = EXCLUDED.api_url,
                        updated_at = NOW()
                """),
                {
                    "cid": company_id,
                    "ats_type": entry.ats_type,
                    "token": entry.board_token,
                    "api_url": api_url,
                },
            )
            seeded += 1

        except Exception as e:
            logger.error(f"Failed to seed {entry.name}: {e}")
            skipped += 1

    db.commit()

    return {
        "seeded": seeded,
        "skipped": skipped,
        "company_ids": company_ids,
        "message": f"Seeded {seeded} ATS companies",
    }


# =============================================================================
# SKILLS EXTRACTION
# =============================================================================


async def _run_backfill_skills(db_factory, job_id: int, company_id: Optional[int], limit: int):
    """Background task to backfill skills extraction on existing postings."""
    db = db_factory()
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            db.commit()

        # Fetch postings that don't have requirements yet
        conditions = ["requirements IS NULL", "description_text IS NOT NULL"]
        params: dict = {"lim": limit}
        if company_id:
            conditions.append("company_id = :cid")
            params["cid"] = company_id

        where = "WHERE " + " AND ".join(conditions)
        rows = db.execute(
            text(f"""
                SELECT id, title, description_text
                FROM job_postings
                {where}
                ORDER BY id
                LIMIT :lim
            """),
            params,
        ).fetchall()

        extracted = 0
        for row_id, title, desc in rows:
            reqs = extract_skills(desc, title or "")
            if reqs and reqs.get("skill_count", 0) > 0:
                db.execute(
                    text("UPDATE job_postings SET requirements = :reqs WHERE id = :id"),
                    {"reqs": json.dumps(reqs), "id": row_id},
                )
                extracted += 1

                # Commit in batches
                if extracted % 100 == 0:
                    db.commit()
                    logger.info(f"Skills backfill: {extracted}/{len(rows)} processed")

        db.commit()

        if job:
            job.status = JobStatus.SUCCESS
            job.records_processed = len(rows)
            job.records_written = extracted
            db.commit()

        logger.info(f"Skills backfill complete: {extracted}/{len(rows)} postings enriched")

    except Exception as e:
        logger.error(f"Skills backfill failed: {e}", exc_info=True)
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.error_message = str(e)[:1000]
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


@router.post("/extract-skills/backfill")
async def backfill_skills(
    body: BackfillSkillsRequest = BackfillSkillsRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """Backfill skills extraction on existing postings that have no requirements."""
    _ensure_tables(db)
    job = _create_job(db, "job_postings_skills", {
        "action": "backfill_skills",
        "company_id": body.company_id,
        "limit": body.limit,
    })

    from app.core.database import get_session_factory
    db_factory = get_session_factory()

    background_tasks.add_task(
        _run_backfill_skills, db_factory, job.id, body.company_id, body.limit
    )

    return {
        "job_id": job.id,
        "status": "pending",
        "message": f"Skills backfill started (limit={body.limit})",
    }


@router.get("/skills-stats")
def skills_stats(db: Session = Depends(get_db)):
    """Aggregate statistics about extracted skills across all postings."""
    _ensure_tables(db)

    # How many have requirements
    coverage = db.execute(
        text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE requirements IS NOT NULL) as with_skills,
                COUNT(*) FILTER (WHERE requirements IS NULL AND description_text IS NOT NULL) as backfill_eligible
            FROM job_postings
        """)
    ).fetchone()

    total = coverage[0] if coverage else 0
    with_skills = coverage[1] if coverage else 0
    backfill_eligible = coverage[2] if coverage else 0

    # Top skills (requires scanning JSONB — sample-based for performance)
    skill_counts: Dict[str, int] = {}
    cert_counts: Dict[str, int] = {}
    education_counts: Dict[str, int] = {}
    exp_sum = 0
    exp_count = 0

    rows = db.execute(
        text("""
            SELECT requirements FROM job_postings
            WHERE requirements IS NOT NULL
            LIMIT 5000
        """)
    ).fetchall()

    for (reqs_raw,) in rows:
        if not reqs_raw:
            continue
        reqs = reqs_raw if isinstance(reqs_raw, dict) else {}
        for skill in reqs.get("skills", []):
            skill_counts[skill] = skill_counts.get(skill, 0) + 1
        for cert in reqs.get("certifications", []):
            cert_counts[cert] = cert_counts.get(cert, 0) + 1
        edu = reqs.get("education")
        if edu:
            education_counts[edu] = education_counts.get(edu, 0) + 1
        yrs = reqs.get("years_experience")
        if yrs is not None:
            exp_sum += yrs
            exp_count += 1

    # Sort by frequency, top 30
    top_skills = sorted(skill_counts.items(), key=lambda x: -x[1])[:30]
    top_certs = sorted(cert_counts.items(), key=lambda x: -x[1])[:10]

    return {
        "total_postings": total,
        "with_skills": with_skills,
        "backfill_eligible": backfill_eligible,
        "coverage_pct": round(with_skills / total * 100, 1) if total else 0,
        "top_skills": [{"skill": s, "count": c} for s, c in top_skills],
        "top_certifications": [{"cert": s, "count": c} for s, c in top_certs],
        "education_distribution": education_counts,
        "avg_years_experience": round(exp_sum / exp_count, 1) if exp_count else None,
    }
