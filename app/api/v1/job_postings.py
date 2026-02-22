"""
Job Posting Intelligence API endpoints.

Provides endpoints for:
- Job posting collection triggers (single company, all, ATS discovery)
- Querying postings with filters
- Trend snapshots
- ATS configuration listing
- Aggregate statistics
"""

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
)

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
                   jp.first_seen_at, jp.last_seen_at, jp.closed_at, jp.posted_date
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
    """Get snapshot time series for a company."""
    _ensure_tables(db)
    rows = db.execute(
        text("""
            SELECT snapshot_date, total_open, new_postings, closed_postings,
                   by_department, by_location, by_seniority, by_employment_type
            FROM job_posting_snapshots
            WHERE company_id = :cid
            AND snapshot_date >= CURRENT_DATE - :days
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
        })

    return {"company_id": company_id, "days": days, "snapshots": snapshots}


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
