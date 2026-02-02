"""
People Collection Jobs API endpoints.

Provides endpoints for job management, monitoring, and alerts:
- Collection job status and history
- Scheduled job management
- Change alerts and digests
"""

from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import PeopleCollectionJob
from app.jobs.people_collection_scheduler import (
    PeopleCollectionScheduler,
    process_pending_jobs,
    get_people_schedule_status,
    register_people_collection_schedules,
)
from app.jobs.change_monitor import ChangeMonitor, AlertDigestGenerator


router = APIRouter(prefix="/people-jobs", tags=["People Collection Jobs"])


# =============================================================================
# Response Models
# =============================================================================

class JobSummary(BaseModel):
    """Summary of a collection job."""
    id: int
    job_type: str
    company_id: Optional[int] = None
    company_count: int = 0
    status: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    changes_detected: int = 0


class JobDetail(BaseModel):
    """Detailed job information."""
    id: int
    job_type: str
    company_id: Optional[int] = None
    company_ids: Optional[List[int]] = None
    config: Optional[dict] = None
    status: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    people_updated: int = 0
    changes_detected: int = 0
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None


class JobStatsResponse(BaseModel):
    """Job statistics."""
    period_days: int
    total_jobs: int
    by_status: dict
    by_type: dict
    total_people_found: int
    total_people_created: int
    total_changes_detected: int
    success_rate: float


class ScheduleJobRequest(BaseModel):
    """Request to schedule a collection job."""
    job_type: str = Field(..., description="website_crawl, sec_parse, news_scan")
    company_ids: Optional[List[int]] = Field(None, description="Specific companies to collect")
    priority: str = Field("all", description="all, portfolio, public")
    limit: int = Field(50, ge=1, le=200, description="Max companies to process")


class ChangeAlertItem(BaseModel):
    """A change alert."""
    change_id: int
    person_name: str
    company_id: int
    company_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    announced_date: Optional[str] = None
    detected_date: Optional[str] = None
    is_c_suite: bool = False
    significance_score: Optional[int] = None


class AlertsResponse(BaseModel):
    """Change alerts response."""
    filter_type: Optional[str] = None
    filter_id: Optional[int] = None
    period_days: int
    total_alerts: int
    alerts: List[ChangeAlertItem]


class DigestSummary(BaseModel):
    """Summary stats in digest."""
    period_days: int
    total_changes: int
    by_type: dict
    c_suite_changes: int
    board_changes: int
    high_significance: int
    companies_affected: int


class DigestResponse(BaseModel):
    """Weekly digest response."""
    generated_at: str
    period: str
    filter: Optional[dict] = None
    summary: DigestSummary
    highlights: List[dict]
    all_changes: List[dict]


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/", response_model=List[JobSummary])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    job_type: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    List collection jobs.

    Returns recent jobs with summary information.
    """
    query = db.query(PeopleCollectionJob)

    if status:
        query = query.filter(PeopleCollectionJob.status == status)
    if job_type:
        query = query.filter(PeopleCollectionJob.job_type == job_type)

    jobs = query.order_by(PeopleCollectionJob.created_at.desc()).limit(limit).all()

    return [
        JobSummary(
            id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            company_count=len(job.company_ids) if job.company_ids else (1 if job.company_id else 0),
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            people_found=job.people_found or 0,
            people_created=job.people_created or 0,
            changes_detected=job.changes_detected or 0,
        )
        for job in jobs
    ]


@router.get("/stats", response_model=JobStatsResponse)
async def get_job_stats(
    days: int = Query(7, ge=1, le=90, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get collection job statistics.

    Returns summary stats for the specified period.
    """
    scheduler = PeopleCollectionScheduler(db)
    stats = scheduler.get_job_stats(days=days)

    return JobStatsResponse(**stats)


@router.get("/{job_id}", response_model=JobDetail)
async def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a specific job.
    """
    job = db.query(PeopleCollectionJob).get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobDetail(
        id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        company_ids=job.company_ids,
        config=job.config,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        people_found=job.people_found or 0,
        people_created=job.people_created or 0,
        people_updated=job.people_updated or 0,
        changes_detected=job.changes_detected or 0,
        errors=job.errors,
        warnings=job.warnings,
    )


@router.post("/schedule", response_model=JobSummary)
async def schedule_job(
    request: ScheduleJobRequest,
    db: Session = Depends(get_db),
):
    """
    Schedule a new collection job.

    Creates a pending job for the specified companies.
    """
    valid_types = {"website_crawl", "sec_parse", "news_scan", "sec_8k_check"}
    if request.job_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"job_type must be one of: {valid_types}"
        )

    scheduler = PeopleCollectionScheduler(db)

    if request.company_ids:
        company_ids = request.company_ids
    else:
        # Get companies based on priority
        companies = scheduler.get_companies_for_refresh(
            job_type=request.job_type,
            limit=request.limit,
            priority=request.priority,
        )
        company_ids = [c.id for c in companies]

    if not company_ids:
        raise HTTPException(
            status_code=400,
            detail="No companies found for the specified criteria"
        )

    job = scheduler.create_batch_job(
        job_type=request.job_type,
        company_ids=company_ids,
        config={
            "source": "api",
            "priority": request.priority,
        },
    )

    return JobSummary(
        id=job.id,
        job_type=job.job_type,
        company_count=len(company_ids),
        status=job.status,
        created_at=job.created_at,
    )


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Cancel a pending job.

    Only pending jobs can be cancelled.
    """
    job = db.query(PeopleCollectionJob).get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "pending":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status: {job.status}"
        )

    job.status = "cancelled"
    job.completed_at = datetime.utcnow()
    db.commit()

    return {"status": "cancelled", "job_id": job_id}


@router.post("/cleanup-stuck")
async def cleanup_stuck_jobs(
    max_age_hours: int = Query(4, ge=1, le=24),
    db: Session = Depends(get_db),
):
    """
    Mark stuck jobs as failed.

    Jobs running longer than max_age_hours are considered stuck.
    """
    scheduler = PeopleCollectionScheduler(db)
    count = scheduler.cleanup_stuck_jobs(max_age_hours=max_age_hours)

    return {
        "cleaned_up": count,
        "max_age_hours": max_age_hours,
    }


@router.post("/process")
async def process_jobs(
    max_jobs: int = Query(5, ge=1, le=20, description="Max jobs to process"),
):
    """
    Manually trigger processing of pending collection jobs.

    This picks up pending jobs and executes them using the
    PeopleCollectionOrchestrator.
    """
    result = await process_pending_jobs(max_jobs=max_jobs)
    return result


@router.get("/schedules/status")
async def get_schedule_status():
    """
    Get status of all people collection scheduled jobs.

    Shows next run times for:
    - Job processor (every 10 min)
    - Weekly website refresh (Sundays 2 AM)
    - Daily SEC check (weekdays 6 PM)
    - Daily news scan (8 AM)
    - Stuck job cleanup (every 2 hours)
    """
    return get_people_schedule_status()


@router.post("/schedules/register")
async def register_schedules():
    """
    Register people collection schedules with APScheduler.

    Call this if schedules are not running after a restart.
    """
    results = register_people_collection_schedules()
    registered = sum(1 for v in results.values() if v)
    return {
        "registered": registered,
        "total": len(results),
        "details": results,
    }


# =============================================================================
# Alert Endpoints
# =============================================================================

@router.get("/alerts/recent", response_model=AlertsResponse)
async def get_recent_alerts(
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Get recent leadership change alerts across all companies.
    """
    monitor = ChangeMonitor(db)
    changes = monitor.get_recent_changes(days=days, c_suite_only=c_suite_only)

    from app.core.people_models import IndustrialCompany
    alerts = []
    for change in changes:
        company = db.query(IndustrialCompany).get(change.company_id)
        alerts.append(ChangeAlertItem(
            change_id=change.id,
            person_name=change.person_name,
            company_id=change.company_id,
            company_name=company.name if company else "Unknown",
            change_type=change.change_type,
            old_title=change.old_title,
            new_title=change.new_title,
            announced_date=change.announced_date.isoformat() if change.announced_date else None,
            detected_date=change.detected_date.isoformat() if change.detected_date else None,
            is_c_suite=change.is_c_suite,
            significance_score=change.significance_score,
        ))

    return AlertsResponse(
        period_days=days,
        total_alerts=len(alerts),
        alerts=alerts,
    )


@router.get("/alerts/portfolio/{portfolio_id}", response_model=AlertsResponse)
async def get_portfolio_alerts(
    portfolio_id: int,
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Get leadership change alerts for a specific portfolio.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_portfolio_alerts(
        portfolio_id=portfolio_id,
        days=days,
        c_suite_only=c_suite_only,
    )

    return AlertsResponse(
        filter_type="portfolio",
        filter_id=portfolio_id,
        period_days=days,
        total_alerts=len(alerts),
        alerts=[ChangeAlertItem(**a) for a in alerts],
    )


@router.get("/alerts/watchlist/{watchlist_id}")
async def get_watchlist_alerts(
    watchlist_id: int,
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """
    Get alerts for people on a specific watchlist.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_watchlist_alerts(watchlist_id=watchlist_id, days=days)

    return {
        "filter_type": "watchlist",
        "filter_id": watchlist_id,
        "period_days": days,
        "total_alerts": len(alerts),
        "alerts": alerts,
    }


@router.get("/alerts/industry/{industry}", response_model=AlertsResponse)
async def get_industry_alerts(
    industry: str,
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    """
    Get leadership change alerts for a specific industry.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_industry_alerts(
        industry=industry,
        days=days,
        c_suite_only=c_suite_only,
    )

    return AlertsResponse(
        filter_type="industry",
        period_days=days,
        total_alerts=len(alerts),
        alerts=[ChangeAlertItem(**a) for a in alerts],
    )


# =============================================================================
# Digest Endpoints
# =============================================================================

@router.get("/digest/weekly")
async def get_weekly_digest(
    portfolio_id: Optional[int] = Query(None, description="Filter to portfolio"),
    industry: Optional[str] = Query(None, description="Filter to industry"),
    db: Session = Depends(get_db),
):
    """
    Generate weekly digest of leadership changes.

    Can be filtered to a specific portfolio or industry.
    """
    generator = AlertDigestGenerator(db)
    digest = generator.generate_weekly_digest(
        portfolio_id=portfolio_id,
        industry=industry,
    )

    return digest


@router.get("/digest/watchlist/{watchlist_id}")
async def get_watchlist_digest(
    watchlist_id: int,
    days: int = Query(7, ge=1, le=30),
    db: Session = Depends(get_db),
):
    """
    Generate digest for a specific watchlist.
    """
    generator = AlertDigestGenerator(db)
    digest = generator.generate_watchlist_digest(
        watchlist_id=watchlist_id,
        days=days,
    )

    if "error" in digest:
        raise HTTPException(status_code=404, detail=digest["error"])

    return digest


@router.get("/digest/summary")
async def get_change_summary(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """
    Get summary of all leadership changes in period.
    """
    monitor = ChangeMonitor(db)
    summary = monitor.get_change_summary(days=days)

    return summary
