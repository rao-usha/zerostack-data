"""
Collection Jobs API endpoints.

Provides endpoints for managing people collection jobs:
- View job status and history
- Trigger batch collections
- Monitor collection progress
"""

from typing import Optional, List
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import (
    PeopleCollectionJob,
    IndustrialCompany,
)

router = APIRouter(prefix="/collection-jobs", tags=["Collection Jobs"])


# =============================================================================
# Response Models
# =============================================================================

class JobSummary(BaseModel):
    """Summary of a collection job."""
    id: int
    job_type: str
    company_id: Optional[int] = None
    company_name: Optional[str] = None
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    people_updated: int = 0
    changes_detected: int = 0
    error_count: int = 0

    class Config:
        from_attributes = True


class JobDetail(BaseModel):
    """Detailed job information."""
    id: int
    job_type: str
    company_id: Optional[int] = None
    company_name: Optional[str] = None
    config: Optional[dict] = None
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    people_updated: int = 0
    changes_detected: int = 0
    errors: Optional[List[str]] = None
    created_at: datetime

    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    """Paginated list of jobs."""
    items: List[JobSummary]
    total: int
    page: int
    page_size: int


class BatchJobRequest(BaseModel):
    """Request to trigger a batch collection job."""
    company_ids: Optional[List[int]] = Field(None, description="Specific company IDs to collect")
    industry: Optional[str] = Field(None, description="Collect all companies in industry")
    sources: List[str] = Field(["website"], description="Sources to collect from")
    max_companies: int = Field(50, ge=1, le=500, description="Maximum companies to process")


class BatchJobResponse(BaseModel):
    """Response when creating a batch job."""
    job_ids: List[int]
    total_companies: int
    status: str
    message: str


# =============================================================================
# Endpoints
# =============================================================================

@router.get("", response_model=JobListResponse)
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    company_id: Optional[int] = Query(None, description="Filter by company"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    List collection jobs with optional filters.

    Jobs track the status of data collection runs.
    """
    query = db.query(PeopleCollectionJob)

    if status:
        query = query.filter(PeopleCollectionJob.status == status)

    if job_type:
        query = query.filter(PeopleCollectionJob.job_type == job_type)

    if company_id:
        query = query.filter(PeopleCollectionJob.company_id == company_id)

    # Order by most recent first
    query = query.order_by(PeopleCollectionJob.created_at.desc())

    total = query.count()

    offset = (page - 1) * page_size
    jobs = query.offset(offset).limit(page_size).all()

    items = []
    for job in jobs:
        company_name = None
        if job.company_id:
            company = db.get(IndustrialCompany, job.company_id)
            company_name = company.name if company else None

        error_count = len(job.errors) if job.errors else 0

        items.append(JobSummary(
            id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            company_name=company_name,
            status=job.status,
            started_at=job.started_at,
            completed_at=job.completed_at,
            people_found=job.people_found or 0,
            people_created=job.people_created or 0,
            people_updated=job.people_updated or 0,
            changes_detected=job.changes_detected or 0,
            error_count=error_count,
        ))

    return JobListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{job_id}", response_model=JobDetail)
async def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a collection job.

    Includes configuration, timing, and any errors.
    """
    job = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.id == job_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    company_name = None
    if job.company_id:
        company = db.get(IndustrialCompany, job.company_id)
        company_name = company.name if company else None

    return JobDetail(
        id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        company_name=company_name,
        config=job.config,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        people_found=job.people_found or 0,
        people_created=job.people_created or 0,
        people_updated=job.people_updated or 0,
        changes_detected=job.changes_detected or 0,
        errors=job.errors,
        created_at=job.created_at,
    )


@router.post("/batch", response_model=BatchJobResponse)
async def create_batch_job(
    request: BatchJobRequest,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Create a batch collection job for multiple companies.

    Can specify companies by ID, industry, or collect all.
    """
    # Determine which companies to collect
    query = db.query(IndustrialCompany)

    if request.company_ids:
        query = query.filter(IndustrialCompany.id.in_(request.company_ids))
    elif request.industry:
        query = query.filter(IndustrialCompany.industry == request.industry)

    # Only companies with websites (for website collection)
    if "website" in request.sources:
        query = query.filter(IndustrialCompany.website.isnot(None))

    # Limit to max companies
    companies = query.limit(request.max_companies).all()

    if not companies:
        raise HTTPException(status_code=404, detail="No companies found matching criteria")

    # Create individual jobs for each company
    job_ids = []
    for company in companies:
        job = PeopleCollectionJob(
            job_type="batch",
            company_id=company.id,
            config={"sources": request.sources},
            status="pending",
        )
        db.add(job)
        db.flush()
        job_ids.append(job.id)

    db.commit()

    # Note: Background task execution would be handled by a worker
    # For now, we just create the job records

    return BatchJobResponse(
        job_ids=job_ids,
        total_companies=len(companies),
        status="pending",
        message=f"Created {len(job_ids)} collection jobs. Sources: {', '.join(request.sources)}",
    )


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Cancel a pending or running job.

    Jobs that have already completed cannot be cancelled.
    """
    job = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.id == job_id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in ["success", "failed", "cancelled"]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status: {job.status}"
        )

    job.status = "cancelled"
    job.completed_at = datetime.utcnow()
    db.commit()

    return {"status": "cancelled", "job_id": job_id}


@router.get("/stats/summary")
async def get_job_stats(
    days: int = Query(30, ge=1, le=365, description="Days to look back"),
    db: Session = Depends(get_db),
):
    """
    Get collection job statistics.

    Shows job counts, success rates, and data collected.
    """
    from datetime import timedelta
    cutoff_date = datetime.utcnow() - timedelta(days=days)

    # Query jobs in date range
    query = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.created_at >= cutoff_date
    )

    total_jobs = query.count()

    # Count by status
    status_counts = db.query(
        PeopleCollectionJob.status,
        func.count(PeopleCollectionJob.id)
    ).filter(
        PeopleCollectionJob.created_at >= cutoff_date
    ).group_by(PeopleCollectionJob.status).all()

    by_status = {status: count for status, count in status_counts}

    # Sum results
    results = db.query(
        func.sum(PeopleCollectionJob.people_found),
        func.sum(PeopleCollectionJob.people_created),
        func.sum(PeopleCollectionJob.people_updated),
        func.sum(PeopleCollectionJob.changes_detected),
    ).filter(
        PeopleCollectionJob.created_at >= cutoff_date,
        PeopleCollectionJob.status == "success",
    ).first()

    # Calculate success rate
    completed = by_status.get("success", 0) + by_status.get("failed", 0)
    success_rate = (by_status.get("success", 0) / completed * 100) if completed > 0 else 0

    return {
        "period_days": days,
        "total_jobs": total_jobs,
        "jobs_by_status": by_status,
        "success_rate_percent": round(success_rate, 1),
        "total_people_found": results[0] or 0,
        "total_people_created": results[1] or 0,
        "total_people_updated": results[2] or 0,
        "total_changes_detected": results[3] or 0,
    }


@router.get("/queue")
async def get_job_queue(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get jobs currently in the queue (pending or running).

    Useful for monitoring collection status.
    """
    jobs = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.status.in_(["pending", "running"])
    ).order_by(PeopleCollectionJob.created_at.asc()).limit(limit).all()

    items = []
    for job in jobs:
        company_name = None
        if job.company_id:
            company = db.get(IndustrialCompany, job.company_id)
            company_name = company.name if company else None

        items.append({
            "id": job.id,
            "job_type": job.job_type,
            "company_id": job.company_id,
            "company_name": company_name,
            "status": job.status,
            "created_at": job.created_at,
            "started_at": job.started_at,
        })

    pending_count = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.status == "pending"
    ).count()

    running_count = db.query(PeopleCollectionJob).filter(
        PeopleCollectionJob.status == "running"
    ).count()

    return {
        "queue": items,
        "pending_count": pending_count,
        "running_count": running_count,
    }
