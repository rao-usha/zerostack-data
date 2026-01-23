"""
LP Collection API Router.

Provides endpoints for:
- Creating and monitoring collection jobs
- Collecting data for specific LPs
- Managing collection schedules
- Viewing coverage and status
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    LpFund,
    LpCollectionJob,
    LpCollectionRun,
    LpCollectionSchedule,
    LpCollectionSourceType,
    LpCollectionFrequency,
    LpGovernanceMember,
    LpBoardMeeting,
    LpPerformanceReturn,
)
from app.sources.lp_collection.types import (
    CollectionConfig,
    CollectionMode,
    LpCollectionSource,
)
from app.sources.lp_collection.runner import LpCollectionOrchestrator
from app.sources.lp_collection.config import get_lp_registry

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/lp-collection",
    tags=["LP Collection"],
)


# =============================================================================
# Pydantic Models
# =============================================================================


class CollectionJobCreate(BaseModel):
    """Request to create a collection job."""
    lp_types: Optional[List[str]] = Field(
        None,
        description="Filter by LP types (public_pension, sovereign_wealth, etc.)"
    )
    regions: Optional[List[str]] = Field(
        None,
        description="Filter by regions (us, europe, asia, etc.)"
    )
    sources: List[str] = Field(
        default=["website"],
        description="Collection sources (website, sec_adv, cafr, news)"
    )
    mode: str = Field(
        default="incremental",
        description="Collection mode: incremental or full"
    )
    max_age_days: int = Field(
        default=90,
        description="Re-collect data older than this (for incremental mode)"
    )
    max_concurrent_lps: int = Field(
        default=5,
        description="Maximum concurrent LP collections"
    )

    class Config:
        schema_extra = {
            "example": {
                "lp_types": ["public_pension"],
                "regions": ["us"],
                "sources": ["website", "sec_adv"],
                "mode": "incremental",
                "max_age_days": 90
            }
        }


class CollectionJobResponse(BaseModel):
    """Response for a collection job."""
    job_id: int
    status: str
    job_type: str
    total_lps: int
    completed_lps: int
    successful_lps: int
    failed_lps: int
    total_items_found: int
    total_items_inserted: int
    total_items_updated: int
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    progress_pct: float


class SingleLpCollectRequest(BaseModel):
    """Request to collect a single LP."""
    sources: List[str] = Field(
        default=["website"],
        description="Collection sources to use"
    )


class ScheduleCreate(BaseModel):
    """Request to create a collection schedule."""
    lp_id: int = Field(..., description="LP fund ID")
    source_type: str = Field(..., description="Collection source type")
    frequency: str = Field(default="weekly", description="Frequency: daily, weekly, monthly, quarterly")
    day_of_week: Optional[int] = Field(None, description="Day of week (0=Monday) for weekly schedules")
    day_of_month: Optional[int] = Field(None, description="Day of month (1-31) for monthly schedules")
    hour: int = Field(default=2, description="Hour to run (0-23)")


class ScheduleResponse(BaseModel):
    """Response for a collection schedule."""
    id: int
    lp_id: int
    lp_name: str
    source_type: str
    frequency: str
    is_active: bool
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    consecutive_failures: int


class CoverageResponse(BaseModel):
    """Response for coverage statistics."""
    total_lps: int
    lps_with_data: int
    lps_never_collected: int
    lps_stale: int
    coverage_by_type: Dict[str, Dict[str, Any]]
    coverage_by_region: Dict[str, Dict[str, Any]]


class CollectionStatusResponse(BaseModel):
    """Response for overall collection system status."""
    active_jobs: int
    pending_schedules: int
    total_lps: int
    lps_collected_today: int
    lps_collected_this_week: int
    recent_runs: List[Dict[str, Any]]


# =============================================================================
# Job Endpoints
# =============================================================================


@router.post("/jobs", response_model=CollectionJobResponse)
async def create_collection_job(
    request: CollectionJobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Create a new LP collection job.

    The job runs in the background and collects data from configured sources.
    """
    # Build config
    try:
        sources = [LpCollectionSource(s) for s in request.sources]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid source: {e}")

    config = CollectionConfig(
        lp_types=request.lp_types,
        regions=request.regions,
        sources=sources,
        mode=CollectionMode(request.mode),
        max_age_days=request.max_age_days,
        max_concurrent_lps=request.max_concurrent_lps,
    )

    # Create orchestrator
    orchestrator = LpCollectionOrchestrator(db, config)

    # Create job record synchronously
    job = LpCollectionJob(
        job_type="batch",
        config=config.to_dict(),
        lp_types=request.lp_types,
        regions=request.regions,
        sources=request.sources,
        mode=request.mode,
        max_age_days=request.max_age_days,
        status="pending",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run collection in background
    async def run_job():
        try:
            await orchestrator.run_collection_job()
        except Exception as e:
            logger.error(f"Error in collection job {job.id}: {e}")

    background_tasks.add_task(asyncio.create_task, run_job())

    return CollectionJobResponse(
        job_id=job.id,
        status=job.status,
        job_type=job.job_type,
        total_lps=job.total_lps,
        completed_lps=job.completed_lps,
        successful_lps=job.successful_lps,
        failed_lps=job.failed_lps,
        total_items_found=job.total_items_found,
        total_items_inserted=job.total_items_inserted,
        total_items_updated=job.total_items_updated,
        started_at=job.started_at,
        completed_at=job.completed_at,
        progress_pct=0.0,
    )


@router.get("/jobs/{job_id}/status", response_model=CollectionJobResponse)
async def get_job_status(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Get the status of a collection job."""
    job = db.query(LpCollectionJob).filter(LpCollectionJob.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    progress_pct = (
        (job.completed_lps / job.total_lps * 100)
        if job.total_lps > 0 else 0.0
    )

    return CollectionJobResponse(
        job_id=job.id,
        status=job.status,
        job_type=job.job_type,
        total_lps=job.total_lps,
        completed_lps=job.completed_lps,
        successful_lps=job.successful_lps,
        failed_lps=job.failed_lps,
        total_items_found=job.total_items_found,
        total_items_inserted=job.total_items_inserted,
        total_items_updated=job.total_items_updated,
        started_at=job.started_at,
        completed_at=job.completed_at,
        progress_pct=progress_pct,
    )


@router.get("/jobs", response_model=List[CollectionJobResponse])
async def list_collection_jobs(
    status: Optional[str] = None,
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """List collection jobs, optionally filtered by status."""
    query = db.query(LpCollectionJob)

    if status:
        query = query.filter(LpCollectionJob.status == status)

    jobs = query.order_by(LpCollectionJob.created_at.desc()).limit(limit).all()

    return [
        CollectionJobResponse(
            job_id=job.id,
            status=job.status,
            job_type=job.job_type,
            total_lps=job.total_lps,
            completed_lps=job.completed_lps,
            successful_lps=job.successful_lps,
            failed_lps=job.failed_lps,
            total_items_found=job.total_items_found,
            total_items_inserted=job.total_items_inserted,
            total_items_updated=job.total_items_updated,
            started_at=job.started_at,
            completed_at=job.completed_at,
            progress_pct=(
                (job.completed_lps / job.total_lps * 100)
                if job.total_lps > 0 else 0.0
            ),
        )
        for job in jobs
    ]


# =============================================================================
# Single LP Collection
# =============================================================================


@router.post("/collect/{lp_id}")
async def collect_single_lp(
    lp_id: int,
    request: SingleLpCollectRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Collect data for a single LP.

    Runs collection immediately for the specified LP.
    """
    # Verify LP exists
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    try:
        sources = [LpCollectionSource(s) for s in request.sources]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid source: {e}")

    config = CollectionConfig(
        lp_id=lp_id,
        sources=sources,
    )

    orchestrator = LpCollectionOrchestrator(db, config)

    # Run synchronously for single LP
    results = await orchestrator.collect_single_lp(lp_id)

    return {
        "lp_id": lp_id,
        "lp_name": lp.name,
        "sources_collected": len(results),
        "success": all(r.success for r in results),
        "total_items_found": sum(r.items_found for r in results),
        "results": [r.to_dict() for r in results],
    }


@router.post("/collect/stale")
async def collect_stale_lps(
    max_age_days: int = 90,
    limit: int = 50,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Collect data for LPs that haven't been updated recently.

    Selects LPs by staleness and collection priority.
    """
    config = CollectionConfig(
        mode=CollectionMode.INCREMENTAL,
        max_age_days=max_age_days,
    )

    orchestrator = LpCollectionOrchestrator(db, config)

    # Get stale LPs count
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    stale_count = db.query(LpFund).filter(
        (LpFund.last_collection_at == None) |
        (LpFund.last_collection_at < cutoff)
    ).count()

    # Run in background
    job = await orchestrator.collect_stale_lps(
        max_age_days=max_age_days,
        limit=limit,
    )

    return {
        "job_id": job.id,
        "total_stale_lps": stale_count,
        "lps_queued": job.total_lps,
        "message": f"Started collection for {job.total_lps} stale LPs"
    }


# =============================================================================
# Schedule Endpoints
# =============================================================================


@router.post("/schedules", response_model=ScheduleResponse)
async def create_schedule(
    request: ScheduleCreate,
    db: Session = Depends(get_db),
):
    """Create a collection schedule for an LP."""
    # Verify LP exists
    lp = db.query(LpFund).filter(LpFund.id == request.lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Validate source type
    try:
        source_type = LpCollectionSourceType(request.source_type)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source type")

    # Validate frequency
    try:
        frequency = LpCollectionFrequency(request.frequency)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid frequency")

    # Check for existing schedule
    existing = db.query(LpCollectionSchedule).filter(
        LpCollectionSchedule.lp_id == request.lp_id,
        LpCollectionSchedule.source_type == source_type,
    ).first()

    if existing:
        raise HTTPException(
            status_code=400,
            detail="Schedule already exists for this LP/source combination"
        )

    # Calculate next run
    next_run = _calculate_next_run(
        frequency,
        request.hour,
        request.day_of_week,
        request.day_of_month,
    )

    schedule = LpCollectionSchedule(
        lp_id=request.lp_id,
        source_type=source_type,
        frequency=frequency,
        day_of_week=request.day_of_week,
        day_of_month=request.day_of_month,
        hour=request.hour,
        is_active=1,
        next_run_at=next_run,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)

    return ScheduleResponse(
        id=schedule.id,
        lp_id=schedule.lp_id,
        lp_name=lp.name,
        source_type=schedule.source_type.value,
        frequency=schedule.frequency.value,
        is_active=bool(schedule.is_active),
        last_run_at=schedule.last_run_at,
        next_run_at=schedule.next_run_at,
        consecutive_failures=schedule.consecutive_failures,
    )


@router.get("/schedules", response_model=List[ScheduleResponse])
async def list_schedules(
    lp_id: Optional[int] = None,
    source_type: Optional[str] = None,
    active_only: bool = False,
    db: Session = Depends(get_db),
):
    """List collection schedules."""
    query = db.query(LpCollectionSchedule)

    if lp_id:
        query = query.filter(LpCollectionSchedule.lp_id == lp_id)

    if source_type:
        try:
            st = LpCollectionSourceType(source_type)
            query = query.filter(LpCollectionSchedule.source_type == st)
        except ValueError:
            pass

    if active_only:
        query = query.filter(LpCollectionSchedule.is_active == 1)

    schedules = query.order_by(LpCollectionSchedule.next_run_at.asc()).all()

    result = []
    for schedule in schedules:
        lp = db.query(LpFund).filter(LpFund.id == schedule.lp_id).first()
        result.append(ScheduleResponse(
            id=schedule.id,
            lp_id=schedule.lp_id,
            lp_name=lp.name if lp else "Unknown",
            source_type=schedule.source_type.value,
            frequency=schedule.frequency.value,
            is_active=bool(schedule.is_active),
            last_run_at=schedule.last_run_at,
            next_run_at=schedule.next_run_at,
            consecutive_failures=schedule.consecutive_failures,
        ))

    return result


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(
    schedule_id: int,
    db: Session = Depends(get_db),
):
    """Delete a collection schedule."""
    schedule = db.query(LpCollectionSchedule).filter(
        LpCollectionSchedule.id == schedule_id
    ).first()

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    db.delete(schedule)
    db.commit()

    return {"message": "Schedule deleted"}


@router.patch("/schedules/{schedule_id}/toggle")
async def toggle_schedule(
    schedule_id: int,
    db: Session = Depends(get_db),
):
    """Toggle a schedule active/inactive."""
    schedule = db.query(LpCollectionSchedule).filter(
        LpCollectionSchedule.id == schedule_id
    ).first()

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    schedule.is_active = 0 if schedule.is_active else 1
    db.commit()

    return {
        "schedule_id": schedule_id,
        "is_active": bool(schedule.is_active),
    }


# =============================================================================
# Status & Coverage Endpoints
# =============================================================================


@router.get("/status", response_model=CollectionStatusResponse)
async def get_collection_status(
    db: Session = Depends(get_db),
):
    """Get overall collection system status."""
    # Active jobs
    active_jobs = db.query(LpCollectionJob).filter(
        LpCollectionJob.status.in_(["pending", "running"])
    ).count()

    # Pending schedules
    now = datetime.utcnow()
    pending_schedules = db.query(LpCollectionSchedule).filter(
        LpCollectionSchedule.is_active == 1,
        LpCollectionSchedule.next_run_at <= now,
    ).count()

    # Total LPs
    total_lps = db.query(LpFund).count()

    # LPs collected today
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    lps_today = db.query(LpFund).filter(
        LpFund.last_collection_at >= today_start
    ).count()

    # LPs collected this week
    week_start = today_start - timedelta(days=today_start.weekday())
    lps_week = db.query(LpFund).filter(
        LpFund.last_collection_at >= week_start
    ).count()

    # Recent runs
    recent_runs = db.query(LpCollectionRun).order_by(
        LpCollectionRun.created_at.desc()
    ).limit(10).all()

    return CollectionStatusResponse(
        active_jobs=active_jobs,
        pending_schedules=pending_schedules,
        total_lps=total_lps,
        lps_collected_today=lps_today,
        lps_collected_this_week=lps_week,
        recent_runs=[
            {
                "run_id": run.id,
                "lp_id": run.lp_id,
                "source_type": run.source_type.value,
                "status": run.status.value,
                "items_found": run.items_found,
                "created_at": run.created_at.isoformat(),
            }
            for run in recent_runs
        ],
    )


@router.get("/coverage", response_model=CoverageResponse)
async def get_coverage(
    db: Session = Depends(get_db),
):
    """Get LP coverage statistics."""
    # Get registry for comparison
    registry = get_lp_registry()

    # All LPs in database
    total_lps = db.query(LpFund).count()

    # LPs with data (have been collected)
    lps_with_data = db.query(LpFund).filter(
        LpFund.last_collection_at != None
    ).count()

    # LPs never collected
    lps_never = db.query(LpFund).filter(
        LpFund.last_collection_at == None
    ).count()

    # Stale LPs (>90 days)
    cutoff = datetime.utcnow() - timedelta(days=90)
    lps_stale = db.query(LpFund).filter(
        LpFund.last_collection_at < cutoff
    ).count()

    # Coverage by type
    coverage_by_type = {}
    for lp_type in ["public_pension", "sovereign_wealth", "endowment", "corporate_pension", "insurance"]:
        total = db.query(LpFund).filter(LpFund.lp_type == lp_type).count()
        collected = db.query(LpFund).filter(
            LpFund.lp_type == lp_type,
            LpFund.last_collection_at != None
        ).count()
        coverage_by_type[lp_type] = {
            "total": total,
            "collected": collected,
            "coverage_pct": round(collected / total * 100, 1) if total > 0 else 0
        }

    # Coverage by region
    coverage_by_region = {}
    for region in ["us", "europe", "asia", "middle_east", "oceania"]:
        total = db.query(LpFund).filter(LpFund.region == region).count()
        collected = db.query(LpFund).filter(
            LpFund.region == region,
            LpFund.last_collection_at != None
        ).count()
        coverage_by_region[region] = {
            "total": total,
            "collected": collected,
            "coverage_pct": round(collected / total * 100, 1) if total > 0 else 0
        }

    return CoverageResponse(
        total_lps=total_lps,
        lps_with_data=lps_with_data,
        lps_never_collected=lps_never,
        lps_stale=lps_stale,
        coverage_by_type=coverage_by_type,
        coverage_by_region=coverage_by_region,
    )


@router.post("/seed-lps")
async def seed_lps_from_registry(
    db: Session = Depends(get_db),
):
    """
    Seed the database with LPs from the expanded registry.

    Creates LpFund records for all LPs in the registry that don't already exist.
    """
    registry = get_lp_registry()

    created = 0
    skipped = 0

    for lp_entry in registry.all_lps:
        # Check if already exists
        existing = db.query(LpFund).filter(LpFund.name == lp_entry.name).first()

        if existing:
            skipped += 1
            continue

        # Create new LP
        lp = LpFund(
            name=lp_entry.name,
            formal_name=lp_entry.formal_name,
            lp_type=lp_entry.lp_type,
            jurisdiction=lp_entry.jurisdiction,
            website_url=lp_entry.website_url,
            region=lp_entry.region,
            country_code=lp_entry.country_code,
            aum_usd_billions=lp_entry.aum_usd_billions,
            has_cafr=1 if lp_entry.has_cafr else 0,
            sec_crd_number=lp_entry.sec_crd_number,
            collection_priority=lp_entry.collection_priority,
        )
        db.add(lp)
        created += 1

    db.commit()

    return {
        "message": f"Seeded {created} LPs from registry",
        "created": created,
        "skipped": skipped,
        "total_in_registry": registry.lp_count,
    }


# =============================================================================
# Governance & Performance Endpoints
# =============================================================================


class GovernanceMemberResponse(BaseModel):
    """Response for a governance member."""
    id: int
    full_name: str
    title: Optional[str]
    governance_role: str
    committee_name: Optional[str]
    representing: Optional[str]
    is_current: bool
    source_url: Optional[str]
    collected_at: Optional[datetime]


class BoardMeetingResponse(BaseModel):
    """Response for a board meeting."""
    id: int
    meeting_date: datetime
    meeting_type: str
    meeting_title: Optional[str]
    agenda_url: Optional[str]
    minutes_url: Optional[str]
    materials_url: Optional[str]
    video_url: Optional[str]


class PerformanceReturnResponse(BaseModel):
    """Response for performance return data."""
    id: int
    fiscal_year: int
    one_year_return_pct: Optional[str]
    three_year_return_pct: Optional[str]
    five_year_return_pct: Optional[str]
    ten_year_return_pct: Optional[str]
    since_inception_return_pct: Optional[str]
    benchmark_name: Optional[str]
    benchmark_one_year_pct: Optional[str]
    total_fund_value_usd: Optional[str]
    source_type: Optional[str]


class GovernanceOverviewResponse(BaseModel):
    """Complete governance overview for an LP."""
    lp_id: int
    lp_name: str
    board_members: List[GovernanceMemberResponse]
    recent_meetings: List[BoardMeetingResponse]


class PerformanceHistoryResponse(BaseModel):
    """Performance history for an LP."""
    lp_id: int
    lp_name: str
    returns: List[PerformanceReturnResponse]


@router.get("/governance/{lp_id}", response_model=GovernanceOverviewResponse)
async def get_lp_governance(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Get governance information for an LP.

    Returns board members, trustees, and recent meetings.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail=f"LP {lp_id} not found")

    # Get governance members (current only)
    members = db.query(LpGovernanceMember).filter(
        LpGovernanceMember.lp_id == lp_id,
        LpGovernanceMember.is_current == 1,
    ).order_by(LpGovernanceMember.governance_role).all()

    # Get recent meetings
    meetings = db.query(LpBoardMeeting).filter(
        LpBoardMeeting.lp_id == lp_id,
    ).order_by(LpBoardMeeting.meeting_date.desc()).limit(20).all()

    return GovernanceOverviewResponse(
        lp_id=lp_id,
        lp_name=lp.name,
        board_members=[
            GovernanceMemberResponse(
                id=m.id,
                full_name=m.full_name,
                title=m.title,
                governance_role=m.governance_role,
                committee_name=m.committee_name,
                representing=m.representing,
                is_current=bool(m.is_current),
                source_url=m.source_url,
                collected_at=m.collected_at,
            )
            for m in members
        ],
        recent_meetings=[
            BoardMeetingResponse(
                id=mtg.id,
                meeting_date=mtg.meeting_date,
                meeting_type=mtg.meeting_type,
                meeting_title=mtg.meeting_title,
                agenda_url=mtg.agenda_url,
                minutes_url=mtg.minutes_url,
                materials_url=mtg.materials_url,
                video_url=mtg.video_url,
            )
            for mtg in meetings
        ],
    )


@router.get("/performance/{lp_id}", response_model=PerformanceHistoryResponse)
async def get_lp_performance(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Get performance return history for an LP.

    Returns fiscal year returns and benchmark comparisons.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail=f"LP {lp_id} not found")

    # Get performance returns (most recent first)
    returns = db.query(LpPerformanceReturn).filter(
        LpPerformanceReturn.lp_id == lp_id,
    ).order_by(LpPerformanceReturn.fiscal_year.desc()).all()

    return PerformanceHistoryResponse(
        lp_id=lp_id,
        lp_name=lp.name,
        returns=[
            PerformanceReturnResponse(
                id=r.id,
                fiscal_year=r.fiscal_year,
                one_year_return_pct=r.one_year_return_pct,
                three_year_return_pct=r.three_year_return_pct,
                five_year_return_pct=r.five_year_return_pct,
                ten_year_return_pct=r.ten_year_return_pct,
                since_inception_return_pct=r.since_inception_return_pct,
                benchmark_name=r.benchmark_name,
                benchmark_one_year_pct=r.benchmark_one_year_pct,
                total_fund_value_usd=r.total_fund_value_usd,
                source_type=r.source_type,
            )
            for r in returns
        ],
    )


@router.post("/collect/{lp_id}/governance")
async def collect_lp_governance(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Collect governance data for a specific LP.

    Extracts board members, trustees, and meeting information.
    Runs synchronously and returns results.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail=f"LP {lp_id} not found")

    # Create config for governance collection
    config = CollectionConfig(
        lp_id=lp_id,
        sources=[LpCollectionSource.GOVERNANCE],
    )

    orchestrator = LpCollectionOrchestrator(db, config)

    try:
        results = await orchestrator.collect_single_lp(lp_id)
        items_found = sum(r.items_found for r in results)
        logger.info(f"Governance collection for {lp.name}: {items_found} items found")

        return {
            "message": f"Completed governance collection for {lp.name}",
            "lp_id": lp_id,
            "items_found": items_found,
            "success": all(r.success for r in results),
        }
    except Exception as e:
        logger.error(f"Error collecting governance for {lp.name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collect/{lp_id}/performance")
async def collect_lp_performance(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Collect performance return data for a specific LP.

    Extracts investment returns from website and annual reports.
    Runs synchronously and returns results.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail=f"LP {lp_id} not found")

    # Create config for performance collection
    config = CollectionConfig(
        lp_id=lp_id,
        sources=[LpCollectionSource.PERFORMANCE],
    )

    orchestrator = LpCollectionOrchestrator(db, config)

    try:
        results = await orchestrator.collect_single_lp(lp_id)
        items_found = sum(r.items_found for r in results)
        logger.info(f"Performance collection for {lp.name}: {items_found} items found")

        return {
            "message": f"Completed performance collection for {lp.name}",
            "lp_id": lp_id,
            "items_found": items_found,
            "success": all(r.success for r in results),
        }
    except Exception as e:
        logger.error(f"Error collecting performance for {lp.name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Helper Functions
# =============================================================================


def _calculate_next_run(
    frequency: LpCollectionFrequency,
    hour: int,
    day_of_week: Optional[int] = None,
    day_of_month: Optional[int] = None,
) -> datetime:
    """Calculate the next run time for a schedule."""
    now = datetime.utcnow()
    next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)

    if next_run <= now:
        # Move to next day/week/month
        if frequency == LpCollectionFrequency.DAILY:
            next_run += timedelta(days=1)
        elif frequency == LpCollectionFrequency.WEEKLY:
            days_ahead = (day_of_week or 0) - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            next_run += timedelta(days=days_ahead)
        elif frequency == LpCollectionFrequency.MONTHLY:
            if now.month == 12:
                next_run = next_run.replace(year=now.year + 1, month=1, day=day_of_month or 1)
            else:
                next_run = next_run.replace(month=now.month + 1, day=day_of_month or 1)
        elif frequency == LpCollectionFrequency.QUARTERLY:
            # Next quarter start
            quarter_month = ((now.month - 1) // 3 + 1) * 3 + 1
            if quarter_month > 12:
                quarter_month = 1
                next_run = next_run.replace(year=now.year + 1)
            next_run = next_run.replace(month=quarter_month, day=day_of_month or 1)

    return next_run
