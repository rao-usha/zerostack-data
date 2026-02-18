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

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
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
    LpStrategySnapshot,
    LpAssetClassTargetAllocation,
    LpManagerOrVehicleExposure,
    PortfolioCompany,
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
        None, description="Filter by LP types (public_pension, sovereign_wealth, etc.)"
    )
    regions: Optional[List[str]] = Field(
        None, description="Filter by regions (us, europe, asia, etc.)"
    )
    sources: List[str] = Field(
        default=["website"],
        description="Collection sources (website, sec_adv, cafr, news)",
    )
    mode: str = Field(
        default="incremental", description="Collection mode: incremental or full"
    )
    max_age_days: int = Field(
        default=90, description="Re-collect data older than this (for incremental mode)"
    )
    max_concurrent_lps: int = Field(
        default=5, description="Maximum concurrent LP collections"
    )

    class Config:
        schema_extra = {
            "example": {
                "lp_types": ["public_pension"],
                "regions": ["us"],
                "sources": ["website", "sec_adv"],
                "mode": "incremental",
                "max_age_days": 90,
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
        default=["website"], description="Collection sources to use"
    )


class ScheduleCreate(BaseModel):
    """Request to create a collection schedule."""

    lp_id: int = Field(..., description="LP fund ID")
    source_type: str = Field(..., description="Collection source type")
    frequency: str = Field(
        default="weekly", description="Frequency: daily, weekly, monthly, quarterly"
    )
    day_of_week: Optional[int] = Field(
        None, description="Day of week (0=Monday) for weekly schedules"
    )
    day_of_month: Optional[int] = Field(
        None, description="Day of month (1-31) for monthly schedules"
    )
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

    # Run collection in background (or queue for worker)
    def run_job_sync():
        """Run the async collection job in a new event loop."""
        try:
            asyncio.run(orchestrator.run_collection_job())
        except Exception as e:
            logger.error(f"Error in collection job {job.id}: {e}")

    from app.core.job_queue_service import submit_job

    submit_job(
        db=db,
        job_type="lp",
        payload=config.to_dict(),
        job_table_id=job.id,
        background_tasks=background_tasks,
        background_func=run_job_sync,
        background_args=(),
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
        (job.completed_lps / job.total_lps * 100) if job.total_lps > 0 else 0.0
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
                (job.completed_lps / job.total_lps * 100) if job.total_lps > 0 else 0.0
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
    stale_count = (
        db.query(LpFund)
        .filter(
            (LpFund.last_collection_at == None) | (LpFund.last_collection_at < cutoff)
        )
        .count()
    )

    # Run in background
    job = await orchestrator.collect_stale_lps(
        max_age_days=max_age_days,
        limit=limit,
    )

    return {
        "job_id": job.id,
        "total_stale_lps": stale_count,
        "lps_queued": job.total_lps,
        "message": f"Started collection for {job.total_lps} stale LPs",
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
    existing = (
        db.query(LpCollectionSchedule)
        .filter(
            LpCollectionSchedule.lp_id == request.lp_id,
            LpCollectionSchedule.source_type == source_type,
        )
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=400,
            detail="Schedule already exists for this LP/source combination",
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
        result.append(
            ScheduleResponse(
                id=schedule.id,
                lp_id=schedule.lp_id,
                lp_name=lp.name if lp else "Unknown",
                source_type=schedule.source_type.value,
                frequency=schedule.frequency.value,
                is_active=bool(schedule.is_active),
                last_run_at=schedule.last_run_at,
                next_run_at=schedule.next_run_at,
                consecutive_failures=schedule.consecutive_failures,
            )
        )

    return result


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(
    schedule_id: int,
    db: Session = Depends(get_db),
):
    """Delete a collection schedule."""
    schedule = (
        db.query(LpCollectionSchedule)
        .filter(LpCollectionSchedule.id == schedule_id)
        .first()
    )

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
    schedule = (
        db.query(LpCollectionSchedule)
        .filter(LpCollectionSchedule.id == schedule_id)
        .first()
    )

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
    active_jobs = (
        db.query(LpCollectionJob)
        .filter(LpCollectionJob.status.in_(["pending", "running"]))
        .count()
    )

    # Pending schedules
    now = datetime.utcnow()
    pending_schedules = (
        db.query(LpCollectionSchedule)
        .filter(
            LpCollectionSchedule.is_active == 1,
            LpCollectionSchedule.next_run_at <= now,
        )
        .count()
    )

    # Total LPs
    total_lps = db.query(LpFund).count()

    # LPs collected today
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    lps_today = (
        db.query(LpFund).filter(LpFund.last_collection_at >= today_start).count()
    )

    # LPs collected this week
    week_start = today_start - timedelta(days=today_start.weekday())
    lps_week = db.query(LpFund).filter(LpFund.last_collection_at >= week_start).count()

    # Recent runs
    recent_runs = (
        db.query(LpCollectionRun)
        .order_by(LpCollectionRun.created_at.desc())
        .limit(10)
        .all()
    )

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
    get_lp_registry()

    # All LPs in database
    total_lps = db.query(LpFund).count()

    # LPs with data (have been collected)
    lps_with_data = db.query(LpFund).filter(LpFund.last_collection_at != None).count()

    # LPs never collected
    lps_never = db.query(LpFund).filter(LpFund.last_collection_at == None).count()

    # Stale LPs (>90 days)
    cutoff = datetime.utcnow() - timedelta(days=90)
    lps_stale = db.query(LpFund).filter(LpFund.last_collection_at < cutoff).count()

    # Coverage by type
    coverage_by_type = {}
    for lp_type in [
        "public_pension",
        "sovereign_wealth",
        "endowment",
        "corporate_pension",
        "insurance",
    ]:
        total = db.query(LpFund).filter(LpFund.lp_type == lp_type).count()
        collected = (
            db.query(LpFund)
            .filter(LpFund.lp_type == lp_type, LpFund.last_collection_at != None)
            .count()
        )
        coverage_by_type[lp_type] = {
            "total": total,
            "collected": collected,
            "coverage_pct": round(collected / total * 100, 1) if total > 0 else 0,
        }

    # Coverage by region
    coverage_by_region = {}
    for region in ["us", "europe", "asia", "middle_east", "oceania"]:
        total = db.query(LpFund).filter(LpFund.region == region).count()
        collected = (
            db.query(LpFund)
            .filter(LpFund.region == region, LpFund.last_collection_at != None)
            .count()
        )
        coverage_by_region[region] = {
            "total": total,
            "collected": collected,
            "coverage_pct": round(collected / total * 100, 1) if total > 0 else 0,
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

    Uses upsert logic - creates new records and updates existing ones.
    Safe to run multiple times.
    """
    registry = get_lp_registry()

    created = 0
    updated = 0
    skipped = 0

    for lp_entry in registry.all_lps:
        try:
            # Check if already exists
            existing = db.query(LpFund).filter(LpFund.name == lp_entry.name).first()

            if existing:
                # Update existing record
                existing.formal_name = lp_entry.formal_name
                existing.lp_type = lp_entry.lp_type
                existing.jurisdiction = lp_entry.jurisdiction
                existing.website_url = lp_entry.website_url
                existing.region = lp_entry.region
                existing.country_code = lp_entry.country_code
                existing.aum_usd_billions = lp_entry.aum_usd_billions
                existing.has_cafr = 1 if lp_entry.has_cafr else 0
                existing.sec_crd_number = lp_entry.sec_crd_number
                existing.collection_priority = lp_entry.collection_priority
                updated += 1
            else:
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

            # Commit after each LP to avoid bulk insert issues
            db.commit()

        except Exception as e:
            db.rollback()
            logger.warning(f"Error seeding LP {lp_entry.name}: {e}")
            skipped += 1
            continue

    return {
        "message": f"Seeded {created} new LPs, updated {updated}, skipped {skipped}",
        "created": created,
        "updated": updated,
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
    members = (
        db.query(LpGovernanceMember)
        .filter(
            LpGovernanceMember.lp_id == lp_id,
            LpGovernanceMember.is_current == 1,
        )
        .order_by(LpGovernanceMember.governance_role)
        .all()
    )

    # Get recent meetings
    meetings = (
        db.query(LpBoardMeeting)
        .filter(
            LpBoardMeeting.lp_id == lp_id,
        )
        .order_by(LpBoardMeeting.meeting_date.desc())
        .limit(20)
        .all()
    )

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
    returns = (
        db.query(LpPerformanceReturn)
        .filter(
            LpPerformanceReturn.lp_id == lp_id,
        )
        .order_by(LpPerformanceReturn.fiscal_year.desc())
        .all()
    )

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
# LP Query Endpoints
# =============================================================================


@router.get("/lps/{lp_id}/allocation-history")
async def get_lp_allocation_history(
    lp_id: int,
    years: int = Query(default=5, le=20, description="Number of years of history"),
    db: Session = Depends(get_db),
):
    """
    Get allocation history for an LP.

    Returns strategy snapshots showing asset allocation over time.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Get strategy snapshots
    snapshots = (
        db.query(LpStrategySnapshot)
        .filter(LpStrategySnapshot.lp_id == lp_id)
        .order_by(LpStrategySnapshot.fiscal_year.desc())
        .limit(years)
        .all()
    )

    # Get asset class allocations
    allocations = (
        db.query(LpAssetClassTargetAllocation)
        .filter(LpAssetClassTargetAllocation.lp_id == lp_id)
        .order_by(LpAssetClassTargetAllocation.fiscal_year.desc())
        .all()
    )

    return {
        "lp_id": lp_id,
        "lp_name": lp.name,
        "snapshots": [
            {
                "fiscal_year": s.fiscal_year,
                "fiscal_quarter": s.fiscal_quarter,
                "program": s.program,
                "summary_text": s.summary_text,
                "risk_positioning": s.risk_positioning,
                "liquidity_profile": s.liquidity_profile,
                "tilt_description": s.tilt_description,
            }
            for s in snapshots
        ],
        "allocations": [
            {
                "fiscal_year": a.fiscal_year,
                "asset_class": a.asset_class,
                "target_allocation_pct": a.target_allocation_pct,
                "actual_allocation_pct": a.actual_allocation_pct,
            }
            for a in allocations
        ],
    }


@router.get("/lps/{lp_id}/holdings")
async def get_lp_holdings(
    lp_id: int,
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
):
    """
    Get 13F holdings for an LP (for institutional investors).

    Returns portfolio companies from SEC 13F filings.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Get portfolio companies from 13F
    holdings = (
        db.query(PortfolioCompany)
        .filter(
            PortfolioCompany.investor_id == lp_id,
            PortfolioCompany.investor_type == "lp",
        )
        .limit(limit)
        .all()
    )

    # market_value_usd is stored as string, convert safely
    total_value = 0
    for h in holdings:
        try:
            if h.market_value_usd:
                total_value += float(h.market_value_usd)
        except (ValueError, TypeError):
            pass

    return {
        "lp_id": lp_id,
        "lp_name": lp.name,
        "total_holdings": len(holdings),
        "total_market_value_usd": total_value,
        "holdings": [
            {
                "id": h.id,
                "company_name": h.company_name,
                "cusip": h.company_cusip,
                "ticker_symbol": h.company_ticker,
                "shares_held": h.shares_held,
                "market_value_usd": h.market_value_usd,
                "investment_type": h.investment_type,
                "investment_date": h.investment_date,
                "source_type": h.source_type,
            }
            for h in holdings
        ],
    }


@router.get("/lps/{lp_id}/managers")
async def get_lp_managers(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Get external managers for an LP.

    Returns fund managers, GPs, and investment vehicles used by the LP.
    Manager exposures are linked through strategy snapshots.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Get strategy snapshot IDs for this LP
    snapshot_ids = (
        db.query(LpStrategySnapshot.id).filter(LpStrategySnapshot.lp_id == lp_id).all()
    )
    snapshot_id_list = [s[0] for s in snapshot_ids]

    if not snapshot_id_list:
        return {
            "lp_id": lp_id,
            "lp_name": lp.name,
            "total_managers": 0,
            "managers": [],
        }

    # Get manager exposures via strategy snapshots
    exposures = (
        db.query(LpManagerOrVehicleExposure)
        .filter(LpManagerOrVehicleExposure.strategy_id.in_(snapshot_id_list))
        .all()
    )

    return {
        "lp_id": lp_id,
        "lp_name": lp.name,
        "total_managers": len(exposures),
        "managers": [
            {
                "id": e.id,
                "manager_name": e.manager_name,
                "vehicle_name": e.vehicle_name,
                "vehicle_type": e.vehicle_type,
                "asset_class": e.asset_class,
            }
            for e in exposures
        ],
    }


@router.get("/lps/{lp_id}/contacts")
async def get_lp_contacts(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Get key contacts for an LP.

    Returns investment officers, board members, and other personnel.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Use raw SQL to avoid column mismatch issues
    from sqlalchemy import text

    result = db.execute(
        text("""
        SELECT id, lp_id, full_name, title, email, phone, linkedin_url, source_type, source_url
        FROM lp_key_contact
        WHERE lp_id = :lp_id
    """),
        {"lp_id": lp_id},
    )

    contacts = []
    for row in result:
        contacts.append(
            {
                "id": row[0],
                "full_name": row[2],
                "title": row[3],
                "email": row[4],
                "phone": row[5],
                "linkedin_url": row[6],
                "source_type": row[7],
                "source_url": row[8],
            }
        )

    return {
        "lp_id": lp_id,
        "lp_name": lp.name,
        "contact_count": len(contacts),
        "contacts": contacts,
    }


@router.get("/lps/{lp_id}/summary")
async def get_lp_summary(
    lp_id: int,
    db: Session = Depends(get_db),
):
    """
    Get a comprehensive summary of an LP.

    Includes basic info, recent allocations, performance, and contacts.
    """
    lp = db.query(LpFund).filter(LpFund.id == lp_id).first()
    if not lp:
        raise HTTPException(status_code=404, detail="LP not found")

    # Get latest strategy snapshot
    latest_snapshot = (
        db.query(LpStrategySnapshot)
        .filter(LpStrategySnapshot.lp_id == lp_id)
        .order_by(LpStrategySnapshot.fiscal_year.desc())
        .first()
    )

    # Get latest performance
    latest_performance = (
        db.query(LpPerformanceReturn)
        .filter(LpPerformanceReturn.lp_id == lp_id)
        .order_by(LpPerformanceReturn.fiscal_year.desc())
        .first()
    )

    # Count contacts, holdings, managers using raw SQL to avoid model mismatch
    from sqlalchemy import text

    contact_count = (
        db.execute(
            text("SELECT COUNT(*) FROM lp_key_contact WHERE lp_id = :lp_id"),
            {"lp_id": lp_id},
        ).scalar()
        or 0
    )
    holding_count = (
        db.query(PortfolioCompany)
        .filter(
            PortfolioCompany.investor_id == lp_id,
            PortfolioCompany.investor_type == "lp",
        )
        .count()
    )
    # Manager count via strategy snapshots
    snapshot_ids = (
        db.query(LpStrategySnapshot.id).filter(LpStrategySnapshot.lp_id == lp_id).all()
    )
    snapshot_id_list = [s[0] for s in snapshot_ids]
    manager_count = 0
    if snapshot_id_list:
        manager_count = (
            db.query(LpManagerOrVehicleExposure)
            .filter(LpManagerOrVehicleExposure.strategy_id.in_(snapshot_id_list))
            .count()
        )

    return {
        "lp": {
            "id": lp.id,
            "name": lp.name,
            "formal_name": lp.formal_name,
            "lp_type": lp.lp_type,
            "jurisdiction": lp.jurisdiction,
            "region": lp.region,
            "country_code": lp.country_code,
            "website_url": lp.website_url,
            "aum_usd_billions": lp.aum_usd_billions,
        },
        "latest_snapshot": {
            "fiscal_year": latest_snapshot.fiscal_year if latest_snapshot else None,
            "program": latest_snapshot.program if latest_snapshot else None,
            "summary_text": latest_snapshot.summary_text if latest_snapshot else None,
            "risk_positioning": latest_snapshot.risk_positioning
            if latest_snapshot
            else None,
        }
        if latest_snapshot
        else None,
        "latest_performance": {
            "fiscal_year": latest_performance.fiscal_year
            if latest_performance
            else None,
            "one_year_return_pct": latest_performance.one_year_return_pct
            if latest_performance
            else None,
            "five_year_return_pct": latest_performance.five_year_return_pct
            if latest_performance
            else None,
        }
        if latest_performance
        else None,
        "counts": {
            "contacts": contact_count,
            "holdings": holding_count,
            "managers": manager_count,
        },
    }


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
                next_run = next_run.replace(
                    year=now.year + 1, month=1, day=day_of_month or 1
                )
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
