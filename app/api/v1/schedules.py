"""
Schedule management endpoints.

Provides API for creating, updating, and managing automated ingestion schedules.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from datetime import datetime

from app.core.database import get_db
from app.core.models import IngestionSchedule, ScheduleFrequency
from app.core import scheduler_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/schedules", tags=["schedules"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class ScheduleCreate(BaseModel):
    """Request schema for creating a schedule."""

    name: str = Field(..., min_length=1, max_length=255)
    source: str = Field(..., min_length=1, max_length=50)
    config: dict = Field(default_factory=dict)
    frequency: ScheduleFrequency = ScheduleFrequency.DAILY
    hour: int = Field(default=6, ge=0, le=23)
    cron_expression: Optional[str] = None
    day_of_week: Optional[int] = Field(default=None, ge=0, le=6)
    day_of_month: Optional[int] = Field(default=None, ge=1, le=31)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = Field(default=5, ge=1, le=10)


class ScheduleUpdate(BaseModel):
    """Request schema for updating a schedule."""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    config: Optional[dict] = None
    frequency: Optional[ScheduleFrequency] = None
    hour: Optional[int] = Field(None, ge=0, le=23)
    cron_expression: Optional[str] = None
    day_of_week: Optional[int] = Field(None, ge=0, le=6)
    day_of_month: Optional[int] = Field(None, ge=1, le=31)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = Field(None, ge=1, le=10)


class ScheduleResponse(BaseModel):
    """Response schema for schedule information."""

    id: int
    name: str
    source: str
    config: dict
    frequency: ScheduleFrequency
    hour: Optional[int]
    cron_expression: Optional[str]
    day_of_week: Optional[int]
    day_of_month: Optional[int]
    is_active: bool
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    last_job_id: Optional[int]
    created_at: datetime
    updated_at: datetime
    description: Optional[str]
    priority: int

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_active(cls, obj: IngestionSchedule) -> "ScheduleResponse":
        """Convert ORM object with is_active as bool."""
        data = {
            "id": obj.id,
            "name": obj.name,
            "source": obj.source,
            "config": obj.config,
            "frequency": obj.frequency,
            "hour": obj.hour,
            "cron_expression": obj.cron_expression,
            "day_of_week": obj.day_of_week,
            "day_of_month": obj.day_of_month,
            "is_active": bool(obj.is_active),
            "last_run_at": obj.last_run_at,
            "next_run_at": obj.next_run_at,
            "last_job_id": obj.last_job_id,
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
            "description": obj.description,
            "priority": obj.priority,
        }
        return cls(**data)


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/status")
def get_scheduler_status():
    """
    Get current scheduler status.

    Returns whether the scheduler is running and list of active jobs.
    """
    return scheduler_service.get_scheduler_status()


@router.post("/start")
def start_scheduler(db: Session = Depends(get_db)):
    """
    Start the scheduler and load all active schedules.
    """
    scheduler_service.start_scheduler()
    count = scheduler_service.load_all_schedules(db)
    return {"message": "Scheduler started", "schedules_loaded": count}


@router.post("/stop")
def stop_scheduler():
    """
    Stop the scheduler.

    All scheduled jobs will be paused until the scheduler is restarted.
    """
    scheduler_service.stop_scheduler()
    return {"message": "Scheduler stopped"}


@router.get("", response_model=List[ScheduleResponse])
def list_schedules(
    source: Optional[str] = None,
    active_only: bool = False,
    db: Session = Depends(get_db),
) -> List[ScheduleResponse]:
    """
    List all schedules with optional filtering.
    """
    query = db.query(IngestionSchedule)

    if source:
        query = query.filter(IngestionSchedule.source == source)
    if active_only:
        query = query.filter(IngestionSchedule.is_active == 1)

    query = query.order_by(IngestionSchedule.priority, IngestionSchedule.name)
    schedules = query.all()

    return [ScheduleResponse.from_orm_with_active(s) for s in schedules]


@router.post("", response_model=ScheduleResponse, status_code=201)
def create_schedule(
    schedule_request: ScheduleCreate, db: Session = Depends(get_db)
) -> ScheduleResponse:
    """
    Create a new ingestion schedule.
    """
    # Check for duplicate name
    existing = (
        db.query(IngestionSchedule)
        .filter(IngestionSchedule.name == schedule_request.name)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Schedule name already exists")

    schedule = scheduler_service.create_schedule(
        db=db,
        name=schedule_request.name,
        source=schedule_request.source,
        config=schedule_request.config,
        frequency=schedule_request.frequency,
        hour=schedule_request.hour,
        cron_expression=schedule_request.cron_expression,
        day_of_week=schedule_request.day_of_week,
        day_of_month=schedule_request.day_of_month,
        description=schedule_request.description,
        is_active=schedule_request.is_active,
        priority=schedule_request.priority,
    )

    return ScheduleResponse.from_orm_with_active(schedule)


@router.get("/{schedule_id}", response_model=ScheduleResponse)
def get_schedule(schedule_id: int, db: Session = Depends(get_db)) -> ScheduleResponse:
    """
    Get a specific schedule by ID.
    """
    schedule = (
        db.query(IngestionSchedule).filter(IngestionSchedule.id == schedule_id).first()
    )

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    return ScheduleResponse.from_orm_with_active(schedule)


@router.put("/{schedule_id}", response_model=ScheduleResponse)
def update_schedule(
    schedule_id: int, schedule_request: ScheduleUpdate, db: Session = Depends(get_db)
) -> ScheduleResponse:
    """
    Update an existing schedule.
    """
    # Get update data, excluding None values
    update_data = schedule_request.model_dump(exclude_none=True)

    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    schedule = scheduler_service.update_schedule(db, schedule_id, **update_data)

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    return ScheduleResponse.from_orm_with_active(schedule)


@router.delete("/{schedule_id}")
def delete_schedule(schedule_id: int, db: Session = Depends(get_db)):
    """
    Delete a schedule.
    """
    success = scheduler_service.delete_schedule(db, schedule_id)

    if not success:
        raise HTTPException(status_code=404, detail="Schedule not found")

    return {"message": f"Schedule {schedule_id} deleted"}


@router.post("/{schedule_id}/activate", response_model=ScheduleResponse)
def activate_schedule(
    schedule_id: int, db: Session = Depends(get_db)
) -> ScheduleResponse:
    """
    Activate a paused schedule.
    """
    schedule = scheduler_service.update_schedule(db, schedule_id, is_active=True)

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    return ScheduleResponse.from_orm_with_active(schedule)


@router.post("/{schedule_id}/pause", response_model=ScheduleResponse)
def pause_schedule(schedule_id: int, db: Session = Depends(get_db)) -> ScheduleResponse:
    """
    Pause an active schedule.
    """
    schedule = scheduler_service.update_schedule(db, schedule_id, is_active=False)

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    return ScheduleResponse.from_orm_with_active(schedule)


@router.post("/{schedule_id}/run")
async def run_schedule_now(schedule_id: int, db: Session = Depends(get_db)):
    """
    Manually trigger a schedule to run immediately.

    Creates a new job and executes it, regardless of the schedule's next_run_at.
    """
    schedule = (
        db.query(IngestionSchedule).filter(IngestionSchedule.id == schedule_id).first()
    )

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    # Run the scheduled job
    await scheduler_service.run_scheduled_job(schedule_id)

    # Refresh to get updated last_job_id
    db.refresh(schedule)

    return {
        "message": f"Schedule '{schedule.name}' triggered",
        "job_id": schedule.last_job_id,
    }


@router.get("/{schedule_id}/history")
def get_schedule_history(
    schedule_id: int,
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get recent job history for a schedule.
    """
    schedule = (
        db.query(IngestionSchedule).filter(IngestionSchedule.id == schedule_id).first()
    )

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    jobs = scheduler_service.get_schedule_history(db, schedule_id, limit)

    return {
        "schedule_id": schedule_id,
        "schedule_name": schedule.name,
        "jobs": [
            {
                "id": job.id,
                "status": job.status.value,
                "created_at": job.created_at.isoformat(),
                "completed_at": job.completed_at.isoformat()
                if job.completed_at
                else None,
                "rows_inserted": job.rows_inserted,
                "error_message": job.error_message[:200] if job.error_message else None,
            }
            for job in jobs
        ],
    }


@router.post("/defaults/create")
def create_default_schedules(db: Session = Depends(get_db)):
    """
    Create default schedule templates.

    Creates predefined schedules for common data sources.
    Schedules are created in a paused state by default.
    """
    created = scheduler_service.create_default_schedules(db)

    return {
        "message": f"Created {len(created)} default schedules",
        "schedules": [s.name for s in created],
    }


# =============================================================================
# Stuck Job Cleanup Endpoints
# =============================================================================


@router.post("/cleanup/stuck-jobs")
async def cleanup_stuck_jobs(timeout_hours: int = 2):
    """
    Manually trigger cleanup of stuck jobs.

    Jobs that have been in RUNNING status for longer than the timeout
    will be marked as FAILED. Sends webhook notifications if configured.

    Args:
        timeout_hours: Hours after which a running job is considered stuck (default 2)

    Returns:
        Cleanup results including list of cleaned up jobs
    """
    result = await scheduler_service.cleanup_stuck_jobs(timeout_hours=timeout_hours)
    return result


@router.get("/cleanup/status")
def get_cleanup_status():
    """
    Get status of the automatic stuck job cleanup task.

    Returns whether the cleanup job is registered and when it will next run.
    """
    scheduler = scheduler_service.get_scheduler()
    job_id = "system_cleanup_stuck_jobs"

    cleanup_job = scheduler.get_job(job_id)

    if cleanup_job:
        return {
            "enabled": True,
            "job_id": job_id,
            "name": cleanup_job.name,
            "next_run": cleanup_job.next_run_time.isoformat()
            if cleanup_job.next_run_time
            else None,
            "trigger": str(cleanup_job.trigger),
            "timeout_hours": scheduler_service.STUCK_JOB_TIMEOUT_HOURS,
        }
    else:
        return {
            "enabled": False,
            "job_id": job_id,
            "message": "Cleanup job is not registered",
        }


@router.post("/cleanup/enable")
def enable_cleanup(interval_minutes: int = 30):
    """
    Enable automatic stuck job cleanup.

    Args:
        interval_minutes: How often to run cleanup (default 30 minutes)
    """
    success = scheduler_service.register_cleanup_job(interval_minutes=interval_minutes)

    if success:
        return {
            "message": f"Stuck job cleanup enabled (runs every {interval_minutes} minutes)",
            "interval_minutes": interval_minutes,
            "timeout_hours": scheduler_service.STUCK_JOB_TIMEOUT_HOURS,
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to enable cleanup job")


@router.post("/cleanup/disable")
def disable_cleanup():
    """
    Disable automatic stuck job cleanup.
    """
    success = scheduler_service.unregister_cleanup_job()

    if success:
        return {"message": "Stuck job cleanup disabled"}
    else:
        raise HTTPException(status_code=500, detail="Failed to disable cleanup job")


# =============================================================================
# Automatic Retry Endpoints
# =============================================================================


@router.get("/retry/status")
def get_retry_processor_status():
    """
    Get status of the automatic retry processor.

    Returns whether the retry processor is registered and when it will next run.
    """
    scheduler = scheduler_service.get_scheduler()
    job_id = "system_retry_processor"

    retry_job = scheduler.get_job(job_id)

    if retry_job:
        return {
            "enabled": True,
            "job_id": job_id,
            "name": retry_job.name,
            "next_run": retry_job.next_run_time.isoformat()
            if retry_job.next_run_time
            else None,
            "trigger": str(retry_job.trigger),
            "backoff_settings": {
                "base_delay_minutes": 5,
                "max_delay_minutes": 1440,
                "multiplier": 2,
            },
        }
    else:
        return {
            "enabled": False,
            "job_id": job_id,
            "message": "Retry processor is not registered",
        }


@router.post("/retry/enable")
def enable_retry_processor(interval_minutes: int = 5):
    """
    Enable automatic retry processing.

    Args:
        interval_minutes: How often to check for jobs ready to retry (default 5 minutes)
    """
    success = scheduler_service.register_retry_processor(
        interval_minutes=interval_minutes
    )

    if success:
        return {
            "message": f"Automatic retry processor enabled (runs every {interval_minutes} minutes)",
            "interval_minutes": interval_minutes,
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to enable retry processor")


@router.post("/retry/disable")
def disable_retry_processor():
    """
    Disable automatic retry processing.
    """
    success = scheduler_service.unregister_retry_processor()

    if success:
        return {"message": "Automatic retry processor disabled"}
    else:
        raise HTTPException(status_code=500, detail="Failed to disable retry processor")


@router.post("/retry/process-now")
async def process_retries_now(limit: int = Query(default=10, ge=1, le=50)):
    """
    Manually trigger processing of scheduled retries.

    Args:
        limit: Maximum number of jobs to process (default 10)

    Returns:
        Processing results including jobs retried
    """
    from app.core.retry_service import process_scheduled_retries

    result = await process_scheduled_retries(limit=limit)
    return result
