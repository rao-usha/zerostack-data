"""
Scheduler service for automated data ingestion.

Uses APScheduler to run ingestion jobs on configurable schedules.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.orm import Session

from app.core.models import IngestionSchedule, IngestionJob, JobStatus, ScheduleFrequency
from app.core.config import get_settings
from app.core.database import get_session_factory

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: Optional[AsyncIOScheduler] = None


def get_scheduler() -> AsyncIOScheduler:
    """Get or create the global scheduler instance with persistent job store."""
    global _scheduler
    if _scheduler is None:
        settings = get_settings()
        jobstores = {
            "default": SQLAlchemyJobStore(url=settings.database_url),
        }
        _scheduler = AsyncIOScheduler(jobstores=jobstores)
    return _scheduler


async def run_scheduled_job(schedule_id: int):
    """
    Execute a scheduled ingestion job.

    This function is called by APScheduler when a schedule triggers.
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Get the schedule
        schedule = db.query(IngestionSchedule).filter(
            IngestionSchedule.id == schedule_id
        ).first()

        if not schedule:
            logger.error(f"Schedule {schedule_id} not found")
            return

        if not schedule.is_active:
            logger.info(f"Schedule {schedule.name} is paused, skipping")
            return

        logger.info(f"Running scheduled job: {schedule.name} (source={schedule.source})")

        # Create a new ingestion job
        job = IngestionJob(
            source=schedule.source,
            status=JobStatus.PENDING,
            config=schedule.config
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        # Update schedule tracking
        schedule.last_run_at = datetime.utcnow()
        schedule.last_job_id = job.id
        schedule.next_run_at = _calculate_next_run(schedule)
        db.commit()

        logger.info(f"Created job {job.id} for schedule {schedule.name}")

        # Execute the job asynchronously
        await _execute_ingestion_job(db, job)

    except Exception as e:
        logger.error(f"Error running scheduled job {schedule_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _execute_ingestion_job(db: Session, job: IngestionJob):
    """Execute an ingestion job based on its source."""
    from app.api.v1.jobs import run_ingestion_job

    try:
        await run_ingestion_job(job.id, job.source, job.config)
    except Exception as e:
        logger.error(f"Error executing job {job.id}: {e}", exc_info=True)
        # Job status should already be updated by run_ingestion_job


def _calculate_next_run(schedule: IngestionSchedule) -> datetime:
    """Calculate the next run time for a schedule."""
    now = datetime.utcnow()

    if schedule.frequency == ScheduleFrequency.HOURLY:
        return now + timedelta(hours=1)

    elif schedule.frequency == ScheduleFrequency.DAILY:
        next_run = now.replace(
            hour=schedule.hour or 6,
            minute=0,
            second=0,
            microsecond=0
        )
        if next_run <= now:
            next_run += timedelta(days=1)
        return next_run

    elif schedule.frequency == ScheduleFrequency.WEEKLY:
        days_ahead = (schedule.day_of_week or 0) - now.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        next_run = now + timedelta(days=days_ahead)
        next_run = next_run.replace(
            hour=schedule.hour or 6,
            minute=0,
            second=0,
            microsecond=0
        )
        return next_run

    elif schedule.frequency == ScheduleFrequency.MONTHLY:
        day = schedule.day_of_month or 1
        next_run = now.replace(
            day=min(day, 28),  # Safe for all months
            hour=schedule.hour or 6,
            minute=0,
            second=0,
            microsecond=0
        )
        if next_run <= now:
            # Move to next month
            if now.month == 12:
                next_run = next_run.replace(year=now.year + 1, month=1)
            else:
                next_run = next_run.replace(month=now.month + 1)
        return next_run

    else:
        # Default to next hour
        return now + timedelta(hours=1)


def _get_trigger_for_schedule(schedule: IngestionSchedule):
    """Create an APScheduler trigger for a schedule."""
    if schedule.frequency == ScheduleFrequency.CUSTOM and schedule.cron_expression:
        return CronTrigger.from_crontab(schedule.cron_expression)

    elif schedule.frequency == ScheduleFrequency.HOURLY:
        return IntervalTrigger(hours=1)

    elif schedule.frequency == ScheduleFrequency.DAILY:
        return CronTrigger(hour=schedule.hour or 6, minute=0)

    elif schedule.frequency == ScheduleFrequency.WEEKLY:
        return CronTrigger(
            day_of_week=schedule.day_of_week or 0,
            hour=schedule.hour or 6,
            minute=0
        )

    elif schedule.frequency == ScheduleFrequency.MONTHLY:
        return CronTrigger(
            day=schedule.day_of_month or 1,
            hour=schedule.hour or 6,
            minute=0
        )

    else:
        # Default to daily at 6 AM
        return CronTrigger(hour=6, minute=0)


def register_schedule(schedule: IngestionSchedule) -> bool:
    """
    Register a schedule with the APScheduler.

    Args:
        schedule: IngestionSchedule instance

    Returns:
        True if registered successfully
    """
    scheduler = get_scheduler()
    job_id = f"schedule_{schedule.id}"

    try:
        # Remove existing job if any
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)

        if not schedule.is_active:
            logger.info(f"Schedule {schedule.name} is not active, not registering")
            return True

        # Create trigger
        trigger = _get_trigger_for_schedule(schedule)

        # Add job to scheduler
        scheduler.add_job(
            run_scheduled_job,
            trigger=trigger,
            id=job_id,
            args=[schedule.id],
            name=schedule.name,
            replace_existing=True
        )

        logger.info(f"Registered schedule: {schedule.name} ({schedule.frequency.value})")
        return True

    except Exception as e:
        logger.error(f"Failed to register schedule {schedule.name}: {e}")
        return False


def unregister_schedule(schedule_id: int) -> bool:
    """Remove a schedule from the APScheduler."""
    scheduler = get_scheduler()
    job_id = f"schedule_{schedule_id}"

    try:
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)
            logger.info(f"Unregistered schedule {schedule_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to unregister schedule {schedule_id}: {e}")
        return False


def load_all_schedules(db: Session) -> int:
    """
    Load all active schedules from database into APScheduler.

    Returns number of schedules loaded.
    """
    schedules = db.query(IngestionSchedule).filter(
        IngestionSchedule.is_active == 1
    ).all()

    count = 0
    for schedule in schedules:
        if register_schedule(schedule):
            count += 1

    logger.info(f"Loaded {count} active schedules")
    return count


def start_scheduler():
    """Start the scheduler if not already running."""
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler():
    """Stop the scheduler."""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def get_scheduler_status() -> Dict[str, Any]:
    """Get current scheduler status."""
    scheduler = get_scheduler()

    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })

    return {
        "running": scheduler.running,
        "job_count": len(jobs),
        "jobs": jobs
    }


# =============================================================================
# Schedule Management Functions
# =============================================================================

def create_schedule(
    db: Session,
    name: str,
    source: str,
    config: Dict[str, Any],
    frequency: ScheduleFrequency = ScheduleFrequency.DAILY,
    hour: int = 6,
    cron_expression: Optional[str] = None,
    day_of_week: Optional[int] = None,
    day_of_month: Optional[int] = None,
    description: Optional[str] = None,
    is_active: bool = True,
    priority: int = 5
) -> IngestionSchedule:
    """
    Create a new ingestion schedule.

    Args:
        db: Database session
        name: Human-readable name for the schedule
        source: Data source identifier
        config: Source-specific configuration
        frequency: How often to run
        hour: Hour to run (0-23) for daily/weekly/monthly
        cron_expression: Custom cron expression (for CUSTOM frequency)
        day_of_week: Day of week (0=Monday) for weekly schedules
        day_of_month: Day of month (1-31) for monthly schedules
        description: Optional description
        is_active: Whether schedule is active
        priority: Priority level (1-10, 1=highest)

    Returns:
        Created IngestionSchedule
    """
    schedule = IngestionSchedule(
        name=name,
        source=source,
        config=config,
        frequency=frequency,
        hour=hour,
        cron_expression=cron_expression,
        day_of_week=day_of_week,
        day_of_month=day_of_month,
        description=description,
        is_active=1 if is_active else 0,
        priority=priority,
        next_run_at=_calculate_next_run(IngestionSchedule(
            frequency=frequency,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month
        ))
    )

    db.add(schedule)
    db.commit()
    db.refresh(schedule)

    # Register with scheduler if active
    if is_active:
        register_schedule(schedule)

    logger.info(f"Created schedule: {name}")
    return schedule


def update_schedule(
    db: Session,
    schedule_id: int,
    **kwargs
) -> Optional[IngestionSchedule]:
    """Update an existing schedule."""
    schedule = db.query(IngestionSchedule).filter(
        IngestionSchedule.id == schedule_id
    ).first()

    if not schedule:
        return None

    # Update fields
    for key, value in kwargs.items():
        if hasattr(schedule, key):
            if key == 'is_active':
                value = 1 if value else 0
            setattr(schedule, key, value)

    # Recalculate next run
    schedule.next_run_at = _calculate_next_run(schedule)
    schedule.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(schedule)

    # Update scheduler registration
    if schedule.is_active:
        register_schedule(schedule)
    else:
        unregister_schedule(schedule.id)

    return schedule


def delete_schedule(db: Session, schedule_id: int) -> bool:
    """Delete a schedule."""
    schedule = db.query(IngestionSchedule).filter(
        IngestionSchedule.id == schedule_id
    ).first()

    if not schedule:
        return False

    # Unregister from scheduler
    unregister_schedule(schedule_id)

    # Delete from database
    db.delete(schedule)
    db.commit()

    logger.info(f"Deleted schedule {schedule_id}")
    return True


def get_schedule_history(
    db: Session,
    schedule_id: int,
    limit: int = 10
) -> List[IngestionJob]:
    """Get recent jobs for a schedule."""
    schedule = db.query(IngestionSchedule).filter(
        IngestionSchedule.id == schedule_id
    ).first()

    if not schedule:
        return []

    return db.query(IngestionJob).filter(
        IngestionJob.source == schedule.source,
        IngestionJob.config == schedule.config
    ).order_by(
        IngestionJob.created_at.desc()
    ).limit(limit).all()


# =============================================================================
# Default Schedule Templates
# =============================================================================

DEFAULT_SCHEDULES = [
    {
        "name": "FEMA Disasters - Daily",
        "source": "fema",
        "config": {"dataset": "disasters", "year_start": 2020},
        "frequency": ScheduleFrequency.DAILY,
        "hour": 6,
        "description": "Daily update of FEMA disaster declarations",
        "priority": 3
    },
    {
        "name": "Treasury Daily Balance",
        "source": "treasury",
        "config": {"dataset": "daily_balance"},
        "frequency": ScheduleFrequency.DAILY,
        "hour": 18,
        "description": "Daily Treasury statement (updates late afternoon)",
        "priority": 2
    },
    {
        "name": "FRED Economic Indicators - Weekly",
        "source": "fred",
        "config": {"category": "gdp"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 7,
        "day_of_week": 0,  # Monday
        "description": "Weekly update of FRED GDP data",
        "priority": 4
    },
    {
        "name": "BLS Employment - Monthly",
        "source": "bls",
        "config": {"dataset": "ces"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 8,
        "day_of_month": 5,
        "description": "Monthly employment data (BLS releases ~first Friday)",
        "priority": 2
    },
    {
        "name": "Census ACS Population - Monthly",
        "source": "census",
        "config": {"survey": "acs5", "table_id": "B01001", "geo_level": "state"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 3,
        "day_of_month": 15,
        "description": "Monthly refresh of ACS population estimates",
        "priority": 5
    },
]


def create_default_schedules(db: Session) -> List[IngestionSchedule]:
    """Create default schedule templates if they don't exist."""
    created = []

    for template in DEFAULT_SCHEDULES:
        # Check if schedule already exists
        existing = db.query(IngestionSchedule).filter(
            IngestionSchedule.name == template["name"]
        ).first()

        if not existing:
            schedule = create_schedule(
                db=db,
                name=template["name"],
                source=template["source"],
                config=template["config"],
                frequency=template["frequency"],
                hour=template.get("hour", 6),
                day_of_week=template.get("day_of_week"),
                day_of_month=template.get("day_of_month"),
                description=template.get("description"),
                is_active=False,  # Start paused by default
                priority=template.get("priority", 5)
            )
            created.append(schedule)
            logger.info(f"Created default schedule: {template['name']}")

    return created


# =============================================================================
# Automatic Stuck Job Cleanup
# =============================================================================

# Default timeout for stuck jobs (in hours)
# Increased from 2 to 6 — many government API ingestions (Census, EIA, FCC)
# make hundreds of rate-limited requests and legitimately run 2-4 hours.
STUCK_JOB_TIMEOUT_HOURS = 6


async def cleanup_stuck_jobs(timeout_hours: int = STUCK_JOB_TIMEOUT_HOURS) -> Dict[str, Any]:
    """
    Find and mark stuck jobs as failed.

    Jobs are considered stuck if they have been in RUNNING status
    for longer than the timeout threshold.

    Args:
        timeout_hours: Hours after which a running job is considered stuck

    Returns:
        Dictionary with cleanup results
    """
    from app.core import webhook_service

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Find stuck jobs
        cutoff = datetime.utcnow() - timedelta(hours=timeout_hours)

        stuck_jobs = db.query(IngestionJob).filter(
            IngestionJob.status == JobStatus.RUNNING,
            IngestionJob.started_at < cutoff
        ).all()

        if not stuck_jobs:
            logger.info("No stuck jobs found during cleanup")
            return {
                "cleaned_up": 0,
                "jobs": [],
                "timeout_hours": timeout_hours
            }

        # Mark stuck jobs as failed
        cleaned_jobs = []
        for job in stuck_jobs:
            running_hours = (datetime.utcnow() - job.started_at).total_seconds() / 3600

            job.status = JobStatus.FAILED
            job.error_message = f"Job timed out after {running_hours:.1f} hours (threshold: {timeout_hours}h) - automatically marked as failed"
            job.completed_at = datetime.utcnow()

            cleaned_jobs.append({
                "job_id": job.id,
                "source": job.source,
                "running_hours": round(running_hours, 2),
                "started_at": job.started_at.isoformat()
            })

            logger.warning(
                f"Marked stuck job {job.id} ({job.source}) as failed - "
                f"was running for {running_hours:.1f} hours"
            )

        db.commit()

        logger.info(f"Cleaned up {len(cleaned_jobs)} stuck jobs")

        # Send webhook notification for cleanup
        try:
            await webhook_service.notify_cleanup_completed(
                cleaned_up=len(cleaned_jobs),
                jobs=cleaned_jobs,
                timeout_hours=timeout_hours
            )
        except Exception as e:
            logger.warning(f"Failed to send cleanup webhook notification: {e}")

        return {
            "cleaned_up": len(cleaned_jobs),
            "jobs": cleaned_jobs,
            "timeout_hours": timeout_hours,
            "cleanup_time": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Error during stuck job cleanup: {e}", exc_info=True)
        db.rollback()
        return {
            "cleaned_up": 0,
            "error": str(e),
            "timeout_hours": timeout_hours
        }

    finally:
        db.close()


def register_cleanup_job(interval_minutes: int = 30) -> bool:
    """
    Register the stuck job cleanup task with the scheduler.

    Args:
        interval_minutes: How often to run cleanup (default 30 minutes)

    Returns:
        True if registered successfully
    """
    scheduler = get_scheduler()
    job_id = "system_cleanup_stuck_jobs"

    try:
        # Remove existing job if any
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)

        # Add cleanup job
        scheduler.add_job(
            cleanup_stuck_jobs,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            name="Stuck Job Cleanup",
            replace_existing=True
        )

        logger.info(f"Registered stuck job cleanup to run every {interval_minutes} minutes")
        return True

    except Exception as e:
        logger.error(f"Failed to register cleanup job: {e}")
        return False


def unregister_cleanup_job() -> bool:
    """Remove the stuck job cleanup task from the scheduler."""
    scheduler = get_scheduler()
    job_id = "system_cleanup_stuck_jobs"

    try:
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)
            logger.info("Unregistered stuck job cleanup")
        return True
    except Exception as e:
        logger.error(f"Failed to unregister cleanup job: {e}")
        return False


# =============================================================================
# Automatic Retry Processing
# =============================================================================

def register_retry_processor(interval_minutes: int = 5) -> bool:
    """
    Register the automatic retry processor with the scheduler.

    Processes failed jobs that are scheduled for retry.

    Args:
        interval_minutes: How often to check for jobs ready to retry (default 5 min)

    Returns:
        True if registered successfully
    """
    from app.core.retry_service import process_scheduled_retries

    scheduler = get_scheduler()
    job_id = "system_retry_processor"

    try:
        # Remove existing job if any
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)

        # Add retry processor job
        scheduler.add_job(
            process_scheduled_retries,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            name="Automatic Retry Processor",
            replace_existing=True
        )

        logger.info(f"Registered automatic retry processor to run every {interval_minutes} minutes")
        return True

    except Exception as e:
        logger.error(f"Failed to register retry processor: {e}")
        return False


def unregister_retry_processor() -> bool:
    """Remove the automatic retry processor from the scheduler."""
    scheduler = get_scheduler()
    job_id = "system_retry_processor"

    try:
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)
            logger.info("Unregistered automatic retry processor")
        return True
    except Exception as e:
        logger.error(f"Failed to unregister retry processor: {e}")
        return False
