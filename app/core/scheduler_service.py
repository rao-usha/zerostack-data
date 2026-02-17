"""
Scheduler service for automated data ingestion.

Uses APScheduler to run ingestion jobs on configurable schedules.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    AsyncIOScheduler = None
    SQLAlchemyJobStore = None
    CronTrigger = None
    IntervalTrigger = None
    APSCHEDULER_AVAILABLE = False

from app.core.models import IngestionSchedule, IngestionJob, JobStatus, ScheduleFrequency
from app.core.config import get_settings
from app.core.database import get_session_factory

logger = logging.getLogger(__name__)

# =============================================================================
# Incremental Loading — Source-specific date parameter mapping
# =============================================================================
# Maps source name -> (param_name, formatter_fn)
# When a schedule has "incremental": true and a non-null last_run_at,
# the formatter converts last_run_at into the source-specific start param.

INCREMENTAL_PARAM_MAP = {
    "fred":     ("observation_start", lambda dt: dt.strftime("%Y-%m-%d")),
    "bls":      ("start_year",        lambda dt: dt.year),
    "eia":      ("start",             lambda dt: dt.strftime("%Y-%m-%d")),
    "sec":      ("start_date",        lambda dt: dt.strftime("%Y-%m-%d")),
    "treasury": ("start_date",        lambda dt: dt.strftime("%Y-%m-%d")),
    "bts":      ("start_date",        lambda dt: dt.strftime("%Y-%m-%d")),
    "census":   ("year",              lambda dt: dt.year),
    "bea":      ("year",              lambda dt: str(dt.year)),
}


def _inject_incremental_params(
    config: Dict[str, Any],
    source: str,
    last_run_at: Optional[datetime],
) -> Dict[str, Any]:
    """
    If config has incremental=true and we have a previous run timestamp,
    inject the source-specific start-date parameter so the ingestor only
    fetches data newer than the last successful run.

    First run (last_run_at is None) or unknown source → full load unchanged.
    """
    if not config or not config.get("incremental"):
        return config or {}

    if last_run_at is None:
        logger.info(f"Incremental enabled for {source} but no last_run_at — full load")
        return config

    mapping = INCREMENTAL_PARAM_MAP.get(source)
    if mapping is None:
        logger.warning(f"Incremental enabled for {source} but no param mapping — full load")
        return config

    param_name, formatter = mapping
    effective = dict(config)  # shallow copy
    effective[param_name] = formatter(last_run_at)
    logger.info(
        f"Incremental injection for {source}: {param_name}={effective[param_name]}"
    )
    return effective

# Global scheduler instance
_scheduler: Optional["AsyncIOScheduler"] = None


def get_scheduler() -> "AsyncIOScheduler":
    """Get or create the global scheduler instance with persistent job store."""
    if not APSCHEDULER_AVAILABLE:
        raise ImportError(
            "APScheduler is not installed. Install it with: pip install apscheduler"
        )
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

        # Inject incremental start params if configured
        effective_config = _inject_incremental_params(
            config=schedule.config or {},
            source=schedule.source,
            last_run_at=schedule.last_run_at,
        )

        # Create a new ingestion job
        job = IngestionJob(
            source=schedule.source,
            status=JobStatus.PENDING,
            config=effective_config,
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

        # Audit trail
        try:
            from app.core import audit_service
            audit_service.log_collection(
                db,
                trigger_type="schedule",
                source=schedule.source,
                job_id=job.id,
                job_type="ingestion",
                trigger_source=f"schedule_{schedule_id}",
                config_snapshot=schedule.config,
            )
        except Exception:
            pass

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

    elif schedule.frequency == ScheduleFrequency.QUARTERLY:
        day = schedule.day_of_month or 2
        quarter_months = [1, 4, 7, 10]
        # Find the next quarter month
        for qm in quarter_months:
            next_run = now.replace(
                month=qm,
                day=min(day, 28),
                hour=schedule.hour or 6,
                minute=0,
                second=0,
                microsecond=0
            )
            if next_run > now:
                return next_run
        # Wrap to next year Q1
        return now.replace(
            year=now.year + 1,
            month=1,
            day=min(day, 28),
            hour=schedule.hour or 6,
            minute=0,
            second=0,
            microsecond=0
        )

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

    elif schedule.frequency == ScheduleFrequency.QUARTERLY:
        return CronTrigger(
            month='1,4,7,10',
            day=schedule.day_of_month or 2,
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
    # =========================================================================
    # TIER 1 — Daily (10:00-13:00 UTC / 5-8 AM ET)
    # =========================================================================
    {
        "name": "Treasury Daily Balance",
        "source": "treasury",
        "config": {"dataset": "daily_balance"},
        "frequency": ScheduleFrequency.DAILY,
        "hour": 10,
        "description": "Daily Treasury statement",
        "priority": 2,
    },
    {
        "name": "FRED Daily Series - Interest Rates",
        "source": "fred",
        "config": {"category": "interest_rates"},
        "frequency": ScheduleFrequency.DAILY,
        "hour": 10,
        "description": "Daily FRED interest rate series",
        "priority": 3,
    },
    {
        "name": "Prediction Markets Snapshot",
        "source": "prediction_markets",
        "config": {"sources": ["kalshi", "polymarket"]},
        "frequency": ScheduleFrequency.DAILY,
        "hour": 11,
        "description": "Daily prediction market odds snapshot",
        "priority": 4,
    },

    # =========================================================================
    # TIER 2 — Weekly (Mon-Wed early AM)
    # =========================================================================
    {
        "name": "FRED All Categories - Weekly",
        "source": "fred",
        "config": {"category": "all"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 10,
        "day_of_week": 0,  # Monday
        "description": "Weekly full refresh of FRED modified series",
        "priority": 3,
    },
    {
        "name": "EIA Petroleum Weekly",
        "source": "eia",
        "config": {"dataset": "petroleum_weekly", "subcategory": "consumption", "frequency": "weekly"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 11,
        "day_of_week": 0,  # Monday
        "description": "Weekly petroleum status report",
        "priority": 3,
    },
    {
        "name": "CFTC COT Weekly",
        "source": "cftc_cot",
        "config": {"report_type": "all"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 12,
        "day_of_week": 0,  # Monday
        "description": "Weekly Commitments of Traders positions",
        "priority": 4,
    },
    {
        "name": "Web Traffic Tranco - Weekly",
        "source": "web_traffic",
        "config": {"list": "tranco_top1m"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 12,
        "day_of_week": 1,  # Tuesday
        "description": "Weekly Tranco ranking changes",
        "priority": 6,
    },
    {
        "name": "GitHub Analytics - Weekly",
        "source": "github",
        "config": {},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 10,
        "day_of_week": 2,  # Wednesday
        "description": "Weekly GitHub repository metrics",
        "priority": 6,
    },
    {
        "name": "NOAA Weather - Weekly",
        "source": "noaa",
        "config": {"dataset": "daily_summaries"},
        "frequency": ScheduleFrequency.WEEKLY,
        "hour": 11,
        "day_of_week": 2,  # Wednesday
        "description": "Weekly weather observations refresh",
        "priority": 5,
    },

    # =========================================================================
    # TIER 3 — Monthly (spread across days 1-15)
    # =========================================================================
    {
        "name": "FEMA Disasters - Monthly",
        "source": "fema",
        "config": {"dataset": "disasters"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 1,
        "description": "Monthly FEMA disaster declarations update",
        "priority": 3,
    },
    {
        "name": "BEA GDP/Income - Monthly",
        "source": "bea",
        "config": {"dataset": "gdp", "table_name": "T10101"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 3,
        "description": "Monthly BEA national accounts data",
        "priority": 2,
    },
    {
        "name": "EIA Electricity - Monthly",
        "source": "eia",
        "config": {"dataset": "electricity", "subcategory": "retail_sales"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 4,
        "description": "Monthly EIA electricity utility rates",
        "priority": 4,
    },
    {
        "name": "EIA Natural Gas - Monthly",
        "source": "eia",
        "config": {"dataset": "natural_gas", "subcategory": "consumption"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 12,
        "day_of_month": 4,
        "description": "Monthly EIA natural gas prices",
        "priority": 4,
    },
    {
        "name": "BLS Employment (CES) - Monthly",
        "source": "bls",
        "config": {"dataset": "ces"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 13,
        "day_of_month": 5,
        "description": "Monthly employment data (BLS releases ~first Friday)",
        "priority": 2,
    },
    {
        "name": "Data Commons - Monthly",
        "source": "data_commons",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 12,
        "day_of_month": 5,
        "description": "Monthly unified public data refresh",
        "priority": 6,
    },
    {
        "name": "BLS CPI - Monthly",
        "source": "bls",
        "config": {"dataset": "cpi"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 13,
        "day_of_month": 6,
        "description": "Monthly consumer price index data",
        "priority": 2,
    },
    {
        "name": "BLS PPI - Monthly",
        "source": "bls",
        "config": {"dataset": "ppi"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 13,
        "day_of_month": 7,
        "description": "Monthly producer price index data",
        "priority": 3,
    },
    {
        "name": "FDIC Bank Financials - Monthly",
        "source": "fdic",
        "config": {"dataset": "financials"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 8,
        "description": "Monthly FDIC bank call reports",
        "priority": 4,
    },
    {
        "name": "SEC Form ADV - Monthly",
        "source": "form_adv",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 9,
        "description": "Monthly SEC investment adviser registrations",
        "priority": 5,
    },
    {
        "name": "SEC Form D - Monthly",
        "source": "form_d",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 12,
        "day_of_month": 9,
        "description": "Monthly SEC private placement filings",
        "priority": 5,
    },
    {
        "name": "CMS Medicare Utilization - Monthly",
        "source": "cms",
        "config": {"dataset": "utilization"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 10,
        "description": "Monthly Medicare healthcare utilization data",
        "priority": 5,
    },
    {
        "name": "App Store Rankings - Monthly",
        "source": "app_rankings",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 11,
        "description": "Monthly app metrics and rankings",
        "priority": 7,
    },
    {
        "name": "FBI Crime Statistics - Monthly",
        "source": "fbi_crime",
        "config": {"dataset": "ucr"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 12,
        "description": "Monthly FBI UCR crime statistics",
        "priority": 5,
    },
    {
        "name": "IRS SOI ZIP Income - Monthly",
        "source": "irs_soi",
        "config": {"dataset": "zip_income"},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 13,
        "description": "Monthly IRS income-by-ZIP update",
        "priority": 6,
    },
    {
        "name": "Glassdoor Reviews - Monthly",
        "source": "glassdoor",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 14,
        "description": "Monthly Glassdoor company reviews/ratings",
        "priority": 7,
    },
    {
        "name": "Yelp Business Listings - Monthly",
        "source": "yelp",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 11,
        "day_of_month": 15,
        "description": "Monthly Yelp business listing refresh",
        "priority": 7,
    },
    {
        "name": "FCC Broadband Coverage - Monthly",
        "source": "fcc_broadband",
        "config": {},
        "frequency": ScheduleFrequency.MONTHLY,
        "hour": 12,
        "day_of_month": 15,
        "description": "Monthly FCC broadband coverage data",
        "priority": 5,
    },

    # =========================================================================
    # TIER 4 — Quarterly (Jan/Apr/Jul/Oct, staggered across first 2 weeks)
    # =========================================================================
    {
        "name": "Census ACS 5-Year - Quarterly",
        "source": "census",
        "config": {"survey": "acs5", "geo_level": "county"},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 8,
        "day_of_month": 2,
        "description": "Quarterly Census ACS 5-year estimates refresh",
        "priority": 3,
    },
    {
        "name": "BEA Regional Data - Quarterly",
        "source": "bea",
        "config": {"dataset": "regional", "table_name": "SAGDP2N"},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 9,
        "day_of_month": 2,
        "description": "Quarterly BEA regional economic data",
        "priority": 4,
    },
    {
        "name": "SEC 10-K/10-Q Filings - Quarterly",
        "source": "sec",
        "config": {"filing_type": "10-K,10-Q"},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 10,
        "day_of_month": 3,
        "description": "Quarterly SEC 10-K/10-Q filing refresh",
        "priority": 3,
    },
    {
        "name": "SEC 13F Holdings - Quarterly",
        "source": "sec",
        "config": {"filing_type": "13F"},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 11,
        "day_of_month": 3,
        "description": "Quarterly SEC 13F institutional holdings",
        "priority": 3,
    },
    {
        "name": "USPTO Patents - Quarterly",
        "source": "uspto",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 10,
        "day_of_month": 4,
        "description": "Quarterly USPTO patent filings refresh",
        "priority": 5,
    },
    {
        "name": "US Trade Imports/Exports - Quarterly",
        "source": "us_trade",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 11,
        "day_of_month": 4,
        "description": "Quarterly US international trade data",
        "priority": 4,
    },
    {
        "name": "BTS Transportation - Quarterly",
        "source": "bts",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 10,
        "day_of_month": 5,
        "description": "Quarterly BTS transportation statistics",
        "priority": 5,
    },
    {
        "name": "International Econ (World Bank/IMF/OECD) - Quarterly",
        "source": "international_econ",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 11,
        "day_of_month": 5,
        "description": "Quarterly international economic indicators",
        "priority": 4,
    },
    {
        "name": "Real Estate FHFA HPI - Quarterly",
        "source": "realestate",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 10,
        "day_of_month": 6,
        "description": "Quarterly FHFA House Price Index",
        "priority": 5,
    },
    {
        "name": "OpenCorporates Registry - Quarterly",
        "source": "opencorporates",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 11,
        "day_of_month": 6,
        "description": "Quarterly global company registry refresh",
        "priority": 6,
    },
    {
        "name": "USDA Agriculture - Quarterly",
        "source": "usda",
        "config": {},
        "frequency": ScheduleFrequency.QUARTERLY,
        "hour": 10,
        "day_of_month": 7,
        "description": "Quarterly USDA crop and livestock data",
        "priority": 5,
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
    for longer than the timeout threshold. Uses per-source timeout
    from SourceConfig when available, falling back to the global default.

    Args:
        timeout_hours: Fallback hours (used only if no per-source config)

    Returns:
        Dictionary with cleanup results
    """
    from app.core import webhook_service
    from app.core import source_config_service

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Get ALL running jobs (we check per-source timeout individually)
        running_jobs = db.query(IngestionJob).filter(
            IngestionJob.status == JobStatus.RUNNING,
            IngestionJob.started_at.isnot(None),
        ).all()

        if not running_jobs:
            logger.info("No running jobs found during cleanup")
            return {
                "cleaned_up": 0,
                "jobs": [],
                "timeout_hours": timeout_hours
            }

        # Mark stuck jobs as failed (check per-source timeout)
        cleaned_jobs = []
        for job in running_jobs:
            # Get per-source timeout, fall back to global
            source_timeout_secs = source_config_service.get_timeout_seconds(db, job.source)
            running_seconds = (datetime.utcnow() - job.started_at).total_seconds()

            if running_seconds < source_timeout_secs:
                continue  # Not stuck yet for this source

            running_hours = running_seconds / 3600
            threshold_hours = source_timeout_secs / 3600

            job.status = JobStatus.FAILED
            job.error_message = f"Job timed out after {running_hours:.1f} hours (threshold: {threshold_hours:.1f}h) - automatically marked as failed"
            job.completed_at = datetime.utcnow()

            cleaned_jobs.append({
                "job_id": job.id,
                "source": job.source,
                "running_hours": round(running_hours, 2),
                "started_at": job.started_at.isoformat(),
                "timeout_hours": round(threshold_hours, 2),
            })

            logger.warning(
                f"Marked stuck job {job.id} ({job.source}) as failed - "
                f"was running for {running_hours:.1f} hours (threshold: {threshold_hours:.1f}h)"
            )

        if not cleaned_jobs:
            logger.info("No stuck jobs found during cleanup")
            return {
                "cleaned_up": 0,
                "jobs": [],
                "timeout_hours": timeout_hours
            }

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


def register_consecutive_failure_checker(interval_minutes: int = 30) -> bool:
    """
    Register periodic check for site intel sources with consecutive failures.

    Fires ALERT_CONSECUTIVE_FAILURES webhook when a source has 3+ consecutive failures.

    Args:
        interval_minutes: How often to run the check (default 30 min)

    Returns:
        True if registered successfully
    """
    from app.core.monitoring import check_and_notify_consecutive_failures

    scheduler = get_scheduler()
    job_id = "system_consecutive_failure_checker"

    try:
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)

        scheduler.add_job(
            check_and_notify_consecutive_failures,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            name="Consecutive Failure Checker",
            replace_existing=True,
        )

        logger.info(f"Registered consecutive failure checker to run every {interval_minutes} minutes")
        return True

    except Exception as e:
        logger.error(f"Failed to register consecutive failure checker: {e}")
        return False


# =============================================================================
# Auto-Refresh Stale Datasets
# =============================================================================

async def check_and_refresh_stale_datasets():
    """
    Background job: evaluate FRESHNESS quality rules and re-ingest
    any source whose data is stale — but only if an active schedule
    exists for it and no job is already PENDING/RUNNING.
    """
    from app.core.models import DataQualityRule, RuleType
    from app.core.data_quality_service import evaluate_freshness_rule

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # 1. Get all enabled FRESHNESS rules
        rules = (
            db.query(DataQualityRule)
            .filter(
                DataQualityRule.rule_type == RuleType.FRESHNESS,
                DataQualityRule.is_enabled == 1,
            )
            .all()
        )

        if not rules:
            logger.debug("Auto-refresh: no freshness rules configured")
            return

        refreshed = 0

        for rule in rules:
            # Evaluate the rule
            try:
                result = evaluate_freshness_rule(
                    db, rule, rule.dataset_pattern or "", rule.column_name or ""
                )
            except Exception as e:
                logger.debug(f"Auto-refresh: could not evaluate rule {rule.name}: {e}")
                continue

            if result.passed:
                continue  # data is fresh

            source = rule.source
            if not source:
                continue

            logger.info(f"Auto-refresh: {source} is stale ({result.message})")

            # Check for active schedule
            schedule = (
                db.query(IngestionSchedule)
                .filter(
                    IngestionSchedule.source == source,
                    IngestionSchedule.is_active == 1,
                )
                .first()
            )
            if not schedule:
                logger.debug(f"Auto-refresh: no active schedule for {source}, skipping")
                continue

            # Skip if a job is already pending or running
            active_job = (
                db.query(IngestionJob)
                .filter(
                    IngestionJob.source == source,
                    IngestionJob.status.in_([JobStatus.PENDING, JobStatus.RUNNING]),
                )
                .first()
            )
            if active_job:
                logger.debug(f"Auto-refresh: {source} already has active job {active_job.id}, skipping")
                continue

            # Create a new job with incremental params
            effective_config = _inject_incremental_params(
                config=schedule.config or {},
                source=source,
                last_run_at=schedule.last_run_at,
            )

            job = IngestionJob(
                source=source,
                status=JobStatus.PENDING,
                config=effective_config,
            )
            db.add(job)
            db.commit()
            db.refresh(job)

            logger.info(f"Auto-refresh: created job {job.id} for stale source {source}")

            # Execute
            await _execute_ingestion_job(db, job)
            refreshed += 1

        if refreshed:
            logger.info(f"Auto-refresh: triggered {refreshed} re-ingestion(s)")

    except Exception as e:
        logger.error(f"Auto-refresh error: {e}", exc_info=True)
    finally:
        db.close()


def register_freshness_checker(interval_minutes: int = 60) -> bool:
    """
    Register the auto-refresh checker as an APScheduler interval job.

    Args:
        interval_minutes: How often to check (default 60 min)

    Returns:
        True if registered successfully
    """
    scheduler = get_scheduler()
    job_id = "system_freshness_checker"

    try:
        existing_job = scheduler.get_job(job_id)
        if existing_job:
            scheduler.remove_job(job_id)

        scheduler.add_job(
            check_and_refresh_stale_datasets,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            name="Freshness Auto-Refresh Checker",
            replace_existing=True,
        )

        logger.info(f"Registered freshness checker to run every {interval_minutes} minutes")
        return True

    except Exception as e:
        logger.error(f"Failed to register freshness checker: {e}")
        return False
