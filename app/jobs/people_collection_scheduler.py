"""
People Collection Scheduler.

Manages scheduled jobs for automated leadership data collection:
- Weekly website refresh
- Daily SEC 8-K checks
- Daily newsroom scanning
- Weekly benchmark recalculation
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session

from app.core.people_models import (
    IndustrialCompany,
    PeopleCollectionJob,
    PeoplePortfolio,
    PeoplePortfolioCompany,
)

logger = logging.getLogger(__name__)


class PeopleCollectionScheduler:
    """
    Manages scheduled collection jobs for people intelligence.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_companies_for_refresh(
        self,
        job_type: str,
        limit: int = 50,
        priority: str = "all",
    ) -> List[IndustrialCompany]:
        """
        Get companies that need data refresh.

        Args:
            job_type: website_crawl, sec_parse, news_scan
            limit: Max companies to return
            priority: all, portfolio, public

        Returns list of companies ordered by staleness.
        """
        query = self.db.query(IndustrialCompany)

        # Filter by priority
        if priority == "portfolio":
            # Only companies in active portfolios
            portfolio_company_ids = [
                pc.company_id for pc in self.db.query(PeoplePortfolioCompany).filter(
                    PeoplePortfolioCompany.is_active == True
                ).all()
            ]
            if portfolio_company_ids:
                query = query.filter(IndustrialCompany.id.in_(portfolio_company_ids))
        elif priority == "public":
            # Only public companies (have CIK)
            query = query.filter(IndustrialCompany.cik.isnot(None))

        # Order by staleness (oldest first)
        query = query.order_by(
            IndustrialCompany.leadership_last_updated.asc().nullsfirst()
        )

        return query.limit(limit).all()

    def create_batch_job(
        self,
        job_type: str,
        company_ids: List[int],
        config: Optional[Dict] = None,
    ) -> PeopleCollectionJob:
        """
        Create a batch collection job for multiple companies.
        """
        job = PeopleCollectionJob(
            job_type=job_type,
            company_ids=company_ids,
            config=config or {},
            status="pending",
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        logger.info(f"Created batch job {job.id} for {len(company_ids)} companies")
        return job

    def get_pending_jobs(self, job_type: Optional[str] = None) -> List[PeopleCollectionJob]:
        """Get all pending collection jobs."""
        query = self.db.query(PeopleCollectionJob).filter(
            PeopleCollectionJob.status == "pending"
        )
        if job_type:
            query = query.filter(PeopleCollectionJob.job_type == job_type)
        return query.order_by(PeopleCollectionJob.created_at).all()

    def get_running_jobs(self) -> List[PeopleCollectionJob]:
        """Get all currently running jobs."""
        return self.db.query(PeopleCollectionJob).filter(
            PeopleCollectionJob.status == "running"
        ).all()

    def mark_job_running(self, job_id: int) -> bool:
        """Mark a job as running."""
        job = self.db.get(PeopleCollectionJob, job_id)
        if not job:
            return False

        job.status = "running"
        job.started_at = datetime.utcnow()
        self.db.commit()
        return True

    def mark_job_complete(
        self,
        job_id: int,
        people_found: int = 0,
        people_created: int = 0,
        people_updated: int = 0,
        changes_detected: int = 0,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
    ) -> bool:
        """Mark a job as complete with results."""
        job = self.db.get(PeopleCollectionJob, job_id)
        if not job:
            return False

        job.status = "success"
        job.completed_at = datetime.utcnow()
        job.people_found = people_found
        job.people_created = people_created
        job.people_updated = people_updated
        job.changes_detected = changes_detected
        job.errors = errors or []
        job.warnings = warnings or []

        self.db.commit()
        logger.info(f"Job {job_id} completed: found={people_found}, created={people_created}")
        return True

    def mark_job_failed(
        self,
        job_id: int,
        errors: List[str],
    ) -> bool:
        """Mark a job as failed."""
        job = self.db.get(PeopleCollectionJob, job_id)
        if not job:
            return False

        job.status = "failed"
        job.completed_at = datetime.utcnow()
        job.errors = errors

        self.db.commit()
        logger.error(f"Job {job_id} failed: {errors}")
        return True

    def cleanup_stuck_jobs(self, max_age_hours: int = 4) -> int:
        """
        Mark stuck jobs as failed.

        Jobs running longer than max_age_hours are considered stuck.
        """
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

        stuck_jobs = self.db.query(PeopleCollectionJob).filter(
            PeopleCollectionJob.status == "running",
            PeopleCollectionJob.started_at < cutoff,
        ).all()

        for job in stuck_jobs:
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.errors = ["Job timed out - marked as stuck"]
            logger.warning(f"Marked stuck job {job.id} as failed")

        self.db.commit()
        return len(stuck_jobs)

    def get_job_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get collection job statistics."""
        cutoff = datetime.utcnow() - timedelta(days=days)

        jobs = self.db.query(PeopleCollectionJob).filter(
            PeopleCollectionJob.created_at >= cutoff
        ).all()

        stats = {
            "period_days": days,
            "total_jobs": len(jobs),
            "by_status": {},
            "by_type": {},
            "total_people_found": 0,
            "total_people_created": 0,
            "total_changes_detected": 0,
            "success_rate": 0,
        }

        success_count = 0
        completed_count = 0

        for job in jobs:
            # By status
            stats["by_status"][job.status] = stats["by_status"].get(job.status, 0) + 1

            # By type
            stats["by_type"][job.job_type] = stats["by_type"].get(job.job_type, 0) + 1

            # Totals
            stats["total_people_found"] += job.people_found or 0
            stats["total_people_created"] += job.people_created or 0
            stats["total_changes_detected"] += job.changes_detected or 0

            if job.status in ["success", "failed"]:
                completed_count += 1
                if job.status == "success":
                    success_count += 1

        if completed_count > 0:
            stats["success_rate"] = round(success_count / completed_count * 100, 1)

        return stats


def schedule_website_refresh(db: Session, limit: int = 50) -> Optional[int]:
    """
    Schedule a batch website refresh job.

    Called by APScheduler weekly.
    """
    scheduler = PeopleCollectionScheduler(db)

    # Get companies needing refresh
    companies = scheduler.get_companies_for_refresh(
        job_type="website_crawl",
        limit=limit,
        priority="portfolio",  # Prioritize portfolio companies
    )

    if not companies:
        logger.info("No companies need website refresh")
        return None

    # Create batch job
    job = scheduler.create_batch_job(
        job_type="website_crawl",
        company_ids=[c.id for c in companies],
        config={"source": "scheduled", "refresh_type": "weekly"},
    )

    return job.id


def schedule_sec_check(db: Session, limit: int = 30) -> Optional[int]:
    """
    Schedule SEC 8-K filing check.

    Called by APScheduler daily.
    """
    scheduler = PeopleCollectionScheduler(db)

    # Get public companies (have CIK)
    companies = scheduler.get_companies_for_refresh(
        job_type="sec_parse",
        limit=limit,
        priority="public",
    )

    if not companies:
        logger.info("No public companies to check SEC filings")
        return None

    job = scheduler.create_batch_job(
        job_type="sec_8k_check",
        company_ids=[c.id for c in companies],
        config={"source": "scheduled", "filing_type": "8-K"},
    )

    return job.id


def schedule_news_scan(db: Session, limit: int = 50) -> Optional[int]:
    """
    Schedule newsroom scanning.

    Called by APScheduler daily.
    """
    scheduler = PeopleCollectionScheduler(db)

    companies = scheduler.get_companies_for_refresh(
        job_type="news_scan",
        limit=limit,
        priority="portfolio",
    )

    if not companies:
        logger.info("No companies need news scanning")
        return None

    job = scheduler.create_batch_job(
        job_type="news_scan",
        company_ids=[c.id for c in companies],
        config={"source": "scheduled", "days_back": 7},
    )

    return job.id


# =============================================================================
# Job Processor - Executes Pending Jobs
# =============================================================================

async def process_pending_jobs(max_jobs: int = 5) -> Dict[str, Any]:
    """
    Process pending people collection jobs.

    This is the main job processor that picks up pending jobs and executes them
    using the PeopleCollectionOrchestrator.

    Args:
        max_jobs: Maximum number of jobs to process in one run

    Returns:
        Summary of processed jobs
    """
    from app.core.database import get_session_factory
    from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator

    SessionLocal = get_session_factory()
    db = SessionLocal()

    results = {
        "processed": 0,
        "successful": 0,
        "failed": 0,
        "jobs": [],
    }

    try:
        scheduler = PeopleCollectionScheduler(db)

        # Get pending jobs
        pending_jobs = scheduler.get_pending_jobs()[:max_jobs]

        if not pending_jobs:
            logger.debug("No pending people collection jobs to process")
            return results

        logger.info(f"Processing {len(pending_jobs)} pending people collection jobs")

        orchestrator = PeopleCollectionOrchestrator(db)

        for job in pending_jobs:
            job_result = {
                "job_id": job.id,
                "job_type": job.job_type,
                "status": "unknown",
            }

            try:
                # Mark job as running
                scheduler.mark_job_running(job.id)

                # Determine sources based on job type
                sources = _get_sources_for_job_type(job.job_type)

                # Get company IDs
                company_ids = job.company_ids or ([job.company_id] if job.company_id else [])

                if not company_ids:
                    scheduler.mark_job_failed(job.id, ["No companies specified for job"])
                    job_result["status"] = "failed"
                    job_result["error"] = "No companies specified"
                    results["failed"] += 1
                    results["jobs"].append(job_result)
                    continue

                # Execute collection
                batch_result = await orchestrator.collect_batch(
                    company_ids=company_ids,
                    sources=sources,
                )

                # Update job with results
                scheduler.mark_job_complete(
                    job_id=job.id,
                    people_found=batch_result.total_people_found,
                    people_created=batch_result.total_people_created,
                    people_updated=0,  # TODO: track this in batch result
                    changes_detected=batch_result.total_changes_detected,
                    errors=[r.errors[0] if r.errors else None for r in batch_result.results if not r.success],
                )

                job_result["status"] = "success"
                job_result["people_found"] = batch_result.total_people_found
                job_result["people_created"] = batch_result.total_people_created
                results["successful"] += 1

                logger.info(
                    f"Job {job.id} completed: {batch_result.successful}/{batch_result.total_companies} companies, "
                    f"{batch_result.total_people_found} people found"
                )

            except Exception as e:
                logger.exception(f"Error processing job {job.id}: {e}")
                scheduler.mark_job_failed(job.id, [str(e)])
                job_result["status"] = "failed"
                job_result["error"] = str(e)
                results["failed"] += 1

            results["processed"] += 1
            results["jobs"].append(job_result)

        return results

    except Exception as e:
        logger.error(f"Error in job processor: {e}", exc_info=True)
        return {**results, "error": str(e)}

    finally:
        db.close()


def _get_sources_for_job_type(job_type: str) -> List[str]:
    """Map job type to collection sources."""
    mapping = {
        "website_crawl": ["website"],
        "sec_parse": ["sec"],
        "sec_8k_check": ["sec"],
        "news_scan": ["news"],
        "full_refresh": ["website", "sec", "news"],
        "single_company": ["website"],
    }
    return mapping.get(job_type, ["website"])


# =============================================================================
# APScheduler Registration
# =============================================================================

def register_people_collection_schedules() -> Dict[str, bool]:
    """
    Register all people collection scheduled jobs with APScheduler.

    Returns:
        Dictionary of job_id -> registration success
    """
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    results = {}

    # 1. Job processor - runs every 10 minutes to process pending jobs
    try:
        scheduler.add_job(
            process_pending_jobs,
            trigger=IntervalTrigger(minutes=10),
            id="people_job_processor",
            name="People Collection Job Processor",
            replace_existing=True,
        )
        results["people_job_processor"] = True
        logger.info("Registered people job processor (every 10 min)")
    except Exception as e:
        logger.error(f"Failed to register job processor: {e}")
        results["people_job_processor"] = False

    # 2. Weekly website refresh - Sundays at 2 AM
    try:
        scheduler.add_job(
            _scheduled_website_refresh,
            trigger=CronTrigger(day_of_week="sun", hour=2, minute=0),
            id="people_weekly_website_refresh",
            name="Weekly Website Leadership Refresh",
            replace_existing=True,
        )
        results["people_weekly_website_refresh"] = True
        logger.info("Registered weekly website refresh (Sundays 2 AM)")
    except Exception as e:
        logger.error(f"Failed to register website refresh: {e}")
        results["people_weekly_website_refresh"] = False

    # 3. Daily SEC check - weekdays at 6 PM (after market close)
    try:
        scheduler.add_job(
            _scheduled_sec_check,
            trigger=CronTrigger(day_of_week="mon-fri", hour=18, minute=0),
            id="people_daily_sec_check",
            name="Daily SEC 8-K Leadership Check",
            replace_existing=True,
        )
        results["people_daily_sec_check"] = True
        logger.info("Registered daily SEC check (weekdays 6 PM)")
    except Exception as e:
        logger.error(f"Failed to register SEC check: {e}")
        results["people_daily_sec_check"] = False

    # 4. Daily news scan - daily at 8 AM
    try:
        scheduler.add_job(
            _scheduled_news_scan,
            trigger=CronTrigger(hour=8, minute=0),
            id="people_daily_news_scan",
            name="Daily News Leadership Scan",
            replace_existing=True,
        )
        results["people_daily_news_scan"] = True
        logger.info("Registered daily news scan (8 AM)")
    except Exception as e:
        logger.error(f"Failed to register news scan: {e}")
        results["people_daily_news_scan"] = False

    # 5. Stuck job cleanup - every 2 hours
    try:
        scheduler.add_job(
            _cleanup_stuck_people_jobs,
            trigger=IntervalTrigger(hours=2),
            id="people_stuck_job_cleanup",
            name="People Collection Stuck Job Cleanup",
            replace_existing=True,
        )
        results["people_stuck_job_cleanup"] = True
        logger.info("Registered stuck job cleanup (every 2 hours)")
    except Exception as e:
        logger.error(f"Failed to register cleanup: {e}")
        results["people_stuck_job_cleanup"] = False

    return results


def unregister_people_collection_schedules() -> Dict[str, bool]:
    """Remove all people collection scheduled jobs."""
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    job_ids = [
        "people_job_processor",
        "people_weekly_website_refresh",
        "people_daily_sec_check",
        "people_daily_news_scan",
        "people_stuck_job_cleanup",
    ]

    results = {}
    for job_id in job_ids:
        try:
            job = scheduler.get_job(job_id)
            if job:
                scheduler.remove_job(job_id)
            results[job_id] = True
        except Exception as e:
            logger.error(f"Failed to unregister {job_id}: {e}")
            results[job_id] = False

    return results


def get_people_schedule_status() -> Dict[str, Any]:
    """Get status of people collection scheduled jobs."""
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    job_ids = [
        "people_job_processor",
        "people_weekly_website_refresh",
        "people_daily_sec_check",
        "people_daily_news_scan",
        "people_stuck_job_cleanup",
    ]

    jobs = []
    for job_id in job_ids:
        job = scheduler.get_job(job_id)
        if job:
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
                "active": True,
            })
        else:
            jobs.append({
                "id": job_id,
                "name": job_id.replace("people_", "").replace("_", " ").title(),
                "next_run": None,
                "trigger": None,
                "active": False,
            })

    return {
        "scheduler_running": scheduler.running,
        "scheduled_jobs": jobs,
        "checked_at": datetime.utcnow().isoformat(),
    }


# =============================================================================
# Scheduled Job Wrappers (called by APScheduler)
# =============================================================================

async def _scheduled_website_refresh():
    """APScheduler wrapper for website refresh."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        job_id = schedule_website_refresh(db, limit=50)
        if job_id:
            logger.info(f"Scheduled website refresh job {job_id}")
            # Immediately process the job
            await process_pending_jobs(max_jobs=1)
        return {"job_id": job_id}
    finally:
        db.close()


async def _scheduled_sec_check():
    """APScheduler wrapper for SEC check."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        job_id = schedule_sec_check(db, limit=30)
        if job_id:
            logger.info(f"Scheduled SEC check job {job_id}")
            await process_pending_jobs(max_jobs=1)
        return {"job_id": job_id}
    finally:
        db.close()


async def _scheduled_news_scan():
    """APScheduler wrapper for news scan."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        job_id = schedule_news_scan(db, limit=50)
        if job_id:
            logger.info(f"Scheduled news scan job {job_id}")
            await process_pending_jobs(max_jobs=1)
        return {"job_id": job_id}
    finally:
        db.close()


async def _cleanup_stuck_people_jobs():
    """APScheduler wrapper for stuck job cleanup."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        scheduler = PeopleCollectionScheduler(db)
        count = scheduler.cleanup_stuck_jobs(max_age_hours=4)
        if count > 0:
            logger.info(f"Cleaned up {count} stuck people collection jobs")
        return {"cleaned_up": count}
    finally:
        db.close()
