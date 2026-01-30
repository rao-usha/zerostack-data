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
        job = self.db.query(PeopleCollectionJob).get(job_id)
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
        job = self.db.query(PeopleCollectionJob).get(job_id)
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
        job = self.db.query(PeopleCollectionJob).get(job_id)
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
