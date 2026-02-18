"""
Site Intelligence Collection Scheduler.

Manages quarterly site intel collection across all domains.
Follows the same pattern as people_collection_scheduler.py.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session

from app.sources.site_intel.types import (
    SiteIntelDomain,
)

logger = logging.getLogger(__name__)

# Domain -> import path for triggering collector registration
DOMAIN_IMPORTS = {
    "power": "app.sources.site_intel.power",
    "telecom": "app.sources.site_intel.telecom",
    "transport": "app.sources.site_intel.transport",
    "risk": "app.sources.site_intel.risk",
    "water_utilities": "app.sources.site_intel.water_utilities",
    "incentives": "app.sources.site_intel.incentives",
    "logistics": "app.sources.site_intel.logistics",
    "labor": "app.sources.site_intel.labor",
}

# Domain string -> SiteIntelDomain enum
DOMAIN_ENUM_MAP = {
    "power": SiteIntelDomain.POWER,
    "telecom": SiteIntelDomain.TELECOM,
    "transport": SiteIntelDomain.TRANSPORT,
    "risk": SiteIntelDomain.RISK,
    "water_utilities": SiteIntelDomain.WATER_UTILITIES,
    "incentives": SiteIntelDomain.INCENTIVES,
    "logistics": SiteIntelDomain.LOGISTICS,
    "labor": SiteIntelDomain.LABOR,
}


class SiteIntelScheduler:
    """Manages quarterly site intel collection across all domains."""

    def __init__(self, db: Session):
        self.db = db

    async def run_domain_collection(
        self,
        domain: str,
        states: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run all collectors for a domain.

        Args:
            domain: Domain name (e.g., "power", "telecom")
            states: Optional list of state codes to collect

        Returns:
            Summary of collection results
        """
        import importlib
        from app.sources.site_intel.runner import SiteIntelOrchestrator

        # Import domain module to trigger collector registration
        import_path = DOMAIN_IMPORTS.get(domain)
        if import_path:
            try:
                importlib.import_module(import_path)
            except ImportError as e:
                logger.warning(f"Could not import {import_path}: {e}")

        domain_enum = DOMAIN_ENUM_MAP.get(domain)
        if not domain_enum:
            logger.error(f"Unknown site intel domain: {domain}")
            return {"domain": domain, "error": f"Unknown domain: {domain}"}

        orchestrator = SiteIntelOrchestrator(self.db)

        logger.info(f"Starting site intel collection for domain: {domain}")
        start_time = datetime.utcnow()

        try:
            kwargs = {}
            if states:
                kwargs["states"] = states

            results = await orchestrator.collect_domain(domain_enum, **kwargs)

            # Summarize results
            summary = {
                "domain": domain,
                "collectors_run": len(results),
                "successful": 0,
                "failed": 0,
                "total_inserted": 0,
                "total_updated": 0,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            }

            for source, result in results.items():
                if result.status.value == "completed":
                    summary["successful"] += 1
                    summary["total_inserted"] += result.inserted_items or 0
                    summary["total_updated"] += result.updated_items or 0
                else:
                    summary["failed"] += 1

            logger.info(
                f"Domain {domain} collection complete: "
                f"{summary['successful']}/{summary['collectors_run']} succeeded, "
                f"{summary['total_inserted']} inserted in {summary['duration_seconds']:.0f}s"
            )

            return summary

        except Exception as e:
            logger.error(f"Error collecting domain {domain}: {e}", exc_info=True)
            return {
                "domain": domain,
                "error": str(e),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            }

    async def run_quarterly_refresh(self) -> Dict[str, Any]:
        """
        Run all domains in sequence with staggered gaps.

        Returns:
            Summary across all domains
        """
        domains = [
            "power",
            "telecom",
            "transport",
            "risk",
            "water_utilities",
            "incentives",
            "logistics",
            "labor",
        ]

        results = {
            "started_at": datetime.utcnow().isoformat(),
            "domains": {},
            "total_successful": 0,
            "total_failed": 0,
        }

        for domain in domains:
            domain_result = await self.run_domain_collection(domain)
            results["domains"][domain] = domain_result
            results["total_successful"] += domain_result.get("successful", 0)
            results["total_failed"] += domain_result.get("failed", 0)

            # 5-minute gap between domains to avoid overwhelming APIs
            if domain != domains[-1]:
                logger.info("Waiting 5 minutes before next domain...")
                await asyncio.sleep(300)

        results["completed_at"] = datetime.utcnow().isoformat()
        logger.info(
            f"Quarterly site intel refresh complete: "
            f"{results['total_successful']} succeeded, {results['total_failed']} failed"
        )

        return results


# =============================================================================
# APScheduler Wrappers (called by scheduler)
# =============================================================================


async def _run_quarterly_site_intel_refresh():
    """APScheduler wrapper for quarterly full refresh."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        scheduler = SiteIntelScheduler(db)
        result = await scheduler.run_quarterly_refresh()
        logger.info(
            f"Quarterly site intel refresh completed: {result.get('total_successful', 0)} domains succeeded"
        )
        return result
    except Exception as e:
        logger.error(f"Quarterly site intel refresh failed: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        db.close()


async def _run_monthly_logistics_update():
    """APScheduler wrapper for monthly logistics-only update."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        scheduler = SiteIntelScheduler(db)
        result = await scheduler.run_domain_collection("logistics")
        logger.info(f"Monthly logistics update completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Monthly logistics update failed: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        db.close()


# =============================================================================
# APScheduler Registration
# =============================================================================


def register_site_intel_schedules() -> Dict[str, bool]:
    """
    Register all site intel scheduled jobs with APScheduler.

    Returns:
        Dictionary of job_id -> registration success
    """
    from apscheduler.triggers.cron import CronTrigger
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    results = {}

    # 1. Quarterly full refresh — 2nd of Jan/Apr/Jul/Oct at 1 AM UTC
    try:
        scheduler.add_job(
            _run_quarterly_site_intel_refresh,
            trigger=CronTrigger(month="1,4,7,10", day=2, hour=1, minute=0),
            id="site_intel_quarterly_refresh",
            name="Quarterly Site Intel Full Refresh",
            replace_existing=True,
        )
        results["site_intel_quarterly_refresh"] = True
        logger.info(
            "Registered quarterly site intel refresh (2nd of Jan/Apr/Jul/Oct, 1 AM UTC)"
        )
    except Exception as e:
        logger.error(f"Failed to register quarterly site intel refresh: {e}")
        results["site_intel_quarterly_refresh"] = False

    # 2. Monthly logistics update — 1st of each month at 2 AM UTC
    #    (freight rates change faster than other site intel data)
    try:
        scheduler.add_job(
            _run_monthly_logistics_update,
            trigger=CronTrigger(day=1, hour=2, minute=0),
            id="site_intel_monthly_logistics",
            name="Monthly Logistics Domain Update",
            replace_existing=True,
        )
        results["site_intel_monthly_logistics"] = True
        logger.info("Registered monthly logistics update (1st of month, 2 AM UTC)")
    except Exception as e:
        logger.error(f"Failed to register monthly logistics update: {e}")
        results["site_intel_monthly_logistics"] = False

    return results


def unregister_site_intel_schedules() -> Dict[str, bool]:
    """Remove all site intel scheduled jobs."""
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    job_ids = [
        "site_intel_quarterly_refresh",
        "site_intel_monthly_logistics",
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


def get_site_intel_schedule_status() -> Dict[str, Any]:
    """Get status of site intel scheduled jobs."""
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    job_ids = [
        "site_intel_quarterly_refresh",
        "site_intel_monthly_logistics",
    ]

    jobs = []
    for job_id in job_ids:
        job = scheduler.get_job(job_id)
        if job:
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat()
                    if job.next_run_time
                    else None,
                    "trigger": str(job.trigger),
                    "active": True,
                }
            )
        else:
            jobs.append(
                {
                    "id": job_id,
                    "name": job_id.replace("site_intel_", "").replace("_", " ").title(),
                    "next_run": None,
                    "trigger": None,
                    "active": False,
                }
            )

    return {
        "scheduler_running": scheduler.running,
        "scheduled_jobs": jobs,
        "checked_at": datetime.utcnow().isoformat(),
    }
