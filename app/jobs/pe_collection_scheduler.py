"""
PE Collection Scheduler.

Manages scheduled jobs for automated PE intelligence collection:
- Weekly firm refresh (firm_website + bio_extractor)
- Monthly SEC sweep (sec_adv + sec_form_d)
"""

import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def schedule_pe_refresh():
    """
    Refresh PE firm data for all active firms.

    Runs firm_website + bio_extractor for firms ordered by staleness.
    Called by APScheduler weekly.
    """
    from app.core.database import get_session_factory
    from sqlalchemy import text

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Get active firm IDs ordered by staleness (oldest updated_at first)
        result = db.execute(
            text(
                "SELECT id FROM pe_firms "
                "WHERE status = 'Active' OR status IS NULL "
                "ORDER BY updated_at ASC NULLS FIRST "
                "LIMIT 100"
            )
        )
        firm_ids = [row[0] for row in result.fetchall()]

        if not firm_ids:
            logger.info("PE refresh: no active firms to refresh")
            return {"firms": 0}

        logger.info(f"PE refresh: scheduling {len(firm_ids)} firms for website + bio collection")

        # Use the PE collection orchestrator
        import app.sources.pe_collection  # noqa: F401
        from app.sources.pe_collection.orchestrator import PECollectionOrchestrator
        from app.sources.pe_collection.persister import PEPersister
        from app.sources.pe_collection.types import PECollectionConfig

        config = PECollectionConfig.from_dict({
            "entity_type": "firm",
            "sources": ["firm_website", "bio_extractor"],
            "firm_ids": firm_ids,
            "max_concurrent": 3,
            "rate_limit_delay": 2.0,
        })

        orchestrator = PECollectionOrchestrator(db_session=db)
        results = await orchestrator.run_collection(config)

        persister = PEPersister(db)
        persist_stats = persister.persist_results(results)

        total_items = sum(r.items_found for r in results)
        logger.info(
            f"PE weekly refresh complete: {len(results)} firms, "
            f"{total_items} items, persisted={persist_stats['persisted']}"
        )

        return {
            "firms": len(firm_ids),
            "results": len(results),
            "items": total_items,
            "persisted": persist_stats["persisted"],
        }

    except Exception as e:
        logger.error(f"PE refresh failed: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        db.close()


async def schedule_pe_sec_sweep():
    """
    Monthly SEC data sweep for PE firms.

    Runs sec_adv + sec_form_d for all active firms.
    Called by APScheduler on the 1st of each month.
    """
    from app.core.database import get_session_factory
    from sqlalchemy import text

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        result = db.execute(
            text(
                "SELECT id FROM pe_firms "
                "WHERE status = 'Active' OR status IS NULL "
                "ORDER BY updated_at ASC NULLS FIRST "
                "LIMIT 100"
            )
        )
        firm_ids = [row[0] for row in result.fetchall()]

        if not firm_ids:
            logger.info("PE SEC sweep: no active firms")
            return {"firms": 0}

        logger.info(f"PE SEC sweep: {len(firm_ids)} firms for sec_adv + sec_form_d")

        import app.sources.pe_collection  # noqa: F401
        from app.sources.pe_collection.orchestrator import PECollectionOrchestrator
        from app.sources.pe_collection.persister import PEPersister
        from app.sources.pe_collection.types import PECollectionConfig

        config = PECollectionConfig.from_dict({
            "entity_type": "firm",
            "sources": ["sec_adv", "sec_form_d"],
            "firm_ids": firm_ids,
            "max_concurrent": 3,
            "rate_limit_delay": 2.0,
        })

        orchestrator = PECollectionOrchestrator(db_session=db)
        results = await orchestrator.run_collection(config)

        persister = PEPersister(db)
        persist_stats = persister.persist_results(results)

        total_items = sum(r.items_found for r in results)
        logger.info(
            f"PE SEC sweep complete: {len(results)} firms, "
            f"{total_items} items, persisted={persist_stats['persisted']}"
        )

        return {
            "firms": len(firm_ids),
            "results": len(results),
            "items": total_items,
            "persisted": persist_stats["persisted"],
        }

    except Exception as e:
        logger.error(f"PE SEC sweep failed: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        db.close()


def register_pe_collection_schedules() -> Dict[str, bool]:
    """
    Register PE collection scheduled jobs with APScheduler.

    Returns:
        Dictionary of job_id -> registration success
    """
    from apscheduler.triggers.cron import CronTrigger
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    results = {}

    # 1. Weekly firm refresh — Saturdays at 3 AM
    try:
        scheduler.add_job(
            schedule_pe_refresh,
            trigger=CronTrigger(day_of_week="sat", hour=3, minute=0),
            id="pe_weekly_firm_refresh",
            name="Weekly PE Firm Website + Bio Refresh",
            replace_existing=True,
        )
        results["pe_weekly_firm_refresh"] = True
        logger.info("Registered PE weekly firm refresh (Saturdays 3 AM)")
    except Exception as e:
        logger.error(f"Failed to register PE firm refresh: {e}")
        results["pe_weekly_firm_refresh"] = False

    # 2. Monthly SEC sweep — 1st of month at 4 AM
    try:
        scheduler.add_job(
            schedule_pe_sec_sweep,
            trigger=CronTrigger(day=1, hour=4, minute=0),
            id="pe_monthly_sec_sweep",
            name="Monthly PE SEC ADV + Form D Sweep",
            replace_existing=True,
        )
        results["pe_monthly_sec_sweep"] = True
        logger.info("Registered PE monthly SEC sweep (1st of month, 4 AM)")
    except Exception as e:
        logger.error(f"Failed to register PE SEC sweep: {e}")
        results["pe_monthly_sec_sweep"] = False

    return results


def get_pe_schedule_status() -> Dict[str, Any]:
    """Get status of PE collection scheduled jobs."""
    from app.core.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    job_ids = [
        "pe_weekly_firm_refresh",
        "pe_monthly_sec_sweep",
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
                "name": job_id.replace("pe_", "").replace("_", " ").title(),
                "next_run": None,
                "trigger": None,
                "active": False,
            })

    return {
        "scheduler_running": scheduler.running,
        "scheduled_jobs": jobs,
        "checked_at": datetime.utcnow().isoformat(),
    }
