"""
Scheduled portfolio updates for agentic collection.

Provides:
- Quarterly refresh for all investors
- Priority queue for stale data
- Incremental updates (only new data)
- Integration with existing APScheduler

Usage:
    from app.agentic.scheduler import (
        register_portfolio_schedules,
        queue_stale_investors,
        get_portfolio_schedule_status
    )

    # Register quarterly updates
    register_portfolio_schedules()

    # Queue investors with stale data for immediate refresh
    await queue_stale_investors(max_age_days=90)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from heapq import heappush, heappop

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.scheduler_service import get_scheduler, start_scheduler
from app.core.database import get_session_factory
from app.agentic.metrics import get_metrics_collector

logger = logging.getLogger(__name__)


class RefreshPriority(Enum):
    """Priority levels for portfolio refresh."""

    CRITICAL = 1  # Never collected or very stale (>180 days)
    HIGH = 2  # Stale data (>90 days)
    NORMAL = 3  # Regular quarterly refresh
    LOW = 4  # Recent data, just maintenance


@dataclass(order=True)
class RefreshTask:
    """A task in the priority queue for portfolio refresh."""

    priority: int
    investor_id: int = field(compare=False)
    investor_type: str = field(compare=False)
    investor_name: str = field(compare=False)
    last_collection: Optional[datetime] = field(compare=False, default=None)
    reason: str = field(compare=False, default="scheduled")


class PortfolioRefreshQueue:
    """
    Priority queue for portfolio refresh tasks.

    Investors are prioritized by:
    1. Never collected (highest priority)
    2. Time since last collection
    3. Investor type (LPs higher than FOs)
    """

    def __init__(self):
        self._queue: List[RefreshTask] = []
        self._queued_investors: set = set()  # Track to avoid duplicates
        self._lock = asyncio.Lock()

    async def add(
        self,
        investor_id: int,
        investor_type: str,
        investor_name: str,
        last_collection: Optional[datetime] = None,
        priority: RefreshPriority = RefreshPriority.NORMAL,
        reason: str = "scheduled",
    ) -> bool:
        """Add an investor to the refresh queue."""
        async with self._lock:
            key = f"{investor_type}:{investor_id}"
            if key in self._queued_investors:
                return False  # Already queued

            task = RefreshTask(
                priority=priority.value,
                investor_id=investor_id,
                investor_type=investor_type,
                investor_name=investor_name,
                last_collection=last_collection,
                reason=reason,
            )

            heappush(self._queue, task)
            self._queued_investors.add(key)

            logger.debug(
                f"Queued {investor_name} for refresh (priority={priority.name})"
            )
            return True

    async def pop(self) -> Optional[RefreshTask]:
        """Get the next highest priority investor to refresh."""
        async with self._lock:
            if not self._queue:
                return None

            task = heappop(self._queue)
            key = f"{task.investor_type}:{task.investor_id}"
            self._queued_investors.discard(key)

            return task

    async def peek(self) -> Optional[RefreshTask]:
        """Peek at the next task without removing it."""
        async with self._lock:
            if not self._queue:
                return None
            return self._queue[0]

    @property
    def size(self) -> int:
        """Get current queue size."""
        return len(self._queue)

    async def clear(self) -> int:
        """Clear the queue and return count of cleared items."""
        async with self._lock:
            count = len(self._queue)
            self._queue.clear()
            self._queued_investors.clear()
            return count

    def get_status(self) -> Dict[str, Any]:
        """Get queue status."""
        priority_counts = {p.name: 0 for p in RefreshPriority}
        for task in self._queue:
            for p in RefreshPriority:
                if task.priority == p.value:
                    priority_counts[p.name] += 1
                    break

        return {
            "size": len(self._queue),
            "by_priority": priority_counts,
        }


# Global refresh queue
_refresh_queue: Optional[PortfolioRefreshQueue] = None


def get_refresh_queue() -> PortfolioRefreshQueue:
    """Get or create the global refresh queue."""
    global _refresh_queue
    if _refresh_queue is None:
        _refresh_queue = PortfolioRefreshQueue()
    return _refresh_queue


# =============================================================================
# Scheduled Job Functions
# =============================================================================


async def run_quarterly_refresh():
    """
    Run quarterly portfolio refresh for all investors.

    This job:
    1. Identifies all investors
    2. Checks last collection date
    3. Queues stale investors for refresh
    4. Processes queue with rate limiting
    """
    logger.info("Starting quarterly portfolio refresh")
    get_metrics_collector()

    try:
        # Queue stale investors
        queued = await queue_stale_investors(max_age_days=90)
        logger.info(f"Queued {queued} investors for quarterly refresh")

        # Process queue
        processed = await process_refresh_queue(max_concurrent=3, delay_seconds=5)
        logger.info(f"Processed {processed} investors in quarterly refresh")

        return {"queued": queued, "processed": processed, "status": "completed"}

    except Exception as e:
        logger.error(f"Error in quarterly refresh: {e}", exc_info=True)
        return {"queued": 0, "processed": 0, "status": "failed", "error": str(e)}


async def run_weekly_stale_check():
    """
    Weekly check for critically stale data.

    Queues investors that haven't been updated in 180+ days.
    """
    logger.info("Running weekly stale data check")

    try:
        queued = await queue_stale_investors(
            max_age_days=180, priority=RefreshPriority.HIGH
        )
        logger.info(f"Queued {queued} critically stale investors")

        return {"queued": queued, "status": "completed"}

    except Exception as e:
        logger.error(f"Error in weekly stale check: {e}", exc_info=True)
        return {"queued": 0, "status": "failed", "error": str(e)}


async def run_queue_processor():
    """
    Background processor for the refresh queue.

    Runs periodically to process queued refresh tasks.
    """
    queue = get_refresh_queue()

    if queue.size == 0:
        return {"processed": 0, "status": "queue_empty"}

    processed = await process_refresh_queue(max_concurrent=2, delay_seconds=10)

    return {"processed": processed, "status": "completed"}


# =============================================================================
# Core Functions
# =============================================================================


async def queue_stale_investors(
    max_age_days: int = 90,
    investor_type: Optional[str] = None,
    priority: RefreshPriority = RefreshPriority.NORMAL,
    limit: int = 100,
) -> int:
    """
    Queue investors with stale portfolio data for refresh.

    Args:
        max_age_days: Consider data stale if older than this many days
        investor_type: Filter by type ('lp' or 'family_office'), None for both
        priority: Priority level for queued tasks
        limit: Maximum investors to queue

    Returns:
        Number of investors queued
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()
    queue = get_refresh_queue()

    try:
        cutoff_date = datetime.utcnow() - timedelta(days=max_age_days)
        queued_count = 0

        # Query LPs with stale or no portfolio data
        if investor_type is None or investor_type == "lp":
            lp_query = text("""
                SELECT
                    lf.id,
                    lf.name,
                    MAX(pc.collected_date) as last_collection
                FROM lp_fund lf
                LEFT JOIN portfolio_companies pc
                    ON pc.investor_id = lf.id AND pc.investor_type = 'lp'
                GROUP BY lf.id, lf.name
                HAVING MAX(pc.collected_date) IS NULL
                    OR MAX(pc.collected_date) < :cutoff
                ORDER BY MAX(pc.collected_date) ASC NULLS FIRST
                LIMIT :limit
            """)

            lp_results = db.execute(
                lp_query, {"cutoff": cutoff_date, "limit": limit}
            ).fetchall()

            for row in lp_results:
                investor_id, name, last_collection = row

                # Determine priority based on staleness
                actual_priority = priority
                if last_collection is None:
                    actual_priority = RefreshPriority.CRITICAL
                elif last_collection < datetime.utcnow() - timedelta(days=180):
                    actual_priority = RefreshPriority.HIGH

                added = await queue.add(
                    investor_id=investor_id,
                    investor_type="lp",
                    investor_name=name,
                    last_collection=last_collection,
                    priority=actual_priority,
                    reason=f"stale_{max_age_days}d",
                )
                if added:
                    queued_count += 1

        # Query Family Offices with stale or no portfolio data
        if investor_type is None or investor_type == "family_office":
            fo_query = text("""
                SELECT
                    fo.id,
                    fo.name,
                    MAX(pc.collected_date) as last_collection
                FROM family_offices fo
                LEFT JOIN portfolio_companies pc
                    ON pc.investor_id = fo.id AND pc.investor_type = 'family_office'
                GROUP BY fo.id, fo.name
                HAVING MAX(pc.collected_date) IS NULL
                    OR MAX(pc.collected_date) < :cutoff
                ORDER BY MAX(pc.collected_date) ASC NULLS FIRST
                LIMIT :limit
            """)

            fo_results = db.execute(
                fo_query, {"cutoff": cutoff_date, "limit": limit}
            ).fetchall()

            for row in fo_results:
                investor_id, name, last_collection = row

                actual_priority = priority
                if last_collection is None:
                    actual_priority = RefreshPriority.CRITICAL
                elif last_collection < datetime.utcnow() - timedelta(days=180):
                    actual_priority = RefreshPriority.HIGH

                added = await queue.add(
                    investor_id=investor_id,
                    investor_type="family_office",
                    investor_name=name,
                    last_collection=last_collection,
                    priority=actual_priority,
                    reason=f"stale_{max_age_days}d",
                )
                if added:
                    queued_count += 1

        logger.info(f"Queued {queued_count} stale investors (max_age={max_age_days}d)")
        return queued_count

    except Exception as e:
        logger.error(f"Error queuing stale investors: {e}", exc_info=True)
        return 0

    finally:
        db.close()


async def process_refresh_queue(
    max_concurrent: int = 2, delay_seconds: float = 5.0, max_tasks: Optional[int] = None
) -> int:
    """
    Process pending tasks in the refresh queue.

    Args:
        max_concurrent: Maximum concurrent collection jobs
        delay_seconds: Delay between starting jobs
        max_tasks: Maximum tasks to process (None for all)

    Returns:
        Number of tasks processed
    """
    from app.agentic.portfolio_agent import PortfolioResearchAgent, InvestorContext

    queue = get_refresh_queue()
    metrics = get_metrics_collector()
    processed = 0

    SessionLocal = get_session_factory()

    while True:
        if max_tasks and processed >= max_tasks:
            break

        task = await queue.pop()
        if not task:
            break

        logger.info(f"Processing refresh for {task.investor_name} ({task.reason})")

        db = SessionLocal()
        try:
            # Get investor details
            if task.investor_type == "lp":
                investor_query = text("""
                    SELECT id, name, formal_name, lp_type, jurisdiction, website_url
                    FROM lp_fund WHERE id = :investor_id
                """)
            else:
                investor_query = text("""
                    SELECT id, name, legal_name, NULL, state_province, website
                    FROM family_offices WHERE id = :investor_id
                """)

            row = db.execute(
                investor_query, {"investor_id": task.investor_id}
            ).fetchone()

            if not row:
                logger.warning(
                    f"Investor not found: {task.investor_type} {task.investor_id}"
                )
                continue

            # Create context
            context = InvestorContext(
                investor_id=row[0],
                investor_type=task.investor_type,
                investor_name=row[1],
                formal_name=row[2],
                lp_type=row[3],
                jurisdiction=row[4],
                website_url=row[5],
            )

            # Record job start
            metrics.record_job_start(
                job_id=processed + 1,
                investor_id=task.investor_id,
                investor_type=task.investor_type,
                investor_name=task.investor_name,
            )
            metrics.record_job_running(processed + 1)

            # Run collection
            agent = PortfolioResearchAgent(db)
            result = await agent.collect_portfolio(
                context=context,
                incremental=True,  # Only fetch new data
            )

            # Record completion
            metrics.record_job_complete(
                job_id=processed + 1,
                success=result.get("status") == "success",
                companies_found=result.get("companies_found", 0),
                tokens_used=result.get("tokens_used", 0),
                strategies_used=result.get("strategies_used", []),
            )

            processed += 1
            logger.info(
                f"Refreshed {task.investor_name}: "
                f"{result.get('companies_found', 0)} companies found"
            )

        except Exception as e:
            logger.error(f"Error processing refresh for {task.investor_name}: {e}")
            metrics.record_job_complete(job_id=processed + 1, success=False)

        finally:
            db.close()

        # Rate limiting delay
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    return processed


async def refresh_investor(
    investor_id: int, investor_type: str, incremental: bool = True
) -> Dict[str, Any]:
    """
    Immediately refresh portfolio data for a single investor.

    Args:
        investor_id: Investor ID
        investor_type: 'lp' or 'family_office'
        incremental: If True, only fetch data newer than last collection

    Returns:
        Collection result dictionary
    """
    from app.agentic.portfolio_agent import PortfolioResearchAgent, InvestorContext

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Get investor details
        if investor_type == "lp":
            query = text("""
                SELECT id, name, formal_name, lp_type, jurisdiction, website_url
                FROM lp_fund WHERE id = :investor_id
            """)
        else:
            query = text("""
                SELECT id, name, legal_name, NULL, state_province, website
                FROM family_offices WHERE id = :investor_id
            """)

        row = db.execute(query, {"investor_id": investor_id}).fetchone()

        if not row:
            return {
                "status": "error",
                "error": f"Investor not found: {investor_type} {investor_id}",
            }

        context = InvestorContext(
            investor_id=row[0],
            investor_type=investor_type,
            investor_name=row[1],
            formal_name=row[2],
            lp_type=row[3],
            jurisdiction=row[4],
            website_url=row[5],
        )

        agent = PortfolioResearchAgent(db)
        result = await agent.collect_portfolio(context=context, incremental=incremental)

        return result

    except Exception as e:
        logger.error(f"Error refreshing investor {investor_type} {investor_id}: {e}")
        return {"status": "error", "error": str(e)}

    finally:
        db.close()


# =============================================================================
# Schedule Registration
# =============================================================================


def register_portfolio_schedules() -> Dict[str, bool]:
    """
    Register all portfolio-related scheduled jobs with APScheduler.

    Returns:
        Dictionary of job_id -> registration success
    """
    scheduler = get_scheduler()
    results = {}

    # 1. Quarterly full refresh (1st of Jan, Apr, Jul, Oct at 2 AM)
    try:
        scheduler.add_job(
            run_quarterly_refresh,
            trigger=CronTrigger(month="1,4,7,10", day=1, hour=2, minute=0),
            id="agentic_quarterly_refresh",
            name="Portfolio Quarterly Refresh",
            replace_existing=True,
        )
        results["agentic_quarterly_refresh"] = True
        logger.info("Registered quarterly portfolio refresh")
    except Exception as e:
        logger.error(f"Failed to register quarterly refresh: {e}")
        results["agentic_quarterly_refresh"] = False

    # 2. Weekly stale data check (Sundays at 3 AM)
    try:
        scheduler.add_job(
            run_weekly_stale_check,
            trigger=CronTrigger(day_of_week="sun", hour=3, minute=0),
            id="agentic_weekly_stale_check",
            name="Portfolio Weekly Stale Check",
            replace_existing=True,
        )
        results["agentic_weekly_stale_check"] = True
        logger.info("Registered weekly stale data check")
    except Exception as e:
        logger.error(f"Failed to register weekly stale check: {e}")
        results["agentic_weekly_stale_check"] = False

    # 3. Queue processor (every 30 minutes)
    try:
        scheduler.add_job(
            run_queue_processor,
            trigger=IntervalTrigger(minutes=30),
            id="agentic_queue_processor",
            name="Portfolio Queue Processor",
            replace_existing=True,
        )
        results["agentic_queue_processor"] = True
        logger.info("Registered queue processor (every 30 min)")
    except Exception as e:
        logger.error(f"Failed to register queue processor: {e}")
        results["agentic_queue_processor"] = False

    return results


def unregister_portfolio_schedules() -> Dict[str, bool]:
    """
    Remove all portfolio-related scheduled jobs.

    Returns:
        Dictionary of job_id -> removal success
    """
    scheduler = get_scheduler()
    job_ids = [
        "agentic_quarterly_refresh",
        "agentic_weekly_stale_check",
        "agentic_queue_processor",
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


def get_portfolio_schedule_status() -> Dict[str, Any]:
    """
    Get status of portfolio-related scheduled jobs.

    Returns:
        Dictionary with schedule status and queue info
    """
    scheduler = get_scheduler()
    queue = get_refresh_queue()

    job_ids = [
        "agentic_quarterly_refresh",
        "agentic_weekly_stale_check",
        "agentic_queue_processor",
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
                    "name": job_id.replace("agentic_", "").replace("_", " ").title(),
                    "next_run": None,
                    "trigger": None,
                    "active": False,
                }
            )

    return {
        "scheduler_running": scheduler.running,
        "scheduled_jobs": jobs,
        "refresh_queue": queue.get_status(),
        "checked_at": datetime.utcnow().isoformat(),
    }
