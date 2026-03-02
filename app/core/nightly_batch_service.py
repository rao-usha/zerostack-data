"""
Batch collection orchestrator.

Enqueues all data sources with tier-based priority ordering.
Tier 1 (daily sources) runs first at highest priority, Tier 4
(quarterly sources) runs last after base data is collected.

Batch runs are tracked via IngestionJob.batch_run_id — no separate
NightlyBatch table. Status is always computed live from job statuses,
so nothing can get "stuck."

Usage:
    POST /api/v1/jobs/batch/launch
    GET  /api/v1/jobs/batch/runs
    GET  /api/v1/jobs/batch/runs/{batch_run_id}

    # Or via scheduler (registered in main.py lifespan):
    Runs automatically at 2 AM UTC
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import func, case, text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.core.job_queue_service import submit_job, WORKER_MODE

logger = logging.getLogger(__name__)


# =============================================================================
# Source tier definitions
# =============================================================================


@dataclass
class SourceDef:
    """A data source to include in a batch."""

    key: str  # matches SOURCE_DISPATCH key in jobs.py
    default_config: Dict = field(default_factory=dict)


@dataclass
class Tier:
    """A priority tier of sources."""

    level: int
    priority: int  # higher = picked first by workers
    name: str
    sources: List[SourceDef] = field(default_factory=list)


# Tier 1 — Daily sources: fast government APIs + market data
TIER_1 = Tier(
    level=1,
    priority=10,
    name="Daily — Rates, Treasury & Markets",
    sources=[
        SourceDef("treasury"),
        SourceDef("fred", {"category": "interest_rates"}),
        SourceDef("prediction_markets", {"sources": ["kalshi", "polymarket"]}),
    ],
)

# Tier 2 — Weekly sources: moderate-frequency data
TIER_2 = Tier(
    level=2,
    priority=7,
    name="Weekly — Energy, Trade & Weather",
    sources=[
        SourceDef("eia", {"dataset": "petroleum_weekly", "subcategory": "consumption", "frequency": "weekly"}),
        SourceDef("cftc_cot", {"report_type": "all"}),
        SourceDef("web_traffic", {"list": "tranco_top1m"}),
        SourceDef("github"),
        SourceDef("noaa", {"dataset": "daily_summaries"}),
    ],
)

# Tier 3 — Monthly sources: government releases, regulatory filings
TIER_3 = Tier(
    level=3,
    priority=5,
    name="Monthly — Employment, Healthcare & Regulatory",
    sources=[
        SourceDef("bea", {"dataset": "gdp", "table_name": "T10101"}),
        SourceDef("bls", {"dataset": "ces"}),
        SourceDef("eia", {"dataset": "electricity", "subcategory": "retail_sales"}),
        SourceDef("eia", {"dataset": "natural_gas", "subcategory": "consumption"}),
        SourceDef("fema", {"dataset": "disasters"}),
        SourceDef("fdic", {"dataset": "financials"}),
        SourceDef("form_adv"),
        SourceDef("form_d"),
        SourceDef("cms", {"dataset": "utilization"}),
        SourceDef("fbi_crime", {"dataset": "ucr"}),
        SourceDef("irs_soi", {"dataset": "zip_income"}),
        SourceDef("data_commons"),
        SourceDef("fcc_broadband"),
        SourceDef("app_rankings"),
        SourceDef("job_postings:all", {"skip_recent_hours": 600}),
    ],
)

# Tier 4 — Quarterly sources: SEC, Census, trade, and slow-moving data
TIER_4 = Tier(
    level=4,
    priority=3,
    name="Quarterly — Census, SEC & Trade",
    sources=[
        SourceDef("census", {"survey": "acs5", "geo_level": "county"}),
        SourceDef("bea", {"dataset": "regional", "table_name": "SAGDP2N"}),
        SourceDef("sec", {"filing_type": "10-K,10-Q"}),
        SourceDef("sec", {"filing_type": "13F"}),
        SourceDef("uspto"),
        SourceDef("us_trade"),
        SourceDef("bts"),
        SourceDef("international_econ"),
        SourceDef("realestate"),
        SourceDef("opencorporates"),
        SourceDef("usda"),
    ],
)

TIERS = [TIER_1, TIER_2, TIER_3, TIER_4]

# Lookup: tier level → Tier object
TIER_BY_LEVEL = {t.level: t for t in TIERS}

# Agentic sources use separate executor types (not in nightly tiers;
# triggered on-demand via their own endpoints)
AGENTIC_SOURCE_MAP = {
    "site_intel_full_sync": "site_intel",
    "people_batch": "people",
    "pe_batch": "pe",
}


def _get_source_display_name(source_key: str) -> str:
    """Get human-readable display name for a source from the registry."""
    try:
        from app.core.source_registry import SOURCE_REGISTRY
        base_key = source_key.split(":")[0]  # handle "job_postings:all" → "job_postings"
        ctx = SOURCE_REGISTRY.get(base_key)
        if ctx:
            return ctx.display_name
    except Exception:
        pass
    return source_key.replace("_", " ").title()


def _get_source_description(source_key: str) -> str:
    """Get brief description for a source from the registry."""
    try:
        from app.core.source_registry import SOURCE_REGISTRY
        base_key = source_key.split(":")[0]
        ctx = SOURCE_REGISTRY.get(base_key)
        if ctx:
            return ctx.description
    except Exception:
        pass
    return ""


def _generate_batch_run_id() -> str:
    """Generate a unique batch_run_id string."""
    return f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


# =============================================================================
# Batch orchestrator
# =============================================================================


async def launch_batch_collection(
    db: Session,
    config: Optional[Dict] = None,
    tiers: Optional[List[int]] = None,
    sources: Optional[List[str]] = None,
) -> Dict:
    """
    Launch a batch collection.

    Creates an IngestionJob + JobQueue entry for each source in each tier.
    Each job is tagged with a shared batch_run_id, trigger="batch", and tier.
    No separate NightlyBatch record — status is computed live from jobs.

    Args:
        db: Database session
        config: Optional overrides (e.g. {"skip_sources": ["kaggle"]})
        tiers: Optional list of tier levels to run (default: all)
        sources: Optional list of specific source keys to run

    Returns:
        Dict with batch_run_id, total_jobs, and started_at
    """
    if not WORKER_MODE:
        raise RuntimeError(
            "Batch collection requires WORKER_MODE=1. "
            "Set WORKER_MODE=1 in docker-compose.yml and start at least one worker."
        )

    config = config or {}
    skip_sources = set(config.get("skip_sources", []))
    batch_run_id = _generate_batch_run_id()

    total = 0
    job_ids = []
    target_tiers = [t for t in TIERS if tiers is None or t.level in tiers]

    for tier in target_tiers:
        for source_def in tier.sources:
            # Filter by skip list or explicit source list
            if source_def.key in skip_sources:
                continue
            if sources and source_def.key not in sources:
                continue

            # Determine job type — agentic sources use their own executor
            job_type = AGENTIC_SOURCE_MAP.get(source_def.key, "ingestion")

            # Create the ingestion_jobs record with batch metadata
            ing_job = IngestionJob(
                source=source_def.key,
                status=JobStatus.PENDING,
                config=source_def.default_config,
                batch_run_id=batch_run_id,
                trigger="batch",
                tier=tier.level,
            )
            db.add(ing_job)
            db.flush()  # get the ID

            # Build queue payload
            payload = {
                "source": source_def.key,
                "config": source_def.default_config,
                "ingestion_job_id": ing_job.id,
                "batch_id": batch_run_id,
                "tier": tier.level,
            }

            # Agentic executors read config from top-level payload keys
            if source_def.key in AGENTIC_SOURCE_MAP:
                payload.update(source_def.default_config)

            # Tier 4 jobs should wait for lower tiers to complete
            if tier.level == 4:
                payload["wait_for_tiers"] = [1, 2, 3]

            submit_job(
                db=db,
                job_type=job_type,
                payload=payload,
                priority=tier.priority,
                job_table_id=ing_job.id,
            )

            job_ids.append(ing_job.id)
            total += 1

    db.commit()

    logger.info(
        f"Batch {batch_run_id} launched: {total} jobs across "
        f"{len(target_tiers)} tiers"
    )

    return {
        "batch_run_id": batch_run_id,
        "total_jobs": total,
        "job_ids": job_ids,
        "started_at": datetime.utcnow().isoformat(),
    }


def get_batch_run_status(db: Session, batch_run_id: str) -> Optional[Dict]:
    """
    Get detailed status of a batch run.

    Queries IngestionJob.batch_run_id directly — status is always live.
    No NightlyBatch table needed. Nothing can get "stuck."
    """
    jobs = (
        db.query(IngestionJob)
        .filter(IngestionJob.batch_run_id == batch_run_id)
        .all()
    )
    if not jobs:
        return None

    # Count by status
    successful = sum(1 for j in jobs if j.status == JobStatus.SUCCESS)
    failed = sum(1 for j in jobs if j.status == JobStatus.FAILED)
    running = sum(1 for j in jobs if j.status == JobStatus.RUNNING)
    pending = sum(1 for j in jobs if j.status == JobStatus.PENDING)
    completed = successful + failed
    total = len(jobs)

    # Derive overall status live from job statuses
    if pending + running == 0 and total > 0:
        if failed == 0:
            status = "completed"
        elif successful == 0:
            status = "failed"
        else:
            status = "partial_success"
    elif running > 0 or pending > 0:
        status = "running"
    else:
        status = "completed"

    # Per-tier breakdown
    tier_status = {}
    for j in jobs:
        tier_level = j.tier or 0
        tier_key = f"tier_{tier_level}"
        if tier_key not in tier_status:
            tier_obj = TIER_BY_LEVEL.get(tier_level)
            tier_status[tier_key] = {
                "name": tier_obj.name if tier_obj else f"Tier {tier_level}",
                "sources": [s.key for s in tier_obj.sources] if tier_obj else [],
                "total": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 0,
            }
        tier_status[tier_key]["total"] += 1
        if j.status == JobStatus.SUCCESS:
            tier_status[tier_key]["completed"] += 1
        elif j.status == JobStatus.FAILED:
            tier_status[tier_key]["failed"] += 1
        elif j.status == JobStatus.RUNNING:
            tier_status[tier_key]["running"] += 1
        elif j.status == JobStatus.PENDING:
            tier_status[tier_key]["pending"] += 1

    # Timing
    started_at = min((j.created_at for j in jobs), default=None)
    completed_at = None
    if status in ("completed", "failed", "partial_success"):
        completed_at = max(
            (j.completed_at for j in jobs if j.completed_at),
            default=None,
        )
    elapsed = None
    if started_at:
        end = completed_at or datetime.utcnow()
        elapsed = (end - started_at).total_seconds()

    # Job details
    job_details = []
    for j in jobs:
        duration = None
        if j.started_at and j.completed_at:
            duration = round((j.completed_at - j.started_at).total_seconds(), 1)
        job_details.append({
            "job_id": j.id,
            "source": j.source,
            "display_name": _get_source_display_name(j.source),
            "tier": j.tier or 0,
            "description": _get_source_description(j.source),
            "status": j.status.value if hasattr(j.status, "value") else j.status,
            "rows_inserted": j.rows_inserted,
            "error_message": j.error_message[:200] if j.error_message else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "duration_seconds": duration,
        })

    return {
        "batch_run_id": batch_run_id,
        "status": status,
        "total_jobs": total,
        "completed_jobs": completed,
        "successful_jobs": successful,
        "failed_jobs": failed,
        "running_jobs": running,
        "pending_jobs": pending,
        "elapsed_seconds": round(elapsed, 1) if elapsed else None,
        "started_at": started_at.isoformat() if started_at else None,
        "completed_at": completed_at.isoformat() if completed_at else None,
        "tier_status": tier_status,
        "jobs": job_details,
    }


def list_batch_runs(
    db: Session, limit: int = 20, status: Optional[str] = None
) -> List[Dict]:
    """
    List recent batch runs by querying IngestionJob.batch_run_id.

    Groups by batch_run_id with aggregate counts. Status is always
    computed live — nothing can get "stuck."
    """
    # Aggregate query: GROUP BY batch_run_id
    query = (
        db.query(
            IngestionJob.batch_run_id,
            func.count().label("total_jobs"),
            func.sum(case((IngestionJob.status == JobStatus.SUCCESS, 1), else_=0)).label("successful"),
            func.sum(case((IngestionJob.status == JobStatus.FAILED, 1), else_=0)).label("failed"),
            func.sum(case((IngestionJob.status == JobStatus.RUNNING, 1), else_=0)).label("running"),
            func.sum(case((IngestionJob.status == JobStatus.PENDING, 1), else_=0)).label("pending"),
            func.min(IngestionJob.created_at).label("started_at"),
            func.max(IngestionJob.completed_at).label("completed_at"),
        )
        .filter(IngestionJob.batch_run_id.isnot(None))
        .group_by(IngestionJob.batch_run_id)
        .order_by(func.min(IngestionJob.created_at).desc())
        .limit(limit)
    )

    results = []
    for row in query.all():
        total = row.total_jobs
        successful = row.successful or 0
        failed = row.failed or 0
        running_count = row.running or 0
        pending_count = row.pending or 0
        completed = successful + failed

        # Derive status live
        if pending_count + running_count == 0 and total > 0:
            if failed == 0:
                batch_status = "completed"
            elif successful == 0:
                batch_status = "failed"
            else:
                batch_status = "partial_success"
        elif running_count > 0 or pending_count > 0:
            batch_status = "running"
        else:
            batch_status = "completed"

        # Filter by status if requested
        if status and batch_status != status:
            continue

        results.append({
            "batch_run_id": row.batch_run_id,
            "status": batch_status,
            "total_jobs": total,
            "completed_jobs": completed,
            "successful_jobs": successful,
            "failed_jobs": failed,
            "running_jobs": running_count,
            "pending_jobs": pending_count,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        })

    return results


# =============================================================================
# Legacy wrappers (deprecated — use launch_batch_collection / get_batch_run_status)
# =============================================================================


async def launch_nightly_batch(
    db: Session,
    config: Optional[Dict] = None,
    tiers: Optional[List[int]] = None,
    sources: Optional[List[str]] = None,
) -> Dict:
    """Deprecated: wraps launch_batch_collection for backwards compatibility."""
    return await launch_batch_collection(db, config=config, tiers=tiers, sources=sources)


def get_batch_status(db: Session, batch_id) -> Optional[Dict]:
    """Deprecated: wraps get_batch_run_status for backwards compatibility."""
    # Handle both legacy int IDs and new string batch_run_ids
    batch_run_id = str(batch_id)
    if batch_run_id.isdigit():
        batch_run_id = f"legacy_batch_{batch_run_id}"
    return get_batch_run_status(db, batch_run_id)


def list_batches(
    db: Session, limit: int = 20, status: Optional[str] = None
) -> List[Dict]:
    """Deprecated: wraps list_batch_runs for backwards compatibility."""
    return list_batch_runs(db, limit=limit, status=status)


async def scheduled_nightly_batch():
    """
    Entry point for APScheduler cron trigger.

    Creates its own DB session since it runs outside a request context.
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        logger.info("Scheduled batch collection starting...")
        result = await launch_batch_collection(db)
        logger.info(
            f"Scheduled batch {result['batch_run_id']} launched: "
            f"{result['total_jobs']} jobs"
        )
    except Exception as e:
        logger.error(f"Scheduled batch collection failed: {e}", exc_info=True)
    finally:
        db.close()
