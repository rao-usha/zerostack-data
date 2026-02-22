"""
Nightly batch collection orchestrator.

Enqueues all data sources with tier-based priority ordering.
Tier 1 (fast gov APIs) runs first at highest priority, Tier 4
(agentic/LLM pipelines) runs last after base data is collected.

Usage:
    POST /api/v1/jobs/nightly/launch
    GET  /api/v1/jobs/nightly/{batch_id}

    # Or via scheduler (registered in main.py lifespan):
    Runs automatically at 2 AM UTC
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.core.models_queue import NightlyBatch, QueueJobStatus
from app.core.job_queue_service import submit_job, WORKER_MODE

logger = logging.getLogger(__name__)


# =============================================================================
# Source tier definitions
# =============================================================================


@dataclass
class SourceDef:
    """A data source to include in the nightly batch."""

    key: str  # matches SOURCE_DISPATCH key in jobs.py
    default_config: Dict = field(default_factory=dict)


@dataclass
class Tier:
    """A priority tier of sources."""

    level: int
    priority: int  # higher = picked first by workers
    name: str
    sources: List[SourceDef] = field(default_factory=list)


# Tier 1 — Fast government APIs, no dependencies, run first
TIER_1 = Tier(
    level=1,
    priority=10,
    name="Fast Government APIs",
    sources=[
        SourceDef("treasury"),
        SourceDef("fred", {"category": "interest_rates"}),
        SourceDef("bea"),
        SourceDef("fdic"),
        SourceDef("fema"),
        SourceDef("bts"),
        SourceDef("cftc_cot"),
        SourceDef("data_commons"),
    ],
)

# Tier 2 — Medium APIs, rate-limited
TIER_2 = Tier(
    level=2,
    priority=7,
    name="Medium Rate-Limited APIs",
    sources=[
        SourceDef("eia"),
        SourceDef("bls"),
        SourceDef("noaa"),
        SourceDef("cms"),
        SourceDef("fbi_crime"),
        SourceDef("irs_soi"),
        SourceDef("usda"),
        SourceDef("us_trade"),
        SourceDef("fcc_broadband"),
        SourceDef("prediction_markets"),
        SourceDef("realestate"),
        SourceDef("uspto"),
    ],
)

# Tier 3 — SEC/complex sources
TIER_3 = Tier(
    level=3,
    priority=5,
    name="Complex / SEC Sources",
    sources=[
        SourceDef("sec"),
        SourceDef("kaggle"),
        SourceDef("international_econ"),
        SourceDef("census", {"survey": "acs5", "year": 2023, "table_id": "B01001", "geo_level": "state"}),
        SourceDef("foot_traffic"),
        SourceDef("yelp"),
    ],
)

# Tier 4 — Agentic/LLM pipelines (depend on base data)
TIER_4 = Tier(
    level=4,
    priority=3,
    name="Agentic / LLM Pipelines",
    sources=[
        SourceDef("site_intel_full_sync", {"mode": "full_sync"}),
        SourceDef("people_batch", {"mode": "batch", "max_jobs": 50}),
        SourceDef("pe_batch", {"mode": "full"}),
    ],
)

TIERS = [TIER_1, TIER_2, TIER_3, TIER_4]

# Tier 4 sources are agentic and use separate executor types
AGENTIC_SOURCE_MAP = {
    "site_intel_full_sync": "site_intel",
    "people_batch": "people",
    "pe_batch": "pe",
}


# =============================================================================
# Batch orchestrator
# =============================================================================


async def launch_nightly_batch(
    db: Session,
    config: Optional[Dict] = None,
    tiers: Optional[List[int]] = None,
    sources: Optional[List[str]] = None,
) -> NightlyBatch:
    """
    Launch a nightly batch collection.

    Creates an IngestionJob + JobQueue entry for each source in each tier.
    All Tier 1-3 jobs run in parallel (bounded by worker slots and rate limits).
    Tier 4 jobs get wait_for_tiers in payload so executors can defer.

    Args:
        db: Database session
        config: Optional overrides (e.g. {"skip_sources": ["kaggle"]})
        tiers: Optional list of tier levels to run (default: all)
        sources: Optional list of specific source keys to run

    Returns:
        The NightlyBatch record
    """
    if not WORKER_MODE:
        raise RuntimeError(
            "Nightly batch requires WORKER_MODE=1. "
            "Set WORKER_MODE=1 in docker-compose.yml and start at least one worker."
        )

    config = config or {}
    skip_sources = set(config.get("skip_sources", []))

    batch = NightlyBatch(config=config, status="running")
    db.add(batch)
    db.commit()
    db.refresh(batch)

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
            source_key = (
                source_def.key
                if source_def.key not in AGENTIC_SOURCE_MAP
                else source_def.key
            )

            # Create the ingestion_jobs record
            ing_job = IngestionJob(
                source=source_key,
                status=JobStatus.PENDING,
                config=source_def.default_config,
            )
            db.add(ing_job)
            db.flush()  # get the ID

            # Build queue payload
            payload = {
                "source": source_key,
                "config": source_def.default_config,
                "ingestion_job_id": ing_job.id,
                "batch_id": batch.id,
                "tier": tier.level,
            }

            # Agentic executors read config from top-level payload keys
            # (e.g., PE reads payload["mode"], people reads payload["max_jobs"])
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

    batch.total_jobs = total
    batch.job_ids = job_ids
    db.commit()

    logger.info(
        f"Nightly batch {batch.id} launched: {total} jobs across "
        f"{len(target_tiers)} tiers"
    )

    return batch


def get_batch_status(db: Session, batch_id: int) -> Optional[Dict]:
    """
    Get detailed status of a nightly batch.

    Returns per-tier progress, overall counts, and timing info.
    """
    batch = db.query(NightlyBatch).filter(NightlyBatch.id == batch_id).first()
    if not batch:
        return None

    # Get all ingestion jobs for this batch
    jobs = []
    if batch.job_ids:
        jobs = (
            db.query(IngestionJob)
            .filter(IngestionJob.id.in_(batch.job_ids))
            .all()
        )

    # Count by status
    completed = sum(1 for j in jobs if j.status in (JobStatus.SUCCESS, JobStatus.FAILED))
    successful = sum(1 for j in jobs if j.status == JobStatus.SUCCESS)
    failed = sum(1 for j in jobs if j.status == JobStatus.FAILED)
    running = sum(1 for j in jobs if j.status == JobStatus.RUNNING)
    pending = sum(1 for j in jobs if j.status == JobStatus.PENDING)

    # Per-tier breakdown
    from app.core.models_queue import JobQueue

    tier_status = {}
    queue_jobs = (
        db.query(JobQueue)
        .filter(JobQueue.job_table_id.in_(batch.job_ids))
        .all()
    )
    for qj in queue_jobs:
        tier = (qj.payload or {}).get("tier", 0)
        tier_key = f"tier_{tier}"
        if tier_key not in tier_status:
            tier_status[tier_key] = {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 0,
            }
        tier_status[tier_key]["total"] += 1
        status_val = qj.status.value if hasattr(qj.status, "value") else qj.status
        if status_val == "success":
            tier_status[tier_key]["completed"] += 1
        elif status_val == "failed":
            tier_status[tier_key]["failed"] += 1
        elif status_val == "running":
            tier_status[tier_key]["running"] += 1
        elif status_val in ("pending", "claimed"):
            tier_status[tier_key]["pending"] += 1

    # Update batch record
    batch.completed_jobs = completed
    batch.failed_jobs = failed

    if completed == batch.total_jobs and batch.total_jobs > 0:
        if failed == 0:
            batch.status = "completed"
        elif successful == 0:
            batch.status = "failed"
        else:
            batch.status = "partial_success"
        batch.completed_at = datetime.utcnow()
        db.commit()

    elapsed = None
    if batch.started_at:
        elapsed = (datetime.utcnow() - batch.started_at).total_seconds()

    # Job details
    job_details = []
    for j in jobs:
        job_details.append({
            "job_id": j.id,
            "source": j.source,
            "status": j.status.value if hasattr(j.status, "value") else j.status,
            "rows_inserted": j.rows_inserted,
            "error_message": j.error_message[:200] if j.error_message else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
        })

    return {
        "batch_id": batch.id,
        "status": batch.status,
        "total_jobs": batch.total_jobs,
        "completed_jobs": completed,
        "successful_jobs": successful,
        "failed_jobs": failed,
        "running_jobs": running,
        "pending_jobs": pending,
        "elapsed_seconds": round(elapsed, 1) if elapsed else None,
        "started_at": batch.started_at.isoformat() if batch.started_at else None,
        "completed_at": batch.completed_at.isoformat() if batch.completed_at else None,
        "tier_status": tier_status,
        "jobs": job_details,
        "config": batch.config,
    }


def list_batches(
    db: Session, limit: int = 20, status: Optional[str] = None
) -> List[Dict]:
    """List recent nightly batches."""
    query = db.query(NightlyBatch).order_by(NightlyBatch.started_at.desc())
    if status:
        query = query.filter(NightlyBatch.status == status)
    batches = query.limit(limit).all()

    return [
        {
            "batch_id": b.id,
            "status": b.status,
            "total_jobs": b.total_jobs,
            "completed_jobs": b.completed_jobs,
            "failed_jobs": b.failed_jobs,
            "started_at": b.started_at.isoformat() if b.started_at else None,
            "completed_at": b.completed_at.isoformat() if b.completed_at else None,
        }
        for b in batches
    ]


async def scheduled_nightly_batch():
    """
    Entry point for APScheduler cron trigger.

    Creates its own DB session since it runs outside a request context.
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        logger.info("Scheduled nightly batch starting...")
        batch = await launch_nightly_batch(db)
        logger.info(
            f"Scheduled nightly batch {batch.id} launched: "
            f"{batch.total_jobs} jobs"
        )
    except Exception as e:
        logger.error(f"Scheduled nightly batch failed: {e}", exc_info=True)
    finally:
        db.close()
