"""
Batch collection orchestrator.

Enqueues all data sources with tier-based priority ordering.
Tier 1 (daily sources) runs first at highest priority, Tier 4
(quarterly sources) runs last after base data is collected.

Batch runs are tracked via IngestionJob.batch_run_id — status is
always computed live from job statuses, so nothing can get "stuck."

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
from app.core.models_queue import QueueJobStatus
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
    max_concurrent: int = 2  # max sources running simultaneously in this tier


# Tier 1 — Daily sources: fast government APIs + market data
TIER_1 = Tier(
    level=1,
    priority=10,
    name="Daily — Rates, Treasury & Markets",
    sources=[
        SourceDef("treasury", {"incremental": True}),
        SourceDef("fred", {"category": "interest_rates", "incremental": True}),
        SourceDef("prediction_markets", {"sources": ["kalshi", "polymarket"]}),
    ],
)

# Tier 2 — Weekly sources: moderate-frequency data
TIER_2 = Tier(
    level=2,
    priority=7,
    name="Weekly — Energy, Trade & Weather",
    sources=[
        SourceDef("eia", {"dataset": "petroleum_weekly", "subcategory": "consumption", "frequency": "weekly", "incremental": True}),
        SourceDef("cftc_cot", {"report_type": "all", "incremental": True}),
        # web_traffic, github — no ingest functions implemented yet
        SourceDef("noaa", {"dataset": "daily_summaries", "incremental": True}),
    ],
)

# Tier 3 — Monthly sources: government releases, regulatory filings
TIER_3 = Tier(
    level=3,
    priority=5,
    name="Monthly — Employment, Healthcare & Regulatory",
    max_concurrent=3,
    sources=[
        SourceDef("bea", {"dataset": "gdp", "table_name": "T10101", "incremental": True}),
        SourceDef("bls", {"dataset": "ces", "incremental": True}),
        SourceDef("eia", {"dataset": "electricity", "subcategory": "retail_sales", "incremental": True}),
        SourceDef("eia", {"dataset": "natural_gas", "subcategory": "consumption", "incremental": True}),
        SourceDef("fema", {"dataset": "disasters", "incremental": True}),
        SourceDef("fdic", {"dataset": "financials", "incremental": True}),
        SourceDef("sec:formadv"),
        # form_d — no ingest function implemented yet
        SourceDef("cms", {"dataset": "utilization", "incremental": True}),
        SourceDef("fbi_crime", {"dataset": "ucr"}),
        SourceDef("irs_soi", {"dataset": "zip_income", "year": 2021, "incremental": True}),
        SourceDef("data_commons:us_states"),
        SourceDef("fcc_broadband:all_states"),
        # app_rankings — no ingest function implemented yet
        SourceDef("job_postings:all", {"skip_recent_hours": 600}),
    ],
)

# Tier 4 — Quarterly sources: SEC, Census, trade, and slow-moving data
TIER_4 = Tier(
    level=4,
    priority=3,
    name="Quarterly — Census, SEC & Trade",
    sources=[
        SourceDef("census", {"survey": "acs5", "geo_level": "county", "year": 2023, "table_id": "B01001", "incremental": True}),
        SourceDef("bea", {"dataset": "regional", "table_name": "SAGDP2N", "incremental": True}),
        # sec — requires cik per-company; skip in batch (use dedicated PE pipeline)
        SourceDef("uspto", {"incremental": True}),
        SourceDef("us_trade:summary", {"year": "2025"}),
        SourceDef("bts", {"incremental": True}),
        SourceDef("international_econ:worldbank_countries"),
        SourceDef("realestate", {"incremental": True}),
        # opencorporates — no ingest function implemented yet
        SourceDef("usda:annual_summary", {"year": "2025"}),
    ],
)

TIERS = [TIER_1, TIER_2, TIER_3, TIER_4]

# Lookup: tier level → Tier object
TIER_BY_LEVEL = {t.level: t for t in TIERS}

# Agentic sources use separate executor types (not in batch tiers;
# triggered on-demand via their own endpoints)
AGENTIC_SOURCE_MAP = {
    "site_intel_full_sync": "site_intel",
    "people_batch": "people",
    "pe_batch": "pe",
}


def resolve_effective_tiers(db) -> list:
    """
    Merge hardcoded TIERS with DB overrides. Returns list of effective Tier objects.

    When DB tables are empty, returns an exact copy of the hardcoded TIERS.
    """
    import copy
    from app.core.models import BatchTierConfig, BatchSourceTierOverride

    effective = copy.deepcopy(TIERS)

    # 1. Apply tier-level overrides (priority, max_concurrent, enabled)
    tier_configs = db.query(BatchTierConfig).all()
    tier_config_map = {tc.tier_level: tc for tc in tier_configs}

    for tier in effective:
        override = tier_config_map.get(tier.level)
        if override:
            if not override.enabled:
                tier.sources = []  # Disable entire tier
                continue
            if override.priority is not None:
                tier.priority = override.priority
            if override.max_concurrent is not None:
                tier.max_concurrent = override.max_concurrent

    # 2. Apply source-level overrides (move between tiers, disable, config override)
    source_overrides = db.query(BatchSourceTierOverride).all()
    override_map = {so.source_key: so for so in source_overrides}

    # Remove overridden sources from their current tiers
    moved_sources = {}
    for tier in effective:
        remaining = []
        for source_def in tier.sources:
            override = override_map.get(source_def.key)
            if override:
                if not override.enabled:
                    continue  # Drop from batch
                if override.tier_level is not None and override.tier_level != tier.level:
                    moved_sources[source_def.key] = (source_def, override)
                    continue  # Will be added to target tier
                if override.default_config:
                    source_def.default_config = {**source_def.default_config, **override.default_config}
            remaining.append(source_def)
        tier.sources = remaining

    # Add moved sources to their target tiers
    tier_by_level = {t.level: t for t in effective}
    for key, (source_def, override) in moved_sources.items():
        target = tier_by_level.get(override.tier_level)
        if target:
            if override.default_config:
                source_def.default_config = {**source_def.default_config, **override.default_config}
            target.sources.append(source_def)

    return [t for t in effective if t.sources]  # Drop empty tiers


# =============================================================================
# Collection groups (flexible replacement for hardcoded tiers)
# =============================================================================

# Default groups seeded from current tier definitions
DEFAULT_COLLECTION_GROUPS = [
    {"name": "critical", "description": "Daily fast APIs — rates, treasury, markets", "priority": 1, "max_concurrent": 2,
     "sources": ["treasury", "fred", "prediction_markets"]},
    {"name": "economic", "description": "Weekly energy, trade, and weather data", "priority": 3, "max_concurrent": 3,
     "sources": ["eia", "cftc_cot", "noaa"]},
    {"name": "government", "description": "Monthly government releases and regulatory filings", "priority": 5, "max_concurrent": 4,
     "sources": ["bea", "bls", "fema", "fdic", "sec:formadv", "cms", "fbi_crime", "irs_soi",
                  "data_commons:us_states", "fcc_broadband:all_states", "job_postings:all"]},
    {"name": "deep", "description": "Quarterly Census, SEC, trade, and slow-moving data", "priority": 8, "max_concurrent": 3,
     "sources": ["census", "uspto", "us_trade:summary", "bts", "international_econ:worldbank_countries",
                  "realestate", "usda:annual_summary"]},
]

# Map from default group sources to their default configs (from tier defs)
_DEFAULT_SOURCE_CONFIGS: Dict[str, Dict] = {}
for _tier in TIERS:
    for _src in _tier.sources:
        _DEFAULT_SOURCE_CONFIGS[_src.key] = _src.default_config


def resolve_collection_groups(db) -> List[Dict]:
    """
    Build the effective list of collection groups from DB + defaults.

    If CollectionGroup table has rows, use those. Otherwise seed from defaults.
    Each group includes its sources resolved from SourceConfig or fallback defaults.

    Returns list of dicts: [{name, priority, max_concurrent, sources: [{key, config}]}]
    """
    from app.core.models import CollectionGroup, SourceConfig

    db_groups = db.query(CollectionGroup).filter(CollectionGroup.enabled == True).all()  # noqa: E712

    if db_groups:
        groups = []
        for g in db_groups:
            # Find sources assigned to this group
            source_configs = (
                db.query(SourceConfig)
                .filter(SourceConfig.collection_group == g.name, SourceConfig.enabled == True)  # noqa: E712
                .order_by(SourceConfig.priority)
                .all()
            )
            sources = []
            for sc in source_configs:
                sources.append({
                    "key": sc.source,
                    "config": {},
                    "depends_on": sc.depends_on or [],
                })
            groups.append({
                "name": g.name,
                "priority": g.priority,
                "max_concurrent": g.max_concurrent,
                "sources": sources,
            })
        return sorted(groups, key=lambda g: g["priority"])

    # Fallback: use defaults (matches current tier behavior)
    groups = []
    for dflt in DEFAULT_COLLECTION_GROUPS:
        sources = []
        for key in dflt["sources"]:
            sources.append({
                "key": key,
                "config": _DEFAULT_SOURCE_CONFIGS.get(key, {}),
                "depends_on": [],
            })
        groups.append({
            "name": dflt["name"],
            "priority": dflt["priority"],
            "max_concurrent": dflt["max_concurrent"],
            "sources": sources,
        })
    return groups


def seed_default_collection_groups(db) -> int:
    """
    Seed default collection groups and source configs into DB.

    Idempotent — skips groups/sources that already exist.
    Returns number of groups created.
    """
    from app.core.models import CollectionGroup, SourceConfig

    created = 0
    for dflt in DEFAULT_COLLECTION_GROUPS:
        existing = db.query(CollectionGroup).filter(CollectionGroup.name == dflt["name"]).first()
        if not existing:
            db.add(CollectionGroup(
                name=dflt["name"],
                description=dflt["description"],
                priority=dflt["priority"],
                max_concurrent=dflt["max_concurrent"],
            ))
            created += 1

        # Ensure source configs exist with group assignment
        for source_key in dflt["sources"]:
            sc = db.query(SourceConfig).filter(SourceConfig.source == source_key).first()
            if sc:
                if not sc.collection_group:
                    sc.collection_group = dflt["name"]
            else:
                db.add(SourceConfig(
                    source=source_key,
                    collection_group=dflt["name"],
                    priority=dflt["priority"],
                ))

    if created:
        db.commit()
        logger.info(f"Seeded {created} default collection groups")

    return created


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
    group_name: Optional[str] = None,
    mode: str = "full",
) -> Dict:
    """
    Launch a batch collection.

    Creates an IngestionJob + JobQueue entry for each source.
    Each job is tagged with a shared batch_run_id and trigger="batch".
    Status is computed live from jobs — nothing can get stuck.

    Args:
        db: Database session
        config: Optional overrides (e.g. {"skip_sources": ["kaggle"]})
        tiers: Optional list of tier levels to run (legacy, default: all)
        sources: Optional list of specific source keys to run
        group_name: Optional collection group name to target
        mode: "full" or "incremental" (incremental uses watermarks)

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
    trigger_type = "incremental" if mode == "incremental" else "batch"

    # Build watermark lookup for incremental mode
    watermark_map: Dict[str, Optional[datetime]] = {}
    if mode == "incremental":
        from app.core.models import SourceWatermark
        watermarks = db.query(SourceWatermark).all()
        watermark_map = {w.source: w.last_success_at for w in watermarks}

    total = 0
    job_ids = []
    effective_tiers = resolve_effective_tiers(db)

    # Filter by group_name if specified
    if group_name:
        # Map group names to tier levels for backward compat
        group_to_sources = {}
        for dflt in DEFAULT_COLLECTION_GROUPS:
            group_to_sources[dflt["name"]] = set(dflt["sources"])
        if group_name in group_to_sources:
            sources = list(group_to_sources[group_name]) if not sources else sources

    target_tiers = [t for t in effective_tiers if tiers is None or t.level in tiers]

    from app.core.job_splitter import get_split_config, create_split_jobs

    for tier in target_tiers:
        for source_def in tier.sources:
            # Filter by skip list or explicit source list
            if source_def.key in skip_sources:
                continue
            if sources and source_def.key not in sources:
                continue

            # Determine job type — agentic sources use their own executor
            job_type = AGENTIC_SOURCE_MAP.get(source_def.key, "ingestion")

            # Tier 2+ jobs start as BLOCKED; tier 1 starts as PENDING
            lower_tiers = [t.level for t in effective_tiers if t.level < tier.level]
            is_blocked = len(lower_tiers) > 0
            ing_status = JobStatus.BLOCKED if is_blocked else JobStatus.PENDING
            queue_status = QueueJobStatus.BLOCKED if is_blocked else None

            # Check if this source can be split into parallel jobs
            split_config = get_split_config(source_def.key)
            if split_config and job_type != "ingestion":
                # Skip agentic-only check — site_intel sources use split
                pass

            # Build source config with incremental watermark if applicable
            source_config = dict(source_def.default_config)
            if mode == "incremental" and source_def.key in watermark_map:
                last_run = watermark_map[source_def.key]
                if last_run:
                    source_config["since"] = last_run.isoformat()

            if split_config:
                # Create N parallel jobs instead of 1
                base_payload = {
                    "source": source_def.key,
                    "config": source_config,
                    "batch_id": batch_run_id,
                    "trigger": trigger_type,
                    "tier": tier.level,
                    "tier_max_concurrent": tier.max_concurrent,
                }
                if source_def.key in AGENTIC_SOURCE_MAP:
                    base_payload.update(source_def.default_config)

                split_ids = create_split_jobs(
                    db=db,
                    source_key=source_def.key,
                    job_type=job_type,
                    base_payload=base_payload,
                    priority=tier.priority,
                    queue_status=queue_status,
                )
                total += len(split_ids)
                logger.info(
                    f"Split {source_def.key} into {len(split_ids)} parallel jobs "
                    f"(tier {tier.level})"
                )
                continue

            # Non-splittable source: single job as before
            ing_job = IngestionJob(
                source=source_def.key,
                status=ing_status,
                config=source_config,
                batch_run_id=batch_run_id,
                trigger=trigger_type,
                tier=tier.level,
            )
            db.add(ing_job)
            db.flush()  # get the ID

            # Build queue payload
            payload = {
                "source": source_def.key,
                "config": source_config,
                "ingestion_job_id": ing_job.id,
                "batch_id": batch_run_id,
                "trigger": trigger_type,
                "tier": tier.level,
                "tier_max_concurrent": tier.max_concurrent,
            }

            # Agentic executors read config from top-level payload keys
            if source_def.key in AGENTIC_SOURCE_MAP:
                payload.update(source_def.default_config)

            submit_job(
                db=db,
                job_type=job_type,
                payload=payload,
                priority=tier.priority,
                job_table_id=ing_job.id,
                status=queue_status,
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
    No separate batch table needed. Nothing can get "stuck."
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
    blocked = sum(1 for j in jobs if j.status == JobStatus.BLOCKED)
    completed = successful + failed
    total = len(jobs)

    # Derive overall status live from job statuses
    if pending + running + blocked == 0 and total > 0:
        if failed == 0:
            status = "completed"
        elif successful == 0:
            status = "failed"
        else:
            status = "partial_success"
    elif running > 0 or pending > 0 or blocked > 0:
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
                "blocked": 0,
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
        elif j.status == JobStatus.BLOCKED:
            tier_status[tier_key]["blocked"] += 1

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

    # Result aggregation
    total_rows = sum(j.rows_inserted or 0 for j in jobs)
    sources_succeeded = [j.source for j in jobs if j.status == JobStatus.SUCCESS]
    sources_failed = [j.source for j in jobs if j.status == JobStatus.FAILED]
    top_errors = []
    for j in jobs:
        if j.status == JobStatus.FAILED and j.error_message:
            top_errors.append({
                "source": j.source,
                "error": j.error_message[:200],
            })

    # LLM cost aggregation
    llm_cost_summary = None
    try:
        from app.core.models import LLMUsage
        batch_job_ids = [j.id for j in jobs]
        if batch_job_ids:
            cost_row = (
                db.query(
                    func.count().label("llm_calls"),
                    func.sum(LLMUsage.input_tokens).label("input_tokens"),
                    func.sum(LLMUsage.output_tokens).label("output_tokens"),
                    func.sum(LLMUsage.cost_usd).label("cost_usd"),
                )
                .filter(LLMUsage.job_id.in_(batch_job_ids))
                .first()
            )
            if cost_row and cost_row.llm_calls:
                llm_cost_summary = {
                    "llm_calls": cost_row.llm_calls,
                    "total_tokens": (cost_row.input_tokens or 0) + (cost_row.output_tokens or 0),
                    "total_cost_usd": float(cost_row.cost_usd or 0),
                }
    except Exception:
        pass

    return {
        "batch_run_id": batch_run_id,
        "status": status,
        "total_jobs": total,
        "completed_jobs": completed,
        "successful_jobs": successful,
        "failed_jobs": failed,
        "running_jobs": running,
        "pending_jobs": pending,
        "blocked_jobs": blocked,
        "elapsed_seconds": round(elapsed, 1) if elapsed else None,
        "started_at": started_at.isoformat() if started_at else None,
        "completed_at": completed_at.isoformat() if completed_at else None,
        "tier_status": tier_status,
        "jobs": job_details,
        # Result aggregation
        "total_rows_inserted": total_rows,
        "sources_succeeded": sources_succeeded,
        "sources_failed": sources_failed,
        "top_errors": top_errors[:10],
        "llm_cost": llm_cost_summary,
    }


def cancel_batch_run(db: Session, batch_run_id: str) -> Optional[Dict]:
    """
    Cancel all PENDING/RUNNING jobs in a batch.

    Sets matching jobs to FAILED with "Batch cancelled by user" error.
    Running jobs stop within one heartbeat interval (30s) because the worker
    heartbeat loop checks for "Cancelled" in the error message.
    """
    from app.core.models_queue import JobQueue, QueueJobStatus

    jobs = db.query(IngestionJob).filter(
        IngestionJob.batch_run_id == batch_run_id
    ).all()
    if not jobs:
        return None

    cancelled_pending = cancelled_running = cancelled_blocked = already_complete = 0
    for job in jobs:
        if job.status == JobStatus.PENDING:
            job.status = JobStatus.FAILED
            job.error_message = "Batch cancelled by user"
            job.completed_at = datetime.utcnow()
            cancelled_pending += 1
        elif job.status == JobStatus.BLOCKED:
            job.status = JobStatus.FAILED
            job.error_message = "Batch cancelled by user"
            job.completed_at = datetime.utcnow()
            cancelled_blocked += 1
        elif job.status == JobStatus.RUNNING:
            job.status = JobStatus.FAILED
            job.error_message = "Batch cancelled by user"
            job.completed_at = datetime.utcnow()
            cancelled_running += 1
        else:
            already_complete += 1

        # Also mark job_queue row so worker heartbeat detects cancellation
        queue_row = db.query(JobQueue).filter(
            JobQueue.job_table_id == job.id
        ).first()
        if queue_row and queue_row.status in (
            QueueJobStatus.PENDING, QueueJobStatus.CLAIMED,
            QueueJobStatus.RUNNING, QueueJobStatus.BLOCKED,
        ):
            queue_row.status = QueueJobStatus.FAILED
            queue_row.error_message = "Batch cancelled by user"
            queue_row.completed_at = datetime.utcnow()

    db.commit()
    return {
        "batch_run_id": batch_run_id,
        "cancelled_pending": cancelled_pending,
        "cancelled_blocked": cancelled_blocked,
        "cancelled_running": cancelled_running,
        "already_complete": already_complete,
        "total_jobs": len(jobs),
    }


def rerun_failed_in_batch(db: Session, batch_run_id: str) -> Optional[Dict]:
    """
    Rerun all FAILED jobs in a batch.

    Resets each failed IngestionJob to PENDING (or BLOCKED for tier 2+),
    creates a new JobQueue entry, and triggers promotion for tier ordering.

    Returns summary of rerun operations.
    """
    from app.core.models_queue import JobQueue

    jobs = (
        db.query(IngestionJob)
        .filter(IngestionJob.batch_run_id == batch_run_id)
        .all()
    )
    if not jobs:
        return None

    failed_jobs = [j for j in jobs if j.status == JobStatus.FAILED]
    if not failed_jobs:
        return {
            "batch_run_id": batch_run_id,
            "rerun_count": 0,
            "message": "No failed jobs to rerun",
        }

    # Determine which tiers have non-terminal jobs (already running/pending)
    active_tiers = set()
    for j in jobs:
        if j.status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.BLOCKED):
            active_tiers.add(j.tier or 0)

    # Find the lowest tier among failed jobs to determine blocking
    failed_tiers = {j.tier or 0 for j in failed_jobs}
    min_rerun_tier = min(failed_tiers)

    rerun_count = 0
    for job in failed_jobs:
        tier_level = job.tier or 0

        # Determine effective tiers that have active/rerun jobs below this one
        has_lower_active = any(
            t < tier_level for t in (active_tiers | failed_tiers) if t < tier_level
        )
        is_blocked = has_lower_active and tier_level > min_rerun_tier

        # Reset IngestionJob
        ing_status = JobStatus.BLOCKED if is_blocked else JobStatus.PENDING
        queue_status = QueueJobStatus.BLOCKED if is_blocked else None

        job.status = ing_status
        job.started_at = None
        job.completed_at = None
        job.error_message = None
        job.rows_inserted = None

        # Clean up old queue row
        old_queue = db.query(JobQueue).filter(
            JobQueue.job_table_id == job.id
        ).first()
        if old_queue:
            db.delete(old_queue)

        # Determine job type
        job_type = AGENTIC_SOURCE_MAP.get(job.source, "ingestion")

        # Look up tier config for max_concurrent
        tier_obj = TIER_BY_LEVEL.get(tier_level)
        max_concurrent = tier_obj.max_concurrent if tier_obj else 2
        tier_priority = tier_obj.priority if tier_obj else 0

        payload = {
            "source": job.source,
            "config": job.config or {},
            "ingestion_job_id": job.id,
            "batch_id": batch_run_id,
            "trigger": "batch",
            "tier": tier_level,
            "tier_max_concurrent": max_concurrent,
        }

        if job.source in AGENTIC_SOURCE_MAP:
            payload.update(job.config or {})

        submit_job(
            db=db,
            job_type=job_type,
            payload=payload,
            priority=tier_priority,
            job_table_id=job.id,
            status=queue_status,
        )

        rerun_count += 1

    db.commit()

    # Promote any blocked jobs whose lower tiers are already complete
    from app.core.job_queue_service import promote_blocked_jobs
    promoted = promote_blocked_jobs(db, batch_run_id)

    logger.info(
        f"Batch {batch_run_id}: rerunning {rerun_count} failed jobs "
        f"({promoted} immediately promoted)"
    )

    return {
        "batch_run_id": batch_run_id,
        "rerun_count": rerun_count,
        "promoted": promoted,
        "sources": [j.source for j in failed_jobs],
    }


async def check_and_notify_batch_completion(db: Session, batch_run_id: str):
    """
    If all batch jobs are terminal (not PENDING/RUNNING), send summary webhook.

    Called after each job completes. Only fires when the very last job finishes.
    """
    pending_or_running = (
        db.query(func.count())
        .select_from(IngestionJob)
        .filter(
            IngestionJob.batch_run_id == batch_run_id,
            IngestionJob.status.in_([
                JobStatus.PENDING, JobStatus.RUNNING, JobStatus.BLOCKED,
            ]),
        )
        .scalar()
    )
    if pending_or_running > 0:
        return

    status_data = get_batch_run_status(db, batch_run_id)
    if not status_data:
        return

    from app.core.webhook_service import notify_batch_completed

    await notify_batch_completed(
        batch_run_id=batch_run_id,
        status=status_data["status"],
        total_jobs=status_data["total_jobs"],
        successful_jobs=status_data["successful_jobs"],
        failed_jobs=status_data["failed_jobs"],
        elapsed_seconds=status_data.get("elapsed_seconds"),
        total_rows=status_data.get("total_rows_inserted", 0),
        top_errors=status_data.get("top_errors", []),
    )


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
            func.sum(case((IngestionJob.status == JobStatus.BLOCKED, 1), else_=0)).label("blocked"),
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
        blocked_count = row.blocked or 0
        completed = successful + failed

        # Derive status live
        if pending_count + running_count + blocked_count == 0 and total > 0:
            if failed == 0:
                batch_status = "completed"
            elif successful == 0:
                batch_status = "failed"
            else:
                batch_status = "partial_success"
        elif running_count > 0 or pending_count > 0 or blocked_count > 0:
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
            "blocked_jobs": blocked_count,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        })

    return results


# =============================================================================
# Legacy wrappers (deprecated — use launch_batch_collection / get_batch_run_status)
# =============================================================================


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


async def scheduled_batch_collection():
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
