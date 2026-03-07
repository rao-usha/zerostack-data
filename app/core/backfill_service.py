"""
Backfill service.

Creates and submits ingestion jobs for a specific date range, translating
universal start/end dates into source-specific query parameters.

Backfill jobs use trigger="backfill" and do NOT advance the source watermark
so incremental loading is unaffected.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.core.job_queue_service import submit_job, WORKER_MODE
from app.core.scheduler_service import INCREMENTAL_PARAM_MAP, INCREMENTAL_END_PARAM_MAP

logger = logging.getLogger(__name__)


def create_backfill_job(
    db: Session,
    source: str,
    start_date: datetime,
    end_date: datetime,
    extra_config: Optional[Dict[str, Any]] = None,
) -> IngestionJob:
    """
    Create a single backfill ingestion job.

    Translates universal start/end dates into source-specific parameters
    via INCREMENTAL_PARAM_MAP and INCREMENTAL_END_PARAM_MAP.

    Sets trigger="backfill" and config["_backfill"]=True so the watermark
    guard in _handle_job_completion() skips advancement.
    """
    base_source = source.split(":")[0]
    config = dict(extra_config or {})
    config["_backfill"] = True

    # Inject source-specific start param
    start_mapping = INCREMENTAL_PARAM_MAP.get(base_source)
    if start_mapping:
        param_name, formatter = start_mapping
        config[param_name] = formatter(start_date)

    # Inject source-specific end param
    end_mapping = INCREMENTAL_END_PARAM_MAP.get(base_source)
    if end_mapping:
        param_name, formatter = end_mapping
        config[param_name] = formatter(end_date)

    job = IngestionJob(
        source=source,
        status=JobStatus.PENDING,
        config=config,
        trigger="backfill",
    )
    db.add(job)
    db.flush()

    logger.info(
        f"Created backfill job {job.id} for {source} "
        f"({start_date.date()} to {end_date.date()})"
    )
    return job


def launch_backfill(
    db: Session,
    sources: List[str],
    start_date: datetime,
    end_date: datetime,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create and submit backfill jobs for multiple sources.

    Jobs are submitted to the queue with priority 3 (below normal batch).
    """
    if not WORKER_MODE:
        raise RuntimeError(
            "Backfill requires WORKER_MODE=1. "
            "Set WORKER_MODE=1 in docker-compose.yml and start at least one worker."
        )

    job_ids = []
    for source in sources:
        job = create_backfill_job(db, source, start_date, end_date, extra_config)

        submit_job(
            db=db,
            job_type="ingestion",
            payload={
                "source": source,
                "config": job.config,
                "ingestion_job_id": job.id,
                "trigger": "backfill",
            },
            priority=3,
            job_table_id=job.id,
        )
        job_ids.append(job.id)

    db.commit()

    logger.info(
        f"Backfill launched: {len(job_ids)} jobs for "
        f"{start_date.date()} to {end_date.date()}"
    )

    return {
        "total_jobs": len(job_ids),
        "job_ids": job_ids,
        "sources": sources,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
