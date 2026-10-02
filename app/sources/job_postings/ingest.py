"""
Job Posting Intelligence — ingestion entry points.

Called by the job dispatch system (app/api/v1/jobs.py) or directly from the API router.

RETIRED (SPEC_152, owner decision 2026-10-02): every entry point raises ``OldLaneRetired`` before
the collector is built, so ``job_postings:all`` / ``:company`` / ``:discover`` jobs fail loudly
with a pointer to the gated ``ats_boards`` lane. Stored rows stay readable.
"""

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.sources.job_postings.metadata import (
    generate_create_job_postings_sql,
    generate_create_company_ats_config_sql,
    generate_create_job_posting_snapshots_sql,
    DATASET_INFO,
)
from app.sources.job_postings.collector import JobPostingCollector
from app.sources.job_postings.retired import OLD_RUN_RETIRED, OldLaneRetired

logger = logging.getLogger(__name__)


def _ensure_tables(db: Session):
    """Create tables if they don't exist."""
    db.execute(text(generate_create_job_postings_sql()))
    db.execute(text(generate_create_company_ats_config_sql()))
    db.execute(text(generate_create_job_posting_snapshots_sql()))
    db.commit()


def _refuse(db: Session, job_id: int):
    """SPEC_152: record the refusal on the job row (best effort) and raise."""
    try:
        _update_job(db, job_id, "failed", error=OLD_RUN_RETIRED)
    except Exception:
        pass
    raise OldLaneRetired(OLD_RUN_RETIRED)


def _update_job(db: Session, job_id: int, status: str, records: int = 0, error: str = None):
    """Update ingestion job record."""
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        return
    job.status = JobStatus(status)
    job.records_processed = records
    if error:
        job.error_message = error
    db.commit()


async def ingest_job_postings_company(
    db: Session, job_id: int, company_id: int = None, force_rediscover: bool = False, **config
):
    """Collect job postings for a single company."""
    _refuse(db, job_id)
    _ensure_tables(db)
    _update_job(db, job_id, "running")

    company_id = company_id or config.get("company_id")
    if not company_id:
        _update_job(db, job_id, "failed", error="company_id is required")
        return

    force = force_rediscover or config.get("force_rediscover", False)

    async with JobPostingCollector() as collector:
        result = await collector.collect_company(db, int(company_id), force_rediscover=force)

    if result.error:
        _update_job(db, job_id, "failed", records=result.total_fetched, error=result.error)
    else:
        _update_job(db, job_id, "success", records=result.total_fetched)

    logger.info(
        f"Job posting collection for company {company_id}: "
        f"{result.total_fetched} fetched, {result.new_postings} new, "
        f"{result.closed_postings} closed, {result.duration_seconds:.1f}s"
    )


async def ingest_job_postings_all(
    db: Session, job_id: int, limit: int = None, skip_recent_hours: int = 24, **config
):
    """Collect job postings for all companies with websites."""
    _refuse(db, job_id)
    _ensure_tables(db)
    _update_job(db, job_id, "running")

    lim = limit or config.get("limit")
    skip_hrs = skip_recent_hours or config.get("skip_recent_hours", 24)

    async with JobPostingCollector() as collector:
        summary = await collector.collect_all(db, limit=lim, skip_recent_hours=int(skip_hrs))

    total = summary.get("total_fetched", 0)
    errors = summary.get("errors", 0)

    logger.info(f"Job posting bulk collection: {summary}")

    if errors > 0 and errors == summary.get("companies_processed", 0):
        _update_job(db, job_id, "failed", records=total, error=f"All {errors} companies failed")
        # SPEC_151: raise, or the dispatcher marks the job success over the failed status
        raise RuntimeError(f"All {errors} companies failed")
    _update_job(db, job_id, "success", records=total)


async def ingest_job_postings_discover(
    db: Session, job_id: int, company_id: int = None, **config
):
    """Just discover ATS type for a company (no job collection)."""
    _refuse(db, job_id)
    _ensure_tables(db)
    _update_job(db, job_id, "running")

    company_id = company_id or config.get("company_id")
    if not company_id:
        _update_job(db, job_id, "failed", error="company_id is required")
        return

    async with JobPostingCollector() as collector:
        result = await collector.discover_ats(db, int(company_id))

    if result.ats_type == "unknown":
        _update_job(db, job_id, "failed", error=result.error or "ATS not detected")
    else:
        _update_job(db, job_id, "success", records=1)

    logger.info(f"ATS discovery for company {company_id}: {result.ats_type} (token={result.board_token})")
