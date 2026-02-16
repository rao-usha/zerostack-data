"""
Job retry service.

Provides functionality to retry failed ingestion jobs with exponential backoff.
"""
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)

# Exponential backoff settings
BASE_DELAY_MINUTES = 5  # First retry after 5 minutes
MAX_DELAY_MINUTES = 60 * 24  # Max 24 hours between retries
BACKOFF_MULTIPLIER = 2  # Double the delay each retry
JITTER_FACTOR = 0.25  # ±25% random jitter to prevent thundering herd


def calculate_retry_delay(
    retry_count: int,
    config: Optional[Dict[str, Any]] = None,
) -> timedelta:
    """
    Calculate delay before next retry using exponential backoff with jitter.

    Adds ±25% random jitter to prevent thundering herd when multiple
    jobs fail and retry at the same time.

    Args:
        retry_count: Number of retries already attempted
        config: Optional per-source retry config dict with keys:
            backoff_base_min, backoff_max_min, backoff_multiplier

    Returns:
        Timedelta for delay before next retry
    """
    base = config.get("backoff_base_min", BASE_DELAY_MINUTES) if config else BASE_DELAY_MINUTES
    max_delay = config.get("backoff_max_min", MAX_DELAY_MINUTES) if config else MAX_DELAY_MINUTES
    multiplier = config.get("backoff_multiplier", BACKOFF_MULTIPLIER) if config else BACKOFF_MULTIPLIER

    delay_minutes = min(
        base * (multiplier ** retry_count),
        max_delay
    )
    # Apply jitter: ±25% randomization
    jitter = delay_minutes * JITTER_FACTOR * (2 * random.random() - 1)
    delay_minutes = max(1, delay_minutes + jitter)
    return timedelta(minutes=delay_minutes)


def get_retryable_jobs(db: Session, limit: int = 100) -> List[IngestionJob]:
    """
    Get list of failed jobs that can be retried.

    Args:
        db: Database session
        limit: Maximum number of jobs to return

    Returns:
        List of IngestionJob instances that can be retried
    """
    now = datetime.utcnow()

    return db.query(IngestionJob).filter(
        and_(
            IngestionJob.status == JobStatus.FAILED,
            IngestionJob.retry_count < IngestionJob.max_retries,
            # Either no next_retry_at set, or it's in the past
            (IngestionJob.next_retry_at == None) | (IngestionJob.next_retry_at <= now)
        )
    ).order_by(
        IngestionJob.created_at.desc()
    ).limit(limit).all()


def get_failed_jobs_summary(db: Session) -> Dict[str, Any]:
    """
    Get summary of failed jobs by source.

    Args:
        db: Database session

    Returns:
        Dictionary with failed jobs summary
    """
    failed_jobs = db.query(IngestionJob).filter(
        IngestionJob.status == JobStatus.FAILED
    ).all()

    by_source = {}
    retryable_count = 0
    exhausted_count = 0

    for job in failed_jobs:
        source = job.source
        if source not in by_source:
            by_source[source] = {
                "total": 0,
                "retryable": 0,
                "exhausted": 0,
                "jobs": []
            }

        by_source[source]["total"] += 1

        if job.can_retry:
            by_source[source]["retryable"] += 1
            retryable_count += 1
        else:
            by_source[source]["exhausted"] += 1
            exhausted_count += 1

        by_source[source]["jobs"].append({
            "id": job.id,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "error_message": job.error_message[:200] if job.error_message else None,
            "can_retry": job.can_retry
        })

    return {
        "total_failed": len(failed_jobs),
        "retryable": retryable_count,
        "exhausted": exhausted_count,
        "by_source": by_source
    }


def schedule_retry(
    db: Session,
    job_id: int,
    delay: Optional[timedelta] = None
) -> Optional[IngestionJob]:
    """
    Schedule a job for retry.

    Args:
        db: Database session
        job_id: ID of the job to retry
        delay: Optional custom delay before retry

    Returns:
        Updated IngestionJob or None if job not found/not retryable
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()

    if not job:
        logger.warning(f"Job {job_id} not found")
        return None

    if not job.can_retry:
        logger.warning(f"Job {job_id} cannot be retried (status={job.status}, retries={job.retry_count}/{job.max_retries})")
        return None

    # Calculate delay
    if delay is None:
        delay = calculate_retry_delay(job.retry_count)

    next_retry = datetime.utcnow() + delay
    job.next_retry_at = next_retry

    db.commit()
    logger.info(f"Job {job_id} scheduled for retry at {next_retry}")

    return job


def create_retry_job(
    db: Session,
    original_job: IngestionJob
) -> Optional[IngestionJob]:
    """
    Create a new job to retry a failed job.

    Args:
        db: Database session
        original_job: The failed job to retry

    Returns:
        New IngestionJob or None if original job cannot be retried
    """
    if not original_job.can_retry:
        logger.warning(f"Job {original_job.id} cannot be retried")
        return None

    # Create new job with same config
    new_job = IngestionJob(
        source=original_job.source,
        status=JobStatus.PENDING,
        config=original_job.config,
        retry_count=original_job.retry_count + 1,
        max_retries=original_job.max_retries,
        parent_job_id=original_job.id
    )

    db.add(new_job)

    # Update original job to prevent duplicate retries
    original_job.retry_count += 1
    original_job.next_retry_at = None  # Clear scheduled retry

    db.commit()
    db.refresh(new_job)

    logger.info(f"Created retry job {new_job.id} for original job {original_job.id} (retry {new_job.retry_count}/{new_job.max_retries})")

    return new_job


def mark_job_for_immediate_retry(
    db: Session,
    job_id: int
) -> Optional[IngestionJob]:
    """
    Mark a failed job for immediate retry by resetting its status to PENDING.

    This is simpler than creating a new job - it just resets the failed job.

    Args:
        db: Database session
        job_id: ID of the job to retry

    Returns:
        Updated IngestionJob or None if job not found/not retryable
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()

    if not job:
        logger.warning(f"Job {job_id} not found")
        return None

    if not job.can_retry:
        logger.warning(f"Job {job_id} cannot be retried")
        return None

    # Reset job for retry
    job.status = JobStatus.PENDING
    job.retry_count += 1
    job.started_at = None
    job.completed_at = None
    job.error_message = None
    job.error_details = None
    job.next_retry_at = None

    db.commit()
    logger.info(f"Job {job_id} marked for immediate retry (attempt {job.retry_count}/{job.max_retries})")

    # Audit trail
    try:
        from app.core import audit_service
        audit_service.log_collection(
            db,
            trigger_type="retry",
            source=job.source,
            job_id=job.id,
            job_type="ingestion",
            trigger_source="immediate_retry",
            config_snapshot=job.config,
        )
    except Exception:
        pass

    return job


def retry_all_eligible_jobs(
    db: Session,
    source: Optional[str] = None,
    limit: int = 10
) -> Dict[str, Any]:
    """
    Retry all eligible failed jobs.

    Args:
        db: Database session
        source: Optional filter by source
        limit: Maximum number of jobs to retry

    Returns:
        Dictionary with retry results
    """
    query = db.query(IngestionJob).filter(
        and_(
            IngestionJob.status == JobStatus.FAILED,
            IngestionJob.retry_count < IngestionJob.max_retries
        )
    )

    if source:
        query = query.filter(IngestionJob.source == source)

    jobs = query.order_by(IngestionJob.created_at.desc()).limit(limit).all()

    results = {
        "total_eligible": len(jobs),
        "retried": [],
        "skipped": []
    }

    for job in jobs:
        try:
            updated = mark_job_for_immediate_retry(db, job.id)
            if updated:
                results["retried"].append({
                    "job_id": job.id,
                    "source": job.source,
                    "retry_count": updated.retry_count,
                    "status": "pending"
                })
            else:
                results["skipped"].append({
                    "job_id": job.id,
                    "reason": "not retryable"
                })
        except Exception as e:
            logger.error(f"Failed to retry job {job.id}: {e}")
            results["skipped"].append({
                "job_id": job.id,
                "reason": str(e)
            })

    return results


# =============================================================================
# Automatic Retry Functions
# =============================================================================

def auto_schedule_retry(db: Session, job: IngestionJob) -> bool:
    """
    Automatically schedule a failed job for retry.

    Called when a job fails. If retries remain, schedules the job
    for retry with exponential backoff. Uses per-source retry config
    from SourceConfig when available.

    Args:
        db: Database session
        job: The failed job

    Returns:
        True if retry was scheduled, False if no retries remain
    """
    if job.status != JobStatus.FAILED:
        logger.warning(f"Job {job.id} is not failed, cannot schedule retry")
        return False

    if job.retry_count >= job.max_retries:
        logger.info(f"Job {job.id} has exhausted all retries ({job.retry_count}/{job.max_retries})")
        return False

    # Load per-source retry config
    retry_config = None
    try:
        from app.core import source_config_service
        retry_config = source_config_service.get_retry_config(db, job.source)
    except Exception:
        pass  # Fall back to global defaults

    # Calculate delay with exponential backoff
    delay = calculate_retry_delay(job.retry_count, config=retry_config)
    next_retry = datetime.utcnow() + delay

    job.next_retry_at = next_retry

    db.commit()
    logger.info(
        f"Job {job.id} scheduled for auto-retry at {next_retry} "
        f"(attempt {job.retry_count + 1}/{job.max_retries}, delay={delay})"
    )

    # Audit trail
    try:
        from app.core import audit_service
        audit_service.log_collection(
            db,
            trigger_type="retry",
            source=job.source,
            job_id=job.id,
            job_type="ingestion",
            trigger_source="auto_schedule",
            config_snapshot={"retry_at": next_retry.isoformat(), "attempt": job.retry_count + 1},
        )
    except Exception:
        pass

    return True


def get_jobs_ready_for_retry(db: Session, limit: int = 20) -> List[IngestionJob]:
    """
    Get jobs that are scheduled for retry and ready to run.

    Args:
        db: Database session
        limit: Maximum number of jobs to return

    Returns:
        List of jobs ready for retry
    """
    now = datetime.utcnow()

    return db.query(IngestionJob).filter(
        and_(
            IngestionJob.status == JobStatus.FAILED,
            IngestionJob.retry_count < IngestionJob.max_retries,
            IngestionJob.next_retry_at.isnot(None),
            IngestionJob.next_retry_at <= now
        )
    ).order_by(
        IngestionJob.next_retry_at.asc()
    ).limit(limit).all()


async def process_scheduled_retries(limit: int = 10) -> Dict[str, Any]:
    """
    Process jobs that are scheduled for retry.

    This function should be called periodically by the scheduler.

    Args:
        limit: Maximum number of jobs to process

    Returns:
        Dictionary with processing results
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    results = {
        "processed": 0,
        "jobs": [],
        "errors": []
    }

    try:
        jobs = get_jobs_ready_for_retry(db, limit)

        if not jobs:
            logger.debug("No jobs ready for scheduled retry")
            return results

        logger.info(f"Processing {len(jobs)} scheduled retries")

        for job in jobs:
            try:
                # Mark job for immediate retry
                updated = mark_job_for_immediate_retry(db, job.id)
                if updated:
                    # Execute the retry
                    from app.api.v1.jobs import run_ingestion_job
                    await run_ingestion_job(updated.id, updated.source, updated.config)

                    results["processed"] += 1
                    results["jobs"].append({
                        "job_id": job.id,
                        "source": job.source,
                        "retry_count": updated.retry_count
                    })
            except Exception as e:
                logger.error(f"Error processing retry for job {job.id}: {e}")
                results["errors"].append({
                    "job_id": job.id,
                    "error": str(e)
                })

    except Exception as e:
        logger.error(f"Error in process_scheduled_retries: {e}", exc_info=True)
        results["error"] = str(e)

    finally:
        db.close()

    return results
