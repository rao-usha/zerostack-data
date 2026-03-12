"""
Post-ingestion DQ hook.

Lightweight, non-blocking hook called after complete_job() marks a job as
SUCCESS. Runs profiling, anomaly detection, and rule evaluation against
the just-ingested table.

Design: fire-and-forget via asyncio.create_task. Failures in the hook
never affect the ingestion job status.
"""

import asyncio
import logging
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


async def _run_post_ingestion_checks(
    job_id: int,
    table_name: str,
    source: Optional[str] = None,
) -> None:
    """
    Async post-ingestion DQ pipeline:
    1. Profile the ingested table
    2. Detect anomalies from the new profile
    3. Evaluate matching DQ rules
    """
    from app.core.database import get_session_factory

    SessionFactory = get_session_factory()
    db: Session = SessionFactory()

    try:
        # 1. Profile the table
        from app.core.data_profiling_service import profile_table

        logger.info(f"[DQ Hook] Profiling table '{table_name}' (job {job_id})")
        snapshot = profile_table(db, table_name, job_id=job_id, source=source)

        if not snapshot:
            logger.info(f"[DQ Hook] Profiling skipped for '{table_name}' (lock or error)")
            return

        # 2. Detect anomalies against new profile
        from app.core.anomaly_detection_service import detect_anomalies

        logger.info(f"[DQ Hook] Running anomaly detection for '{table_name}'")
        alerts = detect_anomalies(db, snapshot, table_name)

        critical_alerts = [a for a in alerts if a.severity and a.severity.value == "error"]
        if critical_alerts:
            logger.warning(
                f"[DQ Hook] {len(critical_alerts)} CRITICAL anomalies detected "
                f"on '{table_name}' after job {job_id}"
            )

        # 3. Evaluate matching DQ rules
        from app.core.data_quality_service import evaluate_rules_for_job
        from app.core.models import IngestionJob

        logger.info(f"[DQ Hook] Evaluating DQ rules for job {job_id}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        report = None
        if job:
            report = evaluate_rules_for_job(db, job, table_name)

        if report and report.rules_failed and report.rules_failed > 0:
            logger.warning(
                f"[DQ Hook] {report.rules_failed} rule failures "
                f"on '{table_name}' after job {job_id}"
            )

        logger.info(
            f"[DQ Hook] Post-ingestion checks complete for '{table_name}' "
            f"(job {job_id}): profile OK, {len(alerts)} anomalies, "
            f"{report.rules_failed if report and report.rules_failed else 0} rule failures"
        )

    except Exception as e:
        # Never propagate — the ingestion job must not be affected
        logger.error(f"[DQ Hook] Error in post-ingestion checks for job {job_id}: {e}")
    finally:
        db.close()


def schedule_post_ingestion_check(
    job_id: int,
    table_name: str,
    source: Optional[str] = None,
) -> None:
    """
    Schedule a post-ingestion DQ check as a fire-and-forget background task.

    Safe to call from synchronous code — gets or creates an event loop.
    If no event loop is running, logs a warning and skips (tests, scripts).
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_run_post_ingestion_checks(job_id, table_name, source))
        logger.debug(f"[DQ Hook] Scheduled post-ingestion check for job {job_id}")
    except RuntimeError:
        # No running event loop — likely called from sync context or tests
        logger.debug(
            f"[DQ Hook] No event loop — skipping async post-ingestion check "
            f"for job {job_id}"
        )
