"""
Monitoring and alerting module.

Provides job monitoring, metrics collection, and alerting capabilities.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, text

from app.core.models import IngestionJob, JobStatus
from app.core import webhook_service

logger = logging.getLogger(__name__)


class JobMonitor:
    """
    Monitors ingestion job health and provides metrics.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_job_metrics(
        self, hours: int = 24, source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get job metrics for the specified time window.

        Uses raw SQL to avoid enum deserialization issues (DB stores lowercase
        status values but SQLAlchemy enum expects uppercase).
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        params: Dict[str, Any] = {"cutoff": cutoff}

        source_filter = ""
        if source:
            source_filter = " AND source = :source"
            params["source"] = source

        # Status breakdown
        result = self.db.execute(text(
            f"SELECT LOWER(status) as status, COUNT(*) FROM ingestion_jobs "
            f"WHERE created_at >= :cutoff{source_filter} GROUP BY status"
        ), params)
        status_counts = {str(row[0]): int(row[1]) for row in result.fetchall()}
        total_jobs = sum(status_counts.values())

        # Calculate rates
        success_rate = (
            (status_counts.get("success", 0) / total_jobs * 100)
            if total_jobs > 0
            else 0
        )
        failure_rate = (
            (status_counts.get("failed", 0) / total_jobs * 100) if total_jobs > 0 else 0
        )

        # Average duration for completed jobs
        dur_result = self.db.execute(text(
            f"SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) "
            f"FROM ingestion_jobs WHERE created_at >= :cutoff{source_filter} "
            f"AND LOWER(status) = 'success' AND started_at IS NOT NULL AND completed_at IS NOT NULL"
        ), params)
        avg_duration = dur_result.scalar() or 0

        # Total rows inserted
        rows_result = self.db.execute(text(
            f"SELECT COALESCE(SUM(rows_inserted), 0) FROM ingestion_jobs "
            f"WHERE created_at >= :cutoff{source_filter} AND rows_inserted IS NOT NULL"
        ), params)
        total_rows = int(rows_result.scalar() or 0)

        # Recent failures
        fail_result = self.db.execute(text(
            f"SELECT id, source, error_message, created_at, retry_count "
            f"FROM ingestion_jobs WHERE created_at >= :cutoff{source_filter} "
            f"AND LOWER(status) = 'failed' ORDER BY created_at DESC LIMIT 5"
        ), params)

        recent_failures = []
        for row in fail_result.fetchall():
            err_msg = str(row[2])[:200] if row[2] else None
            recent_failures.append({
                "job_id": row[0],
                "source": row[1],
                "error_message": err_msg,
                "created_at": row[3].isoformat() if row[3] else None,
                "retry_count": row[4],
                "can_retry": False,
            })

        return {
            "time_window_hours": hours,
            "source_filter": source,
            "total_jobs": total_jobs,
            "status_breakdown": status_counts,
            "success_rate_percent": round(success_rate, 2),
            "failure_rate_percent": round(failure_rate, 2),
            "avg_duration_seconds": round(float(avg_duration), 2),
            "total_rows_inserted": total_rows,
            "recent_failures": recent_failures,
            "collected_at": datetime.utcnow().isoformat(),
        }

    def get_source_health(self) -> Dict[str, Any]:
        """
        Get health status for each data source.

        Uses raw SQL to avoid enum deserialization issues.
        """
        cutoff = datetime.utcnow() - timedelta(hours=24)

        # Get all distinct sources
        src_result = self.db.execute(text(
            "SELECT DISTINCT source FROM ingestion_jobs"
        ))
        sources = [row[0] for row in src_result.fetchall()]

        # Get 24h status counts per source in one query
        counts_result = self.db.execute(text(
            "SELECT source, LOWER(status), COUNT(*) FROM ingestion_jobs "
            "WHERE created_at >= :cutoff GROUP BY source, status"
        ), {"cutoff": cutoff})

        source_counts: Dict[str, Dict[str, int]] = {}
        for row in counts_result.fetchall():
            src, status, cnt = row[0], str(row[1]), int(row[2])
            source_counts.setdefault(src, {})
            source_counts[src][status] = cnt

        # Get last success/failure per source
        last_success_result = self.db.execute(text(
            "SELECT DISTINCT ON (source) source, completed_at "
            "FROM ingestion_jobs WHERE LOWER(status) = 'success' AND completed_at IS NOT NULL "
            "ORDER BY source, completed_at DESC"
        ))
        last_success_map = {row[0]: row[1] for row in last_success_result.fetchall()}

        last_fail_result = self.db.execute(text(
            "SELECT DISTINCT ON (source) source, created_at, error_message "
            "FROM ingestion_jobs WHERE LOWER(status) = 'failed' "
            "ORDER BY source, created_at DESC"
        ))
        last_fail_map = {row[0]: (row[1], row[2]) for row in last_fail_result.fetchall()}

        health_report = {}
        for source in sources:
            counts = source_counts.get(source, {})
            total = sum(counts.values())
            success = counts.get("success", 0)
            failed = counts.get("failed", 0)

            if total == 0:
                health_status = "unknown"
                health_score = 0
            elif failed == 0:
                health_status = "healthy"
                health_score = 100
            elif success == 0:
                health_status = "critical"
                health_score = 0
            elif failed / total > 0.5:
                health_status = "degraded"
                health_score = round((success / total) * 100)
            else:
                health_status = "warning"
                health_score = round((success / total) * 100)

            last_success_at = last_success_map.get(source)
            last_fail_info = last_fail_map.get(source)

            health_report[source] = {
                "status": health_status,
                "health_score": health_score,
                "jobs_24h": total,
                "success_24h": success,
                "failed_24h": failed,
                "last_success_at": last_success_at.isoformat() if last_success_at else None,
                "last_failure_at": last_fail_info[0].isoformat() if last_fail_info else None,
                "last_failure_message": str(last_fail_info[1])[:200]
                if last_fail_info and last_fail_info[1]
                else None,
            }

        return {
            "sources": health_report,
            "overall_health": self._calculate_overall_health(health_report),
            "collected_at": datetime.utcnow().isoformat(),
        }

    def _calculate_overall_health(self, health_report: Dict[str, Any]) -> str:
        """Calculate overall system health from source health."""
        if not health_report:
            return "unknown"

        statuses = [s["status"] for s in health_report.values()]

        if all(s == "healthy" for s in statuses):
            return "healthy"
        elif any(s == "critical" for s in statuses):
            return "critical"
        elif any(s == "degraded" for s in statuses):
            return "degraded"
        elif any(s == "warning" for s in statuses):
            return "warning"
        else:
            return "unknown"

    def check_alerts(
        self, failure_threshold: int = 3, time_window_hours: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Check for alert conditions.

        Uses raw SQL to avoid enum deserialization issues.
        """
        alerts = []
        cutoff = datetime.utcnow() - timedelta(hours=time_window_hours)

        # Sources with high failure rates
        fail_result = self.db.execute(text(
            "SELECT source, COUNT(*) FROM ingestion_jobs "
            "WHERE LOWER(status) = 'failed' AND created_at >= :cutoff "
            "GROUP BY source"
        ), {"cutoff": cutoff})

        for row in fail_result.fetchall():
            source, recent_failures = row[0], int(row[1])
            if recent_failures >= failure_threshold:
                alerts.append({
                    "alert_type": "high_failure_rate",
                    "source": source,
                    "severity": "critical"
                    if recent_failures >= failure_threshold * 2
                    else "warning",
                    "message": f"Source '{source}' has {recent_failures} failures in the last {time_window_hours} hour(s)",
                    "failure_count": recent_failures,
                    "threshold": failure_threshold,
                    "time_window_hours": time_window_hours,
                    "created_at": datetime.utcnow().isoformat(),
                })

        # Jobs stuck in running state
        stuck_threshold = datetime.utcnow() - timedelta(hours=2)
        stuck_result = self.db.execute(text(
            "SELECT id, source, started_at FROM ingestion_jobs "
            "WHERE LOWER(status) = 'running' AND started_at < :stuck"
        ), {"stuck": stuck_threshold})

        for row in stuck_result.fetchall():
            job_id, source, started_at = row[0], row[1], row[2]
            running_time = datetime.utcnow() - started_at
            alerts.append({
                "alert_type": "stuck_job",
                "source": source,
                "severity": "warning",
                "message": f"Job {job_id} has been running for {running_time.total_seconds() / 3600:.1f} hours",
                "job_id": job_id,
                "started_at": started_at.isoformat(),
                "running_hours": round(running_time.total_seconds() / 3600, 2),
                "created_at": datetime.utcnow().isoformat(),
            })

        # Data staleness — sources with no recent jobs
        stale_result = self.db.execute(text(
            "SELECT source, MAX(created_at) as last_job_at FROM ingestion_jobs "
            "GROUP BY source"
        ))

        for row in stale_result.fetchall():
            source, last_job_at = row[0], row[1]
            if last_job_at:
                time_since_last = datetime.utcnow() - last_job_at
                if time_since_last > timedelta(hours=24):
                    alerts.append({
                        "alert_type": "data_staleness",
                        "source": source,
                        "severity": "info",
                        "message": f"No jobs for source '{source}' in {time_since_last.days} days, {time_since_last.seconds // 3600} hours",
                        "last_job_at": last_job_at.isoformat(),
                        "hours_since_last": round(
                            time_since_last.total_seconds() / 3600, 2
                        ),
                        "created_at": datetime.utcnow().isoformat(),
                    })

        return alerts


def get_monitoring_dashboard(db: Session) -> Dict[str, Any]:
    """
    Get comprehensive monitoring dashboard data.

    Returns all metrics, health status, and alerts in one call.
    """
    monitor = JobMonitor(db)

    return {
        "metrics_24h": monitor.get_job_metrics(hours=24),
        "metrics_1h": monitor.get_job_metrics(hours=1),
        "source_health": monitor.get_source_health(),
        "alerts": monitor.check_alerts(),
        "dashboard_generated_at": datetime.utcnow().isoformat(),
    }


async def check_and_notify_alerts(
    db: Session, failure_threshold: int = 3, time_window_hours: int = 1
) -> Dict[str, Any]:
    """
    Check for alerts and send webhook notifications.

    Args:
        db: Database session
        failure_threshold: Number of failures to trigger alert
        time_window_hours: Time window for failure count

    Returns:
        Dictionary with alerts found and notifications sent
    """
    monitor = JobMonitor(db)
    alerts = monitor.check_alerts(
        failure_threshold=failure_threshold, time_window_hours=time_window_hours
    )

    if not alerts:
        return {"alerts_found": 0, "notifications_sent": 0, "alerts": []}

    notifications_sent = 0
    for alert in alerts:
        try:
            result = await webhook_service.notify_alert(
                alert_type=alert["alert_type"],
                source=alert["source"],
                message=alert["message"],
                details=alert,
            )
            if result.get("successful", 0) > 0:
                notifications_sent += 1
        except Exception as e:
            logger.error(f"Failed to send alert notification: {e}")

    return {
        "alerts_found": len(alerts),
        "notifications_sent": notifications_sent,
        "alerts": alerts,
    }


async def check_consecutive_failures(
    db: Session,
    threshold: int = 3,
) -> List[Dict[str, Any]]:
    """
    Check for site intel sources with N consecutive failures.

    Queries SiteIntelCollectionJob for sources where the last N jobs all failed.

    Args:
        db: Database session
        threshold: Number of consecutive failures to trigger alert

    Returns:
        List of sources with consecutive failures
    """
    from app.core.models_site_intel import SiteIntelCollectionJob

    # Get distinct domain/source pairs
    pairs = (
        db.query(
            SiteIntelCollectionJob.domain,
            SiteIntelCollectionJob.source,
        )
        .distinct()
        .all()
    )

    alerts = []
    for domain_val, source_val in pairs:
        # Get last N jobs for this source
        recent_jobs = (
            db.query(SiteIntelCollectionJob)
            .filter(
                SiteIntelCollectionJob.domain == domain_val,
                SiteIntelCollectionJob.source == source_val,
            )
            .order_by(SiteIntelCollectionJob.created_at.desc())
            .limit(threshold)
            .all()
        )

        if len(recent_jobs) < threshold:
            continue

        # Check if all are failed
        if all(j.status == "failed" for j in recent_jobs):
            alerts.append(
                {
                    "domain": domain_val,
                    "source": source_val,
                    "consecutive_failures": len(recent_jobs),
                    "last_error": recent_jobs[0].error_message[:200]
                    if recent_jobs[0].error_message
                    else None,
                    "last_failure_at": recent_jobs[0].created_at.isoformat()
                    if recent_jobs[0].created_at
                    else None,
                }
            )

    return alerts


async def check_and_notify_consecutive_failures(
    threshold: int = 3,
) -> Dict[str, Any]:
    """
    Check for consecutive failures and send webhook notifications.

    Args:
        threshold: Number of consecutive failures to trigger alert

    Returns:
        Summary of alerts found and notifications sent
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        alerts = await check_consecutive_failures(db, threshold=threshold)

        if not alerts:
            return {"alerts_found": 0, "notifications_sent": 0}

        notifications_sent = 0
        for alert in alerts:
            try:
                result = await webhook_service.notify_consecutive_failures(
                    source=alert["source"],
                    count=alert["consecutive_failures"],
                    domain=alert["domain"],
                )
                if result.get("successful", 0) > 0:
                    notifications_sent += 1
            except Exception as e:
                logger.error(f"Failed to send consecutive failure alert: {e}")

        return {
            "alerts_found": len(alerts),
            "notifications_sent": notifications_sent,
            "alerts": alerts,
        }

    finally:
        db.close()


async def notify_job_completion(
    job_id: int,
    source: str,
    status: JobStatus,
    rows_inserted: int = 0,
    error_message: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Send webhook notification for job completion.

    Args:
        job_id: The job ID
        source: The data source
        status: The job status
        rows_inserted: Number of rows inserted (for successful jobs)
        error_message: Error message (for failed jobs)
        config: Job configuration

    Returns:
        Webhook trigger result
    """
    try:
        if status == JobStatus.SUCCESS:
            return await webhook_service.notify_job_success(
                job_id=job_id, source=source, rows_inserted=rows_inserted, config=config
            )
        elif status == JobStatus.FAILED:
            return await webhook_service.notify_job_failed(
                job_id=job_id,
                source=source,
                error_message=error_message or "Unknown error",
                config=config,
            )
        else:
            return {"webhooks_triggered": 0}
    except Exception as e:
        logger.error(f"Failed to send job completion notification: {e}")
        return {"webhooks_triggered": 0, "error": str(e)}
