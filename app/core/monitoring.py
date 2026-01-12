"""
Monitoring and alerting module.

Provides job monitoring, metrics collection, and alerting capabilities.
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)


class JobMonitor:
    """
    Monitors ingestion job health and provides metrics.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_job_metrics(
        self,
        hours: int = 24,
        source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get job metrics for the specified time window.

        Args:
            hours: Time window in hours (default 24)
            source: Optional filter by source

        Returns:
            Dictionary with job metrics
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # Base query
        base_query = self.db.query(IngestionJob).filter(
            IngestionJob.created_at >= cutoff
        )
        if source:
            base_query = base_query.filter(IngestionJob.source == source)

        # Total jobs
        total_jobs = base_query.count()

        # Status breakdown
        status_counts = {}
        for status in JobStatus:
            count = base_query.filter(IngestionJob.status == status).count()
            status_counts[status.value] = count

        # Calculate rates
        success_rate = (status_counts.get("success", 0) / total_jobs * 100) if total_jobs > 0 else 0
        failure_rate = (status_counts.get("failed", 0) / total_jobs * 100) if total_jobs > 0 else 0

        # Average duration for completed jobs
        completed_jobs = base_query.filter(
            and_(
                IngestionJob.status == JobStatus.SUCCESS,
                IngestionJob.started_at.isnot(None),
                IngestionJob.completed_at.isnot(None)
            )
        ).all()

        durations = []
        for job in completed_jobs:
            if job.started_at and job.completed_at:
                duration = (job.completed_at - job.started_at).total_seconds()
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else 0

        # Get total rows inserted
        total_rows = base_query.filter(
            IngestionJob.rows_inserted.isnot(None)
        ).with_entities(func.sum(IngestionJob.rows_inserted)).scalar() or 0

        # Recent failures
        recent_failures = base_query.filter(
            IngestionJob.status == JobStatus.FAILED
        ).order_by(IngestionJob.created_at.desc()).limit(5).all()

        return {
            "time_window_hours": hours,
            "source_filter": source,
            "total_jobs": total_jobs,
            "status_breakdown": status_counts,
            "success_rate_percent": round(success_rate, 2),
            "failure_rate_percent": round(failure_rate, 2),
            "avg_duration_seconds": round(avg_duration, 2),
            "total_rows_inserted": total_rows,
            "recent_failures": [
                {
                    "job_id": job.id,
                    "source": job.source,
                    "error_message": job.error_message[:200] if job.error_message else None,
                    "created_at": job.created_at.isoformat(),
                    "retry_count": job.retry_count,
                    "can_retry": job.can_retry
                }
                for job in recent_failures
            ],
            "collected_at": datetime.utcnow().isoformat()
        }

    def get_source_health(self) -> Dict[str, Any]:
        """
        Get health status for each data source.

        Returns health score based on recent job success rates.
        """
        # Get unique sources from recent jobs
        cutoff = datetime.utcnow() - timedelta(hours=24)

        sources = self.db.query(IngestionJob.source).distinct().all()
        sources = [s[0] for s in sources]

        health_report = {}
        for source in sources:
            recent_jobs = self.db.query(IngestionJob).filter(
                and_(
                    IngestionJob.source == source,
                    IngestionJob.created_at >= cutoff
                )
            ).all()

            total = len(recent_jobs)
            success = len([j for j in recent_jobs if j.status == JobStatus.SUCCESS])
            failed = len([j for j in recent_jobs if j.status == JobStatus.FAILED])

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

            # Get last successful and failed jobs
            last_success = self.db.query(IngestionJob).filter(
                and_(
                    IngestionJob.source == source,
                    IngestionJob.status == JobStatus.SUCCESS
                )
            ).order_by(IngestionJob.completed_at.desc()).first()

            last_failure = self.db.query(IngestionJob).filter(
                and_(
                    IngestionJob.source == source,
                    IngestionJob.status == JobStatus.FAILED
                )
            ).order_by(IngestionJob.created_at.desc()).first()

            health_report[source] = {
                "status": health_status,
                "health_score": health_score,
                "jobs_24h": total,
                "success_24h": success,
                "failed_24h": failed,
                "last_success_at": last_success.completed_at.isoformat() if last_success and last_success.completed_at else None,
                "last_failure_at": last_failure.created_at.isoformat() if last_failure else None,
                "last_failure_message": last_failure.error_message[:200] if last_failure and last_failure.error_message else None
            }

        return {
            "sources": health_report,
            "overall_health": self._calculate_overall_health(health_report),
            "collected_at": datetime.utcnow().isoformat()
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
        self,
        failure_threshold: int = 3,
        time_window_hours: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Check for alert conditions.

        Args:
            failure_threshold: Number of failures to trigger alert
            time_window_hours: Time window for failure count

        Returns:
            List of active alerts
        """
        alerts = []
        cutoff = datetime.utcnow() - timedelta(hours=time_window_hours)

        # Get sources with high failure rates
        sources = self.db.query(IngestionJob.source).distinct().all()
        sources = [s[0] for s in sources]

        for source in sources:
            recent_failures = self.db.query(IngestionJob).filter(
                and_(
                    IngestionJob.source == source,
                    IngestionJob.status == JobStatus.FAILED,
                    IngestionJob.created_at >= cutoff
                )
            ).count()

            if recent_failures >= failure_threshold:
                alerts.append({
                    "alert_type": "high_failure_rate",
                    "source": source,
                    "severity": "critical" if recent_failures >= failure_threshold * 2 else "warning",
                    "message": f"Source '{source}' has {recent_failures} failures in the last {time_window_hours} hour(s)",
                    "failure_count": recent_failures,
                    "threshold": failure_threshold,
                    "time_window_hours": time_window_hours,
                    "created_at": datetime.utcnow().isoformat()
                })

        # Check for jobs stuck in running state
        stuck_threshold = timedelta(hours=2)
        stuck_jobs = self.db.query(IngestionJob).filter(
            and_(
                IngestionJob.status == JobStatus.RUNNING,
                IngestionJob.started_at < datetime.utcnow() - stuck_threshold
            )
        ).all()

        for job in stuck_jobs:
            running_time = datetime.utcnow() - job.started_at
            alerts.append({
                "alert_type": "stuck_job",
                "source": job.source,
                "severity": "warning",
                "message": f"Job {job.id} has been running for {running_time.total_seconds() / 3600:.1f} hours",
                "job_id": job.id,
                "started_at": job.started_at.isoformat(),
                "running_hours": round(running_time.total_seconds() / 3600, 2),
                "created_at": datetime.utcnow().isoformat()
            })

        # Check for no recent jobs (data staleness)
        for source in sources:
            last_job = self.db.query(IngestionJob).filter(
                IngestionJob.source == source
            ).order_by(IngestionJob.created_at.desc()).first()

            if last_job:
                time_since_last = datetime.utcnow() - last_job.created_at
                # Alert if no jobs in 24 hours
                if time_since_last > timedelta(hours=24):
                    alerts.append({
                        "alert_type": "data_staleness",
                        "source": source,
                        "severity": "info",
                        "message": f"No jobs for source '{source}' in {time_since_last.days} days, {time_since_last.seconds // 3600} hours",
                        "last_job_at": last_job.created_at.isoformat(),
                        "hours_since_last": round(time_since_last.total_seconds() / 3600, 2),
                        "created_at": datetime.utcnow().isoformat()
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
        "dashboard_generated_at": datetime.utcnow().isoformat()
    }
