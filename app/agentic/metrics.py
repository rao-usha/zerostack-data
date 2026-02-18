"""
Metrics and monitoring for agentic portfolio collection jobs.

Provides:
- Job success/failure counters
- Strategy execution time tracking
- Token usage and cost tracking
- Per-investor statistics
- Real-time metrics for monitoring dashboards
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StrategyMetrics:
    """Metrics for a single strategy."""

    name: str
    executions: int = 0
    successes: int = 0
    failures: int = 0
    total_duration_seconds: float = 0.0
    total_companies_found: int = 0
    total_requests_made: int = 0
    total_tokens_used: int = 0
    total_cost_usd: float = 0.0
    last_execution: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.executions == 0:
            return 0.0
        return (self.successes / self.executions) * 100

    @property
    def avg_duration_seconds(self) -> float:
        """Calculate average execution duration."""
        if self.executions == 0:
            return 0.0
        return self.total_duration_seconds / self.executions

    @property
    def avg_companies_per_execution(self) -> float:
        """Calculate average companies found per execution."""
        if self.successes == 0:
            return 0.0
        return self.total_companies_found / self.successes

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "name": self.name,
            "executions": self.executions,
            "successes": self.successes,
            "failures": self.failures,
            "success_rate": round(self.success_rate, 2),
            "avg_duration_seconds": round(self.avg_duration_seconds, 3),
            "avg_companies_per_execution": round(self.avg_companies_per_execution, 2),
            "total_companies_found": self.total_companies_found,
            "total_requests_made": self.total_requests_made,
            "total_tokens_used": self.total_tokens_used,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "last_execution": self.last_execution.isoformat()
            if self.last_execution
            else None,
        }


@dataclass
class JobMetrics:
    """Metrics for collection jobs."""

    total_jobs: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0
    total_duration_seconds: float = 0.0
    total_companies_found: int = 0
    total_tokens_used: int = 0
    total_cost_usd: float = 0.0
    jobs_last_hour: int = 0
    jobs_last_24h: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate job success rate."""
        completed = self.successful_jobs + self.failed_jobs
        if completed == 0:
            return 0.0
        return (self.successful_jobs / completed) * 100

    @property
    def avg_duration_seconds(self) -> float:
        """Calculate average job duration."""
        completed = self.successful_jobs + self.failed_jobs
        if completed == 0:
            return 0.0
        return self.total_duration_seconds / completed

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "total_jobs": self.total_jobs,
            "by_status": {
                "pending": self.pending_jobs,
                "running": self.running_jobs,
                "successful": self.successful_jobs,
                "failed": self.failed_jobs,
            },
            "success_rate": round(self.success_rate, 2),
            "avg_duration_seconds": round(self.avg_duration_seconds, 3),
            "total_companies_found": self.total_companies_found,
            "total_tokens_used": self.total_tokens_used,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "throughput": {
                "jobs_last_hour": self.jobs_last_hour,
                "jobs_last_24h": self.jobs_last_24h,
            },
        }


@dataclass
class InvestorMetrics:
    """Metrics for a single investor."""

    investor_id: int
    investor_type: str
    investor_name: str
    jobs_run: int = 0
    last_collection: Optional[datetime] = None
    total_companies: int = 0
    total_cost_usd: float = 0.0
    strategies_used: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "investor_id": self.investor_id,
            "investor_type": self.investor_type,
            "investor_name": self.investor_name,
            "jobs_run": self.jobs_run,
            "last_collection": self.last_collection.isoformat()
            if self.last_collection
            else None,
            "total_companies": self.total_companies,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "strategies_used": list(set(self.strategies_used)),
        }


class MetricsCollector:
    """
    Collects and aggregates metrics for agentic portfolio collection.

    Thread-safe singleton that tracks:
    - Job-level metrics (success/failure, duration)
    - Strategy-level metrics (per-strategy performance)
    - Investor-level metrics (cost per investor)
    - Resource usage (tokens, API calls)

    Usage:
        metrics = get_metrics_collector()
        metrics.record_job_start(job_id, investor_id, investor_type)
        metrics.record_strategy_execution(strategy_name, success=True, duration=1.5)
        metrics.record_job_complete(job_id, success=True, companies_found=10)
    """

    _instance: Optional["MetricsCollector"] = None
    _lock = Lock()

    def __new__(cls) -> "MetricsCollector":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._metrics_lock = Lock()

        # Strategy metrics
        self._strategy_metrics: Dict[str, StrategyMetrics] = {}

        # Job metrics
        self._job_metrics = JobMetrics()

        # Per-investor metrics
        self._investor_metrics: Dict[str, InvestorMetrics] = {}

        # Active jobs tracking
        self._active_jobs: Dict[int, Dict[str, Any]] = {}

        # Time-series data for throughput calculation
        self._job_completions: List[datetime] = []

        # Start time for uptime tracking
        self._start_time = datetime.utcnow()

        logger.info("MetricsCollector initialized")

    def record_job_start(
        self, job_id: int, investor_id: int, investor_type: str, investor_name: str = ""
    ) -> None:
        """Record the start of a collection job."""
        with self._metrics_lock:
            self._job_metrics.total_jobs += 1
            self._job_metrics.pending_jobs += 1

            self._active_jobs[job_id] = {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "investor_name": investor_name,
                "started_at": time.time(),
                "strategies": [],
            }

            # Initialize investor metrics if needed
            investor_key = f"{investor_type}:{investor_id}"
            if investor_key not in self._investor_metrics:
                self._investor_metrics[investor_key] = InvestorMetrics(
                    investor_id=investor_id,
                    investor_type=investor_type,
                    investor_name=investor_name,
                )

            logger.debug(f"Job {job_id} started for {investor_name}")

    def record_job_running(self, job_id: int) -> None:
        """Record that a job has started running."""
        with self._metrics_lock:
            if self._job_metrics.pending_jobs > 0:
                self._job_metrics.pending_jobs -= 1
            self._job_metrics.running_jobs += 1

    def record_job_complete(
        self,
        job_id: int,
        success: bool,
        companies_found: int = 0,
        tokens_used: int = 0,
        cost_usd: float = 0.0,
        strategies_used: Optional[List[str]] = None,
    ) -> None:
        """Record the completion of a collection job."""
        with self._metrics_lock:
            now = datetime.utcnow()

            # Update job counts
            if self._job_metrics.running_jobs > 0:
                self._job_metrics.running_jobs -= 1

            if success:
                self._job_metrics.successful_jobs += 1
            else:
                self._job_metrics.failed_jobs += 1

            # Calculate duration
            duration = 0.0
            if job_id in self._active_jobs:
                job_info = self._active_jobs[job_id]
                duration = time.time() - job_info["started_at"]
                self._job_metrics.total_duration_seconds += duration

                # Update investor metrics
                investor_key = f"{job_info['investor_type']}:{job_info['investor_id']}"
                if investor_key in self._investor_metrics:
                    inv_metrics = self._investor_metrics[investor_key]
                    inv_metrics.jobs_run += 1
                    inv_metrics.last_collection = now
                    inv_metrics.total_companies += companies_found
                    inv_metrics.total_cost_usd += cost_usd
                    if strategies_used:
                        inv_metrics.strategies_used.extend(strategies_used)

                del self._active_jobs[job_id]

            # Update totals
            self._job_metrics.total_companies_found += companies_found
            self._job_metrics.total_tokens_used += tokens_used
            self._job_metrics.total_cost_usd += cost_usd

            # Record completion time for throughput calculation
            self._job_completions.append(now)
            self._cleanup_old_completions()

            logger.debug(
                f"Job {job_id} completed: success={success}, "
                f"companies={companies_found}, duration={duration:.2f}s"
            )

    def record_strategy_execution(
        self,
        strategy_name: str,
        success: bool,
        duration_seconds: float = 0.0,
        companies_found: int = 0,
        requests_made: int = 0,
        tokens_used: int = 0,
        cost_usd: float = 0.0,
    ) -> None:
        """Record a strategy execution."""
        with self._metrics_lock:
            if strategy_name not in self._strategy_metrics:
                self._strategy_metrics[strategy_name] = StrategyMetrics(
                    name=strategy_name
                )

            metrics = self._strategy_metrics[strategy_name]
            metrics.executions += 1
            metrics.total_duration_seconds += duration_seconds
            metrics.total_requests_made += requests_made
            metrics.total_tokens_used += tokens_used
            metrics.total_cost_usd += cost_usd
            metrics.last_execution = datetime.utcnow()

            if success:
                metrics.successes += 1
                metrics.total_companies_found += companies_found
            else:
                metrics.failures += 1

            logger.debug(
                f"Strategy {strategy_name}: success={success}, "
                f"companies={companies_found}, duration={duration_seconds:.2f}s"
            )

    def _cleanup_old_completions(self) -> None:
        """Remove job completions older than 24 hours."""
        cutoff = datetime.utcnow() - timedelta(hours=24)
        self._job_completions = [dt for dt in self._job_completions if dt > cutoff]

    def _calculate_throughput(self) -> None:
        """Calculate jobs completed in last hour and 24 hours."""
        now = datetime.utcnow()
        hour_ago = now - timedelta(hours=1)
        day_ago = now - timedelta(hours=24)

        self._job_metrics.jobs_last_hour = sum(
            1 for dt in self._job_completions if dt > hour_ago
        )
        self._job_metrics.jobs_last_24h = len(self._job_completions)

    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary for API response."""
        with self._metrics_lock:
            self._calculate_throughput()

            uptime = datetime.utcnow() - self._start_time

            return {
                "uptime_seconds": int(uptime.total_seconds()),
                "collected_at": datetime.utcnow().isoformat(),
                "jobs": self._job_metrics.to_dict(),
                "strategies": {
                    name: metrics.to_dict()
                    for name, metrics in self._strategy_metrics.items()
                },
                "active_jobs": len(self._active_jobs),
                "summary": {
                    "total_companies_found": self._job_metrics.total_companies_found,
                    "total_tokens_used": self._job_metrics.total_tokens_used,
                    "total_cost_usd": round(self._job_metrics.total_cost_usd, 4),
                    "avg_cost_per_job": round(
                        self._job_metrics.total_cost_usd
                        / max(1, self._job_metrics.successful_jobs),
                        4,
                    ),
                    "investors_processed": len(self._investor_metrics),
                },
            }

    def get_strategy_metrics(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific strategy."""
        with self._metrics_lock:
            if strategy_name in self._strategy_metrics:
                return self._strategy_metrics[strategy_name].to_dict()
            return None

    def get_investor_metrics(
        self, investor_id: int, investor_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific investor."""
        with self._metrics_lock:
            investor_key = f"{investor_type}:{investor_id}"
            if investor_key in self._investor_metrics:
                return self._investor_metrics[investor_key].to_dict()
            return None

    def get_top_investors_by_cost(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get investors with highest collection costs."""
        with self._metrics_lock:
            sorted_investors = sorted(
                self._investor_metrics.values(),
                key=lambda x: x.total_cost_usd,
                reverse=True,
            )
            return [inv.to_dict() for inv in sorted_investors[:limit]]

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        with self._metrics_lock:
            self._strategy_metrics.clear()
            self._job_metrics = JobMetrics()
            self._investor_metrics.clear()
            self._active_jobs.clear()
            self._job_completions.clear()
            self._start_time = datetime.utcnow()
            logger.info("Metrics reset")


# Global instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create the global MetricsCollector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


class MetricsContext:
    """
    Context manager for tracking strategy execution metrics.

    Usage:
        with MetricsContext("website_scraping") as ctx:
            # Execute strategy
            companies = await strategy.execute(context)
            ctx.companies_found = len(companies)
            ctx.requests_made = 5
    """

    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.start_time: float = 0.0
        self.success: bool = True
        self.companies_found: int = 0
        self.requests_made: int = 0
        self.tokens_used: int = 0
        self.cost_usd: float = 0.0

    def __enter__(self) -> "MetricsContext":
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.time() - self.start_time
        self.success = exc_type is None

        get_metrics_collector().record_strategy_execution(
            strategy_name=self.strategy_name,
            success=self.success,
            duration_seconds=duration,
            companies_found=self.companies_found,
            requests_made=self.requests_made,
            tokens_used=self.tokens_used,
            cost_usd=self.cost_usd,
        )


async def with_metrics(strategy_name: str):
    """
    Async context manager for tracking strategy execution metrics.

    Usage:
        async with with_metrics("website_scraping") as ctx:
            companies = await strategy.execute(context)
            ctx.companies_found = len(companies)
    """
    return MetricsContext(strategy_name)
