"""
SQLAlchemy model for the distributed job queue.

Workers claim jobs via SELECT ... FOR UPDATE SKIP LOCKED.
The API process listens for PG NOTIFY events to stream progress via SSE.
"""

import enum
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Text,
    JSON,
    Enum,
    Index,
    Float,
)

from app.core.models import Base


class QueueJobStatus(str, enum.Enum):
    """Status values for queued jobs."""

    PENDING = "pending"
    CLAIMED = "claimed"  # Worker picked it up, not yet running
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class QueueJobType(str, enum.Enum):
    """Known job types routed to executors."""

    SITE_INTEL = "site_intel"
    PEOPLE = "people"
    LP = "lp"
    PE = "pe"
    FO = "fo"
    AGENTIC = "agentic"
    FOOT_TRAFFIC = "foot_traffic"
    INGESTION = "ingestion"


class JobQueue(Base):
    """
    Distributed job queue backed by PostgreSQL.

    Workers claim rows with:
        UPDATE job_queue SET status='claimed', worker_id=:wid
        WHERE id = (
            SELECT id FROM job_queue
            WHERE status='pending'
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
    """

    __tablename__ = "job_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # What kind of work and optional link to domain job table
    job_type = Column(
        Enum(QueueJobType, native_enum=False, length=30,
             values_callable=lambda e: [m.value for m in e]),
        nullable=False,
        index=True,
    )
    job_table_id = Column(
        Integer, nullable=True
    )  # FK to domain-specific job table (optional)

    # Status lifecycle: pending -> claimed -> running -> success/failed
    status = Column(
        Enum(QueueJobStatus, native_enum=False, length=20,
             values_callable=lambda e: [m.value for m in e]),
        nullable=False,
        default=QueueJobStatus.PENDING,
        index=True,
    )

    # Scheduling
    priority = Column(Integer, nullable=False, default=0)  # Higher = picked first

    # Worker assignment
    worker_id = Column(String(100), nullable=True)
    claimed_at = Column(DateTime, nullable=True)
    heartbeat_at = Column(DateTime, nullable=True)

    # Job configuration — everything the executor needs
    payload = Column(JSON, nullable=False, default=dict)

    # Progress tracking (updated by workers via pg_notify)
    progress_pct = Column(Float, nullable=True)  # 0.0 – 100.0
    progress_message = Column(String(500), nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Error info
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        # Fast claim query: only look at pending rows
        Index(
            "ix_job_queue_pending",
            "priority",
            "created_at",
            postgresql_where=(status == QueueJobStatus.PENDING.value),
        ),
        # Find stale claimed/running jobs for recovery
        Index(
            "ix_job_queue_heartbeat",
            "heartbeat_at",
            postgresql_where=(
                status.in_(
                    [
                        QueueJobStatus.CLAIMED.value,
                        QueueJobStatus.RUNNING.value,
                    ]
                )
            ),
        ),
    )

    def __repr__(self):
        return (
            f"<JobQueue id={self.id} type={self.job_type} "
            f"status={self.status} worker={self.worker_id}>"
        )


class JobEvent(Base):
    """
    Persistent log of job lifecycle events.

    Every call to send_job_event() writes a row here, giving a full
    timeline for any job (active or historical).
    """

    __tablename__ = "job_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, nullable=False, index=True)
    event_type = Column(String(50), nullable=False)       # job_started, job_progress, etc.
    message = Column(String(500), nullable=True)           # Human-readable summary
    data = Column(JSON, nullable=True)                     # Full event payload
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<JobEvent id={self.id} job_id={self.job_id} "
            f"type={self.event_type}>"
        )


class NightlyBatch(Base):
    """
    Tracks a nightly batch collection run.

    Each batch enqueues jobs for all configured sources with tier-based
    priority and dependency ordering.
    """

    __tablename__ = "nightly_batch"

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(
        String(20), nullable=False, default="running"
    )  # running, completed, failed, partial_success
    total_jobs = Column(Integer, nullable=False, default=0)
    completed_jobs = Column(Integer, nullable=False, default=0)
    failed_jobs = Column(Integer, nullable=False, default=0)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    config = Column(JSON, nullable=False, default=dict)  # which sources, overrides
    job_ids = Column(JSON, nullable=False, default=list)  # list of ingestion_job IDs

    def __repr__(self):
        return (
            f"<NightlyBatch id={self.id} status={self.status} "
            f"{self.completed_jobs}/{self.total_jobs}>"
        )


class RateLimitBucket(Base):
    """
    Distributed rate limit token bucket backed by PostgreSQL.

    Workers call SELECT ... FOR UPDATE to serialize token acquisition
    across all worker processes, ensuring per-domain rate limits are
    respected globally.
    """

    __tablename__ = "rate_limit_bucket"

    domain = Column(String(100), primary_key=True)  # e.g. "api.stlouisfed.org"
    tokens = Column(Float, nullable=False, default=5.0)
    max_tokens = Column(Float, nullable=False, default=5.0)
    refill_rate = Column(Float, nullable=False, default=1.0)  # tokens per second
    last_refill_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<RateLimitBucket domain={self.domain} "
            f"tokens={self.tokens:.1f}/{self.max_tokens:.1f}>"
        )
