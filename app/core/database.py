"""
Database connection and session management.
"""

import os
import socket
from datetime import datetime
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from app.core.config import get_settings
from app.core.models import Base

# Import SEC models so they're registered with SQLAlchemy

# Import PE models for PE Intelligence Platform

# Import People & Org Chart models

# Import Site Intelligence Platform models

# Import Macro Causal Graph models
import app.core.macro_models  # noqa: F401 — registers tables with Base.metadata

# Import Eval Builder models
import app.core.eval_models  # noqa: F401 — registers eval_suites/cases/runs/results with Base.metadata

# Import PE models (required so FKs from probability_models resolve correctly)
import app.core.pe_models  # noqa: F401 — registers pe_portfolio_companies etc. with Base.metadata

# Import Deal Probability Engine models (PLAN_059 Phase 1)
import app.core.probability_models  # noqa: F401 — registers txn_prob_* tables with Base.metadata

# Import Economic Data Quality models
from app.core.models import EconDataRevision  # noqa: F401 — registers econ_data_revisions with Base.metadata

# Import data watchdog alert state (SPEC_128)
import app.core.models_watchdog  # noqa: F401 — registers watchdog_alerts with Base.metadata

# Import Job Queue model for distributed workers
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton engine & session factory — created once, reused everywhere
# ---------------------------------------------------------------------------
_engine = None
_SessionLocal = None

# pg_stat_activity.application_name for this process unless DB_APPLICATION_NAME
# overrides it (SPEC_160). The worker entry point sets "nexdata-worker".
_process_role = "nexdata-api"

# Who this process is and when it started (SPEC_160 review): connections are
# named "<role>@<host>:<pid>" so the scheduler leader can tell whether another
# API process is alive (and may own in-process RUNNING jobs) before it fails
# stale ones, and only jobs started before this process booted can be ours-dead.
PROCESS_INSTANCE = f"{socket.gethostname()}:{os.getpid()}"
PROCESS_STARTED_AT = datetime.utcnow()


def set_process_role(name: str) -> None:
    """Name this process's connections; call before the engine is created."""
    global _process_role
    _process_role = name


def process_role_name() -> str:
    """The role part of application_name (``nexdata-api`` unless overridden)."""
    try:
        override = get_settings().db_application_name
    except Exception:
        override = None
    return override or _process_role


def process_application_name() -> str:
    """``<role>@<host>:<pid>`` (Postgres keeps at most 63 characters)."""
    return f"{process_role_name()}@{PROCESS_INSTANCE}"[:63]


def end_read_transaction(db: Session) -> None:
    """End the session's open (read) transaction but keep loaded attributes.

    SPEC_160: committing expires every ORM object, so the next attribute read
    re-SELECTs the row and opens a transaction that then sits "idle in
    transaction" through whatever the caller awaits next. Committing with
    expire_on_commit off returns the connection to the pool and leaves the
    objects' attributes readable without SQL.
    """
    previous = db.expire_on_commit
    db.expire_on_commit = False
    try:
        db.commit()
    finally:
        db.expire_on_commit = previous


def get_engine():
    """
    Get the shared database engine (singleton).

    Uses connection pooling for efficiency. The engine is created once
    and reused for the lifetime of the process. Pool size, overflow and
    recycle come from DB_POOL_SIZE / DB_MAX_OVERFLOW / DB_POOL_RECYCLE.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        kwargs = {}
        if settings.database_url.startswith("postgresql"):
            kwargs["connect_args"] = {
                "application_name": process_application_name(),
            }
        _engine = create_engine(
            settings.database_url,
            poolclass=QueuePool,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=True,  # Verify connections before using
            echo=False,  # Set to True for SQL debugging
            **kwargs,
        )
    return _engine


def create_tables(engine=None):
    """
    Create all core tables if they don't exist.

    Idempotent - safe to call multiple times.
    """
    if engine is None:
        engine = get_engine()

    logger.info("Creating core tables if they don't exist...")
    Base.metadata.create_all(bind=engine)
    logger.info("Core tables ready")

    # Run schema migrations for new columns (idempotent)
    _apply_schema_migrations(engine)


def _apply_schema_migrations(engine) -> None:
    """
    Apply incremental schema changes that can't be handled by create_all().
    Each statement is idempotent — safe to run on every startup.
    """
    migrations = [
        "ALTER TABLE lp_fund ADD COLUMN IF NOT EXISTS lp_tier INTEGER",
        "ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS data_origin VARCHAR(16) NOT NULL DEFAULT 'real'",
        "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS role_type VARCHAR(50)",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                # Surface failures loudly — a silently-swallowed permission
                # error previously left the schema out of sync with models.
                logger.error(f"Migration FAILED: {sql} -- {e}")


def get_session_factory():
    """Get the shared session factory (singleton)."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=get_engine()
        )
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI routes to get a database session.

    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def execute_raw_sql(sql: str, params: dict = None) -> None:
    """
    Execute raw SQL with parameterization.

    SAFETY: Always use parameterized queries. Never concatenate untrusted input.

    Args:
        sql: SQL query with :param style placeholders
        params: Dictionary of parameter values
    """
    with get_engine().connect() as conn:
        if params:
            conn.execute(text(sql), params)
        else:
            conn.execute(text(sql))
        conn.commit()
