"""
Database connection and session management.
"""

from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from app.core.config import get_settings
from app.core.models import Base

# Import SEC models so they're registered with SQLAlchemy
from app.sources.sec import models as sec_models

# Import PE models for PE Intelligence Platform
from app.core import pe_models

# Import People & Org Chart models
from app.core import people_models

# Import Site Intelligence Platform models
from app.core import models_site_intel

# Import Job Queue model for distributed workers
from app.core import models_queue
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton engine & session factory — created once, reused everywhere
# ---------------------------------------------------------------------------
_engine = None
_SessionLocal = None


def get_engine():
    """
    Get the shared database engine (singleton).

    Uses connection pooling for efficiency. The engine is created once
    and reused for the lifetime of the process.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Verify connections before using
            echo=False,  # Set to True for SQL debugging
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
