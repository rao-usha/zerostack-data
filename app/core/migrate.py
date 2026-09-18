"""
Run Alembic migrations at startup (SPEC_105).

API and worker processes both call ``run_migrations()`` before ``create_tables()``.
A Postgres advisory lock ensures only one process upgrades at a time; the
others wait and then find the schema already at head.
"""

import logging
from pathlib import Path

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Arbitrary constant key for pg_advisory_lock ("nexdata migrations")
MIGRATION_LOCK_KEY = 804_261_117

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_migrations(engine=None) -> bool:
    """Upgrade the database to Alembic head. Returns True on success; never raises."""
    try:
        from alembic import command
        from alembic.config import Config

        from app.core.database import get_engine

        engine = engine or get_engine()
        ini_path = REPO_ROOT / "alembic.ini"
        if not ini_path.exists():
            logger.warning(f"alembic.ini not found at {ini_path}; skipping migrations")
            return False

        cfg = Config(str(ini_path))
        cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
        cfg.attributes["configure_logger"] = False

        with engine.connect() as lock_conn:
            lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": MIGRATION_LOCK_KEY})
            try:
                command.upgrade(cfg, "head")
            finally:
                lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": MIGRATION_LOCK_KEY})
        logger.info("Alembic migrations at head")
        return True
    except Exception as e:
        logger.error(f"Alembic migration failed (continuing startup): {e}", exc_info=True)
        return False
