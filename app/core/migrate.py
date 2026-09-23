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


# Columns a migration adds to an existing table AND an ORM model maps.
# create_all never adds columns to existing tables, so if the migration did
# not apply, every ORM query of that model fails (UndefinedColumn): the jobs
# API, scheduler, batch, the worker's write-back, the audit log. Startup
# checks them after create_all and refuses to serve without them.
REQUIRED_COLUMNS = (
    ("ingestion_jobs", "dataset_key"),     # 0014_dataset_status (SPEC_124)
    ("collection_audit_log", "actor"),     # 0014_dataset_status (SPEC_124)
)


class SchemaNotMigrated(RuntimeError):
    """An ORM-mapped column that a migration adds is missing."""


def missing_mapped_columns(engine=None) -> list:
    """``table.column`` for each REQUIRED_COLUMNS entry whose table exists
    but lacks the column (PostgreSQL only; [] elsewhere)."""
    from app.core.database import get_engine

    engine = engine or get_engine()
    if engine.dialect.name != "postgresql":
        return []
    missing = []
    with engine.connect() as conn:
        for table, column in REQUIRED_COLUMNS:
            if not conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"),
                                {"t": f"public.{table}"}).scalar():
                continue
            if conn.execute(text(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name = :t AND column_name = :c"
            ), {"t": table, "c": column}).first() is None:
                missing.append(f"{table}.{column}")
    return missing


def verify_mapped_columns(engine=None) -> None:
    """Raise SchemaNotMigrated if a migration-added, ORM-mapped column is missing.

    Called after ``run_migrations()`` + ``create_tables()``: a failed
    migration must stop the process rather than let it serve every
    IngestionJob query into an error. Restarting retries the migration.
    """
    missing = missing_mapped_columns(engine)
    if missing:
        raise SchemaNotMigrated(
            f"database schema is behind the code: missing {', '.join(missing)}. "
            "An Alembic migration did not apply (see the 'Alembic migration failed' "
            "error above; a lock timeout on a busy table is retried on restart). "
            "Refusing to start."
        )


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
