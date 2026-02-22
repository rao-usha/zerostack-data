"""
Alembic environment configuration for Nexdata.

Imports all model modules so their tables are registered with Base.metadata,
then uses DATABASE_URL from app config (single source of truth).
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.core.config import get_settings
from app.core.models import Base

# Import all model modules so their tables are registered with Base.metadata
import app.core.models  # noqa: F401 — core tables (IngestionJob, DatasetRegistry, etc.)
import app.core.models_site_intel  # noqa: F401 — Site Intelligence domain tables
import app.core.people_models  # noqa: F401 — People and org chart tables
import app.core.pe_models  # noqa: F401 — PE Intelligence tables
import app.core.family_office_models  # noqa: F401 — Family Office tables
import app.core.models_queue  # noqa: F401 — Job queue tables
import app.core.entity_resolver  # noqa: F401 — Entity resolution tables
import app.sources.sec.models  # noqa: F401 — SEC-specific tables

# Alembic Config object (provides access to alembic.ini values)
config = context.config

# Set up Python logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for autogenerate
target_metadata = Base.metadata

# Tables managed by Alembic (defined in model files above).
# Dynamically-created data tables (acs5_*, fred_*, bls_*, etc.) are NOT in
# Base.metadata and should be ignored by autogenerate.
_model_table_names = set(target_metadata.tables.keys())


def include_name(name, type_, parent_names):
    """Filter for autogenerate: only track tables defined in our models."""
    if type_ == "table":
        return name in _model_table_names
    # Always include indexes, constraints, etc. for tracked tables
    return True


def get_url() -> str:
    """Read DATABASE_URL from app settings (same source as the running app)."""
    settings = get_settings()
    return settings.database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — generates SQL without a live DB connection."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_name=include_name,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode — connects to the database and applies changes."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
