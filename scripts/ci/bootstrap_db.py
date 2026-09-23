"""
Bring an EMPTY database to Alembic head, the way a fresh deployment does (SPEC_129).

The baseline migration (3fb893199e22) is intentionally empty: it assumes the
tables already exist from ``create_all()``. So ``alembic upgrade head`` alone
cannot run against a blank database -- 0001 alters ``pe_firm_people``, which
nothing has created yet. Production gets away with it because startup swallows
the migration error, runs ``create_tables()``, and the next start migrates.
CI must not rely on that retry, so this script runs the two steps in the order
that actually works on a blank database:

    1. create_tables()          (Base.metadata.create_all + idempotent ALTERs)
    2. alembic upgrade head     (every migration must apply on top of it)

Any migration error fails CI. Usage:

    DATABASE_URL=postgresql://... python scripts/ci/bootstrap_db.py
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))


def main() -> int:
    from alembic import command
    from alembic.config import Config

    # Register every model module, exactly as alembic/env.py does.
    import app.core.models  # noqa: F401
    import app.core.models_site_intel  # noqa: F401
    import app.core.people_models  # noqa: F401
    import app.core.pe_models  # noqa: F401
    import app.core.family_office_models  # noqa: F401
    import app.core.models_queue  # noqa: F401
    import app.core.entity_resolver  # noqa: F401
    import app.sources.sec.models  # noqa: F401
    from app.core.database import create_tables, get_engine

    create_tables(get_engine())

    cfg = Config(str(REPO / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO / "alembic"))
    command.upgrade(cfg, "head")
    print("database at alembic head")
    return 0


if __name__ == "__main__":
    sys.exit(main())
