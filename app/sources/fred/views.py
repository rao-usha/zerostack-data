"""
FRED-derived analytical views.

`fred_observations` is a unified view across all per-category FRED tables.
v1 macro_scenarios generator queries it; future PLAN_062 Phase A2 training
data pipeline reads from it.

When new fred_{category} tables are added (e.g., fred_employment, fred_inflation),
update FRED_CATEGORY_TABLES below and the view is rebuilt at startup.
"""

import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# Per-category FRED tables that should be unioned into fred_observations.
# Names match COMMON_SERIES keys in app.sources.fred.client (fred_{category}).
# View is rebuilt at startup against whichever of these tables actually exists,
# so it's safe to list categories that haven't been ingested yet.
FRED_CATEGORY_TABLES = [
    "fred_interest_rates",
    "fred_monetary_aggregates",
    "fred_industrial_production",
    "fred_economic_indicators",     # contains UNRATE, CPIAUCSL, GDP, PCE, RSXFS
    "fred_consumer_sentiment",       # contains UMCSENT
    "fred_commodities",              # contains DCOILWTICO, DHHNGSP
]


def _build_view_sql() -> str:
    """
    Build CREATE VIEW SQL that UNIONs across whatever FRED category tables
    currently exist. Tables that don't exist yet are silently skipped via
    information_schema check at view-build time (NOT at query time — the view
    is rebuilt on each startup, so any newly-added category tables show up).
    """
    # We can't conditionally UNION inside a single CREATE VIEW based on
    # table existence at view-definition time without dynamic SQL.
    # Approach: build the UNION explicitly using the known list; at startup
    # we filter to tables that actually exist before constructing the view.
    return ""  # Built dynamically in create_fred_observations_view below


def create_fred_observations_view(engine: Engine) -> None:
    """
    Create or refresh the fred_observations view by UNION-ing across all
    existing fred_{category} tables. Tables that don't exist yet are skipped.
    Idempotent.
    """
    try:
        with engine.begin() as conn:
            # Discover which category tables actually exist
            existing = []
            for tbl in FRED_CATEGORY_TABLES:
                result = conn.execute(
                    text(
                        "SELECT EXISTS ("
                        "  SELECT 1 FROM information_schema.tables "
                        "  WHERE table_schema = 'public' AND table_name = :tbl"
                        ")"
                    ),
                    {"tbl": tbl},
                ).scalar()
                if result:
                    existing.append(tbl)

            if not existing:
                logger.warning(
                    "No fred_{category} tables exist; fred_observations view "
                    "will not be created"
                )
                return

            # Build UNION ALL across existing tables, with category column
            select_parts = []
            for tbl in existing:
                # Extract category name from table name (fred_interest_rates -> interest_rates)
                category = tbl.replace("fred_", "", 1)
                select_parts.append(
                    f"  SELECT series_id, date, value, realtime_start, "
                    f"realtime_end, ingested_at, '{category}' AS category "
                    f"FROM {tbl}"
                )

            union_sql = "\n  UNION ALL\n".join(select_parts)

            view_sql = f"""
            DROP VIEW IF EXISTS fred_observations;

            CREATE VIEW fred_observations AS
            {union_sql};
            """

            conn.execute(text(view_sql))
            logger.info(
                "fred_observations view created/refreshed across %d category tables: %s",
                len(existing),
                ", ".join(existing),
            )
    except Exception as exc:
        logger.error("Failed to create fred_observations view: %s", exc)
        raise
