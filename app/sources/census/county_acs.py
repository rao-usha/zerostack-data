"""
SPEC_070 — focused county-grain ACS ingest path.

A small, dedicated entry point alongside `ingest.py` for ingesting an ACS
table directly at county summary level. We don't reuse `ingest_acs_table`
here because its `generate_table_name(survey, year, table_id)` is
geo-level-agnostic — adding county-grain data via that path would collide
with the legacy ZCTA `acs5_2023_b19013` table.

Instead this module writes to a grain-explicit table namespace:
    acs5_county_{year}_{table_id_lower}

Currently scoped to B19013 (median household income). Future county
variables (B25077 home value, B23025 employment) can follow the same
pattern by parameter — the schema is generic.

The ingester is idempotent: it CREATEs IF NOT EXISTS, then UPSERTs on
`geo_id`. Safe to re-run.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.sources.census.client import CensusClient

logger = logging.getLogger(__name__)


def county_acs_table_name(year: int, table_id: str) -> str:
    """The grain-explicit table name pattern reserved for county-grain ACS."""
    return f"acs5_county_{year}_{table_id.lower()}"


def _create_table_sql(table_name: str, var_column: str) -> str:
    """Schema is identical for every county-grain ACS scalar variable."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        geo_id      TEXT PRIMARY KEY,
        state_fips  TEXT NOT NULL,
        geo_name    TEXT,
        {var_column} INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(state_fips);
    """


async def ingest_county_acs_variable(
    db: Session,
    year: int = 2023,
    table_id: str = "B19013",
    variable: str = "B19013_001E",
) -> Dict[str, Any]:
    """
    Fetch one ACS scalar variable at county grain in a single API call,
    UPSERT into the grain-explicit table, return a summary dict.

    Census endpoint:
        GET https://api.census.gov/data/{year}/acs/acs5
            ?get=NAME,{variable}&for=county:*&key=...

    Returns:
        {table_name, rows_inserted, rows_in_table, duration_seconds}
    """
    started = datetime.utcnow()
    settings = get_settings()
    api_key = settings.require_census_api_key()

    table_name = county_acs_table_name(year, table_id)
    var_column = variable.lower()

    # 1. Ensure table exists
    for stmt in _create_table_sql(table_name, var_column).strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    # 2. Fetch all counties in one call
    client = CensusClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )
    try:
        logger.info("Fetching ACS county data: %s %s %s", year, table_id, variable)
        rows: List[Dict[str, Any]] = await client.fetch_acs_data(
            survey="acs5",
            year=year,
            variables=[variable],
            geo_level="county",
            geo_filter=None,
        )
    finally:
        await client.close()

    if not rows:
        return {"table_name": table_name, "rows_inserted": 0, "rows_in_table": 0,
                "duration_seconds": (datetime.utcnow() - started).total_seconds(),
                "error": "Census API returned no rows"}

    # 3. Normalize: Census returns dicts with keys NAME, <VARIABLE>, state, county
    upsert_sql = text(f"""
        INSERT INTO {table_name} (geo_id, state_fips, geo_name, {var_column})
        VALUES (:geo_id, :state_fips, :geo_name, :val)
        ON CONFLICT (geo_id) DO UPDATE
        SET geo_name = EXCLUDED.geo_name,
            {var_column} = EXCLUDED.{var_column},
            ingested_at = NOW()
    """)

    inserted = 0
    batch_size = 500
    batch: List[Dict[str, Any]] = []
    for r in rows:
        state_fips = r.get("state")
        county_code = r.get("county")
        if not state_fips or not county_code:
            continue
        # Build canonical 5-digit county FIPS
        geo_id = f"{state_fips.zfill(2)}{county_code.zfill(3)}"
        raw = r.get(variable)
        # Census uses sentinel -666666666 for "no data"; treat as null.
        try:
            val = int(raw) if raw not in (None, "", "null") else None
            if val is not None and val < 0:
                val = None
        except (ValueError, TypeError):
            val = None
        batch.append({
            "geo_id": geo_id,
            "state_fips": state_fips.zfill(2),
            "geo_name": r.get("NAME"),
            "val": val,
        })
        if len(batch) >= batch_size:
            db.execute(upsert_sql, batch); db.commit(); inserted += len(batch); batch = []
    if batch:
        db.execute(upsert_sql, batch); db.commit(); inserted += len(batch)

    final = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    return {
        "table_name": table_name,
        "rows_inserted": inserted,
        "rows_in_table": int(final or 0),
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_072 — multi-variable variant for tables that need >1 ACS scalar
# (e.g. B28002 broadband: total households + with-broadband, both required
# to derive the percentage). Kept as a separate function instead of
# refactoring `ingest_county_acs_variable` so the SPEC_070 path stays
# byte-identical.
# ─────────────────────────────────────────────────────────────────────────────

def _create_multi_table_sql(table_name: str, var_columns: List[str]) -> str:
    cols_sql = ",\n        ".join(f"{c} INTEGER" for c in var_columns)
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        geo_id      TEXT PRIMARY KEY,
        state_fips  TEXT NOT NULL,
        geo_name    TEXT,
        {cols_sql},
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(state_fips);
    """


async def ingest_county_acs_multi(
    db: Session,
    year: int = 2023,
    table_id: str = "B28002",
    variables: List[str] = None,
) -> Dict[str, Any]:
    """
    Multi-variable county-grain ACS ingest.

    Same single-API-call pattern as `ingest_county_acs_variable`, but
    fetches N variables in one request and stores each as its own
    integer column (lowercase of the Census variable code). Derived
    metrics (rates, percentages, ratios) are NOT computed at ingest —
    they live in the layer builder so the raw counts are preserved.

    Returns:
        {table_name, variables, rows_inserted, rows_in_table, duration_seconds}
    """
    if not variables:
        raise ValueError("variables must be a non-empty list")
    started = datetime.utcnow()
    settings = get_settings()
    api_key = settings.require_census_api_key()

    table_name = county_acs_table_name(year, table_id)
    var_columns = [v.lower() for v in variables]

    # 1. Ensure table exists
    for stmt in _create_multi_table_sql(table_name, var_columns).strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    # 2. Fetch all counties in one call
    client = CensusClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )
    try:
        logger.info("Fetching multi-variable ACS county data: %s %s %s",
                    year, table_id, variables)
        rows: List[Dict[str, Any]] = await client.fetch_acs_data(
            survey="acs5", year=year, variables=variables,
            geo_level="county", geo_filter=None,
        )
    finally:
        await client.close()

    if not rows:
        return {"table_name": table_name, "variables": variables,
                "rows_inserted": 0, "rows_in_table": 0,
                "duration_seconds": (datetime.utcnow() - started).total_seconds(),
                "error": "Census API returned no rows"}

    # 3. Build dynamic UPSERT
    cols_csv = ", ".join(var_columns)
    placeholders = ", ".join(f":{c}" for c in var_columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in var_columns)
    upsert_sql = text(f"""
        INSERT INTO {table_name} (geo_id, state_fips, geo_name, {cols_csv})
        VALUES (:geo_id, :state_fips, :geo_name, {placeholders})
        ON CONFLICT (geo_id) DO UPDATE
        SET geo_name = EXCLUDED.geo_name,
            {updates},
            ingested_at = NOW()
    """)

    def _parse_int(raw):
        try:
            v = int(raw) if raw not in (None, "", "null") else None
            return None if (v is not None and v < 0) else v
        except (ValueError, TypeError):
            return None

    inserted = 0
    batch_size = 500
    batch: List[Dict[str, Any]] = []
    for r in rows:
        state_fips = r.get("state"); county_code = r.get("county")
        if not state_fips or not county_code:
            continue
        rec: Dict[str, Any] = {
            "geo_id": f"{state_fips.zfill(2)}{county_code.zfill(3)}",
            "state_fips": state_fips.zfill(2),
            "geo_name": r.get("NAME"),
        }
        for var, col in zip(variables, var_columns):
            rec[col] = _parse_int(r.get(var))
        batch.append(rec)
        if len(batch) >= batch_size:
            db.execute(upsert_sql, batch); db.commit(); inserted += len(batch); batch = []
    if batch:
        db.execute(upsert_sql, batch); db.commit(); inserted += len(batch)

    final = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    return {
        "table_name": table_name,
        "variables": variables,
        "rows_inserted": inserted,
        "rows_in_table": int(final or 0),
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }
