"""
SPEC_077 A.3 — focused tract-grain ACS multi-variable ingest.

Same shape as SPEC_070's county_acs.py but the Census API requires
state-scoped queries for tract data (`for=tract:*&in=state:XX`), so
this loops per state. ~52 API calls for the full US + DC + territories.

Single canonical table for Phase A demand-surface needs:
    acs5_tract_{year}_demand
with five variables that feed four Atlas layers:
    B19013_001E  → median household income
    B01002_001E  → median age
    B01003_001E  → total population (drives density derivation)
    B25003_001E  → housing units (denom for tenure share)
    B25003_002E  → owner-occupied housing units (numerator)

Idempotent: CREATE IF NOT EXISTS + UPSERT on geo_id.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.sources.census.client import CensusClient


logger = logging.getLogger(__name__)


# Same real-US state-FIPS list as the tiger ingest
US_STATE_FIPS = [
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19",
    "20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35",
    "36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53",
    "54","55","56",
    "72",   # PR — has tracts; territories AS/GU/MP/VI mostly don't
]

# The five variables that power Phase A demand layers
DEMAND_VARIABLES = [
    "B19013_001E",   # median household income
    "B01002_001E",   # median age
    "B01003_001E",   # total population
    "B25003_001E",   # total housing units
    "B25003_002E",   # owner-occupied
]


def tract_acs_table_name(year: int) -> str:
    return f"acs5_tract_{year}_demand"


def _create_table_sql(table_name: str) -> str:
    cols_sql = ",\n        ".join(
        f"{v.lower()} INTEGER" for v in DEMAND_VARIABLES
    )
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        geo_id      TEXT PRIMARY KEY,           -- 11-digit tract FIPS
        state_fips  TEXT NOT NULL,
        county_fips TEXT NOT NULL,
        geo_name    TEXT,
        {cols_sql},
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(state_fips);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_county ON {table_name}(state_fips, county_fips);
    """


def _parse_int(raw) -> Optional[int]:
    try:
        v = int(raw) if raw not in (None, "", "null") else None
        return None if (v is not None and v < 0) else v
    except (ValueError, TypeError):
        return None


async def ingest_tract_demand_state(
    db: Session,
    year: int,
    state_fips: str,
    client: CensusClient,
) -> Dict[str, Any]:
    """One state's tracts in one API call. Returns summary."""
    started = datetime.utcnow()
    table_name = tract_acs_table_name(year)
    var_columns = [v.lower() for v in DEMAND_VARIABLES]

    rows = await client.fetch_acs_data(
        survey="acs5", year=year,
        variables=DEMAND_VARIABLES,
        geo_level="tract",
        geo_filter={"state": state_fips},
    )
    if not rows:
        return {"state": state_fips, "rows_inserted": 0,
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    cols_csv = ", ".join(var_columns)
    placeholders = ", ".join(f":{c}" for c in var_columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in var_columns)
    upsert_sql = text(f"""
        INSERT INTO {table_name}
            (geo_id, state_fips, county_fips, geo_name, {cols_csv})
        VALUES
            (:geo_id, :state_fips, :county_fips, :geo_name, {placeholders})
        ON CONFLICT (geo_id) DO UPDATE
        SET geo_name = EXCLUDED.geo_name,
            {updates},
            ingested_at = NOW()
    """)

    batch: List[Dict[str, Any]] = []
    inserted = 0
    for r in rows:
        st = (r.get("state") or "").zfill(2)
        county = (r.get("county") or "").zfill(3)
        tract = (r.get("tract") or "").zfill(6)
        if not (st and county and tract):
            continue
        geo_id = st + county + tract
        if len(geo_id) != 11 or not geo_id.isdigit():
            continue
        rec = {
            "geo_id": geo_id, "state_fips": st, "county_fips": county,
            "geo_name": r.get("NAME"),
        }
        for var, col in zip(DEMAND_VARIABLES, var_columns):
            rec[col] = _parse_int(r.get(var))
        batch.append(rec)
        if len(batch) >= 500:
            db.execute(upsert_sql, batch); db.commit()
            inserted += len(batch); batch = []
    if batch:
        db.execute(upsert_sql, batch); db.commit()
        inserted += len(batch)

    return {
        "state": state_fips,
        "rows_inserted": inserted,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


async def ingest_tract_demand_all(
    db: Session,
    year: int = 2023,
    state_fips_list: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Multi-state wrapper. Creates the table, loops per state."""
    started = datetime.utcnow()
    state_fips_list = state_fips_list or US_STATE_FIPS
    settings = get_settings()
    api_key = settings.require_census_api_key()

    table_name = tract_acs_table_name(year)
    for stmt in _create_table_sql(table_name).strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    client = CensusClient(
        api_key=api_key, max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )
    try:
        summaries = []
        for st in state_fips_list:
            try:
                s = await ingest_tract_demand_state(db, year, st, client)
                summaries.append(s)
                logger.info("State %s tract ACS: %d rows (%.1fs)",
                            st, s["rows_inserted"], s["duration_seconds"])
            except Exception as exc:  # noqa: BLE001
                logger.exception("State %s tract ACS failed: %s", st, exc)
                summaries.append({"state": st, "error": str(exc)})
    finally:
        await client.close()

    final = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    return {
        "table_name": table_name,
        "year": year,
        "rows_in_table": int(final or 0),
        "per_state": summaries,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }
