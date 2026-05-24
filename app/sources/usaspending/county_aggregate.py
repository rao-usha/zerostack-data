"""
SPEC_071 — focused county-aggregate USAspending ingest.

Sits alongside the existing `app/sources/usaspending/{client,ingest,metadata}.py`
trio without modifying it. The existing path hits
`/api/v2/search/spending_by_award/` per-award (and stores rows with no
county FIPS + null dates); SPEC_071 instead hits
`/api/v2/search/spending_by_geography/` which **already aggregates
server-side to county for a given filter window in one request**,
returning ~3,000 county rows per (fiscal_year, award_type_group).

Volume on `db-f1-micro` is a non-issue with this endpoint — one row per
(geo_id, fiscal_year, award_type_group). The PLAN_067 §3 "aggregate at
ingest if too heavy" note reduces to "use the aggregate endpoint".

Idempotent: CREATE IF NOT EXISTS + UPSERT on the composite PK so
re-runs and additional fiscal years just merge in.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


USASPENDING_GEOGRAPHY_URL = (
    "https://api.usaspending.gov/api/v2/search/spending_by_geography/"
)

# Award-type-code groups per USAspending docs.
AWARD_TYPE_GROUPS: Dict[str, List[str]] = {
    "contracts": ["A", "B", "C", "D"],
    "grants":    ["02", "03", "04", "05"],
    "loans":     ["07", "08"],
    "direct_payments": ["06", "10"],
    "other":     ["09", "11"],
}


def _fiscal_year_window(fiscal_year: int) -> Dict[str, str]:
    """USAspending fiscal year runs Oct 1 (prior calendar) → Sep 30."""
    return {
        "start_date": f"{fiscal_year - 1}-10-01",
        "end_date":   f"{fiscal_year}-09-30",
    }


def _create_table_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS usaspending_county_fy_totals (
        geo_id            TEXT NOT NULL,
        fiscal_year       INTEGER NOT NULL,
        award_type_group  TEXT NOT NULL,
        total_obligation  NUMERIC NOT NULL,
        award_count       INTEGER,
        ingested_at       TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (geo_id, fiscal_year, award_type_group)
    );
    CREATE INDEX IF NOT EXISTS idx_usaspending_county_fy_totals_year
        ON usaspending_county_fy_totals(fiscal_year);
    """


def ingest_county_fy_totals(
    db: Session,
    fiscal_year: int = 2024,
    award_type_group: str = "contracts",
    timeout_seconds: float = 60.0,
) -> Dict[str, Any]:
    """
    Single POST to USAspending spending_by_geography for one (fiscal_year,
    group) combo. UPSERT each county row into the SPEC_071 table.

    Returns:
        {fiscal_year, award_type_group, api_results, rows_inserted,
         rows_in_table, total_obligation, duration_seconds}
    """
    started = datetime.utcnow()
    if award_type_group not in AWARD_TYPE_GROUPS:
        raise ValueError(
            f"unknown award_type_group {award_type_group!r}; "
            f"expected one of {sorted(AWARD_TYPE_GROUPS)}")

    # 1. Ensure table exists
    for stmt in _create_table_sql().strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    # 2. Hit USAspending — one POST returns all counties for the window
    body = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "filters": {
            "time_period": [_fiscal_year_window(fiscal_year)],
            "award_type_codes": AWARD_TYPE_GROUPS[award_type_group],
        },
        "subawards": False,
    }
    logger.info("USAspending spending_by_geography: FY%d %s",
                fiscal_year, award_type_group)
    try:
        with httpx.Client(timeout=timeout_seconds) as cli:
            resp = cli.post(USASPENDING_GEOGRAPHY_URL, json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as exc:
        logger.exception("USAspending request failed")
        return {"error": str(exc), "fiscal_year": fiscal_year,
                "award_type_group": award_type_group,
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    results = data.get("results") or []

    # 3. UPSERT non-zero county rows
    upsert_sql = text("""
        INSERT INTO usaspending_county_fy_totals
            (geo_id, fiscal_year, award_type_group, total_obligation, award_count)
        VALUES (:geo_id, :fy, :grp, :amt, :n)
        ON CONFLICT (geo_id, fiscal_year, award_type_group) DO UPDATE
        SET total_obligation = EXCLUDED.total_obligation,
            award_count = EXCLUDED.award_count,
            ingested_at = NOW()
    """)

    batch: List[Dict[str, Any]] = []
    inserted = 0
    total = 0.0
    for r in results:
        geo_id = r.get("shape_code")
        amt = r.get("aggregated_amount")
        if not geo_id or len(geo_id) != 5 or not geo_id.isdigit():
            continue
        if amt is None or amt <= 0:
            continue   # spending_by_geography returns 0-rows for some counties
        batch.append({
            "geo_id": geo_id,
            "fy": fiscal_year,
            "grp": award_type_group,
            "amt": float(amt),
            # The geography endpoint doesn't return per-county count;
            # leave null. Stretch: hit search/spending_by_award to enrich.
            "n": None,
        })
        total += float(amt)
        if len(batch) >= 500:
            db.execute(upsert_sql, batch); db.commit(); inserted += len(batch); batch = []
    if batch:
        db.execute(upsert_sql, batch); db.commit(); inserted += len(batch)

    in_table = db.execute(text("""
        SELECT COUNT(*) FROM usaspending_county_fy_totals
        WHERE fiscal_year = :fy AND award_type_group = :grp
    """), {"fy": fiscal_year, "grp": award_type_group}).scalar()

    return {
        "fiscal_year": fiscal_year,
        "award_type_group": award_type_group,
        "api_results": len(results),
        "rows_inserted": inserted,
        "rows_in_table": int(in_table or 0),
        "total_obligation_usd": total,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }
