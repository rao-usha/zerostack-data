"""
SPEC_075 — focused multi-year county-grain CBP ingest.

Same shape as SPEC_070's county_acs.py: hit the Census API once per
year for total-NAICS county data, UPSERT into a grain-explicit table.
PLAN_067 SPEC_073-renumbered.

Census endpoint per year:
    GET https://api.census.gov/data/{year}/cbp
        ?get=NAME,ESTAB,EMP,PAYANN
        &for=county:*
        &NAICS2017=00
        &key={CENSUS_API_KEY}

Returns one row per US county (~3,143). NAICS=00 = "Total for all sectors."
Per-NAICS-2 slicing is a follow-on; this MVP ships totals only.

Idempotent: CREATE IF NOT EXISTS + UPSERT on (year, geo_id, naics_code).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings

logger = logging.getLogger(__name__)


CENSUS_CBP_URL_TEMPLATE = "https://api.census.gov/data/{year}/cbp"
NAICS_VINTAGE = "NAICS2017"   # 2017-2022 all use NAICS2017 vintage


def _create_table_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS census_cbp_county_yearly (
        year       INTEGER NOT NULL,
        geo_id     TEXT NOT NULL,
        naics_code TEXT NOT NULL DEFAULT '00',
        establishments         INTEGER,
        employees              INTEGER,
        annual_payroll_thousands BIGINT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (year, geo_id, naics_code)
    );
    CREATE INDEX IF NOT EXISTS idx_census_cbp_county_yearly_year
        ON census_cbp_county_yearly(year);
    """


def _parse_int(raw) -> int | None:
    try:
        v = int(raw) if raw not in (None, "", "null", "N", "D", "S") else None
        return None if v is not None and v < 0 else v
    except (ValueError, TypeError):
        return None


def ingest_county_cbp_year(db: Session, year: int, naics_code: str = "00",
                           timeout_seconds: float = 60.0) -> Dict[str, Any]:
    """Single-year ingest: one Census CBP API call → UPSERT into the
    grain-explicit table. Returns a summary dict."""
    started = datetime.utcnow()
    settings = get_settings()
    api_key = settings.require_census_api_key()

    # 1. Ensure table exists (no-op after first year)
    for stmt in _create_table_sql().strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    # 2. Fetch
    url = CENSUS_CBP_URL_TEMPLATE.format(year=year)
    params = {
        "get": "NAME,ESTAB,EMP,PAYANN",
        "for": "county:*",
        NAICS_VINTAGE: naics_code,
        "key": api_key,
    }
    logger.info("Census CBP fetch: year=%s naics=%s", year, naics_code)
    try:
        with httpx.Client(timeout=timeout_seconds) as cli:
            resp = cli.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as exc:
        logger.exception("Census CBP request failed for year %s", year)
        return {"year": year, "error": str(exc),
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    if not data or len(data) < 2:
        return {"year": year, "rows_inserted": 0,
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    headers = data[0]
    # Headers vary slightly year to year — look up indices defensively
    try:
        i_estab = headers.index("ESTAB"); i_emp = headers.index("EMP")
        i_pay = headers.index("PAYANN"); i_state = headers.index("state")
        i_county = headers.index("county")
    except ValueError as exc:
        return {"year": year, "error": f"unexpected headers: {headers!r}",
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    # 3. UPSERT
    upsert_sql = text("""
        INSERT INTO census_cbp_county_yearly
            (year, geo_id, naics_code, establishments, employees, annual_payroll_thousands)
        VALUES (:year, :geo_id, :naics, :est, :emp, :pay)
        ON CONFLICT (year, geo_id, naics_code) DO UPDATE
        SET establishments = EXCLUDED.establishments,
            employees = EXCLUDED.employees,
            annual_payroll_thousands = EXCLUDED.annual_payroll_thousands,
            ingested_at = NOW()
    """)

    inserted = 0
    batch: List[Dict[str, Any]] = []
    for row in data[1:]:
        state = (row[i_state] or "").zfill(2)
        county = (row[i_county] or "").zfill(3)
        if not state or not county:
            continue
        geo_id = state + county
        if len(geo_id) != 5 or not geo_id.isdigit():
            continue
        batch.append({
            "year": year, "geo_id": geo_id, "naics": naics_code,
            "est": _parse_int(row[i_estab]),
            "emp": _parse_int(row[i_emp]),
            "pay": _parse_int(row[i_pay]),
        })
        if len(batch) >= 500:
            db.execute(upsert_sql, batch); db.commit()
            inserted += len(batch); batch = []
    if batch:
        db.execute(upsert_sql, batch); db.commit()
        inserted += len(batch)

    final = db.execute(text(
        "SELECT COUNT(*) FROM census_cbp_county_yearly WHERE year = :y AND naics_code = :n"
    ), {"y": year, "n": naics_code}).scalar()

    return {
        "year": year, "naics_code": naics_code,
        "api_results": len(data) - 1,
        "rows_inserted": inserted,
        "rows_in_table_year": int(final or 0),
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


def ingest_county_cbp(db: Session, years: List[int],
                      naics_code: str = "00") -> Dict[str, Any]:
    """Multi-year wrapper. Calls ingest_county_cbp_year per year, returns
    a summary dict with one entry per year."""
    summaries = []
    for yr in years:
        summaries.append(ingest_county_cbp_year(db, yr, naics_code=naics_code))
    return {
        "years": years,
        "naics_code": naics_code,
        "per_year": summaries,
        "total_inserted": sum(s.get("rows_inserted", 0) for s in summaries),
    }
