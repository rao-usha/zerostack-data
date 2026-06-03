"""
SPEC_075 — focused multi-year county-grain CBP ingest.
SPEC_100 — extended to loop NAICS-2 sectors so the table holds NAICS
detail rows (not just the "00" total). Powers the CBP-backed Decision
Map competition lookup.

Same shape as SPEC_070's county_acs.py: hit the Census API per year +
NAICS request, UPSERT into a grain-explicit table.

Census endpoint per year:
    GET https://api.census.gov/data/{year}/cbp
        ?get=NAME,ESTAB,EMP,PAYANN,NAICS2017
        &for=county:*
        &NAICS2017={code}
        &key={CENSUS_API_KEY}

NAICS request modes:
- "00"       → one row per county (total across all sectors). Original
               behaviour; what powers the Establishments choropleth.
- "11", "21" → one row per (county × naics) for that 2-digit sector
               plus its 4/6-digit children. CBP's NAICS hierarchy
               flattens in the response — a single sector call returns
               its 2-digit row + every deeper row that rolls up to it.
- "*"        → all NAICS rows for the year. The Census API supports
               this for some years; the multi-sector loop is more
               reliable.

Idempotent: CREATE IF NOT EXISTS + UPSERT on (year, geo_id, naics_code).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

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
    grain-explicit table. Returns a summary dict.

    SPEC_100 — the request `naics_code` is still passed to the API, but
    the ACTUAL NAICS stored per row is read from the response's
    `NAICS2017` column. That matters because some sector requests
    (e.g. `NAICS2017=44`) actually return both the 2-digit row AND the
    underlying 4/6-digit rows in a single call.
    """
    started = datetime.utcnow()
    settings = get_settings()
    api_key = settings.require_census_api_key()

    # 1. Ensure table exists (no-op after first year)
    for stmt in _create_table_sql().strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()

    # 2. Fetch — include NAICS2017 in the response so we can store the
    # actual code per row, not just the requested one.
    # The Census CBP API requires `in=state:*` for `for=county:*` when
    # NAICS is anything other than the special "00" total — without it
    # the API returns 204 No Content. Verified live against the 2022
    # CBP endpoint on 2026-06-03.
    url = CENSUS_CBP_URL_TEMPLATE.format(year=year)
    params: Dict[str, str] = {
        "get": f"NAME,ESTAB,EMP,PAYANN,{NAICS_VINTAGE}",
        "for": "county:*",
        NAICS_VINTAGE: naics_code,
        "key": api_key,
    }
    if naics_code != "00":
        params["in"] = "state:*"
    logger.info("Census CBP fetch: year=%s naics=%s", year, naics_code)
    try:
        # follow_redirects=True: Census 302s when the params are slightly
        # wrong (e.g. missing in=state:*) and drops the key in the
        # redirect → "Missing Key" HTML page. Following redirects makes
        # that failure mode visible rather than silently 204.
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as cli:
            resp = cli.get(url, params=params)
            # 204 No Content = valid request but no data for this
            # NAICS in this vintage (common for sector codes that don't
            # exist standalone — see 44 vs 44-45 below). Soft-return.
            if resp.status_code == 204:
                return {"year": year, "naics_code": naics_code,
                        "rows_inserted": 0, "api_results": 0,
                        "note": "204 no content",
                        "duration_seconds": (datetime.utcnow() - started).total_seconds()}
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as exc:
        logger.exception("Census CBP request failed for year %s", year)
        return {"year": year, "naics_code": naics_code, "error": str(exc),
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    if not data or len(data) < 2:
        return {"year": year, "naics_code": naics_code, "rows_inserted": 0,
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}

    headers = data[0]
    # Headers vary slightly year to year — look up indices defensively
    try:
        i_estab = headers.index("ESTAB"); i_emp = headers.index("EMP")
        i_pay = headers.index("PAYANN"); i_state = headers.index("state")
        i_county = headers.index("county")
    except ValueError:
        return {"year": year, "naics_code": naics_code,
                "error": f"unexpected headers: {headers!r}",
                "duration_seconds": (datetime.utcnow() - started).total_seconds()}
    # NAICS2017 in the response is optional — older years sometimes
    # echo the request param only. If missing, fall back to the request.
    try:
        i_naics = headers.index(NAICS_VINTAGE)
    except ValueError:
        i_naics = None

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
        # Prefer the per-row NAICS from the API response; fall back to
        # the request param if the column wasn't returned.
        actual_naics = (
            (row[i_naics] or naics_code).strip()
            if i_naics is not None else naics_code
        )
        batch.append({
            "year": year, "geo_id": geo_id, "naics": actual_naics,
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
        "SELECT COUNT(*) FROM census_cbp_county_yearly WHERE year = :y"
    ), {"y": year}).scalar()

    return {
        "year": year, "naics_code": naics_code,
        "api_results": len(data) - 1,
        "rows_inserted": inserted,
        "rows_in_table_year": int(final or 0),
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


# NAICS-2 sectors that CBP reports semantically. CBP uses combined
# codes for three multi-code sectors — "31-33" (manufacturing),
# "44-45" (retail trade), "48-49" (transportation & warehousing).
# This list is for documentation + UI labels only.
NAICS_2_SECTORS: List[str] = [
    "11",     # Agriculture, forestry, fishing & hunting
    "21",     # Mining, quarrying, oil & gas extraction
    "22",     # Utilities
    "23",     # Construction
    "31-33",  # Manufacturing
    "42",     # Wholesale trade
    "44-45",  # Retail trade
    "48-49",  # Transportation & warehousing
    "51",     # Information
    "52",     # Finance & insurance
    "53",     # Real estate & rental
    "54",     # Professional, scientific, technical services
    "55",     # Management of companies
    "56",     # Admin & support, waste mgmt
    "61",     # Educational services
    "62",     # Health care & social assistance
    "71",     # Arts, entertainment, recreation
    "72",     # Accommodation & food services
    "81",     # Other services
    "92",     # Public administration
]

# Operational prefix list used by the all-NAICS backfill. Census's
# `NAICS2017={prefix}*` wildcard returns all underlying 3/4/5/6-digit
# detail rows for that prefix, BUT only for SINGLE-CODE sectors. The
# three combined sectors must be split:
#   - 44-45 → "44*" + "45*"   (retail trade)
#   - 31-33 → "31*" + "32*" + "33*" (manufacturing)
#   - 48-49 → "48*" + "49*"   (transportation & warehousing)
# A single-code wildcard like "44*" includes the "44-45" combined row
# itself plus 441, 4411, etc. Verified 2026-06-03 against the live
# 2022 CBP endpoint.
NAICS_2_BACKFILL_PREFIXES: List[str] = [
    "11", "21", "22", "23",
    "31", "32", "33",
    "42",
    "44", "45",
    "48", "49",
    "51", "52", "53", "54", "55", "56",
    "61", "62", "71", "72", "81", "92",
]


def ingest_county_cbp_all_naics(db: Session, year: int,
                                 prefixes: Optional[List[str]] = None,
                                 sleep_seconds: float = 1.0,
                                 ) -> Dict[str, Any]:
    """SPEC_100 — backfill all NAICS levels for one year.

    Loops `prefixes` (default: every NAICS-2 backfill prefix) and
    calls `ingest_county_cbp_year` with `NAICS2017={prefix}*` so the
    response includes the 2/3/4/5/6-digit detail under that prefix.
    The actual per-row NAICS comes from the API response.

    Always re-ingests NAICS='00' first so the existing
    Establishments choropleth keeps working without a gap.
    """
    started = datetime.utcnow()
    prefixes = prefixes or NAICS_2_BACKFILL_PREFIXES
    per_request: List[Dict[str, Any]] = []
    total_inserted = 0
    # NAICS='00' first (the all-sectors totals) — keeps the existing
    # Establishments layer consistent across the backfill.
    requests = ["00"] + [f"{p}*" for p in prefixes]
    for naics_request in requests:
        summary = ingest_county_cbp_year(db, year, naics_code=naics_request)
        per_request.append(summary)
        total_inserted += int(summary.get("rows_inserted", 0))
        # Gentle pacing so the Census API doesn't 429 us.
        if sleep_seconds > 0 and naics_request != requests[-1]:
            time.sleep(sleep_seconds)
    return {
        "year": year,
        "requests": requests,
        "rows_inserted": total_inserted,
        "per_request": per_request,
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
