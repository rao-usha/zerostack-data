"""SPEC_102 — live Census CBP fallback for (county, NAICS) cells the
backfill missed.

SPEC_100 bulk-backfilled ~1.1M rows from the Census CBP wildcard
endpoint, but disclosure suppression + wildcard quirks leave some
(county, NAICS) tuples missing. This module fetches one cell at a
time via Census's per-county endpoint, caches in-process for 60s, and
UPSERTs the result back into `census_cbp_county_yearly` so the second
request hits the DB.

Single-cell request shape (verified live 2026-06-03):

    GET https://api.census.gov/data/2022/cbp
        ?get=NAME,ESTAB,EMP,PAYANN,NAICS2017
        &for=county:{ccc}
        &in=state:{ss}
        &NAICS2017={code}
        &key={CENSUS_SURVEY_API_KEY}

Returns one row when data exists; 204 No Content when truly missing.
Both outcomes are cached; 204 is cached as `None` so repeated misses
don't hammer Census.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_CENSUS_URL_TEMPLATE = "https://api.census.gov/data/{year}/cbp"
_NAICS_VINTAGE = "NAICS2017"
_CACHE_TTL = timedelta(seconds=60)
_CACHE_MAX_ENTRIES = 5000  # rough cap to bound memory

# (geo_id, naics, year) → (establishments_or_None, fetched_at)
_CACHE: Dict[Tuple[str, str, int], Tuple[Optional[int], datetime]] = {}


def _cache_get(key: Tuple[str, str, int]) -> Tuple[bool, Optional[int]]:
    hit = _CACHE.get(key)
    if not hit:
        return False, None
    value, when = hit
    if datetime.utcnow() - when >= _CACHE_TTL:
        _CACHE.pop(key, None)
        return False, None
    return True, value


def _cache_put(key: Tuple[str, str, int], value: Optional[int]) -> None:
    if len(_CACHE) >= _CACHE_MAX_ENTRIES:
        # Cheap eviction — drop the 1k oldest by fetched_at. This is
        # only hit under extreme load; LRU machinery isn't worth it.
        oldest = sorted(_CACHE.items(), key=lambda kv: kv[1][1])[:1000]
        for k, _ in oldest:
            _CACHE.pop(k, None)
    _CACHE[key] = (value, datetime.utcnow())


def clear_cache() -> None:
    """Test hook — drop all cached entries."""
    _CACHE.clear()


def _parse_int(raw: Any) -> Optional[int]:
    try:
        v = int(raw) if raw not in (None, "", "null", "N", "D", "S") else None
        return None if v is not None and v < 0 else v
    except (ValueError, TypeError):
        return None


def _upsert(db: Session, geo_id: str, naics: str, year: int,
            establishments: int,
            employees: Optional[int] = None,
            payroll_thousands: Optional[int] = None) -> None:
    """UPSERT a single cell into census_cbp_county_yearly. Idempotent."""
    db.execute(text("""
        INSERT INTO census_cbp_county_yearly
            (year, geo_id, naics_code, establishments, employees, annual_payroll_thousands)
        VALUES (:y, :g, :n, :e, :emp, :pay)
        ON CONFLICT (year, geo_id, naics_code) DO UPDATE
        SET establishments = EXCLUDED.establishments,
            employees = COALESCE(EXCLUDED.employees, census_cbp_county_yearly.employees),
            annual_payroll_thousands = COALESCE(
                EXCLUDED.annual_payroll_thousands,
                census_cbp_county_yearly.annual_payroll_thousands),
            ingested_at = NOW()
    """), {
        "y": year, "g": geo_id, "n": naics,
        "e": establishments, "emp": employees, "pay": payroll_thousands,
    })
    db.commit()


def _call_census(geo_id: str, naics: str, year: int,
                 timeout_seconds: float = 10.0
                 ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Single Census CBP call for one (county, NAICS) tuple.

    Returns (establishments, employees, payroll_thousands). Any of
    them can be None if Census suppressed that column. Returns
    (None, None, None) when Census returns 204 No Content (truly
    missing) OR when the API errors. Errors are logged WARN.
    """
    api_key = os.environ.get("CENSUS_SURVEY_API_KEY") or \
              os.environ.get("CENSUS_API_KEY") or ""
    if not api_key:
        logger.warning("cbp_live: no CENSUS_SURVEY_API_KEY; can't fetch")
        return None, None, None
    if not (len(geo_id) == 5 and geo_id.isdigit()):
        return None, None, None
    state = geo_id[:2]
    county = geo_id[2:]
    url = _CENSUS_URL_TEMPLATE.format(year=year)
    params = {
        "get": f"NAME,ESTAB,EMP,PAYANN,{_NAICS_VINTAGE}",
        "for": f"county:{county}",
        "in": f"state:{state}",
        _NAICS_VINTAGE: naics,
        "key": api_key,
    }
    try:
        with httpx.Client(timeout=timeout_seconds,
                          follow_redirects=True) as cli:
            resp = cli.get(url, params=params)
            if resp.status_code == 204:
                return None, None, None
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("cbp_live: Census error for %s/%s/%s: %s",
                       year, geo_id, naics, exc)
        return None, None, None
    if not data or len(data) < 2:
        return None, None, None
    headers = data[0]
    try:
        i_e = headers.index("ESTAB")
        i_m = headers.index("EMP")
        i_p = headers.index("PAYANN")
    except ValueError:
        return None, None, None
    row = data[1]
    return _parse_int(row[i_e]), _parse_int(row[i_m]), _parse_int(row[i_p])


def fetch_cbp_cell(db: Session, geo_id: str, naics: str,
                   year: int = 2022) -> Optional[int]:
    """Return the establishments count for a (county, NAICS) cell,
    fetching from Census if not cached or in the DB.

    Returns None when Census itself has no data for the cell.
    """
    geo_id = str(geo_id).strip()
    naics = str(naics).strip()
    key = (geo_id, naics, year)

    hit, cached = _cache_get(key)
    if hit:
        return cached

    estab, emp, pay = _call_census(geo_id, naics, year)
    if estab is not None:
        try:
            _upsert(db, geo_id, naics, year, estab, emp, pay)
        except Exception as exc:  # noqa: BLE001
            # Don't let an upsert failure invalidate the fetch. Worst
            # case the next request goes back to Census.
            logger.warning("cbp_live: upsert failed for %s/%s: %s",
                           geo_id, naics, exc)
    _cache_put(key, estab)
    return estab


def fetch_cbp_cells(db: Session, requests: List[Tuple[str, str]],
                    year: int = 2022,
                    per_request_cap: int = 30,
                    ) -> Dict[Tuple[str, str], Optional[int]]:
    """Fetch multiple cells. Honoured cap is `per_request_cap` —
    requests above the cap are skipped (returned as None) so a single
    trade-area lookup can't blow the Census rate limit.

    Requests already in the cache or DB will NOT be re-fetched, since
    `fetch_cbp_cell` checks the cache first. So the cap really only
    bites when many distinct missing cells stack up in one request.
    """
    out: Dict[Tuple[str, str], Optional[int]] = {}
    fetched = 0
    for geo_id, naics in requests:
        if fetched >= per_request_cap:
            out[(geo_id, naics)] = None
            continue
        # Cache check before counting against the cap. If it's cached,
        # we don't burn one of the 30 budgeted Census calls.
        key = (geo_id, naics, year)
        hit, cached = _cache_get(key)
        if hit:
            out[(geo_id, naics)] = cached
            continue
        out[(geo_id, naics)] = fetch_cbp_cell(db, geo_id, naics, year)
        fetched += 1
    return out
