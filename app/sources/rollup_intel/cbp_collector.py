"""
Census County Business Patterns (CBP) fetch-and-cache collector.

Fetches establishment counts, employment, and payroll by NAICS code and
county from the Census CBP API, computes size distribution metrics (HHI,
small_biz_pct), and caches results in the census_cbp table.

Census CBP API: https://api.census.gov/data/{year}/cbp
"""

import logging
import math
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.rollup_intel.metadata import (
    generate_create_census_cbp_sql,
    CBP_VARIABLES,
)

logger = logging.getLogger(__name__)

# Census CBP base URL pattern
CBP_BASE_URL = "https://api.census.gov/data/{year}/cbp"


class CBPCollector:
    """Fetch and cache Census County Business Patterns data."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        from app.core.database import get_engine
        try:
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.execute(generate_create_census_cbp_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"CBP table creation warning: {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def collect(
        self,
        naics_code: str,
        year: int = 2021,
        state: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Fetch CBP data for a NAICS code from Census API.

        Args:
            naics_code: NAICS industry code (e.g. "621111")
            year: Data year (default 2021, latest available)
            state: Optional 2-digit state FIPS to filter
            force: Re-fetch even if cached
        """
        # Check cache first
        if not force:
            cached_count = self._count_cached(naics_code, year, state)
            if cached_count > 0:
                logger.info(
                    f"CBP cache hit: {cached_count} records for "
                    f"NAICS {naics_code}, year {year}"
                )
                return {
                    "source": "cache",
                    "naics_code": naics_code,
                    "year": year,
                    "records": cached_count,
                }

        # Fetch from Census API
        from app.core.config import get_settings
        settings = get_settings()
        api_key = settings.census_survey_api_key
        if not api_key:
            return {"error": "CENSUS_SURVEY_API_KEY not configured"}

        from app.sources.census.client import CensusClient

        records = []
        async with CensusClient(api_key=api_key) as client:
            # Fetch county-level data
            records = await self._fetch_county_data(
                client, naics_code, year, state
            )

        if not records:
            return {
                "source": "api",
                "naics_code": naics_code,
                "year": year,
                "records": 0,
                "note": "No data returned from Census API",
            }

        # Compute derived fields and persist
        enriched = self._compute_derived(records)
        saved = self._bulk_save(enriched)

        return {
            "source": "api",
            "naics_code": naics_code,
            "year": year,
            "records_fetched": len(records),
            "records_saved": saved,
        }

    def get_cached(
        self,
        naics_code: str,
        year: int = 2021,
        state: Optional[str] = None,
        min_establishments: int = 0,
    ) -> List[Dict]:
        """Return cached CBP data from the database."""
        where = ["naics_code = :naics", "year = :year"]
        params: Dict[str, Any] = {"naics": naics_code, "year": year}

        if state:
            where.append("state_fips = :state")
            params["state"] = state

        if min_establishments > 0:
            where.append("establishments >= :min_estab")
            params["min_estab"] = min_establishments

        where_sql = " AND ".join(where)
        query = text(f"""
            SELECT * FROM census_cbp
            WHERE {where_sql}
            ORDER BY establishments DESC
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Error reading CBP cache: {e}")
            self.db.rollback()
            return []

    # ------------------------------------------------------------------
    # Census API fetch
    # ------------------------------------------------------------------

    async def _fetch_county_data(
        self,
        client: Any,
        naics_code: str,
        year: int,
        state: Optional[str],
    ) -> List[Dict]:
        """Fetch county-level CBP data from Census API."""
        url = f"{year}/cbp"
        params: Dict[str, Any] = {
            "get": "NAICS2017,ESTAB,EMP,PAYANN,NAME",
            "for": "county:*",
            "NAICS2017": naics_code,
        }

        if state:
            params["in"] = f"state:{state}"
        else:
            params["in"] = "state:*"

        try:
            data = await client.get(url, params=params)
        except Exception as e:
            logger.error(f"Census CBP API error: {e}")
            return []

        if not data or len(data) < 2:
            return []

        headers = data[0]
        records = []
        for row in data[1:]:
            rec = {headers[i]: row[i] for i in range(len(headers))}
            state_fips = rec.get("state", "")
            county_code = rec.get("county", "")
            county_fips = f"{state_fips}{county_code}"

            records.append({
                "year": year,
                "naics_code": naics_code,
                "geo_level": "county",
                "state_fips": state_fips,
                "county_fips": county_fips,
                "geo_name": rec.get("NAME", ""),
                "establishments": _safe_int(rec.get("ESTAB")),
                "employees": _safe_int(rec.get("EMP")),
                "annual_payroll_thousands": _safe_int(rec.get("PAYANN")),
            })

        logger.info(
            f"Fetched {len(records)} county records for NAICS {naics_code}"
        )
        return records

    # ------------------------------------------------------------------
    # Derived metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_derived(records: List[Dict]) -> List[Dict]:
        """Compute avg_employees_per_estab, small_biz_pct, HHI."""
        for rec in records:
            estab = rec.get("establishments") or 0
            emp = rec.get("employees") or 0

            # Average employees per establishment
            rec["avg_employees_per_estab"] = (
                round(emp / estab, 2) if estab > 0 else None
            )

            # Size class fields (populated if size class data fetched)
            s1 = rec.get("estab_1_4") or 0
            s2 = rec.get("estab_5_9") or 0
            s3 = rec.get("estab_10_19") or 0
            s4 = rec.get("estab_20_49") or 0
            s5 = rec.get("estab_50_99") or 0
            s6 = rec.get("estab_100_249") or 0
            s7 = rec.get("estab_250_plus") or 0
            total_size = s1 + s2 + s3 + s4 + s5 + s6 + s7

            if total_size > 0:
                # Small biz = under 50 employees
                small = s1 + s2 + s3 + s4
                rec["small_biz_pct"] = round(small / total_size, 4)

                # HHI from size class distribution
                shares = [s / total_size for s in [s1, s2, s3, s4, s5, s6, s7] if s > 0]
                rec["hhi"] = round(sum(sh ** 2 for sh in shares), 6)
            else:
                # Estimate from average size
                if estab > 0 and emp > 0:
                    avg_size = emp / estab
                    # Heuristic: smaller avg size = more fragmented
                    rec["small_biz_pct"] = round(
                        min(1.0, max(0.0, 1.0 - (avg_size - 5) / 100)), 4
                    )
                    # Approximate HHI: 1/N (equal shares)
                    rec["hhi"] = round(1.0 / estab, 6) if estab > 0 else None
                else:
                    rec["small_biz_pct"] = None
                    rec["hhi"] = None

        return records

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _count_cached(
        self, naics_code: str, year: int, state: Optional[str]
    ) -> int:
        where = "naics_code = :naics AND year = :year"
        params: Dict[str, Any] = {"naics": naics_code, "year": year}
        if state:
            where += " AND state_fips = :state"
            params["state"] = state
        try:
            return (
                self.db.execute(
                    text(f"SELECT COUNT(*) FROM census_cbp WHERE {where}"),
                    params,
                ).scalar()
                or 0
            )
        except Exception:
            self.db.rollback()
            return 0

    def _bulk_save(self, records: List[Dict]) -> int:
        """Bulk upsert CBP records."""
        if not records:
            return 0

        upsert_sql = text("""
            INSERT INTO census_cbp (
                year, naics_code, geo_level, state_fips, county_fips,
                geo_name, establishments, employees, annual_payroll_thousands,
                estab_1_4, estab_5_9, estab_10_19, estab_20_49,
                estab_50_99, estab_100_249, estab_250_plus,
                avg_employees_per_estab, small_biz_pct, hhi
            ) VALUES (
                :year, :naics_code, :geo_level, :state_fips, :county_fips,
                :geo_name, :establishments, :employees, :annual_payroll_thousands,
                :estab_1_4, :estab_5_9, :estab_10_19, :estab_20_49,
                :estab_50_99, :estab_100_249, :estab_250_plus,
                :avg_employees_per_estab, :small_biz_pct, :hhi
            )
            ON CONFLICT (year, naics_code, geo_level, county_fips) DO UPDATE SET
                geo_name = EXCLUDED.geo_name,
                establishments = EXCLUDED.establishments,
                employees = EXCLUDED.employees,
                annual_payroll_thousands = EXCLUDED.annual_payroll_thousands,
                estab_1_4 = EXCLUDED.estab_1_4,
                estab_5_9 = EXCLUDED.estab_5_9,
                estab_10_19 = EXCLUDED.estab_10_19,
                estab_20_49 = EXCLUDED.estab_20_49,
                estab_50_99 = EXCLUDED.estab_50_99,
                estab_100_249 = EXCLUDED.estab_100_249,
                estab_250_plus = EXCLUDED.estab_250_plus,
                avg_employees_per_estab = EXCLUDED.avg_employees_per_estab,
                small_biz_pct = EXCLUDED.small_biz_pct,
                hhi = EXCLUDED.hhi,
                fetched_at = NOW()
        """)

        batch_size = 500
        total_saved = 0
        try:
            for start in range(0, len(records), batch_size):
                batch = records[start:start + batch_size]
                for rec in batch:
                    # Ensure all keys present
                    for key in (
                        "estab_1_4", "estab_5_9", "estab_10_19", "estab_20_49",
                        "estab_50_99", "estab_100_249", "estab_250_plus",
                    ):
                        rec.setdefault(key, None)
                    self.db.execute(upsert_sql, rec)
                self.db.commit()
                total_saved += len(batch)
            logger.info(f"Saved {total_saved} CBP records")
        except Exception as e:
            logger.error(f"Error saving CBP records: {e}")
            self.db.rollback()

        return total_saved


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(val: Any) -> Optional[int]:
    """Convert Census API value to int, handling None and non-numeric."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
