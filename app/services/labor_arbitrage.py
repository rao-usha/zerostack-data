"""
Labor Arbitrage Maps — compare labor costs across geographies.

Queries the existing `occupational_wage` table (BLS OES data) to help PE
firms identify geographies with lower labor costs for specific occupations
relevant to their vertical strategy.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vertical → SOC occupation mapping
# ---------------------------------------------------------------------------

VERTICAL_OCCUPATIONS: Dict[str, List[tuple]] = {
    "medspa": [
        ("29-1229", "Physicians (All Other)"),
        ("29-1141", "Registered Nurses"),
        ("29-1171", "Nurse Practitioners"),
        ("31-9011", "Massage Therapists"),
        ("39-5012", "Hairdressers & Cosmetologists"),
    ],
    "dental": [
        ("29-1021", "Dentists, General"),
        ("29-2021", "Dental Hygienists"),
        ("31-9091", "Dental Assistants"),
        ("29-1029", "Dentists, All Other Specialists"),
    ],
    "hvac": [
        ("49-9021", "Heating, AC & Refrigeration Mechanics"),
        ("47-2111", "Electricians"),
        ("49-9071", "Maintenance & Repair Workers"),
        ("47-2152", "Plumbers, Pipefitters & Steamfitters"),
    ],
    "veterinary": [
        ("29-1131", "Veterinarians"),
        ("29-2056", "Veterinary Technologists & Technicians"),
        ("39-2021", "Animal Caretakers"),
    ],
    "car_wash": [
        ("53-7061", "Cleaners of Vehicles & Equipment"),
        ("41-2031", "Retail Salespersons"),
        ("53-7062", "Laborers & Freight Movers"),
    ],
    "physical_therapy": [
        ("29-1123", "Physical Therapists"),
        ("31-2021", "Physical Therapist Assistants"),
        ("31-2022", "Physical Therapist Aides"),
    ],
}


class LaborArbitrageService:
    """Compare labor costs across geographies by occupation."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Core: compare a single occupation across areas
    # ------------------------------------------------------------------

    def compare_wages(
        self,
        occupation_code: str,
        base_area: Optional[str] = None,
        area_type: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Compare wages for one occupation across geographies.

        Args:
            occupation_code: SOC code (e.g. "29-1021")
            base_area: Optional area_code to use as comparison baseline
            area_type: Filter by area_type (e.g. "state", "msa")
            limit: Max results
        """
        where = ["occupation_code = :occ"]
        params: Dict[str, Any] = {"occ": occupation_code, "lim": limit}

        if area_type:
            where.append("area_type = :area_type")
            params["area_type"] = area_type

        where_sql = " AND ".join(where)

        query = text(f"""
            SELECT
                area_type, area_code, area_name,
                occupation_code, occupation_title,
                employment,
                mean_hourly_wage, median_hourly_wage,
                pct_10_hourly, pct_25_hourly, pct_75_hourly, pct_90_hourly,
                mean_annual_wage, median_annual_wage,
                period_year
            FROM occupational_wage
            WHERE {where_sql}
            ORDER BY mean_annual_wage ASC NULLS LAST
            LIMIT :lim
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error querying occupational_wage: {e}")
            self.db.rollback()
            return {"error": str(e), "results": []}

        results = [dict(r) for r in rows]

        # If base_area provided, compute differentials
        base_wage = None
        if base_area and results:
            base_row = next(
                (r for r in results if r["area_code"] == base_area), None
            )
            if not base_row:
                # Fetch base area separately
                base_q = text("""
                    SELECT mean_annual_wage FROM occupational_wage
                    WHERE occupation_code = :occ AND area_code = :base
                    LIMIT 1
                """)
                base_result = self.db.execute(
                    base_q, {"occ": occupation_code, "base": base_area}
                ).fetchone()
                if base_result and base_result[0]:
                    base_wage = float(base_result[0])
            else:
                base_wage = (
                    float(base_row["mean_annual_wage"])
                    if base_row["mean_annual_wage"]
                    else None
                )

            if base_wage and base_wage > 0:
                for r in results:
                    if r["mean_annual_wage"] is not None:
                        diff = float(r["mean_annual_wage"]) - base_wage
                        r["wage_differential"] = round(diff, 2)
                        r["wage_differential_pct"] = round(
                            (diff / base_wage) * 100, 2
                        )
                    else:
                        r["wage_differential"] = None
                        r["wage_differential_pct"] = None

        return {
            "occupation_code": occupation_code,
            "base_area": base_area,
            "base_wage": base_wage,
            "total_areas": len(results),
            "results": results,
        }

    # ------------------------------------------------------------------
    # Vertical profile: all occupations for a vertical across areas
    # ------------------------------------------------------------------

    def vertical_profile(
        self,
        slug: str,
        area_codes: Optional[List[str]] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Get wage data for all occupations in a vertical across areas.

        Args:
            slug: Vertical slug (e.g. "dental", "hvac")
            area_codes: Optional list of area_codes to filter (e.g. ["ST06", "ST48"])
            limit: Max areas per occupation
        """
        occupations = VERTICAL_OCCUPATIONS.get(slug)
        if not occupations:
            return {
                "error": f"Unknown vertical: {slug}",
                "available_verticals": list(VERTICAL_OCCUPATIONS.keys()),
            }

        occ_codes = [o[0] for o in occupations]

        where = ["occupation_code = ANY(:codes)"]
        params: Dict[str, Any] = {"codes": occ_codes, "lim": limit}

        if area_codes:
            where.append("area_code = ANY(:areas)")
            params["areas"] = area_codes

        where_sql = " AND ".join(where)

        query = text(f"""
            SELECT
                area_type, area_code, area_name,
                occupation_code, occupation_title,
                employment,
                mean_hourly_wage, median_hourly_wage,
                mean_annual_wage, median_annual_wage,
                period_year
            FROM occupational_wage
            WHERE {where_sql}
            ORDER BY occupation_code, mean_annual_wage ASC NULLS LAST
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error querying vertical profile: {e}")
            self.db.rollback()
            return {"error": str(e)}

        # Group by occupation
        by_occupation: Dict[str, List[Dict]] = {}
        for r in rows:
            code = r["occupation_code"]
            by_occupation.setdefault(code, []).append(dict(r))

        # Compute summary stats per occupation
        occupation_summaries = []
        for occ_code, occ_title in occupations:
            areas = by_occupation.get(occ_code, [])
            wages = [
                float(a["mean_annual_wage"])
                for a in areas
                if a["mean_annual_wage"] is not None
            ]
            if wages:
                occupation_summaries.append({
                    "occupation_code": occ_code,
                    "occupation_title": occ_title,
                    "areas_with_data": len(wages),
                    "min_wage": min(wages),
                    "max_wage": max(wages),
                    "mean_wage": round(sum(wages) / len(wages), 2),
                    "wage_spread": round(max(wages) - min(wages), 2),
                    "cheapest_area": areas[0]["area_name"] if areas else None,
                    "most_expensive_area": (
                        areas[-1]["area_name"] if areas else None
                    ),
                    "areas": areas[:limit],
                })
            else:
                occupation_summaries.append({
                    "occupation_code": occ_code,
                    "occupation_title": occ_title,
                    "areas_with_data": 0,
                    "note": "No wage data found",
                })

        return {
            "vertical": slug,
            "vertical_occupations": [
                {"code": c, "title": t} for c, t in occupations
            ],
            "area_filter": area_codes,
            "occupations": occupation_summaries,
        }

    # ------------------------------------------------------------------
    # Reference data
    # ------------------------------------------------------------------

    def list_occupations(self) -> Dict[str, Any]:
        """List all distinct occupations in the wage table."""
        query = text("""
            SELECT DISTINCT occupation_code, occupation_title,
                   COUNT(DISTINCT area_code) AS area_count,
                   AVG(mean_annual_wage) AS avg_wage
            FROM occupational_wage
            WHERE mean_annual_wage IS NOT NULL
            GROUP BY occupation_code, occupation_title
            ORDER BY occupation_code
        """)
        try:
            rows = self.db.execute(query).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error listing occupations: {e}")
            self.db.rollback()
            return {"error": str(e), "occupations": []}

        return {
            "total_occupations": len(rows),
            "occupations": [dict(r) for r in rows],
            "vertical_mappings": {
                slug: [{"code": c, "title": t} for c, t in occs]
                for slug, occs in VERTICAL_OCCUPATIONS.items()
            },
        }

    def list_areas(self, area_type: Optional[str] = None) -> Dict[str, Any]:
        """List all distinct geographic areas in the wage table."""
        where = ""
        params: Dict[str, Any] = {}
        if area_type:
            where = "WHERE area_type = :area_type"
            params["area_type"] = area_type

        query = text(f"""
            SELECT DISTINCT area_type, area_code, area_name,
                   COUNT(DISTINCT occupation_code) AS occupation_count
            FROM occupational_wage
            {where}
            GROUP BY area_type, area_code, area_name
            ORDER BY area_type, area_name
        """)
        try:
            rows = self.db.execute(query, params).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error listing areas: {e}")
            self.db.rollback()
            return {"error": str(e), "areas": []}

        return {
            "total_areas": len(rows),
            "areas": [dict(r) for r in rows],
        }
