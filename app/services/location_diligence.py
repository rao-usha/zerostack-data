"""
Integrated Due Diligence Package — one-call location intelligence.

Queries existing tables across all domains to assemble a comprehensive
location DD profile: market overview, environmental compliance, labor
market, infrastructure, risk, incentives, competitive landscape, and
healthcare providers.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section definitions
# ---------------------------------------------------------------------------

DD_SECTIONS = [
    {
        "key": "market_overview",
        "label": "Market Overview",
        "description": "IRS SOI income demographics, population, and industry employment",
        "tables": ["irs_soi_county_income", "irs_soi_zip_income"],
    },
    {
        "key": "environmental",
        "label": "Environmental Compliance",
        "description": "EPA ECHO facilities, violations, penalties, and compliance history",
        "tables": ["epa_echo_facilities"],
    },
    {
        "key": "labor_market",
        "label": "Labor Market",
        "description": "BLS OES wages, employment by occupation",
        "tables": ["occupational_wage"],
    },
    {
        "key": "infrastructure",
        "label": "Infrastructure",
        "description": "Utility rates, broadband availability, transport access",
        "tables": ["utility_rate", "fcc_broadband"],
    },
    {
        "key": "risk_profile",
        "label": "Risk Profile",
        "description": "FEMA National Risk Index, flood zones, seismic hazards",
        "tables": ["national_risk_index"],
    },
    {
        "key": "incentives",
        "label": "Incentives & Programs",
        "description": "Good Jobs First subsidy deals and economic development programs",
        "tables": ["incentive_deal", "incentive_program"],
    },
    {
        "key": "competitive_landscape",
        "label": "Competitive Landscape",
        "description": "Same-industry establishments, EPA-regulated facilities",
        "tables": ["epa_echo_facilities"],
    },
    {
        "key": "healthcare_providers",
        "label": "Healthcare Providers",
        "description": "NPPES provider counts by specialty (healthcare verticals only)",
        "tables": ["nppes_providers"],
    },
]


class LocationDiligenceService:
    """Assemble a comprehensive location DD package from existing data."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Full package
    # ------------------------------------------------------------------

    def get_package(
        self,
        county_fips: Optional[str] = None,
        state_fips: Optional[str] = None,
        naics_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a full DD package for a location.

        Args:
            county_fips: 5-digit county FIPS (e.g. "06037" for LA County)
            state_fips: 2-digit state FIPS (e.g. "06" for California)
            naics_code: Optional NAICS code for industry-specific sections
        """
        if not county_fips and not state_fips:
            return {"error": "Provide county_fips or state_fips"}

        # Derive state from county if not provided
        if county_fips and not state_fips:
            state_fips = county_fips[:2]

        package: Dict[str, Any] = {
            "county_fips": county_fips,
            "state_fips": state_fips,
            "naics_code": naics_code,
            "sections": {},
        }

        # Build each section independently (fail-safe per section)
        package["sections"]["market_overview"] = self._market_overview(
            county_fips, state_fips
        )
        package["sections"]["environmental"] = self._environmental(
            county_fips, state_fips
        )
        package["sections"]["labor_market"] = self._labor_market(state_fips)
        package["sections"]["infrastructure"] = self._infrastructure(state_fips)
        package["sections"]["risk_profile"] = self._risk_profile(
            county_fips, state_fips
        )
        package["sections"]["incentives"] = self._incentives(state_fips)
        package["sections"]["competitive_landscape"] = self._competitive(
            county_fips, state_fips, naics_code
        )
        package["sections"]["healthcare_providers"] = self._healthcare(
            county_fips, state_fips
        )

        # Coverage summary
        available = sum(
            1 for s in package["sections"].values()
            if s.get("data_available", False)
        )
        package["coverage"] = {
            "total_sections": len(DD_SECTIONS),
            "sections_with_data": available,
            "coverage_pct": round(available / len(DD_SECTIONS) * 100, 1),
        }

        return package

    # ------------------------------------------------------------------
    # Compare multiple locations
    # ------------------------------------------------------------------

    def compare_locations(
        self,
        locations: List[str],
        naics_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare DD packages across multiple county FIPS codes."""
        if not locations or len(locations) < 2:
            return {"error": "Provide at least 2 county FIPS codes"}

        results = {}
        for fips in locations[:10]:  # Cap at 10
            results[fips] = self.get_package(
                county_fips=fips, naics_code=naics_code
            )

        return {
            "locations_compared": len(results),
            "naics_code": naics_code,
            "packages": results,
        }

    # ------------------------------------------------------------------
    # Coverage check
    # ------------------------------------------------------------------

    def check_coverage(
        self,
        county_fips: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Check which DD sections have data for a location."""
        table_counts: Dict[str, int] = {}

        # Check each table for data
        check_tables = [
            "irs_soi_county_income", "irs_soi_zip_income",
            "epa_echo_facilities", "occupational_wage",
            "utility_rate", "national_risk_index",
            "incentive_deal", "nppes_providers",
        ]

        for table in check_tables:
            try:
                where = "WHERE 1=1"
                params: Dict[str, Any] = {}
                if county_fips:
                    if table == "irs_soi_county_income":
                        where += " AND county_fips = :fips"
                        params["fips"] = county_fips
                    elif table == "epa_echo_facilities":
                        where += " AND fips_code = :fips"
                        params["fips"] = county_fips
                elif state:
                    if table in ("irs_soi_county_income", "irs_soi_zip_income"):
                        where += " AND state_abbr = :st"
                        params["st"] = state.upper()
                    elif table == "occupational_wage":
                        where += " AND area_code = :area"
                        params["area"] = f"ST{state.upper()}" if len(state) == 2 else state

                q = text(f"SELECT COUNT(*) FROM {table} {where}")
                count = self.db.execute(q, params).scalar() or 0
                table_counts[table] = count
            except Exception:
                table_counts[table] = 0
                self.db.rollback()

        # Map tables to sections
        section_coverage = []
        for section in DD_SECTIONS:
            has_data = any(
                table_counts.get(t, 0) > 0 for t in section["tables"]
            )
            section_coverage.append({
                "key": section["key"],
                "label": section["label"],
                "has_data": has_data,
                "tables": {
                    t: table_counts.get(t, 0) for t in section["tables"]
                },
            })

        return {
            "county_fips": county_fips,
            "state": state,
            "sections": section_coverage,
            "total_sections": len(DD_SECTIONS),
            "sections_with_data": sum(
                1 for s in section_coverage if s["has_data"]
            ),
        }

    # ------------------------------------------------------------------
    # Section builders (each is fail-safe)
    # ------------------------------------------------------------------

    def _safe_query(self, query_str: str, params: Dict) -> List[Dict]:
        """Execute a query and return list of dicts, empty on error."""
        try:
            rows = self.db.execute(text(query_str), params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.debug(f"DD query returned no results: {e}")
            self.db.rollback()
            return []

    def _market_overview(
        self, county_fips: Optional[str], state_fips: Optional[str]
    ) -> Dict[str, Any]:
        """IRS SOI income data for the county."""
        if not county_fips:
            return {"data_available": False, "note": "Requires county_fips"}

        rows = self._safe_query(
            """
            SELECT state_abbr, county_name,
                   SUM(num_returns) AS total_returns,
                   SUM(total_agi) AS total_agi,
                   SUM(CASE WHEN agi_class IN ('5','6')
                       THEN num_returns ELSE 0 END) AS returns_100k_plus,
                   SUM(total_wages_salaries) AS total_wages
            FROM irs_soi_county_income
            WHERE county_fips = :fips
            GROUP BY state_abbr, county_name
            """,
            {"fips": county_fips},
        )

        if not rows:
            return {"data_available": False}

        r = rows[0]
        total = r.get("total_returns") or 0
        total_agi = r.get("total_agi") or 0
        returns_100k = r.get("returns_100k_plus") or 0

        return {
            "data_available": True,
            "county_name": r.get("county_name"),
            "state_abbr": r.get("state_abbr"),
            "total_tax_returns": total,
            "total_agi_thousands": total_agi,
            "avg_agi": round(total_agi / total, 2) if total > 0 else None,
            "pct_returns_100k_plus": (
                round(returns_100k / total * 100, 2) if total > 0 else None
            ),
            "total_wages_thousands": r.get("total_wages"),
        }

    def _environmental(
        self, county_fips: Optional[str], state_fips: Optional[str]
    ) -> Dict[str, Any]:
        """EPA ECHO facility summary."""
        where = "1=1"
        params: Dict[str, Any] = {}
        if county_fips:
            where = "fips_code = :fips"
            params["fips"] = county_fips
        elif state_fips:
            where = "LEFT(fips_code, 2) = :st"
            params["st"] = state_fips

        rows = self._safe_query(
            f"""
            SELECT COUNT(*) AS facility_count,
                   COUNT(CASE WHEN violation_status = 'Violation' THEN 1 END) AS violators,
                   SUM(COALESCE(penalties_amount, 0)) AS total_penalties
            FROM epa_echo_facilities
            WHERE {where}
            """,
            params,
        )

        if not rows or not rows[0].get("facility_count"):
            return {"data_available": False}

        r = rows[0]
        return {
            "data_available": True,
            "total_facilities": r["facility_count"],
            "facilities_with_violations": r.get("violators", 0),
            "total_penalties": r.get("total_penalties", 0),
            "violation_rate": (
                round(r["violators"] / r["facility_count"] * 100, 2)
                if r["facility_count"] > 0 and r.get("violators")
                else 0
            ),
        }

    def _labor_market(self, state_fips: Optional[str]) -> Dict[str, Any]:
        """BLS OES wage summary for the state."""
        if not state_fips:
            return {"data_available": False, "note": "Requires state_fips"}

        area_code = f"ST{state_fips}" if len(state_fips) == 2 else state_fips
        rows = self._safe_query(
            """
            SELECT occupation_code, occupation_title,
                   employment, mean_annual_wage, median_annual_wage
            FROM occupational_wage
            WHERE area_code = :area
            ORDER BY employment DESC NULLS LAST
            LIMIT 20
            """,
            {"area": area_code},
        )

        if not rows:
            return {"data_available": False}

        return {
            "data_available": True,
            "area_code": area_code,
            "top_occupations": len(rows),
            "occupations": rows,
        }

    def _infrastructure(self, state_fips: Optional[str]) -> Dict[str, Any]:
        """Utility rates for the state."""
        if not state_fips:
            return {"data_available": False, "note": "Requires state_fips"}

        rows = self._safe_query(
            """
            SELECT sector, AVG(rate_cents_kwh) AS avg_rate,
                   COUNT(*) AS utility_count
            FROM utility_rate
            WHERE state_fips = :st
            GROUP BY sector
            ORDER BY sector
            """,
            {"st": state_fips},
        )

        if not rows:
            return {"data_available": False}

        return {
            "data_available": True,
            "utility_rates_by_sector": rows,
        }

    def _risk_profile(
        self, county_fips: Optional[str], state_fips: Optional[str]
    ) -> Dict[str, Any]:
        """FEMA NRI risk scores."""
        where = "1=1"
        params: Dict[str, Any] = {}
        if county_fips:
            where = "county_fips = :fips"
            params["fips"] = county_fips
        elif state_fips:
            where = "state_fips = :st"
            params["st"] = state_fips

        rows = self._safe_query(
            f"""
            SELECT county_name, state_abbr,
                   risk_score, risk_rating,
                   expected_annual_loss,
                   social_vulnerability_score
            FROM national_risk_index
            WHERE {where}
            ORDER BY risk_score DESC
            LIMIT 10
            """,
            params,
        )

        if not rows:
            return {"data_available": False}

        return {
            "data_available": True,
            "counties": len(rows),
            "risk_data": rows,
        }

    def _incentives(self, state_fips: Optional[str]) -> Dict[str, Any]:
        """Good Jobs First incentive deals and programs."""
        if not state_fips:
            return {"data_available": False, "note": "Requires state_fips"}

        rows = self._safe_query(
            """
            SELECT COUNT(*) AS deal_count,
                   SUM(subsidy_value) AS total_subsidy,
                   COUNT(DISTINCT program_name) AS program_count
            FROM incentive_deal
            WHERE state_fips = :st
            """,
            {"st": state_fips},
        )

        if not rows or not rows[0].get("deal_count"):
            return {"data_available": False}

        return {
            "data_available": True,
            "total_deals": rows[0]["deal_count"],
            "total_subsidy_value": rows[0].get("total_subsidy"),
            "unique_programs": rows[0].get("program_count", 0),
        }

    def _competitive(
        self,
        county_fips: Optional[str],
        state_fips: Optional[str],
        naics_code: Optional[str],
    ) -> Dict[str, Any]:
        """EPA-regulated facilities as a proxy for industrial presence."""
        where = "1=1"
        params: Dict[str, Any] = {}
        if county_fips:
            where = "fips_code = :fips"
            params["fips"] = county_fips
        elif state_fips:
            where = "LEFT(fips_code, 2) = :st"
            params["st"] = state_fips

        rows = self._safe_query(
            f"""
            SELECT naics_code, COUNT(*) AS estab_count
            FROM epa_echo_facilities
            WHERE {where} AND naics_code IS NOT NULL
            GROUP BY naics_code
            ORDER BY estab_count DESC
            LIMIT 20
            """,
            params,
        )

        if not rows:
            return {"data_available": False}

        # Highlight target NAICS if provided
        target = None
        if naics_code:
            target = next(
                (r for r in rows if r["naics_code"] == naics_code), None
            )

        return {
            "data_available": True,
            "top_industries": rows,
            "target_naics": target,
            "target_naics_code": naics_code,
        }

    def _healthcare(
        self, county_fips: Optional[str], state_fips: Optional[str]
    ) -> Dict[str, Any]:
        """NPPES provider counts by taxonomy."""
        where_col = "state_fips"
        params: Dict[str, Any] = {}

        if county_fips:
            # NPPES may not have county_fips, use state
            params["st"] = county_fips[:2] if len(county_fips) >= 2 else county_fips
        elif state_fips:
            params["st"] = state_fips
        else:
            return {"data_available": False}

        rows = self._safe_query(
            """
            SELECT taxonomy_code, taxonomy_description,
                   COUNT(*) AS provider_count
            FROM nppes_providers
            WHERE state_code = :st
            GROUP BY taxonomy_code, taxonomy_description
            ORDER BY provider_count DESC
            LIMIT 20
            """,
            {"st": params.get("st")},
        )

        if not rows:
            return {"data_available": False}

        return {
            "data_available": True,
            "specialties": len(rows),
            "providers": rows,
        }

    # ------------------------------------------------------------------
    # Static
    # ------------------------------------------------------------------

    @staticmethod
    def get_sections() -> List[Dict[str, Any]]:
        """Return DD section definitions."""
        return DD_SECTIONS
