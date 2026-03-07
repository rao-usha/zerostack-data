"""
County Regulatory Speed Score — scoring engine.

Ranks US counties by regulatory speed / development-friendliness
using permit velocity, jurisdictional simplicity, energy siting
friendliness, and historical datacenter deals.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.county_regulatory_metadata import (
    generate_create_county_regulatory_scores_sql,
    FACTOR_DOCUMENTATION,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "v1.0"

WEIGHTS = {
    "permit_velocity": 0.30,
    "jurisdictional_simplicity": 0.25,
    "energy_siting_friendliness": 0.20,
    "historical_dc_deals": 0.25,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]


class CountyRegulatoryScorer:
    """Compute regulatory speed scores for US counties."""

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
                cursor.execute(generate_create_county_regulatory_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"County regulatory table creation warning: {e}")

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _percentile_rank(values: List[float]) -> List[float]:
        """Return percentile ranks (0-100) for a list of values. Ties get average rank."""
        n = len(values)
        if n == 0:
            return []
        if n == 1:
            return [50.0]
        indexed = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n and values[indexed[j]] == values[indexed[i]]:
                j += 1
            avg_rank = sum(range(i, j)) / (j - i)
            for k in range(i, j):
                ranks[indexed[k]] = (avg_rank / (n - 1)) * 100.0
            i = j
        return ranks

    @staticmethod
    def _inverted_percentile_rank(values: List[float]) -> List[float]:
        """Return inverted percentile ranks — lower raw value = higher score."""
        ranks = CountyRegulatoryScorer._percentile_rank(values)
        return [100.0 - r for r in ranks]

    def score_all_counties(
        self, force: bool = False, state: Optional[str] = None
    ) -> Dict[str, Any]:
        """Score all counties and save results."""
        today = date.today()

        if not force:
            existing = self.db.execute(
                text("SELECT COUNT(*) FROM county_regulatory_scores WHERE score_date = :d"),
                {"d": today},
            ).scalar()
            if existing and existing > 0:
                logger.info(f"Scores already exist for {today}, use force=True to rescore")
                return self._load_summary(state)

        county_data = self._fetch_all_county_data(state)
        if not county_data:
            logger.warning("No county data available for scoring")
            return {"total_counties": 0, "grade_distribution": {}}

        # Compute factor scores
        permit_scores = self._compute_permit_velocity(county_data)
        jurisdiction_scores = self._compute_jurisdictional_simplicity(county_data)
        energy_scores = self._compute_energy_siting_friendliness(county_data)
        dc_deal_scores = self._compute_historical_dc_deals(county_data)

        # Build records
        records = []
        for i, county in enumerate(county_data):
            composite = (
                permit_scores[i] * WEIGHTS["permit_velocity"]
                + jurisdiction_scores[i] * WEIGHTS["jurisdictional_simplicity"]
                + energy_scores[i] * WEIGHTS["energy_siting_friendliness"]
                + dc_deal_scores[i] * WEIGHTS["historical_dc_deals"]
            )

            records.append({
                "county_fips": county["county_fips"],
                "county_name": county.get("county_name"),
                "state": county.get("state"),
                "score_date": today,
                "overall_score": round(composite, 2),
                "grade": self._get_grade(composite),
                "permit_velocity_score": round(permit_scores[i], 2),
                "jurisdictional_simplicity_score": round(jurisdiction_scores[i], 2),
                "energy_siting_score": round(energy_scores[i], 2),
                "historical_dc_deals_score": round(dc_deal_scores[i], 2),
                "permits_per_10k_pop": county.get("permits_per_10k_pop"),
                "permit_yoy_growth_pct": county.get("yoy_growth_pct"),
                "total_govt_units": county.get("total_governments"),
                "govts_per_10k_pop": county.get("govts_per_10k_pop"),
                "dc_incentive_programs": county.get("dc_incentive_programs", 0),
                "dc_disclosed_deals": county.get("dc_disclosed_deals", 0),
                "model_version": MODEL_VERSION,
            })

        # Assign ranks
        records.sort(key=lambda r: r["overall_score"], reverse=True)
        for i, rec in enumerate(records):
            rec["national_rank"] = i + 1

        # State ranks
        by_state: Dict[str, int] = {}
        for rec in records:
            st = rec.get("state", "")
            by_state[st] = by_state.get(st, 0) + 1
            rec["state_rank"] = by_state[st]

        # Bulk save
        self._bulk_save(records)

        return self._build_summary(records, state)

    def _fetch_all_county_data(self, state: Optional[str] = None) -> List[Dict]:
        """Fetch all county-level data for scoring."""
        state_filter = "WHERE bp.state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        query = text(f"""
            SELECT DISTINCT
                bp.county_fips,
                bp.county_name,
                bp.state,
                bp.permits_per_10k_pop,
                bp.yoy_growth_pct,
                bp.total_units,
                gu.total_governments,
                gu.govts_per_10k_pop,
                COALESCE(ip.dc_programs, 0) as dc_incentive_programs,
                COALESCE(id2.dc_deals, 0) as dc_disclosed_deals
            FROM building_permit bp
            LEFT JOIN government_unit gu ON bp.county_fips = gu.county_fips
            LEFT JOIN (
                SELECT state, COUNT(*) as dc_programs
                FROM incentive_program
                WHERE LOWER(program_name) LIKE '%%data center%%'
                   OR LOWER(program_name) LIKE '%%datacenter%%'
                   OR LOWER(target_industries::text) LIKE '%%data center%%'
                   OR LOWER(target_industries::text) LIKE '%%technology%%'
                GROUP BY state
            ) ip ON bp.state = ip.state
            LEFT JOIN (
                SELECT state, county, COUNT(*) as dc_deals
                FROM incentive_deal
                WHERE LOWER(company_name) LIKE '%%data%%center%%'
                   OR LOWER(company_name) LIKE '%%amazon%%'
                   OR LOWER(company_name) LIKE '%%google%%'
                   OR LOWER(company_name) LIKE '%%microsoft%%'
                   OR LOWER(company_name) LIKE '%%meta%%'
                   OR LOWER(industry) LIKE '%%data center%%'
                   OR LOWER(industry) LIKE '%%cloud%%'
                GROUP BY state, county
            ) id2 ON bp.state = id2.state AND bp.county_name ILIKE '%%' || id2.county || '%%'
            {state_filter}
            ORDER BY bp.county_fips
        """)

        try:
            result = self.db.execute(query, params)
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.warning(f"County data fetch failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return []

    def _compute_permit_velocity(self, counties: List[Dict]) -> List[float]:
        """Percentile rank based on permits_per_10k_pop + YoY growth."""
        raw = []
        for c in counties:
            ppop = float(c.get("permits_per_10k_pop") or 0)
            yoy = float(c.get("yoy_growth_pct") or 0)
            # Combine: 70% level, 30% growth
            raw.append(ppop * 0.7 + max(yoy, 0) * 0.3)
        return self._percentile_rank(raw)

    def _compute_jurisdictional_simplicity(self, counties: List[Dict]) -> List[float]:
        """Inverted percentile: fewer govts per capita = higher score."""
        raw = [float(c.get("govts_per_10k_pop") or 50) for c in counties]
        return self._inverted_percentile_rank(raw)

    def _compute_energy_siting_friendliness(self, counties: List[Dict]) -> List[float]:
        """Score based on state-level DC incentive programs."""
        raw = [float(c.get("dc_incentive_programs") or 0) for c in counties]
        if all(v == 0 for v in raw):
            return [50.0] * len(raw)
        return self._percentile_rank(raw)

    def _compute_historical_dc_deals(self, counties: List[Dict]) -> List[float]:
        """Score based on past datacenter deals in county/state."""
        raw = [float(c.get("dc_disclosed_deals") or 0) for c in counties]
        if all(v == 0 for v in raw):
            return [50.0] * len(raw)
        return self._percentile_rank(raw)

    def _bulk_save(self, records: List[Dict]) -> None:
        """Upsert scored records."""
        if not records:
            return

        try:
            for rec in records:
                self.db.execute(
                    text("""
                        INSERT INTO county_regulatory_scores (
                            county_fips, county_name, state, score_date,
                            overall_score, grade, national_rank, state_rank,
                            permit_velocity_score, jurisdictional_simplicity_score,
                            energy_siting_score, historical_dc_deals_score,
                            permits_per_10k_pop, permit_yoy_growth_pct,
                            total_govt_units, govts_per_10k_pop,
                            dc_incentive_programs, dc_disclosed_deals,
                            model_version
                        ) VALUES (
                            :county_fips, :county_name, :state, :score_date,
                            :overall_score, :grade, :national_rank, :state_rank,
                            :permit_velocity_score, :jurisdictional_simplicity_score,
                            :energy_siting_score, :historical_dc_deals_score,
                            :permits_per_10k_pop, :permit_yoy_growth_pct,
                            :total_govt_units, :govts_per_10k_pop,
                            :dc_incentive_programs, :dc_disclosed_deals,
                            :model_version
                        )
                        ON CONFLICT (county_fips, score_date) DO UPDATE SET
                            county_name = EXCLUDED.county_name,
                            state = EXCLUDED.state,
                            overall_score = EXCLUDED.overall_score,
                            grade = EXCLUDED.grade,
                            national_rank = EXCLUDED.national_rank,
                            state_rank = EXCLUDED.state_rank,
                            permit_velocity_score = EXCLUDED.permit_velocity_score,
                            jurisdictional_simplicity_score = EXCLUDED.jurisdictional_simplicity_score,
                            energy_siting_score = EXCLUDED.energy_siting_score,
                            historical_dc_deals_score = EXCLUDED.historical_dc_deals_score,
                            permits_per_10k_pop = EXCLUDED.permits_per_10k_pop,
                            permit_yoy_growth_pct = EXCLUDED.permit_yoy_growth_pct,
                            total_govt_units = EXCLUDED.total_govt_units,
                            govts_per_10k_pop = EXCLUDED.govts_per_10k_pop,
                            dc_incentive_programs = EXCLUDED.dc_incentive_programs,
                            dc_disclosed_deals = EXCLUDED.dc_disclosed_deals,
                            model_version = EXCLUDED.model_version
                    """),
                    rec,
                )
            self.db.commit()
            logger.info(f"Saved {len(records)} county regulatory scores")
        except Exception as e:
            logger.error(f"Failed to save county scores: {e}")
            self.db.rollback()

    def _build_summary(
        self, records: List[Dict], state: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build summary from scored records."""
        if state:
            records = [r for r in records if r.get("state") == state.upper()]

        grade_dist: Dict[str, int] = {}
        for rec in records:
            g = rec["grade"]
            grade_dist[g] = grade_dist.get(g, 0) + 1

        top_10 = records[:10]

        return {
            "total_counties": len(records),
            "grade_distribution": grade_dist,
            "top_10": [
                {
                    "county_fips": r["county_fips"],
                    "county_name": r["county_name"],
                    "state": r["state"],
                    "overall_score": r["overall_score"],
                    "grade": r["grade"],
                    "national_rank": r["national_rank"],
                }
                for r in top_10
            ],
            "model_version": MODEL_VERSION,
        }

    def _load_summary(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Load existing scores as summary."""
        state_filter = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            result = self.db.execute(
                text(f"""
                    SELECT county_fips, county_name, state, overall_score, grade,
                           national_rank
                    FROM county_regulatory_scores
                    WHERE score_date = (SELECT MAX(score_date) FROM county_regulatory_scores)
                    {state_filter}
                    ORDER BY national_rank
                """),
                params,
            )
            rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
            return self._build_summary(rows, state)
        except Exception:
            return {"total_counties": 0, "grade_distribution": {}}

    def get_county_score(self, county_fips: str) -> Optional[Dict[str, Any]]:
        """Get detailed score for a single county."""
        try:
            result = self.db.execute(
                text("""
                    SELECT * FROM county_regulatory_scores
                    WHERE county_fips = :fips
                    ORDER BY score_date DESC
                    LIMIT 1
                """),
                {"fips": county_fips},
            )
            row = result.fetchone()
            if not row:
                return None
            return dict(zip(result.keys(), row))
        except Exception:
            return None

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "County Regulatory Speed Score ranks US counties by how "
                "quickly and easily datacenter projects can get approved. "
                "Higher scores indicate faster, more DC-friendly jurisdictions."
            ),
            "weights": WEIGHTS,
            "grade_thresholds": {
                grade: threshold for threshold, grade in GRADE_THRESHOLDS
            },
            "factors": FACTOR_DOCUMENTATION,
        }
