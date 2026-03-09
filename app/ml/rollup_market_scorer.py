"""
Roll-Up Market Scorer — rank counties by roll-up attractiveness.

Given a NAICS code, scores counties using Census CBP data (fragmentation,
market size), IRS SOI income (affluence), and BLS employment data (labor).
Follows the ZipMedSpaScorer percentile-rank + weighted-composite pattern.
"""

import logging
import math
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.rollup_intel.metadata import (
    generate_create_rollup_scores_sql,
    NAICS_DESCRIPTIONS,
    MODEL_VERSION,
    ROLLUP_WEIGHTS,
    GRADE_THRESHOLDS,
)

logger = logging.getLogger(__name__)


class RollupMarketScorer:
    """Score and rank counties by roll-up attractiveness for a NAICS industry."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        from app.core.database import get_engine
        try:
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.execute(generate_create_rollup_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"Rollup scores table creation warning: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _percentile_rank(values: List[float]) -> List[float]:
        """Return percentile ranks (0-100) for a list of values."""
        n = len(values)
        if n == 0:
            return []
        indexed = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        for rank_pos, original_idx in enumerate(indexed):
            ranks[original_idx] = (rank_pos / (n - 1)) * 100.0 if n > 1 else 50.0
        return ranks

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _fetch_cbp_data(
        self, naics_code: str, year: int, state: Optional[str] = None
    ) -> List[Dict]:
        """Get CBP records from census_cbp cache."""
        where = ["naics_code = :naics", "year = :year", "establishments > 0"]
        params: Dict[str, Any] = {"naics": naics_code, "year": year}

        if state:
            where.append("state_fips = :state")
            params["state"] = state

        query = text(f"""
            SELECT county_fips, state_fips, geo_name,
                   establishments, employees, annual_payroll_thousands,
                   avg_employees_per_estab, small_biz_pct, hhi
            FROM census_cbp
            WHERE {" AND ".join(where)}
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching CBP data: {e}")
            self.db.rollback()
            return []

    def _fetch_affluence_data(self) -> Dict[str, Dict]:
        """Get county-level IRS SOI income data, keyed by county_fips."""
        query = text("""
            SELECT county_fips,
                   SUM(num_returns) AS total_returns,
                   SUM(total_agi) AS total_agi,
                   SUM(CASE WHEN agi_class IN ('5','6')
                       THEN num_returns ELSE 0 END) AS returns_100k_plus
            FROM irs_soi_county_income
            GROUP BY county_fips
        """)

        try:
            rows = self.db.execute(query).mappings().fetchall()
        except Exception as e:
            logger.warning(f"IRS SOI data not available: {e}")
            self.db.rollback()
            return {}

        result = {}
        for r in rows:
            fips = r["county_fips"]
            total = int(r["total_returns"]) if r["total_returns"] else 0
            if total == 0:
                continue
            result[fips] = {
                "total_returns": total,
                "avg_agi": float(r["total_agi"]) / total if r["total_agi"] else 0,
                "pct_returns_100k_plus": (
                    (int(r["returns_100k_plus"]) / total) if r["returns_100k_plus"] else 0
                ),
            }
        return result

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score_markets(
        self,
        naics_code: str,
        year: int = 2021,
        state: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Score all counties for roll-up attractiveness.

        Args:
            naics_code: NAICS industry code
            year: CBP data year
            state: Optional 2-digit state FIPS filter
            force: Re-score even if cached
        """
        score_date = date.today()
        naics_desc = NAICS_DESCRIPTIONS.get(naics_code, naics_code)

        # Check cache
        if not force:
            cached = self._count_cached_scores(naics_code, score_date, state)
            if cached > 0:
                return {
                    "source": "cache",
                    "naics_code": naics_code,
                    "total_scored": cached,
                    "note": "Use force=true to re-score",
                }

        # Fetch data
        cbp_data = self._fetch_cbp_data(naics_code, year, state)
        if not cbp_data:
            return {
                "error": "No CBP data found. Run CBP collection first.",
                "naics_code": naics_code,
                "year": year,
            }

        affluence = self._fetch_affluence_data()

        n = len(cbp_data)
        logger.info(f"Scoring {n} counties for NAICS {naics_code}...")

        # --- Compute raw signals ---
        frag_raw = []
        market_raw = []
        affluence_raw = []
        growth_raw = []
        labor_raw = []

        for c in cbp_data:
            estab = c["establishments"] or 0
            emp = c["employees"] or 0
            payroll = c["annual_payroll_thousands"] or 0
            hhi = float(c["hhi"]) if c["hhi"] is not None else 0.5
            small_pct = float(c["small_biz_pct"]) if c["small_biz_pct"] is not None else 0.5
            avg_size = float(c["avg_employees_per_estab"]) if c["avg_employees_per_estab"] else 10

            # Fragmentation: log(estab) + (1-HHI) + small_biz_pct + 1/avg_size
            frag = (
                math.log(max(estab, 1))
                + (1 - hhi)
                + small_pct
                + (1 / max(avg_size, 1))
            )
            frag_raw.append(frag)

            # Market size: total_employees * avg_payroll_per_employee
            avg_pay = (payroll * 1000 / emp) if emp > 0 else 0
            market_raw.append(emp * avg_pay)

            # Affluence: from IRS SOI
            fips = c["county_fips"]
            aff_data = affluence.get(fips, {})
            affluence_raw.append(aff_data.get("avg_agi", 0))

            # Growth proxy: employment density (emp / estab ratio)
            growth_raw.append(emp / max(estab, 1))

            # Labor: county total employment relative to industry
            labor_raw.append(emp)

        # --- Percentile rank each dimension ---
        frag_pct = self._percentile_rank(frag_raw)
        market_pct = self._percentile_rank(market_raw)
        affluence_pct = self._percentile_rank(affluence_raw)
        growth_pct = self._percentile_rank(growth_raw)
        labor_pct = self._percentile_rank(labor_raw)

        # --- Weighted composite ---
        records = []
        for i, c in enumerate(cbp_data):
            f_score = round(frag_pct[i], 2)
            m_score = round(market_pct[i], 2)
            a_score = round(affluence_pct[i], 2)
            g_score = round(growth_pct[i], 2)
            l_score = round(labor_pct[i], 2)

            overall = (
                f_score * ROLLUP_WEIGHTS["fragmentation"]
                + m_score * ROLLUP_WEIGHTS["market_size"]
                + a_score * ROLLUP_WEIGHTS["affluence"]
                + g_score * ROLLUP_WEIGHTS["growth"]
                + l_score * ROLLUP_WEIGHTS["labor"]
            )
            overall = max(0.0, min(100.0, round(overall, 2)))
            grade = self._get_grade(overall)

            fips = c["county_fips"]
            aff_data = affluence.get(fips, {})

            records.append({
                "naics_code": naics_code,
                "naics_description": naics_desc,
                "county_fips": fips,
                "state_fips": c["state_fips"],
                "geo_name": c["geo_name"],
                "score_date": score_date,
                "data_year": year,
                "overall_score": overall,
                "grade": grade,
                "fragmentation_score": f_score,
                "market_size_score": m_score,
                "affluence_score": a_score,
                "growth_score": g_score,
                "labor_score": l_score,
                "establishment_count": c["establishments"],
                "hhi": c["hhi"],
                "small_biz_pct": c["small_biz_pct"],
                "avg_estab_size": c["avg_employees_per_estab"],
                "total_employees": c["employees"],
                "total_payroll_thousands": c["annual_payroll_thousands"],
                "avg_agi": round(aff_data.get("avg_agi", 0), 2) if aff_data else None,
                "pct_returns_100k_plus": (
                    round(aff_data.get("pct_returns_100k_plus", 0), 4)
                    if aff_data else None
                ),
                "total_returns": aff_data.get("total_returns"),
                "model_version": MODEL_VERSION,
            })

        # Compute rankings
        records.sort(key=lambda r: r["overall_score"], reverse=True)
        for i, rec in enumerate(records, 1):
            rec["national_rank"] = i

        # State rankings
        by_state: Dict[str, List] = {}
        for rec in records:
            by_state.setdefault(rec["state_fips"], []).append(rec)
        for state_recs in by_state.values():
            for i, rec in enumerate(state_recs, 1):
                rec["state_rank"] = i

        # Persist
        self._bulk_save(records)

        # Summary
        grade_dist = {}
        for rec in records:
            grade_dist[rec["grade"]] = grade_dist.get(rec["grade"], 0) + 1

        top_10 = records[:10]

        return {
            "naics_code": naics_code,
            "naics_description": naics_desc,
            "data_year": year,
            "total_scored": len(records),
            "grade_distribution": grade_dist,
            "weights": ROLLUP_WEIGHTS,
            "top_10": [
                {
                    "county_fips": r["county_fips"],
                    "geo_name": r["geo_name"],
                    "state_fips": r["state_fips"],
                    "overall_score": r["overall_score"],
                    "grade": r["grade"],
                    "establishment_count": r["establishment_count"],
                    "national_rank": r["national_rank"],
                }
                for r in top_10
            ],
        }

    # ------------------------------------------------------------------
    # Query scored markets
    # ------------------------------------------------------------------

    def get_rankings(
        self,
        naics_code: str,
        state: Optional[str] = None,
        grade: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get scored and ranked counties."""
        where = ["naics_code = :naics"]
        params: Dict[str, Any] = {
            "naics": naics_code, "lim": limit, "off": offset,
        }

        if state:
            where.append("state_fips = :state")
            params["state"] = state
        if grade:
            where.append("grade = :grade")
            params["grade"] = grade.upper()

        where_sql = " AND ".join(where)

        query = text(f"""
            SELECT * FROM rollup_market_scores
            WHERE {where_sql}
            ORDER BY overall_score DESC
            LIMIT :lim OFFSET :off
        """)
        count_query = text(f"""
            SELECT COUNT(*) FROM rollup_market_scores
            WHERE {where_sql}
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
            count_params = {k: v for k, v in params.items() if k not in ("lim", "off")}
            total = self.db.execute(count_query, count_params).scalar() or 0
        except Exception as e:
            logger.error(f"Error querying rankings: {e}")
            self.db.rollback()
            return {"error": str(e)}

        return {
            "naics_code": naics_code,
            "total_matching": total,
            "returned": len(rows),
            "filters": {"state": state, "grade": grade},
            "rankings": [dict(r) for r in rows],
        }

    def get_market(self, naics_code: str, county_fips: str) -> Dict[str, Any]:
        """Get detailed score for a specific county + NAICS."""
        query = text("""
            SELECT * FROM rollup_market_scores
            WHERE naics_code = :naics AND county_fips = :fips
            ORDER BY score_date DESC
            LIMIT 1
        """)
        try:
            row = self.db.execute(
                query, {"naics": naics_code, "fips": county_fips}
            ).mappings().fetchone()
        except Exception as e:
            logger.error(f"Error fetching market detail: {e}")
            self.db.rollback()
            return {"error": str(e)}

        if not row:
            return {
                "error": "No score found",
                "naics_code": naics_code,
                "county_fips": county_fips,
            }

        return dict(row)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _count_cached_scores(
        self, naics_code: str, score_date: date, state: Optional[str]
    ) -> int:
        where = "naics_code = :naics AND score_date = :sd"
        params: Dict[str, Any] = {"naics": naics_code, "sd": score_date}
        if state:
            where += " AND state_fips = :state"
            params["state"] = state
        try:
            return (
                self.db.execute(
                    text(f"SELECT COUNT(*) FROM rollup_market_scores WHERE {where}"),
                    params,
                ).scalar()
                or 0
            )
        except Exception:
            self.db.rollback()
            return 0

    def _bulk_save(self, records: List[Dict]) -> None:
        if not records:
            return

        upsert_sql = text("""
            INSERT INTO rollup_market_scores (
                naics_code, naics_description, county_fips, state_fips, geo_name,
                score_date, data_year, overall_score, grade,
                fragmentation_score, market_size_score, affluence_score,
                growth_score, labor_score,
                establishment_count, hhi, small_biz_pct, avg_estab_size,
                total_employees, total_payroll_thousands,
                avg_agi, pct_returns_100k_plus, total_returns,
                national_rank, state_rank, model_version
            ) VALUES (
                :naics_code, :naics_description, :county_fips, :state_fips, :geo_name,
                :score_date, :data_year, :overall_score, :grade,
                :fragmentation_score, :market_size_score, :affluence_score,
                :growth_score, :labor_score,
                :establishment_count, :hhi, :small_biz_pct, :avg_estab_size,
                :total_employees, :total_payroll_thousands,
                :avg_agi, :pct_returns_100k_plus, :total_returns,
                :national_rank, :state_rank, :model_version
            )
            ON CONFLICT (naics_code, county_fips, score_date) DO UPDATE SET
                naics_description = EXCLUDED.naics_description,
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                fragmentation_score = EXCLUDED.fragmentation_score,
                market_size_score = EXCLUDED.market_size_score,
                affluence_score = EXCLUDED.affluence_score,
                growth_score = EXCLUDED.growth_score,
                labor_score = EXCLUDED.labor_score,
                establishment_count = EXCLUDED.establishment_count,
                hhi = EXCLUDED.hhi,
                small_biz_pct = EXCLUDED.small_biz_pct,
                avg_estab_size = EXCLUDED.avg_estab_size,
                total_employees = EXCLUDED.total_employees,
                total_payroll_thousands = EXCLUDED.total_payroll_thousands,
                avg_agi = EXCLUDED.avg_agi,
                pct_returns_100k_plus = EXCLUDED.pct_returns_100k_plus,
                total_returns = EXCLUDED.total_returns,
                national_rank = EXCLUDED.national_rank,
                state_rank = EXCLUDED.state_rank,
                model_version = EXCLUDED.model_version
        """)

        batch_size = 500
        total_saved = 0
        try:
            for start in range(0, len(records), batch_size):
                batch = records[start:start + batch_size]
                for rec in batch:
                    self.db.execute(upsert_sql, rec)
                self.db.commit()
                total_saved += len(batch)
            logger.info(f"Saved {total_saved} rollup market scores")
        except Exception as e:
            logger.error(f"Error saving rollup scores: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # Methodology
    # ------------------------------------------------------------------

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Roll-Up Market Screener scores U.S. counties by their "
                "attractiveness for PE roll-up strategies in a given NAICS "
                "industry. Uses Census CBP (establishment counts, employment, "
                "payroll), IRS SOI income data (affluence), and derived "
                "fragmentation metrics."
            ),
            "sub_scores": [
                {
                    "name": "fragmentation",
                    "weight": ROLLUP_WEIGHTS["fragmentation"],
                    "description": (
                        "log(establishment_count) + (1-HHI) + small_biz_pct + "
                        "1/avg_size. Higher = more fragmented = better for roll-up."
                    ),
                },
                {
                    "name": "market_size",
                    "weight": ROLLUP_WEIGHTS["market_size"],
                    "description": (
                        "Total employees x avg payroll. Larger markets support "
                        "bigger platforms."
                    ),
                },
                {
                    "name": "affluence",
                    "weight": ROLLUP_WEIGHTS["affluence"],
                    "description": (
                        "Average AGI from IRS SOI county income data. Wealthier "
                        "counties = higher revenue potential per location."
                    ),
                },
                {
                    "name": "growth",
                    "weight": ROLLUP_WEIGHTS["growth"],
                    "description": (
                        "Employment density as growth proxy. Higher emp/estab "
                        "ratio signals larger, growing businesses."
                    ),
                },
                {
                    "name": "labor",
                    "weight": ROLLUP_WEIGHTS["labor"],
                    "description": (
                        "Total industry employment. Larger labor pool = easier "
                        "to staff post-acquisition."
                    ),
                },
            ],
            "grade_thresholds": {
                "A": ">=80 — Top-tier roll-up market",
                "B": ">=65 — Strong market",
                "C": ">=50 — Moderate potential",
                "D": ">=35 — Below-average",
                "F": "<35 — Weak roll-up market",
            },
            "data_sources": [
                "Census CBP (County Business Patterns)",
                "IRS SOI (Statistics of Income, county-level)",
            ],
        }
