"""
ZIP Med-Spa Revenue Potential Score — core scoring engine.

Ranks every US ZIP code by med-spa revenue potential using a weighted
composite of affluence density, discretionary wealth, market size,
professional density, and wealth concentration — all derived from
IRS SOI ZIP-level income and business data already in the database.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.zip_medspa_metadata import generate_create_zip_medspa_scores_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL_VERSION = "v1.0"

WEIGHTS = {
    "affluence_density": 0.30,
    "discretionary_wealth": 0.25,
    "market_size": 0.20,
    "professional_density": 0.15,
    "wealth_concentration": 0.10,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]


class ZipMedSpaScorer:
    """Compute med-spa revenue potential scores for US ZIP codes."""

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
                cursor.execute(generate_create_zip_medspa_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"ZIP med-spa table creation warning: {e}")

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
    # Data retrieval — batch (all ZIPs in one query)
    # ------------------------------------------------------------------

    def _fetch_all_zip_data(self, state: Optional[str] = None) -> List[Dict]:
        """Single aggregation query across IRS SOI income + business tables."""
        state_filter = "AND z.state_abbr = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        query = text(f"""
            SELECT
                z.zip_code,
                z.state_abbr,
                SUM(z.num_returns) AS total_returns,
                SUM(CASE WHEN z.agi_class IN ('5','6')
                    THEN z.num_returns ELSE 0 END) AS returns_100k_plus,
                SUM(CASE WHEN z.agi_class = '6'
                    THEN z.num_returns ELSE 0 END) AS returns_200k_plus,
                SUM(z.total_capital_gains) AS total_cap_gains,
                SUM(z.total_dividends) AS total_dividends,
                SUM(z.total_agi) AS total_agi,
                SUM(CASE WHEN z.agi_class = '6'
                    THEN z.num_joint_returns ELSE 0 END) AS joint_top,
                SUM(CASE WHEN z.agi_class = '6'
                    THEN z.num_returns ELSE 0 END) AS total_top,
                SUM(z.total_amt) AS total_amt,
                b.num_partnership_income,
                b.num_with_business_income,
                b.num_returns AS biz_total_returns
            FROM irs_soi_zip_income z
            LEFT JOIN irs_soi_business_income b
                ON z.zip_code = b.zip_code
            WHERE 1=1 {state_filter}
            GROUP BY z.zip_code, z.state_abbr,
                     b.num_partnership_income,
                     b.num_with_business_income,
                     b.num_returns
        """)

        try:
            rows = self.db.execute(query, params).fetchall()
        except Exception as e:
            logger.error(f"Error fetching ZIP data: {e}")
            self.db.rollback()
            return []

        results = []
        for r in rows:
            total_returns = int(r[2]) if r[2] else 0
            if total_returns == 0:
                continue

            returns_100k = int(r[3]) if r[3] else 0
            returns_200k = int(r[4]) if r[4] else 0
            total_cap_gains = float(r[5]) if r[5] else 0
            total_dividends = float(r[6]) if r[6] else 0
            total_agi = float(r[7]) if r[7] else 0
            joint_top = int(r[8]) if r[8] else 0
            total_top = int(r[9]) if r[9] else 0
            total_amt = float(r[10]) if r[10] else 0
            num_partnership = int(r[11]) if r[11] else 0
            num_biz_income = int(r[12]) if r[12] else 0
            biz_total_returns = int(r[13]) if r[13] else 0

            has_biz_data = biz_total_returns > 0

            results.append({
                "zip_code": r[0],
                "state_abbr": r[1],
                "total_returns": total_returns,
                # Affluence density raw
                "pct_returns_100k_plus": returns_100k / total_returns,
                "pct_returns_200k_plus": returns_200k / total_returns,
                # Discretionary wealth raw
                "cap_gains_per_return": total_cap_gains / total_returns,
                "dividends_per_return": total_dividends / total_returns,
                # Market size raw
                "avg_agi": total_agi / total_returns,
                "total_market_income": total_agi,
                # Professional density raw
                "partnership_density": (
                    num_partnership / biz_total_returns if biz_total_returns > 0 else 0
                ),
                "self_employment_density": (
                    num_biz_income / biz_total_returns if biz_total_returns > 0 else 0
                ),
                # Wealth concentration raw
                "joint_pct_top_bracket": (
                    joint_top / total_top if total_top > 0 else 0
                ),
                "amt_per_return": total_amt / total_returns,
                # Confidence
                "has_biz_data": has_biz_data,
            })

        return results

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score_all_zips(
        self,
        force: bool = False,
        state: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Batch-score all ZIPs. Returns summary with grade distribution and top 10."""
        score_date = date.today()

        zip_data = self._fetch_all_zip_data(state=state)
        if not zip_data:
            return {"error": "No IRS SOI data found", "total_scored": 0}

        n = len(zip_data)
        logger.info(f"Scoring {n} ZIP codes for med-spa potential...")

        # --- Compute raw composite signals per ZIP ---
        affluence_raw = []
        wealth_raw = []
        market_raw = []
        professional_raw = []
        concentration_raw = []

        for z in zip_data:
            # Affluence: blend of 100K+ pct (60%) and 200K+ pct (40%)
            affluence_raw.append(
                z["pct_returns_100k_plus"] * 0.6
                + z["pct_returns_200k_plus"] * 0.4
            )
            # Discretionary wealth: cap gains + dividends per return
            wealth_raw.append(
                z["cap_gains_per_return"] + z["dividends_per_return"]
            )
            # Market size: total_returns * avg_agi (volume × quality)
            market_raw.append(
                z["total_returns"] * z["avg_agi"]
            )
            # Professional density: partnership + self-employment
            professional_raw.append(
                z["partnership_density"] + z["self_employment_density"]
            )
            # Wealth concentration: joint top bracket pct + AMT per return
            # Normalize AMT contribution by blending (AMT is in thousands)
            concentration_raw.append(
                z["joint_pct_top_bracket"] * 50
                + z["amt_per_return"] * 50
            )

        # --- Percentile rank each dimension ---
        affluence_pct = self._percentile_rank(affluence_raw)
        wealth_pct = self._percentile_rank(wealth_raw)
        market_pct = self._percentile_rank(market_raw)
        professional_pct = self._percentile_rank(professional_raw)
        concentration_pct = self._percentile_rank(concentration_raw)

        # --- Weighted composite + grade + persist ---
        records = []
        for i, z in enumerate(zip_data):
            a_score = round(affluence_pct[i], 2)
            w_score = round(wealth_pct[i], 2)
            m_score = round(market_pct[i], 2)
            p_score = round(professional_pct[i], 2)
            c_score = round(concentration_pct[i], 2)

            overall = (
                a_score * WEIGHTS["affluence_density"]
                + w_score * WEIGHTS["discretionary_wealth"]
                + m_score * WEIGHTS["market_size"]
                + p_score * WEIGHTS["professional_density"]
                + c_score * WEIGHTS["wealth_concentration"]
            )
            overall = max(0.0, min(100.0, round(overall, 2)))
            grade = self._get_grade(overall)
            confidence = 1.0 if z["has_biz_data"] else 0.7

            records.append({
                "zip_code": z["zip_code"],
                "state_abbr": z["state_abbr"],
                "score_date": score_date,
                "overall_score": overall,
                "grade": grade,
                "confidence": confidence,
                "affluence_density_score": a_score,
                "discretionary_wealth_score": w_score,
                "market_size_score": m_score,
                "professional_density_score": p_score,
                "wealth_concentration_score": c_score,
                "pct_returns_100k_plus": round(z["pct_returns_100k_plus"], 4),
                "pct_returns_200k_plus": round(z["pct_returns_200k_plus"], 4),
                "avg_agi": round(z["avg_agi"], 2),
                "total_returns": z["total_returns"],
                "cap_gains_per_return": round(z["cap_gains_per_return"], 2),
                "dividends_per_return": round(z["dividends_per_return"], 2),
                "total_market_income": round(z["total_market_income"], 2),
                "partnership_density": round(z["partnership_density"], 4),
                "self_employment_density": round(z["self_employment_density"], 4),
                "joint_pct_top_bracket": round(z["joint_pct_top_bracket"], 4),
                "amt_per_return": round(z["amt_per_return"], 2),
                "model_version": MODEL_VERSION,
            })

        # Bulk upsert
        self._bulk_save(records)

        # Build summary
        grade_dist = {}
        for rec in records:
            grade_dist[rec["grade"]] = grade_dist.get(rec["grade"], 0) + 1

        top_10 = sorted(records, key=lambda r: r["overall_score"], reverse=True)[:10]

        # State averages
        state_totals: Dict[str, List[float]] = {}
        for rec in records:
            st = rec["state_abbr"] or "??"
            state_totals.setdefault(st, []).append(rec["overall_score"])
        by_state_avg = {
            st: round(sum(scores) / len(scores), 2)
            for st, scores in state_totals.items()
        }

        logger.info(
            f"Scored {len(records)} ZIPs. "
            f"Grade distribution: {grade_dist}"
        )

        return {
            "total_scored": len(records),
            "grade_distribution": grade_dist,
            "top_10": [
                {
                    "zip_code": r["zip_code"],
                    "state_abbr": r["state_abbr"],
                    "overall_score": r["overall_score"],
                    "grade": r["grade"],
                }
                for r in top_10
            ],
            "by_state_avg": dict(
                sorted(by_state_avg.items(), key=lambda x: x[1], reverse=True)
            ),
        }

    def score_zip(
        self, zip_code: str, force: bool = False
    ) -> Dict[str, Any]:
        """Score a single ZIP code. Uses cached result if available."""
        score_date = date.today()

        if not force:
            cached = self._get_cached_score(zip_code, score_date)
            if cached:
                return cached

        # For single-ZIP, we still need the full percentile context.
        # Score all ZIPs (uses cache on subsequent calls) then return this one.
        self.score_all_zips(force=force)
        result = self._get_cached_score(zip_code, score_date)
        if result:
            return result
        return {"error": f"ZIP {zip_code} not found in IRS SOI data"}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _get_cached_score(
        self, zip_code: str, score_date: date
    ) -> Optional[Dict[str, Any]]:
        query = text("""
            SELECT * FROM zip_medspa_scores
            WHERE zip_code = :zip AND score_date = :sd
              AND model_version = :ver
            LIMIT 1
        """)
        try:
            row = self.db.execute(
                query,
                {"zip": zip_code, "sd": score_date, "ver": MODEL_VERSION},
            ).mappings().fetchone()
            if row:
                result = dict(row)
                result["cached"] = True
                return result
        except Exception:
            self.db.rollback()
        return None

    def _bulk_save(self, records: List[Dict]) -> None:
        """Bulk upsert scored ZIP records."""
        if not records:
            return

        upsert_sql = text("""
            INSERT INTO zip_medspa_scores (
                zip_code, state_abbr, score_date,
                overall_score, grade, confidence,
                affluence_density_score, discretionary_wealth_score,
                market_size_score, professional_density_score,
                wealth_concentration_score,
                pct_returns_100k_plus, pct_returns_200k_plus,
                avg_agi, total_returns,
                cap_gains_per_return, dividends_per_return,
                total_market_income,
                partnership_density, self_employment_density,
                joint_pct_top_bracket, amt_per_return,
                model_version
            ) VALUES (
                :zip_code, :state_abbr, :score_date,
                :overall_score, :grade, :confidence,
                :affluence_density_score, :discretionary_wealth_score,
                :market_size_score, :professional_density_score,
                :wealth_concentration_score,
                :pct_returns_100k_plus, :pct_returns_200k_plus,
                :avg_agi, :total_returns,
                :cap_gains_per_return, :dividends_per_return,
                :total_market_income,
                :partnership_density, :self_employment_density,
                :joint_pct_top_bracket, :amt_per_return,
                :model_version
            )
            ON CONFLICT (zip_code, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                confidence = EXCLUDED.confidence,
                affluence_density_score = EXCLUDED.affluence_density_score,
                discretionary_wealth_score = EXCLUDED.discretionary_wealth_score,
                market_size_score = EXCLUDED.market_size_score,
                professional_density_score = EXCLUDED.professional_density_score,
                wealth_concentration_score = EXCLUDED.wealth_concentration_score,
                pct_returns_100k_plus = EXCLUDED.pct_returns_100k_plus,
                pct_returns_200k_plus = EXCLUDED.pct_returns_200k_plus,
                avg_agi = EXCLUDED.avg_agi,
                total_returns = EXCLUDED.total_returns,
                cap_gains_per_return = EXCLUDED.cap_gains_per_return,
                dividends_per_return = EXCLUDED.dividends_per_return,
                total_market_income = EXCLUDED.total_market_income,
                partnership_density = EXCLUDED.partnership_density,
                self_employment_density = EXCLUDED.self_employment_density,
                joint_pct_top_bracket = EXCLUDED.joint_pct_top_bracket,
                amt_per_return = EXCLUDED.amt_per_return,
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
                if total_saved % 5000 == 0:
                    logger.info(f"  ...saved {total_saved}/{len(records)} ZIP scores")
            logger.info(f"Saved {total_saved} ZIP med-spa scores")
        except Exception as e:
            logger.error(f"Error bulk-saving ZIP scores: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # Public API — methodology
    # ------------------------------------------------------------------

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "ZIP Med-Spa Revenue Potential Score ranks US ZIP codes by "
                "their predicted consumer spend potential for aesthetics and "
                "med-spa services. Uses IRS SOI ZIP-level income distribution "
                "data (tax year 2021, 27,604 ZIPs). Each sub-score is a "
                "percentile rank (0-100) across all ZIPs."
            ),
            "use_case": (
                "Identify the best US ZIP codes for acquiring aesthetics/"
                "med-spa businesses as part of a PE roll-up strategy."
            ),
            "sub_scores": [
                {
                    "name": "affluence_density",
                    "weight": WEIGHTS["affluence_density"],
                    "description": (
                        "% of tax returns in $100K+ brackets (AGI classes 5+6) "
                        "and $200K+ bracket (class 6). Blend: 60% $100K+, "
                        "40% $200K+."
                    ),
                    "source": "irs_soi_zip_income",
                },
                {
                    "name": "discretionary_wealth",
                    "weight": WEIGHTS["discretionary_wealth"],
                    "description": (
                        "Capital gains + dividends per return. Investment "
                        "income signals disposable spend beyond salary."
                    ),
                    "source": "irs_soi_zip_income",
                },
                {
                    "name": "market_size",
                    "weight": WEIGHTS["market_size"],
                    "description": (
                        "Total returns x average AGI. A small rich ZIP isn't "
                        "a market — this captures volume x quality."
                    ),
                    "source": "irs_soi_zip_income",
                },
                {
                    "name": "professional_density",
                    "weight": WEIGHTS["professional_density"],
                    "description": (
                        "Partnership income density + self-employment density "
                        "per return. Professionals = high-propensity consumers "
                        "for aesthetics services."
                    ),
                    "source": "irs_soi_business_income",
                },
                {
                    "name": "wealth_concentration",
                    "weight": WEIGHTS["wealth_concentration"],
                    "description": (
                        "Joint returns in top bracket + AMT per return. "
                        "Ultra-high-net-worth household signal."
                    ),
                    "source": "irs_soi_zip_income",
                },
            ],
            "grade_thresholds": {
                "A": ">=80 — Top-tier med-spa market",
                "B": ">=65 — Strong market potential",
                "C": ">=50 — Moderate potential",
                "D": ">=35 — Below-average potential",
                "F": "<35 — Weak market for med-spa",
            },
            "data_source": "IRS Statistics of Income (SOI), ZIP-level, tax year 2021",
            "total_zips": "~27,604 (all US ZIPs with IRS data)",
        }
