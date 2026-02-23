"""
Hiring Velocity Score — core scoring engine.

Cross-references job posting snapshots with BLS CES employment
baselines to produce a 0-100 velocity score per company.

Follows the pattern in app/ml/company_scorer.py.
"""

import json
import logging
import math
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.job_postings.naics_crosswalk import (
    BLS_CES_FALLBACK,
    get_bls_series_for_company,
    get_series_label,
)
from app.sources.job_postings.velocity_metadata import (
    generate_create_hiring_velocity_scores_sql,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL_VERSION = "v1.0"

WEIGHTS = {
    "posting_growth": 0.30,
    "industry_relative": 0.30,
    "momentum": 0.20,
    "seniority_signal": 0.10,
    "dept_diversity": 0.10,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

# Guardrails
MIN_SNAPSHOT_DAYS = 14
MIN_POSTINGS_FOR_SCORE = 5


class HiringVelocityScorer:
    """Compute hiring velocity scores for tracked companies."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        try:
            self.db.execute(text(generate_create_hiring_velocity_scores_sql()))
            self.db.commit()
        except Exception as e:
            logger.warning(f"Velocity table creation warning: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_score(value: float, min_val: float, max_val: float) -> float:
        if max_val == min_val:
            return 50.0
        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return max(0.0, min(100.0, normalized))

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_snapshot_series(
        self, company_id: int, days: int = 90
    ) -> List[Dict[str, Any]]:
        """Fetch daily snapshot time-series for a company."""
        query = text("""
            SELECT snapshot_date, total_open, new_postings, closed_postings,
                   by_department, by_seniority
            FROM job_posting_snapshots
            WHERE company_id = :cid
              AND snapshot_date >= CURRENT_DATE - :days
            ORDER BY snapshot_date ASC
        """)
        try:
            rows = self.db.execute(
                query, {"cid": company_id, "days": days}
            ).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"Error fetching snapshots for company {company_id}: {e}")
            return []

    def _get_bls_baseline(self, series_id: str) -> Dict[str, Any]:
        """
        Fetch recent BLS CES employment data and compute YoY / MoM growth.

        Returns dict with keys: yoy_growth_pct, mom_growth_pct, available.
        """
        query = text("""
            SELECT year, period, value
            FROM bls_ces_employment
            WHERE series_id = :sid
            ORDER BY year DESC, period DESC
            LIMIT 24
        """)
        try:
            rows = self.db.execute(
                query, {"sid": series_id}
            ).mappings().fetchall()
            if len(rows) < 2:
                return {"available": False, "yoy_growth_pct": 0, "mom_growth_pct": 0}

            latest = float(rows[0]["value"])
            prev_month = float(rows[1]["value"]) if len(rows) > 1 else latest

            # Find same month, previous year
            latest_year = rows[0]["year"]
            latest_period = rows[0]["period"]
            prev_year_val = None
            for r in rows:
                if r["year"] == latest_year - 1 and r["period"] == latest_period:
                    prev_year_val = float(r["value"])
                    break

            mom = ((latest - prev_month) / prev_month * 100) if prev_month else 0
            yoy = (
                ((latest - prev_year_val) / prev_year_val * 100)
                if prev_year_val
                else 0
            )
            return {
                "available": True,
                "yoy_growth_pct": round(yoy, 4),
                "mom_growth_pct": round(mom, 4),
            }
        except Exception as e:
            logger.warning(f"Error fetching BLS baseline for {series_id}: {e}")
            return {"available": False, "yoy_growth_pct": 0, "mom_growth_pct": 0}

    def _get_company_naics(self, company_id: int) -> Optional[str]:
        """Lookup NAICS code from industrial_companies."""
        query = text("""
            SELECT naics_code FROM industrial_companies WHERE id = :cid
        """)
        try:
            row = self.db.execute(query, {"cid": company_id}).mappings().fetchone()
            return row["naics_code"] if row and row["naics_code"] else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Sub-score calculations (each returns score, metadata dict)
    # ------------------------------------------------------------------

    def _calc_posting_growth(
        self, snapshots: List[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        WoW/MoM % change in total_open, blended 60/40.
        Positive growth → high score, contraction → low score.
        """
        if len(snapshots) < 2:
            return 50.0, {"wow_pct": 0, "mom_pct": 0, "note": "insufficient data"}

        latest_total = snapshots[-1]["total_open"] or 0

        # Week-over-week (compare latest vs 7 days ago)
        wow_pct = 0.0
        if len(snapshots) >= 7:
            week_ago_total = snapshots[-7]["total_open"] or 0
            if week_ago_total > 0:
                wow_pct = (latest_total - week_ago_total) / week_ago_total * 100

        # Month-over-month (compare latest vs 30 days ago or earliest)
        mom_pct = 0.0
        lookback = min(30, len(snapshots) - 1)
        month_ago_total = snapshots[-1 - lookback]["total_open"] or 0
        if month_ago_total > 0:
            mom_pct = (latest_total - month_ago_total) / month_ago_total * 100

        # Blend: 60% WoW, 40% MoM
        blended = wow_pct * 0.6 + mom_pct * 0.4

        # Normalize: -50% contraction → 0, 0% = 50, +50% growth → 100
        score = self._normalize_score(blended, -50, 50)

        return score, {
            "wow_pct": round(wow_pct, 4),
            "mom_pct": round(mom_pct, 4),
            "blended_pct": round(blended, 4),
        }

    def _calc_industry_relative(
        self,
        snapshots: List[Dict],
        bls_data: Dict[str, Any],
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Company posting growth vs BLS industry employment growth.
        Outperforming the sector → high score.
        """
        if not bls_data.get("available"):
            return 50.0, {"note": "BLS data unavailable, neutral score applied"}

        # Company MoM posting growth
        if len(snapshots) < 2:
            company_mom = 0.0
        else:
            lookback = min(30, len(snapshots) - 1)
            old_total = snapshots[-1 - lookback]["total_open"] or 0
            new_total = snapshots[-1]["total_open"] or 0
            company_mom = (
                ((new_total - old_total) / old_total * 100) if old_total > 0 else 0
            )

        bls_mom = bls_data["mom_growth_pct"]
        relative_rate = company_mom - bls_mom

        # Normalize: -20pp underperform → 0, 0 = 50, +20pp outperform → 100
        score = self._normalize_score(relative_rate, -20, 20)

        return score, {
            "company_mom_pct": round(company_mom, 4),
            "bls_mom_pct": bls_mom,
            "relative_rate": round(relative_rate, 4),
        }

    def _calc_momentum(self, snapshots: List[Dict]) -> Tuple[float, Dict[str, Any]]:
        """
        Acceleration (2nd derivative) — is growth speeding up or slowing?
        Uses weekly totals to smooth noise.
        """
        if len(snapshots) < 21:
            return 50.0, {"note": "insufficient data for momentum"}

        # Compute weekly averages
        weekly_totals = []
        for i in range(0, len(snapshots) - 6, 7):
            week_slice = snapshots[i : i + 7]
            avg = sum(s["total_open"] or 0 for s in week_slice) / len(week_slice)
            weekly_totals.append(avg)

        if len(weekly_totals) < 3:
            return 50.0, {"note": "insufficient weekly data"}

        # Weekly growth rates
        growth_rates = []
        for i in range(1, len(weekly_totals)):
            prev = weekly_totals[i - 1]
            if prev > 0:
                growth_rates.append(
                    (weekly_totals[i] - prev) / prev * 100
                )

        if len(growth_rates) < 2:
            return 50.0, {"note": "insufficient growth rate data"}

        # Acceleration = change in growth rate (latest minus prior average)
        recent_rate = growth_rates[-1]
        prior_avg = sum(growth_rates[:-1]) / len(growth_rates[:-1])
        acceleration = recent_rate - prior_avg

        # Normalize: decelerating -10pp → 0, steady = 50, accelerating +10pp → 100
        score = self._normalize_score(acceleration, -10, 10)

        return score, {
            "acceleration": round(acceleration, 4),
            "recent_weekly_growth_pct": round(recent_rate, 4),
            "prior_avg_growth_pct": round(prior_avg, 4),
        }

    def _calc_seniority_signal(
        self, snapshots: List[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Shannon entropy of seniority mix + senior role share shift.
        Higher entropy (diverse hiring across levels) + growing senior
        share → expansion signal.
        """
        latest = snapshots[-1] if snapshots else {}
        seniority_dist = latest.get("by_seniority") or {}
        if isinstance(seniority_dist, str):
            try:
                seniority_dist = json.loads(seniority_dist)
            except (json.JSONDecodeError, TypeError):
                seniority_dist = {}

        if not seniority_dist:
            return 50.0, {"note": "no seniority data"}

        total = sum(seniority_dist.values())
        if total == 0:
            return 50.0, {"note": "empty seniority distribution"}

        # Shannon entropy
        entropy = 0.0
        for count in seniority_dist.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)

        # Max entropy for comparison (uniform across all categories)
        n_categories = len(seniority_dist)
        max_entropy = math.log2(n_categories) if n_categories > 1 else 1.0
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0

        # Senior role share (director+, vp, c_suite)
        senior_keys = {"senior", "lead", "director", "vp", "c_suite"}
        senior_count = sum(
            v for k, v in seniority_dist.items() if k in senior_keys
        )
        senior_share = senior_count / total if total > 0 else 0

        # Combined: 70% entropy diversity + 30% senior share
        raw = normalized_entropy * 0.7 + senior_share * 0.3
        score = self._normalize_score(raw, 0, 1)

        return score, {
            "entropy": round(entropy, 4),
            "normalized_entropy": round(normalized_entropy, 4),
            "senior_share": round(senior_share, 4),
            "distribution": seniority_dist,
        }

    def _calc_dept_diversity(
        self, snapshots: List[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Number of departments actively hiring.
        More departments → broader expansion signal.
        """
        latest = snapshots[-1] if snapshots else {}
        dept_dist = latest.get("by_department") or {}
        if isinstance(dept_dist, str):
            try:
                dept_dist = json.loads(dept_dist)
            except (json.JSONDecodeError, TypeError):
                dept_dist = {}

        # Count departments with > 0 postings
        active = sum(1 for v in dept_dist.values() if v and v > 0)

        # Normalize: 1 department → low, 10+ → high
        score = self._normalize_score(active, 1, 10)

        return score, {"active_departments": active, "departments": dept_dist}

    # ------------------------------------------------------------------
    # Confidence calculation
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        snapshots: List[Dict],
        bls_data: Dict[str, Any],
        has_seniority: bool,
    ) -> float:
        """0-1 confidence based on data completeness."""
        confidence = 0.0

        # Snapshot coverage (up to 0.40)
        n_days = len(snapshots)
        if n_days >= 60:
            confidence += 0.40
        elif n_days >= 30:
            confidence += 0.30
        elif n_days >= MIN_SNAPSHOT_DAYS:
            confidence += 0.20

        # Posting volume (up to 0.20)
        if snapshots:
            latest_total = snapshots[-1].get("total_open") or 0
            if latest_total >= 50:
                confidence += 0.20
            elif latest_total >= 20:
                confidence += 0.15
            elif latest_total >= MIN_POSTINGS_FOR_SCORE:
                confidence += 0.10

        # BLS availability (up to 0.25)
        if bls_data.get("available"):
            confidence += 0.25

        # Seniority data (up to 0.15)
        if has_seniority:
            confidence += 0.15

        return min(confidence, 1.0)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _get_cached_score(
        self, company_id: int, score_date: date
    ) -> Optional[Dict[str, Any]]:
        query = text("""
            SELECT * FROM hiring_velocity_scores
            WHERE company_id = :cid AND score_date = :sd
              AND model_version = :ver
            LIMIT 1
        """)
        try:
            row = (
                self.db.execute(
                    query,
                    {"cid": company_id, "sd": score_date, "ver": MODEL_VERSION},
                )
                .mappings()
                .fetchone()
            )
            if row:
                return dict(row)
        except Exception as e:
            logger.warning(f"Cache check error: {e}")
        return None

    def _save_score(self, result: Dict[str, Any]) -> None:
        query = text("""
            INSERT INTO hiring_velocity_scores (
                company_id, score_date, overall_score, grade, confidence,
                posting_growth_score, industry_relative_score, momentum_score,
                seniority_signal_score, dept_diversity_score,
                posting_growth_rate_wow, posting_growth_rate_mom,
                bls_baseline_series_id, bls_baseline_growth_pct,
                industry_relative_rate, momentum_acceleration,
                active_departments, total_open_postings,
                seniority_distribution, metadata, model_version
            ) VALUES (
                :company_id, :score_date, :overall_score, :grade, :confidence,
                :posting_growth_score, :industry_relative_score, :momentum_score,
                :seniority_signal_score, :dept_diversity_score,
                :wow, :mom,
                :bls_series, :bls_growth,
                :relative_rate, :acceleration,
                :active_depts, :total_open,
                CAST(:seniority AS jsonb), CAST(:meta AS jsonb), :version
            )
            ON CONFLICT (company_id, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                confidence = EXCLUDED.confidence,
                posting_growth_score = EXCLUDED.posting_growth_score,
                industry_relative_score = EXCLUDED.industry_relative_score,
                momentum_score = EXCLUDED.momentum_score,
                seniority_signal_score = EXCLUDED.seniority_signal_score,
                dept_diversity_score = EXCLUDED.dept_diversity_score,
                posting_growth_rate_wow = EXCLUDED.posting_growth_rate_wow,
                posting_growth_rate_mom = EXCLUDED.posting_growth_rate_mom,
                bls_baseline_series_id = EXCLUDED.bls_baseline_series_id,
                bls_baseline_growth_pct = EXCLUDED.bls_baseline_growth_pct,
                industry_relative_rate = EXCLUDED.industry_relative_rate,
                momentum_acceleration = EXCLUDED.momentum_acceleration,
                active_departments = EXCLUDED.active_departments,
                total_open_postings = EXCLUDED.total_open_postings,
                seniority_distribution = EXCLUDED.seniority_distribution,
                metadata = EXCLUDED.metadata,
                model_version = EXCLUDED.model_version
        """)
        try:
            meta = result.get("metadata", {})
            self.db.execute(
                query,
                {
                    "company_id": result["company_id"],
                    "score_date": result["score_date"],
                    "overall_score": result["overall_score"],
                    "grade": result["grade"],
                    "confidence": result["confidence"],
                    "posting_growth_score": result["sub_scores"]["posting_growth"],
                    "industry_relative_score": result["sub_scores"][
                        "industry_relative"
                    ],
                    "momentum_score": result["sub_scores"]["momentum"],
                    "seniority_signal_score": result["sub_scores"][
                        "seniority_signal"
                    ],
                    "dept_diversity_score": result["sub_scores"]["dept_diversity"],
                    "wow": meta.get("posting_growth", {}).get("wow_pct"),
                    "mom": meta.get("posting_growth", {}).get("mom_pct"),
                    "bls_series": result.get("bls_baseline_series_id"),
                    "bls_growth": meta.get("industry_relative", {}).get(
                        "bls_mom_pct"
                    ),
                    "relative_rate": meta.get("industry_relative", {}).get(
                        "relative_rate"
                    ),
                    "acceleration": meta.get("momentum", {}).get("acceleration"),
                    "active_depts": meta.get("dept_diversity", {}).get(
                        "active_departments"
                    ),
                    "total_open": result.get("total_open_postings"),
                    "seniority": json.dumps(
                        meta.get("seniority_signal", {}).get("distribution", {})
                    ),
                    "meta": json.dumps(meta),
                    "version": MODEL_VERSION,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving velocity score: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score_company(
        self,
        company_id: int,
        score_date: Optional[date] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Compute the Hiring Velocity Score for a single company.

        Returns a result dict with overall_score, grade, sub-scores, and metadata.
        """
        score_date = score_date or date.today()

        # Cache check
        if not force:
            cached = self._get_cached_score(company_id, score_date)
            if cached:
                cached["cached"] = True
                return cached

        # Gather data
        snapshots = self._get_snapshot_series(company_id, days=90)

        # Guardrails
        if len(snapshots) < MIN_SNAPSHOT_DAYS:
            return {
                "company_id": company_id,
                "score_date": str(score_date),
                "error": f"Insufficient data: {len(snapshots)} snapshot days "
                f"(minimum {MIN_SNAPSHOT_DAYS})",
                "overall_score": None,
            }

        latest_total = snapshots[-1].get("total_open") or 0
        if latest_total < MIN_POSTINGS_FOR_SCORE:
            return {
                "company_id": company_id,
                "score_date": str(score_date),
                "error": f"Too few postings: {latest_total} "
                f"(minimum {MIN_POSTINGS_FOR_SCORE})",
                "overall_score": None,
            }

        # BLS baseline
        naics = self._get_company_naics(company_id)
        series_id = get_bls_series_for_company(naics)
        bls_data = self._get_bls_baseline(series_id)

        # Compute sub-scores
        pg_score, pg_meta = self._calc_posting_growth(snapshots)
        ir_score, ir_meta = self._calc_industry_relative(snapshots, bls_data)
        mom_score, mom_meta = self._calc_momentum(snapshots)
        sen_score, sen_meta = self._calc_seniority_signal(snapshots)
        dept_score, dept_meta = self._calc_dept_diversity(snapshots)

        # Weighted composite
        overall = (
            pg_score * WEIGHTS["posting_growth"]
            + ir_score * WEIGHTS["industry_relative"]
            + mom_score * WEIGHTS["momentum"]
            + sen_score * WEIGHTS["seniority_signal"]
            + dept_score * WEIGHTS["dept_diversity"]
        )
        overall = max(0.0, min(100.0, overall))

        has_seniority = bool(sen_meta.get("distribution"))
        confidence = self._calculate_confidence(snapshots, bls_data, has_seniority)

        result = {
            "company_id": company_id,
            "score_date": score_date,
            "overall_score": round(overall, 2),
            "grade": self._get_grade(overall),
            "confidence": round(confidence, 3),
            "sub_scores": {
                "posting_growth": round(pg_score, 2),
                "industry_relative": round(ir_score, 2),
                "momentum": round(mom_score, 2),
                "seniority_signal": round(sen_score, 2),
                "dept_diversity": round(dept_score, 2),
            },
            "bls_baseline_series_id": series_id,
            "bls_baseline_label": get_series_label(series_id),
            "total_open_postings": latest_total,
            "snapshot_days": len(snapshots),
            "metadata": {
                "posting_growth": pg_meta,
                "industry_relative": ir_meta,
                "momentum": mom_meta,
                "seniority_signal": sen_meta,
                "dept_diversity": dept_meta,
            },
            "model_version": MODEL_VERSION,
        }

        self._save_score(result)
        return result

    def score_all_companies(self, force: bool = False) -> Dict[str, Any]:
        """
        Batch-score all companies that have snapshot data.

        Returns summary with counts of scored, skipped, and errored companies.
        """
        query = text("""
            SELECT DISTINCT company_id
            FROM job_posting_snapshots
            WHERE snapshot_date >= CURRENT_DATE - 90
            GROUP BY company_id
            HAVING COUNT(*) >= :min_days
        """)
        try:
            rows = self.db.execute(
                query, {"min_days": MIN_SNAPSHOT_DAYS}
            ).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error fetching eligible companies: {e}")
            return {"error": str(e)}

        scored = 0
        skipped = 0
        errors = 0

        for row in rows:
            cid = row["company_id"]
            try:
                result = self.score_company(cid, force=force)
                if result.get("error"):
                    skipped += 1
                else:
                    scored += 1
            except Exception as e:
                logger.warning(f"Scoring error for company {cid}: {e}")
                errors += 1

        return {
            "total_eligible": len(rows),
            "scored": scored,
            "skipped": skipped,
            "errors": errors,
        }

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Hiring Velocity Score cross-references job posting snapshots "
                "with BLS CES employment baselines to quantify a company's "
                "hiring momentum on a 0-100 scale."
            ),
            "sub_scores": [
                {
                    "name": "posting_growth",
                    "weight": WEIGHTS["posting_growth"],
                    "description": (
                        "WoW/MoM % change in total open postings, "
                        "blended 60/40"
                    ),
                },
                {
                    "name": "industry_relative",
                    "weight": WEIGHTS["industry_relative"],
                    "description": (
                        "Company posting growth vs BLS CES industry "
                        "employment growth"
                    ),
                },
                {
                    "name": "momentum",
                    "weight": WEIGHTS["momentum"],
                    "description": (
                        "Acceleration (2nd derivative) — is growth "
                        "speeding up or slowing?"
                    ),
                },
                {
                    "name": "seniority_signal",
                    "weight": WEIGHTS["seniority_signal"],
                    "description": (
                        "Shannon entropy of seniority mix + senior "
                        "role share shift"
                    ),
                },
                {
                    "name": "dept_diversity",
                    "weight": WEIGHTS["dept_diversity"],
                    "description": "Number of departments actively hiring",
                },
            ],
            "grade_thresholds": {
                "A": ">=80",
                "B": ">=65",
                "C": ">=50",
                "D": ">=35",
                "F": "<35",
            },
            "guardrails": {
                "min_snapshot_days": MIN_SNAPSHOT_DAYS,
                "min_postings": MIN_POSTINGS_FOR_SCORE,
                "missing_bls": "neutral 50 + confidence penalty",
                "missing_naics": "falls back to Total Private baseline",
            },
            "data_sources": [
                "job_posting_snapshots (internal)",
                "bls_ces_employment (BLS Current Employment Statistics)",
                "industrial_companies.naics_code (for sector mapping)",
            ],
            "confidence": {
                "description": "0-1 scale based on data completeness",
                "components": {
                    "snapshot_coverage": "Up to 0.40 (60+ days = full credit)",
                    "posting_volume": "Up to 0.20 (50+ postings = full credit)",
                    "bls_availability": "0.25 if BLS data is present",
                    "seniority_data": "0.15 if seniority breakdown exists",
                },
            },
        }
