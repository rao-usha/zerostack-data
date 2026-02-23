"""
Private Company Health Score — core scoring engine.

Combines hiring momentum, web traffic (Tranco), employee sentiment
(Glassdoor), and foot traffic into a 0-100 health score for companies
that lack public financials.

Follows the pattern in app/sources/job_postings/velocity_scorer.py.
"""

import json
import logging
import math
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.health_score_metadata import (
    generate_create_company_health_scores_sql,
    generate_create_company_web_traffic_sql,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL_VERSION = "v1.0"

WEIGHTS = {
    "hiring_momentum": 0.35,
    "web_presence": 0.25,
    "employee_sentiment": 0.20,
    "foot_traffic": 0.20,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

# Neutral score for missing signals
NEUTRAL_SCORE = 50.0


class PrivateCompanyHealthScorer:
    """Compute multi-signal health scores for private companies."""

    def __init__(self, db: Session):
        self.db = db
        self._tranco_client = None
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
                cursor.execute(generate_create_company_health_scores_sql())
                cursor.execute(generate_create_company_web_traffic_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"Health score table creation warning: {e}")

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

    @staticmethod
    def _extract_domain(website: str) -> Optional[str]:
        """Extract bare domain from a company website URL."""
        if not website:
            return None
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
        try:
            parsed = urlparse(website)
            domain = parsed.netloc or parsed.path
            domain = domain.lower().strip().rstrip("/")
            if domain.startswith("www."):
                domain = domain[4:]
            return domain if domain else None
        except Exception:
            return None

    def _get_tranco_client(self):
        """Lazy-load TrancoClient (avoids downloading list at init)."""
        if self._tranco_client is None:
            from app.sources.web_traffic.tranco import TrancoClient
            self._tranco_client = TrancoClient()
        return self._tranco_client

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_hiring_velocity(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get latest hiring velocity score for a company."""
        query = text("""
            SELECT overall_score, confidence, grade, score_date
            FROM hiring_velocity_scores
            WHERE company_id = :cid
            ORDER BY score_date DESC
            LIMIT 1
        """)
        try:
            row = self.db.execute(
                query, {"cid": company_id}
            ).mappings().fetchone()
            return dict(row) if row else None
        except Exception:
            self.db.rollback()
            return None

    def _get_web_traffic(self, company_id: int) -> Optional[Dict[str, Any]]:
        """
        Get web traffic rank for a company.

        Checks cache first, then fetches from Tranco and caches result.
        """
        today = date.today()

        # Check cache
        cache_query = text("""
            SELECT domain, tranco_rank, list_date
            FROM company_web_traffic
            WHERE company_id = :cid
              AND list_date = :today
            LIMIT 1
        """)
        try:
            cached = self.db.execute(
                cache_query, {"cid": company_id, "today": today}
            ).mappings().fetchone()
            if cached:
                return dict(cached)
        except Exception:
            self.db.rollback()

        # Look up company website
        website_query = text("""
            SELECT website FROM industrial_companies WHERE id = :cid
        """)
        try:
            row = self.db.execute(
                website_query, {"cid": company_id}
            ).mappings().fetchone()
            if not row or not row["website"]:
                return None
            domain = self._extract_domain(row["website"])
            if not domain:
                return None
        except Exception:
            self.db.rollback()
            return None

        # Fetch from Tranco
        try:
            tranco = self._get_tranco_client()
            rank = tranco.get_rank(domain)
        except Exception as e:
            logger.warning(f"Tranco fetch failed for {domain}: {e}")
            rank = None

        # Cache result (even if rank is None — prevents re-fetching)
        try:
            insert_query = text("""
                INSERT INTO company_web_traffic (company_id, domain, tranco_rank, list_date)
                VALUES (:cid, :domain, :rank, :today)
                ON CONFLICT (company_id, list_date) DO UPDATE SET
                    domain = EXCLUDED.domain,
                    tranco_rank = EXCLUDED.tranco_rank,
                    fetched_at = NOW()
            """)
            self.db.execute(insert_query, {
                "cid": company_id,
                "domain": domain,
                "rank": rank,
                "today": today,
            })
            self.db.commit()
        except Exception:
            self.db.rollback()

        return {"domain": domain, "tranco_rank": rank, "list_date": today}

    def _get_glassdoor_data(self, company_id: int) -> Optional[Dict[str, Any]]:
        """Get Glassdoor rating data by matching company name."""
        query = text("""
            SELECT gc.overall_rating, gc.business_outlook, gc.review_count,
                   gc.company_name AS glassdoor_name
            FROM glassdoor_companies gc
            JOIN industrial_companies ic ON LOWER(gc.company_name) = LOWER(ic.name)
            WHERE ic.id = :cid
            LIMIT 1
        """)
        try:
            row = self.db.execute(
                query, {"cid": company_id}
            ).mappings().fetchone()
            return dict(row) if row else None
        except Exception:
            self.db.rollback()
            return None

    def _get_foot_traffic_trend(self, company_id: int) -> Optional[Dict[str, Any]]:
        """
        Get MoM foot traffic trend via locations table.

        Returns None gracefully if tables/data don't exist.
        """
        query = text("""
            SELECT
                ft_recent.avg_visits AS recent_visits,
                ft_prior.avg_visits AS prior_visits,
                CASE
                    WHEN ft_prior.avg_visits > 0
                    THEN ((ft_recent.avg_visits - ft_prior.avg_visits)
                          / ft_prior.avg_visits * 100)
                    ELSE NULL
                END AS mom_change_pct
            FROM (
                SELECT AVG(visit_count) AS avg_visits
                FROM foot_traffic_observations fto
                JOIN locations loc ON fto.location_id = loc.id
                WHERE loc.company_id = :cid
                  AND fto.observation_date >= CURRENT_DATE - 30
            ) ft_recent,
            (
                SELECT AVG(visit_count) AS avg_visits
                FROM foot_traffic_observations fto
                JOIN locations loc ON fto.location_id = loc.id
                WHERE loc.company_id = :cid
                  AND fto.observation_date >= CURRENT_DATE - 60
                  AND fto.observation_date < CURRENT_DATE - 30
            ) ft_prior
        """)
        try:
            row = self.db.execute(
                query, {"cid": company_id}
            ).mappings().fetchone()
            if row and row["recent_visits"] is not None:
                return dict(row)
            return None
        except Exception:
            self.db.rollback()
            return None

    # ------------------------------------------------------------------
    # Sub-score calculations (each returns score, metadata dict)
    # ------------------------------------------------------------------

    def _calc_hiring_momentum(
        self, velocity_data: Optional[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Map hiring velocity score directly (already 0-100).
        Missing → neutral 50.
        """
        if not velocity_data:
            return NEUTRAL_SCORE, {"note": "no hiring velocity data", "available": False}

        score = float(velocity_data["overall_score"])
        return score, {
            "hiring_velocity_score": score,
            "velocity_grade": velocity_data.get("grade"),
            "velocity_date": str(velocity_data.get("score_date", "")),
            "available": True,
        }

    def _calc_web_presence(
        self, traffic_data: Optional[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Log-normalize Tranco rank: rank 1 → 100, rank 1M → 0.
        Not in top 1M → 10 (small signal for having a website at all).
        """
        if not traffic_data:
            return NEUTRAL_SCORE, {"note": "no website found", "available": False}

        rank = traffic_data.get("tranco_rank")
        domain = traffic_data.get("domain", "")

        if rank is None:
            # Has website but not in top 1M
            return 10.0, {
                "domain": domain,
                "tranco_rank": None,
                "note": "domain not in Tranco top 1M",
                "available": True,
            }

        # Log-normalize: log10(1M) = 6, log10(1) = 0
        # score = (1 - log10(rank)/6) * 100
        log_rank = math.log10(max(rank, 1))
        score = max(0.0, min(100.0, (1 - log_rank / 6) * 100))

        return score, {
            "domain": domain,
            "tranco_rank": rank,
            "available": True,
        }

    def _calc_employee_sentiment(
        self, glassdoor_data: Optional[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Glassdoor overall_rating (0-5) → 0-100, blended with business_outlook.
        """
        if not glassdoor_data:
            return NEUTRAL_SCORE, {"note": "no Glassdoor data", "available": False}

        rating = glassdoor_data.get("overall_rating")
        if rating is None:
            return NEUTRAL_SCORE, {"note": "no rating available", "available": False}

        try:
            rating = float(rating)
        except (TypeError, ValueError):
            return NEUTRAL_SCORE, {"note": "invalid rating", "available": False}

        # Rating 0-5 → 0-100
        rating_score = (rating / 5.0) * 100

        # Outlook bonus/penalty
        outlook = (glassdoor_data.get("business_outlook") or "").lower()
        outlook_adjustment = 0.0
        if "positive" in outlook or "getting better" in outlook:
            outlook_adjustment = 10.0
        elif "negative" in outlook or "getting worse" in outlook:
            outlook_adjustment = -10.0

        # Blend: 80% rating + 20% outlook adjustment
        score = rating_score + outlook_adjustment * 0.2
        score = max(0.0, min(100.0, score))

        return score, {
            "overall_rating": rating,
            "business_outlook": glassdoor_data.get("business_outlook"),
            "review_count": glassdoor_data.get("review_count"),
            "available": True,
        }

    def _calc_foot_traffic(
        self, traffic_data: Optional[Dict]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        MoM visit_count change → 0-100.
        -50% decline → 0, flat → 50, +50% growth → 100.
        """
        if not traffic_data:
            return NEUTRAL_SCORE, {"note": "no foot traffic data", "available": False}

        mom_pct = traffic_data.get("mom_change_pct")
        if mom_pct is None:
            return NEUTRAL_SCORE, {
                "note": "insufficient foot traffic history",
                "available": False,
            }

        mom_pct = float(mom_pct)
        score = self._normalize_score(mom_pct, -50, 50)

        return score, {
            "mom_change_pct": round(mom_pct, 4),
            "recent_visits": traffic_data.get("recent_visits"),
            "prior_visits": traffic_data.get("prior_visits"),
            "available": True,
        }

    # ------------------------------------------------------------------
    # Confidence calculation
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        velocity_data: Optional[Dict],
        traffic_data: Optional[Dict],
        glassdoor_data: Optional[Dict],
        foot_data: Optional[Dict],
    ) -> float:
        """0-1 confidence based on signal availability."""
        confidence = 0.0

        if velocity_data:
            confidence += 0.35
        if traffic_data and traffic_data.get("tranco_rank") is not None:
            confidence += 0.25
        if glassdoor_data:
            confidence += 0.20
            # Bonus for high review count
            review_count = glassdoor_data.get("review_count")
            if review_count is not None:
                try:
                    if int(review_count) >= 50:
                        confidence += 0.05
                except (TypeError, ValueError):
                    pass
        if foot_data and foot_data.get("mom_change_pct") is not None:
            confidence += 0.15

        return min(confidence, 1.0)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _get_cached_score(
        self, company_id: int, score_date: date
    ) -> Optional[Dict[str, Any]]:
        query = text("""
            SELECT * FROM company_health_scores
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
        except Exception:
            self.db.rollback()
        return None

    def _save_score(self, result: Dict[str, Any]) -> None:
        query = text("""
            INSERT INTO company_health_scores (
                company_id, score_date, overall_score, grade, confidence,
                hiring_momentum_score, web_presence_score,
                employee_sentiment_score, foot_traffic_score,
                hiring_velocity_raw, tranco_rank,
                glassdoor_rating, glassdoor_outlook,
                foot_traffic_trend_pct,
                metadata, signals_available, model_version
            ) VALUES (
                :company_id, :score_date, :overall_score, :grade, :confidence,
                :hiring_momentum_score, :web_presence_score,
                :employee_sentiment_score, :foot_traffic_score,
                :hiring_velocity_raw, :tranco_rank,
                :glassdoor_rating, :glassdoor_outlook,
                :foot_traffic_trend_pct,
                CAST(:metadata AS jsonb), CAST(:signals AS jsonb), :version
            )
            ON CONFLICT (company_id, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                confidence = EXCLUDED.confidence,
                hiring_momentum_score = EXCLUDED.hiring_momentum_score,
                web_presence_score = EXCLUDED.web_presence_score,
                employee_sentiment_score = EXCLUDED.employee_sentiment_score,
                foot_traffic_score = EXCLUDED.foot_traffic_score,
                hiring_velocity_raw = EXCLUDED.hiring_velocity_raw,
                tranco_rank = EXCLUDED.tranco_rank,
                glassdoor_rating = EXCLUDED.glassdoor_rating,
                glassdoor_outlook = EXCLUDED.glassdoor_outlook,
                foot_traffic_trend_pct = EXCLUDED.foot_traffic_trend_pct,
                metadata = EXCLUDED.metadata,
                signals_available = EXCLUDED.signals_available,
                model_version = EXCLUDED.model_version
        """)
        try:
            meta = result.get("metadata", {})
            signals = result.get("signals_available", {})
            sub = result.get("sub_scores", {})

            self.db.execute(
                query,
                {
                    "company_id": result["company_id"],
                    "score_date": result["score_date"],
                    "overall_score": result["overall_score"],
                    "grade": result["grade"],
                    "confidence": result["confidence"],
                    "hiring_momentum_score": sub.get("hiring_momentum"),
                    "web_presence_score": sub.get("web_presence"),
                    "employee_sentiment_score": sub.get("employee_sentiment"),
                    "foot_traffic_score": sub.get("foot_traffic"),
                    "hiring_velocity_raw": meta.get("hiring_momentum", {}).get(
                        "hiring_velocity_score"
                    ),
                    "tranco_rank": meta.get("web_presence", {}).get("tranco_rank"),
                    "glassdoor_rating": meta.get("employee_sentiment", {}).get(
                        "overall_rating"
                    ),
                    "glassdoor_outlook": meta.get("employee_sentiment", {}).get(
                        "business_outlook"
                    ),
                    "foot_traffic_trend_pct": meta.get("foot_traffic", {}).get(
                        "mom_change_pct"
                    ),
                    "metadata": json.dumps(meta),
                    "signals": json.dumps(signals),
                    "version": MODEL_VERSION,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving health score: {e}")
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
        Compute the Private Company Health Score for a single company.

        Returns a result dict with overall_score, grade, sub-scores, and metadata.
        """
        score_date = score_date or date.today()

        # Cache check
        if not force:
            cached = self._get_cached_score(company_id, score_date)
            if cached:
                cached["cached"] = True
                return cached

        # Gather signals
        velocity_data = self._get_hiring_velocity(company_id)
        traffic_data = self._get_web_traffic(company_id)
        glassdoor_data = self._get_glassdoor_data(company_id)
        foot_data = self._get_foot_traffic_trend(company_id)

        # Compute sub-scores
        hm_score, hm_meta = self._calc_hiring_momentum(velocity_data)
        wp_score, wp_meta = self._calc_web_presence(traffic_data)
        es_score, es_meta = self._calc_employee_sentiment(glassdoor_data)
        ft_score, ft_meta = self._calc_foot_traffic(foot_data)

        # Weighted composite
        overall = (
            hm_score * WEIGHTS["hiring_momentum"]
            + wp_score * WEIGHTS["web_presence"]
            + es_score * WEIGHTS["employee_sentiment"]
            + ft_score * WEIGHTS["foot_traffic"]
        )
        overall = max(0.0, min(100.0, overall))

        confidence = self._calculate_confidence(
            velocity_data, traffic_data, glassdoor_data, foot_data
        )

        signals_available = {
            "hiring_velocity": hm_meta.get("available", False),
            "web_traffic": wp_meta.get("available", False),
            "glassdoor": es_meta.get("available", False),
            "foot_traffic": ft_meta.get("available", False),
        }
        signals_count = sum(1 for v in signals_available.values() if v)

        result = {
            "company_id": company_id,
            "score_date": score_date,
            "overall_score": round(overall, 2),
            "grade": self._get_grade(overall),
            "confidence": round(confidence, 3),
            "signals_available": signals_available,
            "signals_count": signals_count,
            "sub_scores": {
                "hiring_momentum": round(hm_score, 2),
                "web_presence": round(wp_score, 2),
                "employee_sentiment": round(es_score, 2),
                "foot_traffic": round(ft_score, 2),
            },
            "metadata": {
                "hiring_momentum": hm_meta,
                "web_presence": wp_meta,
                "employee_sentiment": es_meta,
                "foot_traffic": ft_meta,
            },
            "model_version": MODEL_VERSION,
        }

        self._save_score(result)
        return result

    def score_all_companies(self, force: bool = False) -> Dict[str, Any]:
        """
        Batch-score all companies in industrial_companies.

        Returns summary with counts of scored, skipped, and errored companies.
        """
        query = text("""
            SELECT id FROM industrial_companies
            ORDER BY id
        """)
        try:
            rows = self.db.execute(query).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error fetching companies for health scoring: {e}")
            return {"error": str(e)}

        scored = 0
        errors = 0

        for row in rows:
            cid = row["id"]
            try:
                self.score_company(cid, force=force)
                scored += 1
            except Exception as e:
                logger.warning(f"Health scoring error for company {cid}: {e}")
                errors += 1

        return {
            "total_companies": len(rows),
            "scored": scored,
            "errors": errors,
        }

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Private Company Health Score combines multiple alternative data "
                "signals to proxy financial health for companies without public "
                "financials. Each signal is normalized to 0-100 and weighted to "
                "produce a composite score."
            ),
            "sub_scores": [
                {
                    "name": "hiring_momentum",
                    "weight": WEIGHTS["hiring_momentum"],
                    "description": (
                        "Latest Hiring Velocity Score (job posting growth "
                        "vs BLS baselines)"
                    ),
                    "source": "hiring_velocity_scores",
                },
                {
                    "name": "web_presence",
                    "weight": WEIGHTS["web_presence"],
                    "description": (
                        "Tranco top-1M domain rank, log-normalized "
                        "(rank 1=100, 1M=0)"
                    ),
                    "source": "Tranco list (free, daily)",
                },
                {
                    "name": "employee_sentiment",
                    "weight": WEIGHTS["employee_sentiment"],
                    "description": (
                        "Glassdoor overall_rating (0-5 → 0-100) "
                        "blended with business_outlook"
                    ),
                    "source": "glassdoor_companies",
                },
                {
                    "name": "foot_traffic",
                    "weight": WEIGHTS["foot_traffic"],
                    "description": (
                        "Month-over-month visit count change from "
                        "foot traffic observations"
                    ),
                    "source": "foot_traffic_observations via locations",
                },
            ],
            "grade_thresholds": {
                "A": ">=80",
                "B": ">=65",
                "C": ">=50",
                "D": ">=35",
                "F": "<35",
            },
            "missing_signals": (
                "Missing signals receive a neutral score of 50 and reduce "
                "the confidence value proportionally."
            ),
            "confidence": {
                "description": "0-1 scale based on signal availability",
                "components": {
                    "hiring_velocity": "+0.35",
                    "tranco_rank": "+0.25",
                    "glassdoor": "+0.20",
                    "foot_traffic": "+0.15",
                    "glassdoor_review_count_bonus": "+0.05 if review_count >= 50",
                },
            },
        }
