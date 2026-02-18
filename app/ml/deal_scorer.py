"""
Predictive Deal Scoring Model (T40)

Combines deal attributes, company health scores (T36), and historical
patterns to predict win probability and provide actionable insights.

Features:
- Win probability (0-100%)
- Category scores (company, deal, pipeline, pattern)
- Similar deal identification
- Risk alerts and recommendations
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Model configuration
MODEL_VERSION = "v1.0"

# Category weights for win probability
WEIGHTS = {
    "company": 0.40,
    "deal": 0.30,
    "pipeline": 0.20,
    "pattern": 0.10,
}

# Confidence thresholds
CONFIDENCE_THRESHOLDS = {
    "high": 0.70,
    "medium": 0.40,
}

# Pipeline stages
ACTIVE_STAGES = ["sourced", "reviewing", "due_diligence", "negotiation"]
CLOSED_STAGES = ["closed_won", "closed_lost", "passed"]

# Sector sweet spots (deal size in millions)
SECTOR_SWEET_SPOTS = {
    "fintech": (5, 50),
    "healthtech": (10, 75),
    "saas": (5, 40),
    "ai": (10, 100),
    "climate": (15, 80),
    "default": (5, 50),
}

# Stage velocity benchmarks (days)
STAGE_BENCHMARKS = {
    "sourced": 14,
    "reviewing": 21,
    "due_diligence": 30,
    "negotiation": 21,
}


@dataclass
class DealPrediction:
    """Prediction result for a deal."""

    deal_id: int
    company_name: str
    win_probability: float
    confidence: str
    tier: str
    company_score: float
    deal_score: float
    pipeline_score: float
    pattern_score: float
    strengths: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    similar_deals: List[Dict] = field(default_factory=list)
    optimal_close_window: str = ""
    days_to_decision: int = 0


@dataclass
class PipelineInsights:
    """Aggregate insights for the pipeline."""

    total_active_deals: int
    total_pipeline_value: float
    expected_value: float
    avg_win_probability: float
    stage_analysis: List[Dict]
    risk_alerts: List[Dict]
    opportunities: List[Dict]
    sector_performance: Dict[str, Dict]


class DealScorer:
    """
    Predictive deal scoring engine.

    Combines multiple signals to predict deal outcomes:
    1. Company quality from T36 company scores
    2. Deal characteristics (size, valuation, sector)
    3. Pipeline signals (velocity, activity, priority)
    4. Historical patterns (win rates for similar deals)
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create prediction tables if they don't exist."""
        create_predictions = text("""
            CREATE TABLE IF NOT EXISTS deal_predictions (
                id SERIAL PRIMARY KEY,
                deal_id INTEGER NOT NULL,

                -- Scores
                win_probability FLOAT NOT NULL,
                confidence VARCHAR(20),
                tier VARCHAR(1),

                -- Category scores
                company_score FLOAT,
                deal_score FLOAT,
                pipeline_score FLOAT,
                pattern_score FLOAT,

                -- Insights
                strengths JSONB,
                risks JSONB,
                recommendations JSONB,

                -- Similar deals
                similar_deal_ids INTEGER[],

                -- Timing
                optimal_close_window VARCHAR(50),
                days_to_decision INTEGER,

                -- Metadata
                model_version VARCHAR(20) DEFAULT 'v1.0',
                predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(deal_id, model_version)
            )
        """)

        create_index = text("""
            CREATE INDEX IF NOT EXISTS idx_deal_predictions_probability
            ON deal_predictions(win_probability DESC)
        """)

        try:
            self.db.execute(create_predictions)
            self.db.execute(create_index)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # DATA FETCHING
    # -------------------------------------------------------------------------

    def _get_deal(self, deal_id: int) -> Optional[Dict]:
        """Get deal data from T28."""
        query = text("""
            SELECT
                d.*,
                (SELECT COUNT(*) FROM deal_activities WHERE deal_id = d.id) as activity_count,
                (SELECT MAX(created_at) FROM deal_activities WHERE deal_id = d.id) as last_activity_at
            FROM deals d
            WHERE d.id = :deal_id
        """)
        try:
            result = self.db.execute(query, {"deal_id": deal_id})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.warning(f"Error fetching deal: {e}")
            return None

    def _get_company_score(self, company_name: str) -> Optional[Dict]:
        """Get company score from T36."""
        query = text("""
            SELECT * FROM company_scores
            WHERE LOWER(company_name) = LOWER(:name)
            ORDER BY scored_at DESC
            LIMIT 1
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.warning(f"Error fetching company score: {e}")
            return None

    def _get_historical_deals(
        self, sector: Optional[str] = None, limit: int = 100
    ) -> List[Dict]:
        """Get historical closed deals for pattern analysis."""
        query = text("""
            SELECT * FROM deals
            WHERE pipeline_stage IN ('closed_won', 'closed_lost', 'passed')
            AND (:sector IS NULL OR company_sector = :sector)
            ORDER BY closed_at DESC
            LIMIT :limit
        """)
        try:
            result = self.db.execute(query, {"sector": sector, "limit": limit})
            return [dict(row) for row in result.mappings()]
        except Exception as e:
            logger.warning(f"Error fetching historical deals: {e}")
            return []

    def _get_active_deals(self) -> List[Dict]:
        """Get all active deals in pipeline."""
        query = text("""
            SELECT
                d.*,
                (SELECT COUNT(*) FROM deal_activities WHERE deal_id = d.id) as activity_count,
                (SELECT MAX(created_at) FROM deal_activities WHERE deal_id = d.id) as last_activity_at
            FROM deals d
            WHERE d.pipeline_stage IN ('sourced', 'reviewing', 'due_diligence', 'negotiation')
            ORDER BY d.priority ASC, d.created_at DESC
        """)
        try:
            result = self.db.execute(query)
            return [dict(row) for row in result.mappings()]
        except Exception as e:
            logger.warning(f"Error fetching active deals: {e}")
            return []

    # -------------------------------------------------------------------------
    # SCORING FUNCTIONS
    # -------------------------------------------------------------------------

    def _calculate_company_score(
        self, deal: Dict, company_score: Optional[Dict]
    ) -> Tuple[float, List[str], List[str]]:
        """
        Calculate company quality score (0-100).

        Uses T36 company score if available, otherwise estimates from deal data.
        """
        strengths = []
        risks = []

        if company_score:
            # Use T36 composite score
            score = company_score.get("composite_score", 50)
            tier = company_score.get("tier", "C")

            if score >= 70:
                strengths.append(
                    f"Strong company health score ({score:.0f}/100, Tier {tier})"
                )
            elif score >= 50:
                strengths.append(
                    f"Moderate company health score ({score:.0f}/100, Tier {tier})"
                )
            else:
                risks.append(
                    f"Below-average company health score ({score:.0f}/100, Tier {tier})"
                )

            # Check category scores
            growth = company_score.get("growth_score")
            if growth and growth >= 70:
                strengths.append("Strong growth trajectory")
            elif growth and growth < 40:
                risks.append("Weak growth indicators")

            return score, strengths, risks

        # Estimate from deal data if no company score
        score = 50.0  # Base score

        # Stage adjustment
        stage = deal.get("company_stage")
        if stage in ["growth", "late"]:
            score += 10
            strengths.append("Mature company stage")
        elif stage in ["seed", "pre-seed"]:
            score -= 10
            risks.append("Early stage company - higher risk")

        # Sector adjustment
        sector = deal.get("company_sector")
        hot_sectors = ["ai", "fintech", "climate"]
        if sector and sector.lower() in hot_sectors:
            score += 5
            strengths.append(f"Hot sector ({sector})")

        return max(0, min(100, score)), strengths, risks

    def _calculate_deal_score(self, deal: Dict) -> Tuple[float, List[str], List[str]]:
        """
        Calculate deal characteristics score (0-100).

        Considers deal size, valuation, and sector fit.
        """
        strengths = []
        risks = []
        score = 50.0

        # Deal size analysis
        deal_size = deal.get("deal_size_millions")
        sector = deal.get("company_sector", "default").lower()
        sweet_spot = SECTOR_SWEET_SPOTS.get(sector, SECTOR_SWEET_SPOTS["default"])

        if deal_size:
            if sweet_spot[0] <= deal_size <= sweet_spot[1]:
                score += 20
                strengths.append(
                    f"Deal size (${deal_size}M) in sweet spot for {sector}"
                )
            elif deal_size < sweet_spot[0]:
                score += 10
                risks.append(f"Deal size (${deal_size}M) below typical range")
            else:
                score -= 10
                risks.append(
                    f"Deal size (${deal_size}M) above typical range - concentration risk"
                )

        # Valuation analysis
        valuation = deal.get("valuation_millions")
        if valuation and deal_size:
            # Check reasonableness (simplified)
            if valuation > deal_size * 20:
                risks.append("High valuation multiple")
                score -= 10
            else:
                strengths.append("Reasonable valuation")
                score += 10

        # Existing fit score (stored as 0-100)
        fit_score = deal.get("fit_score")
        if fit_score:
            # Normalize to 0-1 range if stored as 0-100
            fit_normalized = fit_score / 100 if fit_score > 1 else fit_score
            if fit_normalized >= 0.7:
                score += 15
                strengths.append(f"High thesis fit ({fit_normalized:.0%})")
            elif fit_normalized >= 0.5:
                score += 5
            else:
                risks.append(f"Low thesis fit ({fit_normalized:.0%})")
                score -= 10

        # Deal type
        deal_type = deal.get("deal_type")
        if deal_type in ["primary", "lead"]:
            strengths.append(f"Favorable deal type ({deal_type})")
            score += 5

        return max(0, min(100, score)), strengths, risks

    def _calculate_pipeline_score(
        self, deal: Dict
    ) -> Tuple[float, List[str], List[str]]:
        """
        Calculate pipeline signals score (0-100).

        Considers velocity, activity, and priority.
        """
        strengths = []
        risks = []
        score = 50.0

        # Stage velocity
        stage = deal.get("pipeline_stage")
        created_at = deal.get("created_at")
        if stage and created_at:
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

            days_in_pipeline = (
                datetime.utcnow() - created_at.replace(tzinfo=None)
            ).days

            benchmark = STAGE_BENCHMARKS.get(stage, 21)
            if days_in_pipeline <= benchmark:
                score += 15
                strengths.append(f"Good velocity ({days_in_pipeline} days in pipeline)")
            elif days_in_pipeline <= benchmark * 1.5:
                score += 5
            else:
                risks.append(f"Slow velocity ({days_in_pipeline} days in pipeline)")
                score -= 15

        # Activity frequency
        activity_count = deal.get("activity_count", 0)
        last_activity = deal.get("last_activity_at")

        if activity_count >= 5:
            score += 15
            strengths.append(f"Active engagement ({activity_count} activities)")
        elif activity_count >= 2:
            score += 5
        else:
            risks.append("Low activity count")
            score -= 10

        # Recent activity
        if last_activity:
            if isinstance(last_activity, str):
                last_activity = datetime.fromisoformat(
                    last_activity.replace("Z", "+00:00")
                )

            days_since_activity = (
                datetime.utcnow() - last_activity.replace(tzinfo=None)
            ).days
            if days_since_activity <= 7:
                score += 10
                strengths.append("Recent activity this week")
            elif days_since_activity > 14:
                risks.append(f"No activity in {days_since_activity} days")
                score -= 10

        # Priority
        priority = deal.get("priority", 3)
        if priority == 1:
            score += 10
            strengths.append("Top priority deal")
        elif priority >= 4:
            risks.append("Low priority assignment")
            score -= 5

        return max(0, min(100, score)), strengths, risks

    def _calculate_pattern_score(
        self, deal: Dict, historical_deals: List[Dict]
    ) -> Tuple[float, List[str], List[str], List[Dict]]:
        """
        Calculate historical pattern score (0-100).

        Analyzes similar historical deals for win rate patterns.
        """
        strengths = []
        risks = []
        similar_deals = []
        score = 50.0

        if not historical_deals:
            return score, strengths, risks, similar_deals

        # Find similar deals
        sector = deal.get("company_sector")
        deal_size = deal.get("deal_size_millions", 0)
        source = deal.get("source")

        for hist in historical_deals:
            similarity = 0.0
            factors = []

            # Sector match
            if hist.get("company_sector") == sector:
                similarity += 0.4
                factors.append("same_sector")

            # Size similarity (within 50%)
            hist_size = hist.get("deal_size_millions", 0)
            if hist_size and deal_size:
                if abs(hist_size - deal_size) / max(hist_size, deal_size, 1) < 0.5:
                    similarity += 0.3
                    factors.append("similar_size")

            # Source match
            if hist.get("source") == source and source:
                similarity += 0.2
                factors.append("same_source")

            # Stage match
            if hist.get("company_stage") == deal.get("company_stage"):
                similarity += 0.1
                factors.append("same_stage")

            if similarity >= 0.4:
                similar_deals.append(
                    {
                        "id": hist.get("id"),
                        "company_name": hist.get("company_name"),
                        "sector": hist.get("company_sector"),
                        "deal_size_millions": hist_size,
                        "outcome": hist.get("pipeline_stage"),
                        "similarity_score": similarity,
                        "similarity_factors": factors,
                    }
                )

        # Sort by similarity
        similar_deals.sort(key=lambda x: x["similarity_score"], reverse=True)
        similar_deals = similar_deals[:5]  # Top 5

        # Calculate win rate from similar deals
        if similar_deals:
            wins = sum(1 for d in similar_deals if d["outcome"] == "closed_won")
            win_rate = wins / len(similar_deals)

            if win_rate >= 0.6:
                score += 25
                strengths.append(f"Similar deals have {win_rate:.0%} win rate")
            elif win_rate >= 0.4:
                score += 10
            else:
                risks.append(f"Similar deals have low win rate ({win_rate:.0%})")
                score -= 15

        # Source analysis
        if source:
            source_deals = [d for d in historical_deals if d.get("source") == source]
            if len(source_deals) >= 3:
                source_wins = sum(
                    1 for d in source_deals if d.get("pipeline_stage") == "closed_won"
                )
                source_rate = source_wins / len(source_deals)
                if source_rate >= 0.5:
                    strengths.append(
                        f"Strong source track record ({source_rate:.0%} win rate)"
                    )
                    score += 10

        return max(0, min(100, score)), strengths, risks, similar_deals

    # -------------------------------------------------------------------------
    # MAIN SCORING
    # -------------------------------------------------------------------------

    def score_deal(
        self, deal_id: int, use_cache: bool = True
    ) -> Optional[DealPrediction]:
        """
        Score a single deal with full breakdown.

        Args:
            deal_id: Deal ID to score
            use_cache: Use cached prediction if available

        Returns:
            DealPrediction with scores and insights
        """
        # Check cache
        if use_cache:
            cached = self._get_cached_prediction(deal_id)
            if cached:
                return cached

        # Fetch deal
        deal = self._get_deal(deal_id)
        if not deal:
            logger.warning(f"Deal {deal_id} not found")
            return None

        company_name = deal.get("company_name", "Unknown")

        # Skip closed deals
        if deal.get("pipeline_stage") in CLOSED_STAGES:
            logger.info(f"Deal {deal_id} is already closed")
            return None

        # Fetch company score
        company_score = self._get_company_score(company_name)

        # Fetch historical deals
        historical = self._get_historical_deals(deal.get("company_sector"))

        # Calculate category scores
        company_pts, company_strengths, company_risks = self._calculate_company_score(
            deal, company_score
        )
        deal_pts, deal_strengths, deal_risks = self._calculate_deal_score(deal)
        pipeline_pts, pipeline_strengths, pipeline_risks = (
            self._calculate_pipeline_score(deal)
        )
        pattern_pts, pattern_strengths, pattern_risks, similar = (
            self._calculate_pattern_score(deal, historical)
        )

        # Combine scores with weights
        win_probability = (
            company_pts * WEIGHTS["company"]
            + deal_pts * WEIGHTS["deal"]
            + pipeline_pts * WEIGHTS["pipeline"]
            + pattern_pts * WEIGHTS["pattern"]
        ) / 100  # Convert to 0-1 scale

        # Determine confidence
        if win_probability >= CONFIDENCE_THRESHOLDS["high"]:
            confidence = "high"
        elif win_probability >= CONFIDENCE_THRESHOLDS["medium"]:
            confidence = "medium"
        else:
            confidence = "low"

        # Determine tier
        if win_probability >= 0.8:
            tier = "A"
        elif win_probability >= 0.6:
            tier = "B"
        elif win_probability >= 0.4:
            tier = "C"
        elif win_probability >= 0.2:
            tier = "D"
        else:
            tier = "F"

        # Combine insights
        strengths = (
            company_strengths + deal_strengths + pipeline_strengths + pattern_strengths
        )
        risks = company_risks + deal_risks + pipeline_risks + pattern_risks

        # Generate recommendations
        recommendations = self._generate_recommendations(deal, win_probability, risks)

        # Calculate timing
        optimal_window, days_to_decision = self._estimate_timing(deal, similar)

        prediction = DealPrediction(
            deal_id=deal_id,
            company_name=company_name,
            win_probability=round(win_probability, 3),
            confidence=confidence,
            tier=tier,
            company_score=round(company_pts, 1),
            deal_score=round(deal_pts, 1),
            pipeline_score=round(pipeline_pts, 1),
            pattern_score=round(pattern_pts, 1),
            strengths=strengths[:5],  # Top 5
            risks=risks[:5],
            recommendations=recommendations[:3],  # Top 3
            similar_deals=similar[:5],
            optimal_close_window=optimal_window,
            days_to_decision=days_to_decision,
        )

        # Cache prediction
        self._cache_prediction(prediction)

        return prediction

    def _generate_recommendations(
        self, deal: Dict, probability: float, risks: List[str]
    ) -> List[str]:
        """Generate actionable recommendations based on risks."""
        recommendations = []

        stage = deal.get("pipeline_stage")
        activity_count = deal.get("activity_count", 0)

        # Stage-specific recommendations
        if stage == "sourced":
            recommendations.append("Schedule initial screening call within 5 days")
        elif stage == "reviewing":
            if activity_count < 3:
                recommendations.append("Request financial documents and pitch deck")
        elif stage == "due_diligence":
            recommendations.append("Complete reference checks and technical review")
        elif stage == "negotiation":
            if probability >= 0.6:
                recommendations.append("Move to term sheet - high probability deal")

        # Risk-based recommendations
        for risk in risks:
            if "velocity" in risk.lower() or "stalled" in risk.lower():
                recommendations.append("Re-engage with founder - deal may be stalling")
            elif "activity" in risk.lower():
                recommendations.append("Schedule follow-up meeting this week")
            elif "valuation" in risk.lower():
                recommendations.append(
                    "Request updated cap table and comparable analysis"
                )

        # High probability optimization
        if probability >= 0.7:
            recommendations.append("Prioritize this deal - high win probability")

        return recommendations

    def _estimate_timing(
        self, deal: Dict, similar_deals: List[Dict]
    ) -> Tuple[str, int]:
        """Estimate optimal close window and days to decision."""
        stage = deal.get("pipeline_stage")

        # Stage-based estimates
        stage_days = {
            "sourced": 60,
            "reviewing": 45,
            "due_diligence": 30,
            "negotiation": 14,
        }

        base_days = stage_days.get(stage, 45)

        # Adjust based on similar deals
        if similar_deals:
            # Get average days to close for won deals
            # Note: We'd need closed_at - created_at from similar deals
            # For now, use heuristic
            pass

        days_to_decision = base_days

        if days_to_decision <= 14:
            window = "1-2 weeks"
        elif days_to_decision <= 30:
            window = "2-4 weeks"
        elif days_to_decision <= 45:
            window = "30-45 days"
        else:
            window = "45-60 days"

        return window, days_to_decision

    # -------------------------------------------------------------------------
    # CACHING
    # -------------------------------------------------------------------------

    def _get_cached_prediction(self, deal_id: int) -> Optional[DealPrediction]:
        """Get cached prediction if still valid (24 hours)."""
        query = text("""
            SELECT dp.*, d.company_name
            FROM deal_predictions dp
            JOIN deals d ON d.id = dp.deal_id
            WHERE dp.deal_id = :deal_id
              AND dp.model_version = :version
              AND dp.predicted_at > NOW() - INTERVAL '24 hours'
        """)
        try:
            result = self.db.execute(
                query, {"deal_id": deal_id, "version": MODEL_VERSION}
            )
            row = result.mappings().fetchone()
            if row:
                return DealPrediction(
                    deal_id=row["deal_id"],
                    company_name=row.get("company_name", ""),
                    win_probability=row["win_probability"],
                    confidence=row["confidence"],
                    tier=row["tier"],
                    company_score=row["company_score"] or 0,
                    deal_score=row["deal_score"] or 0,
                    pipeline_score=row["pipeline_score"] or 0,
                    pattern_score=row["pattern_score"] or 0,
                    strengths=row["strengths"] or [],
                    risks=row["risks"] or [],
                    recommendations=row["recommendations"] or [],
                    similar_deals=[],
                    optimal_close_window=row["optimal_close_window"] or "",
                    days_to_decision=row["days_to_decision"] or 0,
                )
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
        return None

    def _cache_prediction(self, prediction: DealPrediction) -> None:
        """Cache prediction result."""
        query = text("""
            INSERT INTO deal_predictions (
                deal_id, win_probability, confidence, tier,
                company_score, deal_score, pipeline_score, pattern_score,
                strengths, risks, recommendations,
                similar_deal_ids, optimal_close_window, days_to_decision,
                model_version
            ) VALUES (
                :deal_id, :win_probability, :confidence, :tier,
                :company_score, :deal_score, :pipeline_score, :pattern_score,
                :strengths, :risks, :recommendations,
                :similar_deal_ids, :optimal_close_window, :days_to_decision,
                :model_version
            )
            ON CONFLICT (deal_id, model_version) DO UPDATE SET
                win_probability = EXCLUDED.win_probability,
                confidence = EXCLUDED.confidence,
                tier = EXCLUDED.tier,
                company_score = EXCLUDED.company_score,
                deal_score = EXCLUDED.deal_score,
                pipeline_score = EXCLUDED.pipeline_score,
                pattern_score = EXCLUDED.pattern_score,
                strengths = EXCLUDED.strengths,
                risks = EXCLUDED.risks,
                recommendations = EXCLUDED.recommendations,
                similar_deal_ids = EXCLUDED.similar_deal_ids,
                optimal_close_window = EXCLUDED.optimal_close_window,
                days_to_decision = EXCLUDED.days_to_decision,
                predicted_at = CURRENT_TIMESTAMP
        """)

        import json

        try:
            self.db.execute(
                query,
                {
                    "deal_id": prediction.deal_id,
                    "win_probability": prediction.win_probability,
                    "confidence": prediction.confidence,
                    "tier": prediction.tier,
                    "company_score": prediction.company_score,
                    "deal_score": prediction.deal_score,
                    "pipeline_score": prediction.pipeline_score,
                    "pattern_score": prediction.pattern_score,
                    "strengths": json.dumps(prediction.strengths),
                    "risks": json.dumps(prediction.risks),
                    "recommendations": json.dumps(prediction.recommendations),
                    "similar_deal_ids": [
                        d["id"] for d in prediction.similar_deals if d.get("id")
                    ],
                    "optimal_close_window": prediction.optimal_close_window,
                    "days_to_decision": prediction.days_to_decision,
                    "model_version": MODEL_VERSION,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # PIPELINE ANALYSIS
    # -------------------------------------------------------------------------

    def score_pipeline(
        self, min_probability: float = 0.0, limit: int = 50
    ) -> List[Dict]:
        """Score all active deals in pipeline."""
        deals = self._get_active_deals()
        scored = []

        for deal in deals[:limit]:
            prediction = self.score_deal(deal["id"])
            if prediction and prediction.win_probability >= min_probability:
                # Calculate days in current stage
                created_at = deal.get("created_at")
                days_in_stage = 0
                if created_at:
                    if isinstance(created_at, str):
                        created_at = datetime.fromisoformat(
                            created_at.replace("Z", "+00:00")
                        )
                    days_in_stage = (
                        datetime.utcnow() - created_at.replace(tzinfo=None)
                    ).days

                scored.append(
                    {
                        "deal_id": prediction.deal_id,
                        "company_name": prediction.company_name,
                        "pipeline_stage": deal.get("pipeline_stage"),
                        "win_probability": prediction.win_probability,
                        "confidence": prediction.confidence,
                        "tier": prediction.tier,
                        "priority": deal.get("priority"),
                        "days_in_stage": days_in_stage,
                        "next_action": prediction.recommendations[0]
                        if prediction.recommendations
                        else None,
                    }
                )

        # Sort by probability descending
        scored.sort(key=lambda x: x["win_probability"], reverse=True)
        return scored

    def get_pipeline_insights(self) -> PipelineInsights:
        """Generate aggregate pipeline insights."""
        deals = self._get_active_deals()
        predictions = []

        for deal in deals:
            pred = self.score_deal(deal["id"])
            if pred:
                predictions.append((deal, pred))

        # Calculate summary stats
        total_value = sum(d.get("deal_size_millions", 0) or 0 for d, _ in predictions)
        probabilities = [p.win_probability for _, p in predictions]
        avg_prob = sum(probabilities) / len(probabilities) if probabilities else 0
        expected_value = sum(
            (d.get("deal_size_millions", 0) or 0) * p.win_probability
            for d, p in predictions
        )

        # Stage analysis
        stage_stats = {}
        for deal, pred in predictions:
            stage = deal.get("pipeline_stage")
            if stage not in stage_stats:
                stage_stats[stage] = {"count": 0, "probabilities": [], "days": []}

            stage_stats[stage]["count"] += 1
            stage_stats[stage]["probabilities"].append(pred.win_probability)

            created_at = deal.get("created_at")
            if created_at:
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    )
                days = (datetime.utcnow() - created_at.replace(tzinfo=None)).days
                stage_stats[stage]["days"].append(days)

        stage_analysis = []
        for stage in ACTIVE_STAGES:
            if stage in stage_stats:
                stats = stage_stats[stage]
                stage_analysis.append(
                    {
                        "stage": stage,
                        "count": stats["count"],
                        "avg_probability": sum(stats["probabilities"])
                        / len(stats["probabilities"]),
                        "avg_days": sum(stats["days"]) / len(stats["days"])
                        if stats["days"]
                        else 0,
                    }
                )

        # Risk alerts
        risk_alerts = []
        for deal, pred in predictions:
            # Stalled deals
            created_at = deal.get("created_at")
            if created_at:
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    )
                days = (datetime.utcnow() - created_at.replace(tzinfo=None)).days

                stage = deal.get("pipeline_stage")
                benchmark = STAGE_BENCHMARKS.get(stage, 21) * 1.5

                if days > benchmark:
                    risk_alerts.append(
                        {
                            "deal_id": deal["id"],
                            "company_name": deal.get("company_name"),
                            "alert": f"Stalled in {stage} for {days} days",
                            "recommendation": "Re-engage or pass",
                        }
                    )

        # Opportunities
        opportunities = []
        for deal, pred in sorted(
            predictions, key=lambda x: x[1].win_probability, reverse=True
        )[:5]:
            if pred.win_probability >= 0.6:
                opportunities.append(
                    {
                        "deal_id": deal["id"],
                        "company_name": deal.get("company_name"),
                        "insight": f"High-probability deal ({pred.win_probability:.0%}) - {pred.recommendations[0] if pred.recommendations else 'ready for advancement'}",
                    }
                )

        # Sector performance
        sector_stats = {}
        for deal, pred in predictions:
            sector = deal.get("company_sector") or "unknown"
            if sector not in sector_stats:
                sector_stats[sector] = {"deals": 0, "probabilities": []}
            sector_stats[sector]["deals"] += 1
            sector_stats[sector]["probabilities"].append(pred.win_probability)

        sector_performance = {
            sector: {
                "deals": stats["deals"],
                "avg_probability": sum(stats["probabilities"])
                / len(stats["probabilities"]),
            }
            for sector, stats in sector_stats.items()
        }

        return PipelineInsights(
            total_active_deals=len(predictions),
            total_pipeline_value=total_value,
            expected_value=expected_value,
            avg_win_probability=avg_prob,
            stage_analysis=stage_analysis,
            risk_alerts=risk_alerts[:5],
            opportunities=opportunities,
            sector_performance=sector_performance,
        )

    # -------------------------------------------------------------------------
    # SIMILAR DEALS
    # -------------------------------------------------------------------------

    def find_similar_deals(
        self, deal_id: int, limit: int = 5, include_lost: bool = True
    ) -> Dict:
        """Find similar historical deals."""
        deal = self._get_deal(deal_id)
        if not deal:
            return {"deal_id": deal_id, "similar_deals": [], "pattern_insights": {}}

        # Get historical deals
        stages = (
            ["closed_won", "closed_lost", "passed"] if include_lost else ["closed_won"]
        )
        query = text("""
            SELECT * FROM deals
            WHERE pipeline_stage = ANY(:stages)
            ORDER BY closed_at DESC
            LIMIT 100
        """)

        try:
            result = self.db.execute(query, {"stages": stages})
            historical = [dict(row) for row in result.mappings()]
        except Exception:
            historical = []

        # Calculate similarities
        _, _, _, similar = self._calculate_pattern_score(deal, historical)

        # Calculate pattern insights
        won_similar = [d for d in similar if d.get("outcome") == "closed_won"]
        win_rate = len(won_similar) / len(similar) if similar else 0

        return {
            "deal_id": deal_id,
            "similar_deals": similar[:limit],
            "pattern_insights": {
                "similar_deal_count": len(similar),
                "win_rate": win_rate,
                "common_success_factors": self._identify_success_factors(won_similar),
            },
        }

    def _identify_success_factors(self, won_deals: List[Dict]) -> List[str]:
        """Identify common factors in won deals."""
        factors = []

        if not won_deals:
            return factors

        # Check common patterns
        high_priority = sum(1 for d in won_deals if d.get("priority") == 1)
        if high_priority >= len(won_deals) / 2:
            factors.append("high_priority_assignment")

        # Check sectors
        sectors = [d.get("sector") for d in won_deals if d.get("sector")]
        if sectors:
            common_sector = max(set(sectors), key=sectors.count)
            factors.append(f"sector_{common_sector}")

        return factors

    # -------------------------------------------------------------------------
    # BATCH SCORING
    # -------------------------------------------------------------------------

    def score_batch(self, deal_ids: List[int]) -> List[Dict]:
        """Score multiple deals at once."""
        results = []
        for deal_id in deal_ids:
            pred = self.score_deal(deal_id)
            if pred:
                results.append(
                    {
                        "deal_id": pred.deal_id,
                        "company_name": pred.company_name,
                        "win_probability": pred.win_probability,
                        "confidence": pred.confidence,
                        "tier": pred.tier,
                    }
                )
        return results
