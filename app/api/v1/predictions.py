"""
Predictive Deal Scoring API Endpoints (T40)

Provides win probability predictions and pipeline insights for deals.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.deal_scorer import DealScorer, DealPrediction, PipelineInsights

router = APIRouter(prefix="/predictions", tags=["Deal Predictions"])


# =============================================================================
# SCHEMAS
# =============================================================================


class ScoreBreakdown(BaseModel):
    """Category score breakdown."""

    company_score: float = Field(..., description="Company quality score (0-100)")
    deal_score: float = Field(..., description="Deal characteristics score (0-100)")
    pipeline_score: float = Field(..., description="Pipeline signals score (0-100)")
    pattern_score: float = Field(..., description="Historical pattern score (0-100)")


class SimilarDeal(BaseModel):
    """A similar historical deal."""

    id: int
    company_name: str
    sector: Optional[str] = None
    deal_size_millions: Optional[float] = None
    outcome: str
    similarity_score: float
    similarity_factors: List[str] = []


class DealPredictionResponse(BaseModel):
    """Full prediction for a single deal."""

    deal_id: int
    company_name: str
    win_probability: float = Field(..., description="Win probability (0-1)")
    confidence: str = Field(..., description="Confidence level: high, medium, low")
    tier: str = Field(..., description="Deal tier: A, B, C, D, F")
    scores: ScoreBreakdown
    strengths: List[str] = []
    risks: List[str] = []
    recommendations: List[str] = []
    similar_deals: List[SimilarDeal] = []
    optimal_close_window: str = ""
    days_to_decision: int = 0


class PipelineDeal(BaseModel):
    """Deal in scored pipeline."""

    deal_id: int
    company_name: str
    pipeline_stage: str
    win_probability: float
    confidence: str
    tier: str
    priority: Optional[int] = None
    days_in_stage: int = 0
    next_action: Optional[str] = None


class PipelineSummary(BaseModel):
    """Pipeline summary statistics."""

    total_deals: int
    avg_probability: float
    high_confidence_count: int
    expected_wins: float


class ScoredPipelineResponse(BaseModel):
    """Scored pipeline with summary."""

    deals: List[PipelineDeal]
    summary: PipelineSummary


class StageAnalysis(BaseModel):
    """Stage-level analysis."""

    stage: str
    count: int
    avg_probability: float
    avg_days: float


class RiskAlert(BaseModel):
    """Risk alert for a deal."""

    deal_id: int
    company_name: str
    alert: str
    recommendation: str


class Opportunity(BaseModel):
    """High-probability opportunity."""

    deal_id: int
    company_name: str
    insight: str


class PipelineHealth(BaseModel):
    """Pipeline health metrics."""

    total_active_deals: int
    total_pipeline_value_millions: float
    expected_value_millions: float
    avg_win_probability: float


class SectorPerformance(BaseModel):
    """Sector performance stats."""

    deals: int
    avg_probability: float


class PipelineInsightsResponse(BaseModel):
    """Aggregate pipeline insights."""

    pipeline_health: PipelineHealth
    stage_analysis: List[StageAnalysis]
    risk_alerts: List[RiskAlert]
    opportunities: List[Opportunity]
    sector_performance: Dict[str, SectorPerformance]


class PatternInsights(BaseModel):
    """Pattern insights from similar deals."""

    similar_deal_count: int
    win_rate: float
    common_success_factors: List[str]


class SimilarDealsResponse(BaseModel):
    """Similar deals response."""

    deal_id: int
    similar_deals: List[SimilarDeal]
    pattern_insights: PatternInsights


class BatchScoreRequest(BaseModel):
    """Request to score multiple deals."""

    deal_ids: List[int] = Field(..., description="Deal IDs to score")


class BatchScoreResult(BaseModel):
    """Result for a single deal in batch."""

    deal_id: int
    company_name: str
    win_probability: float
    confidence: str
    tier: str


class BatchScoreResponse(BaseModel):
    """Batch scoring response."""

    results: List[BatchScoreResult]
    scored_count: int
    failed_count: int


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def prediction_to_response(pred: DealPrediction) -> DealPredictionResponse:
    """Convert DealPrediction to response model."""
    return DealPredictionResponse(
        deal_id=pred.deal_id,
        company_name=pred.company_name,
        win_probability=pred.win_probability,
        confidence=pred.confidence,
        tier=pred.tier,
        scores=ScoreBreakdown(
            company_score=pred.company_score,
            deal_score=pred.deal_score,
            pipeline_score=pred.pipeline_score,
            pattern_score=pred.pattern_score,
        ),
        strengths=pred.strengths,
        risks=pred.risks,
        recommendations=pred.recommendations,
        similar_deals=[
            SimilarDeal(
                id=d.get("id", 0),
                company_name=d.get("company_name", ""),
                sector=d.get("sector"),
                deal_size_millions=d.get("deal_size_millions"),
                outcome=d.get("outcome", ""),
                similarity_score=d.get("similarity_score", 0),
                similarity_factors=d.get("similarity_factors", []),
            )
            for d in pred.similar_deals
        ],
        optimal_close_window=pred.optimal_close_window,
        days_to_decision=pred.days_to_decision,
    )


def insights_to_response(insights: PipelineInsights) -> PipelineInsightsResponse:
    """Convert PipelineInsights to response model."""
    return PipelineInsightsResponse(
        pipeline_health=PipelineHealth(
            total_active_deals=insights.total_active_deals,
            total_pipeline_value_millions=insights.total_pipeline_value,
            expected_value_millions=insights.expected_value,
            avg_win_probability=insights.avg_win_probability,
        ),
        stage_analysis=[
            StageAnalysis(
                stage=s["stage"],
                count=s["count"],
                avg_probability=s["avg_probability"],
                avg_days=s["avg_days"],
            )
            for s in insights.stage_analysis
        ],
        risk_alerts=[
            RiskAlert(
                deal_id=r["deal_id"],
                company_name=r["company_name"],
                alert=r["alert"],
                recommendation=r["recommendation"],
            )
            for r in insights.risk_alerts
        ],
        opportunities=[
            Opportunity(
                deal_id=o["deal_id"],
                company_name=o["company_name"],
                insight=o["insight"],
            )
            for o in insights.opportunities
        ],
        sector_performance={
            sector: SectorPerformance(
                deals=stats["deals"],
                avg_probability=stats["avg_probability"],
            )
            for sector, stats in insights.sector_performance.items()
        },
    )


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/deal/{deal_id}", response_model=DealPredictionResponse)
async def score_deal(
    deal_id: int,
    use_cache: bool = Query(True, description="Use cached prediction if available"),
    db: Session = Depends(get_db),
):
    """
    Score a single deal with full breakdown.

    Returns win probability, category scores, strengths, risks,
    recommendations, and similar historical deals.
    """
    scorer = DealScorer(db)
    prediction = scorer.score_deal(deal_id, use_cache=use_cache)

    if not prediction:
        raise HTTPException(
            status_code=404, detail=f"Deal {deal_id} not found or is already closed"
        )

    return prediction_to_response(prediction)


@router.get("/pipeline", response_model=ScoredPipelineResponse)
async def get_scored_pipeline(
    pipeline_stage: Optional[str] = Query(None, description="Filter by stage"),
    min_probability: float = Query(
        0.0, description="Minimum win probability", ge=0, le=1
    ),
    limit: int = Query(50, description="Maximum results", le=200),
    db: Session = Depends(get_db),
):
    """
    Get all active deals with scores, sorted by win probability.

    Returns deals with their predictions and a summary of the pipeline.
    """
    scorer = DealScorer(db)
    scored_deals = scorer.score_pipeline(min_probability=min_probability, limit=limit)

    # Filter by stage if specified
    if pipeline_stage:
        scored_deals = [
            d for d in scored_deals if d.get("pipeline_stage") == pipeline_stage
        ]

    # Build response
    deals = [
        PipelineDeal(
            deal_id=d["deal_id"],
            company_name=d["company_name"],
            pipeline_stage=d.get("pipeline_stage", ""),
            win_probability=d["win_probability"],
            confidence=d["confidence"],
            tier=d["tier"],
            priority=d.get("priority"),
            days_in_stage=d.get("days_in_stage", 0),
            next_action=d.get("next_action"),
        )
        for d in scored_deals
    ]

    # Calculate summary
    probabilities = [d.win_probability for d in deals]
    high_confidence = sum(1 for d in deals if d.confidence == "high")
    expected_wins = sum(probabilities) if probabilities else 0

    summary = PipelineSummary(
        total_deals=len(deals),
        avg_probability=sum(probabilities) / len(probabilities) if probabilities else 0,
        high_confidence_count=high_confidence,
        expected_wins=expected_wins,
    )

    return ScoredPipelineResponse(deals=deals, summary=summary)


@router.get("/similar/{deal_id}", response_model=SimilarDealsResponse)
async def get_similar_deals(
    deal_id: int,
    limit: int = Query(5, description="Maximum similar deals", le=20),
    include_lost: bool = Query(True, description="Include closed_lost deals"),
    db: Session = Depends(get_db),
):
    """
    Find similar historical deals to guide strategy.

    Returns similar deals based on sector, size, and source,
    along with pattern insights like win rate.
    """
    scorer = DealScorer(db)
    result = scorer.find_similar_deals(
        deal_id=deal_id, limit=limit, include_lost=include_lost
    )

    if not result.get("similar_deals") and result.get("deal_id") == deal_id:
        # Check if deal exists
        deal = scorer._get_deal(deal_id)
        if not deal:
            raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found")

    pattern_insights = result.get("pattern_insights", {})

    return SimilarDealsResponse(
        deal_id=result["deal_id"],
        similar_deals=[
            SimilarDeal(
                id=d.get("id", 0),
                company_name=d.get("company_name", ""),
                sector=d.get("sector"),
                deal_size_millions=d.get("deal_size_millions"),
                outcome=d.get("outcome", ""),
                similarity_score=d.get("similarity_score", 0),
                similarity_factors=d.get("similarity_factors", []),
            )
            for d in result.get("similar_deals", [])
        ],
        pattern_insights=PatternInsights(
            similar_deal_count=pattern_insights.get("similar_deal_count", 0),
            win_rate=pattern_insights.get("win_rate", 0),
            common_success_factors=pattern_insights.get("common_success_factors", []),
        ),
    )


@router.get("/insights", response_model=PipelineInsightsResponse)
async def get_pipeline_insights(db: Session = Depends(get_db)):
    """
    Get aggregate insights across the pipeline.

    Returns pipeline health metrics, stage analysis, risk alerts,
    opportunities, and sector performance breakdown.
    """
    scorer = DealScorer(db)
    insights = scorer.get_pipeline_insights()

    return insights_to_response(insights)


@router.post("/batch", response_model=BatchScoreResponse)
async def score_batch(request: BatchScoreRequest, db: Session = Depends(get_db)):
    """
    Score multiple deals at once.

    Returns predictions for all requested deals that exist and are active.
    """
    if not request.deal_ids:
        raise HTTPException(status_code=400, detail="deal_ids cannot be empty")

    if len(request.deal_ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 deals per batch")

    scorer = DealScorer(db)
    results = scorer.score_batch(request.deal_ids)

    return BatchScoreResponse(
        results=[
            BatchScoreResult(
                deal_id=r["deal_id"],
                company_name=r["company_name"],
                win_probability=r["win_probability"],
                confidence=r["confidence"],
                tier=r["tier"],
            )
            for r in results
        ],
        scored_count=len(results),
        failed_count=len(request.deal_ids) - len(results),
    )


@router.get("/methodology")
async def get_methodology():
    """
    Get scoring methodology documentation.

    Explains how win probability is calculated.
    """
    return {
        "model_version": "v1.0",
        "description": "Predictive model combining multiple signals to estimate deal win probability",
        "category_weights": {
            "company_quality": {
                "weight": 0.40,
                "description": "Company health from T36 scoring (growth, stability, market position, tech velocity)",
                "signals": ["composite_score", "tier", "growth_trajectory"],
            },
            "deal_characteristics": {
                "weight": 0.30,
                "description": "Deal size, valuation, and sector fit analysis",
                "signals": [
                    "size_in_sweet_spot",
                    "valuation_reasonableness",
                    "thesis_fit",
                ],
            },
            "pipeline_signals": {
                "weight": 0.20,
                "description": "Pipeline velocity, activity frequency, and priority",
                "signals": ["days_in_stage", "activity_count", "recency", "priority"],
            },
            "historical_patterns": {
                "weight": 0.10,
                "description": "Win rates for similar deals by sector, size, and source",
                "signals": ["similar_deal_win_rate", "source_track_record"],
            },
        },
        "confidence_thresholds": {
            "high": "70%+ win probability",
            "medium": "40-70% win probability",
            "low": "<40% win probability",
        },
        "tiers": {
            "A": "80-100% probability",
            "B": "60-79% probability",
            "C": "40-59% probability",
            "D": "20-39% probability",
            "F": "0-19% probability",
        },
        "sector_sweet_spots": {
            "fintech": "$5-50M",
            "healthtech": "$10-75M",
            "saas": "$5-40M",
            "ai": "$10-100M",
            "climate": "$15-80M",
        },
        "cache_duration": "24 hours",
        "update_triggers": ["deal_update", "manual_refresh"],
    }
