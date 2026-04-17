"""
Deal Probability Engine — REST API (SPEC 046, PLAN_059 Phase 2).

10 endpoints exposing the TransactionProbabilityEngine:
- POST /score/{company_id}   — score a single company
- POST /scan                 — batch-score the universe
- GET  /rankings             — top companies by probability
- GET  /company/{id}         — latest detail + signal chain
- GET  /company/{id}/history — signal time-series
- GET  /company/{id}/signals — latest signals with velocity
- GET  /stats                — dashboard stats
- GET  /sectors              — per-sector summary
- GET  /alerts               — recent alerts (placeholder for Phase 3)
- GET  /methodology          — static methodology doc
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.probability_models import TxnProbAlert, TxnProbCompany, TxnProbSignal
from app.ml.probability_signal_taxonomy import (
    SECTOR_WEIGHT_OVERRIDES,
    SIGNAL_TAXONOMY,
)
from app.ml.probability_calibrator import (
    MIN_SAMPLES_FOR_FIT,
    ProbabilityCalibrator,
)
from app.ml.probability_model import (
    MIN_LABELED_SAMPLES as ML_MIN_SAMPLES,
    ModelUnavailableError,
    is_lightgbm_available,
    is_shap_available,
)
from app.ml.probability_weight_optimizer import (
    MIN_SAMPLES_FOR_OPTIMIZATION,
    SignalWeightOptimizer,
)
from app.services.probability_convergence import (
    CONVERGENCE_PATTERNS,
    ConvergenceDetector,
)
from app.services.probability_engine import (
    MODEL_VERSION,
    WEIGHTS_VERSION,
    TransactionProbabilityEngine,
)
from app.services.probability_narrative import ProbabilityNarrativeGenerator
from app.services.probability_nlq import ProbabilityNLQ
from app.services.probability_outcome_tracker import OutcomeTracker
from app.services.probability_universe import CompanyUniverseBuilder

router = APIRouter(prefix="/txn-probability", tags=["Transaction Probability"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ScoreResponse(BaseModel):
    company_id: int
    company_name: str
    probability: float
    raw_composite_score: float
    grade: str
    confidence: float
    signal_count: int
    active_signal_count: int
    convergence_factor: float
    top_signals: List[Dict[str, Any]] = []
    signal_chain: List[Dict[str, Any]] = []
    convergences: List[Dict[str, Any]] = []
    model_version: str
    batch_id: Optional[str] = None
    scored_at: Optional[str] = None


class ScanResponse(BaseModel):
    batch_id: str
    total_companies: int
    succeeded: int
    failed: int


class RankingEntry(BaseModel):
    company_id: int
    company_name: str
    sector: Optional[str] = None
    hq_state: Optional[str] = None
    probability: float
    raw_composite_score: float
    grade: Optional[str] = None
    confidence: float
    active_signal_count: int
    top_signals: List[Dict[str, Any]] = []
    scored_at: Optional[str] = None


class StatsResponse(BaseModel):
    universe_size: int
    total_scored: int
    avg_probability: float
    hot_count: int
    model_version: str
    weights_version: str


class SectorSummaryEntry(BaseModel):
    sector: Optional[str] = None
    company_count: int
    avg_probability: float
    max_probability: float


class SignalSnapshot(BaseModel):
    signal_type: str
    score: float
    previous_score: Optional[float] = None
    velocity: Optional[float] = None
    acceleration: Optional[float] = None
    confidence: Optional[float] = None
    scored_at: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/score/{company_id}",
    response_model=ScoreResponse,
    summary="Score a single company",
    description="Compute all 12 signals, probability, and signal chain for a company.",
)
def score_company(company_id: int, db: Session = Depends(get_db)) -> ScoreResponse:
    try:
        result = TransactionProbabilityEngine(db).score_company(company_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return ScoreResponse(**result)


@router.post(
    "/scan",
    response_model=ScanResponse,
    summary="Batch-score the universe",
    description="Score all active companies. Use for scheduled daily refresh.",
)
def scan_universe(
    build_universe_first: bool = Query(
        default=False,
        description="If true, refresh the company universe from sources before scoring.",
    ),
    db: Session = Depends(get_db),
) -> ScanResponse:
    if build_universe_first:
        CompanyUniverseBuilder(db).refresh_universe()
    stats = TransactionProbabilityEngine(db).score_universe()
    return ScanResponse(**stats)


@router.get(
    "/rankings",
    response_model=List[RankingEntry],
    summary="Top companies by probability",
    description="Ranked list of companies, filterable by sector, grade, and minimum probability.",
)
def get_rankings(
    sector: Optional[str] = Query(None, description="Filter by sector (e.g., Healthcare)"),
    min_probability: float = Query(0.0, ge=0.0, le=1.0),
    grade: Optional[str] = Query(None, description="Filter by grade (A/B/C/D/F)"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[RankingEntry]:
    rankings = TransactionProbabilityEngine(db).get_rankings(
        sector=sector,
        min_probability=min_probability,
        limit=limit,
        grade=grade,
    )
    return [RankingEntry(**r) for r in rankings]


@router.get(
    "/company/{company_id}",
    summary="Company detail with latest signal chain",
    description="Returns company metadata + latest composite score + full signal chain.",
)
def get_company_detail(company_id: int, db: Session = Depends(get_db)) -> Dict:
    detail = TransactionProbabilityEngine(db).get_company_detail(company_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")
    return detail


@router.get(
    "/company/{company_id}/history",
    summary="Signal time-series for a company",
    description="Historical snapshots of signal scores with velocity and acceleration.",
)
def get_company_history(
    company_id: int,
    signal_type: Optional[str] = Query(None),
    periods: int = Query(24, ge=1, le=200),
    db: Session = Depends(get_db),
) -> List[Dict]:
    return TransactionProbabilityEngine(db).get_signal_history(
        company_id=company_id, signal_type=signal_type, periods=periods
    )


@router.get(
    "/company/{company_id}/signals",
    summary="Latest signal snapshot per signal type",
    description="Returns one row per signal type with the most recent score, velocity, and acceleration.",
)
def get_company_signals(company_id: int, db: Session = Depends(get_db)) -> List[SignalSnapshot]:
    # Find the latest snapshot per signal_type
    subq = (
        db.query(
            TxnProbSignal.signal_type,
            db.query(TxnProbSignal.scored_at)
            .filter(TxnProbSignal.company_id == company_id)
            .correlate(TxnProbSignal)
            .label("latest"),
        )
    )
    # Simpler approach: fetch all and group by signal_type in Python
    all_signals = (
        db.query(TxnProbSignal)
        .filter_by(company_id=company_id)
        .order_by(TxnProbSignal.scored_at.desc())
        .all()
    )
    latest_by_type: Dict[str, TxnProbSignal] = {}
    for s in all_signals:
        if s.signal_type not in latest_by_type:
            latest_by_type[s.signal_type] = s
    return [
        SignalSnapshot(
            signal_type=s.signal_type,
            score=s.score,
            previous_score=s.previous_score,
            velocity=s.velocity,
            acceleration=s.acceleration,
            confidence=s.confidence,
            scored_at=s.scored_at.isoformat() if s.scored_at else None,
            details=s.signal_details,
        )
        for s in latest_by_type.values()
    ]


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Dashboard statistics",
    description="Universe size, total scored, average probability, hot-count.",
)
def get_stats(db: Session = Depends(get_db)) -> StatsResponse:
    return StatsResponse(**TransactionProbabilityEngine(db).get_stats())


@router.get(
    "/sectors",
    response_model=List[SectorSummaryEntry],
    summary="Per-sector probability summary",
)
def get_sector_summaries(db: Session = Depends(get_db)) -> List[SectorSummaryEntry]:
    sectors = TransactionProbabilityEngine(db).get_sectors()
    return [SectorSummaryEntry(**s) for s in sectors]


@router.get(
    "/alerts",
    summary="Recent probability alerts",
    description="Phase 3 alert engine populates this. Returns empty list until then.",
)
def get_alerts(
    is_read: Optional[bool] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[Dict]:
    q = db.query(TxnProbAlert)
    if is_read is not None:
        q = q.filter(TxnProbAlert.is_read == is_read)
    if severity:
        q = q.filter(TxnProbAlert.severity == severity)
    rows = q.order_by(TxnProbAlert.created_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "company_id": r.company_id,
            "alert_type": r.alert_type,
            "severity": r.severity,
            "title": r.title,
            "description": r.description,
            "probability_before": r.probability_before,
            "probability_after": r.probability_after,
            "triggering_signals": r.triggering_signals,
            "is_read": r.is_read,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Phase 3 — Intelligence Layer endpoints
# ---------------------------------------------------------------------------


class NLQRequest(BaseModel):
    query: str = Field(..., description="Natural-language question")
    limit: int = Field(default=25, ge=1, le=200)


@router.post(
    "/company/{company_id}/narrative",
    summary="Generate an AI narrative for a company",
    description="3-5 sentence LLM-backed explainer of why the company scores what it does. Falls back to template on LLM failure.",
)
async def generate_narrative(company_id: int, db: Session = Depends(get_db)) -> Dict:
    result = await ProbabilityNarrativeGenerator(db).generate_narrative(company_id)
    if result.get("source") == "error":
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.post(
    "/company/{company_id}/memo",
    summary="Generate a deal memo for a company",
    description="6-section memo (exec summary, signals, comparables, risks, recommendation, timing) plus self-contained HTML.",
)
async def generate_memo(company_id: int, db: Session = Depends(get_db)) -> Dict:
    result = await ProbabilityNarrativeGenerator(db).generate_memo(company_id)
    if result.get("source") == "error":
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.get(
    "/convergences",
    summary="Companies with active named convergence patterns",
    description="Scans all latest-scored companies and returns those matching ≥1 convergence pattern (classic_exit_setup, founder_transition, etc.).",
)
def list_convergences(db: Session = Depends(get_db)) -> Dict:
    detector = ConvergenceDetector(db)
    companies = detector.scan_all_companies()
    return {
        "pattern_count": len(CONVERGENCE_PATTERNS),
        "patterns": [
            {
                "key": p.key,
                "label": p.label,
                "description": p.description,
                "severity": p.severity,
            }
            for p in CONVERGENCE_PATTERNS.values()
        ],
        "companies_with_matches": len(companies),
        "companies": companies,
    }


@router.post(
    "/query",
    summary="Natural-language query",
    description="Parse NL query via Claude (keyword fallback on failure), validate against whitelist, execute against latest scores.",
)
async def natural_language_query(
    request: NLQRequest, db: Session = Depends(get_db)
) -> Dict:
    result = await ProbabilityNLQ(db).query(request.query, limit=request.limit)
    return {
        "query": result.query,
        "filters": result.filters,
        "results": result.results,
        "explanation": result.explanation,
        "total_matches": result.total_matches,
    }


@router.get(
    "/sector/{sector}/briefing",
    summary="AI-generated sector briefing",
    description="4-6 sentence briefing summarizing top movers in a sector. LLM-backed with template fallback.",
)
async def sector_briefing(
    sector: str, top_n: int = Query(5, ge=1, le=20), db: Session = Depends(get_db)
) -> Dict:
    return await ProbabilityNarrativeGenerator(db).generate_sector_briefing(
        sector, top_n
    )


# ---------------------------------------------------------------------------
# Phase 4 — Learning Loop endpoints
# ---------------------------------------------------------------------------


class CalibrationFitRequest(BaseModel):
    scope: str = Field(default="global", description="'global' or sector name")
    method: str = Field(default="platt", description="'platt' or 'isotonic'")


@router.post(
    "/outcomes/scan",
    summary="Scan pe_deals for real transactions and populate outcomes",
    description="Phase 4 learning loop — reads pe_deals, writes txn_prob_outcomes rows for universe companies, then backfills our prior probability snapshots.",
)
def scan_outcomes(
    lookback_days: int = Query(730, ge=30, le=3650),
    backfill: bool = Query(True, description="Also backfill historical prediction snapshots"),
    db: Session = Depends(get_db),
) -> Dict:
    tracker = OutcomeTracker(db)
    scan_stats = tracker.scan_for_outcomes(lookback_days=lookback_days)
    backfill_stats = tracker.backfill_predictions() if backfill else {}
    metrics = tracker.compute_calibration_metrics()
    return {
        "scan": scan_stats,
        "backfill": backfill_stats,
        "metrics": metrics,
    }


@router.get(
    "/calibration",
    summary="List fitted calibrations",
    description="Returns all persisted calibration models with their parameters, Brier scores, and active status.",
)
def list_calibrations(
    scope: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> Dict:
    cal = ProbabilityCalibrator(db)
    return {
        "min_samples_to_fit": MIN_SAMPLES_FOR_FIT,
        "calibrations": cal.list_calibrations(scope=scope),
    }


@router.post(
    "/calibration/fit",
    summary="Fit a new calibration from labeled outcomes",
    description="Builds a labeled dataset, fits Platt or isotonic calibration, and persists + activates it at the requested scope.",
)
def fit_calibration(
    request: CalibrationFitRequest,
    db: Session = Depends(get_db),
) -> Dict:
    tracker = OutcomeTracker(db)
    df = tracker.get_labeled_dataset()
    if df is None or df.empty:
        raise HTTPException(
            status_code=400,
            detail="No labeled dataset available. Run POST /outcomes/scan first.",
        )

    # Filter to positive/negative examples (both must exist for fit to work)
    if "raw_composite_score" not in df.columns or "outcome_within_12mo" not in df.columns:
        raise HTTPException(status_code=400, detail="Dataset missing required columns.")

    if request.scope != "global":
        # Future: join with TxnProbCompany to filter by sector. For now, global only
        # until we wire sector into the dataset builder.
        pass

    raw = df["raw_composite_score"].dropna().tolist()
    outcomes = df["outcome_within_12mo"].astype(int).tolist()
    result = ProbabilityCalibrator(db).fit_and_persist(
        raw_scores=raw,
        outcomes=outcomes,
        scope=request.scope,
        method=request.method,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("reason"))
    return result


@router.post(
    "/weights/optimize",
    summary="Optimize per-signal weights via AUC-ROC",
    description=f"Requires ≥ {MIN_SAMPLES_FOR_OPTIMIZATION} labeled samples. Returns new weights + AUC.",
)
def optimize_weights(db: Session = Depends(get_db)) -> Dict:
    tracker = OutcomeTracker(db)
    df = tracker.get_labeled_dataset()
    if df is None or df.empty or len(df) < MIN_SAMPLES_FOR_OPTIMIZATION:
        return {
            "ok": False,
            "reason": f"need ≥ {MIN_SAMPLES_FOR_OPTIMIZATION} samples (got {0 if df is None else len(df)})",
        }

    import numpy as np
    from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

    signal_cols = [s for s in SIGNAL_TAXONOMY.keys() if s in df.columns]
    if len(signal_cols) < len(SIGNAL_TAXONOMY):
        return {
            "ok": False,
            "reason": f"dataset missing signal columns: {set(SIGNAL_TAXONOMY) - set(signal_cols)}",
        }

    feature_matrix = df[signal_cols].fillna(50).to_numpy(dtype=float)
    outcomes = df["outcome_within_12mo"].astype(int).to_numpy()
    opt = SignalWeightOptimizer(db)
    result = opt.optimize_weights(feature_matrix, outcomes)
    if result.get("ok"):
        result["univariate_importance"] = opt.compute_signal_importance(
            feature_matrix, outcomes
        )
    return result


@router.get(
    "/model/status",
    summary="ML model availability and status",
    description="Reports whether LightGBM is installed, whether the model is trained, and how many labeled samples are available.",
)
def model_status(db: Session = Depends(get_db)) -> Dict:
    tracker = OutcomeTracker(db)
    df = tracker.get_labeled_dataset()
    n_labeled = 0 if df is None or df.empty else len(df)
    return {
        "lightgbm_available": is_lightgbm_available(),
        "shap_available": is_shap_available(),
        "min_samples_for_training": ML_MIN_SAMPLES,
        "current_labeled_samples": n_labeled,
        "trainable": is_lightgbm_available() and n_labeled >= ML_MIN_SAMPLES,
        "active_calibration": bool(
            ProbabilityCalibrator(db).list_calibrations(scope="global")
        ),
    }


@router.get(
    "/methodology",
    summary="Engine methodology documentation",
    description="Static methodology doc: signal taxonomy, weights, composite formula, calibration.",
)
def get_methodology() -> Dict:
    return {
        "model_version": MODEL_VERSION,
        "weights_version": WEIGHTS_VERSION,
        "composite_formula": (
            "raw = min(100, weighted_sum * (1 + above_60_count * 0.08)); "
            "probability = 1 / (1 + exp(-0.08 * (raw - 55)))"
        ),
        "signal_count": len(SIGNAL_TAXONOMY),
        "signals": [
            {
                "signal_type": k,
                "default_weight": v["default_weight"],
                "description": v.get("description"),
                "refresh_cadence": v.get("refresh_cadence"),
                "source_kind": v.get("scorer_source"),
            }
            for k, v in SIGNAL_TAXONOMY.items()
        ],
        "sector_overrides": list(SECTOR_WEIGHT_OVERRIDES.keys()),
        "grade_thresholds": {"A": 85, "B": 70, "C": 55, "D": 40, "F": 0},
        "calibration": {
            "method": "sigmoid",
            "params": {"k": 0.08, "x0": 55},
            "note": "Sector-specific Platt scaling applied in Phase 4 when outcome data is sufficient.",
        },
    }
