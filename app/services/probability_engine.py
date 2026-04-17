"""
Deal Probability Engine — Core Scoring Orchestrator (SPEC 046, PLAN_059 Phase 2).

TransactionProbabilityEngine composes existing scorers + new signal computers
into a single calibrated P(transaction in 6-12 months). Each score fully
decomposes into an explainable signal chain.

Formula:
  weighted_sum = sum(signal.score * sector_weight[signal.type])
  convergence_factor = 1 + (above_60_count * 0.08)
  raw_composite = min(100, weighted_sum * convergence_factor)
  probability = sigmoid(raw_composite)  # k=0.08, x0=55 — calibrated in Phase 4

Reuses existing scorers via composition (no modifications):
  - CompanyScorer              → financial_health
  - ExitReadinessScorer        → exit_readiness
  - AcquisitionTargetScorer    → acquisition_attractiveness
  - ExecSignalScorer           → exec_transition
  - CompanyDiligenceScorer     → diligence_health

Plus 6 new computers in probability_signal_computers.py.
"""

from __future__ import annotations

import importlib
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.probability_models import (
    TxnProbCompany,
    TxnProbScore,
    TxnProbSignal,
)
from app.ml.probability_calibrator import (
    calibrate_with_active,
    get_active_calibration,
)
from app.ml.probability_signal_taxonomy import (
    SIGNAL_TAXONOMY,
    get_weights_for_sector,
)
from app.services.probability_alerts import AlertEngine
from app.services.probability_convergence import ConvergenceDetector
from app.services.probability_signal_computers import (
    NEUTRAL_SCORE,
    NEW_COMPUTERS,
    SignalResult,
)

logger = logging.getLogger(__name__)


# Engine configuration
MODEL_VERSION = "v1.0"
WEIGHTS_VERSION = "v1.0"
SIGNAL_ABOVE_THRESHOLD = 60.0
CONVERGENCE_PER_SIGNAL_BOOST = 0.08
SIGMOID_K = 0.08
SIGMOID_X0 = 55.0


@dataclass
class CompositeResult:
    raw_composite_score: float
    convergence_factor: float
    active_signal_count: int


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class TransactionProbabilityEngine:
    """Scores companies end-to-end and persists the full signal chain."""

    def __init__(self, db: Session):
        self.db = db
        # Lazy-load scorer classes on first score
        self._scorer_cache: Dict[str, object] = {}
        self._computer_cache: Dict[str, object] = {}

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def score_company(
        self, company_id: int, batch_id: Optional[str] = None
    ) -> Dict:
        """
        Score a single company end-to-end.

        Computes all 12 signals, composes into calibrated probability,
        persists to txn_prob_signals + txn_prob_scores.
        Returns the full result dict (probability, raw, grade, signal_chain).
        """
        company = self.db.query(TxnProbCompany).filter_by(id=company_id).first()
        if not company:
            raise ValueError(f"Company {company_id} not found")

        # Capture previous score BEFORE persisting the new one (for alerts)
        previous_score = (
            self.db.query(TxnProbScore)
            .filter_by(company_id=company_id)
            .order_by(TxnProbScore.scored_at.desc())
            .first()
        )

        batch_id = batch_id or self._new_batch_id()
        signals = self._compute_signals(company)
        weights = get_weights_for_sector(company.sector or "")

        composite = self._compute_composite(signals, weights)
        # Phase 4: prefer a fitted calibration (sector-specific, then global);
        # fall back to the default sigmoid.
        probability = self._calibrate_to_probability(
            composite.raw_composite_score, company.sector or ""
        )
        grade = self._grade_from_score(composite.raw_composite_score)

        signal_chain = self._build_signal_chain(signals, weights)
        top_signals = self._top_signals(signal_chain, n=5)

        confidence = (
            sum(s.confidence for s in signals) / len(signals) if signals else 0.0
        )

        # Phase 3: detect named convergence patterns from the fresh signals
        convergences: List[Dict] = []
        try:
            convergences = ConvergenceDetector(self.db).detect_from_signals(signals)
        except Exception as exc:
            logger.debug("convergence detection failed for %s: %s", company.id, exc)

        # Persist signal snapshots (with velocity/acceleration)
        self._persist_signals(company.id, signals, batch_id)

        # Persist composite score
        score_row = TxnProbScore(
            company_id=company.id,
            probability=probability,
            raw_composite_score=composite.raw_composite_score,
            grade=grade,
            confidence=confidence,
            sector_weights_version=WEIGHTS_VERSION,
            signal_count=len(signals),
            active_signal_count=composite.active_signal_count,
            convergence_factor=composite.convergence_factor,
            top_signals=top_signals,
            signal_chain=signal_chain,
            model_version=MODEL_VERSION,
            batch_id=batch_id,
        )
        self.db.add(score_row)
        self.db.flush()

        # Phase 3: evaluate alerts against the previous score
        try:
            AlertEngine(self.db).evaluate(
                company_id=company.id,
                prev_score=previous_score,
                new_score_row=score_row,
                new_convergences=convergences,
                company_name=company.company_name,
            )
        except Exception as exc:
            logger.debug("alert evaluation failed for %s: %s", company.id, exc)

        self.db.commit()

        return {
            "company_id": company.id,
            "company_name": company.company_name,
            "probability": probability,
            "raw_composite_score": composite.raw_composite_score,
            "grade": grade,
            "confidence": confidence,
            "signal_count": len(signals),
            "active_signal_count": composite.active_signal_count,
            "convergence_factor": composite.convergence_factor,
            "top_signals": top_signals,
            "signal_chain": signal_chain,
            "model_version": MODEL_VERSION,
            "batch_id": batch_id,
            "scored_at": score_row.scored_at.isoformat() if score_row.scored_at else None,
            "convergences": convergences,
        }

    def score_universe(self, batch_size: int = 100) -> Dict:
        """Batch-score all active companies."""
        batch_id = self._new_batch_id()
        active_companies = (
            self.db.query(TxnProbCompany).filter_by(is_active=True).all()
        )

        succeeded = 0
        failed = 0
        for c in active_companies:
            try:
                self.score_company(c.id, batch_id=batch_id)
                succeeded += 1
            except Exception as exc:
                failed += 1
                logger.warning("scoring company %s failed: %s", c.id, exc)
                self.db.rollback()

        return {
            "batch_id": batch_id,
            "total_companies": len(active_companies),
            "succeeded": succeeded,
            "failed": failed,
        }

    def get_rankings(
        self,
        sector: Optional[str] = None,
        min_probability: float = 0.0,
        limit: int = 50,
        grade: Optional[str] = None,
    ) -> List[Dict]:
        """Return top companies by latest probability."""
        # Latest score per company — use a subquery for max(scored_at)
        latest_subq = (
            self.db.query(
                TxnProbScore.company_id,
                func.max(TxnProbScore.scored_at).label("latest_at"),
            )
            .group_by(TxnProbScore.company_id)
            .subquery()
        )

        q = (
            self.db.query(TxnProbScore, TxnProbCompany)
            .join(
                latest_subq,
                (TxnProbScore.company_id == latest_subq.c.company_id)
                & (TxnProbScore.scored_at == latest_subq.c.latest_at),
            )
            .join(TxnProbCompany, TxnProbCompany.id == TxnProbScore.company_id)
            .filter(TxnProbScore.probability >= min_probability)
            .filter(TxnProbCompany.is_active == True)  # noqa: E712
        )
        if sector:
            q = q.filter(TxnProbCompany.sector == sector)
        if grade:
            q = q.filter(TxnProbScore.grade == grade)

        rows = q.order_by(TxnProbScore.probability.desc()).limit(limit).all()

        return [
            {
                "company_id": c.id,
                "company_name": c.company_name,
                "sector": c.sector,
                "hq_state": c.hq_state,
                "probability": s.probability,
                "raw_composite_score": s.raw_composite_score,
                "grade": s.grade,
                "confidence": s.confidence,
                "active_signal_count": s.active_signal_count,
                "top_signals": s.top_signals,
                "scored_at": s.scored_at.isoformat() if s.scored_at else None,
            }
            for s, c in rows
        ]

    def get_company_detail(self, company_id: int) -> Optional[Dict]:
        """Return full signal chain for latest score."""
        company = self.db.query(TxnProbCompany).filter_by(id=company_id).first()
        if not company:
            return None

        latest_score = (
            self.db.query(TxnProbScore)
            .filter_by(company_id=company_id)
            .order_by(TxnProbScore.scored_at.desc())
            .first()
        )
        if not latest_score:
            return {
                "company": company.to_dict(),
                "latest_score": None,
                "message": "Company not yet scored. Call POST /score/{id}.",
            }

        return {
            "company": company.to_dict(),
            "latest_score": latest_score.to_dict(),
        }

    def get_signal_history(
        self,
        company_id: int,
        signal_type: Optional[str] = None,
        periods: int = 12,
    ) -> List[Dict]:
        """Return time-series of signal snapshots."""
        q = self.db.query(TxnProbSignal).filter_by(company_id=company_id)
        if signal_type:
            q = q.filter_by(signal_type=signal_type)
        rows = q.order_by(TxnProbSignal.scored_at.desc()).limit(periods).all()
        return [r.to_dict() for r in rows]

    def get_stats(self) -> Dict:
        """Dashboard statistics."""
        universe_size = (
            self.db.query(TxnProbCompany).filter_by(is_active=True).count()
        )
        total_scored = (
            self.db.query(TxnProbScore.company_id).distinct().count()
        )
        avg_prob = (
            self.db.query(func.avg(TxnProbScore.probability)).scalar() or 0
        )
        hot_count = (
            self.db.query(TxnProbScore)
            .filter(TxnProbScore.probability >= 0.7)
            .distinct(TxnProbScore.company_id)
            .count()
        )
        return {
            "universe_size": universe_size,
            "total_scored": total_scored,
            "avg_probability": round(float(avg_prob), 4),
            "hot_count": hot_count,
            "model_version": MODEL_VERSION,
            "weights_version": WEIGHTS_VERSION,
        }

    def get_sectors(self) -> List[Dict]:
        """Per-sector summary."""
        rows = (
            self.db.query(
                TxnProbCompany.sector,
                func.count(TxnProbCompany.id).label("company_count"),
                func.avg(TxnProbScore.probability).label("avg_prob"),
                func.max(TxnProbScore.probability).label("max_prob"),
            )
            .outerjoin(TxnProbScore, TxnProbScore.company_id == TxnProbCompany.id)
            .filter(TxnProbCompany.is_active == True)  # noqa: E712
            .group_by(TxnProbCompany.sector)
            .all()
        )
        return [
            {
                "sector": r.sector,
                "company_count": r.company_count,
                "avg_probability": round(float(r.avg_prob or 0), 4),
                "max_probability": round(float(r.max_prob or 0), 4),
            }
            for r in rows
        ]

    # -------------------------------------------------------------------
    # Signal computation
    # -------------------------------------------------------------------

    def _compute_signals(self, company: TxnProbCompany) -> List[SignalResult]:
        """Compute all 12 signals for a company."""
        results: List[SignalResult] = []
        for signal_type, meta in SIGNAL_TAXONOMY.items():
            try:
                source_kind = meta.get("scorer_source")
                if source_kind == "existing_scorer":
                    result = self._score_via_existing(signal_type, meta, company)
                elif source_kind == "query":
                    result = self._score_via_query(signal_type, meta, company)
                elif source_kind == "new_computer":
                    result = self._score_via_computer(signal_type, company)
                else:
                    result = self._neutral_signal(signal_type, "unknown scorer_source")
            except Exception as exc:
                logger.warning("signal %s failed for company %s: %s", signal_type, company.id, exc)
                self.db.rollback()
                result = self._neutral_signal(signal_type, f"scorer raised: {exc}")
            results.append(result)
        return results

    def _score_via_existing(
        self, signal_type: str, meta: Dict, company: TxnProbCompany
    ) -> SignalResult:
        """Call an existing scorer class and extract the score_field."""
        scorer_class_path = meta["scorer_class"]
        scorer = self._get_scorer(scorer_class_path)
        method_name = meta.get("scorer_method", "score_company")
        score_field = meta.get("score_field")

        # Different scorers accept different args (company_id vs company_name)
        # Detect by method signature when possible; fall back by convention.
        method = getattr(scorer, method_name)

        # Try the most common patterns
        try:
            if scorer_class_path.endswith("CompanyScorer"):
                # score_company(company_name: str)
                result = method(company.company_name)
            elif scorer_class_path.endswith("CompanyDiligenceScorer"):
                # score_company(company_name, state=..., naics=...)
                result = method(
                    company.company_name,
                    state=company.hq_state,
                    naics=company.naics_code,
                )
            elif company.canonical_company_id is not None:
                # scorers that accept company_id
                result = method(company.canonical_company_id)
            else:
                # No canonical id to pass — skip gracefully
                return self._neutral_signal(
                    signal_type, "no canonical_company_id for id-based scorer"
                )
        except Exception as exc:
            return self._neutral_signal(signal_type, f"existing scorer failed: {exc}")

        raw_score = self._extract_score(result, score_field)
        if raw_score is None:
            return self._neutral_signal(signal_type, "score field not found on result")

        confidence = self._extract_confidence(result)
        return SignalResult(
            signal_type=signal_type,
            score=max(0.0, min(100.0, float(raw_score))),
            confidence=confidence,
            details={"source": "existing_scorer", "class": scorer_class_path},
            data_sources=[scorer_class_path.rsplit(".", 1)[-1]],
        )

    def _score_via_query(
        self, signal_type: str, meta: Dict, company: TxnProbCompany
    ) -> SignalResult:
        """Compute a signal via direct DB query (e.g., pe_market_signals)."""
        if signal_type == "sector_momentum":
            return self._score_sector_momentum(company)
        return self._neutral_signal(signal_type, "no query handler defined")

    def _score_sector_momentum(self, company: TxnProbCompany) -> SignalResult:
        if not company.sector:
            return self._neutral_signal(
                "sector_momentum", "company has no sector"
            )
        from sqlalchemy import text as sa_text
        try:
            row = (
                self.db.execute(
                    sa_text(
                        """
                        SELECT momentum_score, signal_type, deal_count
                        FROM pe_market_signals
                        WHERE sector = :sector
                        ORDER BY scanned_at DESC
                        LIMIT 1
                        """
                    ),
                    {"sector": company.sector},
                )
                .mappings()
                .first()
            )
        except Exception as exc:
            self.db.rollback()
            logger.debug("sector_momentum query failed: %s", exc)
            return self._neutral_signal("sector_momentum", "query failed")

        if not row:
            return self._neutral_signal(
                "sector_momentum", f"no pe_market_signals for {company.sector}"
            )

        score = float(row.get("momentum_score") or 50)
        return SignalResult(
            signal_type="sector_momentum",
            score=max(0.0, min(100.0, score)),
            confidence=0.8,
            details={
                "sector": company.sector,
                "signal_type": row.get("signal_type"),
                "deal_count": row.get("deal_count"),
            },
            data_sources=["pe_market_signals"],
        )

    def _score_via_computer(
        self, signal_type: str, company: TxnProbCompany
    ) -> SignalResult:
        """Call one of the new signal computers."""
        computer_cls = NEW_COMPUTERS.get(signal_type)
        if not computer_cls:
            return self._neutral_signal(signal_type, "no computer registered")
        if signal_type not in self._computer_cache:
            self._computer_cache[signal_type] = computer_cls(self.db)
        return self._computer_cache[signal_type].compute(company)

    def _get_scorer(self, class_path: str):
        if class_path in self._scorer_cache:
            return self._scorer_cache[class_path]
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        instance = cls(self.db)
        self._scorer_cache[class_path] = instance
        return instance

    @staticmethod
    def _extract_score(result, field_name: str) -> Optional[float]:
        """Extract a score from either a dict or a dataclass."""
        if field_name is None:
            return None
        if isinstance(result, dict):
            return result.get(field_name)
        return getattr(result, field_name, None)

    @staticmethod
    def _extract_confidence(result) -> float:
        if isinstance(result, dict):
            return float(result.get("confidence", 0.7))
        return float(getattr(result, "confidence", 0.7))

    def _neutral_signal(self, signal_type: str, reason: str) -> SignalResult:
        return SignalResult(
            signal_type=signal_type,
            score=NEUTRAL_SCORE,
            confidence=0.0,
            details={"reason": reason},
            data_sources=[],
        )

    # -------------------------------------------------------------------
    # Composite + calibration
    # -------------------------------------------------------------------

    def _compute_composite(
        self, signals: List[SignalResult], weights: Dict[str, float]
    ) -> CompositeResult:
        """Weighted sum with convergence bonus. Pure function wrapper for consistency."""
        raw, convergence = self._compute_composite_static(signals, weights)
        active = sum(1 for s in signals if s.score >= SIGNAL_ABOVE_THRESHOLD)
        return CompositeResult(
            raw_composite_score=raw,
            convergence_factor=convergence,
            active_signal_count=active,
        )

    @staticmethod
    def _compute_composite_static(
        signals: List[SignalResult], weights: Dict[str, float]
    ) -> Tuple[float, float]:
        """Return (raw_composite, convergence_factor)."""
        weighted_sum = sum(s.score * weights.get(s.signal_type, 0.0) for s in signals)
        above_count = sum(1 for s in signals if s.score >= SIGNAL_ABOVE_THRESHOLD)
        convergence_factor = 1.0 + (above_count * CONVERGENCE_PER_SIGNAL_BOOST)
        raw = min(100.0, weighted_sum * convergence_factor)
        return raw, convergence_factor

    @staticmethod
    def _calibrate_sigmoid(raw: float, k: float = SIGMOID_K, x0: float = SIGMOID_X0) -> float:
        """Convert raw composite [0-100] to probability [0-1]."""
        try:
            return 1.0 / (1.0 + math.exp(-k * (raw - x0)))
        except OverflowError:
            return 0.0 if raw < x0 else 1.0

    def _calibrate_to_probability(self, raw: float, sector: str) -> float:
        """
        Phase 4: use a fitted Platt/isotonic calibration if available
        (preferring sector-specific, then global), falling back to the
        default sigmoid when no calibration has been fit yet.
        """
        try:
            # Sector-specific first
            if sector:
                cal = get_active_calibration(self.db, scope=sector)
                p = calibrate_with_active(raw, cal)
                if p is not None:
                    return float(p)
            # Then global
            cal = get_active_calibration(self.db, scope="global")
            p = calibrate_with_active(raw, cal)
            if p is not None:
                return float(p)
        except Exception as exc:
            logger.debug("calibration lookup failed: %s", exc)
            self.db.rollback()
        return self._calibrate_sigmoid(raw)

    @staticmethod
    def _grade_from_score(raw: float) -> str:
        if raw >= 85:
            return "A"
        if raw >= 70:
            return "B"
        if raw >= 55:
            return "C"
        if raw >= 40:
            return "D"
        return "F"

    @staticmethod
    def _compute_velocity_static(
        current: float,
        previous: Optional[float],
        prev_velocity: Optional[float],
    ) -> Tuple[float, float]:
        if previous is None:
            return 0.0, 0.0
        velocity = current - previous
        acceleration = velocity - (prev_velocity if prev_velocity is not None else 0.0)
        return velocity, acceleration

    # -------------------------------------------------------------------
    # Signal chain + persistence
    # -------------------------------------------------------------------

    def _build_signal_chain(
        self, signals: List[SignalResult], weights: Dict[str, float]
    ) -> List[Dict]:
        """Produce the explainable decomposition: score × weight = contribution."""
        chain = []
        for s in signals:
            w = weights.get(s.signal_type, 0.0)
            chain.append(
                {
                    "signal_type": s.signal_type,
                    "score": round(s.score, 2),
                    "weight": round(w, 4),
                    "contribution": round(s.score * w, 2),
                    "confidence": round(s.confidence, 2),
                    "data_sources": s.data_sources,
                }
            )
        return chain

    @staticmethod
    def _top_signals(signal_chain: List[Dict], n: int = 5) -> List[Dict]:
        """Top N signals by contribution."""
        return sorted(signal_chain, key=lambda x: x.get("contribution", 0), reverse=True)[:n]

    def _persist_signals(
        self, company_id: int, signals: List[SignalResult], batch_id: str
    ) -> None:
        """Persist signal snapshots with velocity/acceleration from previous snapshot."""
        for s in signals:
            prev = (
                self.db.query(TxnProbSignal)
                .filter_by(company_id=company_id, signal_type=s.signal_type)
                .order_by(TxnProbSignal.scored_at.desc())
                .first()
            )
            previous_score = prev.score if prev else None
            prev_velocity = prev.velocity if prev else None
            velocity, acceleration = self._compute_velocity_static(
                current=s.score,
                previous=previous_score,
                prev_velocity=prev_velocity,
            )
            snap = TxnProbSignal(
                company_id=company_id,
                signal_type=s.signal_type,
                score=s.score,
                previous_score=previous_score,
                velocity=velocity,
                acceleration=acceleration,
                signal_details=s.details,
                data_sources=s.data_sources,
                confidence=s.confidence,
                batch_id=batch_id,
                # Use explicit microsecond-precision timestamp to avoid
                # collisions on the unique (company_id, signal_type, scored_at)
                # constraint when scoring runs back-to-back (SQLite's
                # func.now() has second-level granularity).
                scored_at=datetime.utcnow(),
            )
            self.db.add(snap)
        # Flush so the score row's FK is consistent
        self.db.flush()

    @staticmethod
    def _new_batch_id() -> str:
        return f"txnp-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
