"""
Deal Probability Engine — Named Convergence Pattern Detector (SPEC 047, PLAN_059 Phase 3).

The product differentiator: multi-signal convergence patterns that are
structurally impossible for a generalist LLM or competitor (PitchBook/Grata)
to surface. Named patterns encode deal-sourcer intuition:

- classic_exit_setup:    management buildout + strong financials + hot sector
- founder_transition:    aging founder + succession scramble + deal signals
- distress_opportunity:  weak fundamentals + insider selling + restructuring hires
- sector_wave:           hot sector + macro tailwind + visible deal activity
- platform_buyer:        strong exit readiness + aggressive hiring + innovation leverage
- covert_process:        low diligence visibility + accelerating exec transition

A pattern matches when ALL required signals pass their thresholds. Patterns
with `inverted` thresholds match when the signal is BELOW the threshold
(e.g., weak diligence, net selling).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbScore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern registry
# ---------------------------------------------------------------------------


@dataclass
class ConvergencePattern:
    """A named multi-signal convergence rule."""

    key: str
    label: str
    description: str
    severity: str  # high|medium|low
    # required_signals[signal_type] = {"min": float} (default)
    #                              or {"max": float} (inverted — signal low = match)
    required_signals: Dict[str, Dict[str, float]] = field(default_factory=dict)


CONVERGENCE_PATTERNS: Dict[str, ConvergencePattern] = {
    "classic_exit_setup": ConvergencePattern(
        key="classic_exit_setup",
        label="Classic Exit Setup",
        description="Management build-out + strong financials + hot sector — the textbook pre-exit signature.",
        severity="high",
        required_signals={
            "exec_transition": {"min": 60},
            "financial_health": {"min": 70},
            "sector_momentum": {"min": 65},
        },
    ),
    "founder_transition": ConvergencePattern(
        key="founder_transition",
        label="Founder Transition",
        description="Aging founder, succession scramble, and visible deal exploration — liquidity event imminent.",
        severity="high",
        required_signals={
            "founder_risk": {"min": 70},
            "exec_transition": {"min": 50},
            "deal_activity_signals": {"min": 40},
        },
    ),
    "distress_opportunity": ConvergencePattern(
        key="distress_opportunity",
        label="Distress Opportunity",
        description="Weak fundamentals, insider selling, and restructuring hires — potential turnaround acquisition.",
        severity="medium",
        required_signals={
            "diligence_health": {"max": 40},
            "insider_activity": {"max": 40},  # low score = net selling
            "hiring_velocity": {"min": 50},
        },
    ),
    "sector_wave": ConvergencePattern(
        key="sector_wave",
        label="Sector Wave",
        description="Hot sector + macro tailwind + visible deal activity — ride the wave.",
        severity="medium",
        required_signals={
            "sector_momentum": {"min": 75},
            "macro_tailwind": {"min": 60},
            "deal_activity_signals": {"min": 40},
        },
    ),
    "platform_buyer": ConvergencePattern(
        key="platform_buyer",
        label="Platform Buyer Ready",
        description="Strong exit readiness + aggressive hiring + innovation leverage — platform candidate.",
        severity="medium",
        required_signals={
            "exit_readiness": {"min": 65},
            "hiring_velocity": {"min": 60},
            "innovation_velocity": {"min": 55},
        },
    ),
    "covert_process": ConvergencePattern(
        key="covert_process",
        label="Covert Process Signal",
        description="Low diligence visibility + accelerating exec transition — quiet process may be underway.",
        severity="high",
        required_signals={
            "diligence_health": {"max": 45},
            "exec_transition": {"min": 60},
            "hiring_velocity": {"min": 55},
        },
    ),
}


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def match_pattern(
    pattern: ConvergencePattern, signal_scores: Dict[str, float]
) -> bool:
    """
    Pure function — does this signal set satisfy this pattern?

    Returns True only if EVERY required signal is present AND its threshold
    is satisfied (min for normal, max for inverted).
    """
    for signal_type, rule in pattern.required_signals.items():
        if signal_type not in signal_scores:
            return False
        score = signal_scores[signal_type]
        if "min" in rule and score < rule["min"]:
            return False
        if "max" in rule and score > rule["max"]:
            return False
    return True


def detect_patterns(signal_scores: Dict[str, float]) -> List[ConvergencePattern]:
    """Return all patterns matched by the given signal_type → score map."""
    return [p for p in CONVERGENCE_PATTERNS.values() if match_pattern(p, signal_scores)]


class ConvergenceDetector:
    """
    Convenience class for scanning companies against the pattern registry.

    Can be used standalone or wired into TransactionProbabilityEngine.
    """

    def __init__(self, db: Session):
        self.db = db

    def detect_company(self, company_id: int) -> List[Dict]:
        """Return matched patterns for a company's latest score's signal chain."""
        score = (
            self.db.query(TxnProbScore)
            .filter_by(company_id=company_id)
            .order_by(TxnProbScore.scored_at.desc())
            .first()
        )
        if not score or not score.signal_chain:
            return []
        signal_scores = {
            entry["signal_type"]: entry["score"] for entry in score.signal_chain
        }
        matched = detect_patterns(signal_scores)
        return [self._pattern_to_dict(p, signal_scores) for p in matched]

    def detect_from_signals(self, signals: List) -> List[Dict]:
        """Detect patterns directly from SignalResult objects (pre-persist)."""
        signal_scores = {s.signal_type: s.score for s in signals}
        matched = detect_patterns(signal_scores)
        return [self._pattern_to_dict(p, signal_scores) for p in matched]

    def scan_all_companies(self) -> List[Dict]:
        """
        Scan every latest-scored company and return a list of
        {company_id, patterns: [...]} for those with at least one match.
        """
        from sqlalchemy import func
        from app.core.probability_models import TxnProbCompany

        latest_subq = (
            self.db.query(
                TxnProbScore.company_id,
                func.max(TxnProbScore.scored_at).label("latest_at"),
            )
            .group_by(TxnProbScore.company_id)
            .subquery()
        )
        rows = (
            self.db.query(TxnProbScore, TxnProbCompany)
            .join(
                latest_subq,
                (TxnProbScore.company_id == latest_subq.c.company_id)
                & (TxnProbScore.scored_at == latest_subq.c.latest_at),
            )
            .join(TxnProbCompany, TxnProbCompany.id == TxnProbScore.company_id)
            .all()
        )

        result = []
        for score, company in rows:
            if not score.signal_chain:
                continue
            signal_scores = {
                entry["signal_type"]: entry["score"] for entry in score.signal_chain
            }
            matched = detect_patterns(signal_scores)
            if matched:
                result.append(
                    {
                        "company_id": company.id,
                        "company_name": company.company_name,
                        "sector": company.sector,
                        "probability": score.probability,
                        "grade": score.grade,
                        "patterns": [
                            self._pattern_to_dict(p, signal_scores) for p in matched
                        ],
                    }
                )
        return result

    @staticmethod
    def _pattern_to_dict(
        pattern: ConvergencePattern, signal_scores: Dict[str, float]
    ) -> Dict:
        """Serialize a matched pattern with the contributing signal scores."""
        return {
            "key": pattern.key,
            "label": pattern.label,
            "description": pattern.description,
            "severity": pattern.severity,
            "matched_signals": {
                sig: signal_scores.get(sig)
                for sig in pattern.required_signals.keys()
            },
        }
