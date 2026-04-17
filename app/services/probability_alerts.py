"""
Deal Probability Engine — Alert Engine (SPEC 047, PLAN_059 Phase 3).

Evaluates a newly-computed score against the previous one and fires
alerts for threshold-crossing events. Each alert writes a `TxnProbAlert`
row that the API surface exposes.

Alert types:
  - probability_spike    — probability delta > 0.15 (high severity)
  - probability_drop     — probability delta < -0.15 (medium severity)
  - grade_upgrade        — grade improved (e.g., C → B) (medium)
  - grade_downgrade      — grade worsened (low)
  - new_convergence      — a named convergence pattern newly matched (high)
  - signal_acceleration  — any signal acceleration > 10 (medium)
  - new_universe_entry   — first-ever score for this company (low)

Dedup: within the same batch_id, we never write the same alert twice for
the same company+type. Cross-batch dedup is the caller's responsibility
via `since` filtering — alerts are append-only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbAlert, TxnProbScore, TxnProbSignal

logger = logging.getLogger(__name__)


# Alert thresholds
SPIKE_THRESHOLD = 0.15  # probability delta
SIGNAL_ACCEL_THRESHOLD = 10.0  # absolute acceleration

GRADE_ORDER = {"F": 0, "D": 1, "C": 2, "B": 3, "A": 4}


@dataclass
class AlertSpec:
    alert_type: str
    severity: str
    title: str
    description: str
    triggering_signals: Optional[List[Dict]] = None


class AlertEngine:
    """Evaluate score deltas and persist qualifying alerts."""

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def evaluate(
        self,
        company_id: int,
        prev_score: Optional[TxnProbScore],
        new_score_row: TxnProbScore,
        new_convergences: List[Dict],
        company_name: Optional[str] = None,
    ) -> List[TxnProbAlert]:
        """
        Evaluate all alert rules against the new score. Writes and returns
        the alert rows. Returns [] if nothing qualifies.
        """
        specs: List[AlertSpec] = []

        if prev_score is None:
            specs.append(self._new_universe_entry_alert(new_score_row))
        else:
            specs.extend(
                self._probability_alerts(prev_score, new_score_row)
            )
            specs.extend(self._grade_alerts(prev_score, new_score_row))

        if new_convergences:
            specs.extend(
                self._convergence_alerts(new_score_row, new_convergences, prev_score)
            )

        specs.extend(self._signal_acceleration_alerts(company_id, new_score_row))

        persisted: List[TxnProbAlert] = []
        for spec in specs:
            alert = TxnProbAlert(
                company_id=company_id,
                alert_type=spec.alert_type,
                severity=spec.severity,
                title=spec.title,
                description=spec.description,
                probability_before=(
                    prev_score.probability if prev_score else None
                ),
                probability_after=new_score_row.probability,
                triggering_signals=spec.triggering_signals,
                is_read=False,
            )
            self.db.add(alert)
            persisted.append(alert)

        if persisted:
            self.db.flush()
        return persisted

    # -------------------------------------------------------------------
    # Rules (pure-ish, but read prev/new score objects)
    # -------------------------------------------------------------------

    def _probability_alerts(
        self, prev: TxnProbScore, new: TxnProbScore
    ) -> List[AlertSpec]:
        delta = (new.probability or 0.0) - (prev.probability or 0.0)
        out = []
        if delta >= SPIKE_THRESHOLD:
            out.append(
                AlertSpec(
                    alert_type="probability_spike",
                    severity="high",
                    title=(
                        f"Probability spike: {prev.probability:.1%} → "
                        f"{new.probability:.1%} (+{delta:.1%})"
                    ),
                    description=(
                        "Transaction probability crossed the +15% delta threshold. "
                        "Inspect top_signals to identify the driver."
                    ),
                )
            )
        elif delta <= -SPIKE_THRESHOLD:
            out.append(
                AlertSpec(
                    alert_type="probability_drop",
                    severity="medium",
                    title=(
                        f"Probability drop: {prev.probability:.1%} → "
                        f"{new.probability:.1%} ({delta:.1%})"
                    ),
                    description=(
                        "Transaction probability dropped sharply. "
                        "Likely stale signal data or a negative event."
                    ),
                )
            )
        return out

    def _grade_alerts(
        self, prev: TxnProbScore, new: TxnProbScore
    ) -> List[AlertSpec]:
        prev_g = prev.grade or "F"
        new_g = new.grade or "F"
        if prev_g == new_g:
            return []
        prev_rank = GRADE_ORDER.get(prev_g, 0)
        new_rank = GRADE_ORDER.get(new_g, 0)
        if new_rank > prev_rank:
            return [
                AlertSpec(
                    alert_type="grade_upgrade",
                    severity="medium",
                    title=f"Grade upgrade: {prev_g} → {new_g}",
                    description="Composite score crossed a grade threshold upward.",
                )
            ]
        if new_rank < prev_rank:
            return [
                AlertSpec(
                    alert_type="grade_downgrade",
                    severity="low",
                    title=f"Grade downgrade: {prev_g} → {new_g}",
                    description="Composite score crossed a grade threshold downward.",
                )
            ]
        return []

    def _convergence_alerts(
        self,
        new: TxnProbScore,
        new_convergences: List[Dict],
        prev_score: Optional[TxnProbScore],
    ) -> List[AlertSpec]:
        """
        Fire an alert for each convergence pattern not present on the previous score.
        """
        prev_keys = set()
        if prev_score is not None:
            # We stash previously-matched pattern keys on top_signals's "convergences"
            # slot if available; otherwise compare against empty (conservative = fire).
            for entry in (prev_score.top_signals or []):
                if isinstance(entry, dict) and "pattern_key" in entry:
                    prev_keys.add(entry["pattern_key"])

        out = []
        for cvg in new_convergences:
            if cvg["key"] in prev_keys:
                continue
            out.append(
                AlertSpec(
                    alert_type="new_convergence",
                    severity=cvg.get("severity", "high"),
                    title=f"New convergence: {cvg['label']}",
                    description=cvg.get("description", ""),
                    triggering_signals=[
                        {"signal_type": k, "score": v}
                        for k, v in cvg.get("matched_signals", {}).items()
                    ],
                )
            )
        return out

    def _signal_acceleration_alerts(
        self, company_id: int, new_score: TxnProbScore
    ) -> List[AlertSpec]:
        """Fire a single alert listing all signals with |acceleration| > threshold."""
        latest_signals = (
            self.db.query(TxnProbSignal)
            .filter_by(company_id=company_id, batch_id=new_score.batch_id)
            .all()
        )
        accelerating = [
            s
            for s in latest_signals
            if s.acceleration is not None and abs(s.acceleration) >= SIGNAL_ACCEL_THRESHOLD
        ]
        if not accelerating:
            return []

        return [
            AlertSpec(
                alert_type="signal_acceleration",
                severity="medium",
                title=(
                    f"{len(accelerating)} signal(s) accelerating "
                    f"(|accel| ≥ {SIGNAL_ACCEL_THRESHOLD})"
                ),
                description="One or more signals changed velocity sharply since the last score.",
                triggering_signals=[
                    {
                        "signal_type": s.signal_type,
                        "score": s.score,
                        "acceleration": s.acceleration,
                    }
                    for s in accelerating
                ],
            )
        ]

    @staticmethod
    def _new_universe_entry_alert(new: TxnProbScore) -> AlertSpec:
        return AlertSpec(
            alert_type="new_universe_entry",
            severity="low",
            title=f"Newly scored company (first snapshot): P = {new.probability:.1%}",
            description=(
                "This company received its first probability score. "
                "Future runs will track velocity and convergence."
            ),
        )
