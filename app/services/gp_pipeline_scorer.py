"""
GP Pipeline Scorer — Chain 3 of PLAN_052.

Flips the LP Conviction lens: instead of "how good is this fund for LPs?",
asks "how LP-favored is this GP firm?" by analyzing the institutional LP
base across all commitment data.

5 weighted signals:
  LP Breadth (25%) — unique LP count
  Tier-1 LP Concentration (25%) — smart money validation
  Re-up Rate (20%) — LP satisfaction / loyalty
  Commitment Momentum (15%) — growing vs declining interest
  Capital Density (15%) — institutional-scale validation
"""
from __future__ import annotations
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PipelineSignal:
    signal: str
    score: int            # 0-100
    weight: float
    reading: str
    data_source: str
    details: Dict = field(default_factory=dict)


@dataclass
class LPSummary:
    lp_id: int
    lp_name: str
    lp_type: str
    vintages_committed: int
    total_committed_usd: float
    commitment_trend: str
    is_tier1: bool


@dataclass
class GPPipelineScore:
    firm_id: int
    firm_name: str
    score: int            # 0-100
    grade: str            # A, B, C, D
    signal: str           # green, yellow, red
    recommendation: str
    signals: List[PipelineSignal] = field(default_factory=list)
    lp_base: List[LPSummary] = field(default_factory=list)
    lp_count: int = 0
    tier1_count: int = 0
    total_committed_usd: float = 0.0


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SIGNAL_WEIGHTS = {
    "lp_breadth": 0.25,
    "tier1_concentration": 0.25,
    "reup_rate": 0.20,
    "commitment_momentum": 0.15,
    "capital_density": 0.15,
}

GRADE_THRESHOLDS = [(80, "A"), (65, "B"), (50, "C"), (0, "D")]
SIGNAL_MAP = [(70, "green"), (50, "yellow"), (0, "red")]

RECOMMENDATIONS = {
    "A": "Tier-1 GP — broad institutional LP base with strong re-up rates. High fundraising confidence.",
    "B": "Solid GP — good LP traction with room to broaden base or deepen relationships.",
    "C": "Emerging GP — limited institutional validation. May be early-stage or niche strategy.",
    "D": "Thin LP base — few institutional commitments on record. High fundraising risk.",
}

# Tier-1 LP criteria
TIER1_LP_TYPES = {"sovereign_wealth", "endowment"}
TIER1_PENSION_AUM_THRESHOLD = 100  # $100B+ public pensions count as tier-1


def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("GP pipeline query failed: %s", exc)
        return []


def _is_tier1(lp_type: str, aum_billions: Optional[float]) -> bool:
    if lp_type in TIER1_LP_TYPES:
        return True
    if lp_type == "public_pension" and aum_billions and aum_billions >= TIER1_PENSION_AUM_THRESHOLD:
        return True
    return False


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

class GPPipelineScorer:

    def __init__(self, db: Session):
        self.db = db

    def score_all_gps(self) -> List[GPPipelineScore]:
        """Score all GPs that have LP relationship data. Returns sorted by score desc."""
        # Get all distinct gp_firm_ids with relationships
        rows = _safe_query(self.db, """
            SELECT DISTINCT gp_firm_id, gp_name
            FROM lp_gp_relationships
            WHERE gp_firm_id IS NOT NULL
            ORDER BY gp_name
        """, {})

        results = []
        for r in rows:
            score = self.score_gp(firm_id=r[0], firm_name=r[1])
            results.append(score)

        return sorted(results, key=lambda s: s.score, reverse=True)

    def score_gp(self, firm_id: int, firm_name: Optional[str] = None) -> GPPipelineScore:
        """Score a single GP firm by its LP base."""

        if not firm_name:
            name_row = _safe_query(self.db, "SELECT name FROM pe_firms WHERE id = :fid", {"fid": firm_id})
            firm_name = name_row[0][0] if name_row else f"Firm #{firm_id}"

        # Load LP base for this GP
        lp_rows = _safe_query(self.db, """
            SELECT r.lp_id, l.name, l.lp_type, l.aum_usd_billions,
                   r.total_vintages_committed, r.total_committed_usd,
                   r.commitment_trend
            FROM lp_gp_relationships r
            JOIN lp_fund l ON l.id = r.lp_id
            WHERE r.gp_firm_id = :fid
            ORDER BY r.total_committed_usd DESC NULLS LAST
        """, {"fid": firm_id})

        if not lp_rows:
            return GPPipelineScore(
                firm_id=firm_id, firm_name=firm_name,
                score=0, grade="D", signal="red",
                recommendation=RECOMMENDATIONS["D"],
            )

        # Build LP summaries
        lp_base = []
        tier1_count = 0
        total_usd = 0.0
        total_vintages = 0
        trend_counts = {"growing": 0, "stable": 0, "declining": 0, "new": 0}

        for r in lp_rows:
            lp_type = r[2] or "other"
            aum_b = float(r[3]) if r[3] else None
            is_t1 = _is_tier1(lp_type, aum_b)
            vintages = int(r[4] or 1)
            committed = float(r[5] or 0)
            trend = r[6] or "new"

            if is_t1:
                tier1_count += 1
            total_usd += committed
            total_vintages += vintages
            trend_counts[trend] = trend_counts.get(trend, 0) + 1

            lp_base.append(LPSummary(
                lp_id=r[0], lp_name=r[1], lp_type=lp_type,
                vintages_committed=vintages, total_committed_usd=committed,
                commitment_trend=trend, is_tier1=is_t1,
            ))

        lp_count = len(lp_base)
        signals = []

        # --- Signal 1: LP Breadth (25%) ---
        if lp_count >= 10:
            breadth_score = 100
            reading = f"{lp_count} institutional LPs — broad base"
        elif lp_count >= 7:
            breadth_score = 85
            reading = f"{lp_count} institutional LPs — strong base"
        elif lp_count >= 4:
            breadth_score = 65
            reading = f"{lp_count} institutional LPs — moderate base"
        elif lp_count >= 2:
            breadth_score = 40
            reading = f"{lp_count} institutional LPs — narrow base"
        else:
            breadth_score = 20
            reading = f"{lp_count} institutional LP — very limited"

        signals.append(PipelineSignal(
            signal="LP breadth", score=breadth_score,
            weight=SIGNAL_WEIGHTS["lp_breadth"],
            reading=reading, data_source="lp_gp_relationships",
            details={"lp_count": lp_count},
        ))

        # --- Signal 2: Tier-1 LP Concentration (25%) ---
        tier1_pct = (tier1_count / lp_count * 100) if lp_count > 0 else 0
        if tier1_pct > 60:
            t1_score = 100
            reading = f"{tier1_count}/{lp_count} tier-1 LPs ({tier1_pct:.0f}%) — smart money validated"
        elif tier1_pct > 40:
            t1_score = 80
            reading = f"{tier1_count}/{lp_count} tier-1 LPs ({tier1_pct:.0f}%) — strong institutional backing"
        elif tier1_pct > 20:
            t1_score = 60
            reading = f"{tier1_count}/{lp_count} tier-1 LPs ({tier1_pct:.0f}%) — moderate validation"
        else:
            t1_score = 30
            reading = f"{tier1_count}/{lp_count} tier-1 LPs ({tier1_pct:.0f}%) — limited smart money"

        signals.append(PipelineSignal(
            signal="Tier-1 LP concentration", score=t1_score,
            weight=SIGNAL_WEIGHTS["tier1_concentration"],
            reading=reading, data_source="lp_fund + lp_gp_relationships",
            details={"tier1_count": tier1_count, "tier1_pct": round(tier1_pct, 1)},
        ))

        # --- Signal 3: Re-up Rate (20%) ---
        avg_vintages = total_vintages / lp_count if lp_count > 0 else 1.0
        if avg_vintages > 2.0:
            reup_score = 100
            reading = f"Avg {avg_vintages:.1f} vintages per LP — exceptional loyalty"
        elif avg_vintages > 1.5:
            reup_score = 80
            reading = f"Avg {avg_vintages:.1f} vintages per LP — strong re-up"
        elif avg_vintages > 1.2:
            reup_score = 60
            reading = f"Avg {avg_vintages:.1f} vintages per LP — moderate re-up"
        else:
            reup_score = 40
            reading = f"Avg {avg_vintages:.1f} vintages per LP — mostly first-time commitments"

        signals.append(PipelineSignal(
            signal="Re-up rate", score=reup_score,
            weight=SIGNAL_WEIGHTS["reup_rate"],
            reading=reading, data_source="lp_gp_relationships",
            details={"avg_vintages": round(avg_vintages, 2), "total_vintages": total_vintages},
        ))

        # --- Signal 4: Commitment Momentum (15%) ---
        growing = trend_counts.get("growing", 0)
        stable = trend_counts.get("stable", 0)
        declining = trend_counts.get("declining", 0)

        growing_pct = (growing / lp_count * 100) if lp_count > 0 else 0
        declining_pct = (declining / lp_count * 100) if lp_count > 0 else 0

        if growing_pct > 50:
            momentum_score = 100
            reading = f"{growing}/{lp_count} LPs growing commitments — strong momentum"
        elif growing_pct + (stable / lp_count * 100 if lp_count > 0 else 0) > 70:
            momentum_score = 70
            reading = f"{growing} growing, {stable} stable — steady momentum"
        elif declining_pct > 50:
            momentum_score = 20
            reading = f"{declining}/{lp_count} LPs declining — losing LP confidence"
        else:
            momentum_score = 50
            reading = f"Mixed: {growing} growing, {stable} stable, {declining} declining"

        signals.append(PipelineSignal(
            signal="Commitment momentum", score=momentum_score,
            weight=SIGNAL_WEIGHTS["commitment_momentum"],
            reading=reading, data_source="lp_gp_relationships",
            details={"growing": growing, "stable": stable, "declining": declining},
        ))

        # --- Signal 5: Capital Density (15%) ---
        avg_commitment = (total_usd / lp_count) if lp_count > 0 else 0
        if avg_commitment > 200_000_000:
            density_score = 100
            reading = f"${avg_commitment / 1e6:.0f}M avg per LP — mega-institutional scale"
        elif avg_commitment > 100_000_000:
            density_score = 80
            reading = f"${avg_commitment / 1e6:.0f}M avg per LP — large institutional scale"
        elif avg_commitment > 50_000_000:
            density_score = 60
            reading = f"${avg_commitment / 1e6:.0f}M avg per LP — mid-market scale"
        else:
            density_score = 40
            reading = f"${avg_commitment / 1e6:.0f}M avg per LP — smaller commitments"

        signals.append(PipelineSignal(
            signal="Capital density", score=density_score,
            weight=SIGNAL_WEIGHTS["capital_density"],
            reading=reading, data_source="lp_gp_relationships",
            details={"avg_commitment_usd": round(avg_commitment), "total_committed_usd": round(total_usd)},
        ))

        # --- Composite ---
        total_weight = sum(s.weight for s in signals)
        composite = sum(s.score * s.weight for s in signals) / total_weight if total_weight > 0 else 0
        score = max(0, min(100, int(round(composite))))

        grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "D")
        sig = next((s for threshold, s in SIGNAL_MAP if score >= threshold), "red")

        return GPPipelineScore(
            firm_id=firm_id, firm_name=firm_name,
            score=score, grade=grade, signal=sig,
            recommendation=RECOMMENDATIONS[grade],
            signals=signals, lp_base=lp_base,
            lp_count=lp_count, tier1_count=tier1_count,
            total_committed_usd=total_usd,
        )
