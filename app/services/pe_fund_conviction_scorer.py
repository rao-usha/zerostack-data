"""
PE Fund Conviction Scorer — measures LP conviction in a PE fund.

6 signals scored 0-100, weighted to produce a composite conviction score.
Mirrors the ExitReadinessScorer pattern.

Weights:
  LP Quality       25%  — tier-1 LP participation
  Re-up Rate       25%  — % of prior-vintage LPs who returned
  Oversubscription 20%  — final_close / target_size
  LP Diversity     15%  — LP count and concentration
  Time to Close    10%  — speed of fundraise
  GP Commitment     5%  — GP's own capital commitment
"""
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# LP tier scores (higher = better quality signal)
LP_TIER_SCORES = {
    "sovereign_wealth": 10,
    "endowment": 10,
    "public_pension": 7,
    "corporate_pension": 6,
    "foundation": 5,
    "insurance": 4,
    "family_office": 3,
    "other": 2,
}


@dataclass
class ConvictionSignal:
    name: str
    score: float  # 0-100
    weight: float
    raw_value: Optional[float]
    explanation: str


@dataclass
class FundConvictionResult:
    fund_id: int
    conviction_score: float
    conviction_grade: str
    signals: list
    lp_count: Optional[int]
    repeat_lp_count: Optional[int]
    tier1_lp_count: Optional[int]
    oversubscription_ratio: Optional[float]
    days_to_final_close: Optional[int]
    reup_rate_pct: Optional[float]
    data_completeness: float
    scoring_notes: str
    scored_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def sub_scores(self) -> dict:
        return {s.name: s.score for s in self.signals}


def _grade(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "F"


class FundConvictionScorer:
    """Computes LP conviction score for a PE fund."""

    WEIGHTS = {
        "lp_quality": 0.25,
        "reup_rate": 0.25,
        "oversubscription": 0.20,
        "lp_diversity": 0.15,
        "time_to_close": 0.10,
        "gp_commitment": 0.05,
    }

    def score_from_data(
        self,
        fund_id: int,
        # LP quality inputs
        lp_type_distribution: Optional[dict] = None,  # {lp_type: count}
        tier1_lp_count: Optional[int] = None,
        lp_count: Optional[int] = None,
        # Re-up inputs
        reup_rate_pct: Optional[float] = None,  # 0-1
        repeat_lp_count: Optional[int] = None,
        # Oversubscription
        target_size_usd: Optional[float] = None,
        final_close_usd: Optional[float] = None,
        # LP diversity
        top_lp_concentration_pct: Optional[float] = None,  # % from largest LP
        # Time to close
        first_close_date: Optional[datetime] = None,
        final_close_date: Optional[datetime] = None,
        days_to_close: Optional[int] = None,
        # GP commitment
        gp_commitment_pct: Optional[float] = None,  # 0-1
    ) -> FundConvictionResult:
        """Score a fund's LP conviction from available data."""

        signals = []
        available_signals = 0
        explanation = "No LP quality data"

        # --- Signal 1: LP Quality (25%) ---
        lp_quality_score = None
        if lp_type_distribution or tier1_lp_count is not None:
            available_signals += 1
            if tier1_lp_count is not None and lp_count and lp_count > 0:
                tier1_pct = tier1_lp_count / lp_count
                lp_quality_score = min(100, tier1_pct * 200)  # 50% tier-1 → score 100
                explanation = f"{tier1_lp_count}/{lp_count} LPs are tier-1 ({tier1_pct:.0%})"
            elif lp_type_distribution:
                total = sum(lp_type_distribution.values())
                weighted = sum(
                    LP_TIER_SCORES.get(t, 2) * c
                    for t, c in lp_type_distribution.items()
                )
                avg_tier = weighted / total if total > 0 else 0
                lp_quality_score = min(100, (avg_tier / 10) * 100)
                explanation = f"Avg LP tier score: {avg_tier:.1f}/10"
            else:
                lp_quality_score = 50
                explanation = "Limited LP type data"
        signals.append(ConvictionSignal(
            name="lp_quality",
            score=lp_quality_score or 0,
            weight=self.WEIGHTS["lp_quality"],
            raw_value=tier1_lp_count,
            explanation=explanation,
        ))

        # --- Signal 2: Re-up Rate (25%) ---
        reup_score = None
        if reup_rate_pct is not None:
            available_signals += 1
            # 80%+ re-up = exceptional, 50% = average, <30% = poor
            reup_score = min(100, max(0, (reup_rate_pct - 0.20) / 0.60 * 100))
            reup_explanation = f"{reup_rate_pct:.0%} of prior-vintage LPs re-upped"
        else:
            reup_score = 0
            reup_explanation = "No re-up data available"
        signals.append(ConvictionSignal(
            name="reup_rate",
            score=reup_score,
            weight=self.WEIGHTS["reup_rate"],
            raw_value=reup_rate_pct,
            explanation=reup_explanation,
        ))

        # --- Signal 3: Oversubscription (20%) ---
        oversubscription_ratio = None
        oversubscription_score = None
        if target_size_usd and final_close_usd and target_size_usd > 0:
            available_signals += 1
            oversubscription_ratio = final_close_usd / target_size_usd
            # 1.5x+ = exceptional, 1.0x = met target, <0.8x = struggled
            oversubscription_score = min(100, max(0, (oversubscription_ratio - 0.7) / 0.8 * 100))
            over_explanation = (
                f"Raised {oversubscription_ratio:.2f}x target "
                f"({final_close_usd / 1e6:.0f}M vs {target_size_usd / 1e6:.0f}M target)"
            )
        else:
            oversubscription_score = 0
            over_explanation = "No fund size data"
        signals.append(ConvictionSignal(
            name="oversubscription",
            score=oversubscription_score or 0,
            weight=self.WEIGHTS["oversubscription"],
            raw_value=oversubscription_ratio,
            explanation=over_explanation,
        ))

        # --- Signal 4: LP Diversity (15%) ---
        diversity_score = None
        if lp_count is not None:
            available_signals += 1
            # More LPs = more diverse = higher conviction (log scale)
            count_score = min(100, math.log10(max(1, lp_count)) / math.log10(200) * 100)
            # Penalize if highly concentrated
            conc_penalty = 0
            if top_lp_concentration_pct:
                conc_penalty = max(0, (top_lp_concentration_pct - 0.15) * 100)
            diversity_score = max(0, count_score - conc_penalty)
            div_explanation = f"{lp_count} LPs" + (
                f"; top LP = {top_lp_concentration_pct:.0%} of fund"
                if top_lp_concentration_pct else ""
            )
        else:
            diversity_score = 0
            div_explanation = "No LP count data"
        signals.append(ConvictionSignal(
            name="lp_diversity",
            score=diversity_score or 0,
            weight=self.WEIGHTS["lp_diversity"],
            raw_value=lp_count,
            explanation=div_explanation,
        ))

        # --- Signal 5: Time to Close (10%) ---
        days_to_final_close = days_to_close
        time_score = None
        if first_close_date and final_close_date:
            available_signals += 1
            days_to_final_close = (final_close_date - first_close_date).days
            # <6 months = exceptional, 12 months = average, >24 months = struggled
            time_score = min(100, max(0, (730 - days_to_final_close) / 550 * 100))
            time_explanation = (
                f"Closed in {days_to_final_close} days ({days_to_final_close // 30} months)"
            )
        elif days_to_close is not None:
            available_signals += 1
            time_score = min(100, max(0, (730 - days_to_close) / 550 * 100))
            time_explanation = f"Closed in {days_to_close} days"
        else:
            time_score = 0
            time_explanation = "No close date data"
        signals.append(ConvictionSignal(
            name="time_to_close",
            score=time_score or 0,
            weight=self.WEIGHTS["time_to_close"],
            raw_value=days_to_final_close,
            explanation=time_explanation,
        ))

        # --- Signal 6: GP Commitment (5%) ---
        gp_score = None
        if gp_commitment_pct is not None:
            available_signals += 1
            # 3%+ = exceptional alignment, 1% = standard, <0.5% = low
            gp_score = min(100, max(0, (gp_commitment_pct - 0.005) / 0.025 * 100))
            gp_explanation = f"GP committed {gp_commitment_pct:.1%} of fund"
        else:
            gp_score = 0
            gp_explanation = "GP commitment % unknown"
        signals.append(ConvictionSignal(
            name="gp_commitment",
            score=gp_score or 0,
            weight=self.WEIGHTS["gp_commitment"],
            raw_value=gp_commitment_pct,
            explanation=gp_explanation,
        ))

        # Compute weighted composite (sum of weight*score / sum of all weights)
        composite = sum(s.score * s.weight for s in signals) / sum(self.WEIGHTS.values())

        data_completeness = available_signals / len(self.WEIGHTS)

        notes_parts = [s.explanation for s in signals if s.raw_value is not None]
        notes = (
            " | ".join(notes_parts)
            if notes_parts
            else "Insufficient data for full scoring"
        )

        return FundConvictionResult(
            fund_id=fund_id,
            conviction_score=round(composite, 1),
            conviction_grade=_grade(composite),
            signals=signals,
            lp_count=lp_count,
            repeat_lp_count=repeat_lp_count,
            tier1_lp_count=tier1_lp_count,
            oversubscription_ratio=oversubscription_ratio,
            days_to_final_close=days_to_final_close,
            reup_rate_pct=reup_rate_pct,
            data_completeness=data_completeness,
            scoring_notes=notes,
        )
