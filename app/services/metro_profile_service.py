"""
MetroProfileService — computes derived development scores for metro areas.

Scoring philosophy:
  - Percentile-rank normalization across all metros (self-calibrating, no hardcoded thresholds)
  - Higher build_hostility_score (0-100) = harder to build in
  - Grades: A (0-25) = very buildable, B (26-50), C (51-75), D (76-100) = very hostile

Component scores:
  permit_velocity_score   — permits per 1000 existing housing units (inverted → hostility)
  multifamily_score       — share of permits that are 5+ unit buildings (inverted → hostility)
  supply_elasticity_score — permit velocity / price appreciation (inverted → hostility)
                            High price + low permits = supply-constrained = hostile

Composite:
  build_hostility = 100 - (
      0.40 * supply_elasticity_percentile +
      0.30 * permit_velocity_percentile +
      0.20 * multifamily_percentile +
      0.10 * low_cost_burden_percentile
  )
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MetroProfileService:
    """Derives development scores for a batch of metro profile dicts."""

    GRADE_THRESHOLDS = [
        (25.0, "A"),   # 0–25: very buildable
        (50.0, "B"),   # 26–50: moderately buildable
        (75.0, "C"),   # 51–75: somewhat hostile
        (100.1, "D"),  # 76–100: very hostile
    ]

    def compute_scores(self, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compute permit_velocity_score, multifamily_score, supply_elasticity_score,
        and build_hostility_score for all metros.

        Modifies profiles in-place and returns the list.
        """
        if not profiles:
            return profiles

        # Extract raw metric arrays (skip None for percentile computation)
        velocity_vals = self._extract(profiles, "permits_per_1000_units")
        mf_vals = self._extract(profiles, "multifamily_share_pct")
        hpi_vals = self._extract(profiles, "hpi_5yr_pct")
        burden_vals = self._extract(profiles, "cost_burden_severe_pct")

        # Supply elasticity: permits_per_1000 / hpi_5yr_pct
        # (only for metros with both values and positive HPI)
        elasticity_raw: Dict[str, Optional[float]] = {}
        for p in profiles:
            code = p["cbsa_code"]
            vel = p.get("permits_per_1000_units")
            hpi = p.get("hpi_5yr_pct")
            if vel is not None and hpi is not None and hpi > 0:
                elasticity_raw[code] = vel / hpi
            else:
                elasticity_raw[code] = None

        elasticity_vals = {k: v for k, v in elasticity_raw.items() if v is not None}

        for p in profiles:
            code = p["cbsa_code"]

            vel = p.get("permits_per_1000_units")
            mf = p.get("multifamily_share_pct")
            burden = p.get("cost_burden_severe_pct")
            elast = elasticity_raw.get(code)

            # Component percentile ranks (0–100; higher = better supply response)
            vel_pct = self._percentile_rank(vel, velocity_vals) if vel is not None else None
            mf_pct = self._percentile_rank(mf, mf_vals) if mf is not None else None
            elast_pct = self._percentile_rank(elast, list(elasticity_vals.values())) if elast is not None else None
            # Low cost burden = good signal → invert (high burden = low score)
            burden_pct = (100.0 - self._percentile_rank(burden, burden_vals)) if burden is not None else None

            # Store component scores (these are "buildability" percentiles, higher = more buildable)
            p["permit_velocity_score"] = round(vel_pct, 1) if vel_pct is not None else None
            p["multifamily_score"] = round(mf_pct, 1) if mf_pct is not None else None
            p["supply_elasticity_score"] = round(elast_pct, 1) if elast_pct is not None else None

            # Composite build_hostility: weighted average of INVERTED component buildability scores
            # Available components determine weighting
            components = []
            weights = []

            if elast_pct is not None:
                components.append(elast_pct)
                weights.append(0.40)
            if vel_pct is not None:
                components.append(vel_pct)
                weights.append(0.30)
            if mf_pct is not None:
                components.append(mf_pct)
                weights.append(0.20)
            if burden_pct is not None:
                components.append(burden_pct)
                weights.append(0.10)

            if components:
                # Normalize weights to sum to 1.0
                total_weight = sum(weights)
                buildability = sum(c * w for c, w in zip(components, weights)) / total_weight
                hostility = round(100.0 - buildability, 1)
                # Clamp to [0, 100]
                hostility = max(0.0, min(100.0, hostility))
                p["build_hostility_score"] = hostility
                p["build_hostility_grade"] = self._grade(hostility)
            else:
                p["build_hostility_score"] = None
                p["build_hostility_grade"] = None

        scored = sum(1 for p in profiles if p.get("build_hostility_score") is not None)
        logger.info(f"Computed build hostility scores for {scored}/{len(profiles)} metros")
        return profiles

    def get_rankings(
        self,
        profiles: List[Dict[str, Any]],
        descending: bool = True,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Return profiles sorted by build_hostility_score."""
        scored = [p for p in profiles if p.get("build_hostility_score") is not None]
        unscored = [p for p in profiles if p.get("build_hostility_score") is None]
        scored.sort(key=lambda p: p["build_hostility_score"], reverse=descending)
        return (scored + unscored)[:limit]

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _extract(profiles: List[Dict[str, Any]], field: str) -> List[float]:
        """Return non-None numeric values for a field across all profiles."""
        vals = []
        for p in profiles:
            v = p.get(field)
            if v is not None:
                try:
                    vals.append(float(v))
                except (ValueError, TypeError):
                    pass
        return vals

    @staticmethod
    def _percentile_rank(value: float, all_values: List[float]) -> float:
        """
        Return the percentile rank of value within all_values (0–100).
        Ties share the average rank.
        """
        if not all_values:
            return 50.0
        n = len(all_values)
        below = sum(1 for v in all_values if v < value)
        equal = sum(1 for v in all_values if v == value)
        # Midpoint rank
        rank = (below + 0.5 * equal) / n * 100.0
        return round(rank, 2)

    def _grade(self, score: float) -> str:
        for threshold, grade in self.GRADE_THRESHOLDS:
            if score < threshold:
                return grade
        return "D"
