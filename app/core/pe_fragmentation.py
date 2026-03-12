"""
PE Industry Fragmentation Scorer.

Uses Census County Business Patterns (CBP) data to score industry
fragmentation and identify markets ripe for roll-up acquisitions.

Scoring formula (0-100, higher = more fragmented):
  - HHI component (40%): Lower HHI → higher score
  - Small biz % component (35%): More small firms → higher score
  - Avg establishment size component (25%): Smaller avg → higher score
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.sources.rollup_intel.metadata import NAICS_DESCRIPTIONS, GRADE_THRESHOLDS

logger = logging.getLogger(__name__)


class FragmentationScorer:
    """Score industry fragmentation using Census CBP data."""

    def __init__(self, db: Session):
        self.db = db
        self._collector = None

    @property
    def collector(self):
        if self._collector is None:
            from app.sources.rollup_intel.cbp_collector import CBPCollector
            self._collector = CBPCollector(self.db)
        return self._collector

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def score_industry(
        self,
        naics_code: str,
        year: int = 2021,
    ) -> Dict[str, Any]:
        """Score fragmentation for a single NAICS code nationally.

        Returns national score, grade, aggregate stats, and top markets.
        """
        # Fetch/cache CBP data
        await self.collector.collect(naics_code, year=year)

        # Read cached data
        records = self.collector.get_cached(naics_code, year=year)

        if not records:
            return {
                "naics_code": naics_code,
                "naics_description": NAICS_DESCRIPTIONS.get(naics_code, "Unknown"),
                "national_score": 0,
                "national_grade": "F",
                "total_establishments": 0,
                "total_employees": 0,
                "county_count": 0,
                "top_markets": [],
                "year": year,
            }

        result = self._aggregate_national(records, naics_code)
        result["naics_description"] = NAICS_DESCRIPTIONS.get(naics_code, "Unknown")
        result["year"] = year
        result["top_markets"] = self._rank_markets(records)[:20]

        return result

    async def scan_industries(
        self,
        naics_codes: List[str],
        year: int = 2021,
    ) -> List[Dict[str, Any]]:
        """Scan multiple NAICS codes and rank by fragmentation score."""
        if not naics_codes:
            return []

        results = []
        for code in naics_codes:
            try:
                result = await self.score_industry(code, year=year)
                results.append(result)
            except Exception as e:
                logger.error(f"Error scoring NAICS {code}: {e}")
                results.append({
                    "naics_code": code,
                    "naics_description": NAICS_DESCRIPTIONS.get(code, "Unknown"),
                    "national_score": 0,
                    "error": str(e),
                })

        # Sort descending by score
        results.sort(key=lambda r: r.get("national_score", 0), reverse=True)
        return results

    def get_roll_up_targets(
        self,
        naics_code: str,
        state: str,
        year: int = 2021,
        top_n: int = 20,
        min_establishments: int = 10,
    ) -> Dict[str, Any]:
        """Find counties in a state with high fragmentation for roll-up.

        Args:
            naics_code: NAICS industry code
            state: 2-digit state FIPS code
            year: Data year
            top_n: Number of top markets to return
            min_establishments: Minimum establishments to be a viable target
        """
        records = self.collector.get_cached(
            naics_code, year=year, state=state,
            min_establishments=min_establishments,
        )

        if not records:
            return {
                "naics_code": naics_code,
                "state": state,
                "targets": [],
                "state_summary": None,
            }

        targets = self._rank_markets(records)[:top_n]

        # State-level summary
        total_estab = sum(r.get("establishments") or 0 for r in records)
        total_emp = sum(r.get("employees") or 0 for r in records)
        avg_hhi = _safe_avg([r.get("hhi") for r in records])
        avg_sbp = _safe_avg([r.get("small_biz_pct") for r in records])
        avg_size = _safe_avg([r.get("avg_employees_per_estab") for r in records])

        state_score = self._compute_score(avg_hhi, avg_sbp, avg_size)

        return {
            "naics_code": naics_code,
            "naics_description": NAICS_DESCRIPTIONS.get(naics_code, "Unknown"),
            "state": state,
            "year": year,
            "targets": targets,
            "state_summary": {
                "total_establishments": total_estab,
                "total_employees": total_emp,
                "county_count": len(records),
                "state_score": state_score,
                "state_grade": _score_to_grade(state_score),
                "avg_hhi": round(avg_hhi, 6) if avg_hhi else None,
                "avg_small_biz_pct": round(avg_sbp, 4) if avg_sbp else None,
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_score(
        hhi: Optional[float],
        small_biz_pct: Optional[float],
        avg_size: Optional[float],
    ) -> float:
        """Compute fragmentation score 0-100.

        Components:
          HHI (40%): 0.0 → 100, 0.15 → 60, 0.25 → 40, 1.0 → 0
          Small biz % (35%): 1.0 → 100, 0.0 → 0
          Avg size (25%): 1 emp → 100, 50 → 50, 500+ → 0
        """
        if hhi is None and small_biz_pct is None and avg_size is None:
            return 0

        # HHI component (40%) — inverse mapping
        if hhi is not None:
            # Clamp HHI to [0, 1]
            hhi_clamped = max(0.0, min(1.0, float(hhi)))
            # Linear inverse: 0 → 100, 1 → 0
            hhi_score = (1.0 - hhi_clamped) * 100
        else:
            hhi_score = 50  # neutral default

        # Small biz % component (35%) — direct mapping
        if small_biz_pct is not None:
            sbp_clamped = max(0.0, min(1.0, float(small_biz_pct)))
            sbp_score = sbp_clamped * 100
        else:
            sbp_score = 50

        # Avg size component (25%) — inverse log scale
        if avg_size is not None:
            avg_s = max(1.0, float(avg_size))
            # 1 → 100, 10 → 75, 50 → 50, 250 → 25, 1000+ → ~0
            import math
            size_score = max(0, 100 - (math.log10(avg_s) / math.log10(1000)) * 100)
        else:
            size_score = 50

        # Weighted combination
        score = (hhi_score * 0.40) + (sbp_score * 0.35) + (size_score * 0.25)
        return round(max(0, min(100, score)), 1)

    def _rank_markets(self, records: List[Dict]) -> List[Dict]:
        """Rank county markets by fragmentation score, descending."""
        scored = []
        for rec in records:
            score = self._compute_score(
                rec.get("hhi"),
                rec.get("small_biz_pct"),
                rec.get("avg_employees_per_estab"),
            )
            scored.append({
                "county_fips": rec.get("county_fips"),
                "state_fips": rec.get("state_fips"),
                "geo_name": rec.get("geo_name"),
                "score": score,
                "grade": _score_to_grade(score),
                "establishments": rec.get("establishments"),
                "employees": rec.get("employees"),
                "hhi": rec.get("hhi"),
                "small_biz_pct": rec.get("small_biz_pct"),
                "avg_employees_per_estab": rec.get("avg_employees_per_estab"),
            })

        scored.sort(key=lambda r: r["score"], reverse=True)
        return scored

    def _aggregate_national(
        self, records: List[Dict], naics_code: str
    ) -> Dict[str, Any]:
        """Aggregate county records into a national summary."""
        total_estab = sum(r.get("establishments") or 0 for r in records)
        total_emp = sum(r.get("employees") or 0 for r in records)

        avg_hhi = _safe_avg([r.get("hhi") for r in records])
        avg_sbp = _safe_avg([r.get("small_biz_pct") for r in records])
        avg_size = (total_emp / total_estab) if total_estab > 0 else None

        national_score = self._compute_score(avg_hhi, avg_sbp, avg_size)

        return {
            "naics_code": naics_code,
            "national_score": national_score,
            "national_grade": _score_to_grade(national_score),
            "total_establishments": total_estab,
            "total_employees": total_emp,
            "county_count": len(records),
            "avg_hhi": round(avg_hhi, 6) if avg_hhi else None,
            "avg_small_biz_pct": round(avg_sbp, 4) if avg_sbp else None,
            "avg_estab_size": round(avg_size, 1) if avg_size else None,
        }

    @staticmethod
    def _filter_by_state(records: List[Dict], state: str) -> List[Dict]:
        """Filter records to a specific state FIPS code."""
        return [r for r in records if r.get("state_fips") == state]


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _score_to_grade(score: float) -> str:
    """Convert numeric score to letter grade."""
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "F"


def _safe_avg(values: List[Optional[float]]) -> Optional[float]:
    """Average of non-None values, or None if empty."""
    valid = [float(v) for v in values if v is not None]
    return sum(valid) / len(valid) if valid else None
