"""
PE Roll-Up Market Screener.

Combines industry fragmentation scoring with company discovery to identify
acquisition targets for PE roll-up strategies. Searches PE portfolio companies
and industrial companies, enriches with financials, and scores each target.

Target scoring (0-100, higher = better acquisition target):
  - Size fit (35%): Sweet spot $5-50M revenue
  - Ownership (25%): Independent > VC-backed > PE-backed
  - Geography match (20%): In target state scores highest
  - Growth signals (20%): Revenue growth + employee count
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PEPortfolioCompany,
)
from app.sources.rollup_intel.metadata import NAICS_DESCRIPTIONS

logger = logging.getLogger(__name__)


class RollUpScreener:
    """Screen for roll-up acquisition targets in fragmented markets."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def screen(
        self,
        naics_code: str,
        state: Optional[str] = None,
        min_revenue: Optional[float] = None,
        max_revenue: Optional[float] = None,
        exclude_pe_backed: bool = False,
        top_n: int = 20,
    ) -> Dict[str, Any]:
        """Screen for acquisition targets in a given industry.

        Returns ranked target list with fragmentation context.
        """
        # Get fragmentation score
        frag_score = await self._get_fragmentation_score(naics_code)

        # Query matching companies with financials
        companies = self._query_companies(naics_code)

        # Apply filters
        filtered = self._apply_filters(
            companies,
            state=state,
            min_revenue=min_revenue,
            max_revenue=max_revenue,
            exclude_pe_backed=exclude_pe_backed,
        )

        # Score and rank
        ranked = self._rank_targets(filtered, target_state=state)[:top_n]

        return {
            "naics_code": naics_code,
            "naics_description": NAICS_DESCRIPTIONS.get(naics_code, "Unknown"),
            "fragmentation_score": frag_score,
            "total_targets": len(filtered),
            "targets": ranked,
        }

    async def get_summary(
        self,
        naics_code: str,
        state: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Market overview for a NAICS code — total addressable, sizing, geos."""
        frag_score = await self._get_fragmentation_score(naics_code)
        companies = self._query_companies(naics_code)

        if state:
            companies = [c for c in companies if c.get("headquarters_state") == state]

        summary = self._build_summary(companies, naics_code)
        summary["fragmentation_score"] = frag_score

        # Top states by target count
        state_counts: Dict[str, int] = {}
        for c in companies:
            st = c.get("headquarters_state")
            if st:
                state_counts[st] = state_counts.get(st, 0) + 1
        top_geos = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        summary["top_states"] = [{"state": s, "count": n} for s, n in top_geos]

        return summary

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    @staticmethod
    def _score_target(
        target: Dict[str, Any],
        target_state: Optional[str] = None,
    ) -> float:
        """Score an acquisition target 0-100.

        Components:
          Size fit (35%): $5-50M → 100; <$2M or >$100M → low
          Ownership (25%): Private → 100; VC → 50; PE → 20
          Geography (20%): In target state → 100; else → 40
          Growth (20%): >15% growth → 100; 0% → 30; negative → 10
        """
        revenue = target.get("revenue_usd") or 0
        ownership = target.get("ownership_status", "Private")
        state = target.get("headquarters_state")
        growth = target.get("revenue_growth_pct") or 0

        # Size fit (35%)
        if revenue <= 0:
            size_score = 20  # unknown revenue
        elif 5_000_000 <= revenue <= 50_000_000:
            size_score = 100  # sweet spot
        elif 2_000_000 <= revenue < 5_000_000:
            size_score = 60 + (revenue - 2_000_000) / 3_000_000 * 40
        elif 50_000_000 < revenue <= 100_000_000:
            size_score = 100 - (revenue - 50_000_000) / 50_000_000 * 60
        elif revenue > 100_000_000:
            size_score = max(0, 40 - (revenue - 100_000_000) / 100_000_000 * 40)
        else:
            size_score = 30  # < $2M

        # Ownership (25%)
        ownership_scores = {
            "Private": 100,
            "Independent": 100,
            "Founder-Owned": 95,
            "Family-Owned": 90,
            "VC-Backed": 50,
            "PE-Backed": 20,
            "Public": 10,
        }
        own_score = ownership_scores.get(ownership, 60)

        # Geography match (20%)
        if target_state and state == target_state:
            geo_score = 100
        elif target_state:
            geo_score = 40
        else:
            geo_score = 60  # no state filter → neutral

        # Growth signals (20%)
        if growth >= 20:
            growth_score = 100
        elif growth >= 10:
            growth_score = 70 + (growth - 10) / 10 * 30
        elif growth >= 0:
            growth_score = 30 + growth / 10 * 40
        else:
            growth_score = max(0, 30 + growth)  # negative growth penalized

        score = (
            size_score * 0.35
            + own_score * 0.25
            + geo_score * 0.20
            + growth_score * 0.20
        )
        return round(max(0, min(100, score)), 1)

    @staticmethod
    def _rank_targets(
        targets: List[Dict[str, Any]],
        target_state: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Score and rank targets descending."""
        scored = []
        for t in targets:
            score = RollUpScreener._score_target(t, target_state=target_state)
            scored.append({
                **t,
                "target_score": score,
                "acquisition_rationale": _build_rationale(t, score),
            })
        scored.sort(key=lambda x: x["target_score"], reverse=True)
        return scored

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_filters(
        companies: List[Dict[str, Any]],
        state: Optional[str] = None,
        min_revenue: Optional[float] = None,
        max_revenue: Optional[float] = None,
        exclude_pe_backed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Apply filters to company list."""
        result = companies

        if state:
            result = [c for c in result if c.get("headquarters_state") == state]

        if exclude_pe_backed:
            result = [c for c in result if c.get("ownership_status") != "PE-Backed"]

        if min_revenue is not None:
            result = [c for c in result if (c.get("revenue_usd") or 0) >= min_revenue]

        if max_revenue is not None:
            result = [c for c in result if (c.get("revenue_usd") or 0) <= max_revenue]

        return result

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    @staticmethod
    def _build_summary(
        targets: List[Dict[str, Any]],
        naics_code: str,
    ) -> Dict[str, Any]:
        """Build market summary from target list."""
        if not targets:
            return {
                "naics_code": naics_code,
                "naics_description": NAICS_DESCRIPTIONS.get(naics_code, "Unknown"),
                "total_targets": 0,
                "total_addressable_revenue": 0,
                "avg_revenue": 0,
                "avg_employee_count": 0,
                "ownership_breakdown": {},
                "top_states": [],
            }

        revenues = [t.get("revenue_usd") or 0 for t in targets]
        employees = [t.get("employee_count") or 0 for t in targets]

        ownership_breakdown: Dict[str, int] = {}
        for t in targets:
            own = t.get("ownership_status", "Unknown")
            ownership_breakdown[own] = ownership_breakdown.get(own, 0) + 1

        return {
            "naics_code": naics_code,
            "naics_description": NAICS_DESCRIPTIONS.get(naics_code, "Unknown"),
            "total_targets": len(targets),
            "total_addressable_revenue": sum(revenues),
            "avg_revenue": sum(revenues) / len(targets) if targets else 0,
            "avg_employee_count": sum(employees) / len(targets) if targets else 0,
            "median_revenue": sorted(revenues)[len(revenues) // 2] if revenues else 0,
            "ownership_breakdown": ownership_breakdown,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _query_companies(self, naics_code: str) -> List[Dict[str, Any]]:
        """Query companies matching a NAICS code, enriched with latest financials."""
        # Find matching companies
        stmt = (
            select(PEPortfolioCompany)
            .where(
                PEPortfolioCompany.naics_code == naics_code,
                PEPortfolioCompany.status == "Active",
            )
            .order_by(PEPortfolioCompany.name)
        )
        companies = self.db.execute(stmt).scalars().all()

        results = []
        for co in companies:
            # Get latest financials
            fin_stmt = (
                select(PECompanyFinancials)
                .where(PECompanyFinancials.company_id == co.id)
                .order_by(PECompanyFinancials.fiscal_year.desc())
                .limit(1)
            )
            fin = self.db.execute(fin_stmt).scalar_one_or_none()

            results.append({
                "id": co.id,
                "name": co.name,
                "industry": co.industry,
                "sub_industry": co.sub_industry,
                "naics_code": co.naics_code,
                "headquarters_city": co.headquarters_city,
                "headquarters_state": co.headquarters_state,
                "ownership_status": co.ownership_status,
                "current_pe_owner": co.current_pe_owner,
                "employee_count": co.employee_count,
                "founded_year": co.founded_year,
                "website": co.website,
                "description": co.description,
                "revenue_usd": float(fin.revenue_usd) if fin and fin.revenue_usd else None,
                "revenue_growth_pct": float(fin.revenue_growth_pct) if fin and fin.revenue_growth_pct else None,
                "ebitda_margin_pct": float(fin.ebitda_margin_pct) if fin and fin.ebitda_margin_pct else None,
                "ebitda_usd": float(fin.ebitda_usd) if fin and fin.ebitda_usd else None,
            })

        return results

    async def _get_fragmentation_score(self, naics_code: str) -> float:
        """Get fragmentation score, returning 0 if unavailable."""
        try:
            from app.core.pe_fragmentation import FragmentationScorer
            scorer = FragmentationScorer(self.db)
            result = await scorer.score_industry(naics_code)
            return result.get("national_score", 0)
        except Exception as e:
            logger.warning("Could not get fragmentation score for %s: %s", naics_code, e)
            return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rationale(target: Dict[str, Any], score: float) -> str:
    """Build a short acquisition rationale string."""
    parts = []
    revenue = target.get("revenue_usd") or 0
    ownership = target.get("ownership_status", "")
    growth = target.get("revenue_growth_pct") or 0

    if 5_000_000 <= revenue <= 50_000_000:
        parts.append(f"${revenue/1_000_000:.0f}M revenue in PE sweet spot")
    elif revenue > 0:
        parts.append(f"${revenue/1_000_000:.0f}M revenue")

    if ownership in ("Private", "Independent", "Founder-Owned", "Family-Owned"):
        parts.append("independently owned")
    elif ownership == "VC-Backed":
        parts.append("VC-backed (potential secondary)")

    if growth >= 15:
        parts.append(f"{growth:.0f}% revenue growth")
    elif growth >= 5:
        parts.append("steady growth")

    if not parts:
        return "Potential acquisition target"
    return "; ".join(parts).capitalize()
