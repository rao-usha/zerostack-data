"""
PE Comparable Transaction Service.

Queries completed exits in the same industry as a target company to provide
valuation support for exit pricing. Calculates market stats including median
deal multiples, deal volume trends, and buyer type distribution.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, or_
from sqlalchemy.orm import Session

from app.core.pe_models import PEDeal, PEPortfolioCompany

logger = logging.getLogger(__name__)


class ComparableTransactionService:
    """Find and analyze comparable M&A transactions."""

    def __init__(self, db: Session):
        self.db = db

    def get_comps(
        self,
        company_id: int,
        years_back: int = 5,
        include_pending: bool = False,
    ) -> Dict[str, Any]:
        """Get comparable transactions for a target company.

        Finds completed exits in the same industry and returns deal details
        plus aggregate market statistics.
        """
        company = self.db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
        ).scalar_one_or_none()

        if not company:
            return {"error": f"Company {company_id} not found"}

        # Find deals in same industry
        deals = self._find_comparable_deals(
            company.industry, years_back, include_pending
        )

        # Build deal list
        deal_list = []
        ev_ebitda_multiples = []
        ev_revenue_multiples = []

        for deal in deals:
            d = {
                "id": deal.id,
                "deal_name": deal.deal_name,
                "deal_type": deal.deal_type,
                "deal_sub_type": deal.deal_sub_type,
                "buyer_name": deal.buyer_name,
                "seller_name": deal.seller_name,
                "seller_type": deal.seller_type,
                "enterprise_value_usd": float(deal.enterprise_value_usd) if deal.enterprise_value_usd else None,
                "ev_ebitda_multiple": float(deal.ev_ebitda_multiple) if deal.ev_ebitda_multiple else None,
                "ev_revenue_multiple": float(deal.ev_revenue_multiple) if deal.ev_revenue_multiple else None,
                "ltm_revenue_usd": float(deal.ltm_revenue_usd) if deal.ltm_revenue_usd else None,
                "ltm_ebitda_usd": float(deal.ltm_ebitda_usd) if deal.ltm_ebitda_usd else None,
                "announced_date": deal.announced_date.isoformat() if deal.announced_date else None,
                "closed_date": deal.closed_date.isoformat() if deal.closed_date else None,
                "status": deal.status,
            }
            deal_list.append(d)

            if deal.ev_ebitda_multiple:
                ev_ebitda_multiples.append(float(deal.ev_ebitda_multiple))
            if deal.ev_revenue_multiple:
                ev_revenue_multiples.append(float(deal.ev_revenue_multiple))

        # Market stats
        market_stats = self._compute_market_stats(
            ev_ebitda_multiples, ev_revenue_multiples, deals
        )

        return {
            "company_id": company.id,
            "company_name": company.name,
            "industry": company.industry,
            "sub_industry": company.sub_industry,
            "comparable_deals": deal_list,
            "deal_count": len(deal_list),
            "market_stats": market_stats,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _find_comparable_deals(
        self,
        industry: str,
        years_back: int,
        include_pending: bool,
    ) -> List[PEDeal]:
        """Find deals in the same industry with financial data."""
        cutoff = date(date.today().year - years_back, 1, 1)

        # Join with portfolio companies to match industry
        status_filter = [PEDeal.status == "Closed"]
        if include_pending:
            status_filter.append(PEDeal.status == "Pending")

        stmt = (
            select(PEDeal)
            .join(PEPortfolioCompany, PEDeal.company_id == PEPortfolioCompany.id)
            .where(
                PEPortfolioCompany.industry == industry,
                or_(*status_filter),
                PEDeal.enterprise_value_usd.isnot(None),
                or_(
                    PEDeal.announced_date >= cutoff,
                    PEDeal.closed_date >= cutoff,
                ),
            )
            .order_by(PEDeal.closed_date.desc().nullslast())
        )

        return list(self.db.execute(stmt).scalars().all())

    @staticmethod
    def _compute_market_stats(
        ev_ebitda: List[float],
        ev_revenue: List[float],
        deals: list,
    ) -> Dict[str, Any]:
        """Compute aggregate market statistics from deal data."""
        import statistics

        stats: Dict[str, Any] = {
            "total_deals": len(deals),
            "deals_with_multiples": len(ev_ebitda),
        }

        if ev_ebitda:
            sorted_m = sorted(ev_ebitda)
            n = len(sorted_m)
            stats["ev_ebitda_median"] = round(statistics.median(sorted_m), 2)
            stats["ev_ebitda_p25"] = round(sorted_m[max(0, n // 4 - (0 if n % 4 else 1))], 2)
            stats["ev_ebitda_p75"] = round(sorted_m[min(n - 1, 3 * n // 4)], 2)
            stats["ev_ebitda_min"] = round(sorted_m[0], 2)
            stats["ev_ebitda_max"] = round(sorted_m[-1], 2)
        else:
            stats["ev_ebitda_median"] = None

        if ev_revenue:
            sorted_r = sorted(ev_revenue)
            stats["ev_revenue_median"] = round(statistics.median(sorted_r), 2)
        else:
            stats["ev_revenue_median"] = None

        # Total deal value
        total_value = sum(
            float(d.enterprise_value_usd) for d in deals
            if d.enterprise_value_usd
        )
        stats["total_deal_value_usd"] = total_value

        # Buyer type breakdown
        buyer_types: Dict[str, int] = {}
        for d in deals:
            bt = d.seller_type or "Unknown"
            buyer_types[bt] = buyer_types.get(bt, 0) + 1
        stats["seller_type_breakdown"] = buyer_types

        # Trend: compare first half vs second half of multiples
        if len(ev_ebitda) >= 4:
            mid = len(ev_ebitda) // 2
            # deals are ordered newest first, so first half = recent
            recent_avg = sum(ev_ebitda[:mid]) / mid
            older_avg = sum(ev_ebitda[mid:]) / (len(ev_ebitda) - mid)
            if recent_avg > older_avg * 1.05:
                stats["multiple_trend"] = "expanding"
            elif recent_avg < older_avg * 0.95:
                stats["multiple_trend"] = "compressing"
            else:
                stats["multiple_trend"] = "stable"
        else:
            stats["multiple_trend"] = "insufficient_data"

        return stats
