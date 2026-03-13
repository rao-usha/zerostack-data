"""
PE Valuation Comparables Service.

Calculates EV/Revenue and EV/EBITDA multiples for a portfolio company
against its peer set. Supports exit pricing by providing median, quartile,
and percentile rank comparisons.
"""

import logging
import statistics
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyValuation,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


class ValuationCompsService:
    """Calculate valuation comparables for PE portfolio companies."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_comps(self, company_id: int) -> Dict[str, Any]:
        """Get valuation comparables for a company vs. its peer set.

        Returns company multiples, peer stats (median/P25/P75),
        and percentile rank within the peer set.
        """
        # Get the target company
        company = self.db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
        ).scalar_one_or_none()

        if not company:
            return {"error": f"Company {company_id} not found"}

        # Get latest valuation for the company
        company_val = self._get_latest_valuation(company_id)
        company_fin = self._get_latest_financials(company_id)

        # Compute company multiples
        ev = float(company_val.enterprise_value_usd) if company_val and company_val.enterprise_value_usd else None
        revenue = float(company_fin.revenue_usd) if company_fin and company_fin.revenue_usd else None
        ebitda = float(company_fin.ebitda_usd) if company_fin and company_fin.ebitda_usd else None

        company_multiples = self._compute_multiples(ev, revenue, ebitda)

        # Build peer set — same industry, excluding target
        peers = self._get_peers(company, exclude_id=company_id)

        # Compute peer multiples
        peer_ev_revenue_list = []
        peer_ev_ebitda_list = []
        peer_details = []

        for peer_co, peer_val, peer_fin in peers:
            p_ev = float(peer_val.enterprise_value_usd) if peer_val and peer_val.enterprise_value_usd else None
            p_rev = float(peer_fin.revenue_usd) if peer_fin and peer_fin.revenue_usd else None
            p_ebitda = float(peer_fin.ebitda_usd) if peer_fin and peer_fin.ebitda_usd else None

            p_multiples = self._compute_multiples(p_ev, p_rev, p_ebitda)

            if p_multiples["ev_revenue"] is not None:
                peer_ev_revenue_list.append(p_multiples["ev_revenue"])
            if p_multiples["ev_ebitda"] is not None:
                peer_ev_ebitda_list.append(p_multiples["ev_ebitda"])

            peer_details.append({
                "id": peer_co.id,
                "name": peer_co.name,
                "industry": peer_co.industry,
                "sub_industry": peer_co.sub_industry,
                "enterprise_value": p_ev,
                "revenue": p_rev,
                "ebitda": p_ebitda,
                "ev_revenue": p_multiples["ev_revenue"],
                "ev_ebitda": p_multiples["ev_ebitda"],
                "ownership_status": peer_co.ownership_status,
            })

        # Compute peer stats
        peer_ev_revenue_stats = self._compute_peer_stats(peer_ev_revenue_list)
        peer_ev_ebitda_stats = self._compute_peer_stats(peer_ev_ebitda_list)

        # Percentile ranks
        ev_rev_pctile = self._percentile_rank(
            company_multiples["ev_revenue"], peer_ev_revenue_list
        ) if company_multiples["ev_revenue"] is not None else None

        ev_ebitda_pctile = self._percentile_rank(
            company_multiples["ev_ebitda"], peer_ev_ebitda_list
        ) if company_multiples["ev_ebitda"] is not None else None

        return {
            "company_id": company.id,
            "company_name": company.name,
            "industry": company.industry,
            "sub_industry": company.sub_industry,
            "enterprise_value": ev,
            "revenue": revenue,
            "ebitda": ebitda,
            "ev_revenue": company_multiples["ev_revenue"],
            "ev_ebitda": company_multiples["ev_ebitda"],
            "peer_ev_revenue": peer_ev_revenue_stats,
            "peer_ev_ebitda": peer_ev_ebitda_stats,
            "ev_revenue_percentile": ev_rev_pctile,
            "ev_ebitda_percentile": ev_ebitda_pctile,
            "peer_companies": peer_details,
        }

    # ------------------------------------------------------------------
    # Static computation methods
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_multiples(
        enterprise_value: Optional[float],
        revenue: Optional[float],
        ebitda: Optional[float],
    ) -> Dict[str, Optional[float]]:
        """Compute EV/Revenue and EV/EBITDA multiples."""
        ev_revenue = None
        ev_ebitda = None

        if enterprise_value and revenue and revenue > 0:
            ev_revenue = round(enterprise_value / revenue, 2)

        if enterprise_value and ebitda and ebitda > 0:
            ev_ebitda = round(enterprise_value / ebitda, 2)

        return {"ev_revenue": ev_revenue, "ev_ebitda": ev_ebitda}

    @staticmethod
    def _compute_peer_stats(multiples: List[float]) -> Dict[str, Any]:
        """Compute median, P25, P75 from a list of multiples."""
        if not multiples:
            return {"median": None, "p25": None, "p75": None, "min": None, "max": None, "count": 0}

        sorted_m = sorted(multiples)
        n = len(sorted_m)

        return {
            "median": round(statistics.median(sorted_m), 2),
            "p25": round(sorted_m[max(0, n // 4 - (0 if n % 4 else 1))], 2) if n >= 2 else round(sorted_m[0], 2),
            "p75": round(sorted_m[min(n - 1, 3 * n // 4)], 2) if n >= 2 else round(sorted_m[0], 2),
            "min": round(sorted_m[0], 2),
            "max": round(sorted_m[-1], 2),
            "count": n,
        }

    @staticmethod
    def _percentile_rank(value: float, peer_values: List[float]) -> Optional[int]:
        """Compute percentile rank of value within peer values (0-100)."""
        if not peer_values:
            return None

        below = sum(1 for v in peer_values if v < value)
        equal = sum(1 for v in peer_values if v == value)
        pct = (below + 0.5 * equal) / len(peer_values) * 100
        return round(pct)

    # ------------------------------------------------------------------
    # DB queries
    # ------------------------------------------------------------------

    def _get_latest_valuation(self, company_id: int) -> Optional[PECompanyValuation]:
        """Get most recent valuation for a company."""
        return self.db.execute(
            select(PECompanyValuation)
            .where(PECompanyValuation.company_id == company_id)
            .order_by(PECompanyValuation.valuation_date.desc())
            .limit(1)
        ).scalar_one_or_none()

    def _get_latest_financials(self, company_id: int) -> Optional[PECompanyFinancials]:
        """Get most recent financials for a company."""
        return self.db.execute(
            select(PECompanyFinancials)
            .where(PECompanyFinancials.company_id == company_id)
            .order_by(PECompanyFinancials.fiscal_year.desc())
            .limit(1)
        ).scalar_one_or_none()

    def _get_peers(self, company: PEPortfolioCompany, exclude_id: int):
        """Find peer companies in same industry with valuations and financials."""
        # Match by industry (broad) — could narrow to sub_industry if enough peers
        peer_companies = self.db.execute(
            select(PEPortfolioCompany)
            .where(
                PEPortfolioCompany.industry == company.industry,
                PEPortfolioCompany.id != exclude_id,
                PEPortfolioCompany.status == "Active",
            )
        ).scalars().all()

        peers = []
        for peer in peer_companies:
            val = self._get_latest_valuation(peer.id)
            fin = self._get_latest_financials(peer.id)
            if val or fin:  # include if we have any data
                peers.append((peer, val, fin))

        return peers
