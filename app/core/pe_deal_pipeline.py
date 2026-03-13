"""
PE Deal Pipeline Service.

CRUD operations for deal pipeline management and pipeline health insights.
Tracks deals from sourcing through close with stage management.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.core.pe_models import PEDeal, PEPortfolioCompany

logger = logging.getLogger(__name__)


class DealPipelineService:
    """Manage the PE deal pipeline."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_deals(
        self,
        status: Optional[str] = None,
        deal_type: Optional[str] = None,
        active_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """List pipeline deals with optional filters."""
        stmt = (
            select(PEDeal)
            .order_by(PEDeal.announced_date.desc().nullslast())
        )

        conditions = []
        if status:
            conditions.append(PEDeal.status == status)
        if deal_type:
            conditions.append(PEDeal.deal_type == deal_type)
        if active_only:
            conditions.append(PEDeal.status.in_(["Announced", "Pending"]))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        deals = list(self.db.execute(stmt).scalars().all())
        return [self._deal_to_dict(d) for d in deals]

    def create_deal(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new pipeline deal."""
        # Validate company exists
        company_id = data.get("company_id")
        if company_id:
            company = self.db.execute(
                select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
            ).scalar_one_or_none()
            if not company:
                return {"error": f"Company {company_id} not found"}

        deal = PEDeal(
            company_id=company_id,
            deal_name=data.get("deal_name"),
            deal_type=data.get("deal_type", "LBO"),
            deal_sub_type=data.get("deal_sub_type"),
            status=data.get("status", "Announced"),
            enterprise_value_usd=Decimal(str(data["enterprise_value_usd"])) if data.get("enterprise_value_usd") else None,
            ev_ebitda_multiple=Decimal(str(data["ev_ebitda_multiple"])) if data.get("ev_ebitda_multiple") else None,
            ev_revenue_multiple=Decimal(str(data["ev_revenue_multiple"])) if data.get("ev_revenue_multiple") else None,
            ltm_revenue_usd=Decimal(str(data["ltm_revenue_usd"])) if data.get("ltm_revenue_usd") else None,
            ltm_ebitda_usd=Decimal(str(data["ltm_ebitda_usd"])) if data.get("ltm_ebitda_usd") else None,
            buyer_name=data.get("buyer_name"),
            seller_name=data.get("seller_name"),
            seller_type=data.get("seller_type"),
            announced_date=data.get("announced_date"),
            expected_close_date=data.get("expected_close_date"),
            data_source=data.get("data_source", "api"),
        )
        self.db.add(deal)
        self.db.flush()
        self.db.commit()

        logger.info("Created pipeline deal: %s (id=%d)", deal.deal_name, deal.id)
        return self._deal_to_dict(deal)

    def update_deal(self, deal_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update a pipeline deal's status or fields."""
        deal = self.db.execute(
            select(PEDeal).where(PEDeal.id == deal_id)
        ).scalar_one_or_none()

        if not deal:
            return {"error": f"Deal {deal_id} not found"}

        # Apply updates
        updatable_fields = [
            "status", "deal_name", "deal_type", "deal_sub_type",
            "enterprise_value_usd", "ev_ebitda_multiple", "ev_revenue_multiple",
            "buyer_name", "seller_name", "seller_type",
            "expected_close_date", "closed_date",
        ]
        for field in updatable_fields:
            if field in updates:
                value = updates[field]
                if field in ("enterprise_value_usd", "ev_ebitda_multiple", "ev_revenue_multiple"):
                    value = Decimal(str(value)) if value is not None else None
                setattr(deal, field, value)

        # If status changed to Closed and no closed_date, set today
        if updates.get("status") == "Closed" and not deal.closed_date:
            deal.closed_date = date.today()

        self.db.flush()
        self.db.commit()

        logger.info("Updated deal %d: %s", deal_id, updates)
        return self._deal_to_dict(deal)

    def get_insights(self) -> Dict[str, Any]:
        """Get pipeline health insights."""
        deals = list(
            self.db.execute(select(PEDeal)).scalars().all()
        )
        return self._compute_insights(deals)

    # ------------------------------------------------------------------
    # Internal (static for testability)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_insights(deals: list) -> Dict[str, Any]:
        """Compute pipeline health metrics."""
        if not deals:
            return {
                "total_pipeline_deals": 0,
                "active_deals": 0,
                "total_pipeline_value_usd": 0,
                "stage_breakdown": {},
                "deal_type_breakdown": {},
                "avg_deal_size_usd": 0,
                "upcoming_closes": [],
            }

        # Stage breakdown
        stage_counts: Dict[str, int] = {}
        for d in deals:
            stage_counts[d.status] = stage_counts.get(d.status, 0) + 1

        # Active = non-Closed, non-Terminated
        active = sum(1 for d in deals if d.status in ("Announced", "Pending"))

        # Total value
        total_value = sum(
            float(d.enterprise_value_usd) for d in deals
            if d.enterprise_value_usd
        )

        # Deal type breakdown
        type_counts: Dict[str, int] = {}
        for d in deals:
            type_counts[d.deal_type] = type_counts.get(d.deal_type, 0) + 1

        # Average deal size
        valued_deals = [d for d in deals if d.enterprise_value_usd]
        avg_size = total_value / len(valued_deals) if valued_deals else 0

        # Upcoming closes (pending with expected close date)
        upcoming = []
        for d in deals:
            if d.status == "Pending" and d.expected_close_date:
                upcoming.append({
                    "deal_id": d.id,
                    "expected_close_date": d.expected_close_date.isoformat() if hasattr(d.expected_close_date, 'isoformat') else str(d.expected_close_date),
                })
        upcoming.sort(key=lambda x: x["expected_close_date"])

        return {
            "total_pipeline_deals": len(deals),
            "active_deals": active,
            "total_pipeline_value_usd": total_value,
            "stage_breakdown": stage_counts,
            "deal_type_breakdown": type_counts,
            "avg_deal_size_usd": round(avg_size, 2),
            "upcoming_closes": upcoming[:5],
        }

    @staticmethod
    def _deal_to_dict(deal: PEDeal) -> Dict[str, Any]:
        """Serialize a PEDeal to dict."""
        return {
            "id": deal.id,
            "company_id": deal.company_id,
            "deal_name": deal.deal_name,
            "deal_type": deal.deal_type,
            "deal_sub_type": deal.deal_sub_type,
            "status": deal.status,
            "enterprise_value_usd": float(deal.enterprise_value_usd) if deal.enterprise_value_usd else None,
            "ev_ebitda_multiple": float(deal.ev_ebitda_multiple) if deal.ev_ebitda_multiple else None,
            "ev_revenue_multiple": float(deal.ev_revenue_multiple) if deal.ev_revenue_multiple else None,
            "ltm_revenue_usd": float(deal.ltm_revenue_usd) if deal.ltm_revenue_usd else None,
            "ltm_ebitda_usd": float(deal.ltm_ebitda_usd) if deal.ltm_ebitda_usd else None,
            "buyer_name": deal.buyer_name,
            "seller_name": deal.seller_name,
            "seller_type": deal.seller_type,
            "announced_date": deal.announced_date.isoformat() if deal.announced_date else None,
            "closed_date": deal.closed_date.isoformat() if deal.closed_date else None,
            "expected_close_date": deal.expected_close_date.isoformat() if deal.expected_close_date else None,
        }
