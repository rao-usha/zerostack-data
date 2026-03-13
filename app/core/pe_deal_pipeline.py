"""
PE Deal Pipeline Service.

CRUD operations for deal pipeline management and pipeline health insights.
Tracks deals from sourcing through close with stage management.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, func
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEDeal, PEDealParticipant, PEFirm, PEFund, PEFundInvestment, PEPortfolioCompany,
)

PIPELINE_STAGES = ["Screening", "DD", "LOI", "Closing", "Won", "Lost"]
ACTIVE_STAGES = ["Screening", "DD", "LOI", "Closing"]
TERMINAL_STAGES = ["Won", "Lost"]

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
    # Firm-scoped pipeline API
    # ------------------------------------------------------------------

    def list_firm_deals(self, firm_id: int) -> Dict[str, Any]:
        """List deals for a firm, grouped by pipeline stage."""
        firm = self.db.execute(
            select(PEFirm).where(PEFirm.id == firm_id)
        ).scalar_one_or_none()
        if not firm:
            return {"error": f"Firm {firm_id} not found"}

        deals = self._fetch_firm_deals(firm_id, firm.name)
        grouped = self._group_by_stage(deals)

        return {
            "firm_id": firm_id,
            "firm_name": firm.name,
            "total_deals": len(deals),
            "stages": {
                stage: [self._deal_to_dict(d) for d in stage_deals]
                for stage, stage_deals in grouped.items()
            },
        }

    def create_firm_deal(self, firm_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a deal linked to a firm via PEDealParticipant."""
        firm = self.db.execute(
            select(PEFirm).where(PEFirm.id == firm_id)
        ).scalar_one_or_none()
        if not firm:
            return {"error": f"Firm {firm_id} not found"}

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
            status=data.get("status", "Screening"),
            enterprise_value_usd=Decimal(str(data["enterprise_value_usd"])) if data.get("enterprise_value_usd") else None,
            ev_ebitda_multiple=Decimal(str(data["ev_ebitda_multiple"])) if data.get("ev_ebitda_multiple") else None,
            ev_revenue_multiple=Decimal(str(data["ev_revenue_multiple"])) if data.get("ev_revenue_multiple") else None,
            ltm_revenue_usd=Decimal(str(data["ltm_revenue_usd"])) if data.get("ltm_revenue_usd") else None,
            ltm_ebitda_usd=Decimal(str(data["ltm_ebitda_usd"])) if data.get("ltm_ebitda_usd") else None,
            buyer_name=firm.name,
            seller_name=data.get("seller_name"),
            seller_type=data.get("seller_type"),
            announced_date=data.get("announced_date"),
            expected_close_date=data.get("expected_close_date"),
            data_source=data.get("data_source", "api"),
        )
        self.db.add(deal)
        self.db.flush()

        # Link deal to firm via participant
        participant = PEDealParticipant(
            deal_id=deal.id,
            firm_id=firm_id,
            participant_name=firm.name,
            participant_type="PE Firm",
            role="Lead Sponsor",
            is_lead=True,
        )
        self.db.add(participant)
        self.db.commit()

        logger.info("Created firm pipeline deal: %s for firm %s", deal.deal_name, firm.name)
        return self._deal_to_dict(deal)

    def update_deal_stage(self, deal_id: int, stage: str) -> Dict[str, Any]:
        """Move a deal to a new pipeline stage."""
        if stage not in PIPELINE_STAGES:
            return {"error": f"Invalid stage '{stage}'. Valid: {PIPELINE_STAGES}"}

        deal = self.db.execute(
            select(PEDeal).where(PEDeal.id == deal_id)
        ).scalar_one_or_none()
        if not deal:
            return {"error": f"Deal {deal_id} not found"}

        old_stage = deal.status
        deal.status = stage

        if stage == "Won" and not deal.closed_date:
            deal.closed_date = date.today()

        self.db.flush()
        self.db.commit()

        logger.info("Deal %d stage: %s → %s", deal_id, old_stage, stage)
        return self._deal_to_dict(deal)

    def get_firm_insights(self, firm_id: int) -> Dict[str, Any]:
        """Pipeline health metrics for a specific firm."""
        firm = self.db.execute(
            select(PEFirm).where(PEFirm.id == firm_id)
        ).scalar_one_or_none()
        if not firm:
            return {"error": f"Firm {firm_id} not found"}

        deals = self._fetch_firm_deals(firm_id, firm.name)
        insights = self._compute_firm_insights(deals)
        insights["firm_id"] = firm_id
        insights["firm_name"] = firm.name
        return insights

    # ------------------------------------------------------------------
    # Internal: data fetching
    # ------------------------------------------------------------------

    def _fetch_firm_deals(self, firm_id: int, firm_name: str) -> List[PEDeal]:
        """Fetch deals linked to a firm via participants, buyer_name, or portfolio."""
        # Deals where firm is a participant
        participant_deal_ids = self.db.execute(
            select(PEDealParticipant.deal_id).where(
                PEDealParticipant.firm_id == firm_id
            )
        ).scalars().all()

        # Deals where firm is buyer
        buyer_deal_ids = self.db.execute(
            select(PEDeal.id).where(PEDeal.buyer_name == firm_name)
        ).scalars().all()

        # Deals for portfolio companies (via fund investments)
        portfolio_deal_ids = self.db.execute(
            select(PEDeal.id)
            .join(PEFundInvestment, PEFundInvestment.company_id == PEDeal.company_id)
            .join(PEFund, PEFund.id == PEFundInvestment.fund_id)
            .where(PEFund.firm_id == firm_id)
        ).scalars().all()

        all_ids = set(participant_deal_ids) | set(buyer_deal_ids) | set(portfolio_deal_ids)
        if not all_ids:
            return []

        return list(
            self.db.execute(
                select(PEDeal)
                .where(PEDeal.id.in_(all_ids))
                .order_by(PEDeal.announced_date.desc().nullslast())
            ).scalars().all()
        )

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
    def _group_by_stage(deals: list) -> Dict[str, list]:
        """Group deals by pipeline stage, ensuring all stages present."""
        grouped: Dict[str, list] = {stage: [] for stage in PIPELINE_STAGES}
        for d in deals:
            stage = d.status
            if stage in grouped:
                grouped[stage].append(d)
            # Legacy statuses map to pipeline stages
            elif stage in ("Announced", "Screening"):
                grouped["Screening"].append(d)
            elif stage == "Pending":
                grouped["DD"].append(d)
            elif stage == "Closed":
                grouped["Won"].append(d)
            elif stage == "Terminated":
                grouped["Lost"].append(d)
        return grouped

    @staticmethod
    def _compute_firm_insights(deals: list) -> Dict[str, Any]:
        """Compute pipeline health metrics for a firm's deals."""
        if not deals:
            return {
                "total_deals": 0,
                "active_deals": 0,
                "won_deals": 0,
                "lost_deals": 0,
                "total_pipeline_value_usd": 0,
                "avg_deal_size_usd": 0,
                "win_rate_pct": None,
                "stage_breakdown": {},
            }

        stage_counts: Dict[str, int] = {}
        for d in deals:
            stage_counts[d.status] = stage_counts.get(d.status, 0) + 1

        won = stage_counts.get("Won", 0) + stage_counts.get("Closed", 0)
        lost = stage_counts.get("Lost", 0) + stage_counts.get("Terminated", 0)
        active = sum(
            1 for d in deals
            if d.status in ACTIVE_STAGES or d.status in ("Announced", "Pending")
        )

        total_value = sum(
            float(d.enterprise_value_usd) for d in deals
            if d.enterprise_value_usd
        )
        valued = [d for d in deals if d.enterprise_value_usd]
        avg_size = total_value / len(valued) if valued else 0

        decided = won + lost
        win_rate = round(won / decided * 100, 1) if decided > 0 else None

        return {
            "total_deals": len(deals),
            "active_deals": active,
            "won_deals": won,
            "lost_deals": lost,
            "total_pipeline_value_usd": total_value,
            "avg_deal_size_usd": round(avg_size, 2),
            "win_rate_pct": win_rate,
            "stage_breakdown": stage_counts,
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
