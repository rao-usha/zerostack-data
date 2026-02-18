"""
PE Deals API endpoints.

Endpoints for managing M&A deal data including:
- Deal information
- Participants (investors, sellers)
- Advisors
- Deal team members
"""

import logging
from typing import Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pe/deals", tags=["PE Intelligence - Deals"])


# =============================================================================
# Request/Response Models
# =============================================================================


class DealCreate(BaseModel):
    """Request model for creating a deal."""

    company_id: int = Field(..., description="Portfolio company ID")
    deal_name: Optional[str] = Field(None, examples=["Blackstone Acquisition of XYZ"])
    deal_type: str = Field(..., examples=["LBO"])
    deal_sub_type: Optional[str] = Field(None, examples=["Platform"])

    announced_date: Optional[date] = Field(None)
    closed_date: Optional[date] = Field(None)

    enterprise_value_usd: Optional[float] = Field(None, examples=[1000000000])
    equity_value_usd: Optional[float] = Field(None)
    debt_amount_usd: Optional[float] = Field(None)

    ev_revenue_multiple: Optional[float] = Field(None, examples=[12.5])
    ev_ebitda_multiple: Optional[float] = Field(None, examples=[15.0])

    ltm_revenue_usd: Optional[float] = Field(None)
    ltm_ebitda_usd: Optional[float] = Field(None)

    buyer_name: Optional[str] = Field(None, examples=["Blackstone"])
    seller_name: Optional[str] = Field(None, examples=["Previous Owner LLC"])
    seller_type: Optional[str] = Field(None, examples=["PE"])

    status: Optional[str] = Field("Closed", examples=["Closed"])


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/")
async def list_deals(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    deal_type: Optional[str] = None,
    status: Optional[str] = None,
    buyer: Optional[str] = None,
    year: Optional[int] = None,
    min_ev: Optional[float] = None,
    db: Session = Depends(get_db),
):
    """
    List deals with filtering.

    **Query Parameters:**
    - `deal_type`: Filter by type (LBO, Growth, Add-on, Exit, etc.)
    - `status`: Filter by status (Announced, Pending, Closed, Terminated)
    - `buyer`: Filter by buyer name
    - `year`: Filter by closing year
    - `min_ev`: Minimum enterprise value (USD)
    """
    try:
        query = """
            SELECT
                d.id, d.deal_name, d.deal_type, d.deal_sub_type,
                c.id as company_id, c.name as company_name, c.industry,
                d.announced_date, d.closed_date,
                d.enterprise_value_usd, d.ev_ebitda_multiple,
                d.buyer_name, d.seller_name, d.seller_type,
                d.status
            FROM pe_deals d
            JOIN pe_portfolio_companies c ON d.company_id = c.id
            WHERE 1=1
        """
        params = {"limit": limit, "offset": offset}

        if deal_type:
            query += " AND d.deal_type = :deal_type"
            params["deal_type"] = deal_type

        if status:
            query += " AND d.status = :status"
            params["status"] = status

        if buyer:
            query += " AND d.buyer_name ILIKE :buyer"
            params["buyer"] = f"%{buyer}%"

        if year:
            query += " AND EXTRACT(YEAR FROM d.closed_date) = :year"
            params["year"] = year

        if min_ev:
            query += " AND d.enterprise_value_usd >= :min_ev"
            params["min_ev"] = min_ev

        query += " ORDER BY d.closed_date DESC NULLS LAST, d.announced_date DESC NULLS LAST LIMIT :limit OFFSET :offset"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        deals = []
        for row in rows:
            deals.append(
                {
                    "id": row[0],
                    "deal_name": row[1],
                    "deal_type": row[2],
                    "deal_sub_type": row[3],
                    "company": {"id": row[4], "name": row[5], "industry": row[6]},
                    "dates": {
                        "announced": row[7].isoformat() if row[7] else None,
                        "closed": row[8].isoformat() if row[8] else None,
                    },
                    "valuation": {
                        "enterprise_value_usd": float(row[9]) if row[9] else None,
                        "ev_ebitda_multiple": float(row[10]) if row[10] else None,
                    },
                    "parties": {
                        "buyer": row[11],
                        "seller": row[12],
                        "seller_type": row[13],
                    },
                    "status": row[14],
                }
            )

        return {"count": len(deals), "limit": limit, "offset": offset, "deals": deals}

    except Exception as e:
        logger.error(f"Error listing deals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_deals(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """
    Search deals by company name or deal name.
    """
    try:
        query = text("""
            SELECT d.id, d.deal_name, d.deal_type, c.name as company_name,
                   d.enterprise_value_usd, d.closed_date, d.buyer_name
            FROM pe_deals d
            JOIN pe_portfolio_companies c ON d.company_id = c.id
            WHERE d.deal_name ILIKE :search OR c.name ILIKE :search
            ORDER BY d.closed_date DESC NULLS LAST
            LIMIT :limit
        """)

        result = db.execute(query, {"search": f"%{q}%", "limit": limit})
        rows = result.fetchall()

        deals = []
        for row in rows:
            deals.append(
                {
                    "id": row[0],
                    "deal_name": row[1],
                    "deal_type": row[2],
                    "company_name": row[3],
                    "enterprise_value_usd": float(row[4]) if row[4] else None,
                    "closed_date": row[5].isoformat() if row[5] else None,
                    "buyer": row[6],
                }
            )

        return {"count": len(deals), "results": deals}

    except Exception as e:
        logger.error(f"Error searching deals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_deal(deal: DealCreate, db: Session = Depends(get_db)):
    """
    Create a deal record.
    """
    try:
        insert_sql = text("""
            INSERT INTO pe_deals (
                company_id, deal_name, deal_type, deal_sub_type,
                announced_date, closed_date,
                enterprise_value_usd, equity_value_usd, debt_amount_usd,
                ev_revenue_multiple, ev_ebitda_multiple,
                ltm_revenue_usd, ltm_ebitda_usd,
                buyer_name, seller_name, seller_type,
                status
            ) VALUES (
                :company_id, :deal_name, :deal_type, :deal_sub_type,
                :announced_date, :closed_date,
                :enterprise_value_usd, :equity_value_usd, :debt_amount_usd,
                :ev_revenue_multiple, :ev_ebitda_multiple,
                :ltm_revenue_usd, :ltm_ebitda_usd,
                :buyer_name, :seller_name, :seller_type,
                :status
            )
            RETURNING id, deal_name, created_at
        """)

        result = db.execute(insert_sql, deal.dict())
        row = result.fetchone()
        db.commit()

        return {
            "id": row[0],
            "deal_name": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
            "message": "Deal created successfully",
        }

    except Exception as e:
        logger.error(f"Error creating deal: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{deal_id}")
async def get_deal(deal_id: int, db: Session = Depends(get_db)):
    """
    Get detailed information for a deal.
    """
    try:
        query = text("""
            SELECT
                d.id, d.deal_name, d.deal_type, d.deal_sub_type,
                c.id as company_id, c.name as company_name, c.industry,
                d.announced_date, d.closed_date, d.expected_close_date,
                d.enterprise_value_usd, d.equity_value_usd, d.debt_amount_usd,
                d.ev_revenue_multiple, d.ev_ebitda_multiple, d.ev_ebit_multiple,
                d.ltm_revenue_usd, d.ltm_ebitda_usd,
                d.equity_pct, d.debt_pct, d.management_rollover_pct,
                d.buyer_name, d.seller_name, d.seller_type,
                d.status, d.is_announced, d.is_confidential,
                d.data_source, d.source_url, d.press_release_url,
                d.created_at, d.updated_at
            FROM pe_deals d
            JOIN pe_portfolio_companies c ON d.company_id = c.id
            WHERE d.id = :deal_id
        """)

        result = db.execute(query, {"deal_id": deal_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found")

        # Get participants
        participants_query = text("""
            SELECT
                id, participant_name, participant_type, role, is_lead,
                equity_contribution_usd, ownership_pct,
                firm_id, fund_id
            FROM pe_deal_participants
            WHERE deal_id = :deal_id
            ORDER BY is_lead DESC, equity_contribution_usd DESC NULLS LAST
        """)
        part_result = db.execute(participants_query, {"deal_id": deal_id})
        participants = [
            {
                "id": p[0],
                "name": p[1],
                "type": p[2],
                "role": p[3],
                "is_lead": p[4],
                "equity_contribution_usd": float(p[5]) if p[5] else None,
                "ownership_pct": float(p[6]) if p[6] else None,
                "firm_id": p[7],
                "fund_id": p[8],
            }
            for p in part_result.fetchall()
        ]

        # Get advisors
        advisors_query = text("""
            SELECT id, advisor_name, advisor_type, side, role_description
            FROM pe_deal_advisors
            WHERE deal_id = :deal_id
            ORDER BY side, advisor_type
        """)
        adv_result = db.execute(advisors_query, {"deal_id": deal_id})
        advisors = [
            {"id": a[0], "name": a[1], "type": a[2], "side": a[3], "role": a[4]}
            for a in adv_result.fetchall()
        ]

        # Get deal team
        team_query = text("""
            SELECT dpi.id, p.id as person_id, p.full_name, dpi.role, dpi.side,
                   f.name as firm_name
            FROM pe_deal_person_involvement dpi
            JOIN pe_people p ON dpi.person_id = p.id
            LEFT JOIN pe_firms f ON dpi.firm_id = f.id
            WHERE dpi.deal_id = :deal_id
        """)
        team_result = db.execute(team_query, {"deal_id": deal_id})
        deal_team = [
            {
                "id": t[0],
                "person_id": t[1],
                "name": t[2],
                "role": t[3],
                "side": t[4],
                "firm": t[5],
            }
            for t in team_result.fetchall()
        ]

        return {
            "id": row[0],
            "deal_name": row[1],
            "deal_type": row[2],
            "deal_sub_type": row[3],
            "company": {"id": row[4], "name": row[5], "industry": row[6]},
            "dates": {
                "announced": row[7].isoformat() if row[7] else None,
                "closed": row[8].isoformat() if row[8] else None,
                "expected_close": row[9].isoformat() if row[9] else None,
            },
            "valuation": {
                "enterprise_value_usd": float(row[10]) if row[10] else None,
                "equity_value_usd": float(row[11]) if row[11] else None,
                "debt_amount_usd": float(row[12]) if row[12] else None,
            },
            "multiples": {
                "ev_revenue": float(row[13]) if row[13] else None,
                "ev_ebitda": float(row[14]) if row[14] else None,
                "ev_ebit": float(row[15]) if row[15] else None,
            },
            "financials_at_deal": {
                "ltm_revenue_usd": float(row[16]) if row[16] else None,
                "ltm_ebitda_usd": float(row[17]) if row[17] else None,
            },
            "structure": {
                "equity_pct": float(row[18]) if row[18] else None,
                "debt_pct": float(row[19]) if row[19] else None,
                "management_rollover_pct": float(row[20]) if row[20] else None,
            },
            "parties": {"buyer": row[21], "seller": row[22], "seller_type": row[23]},
            "status": {
                "status": row[24],
                "is_announced": row[25],
                "is_confidential": row[26],
            },
            "sources": {
                "data_source": row[27],
                "source_url": row[28],
                "press_release_url": row[29],
            },
            "metadata": {
                "created_at": row[30].isoformat() if row[30] else None,
                "updated_at": row[31].isoformat() if row[31] else None,
            },
            "participants": participants,
            "advisors": advisors,
            "deal_team": deal_team,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching deal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/overview")
async def get_deal_stats(year: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Get deal statistics.
    """
    try:
        year_filter = ""
        params = {}
        if year:
            year_filter = "WHERE EXTRACT(YEAR FROM closed_date) = :year"
            params["year"] = year

        stats_query = text(f"""
            SELECT
                COUNT(*) as total_deals,
                COUNT(CASE WHEN deal_type = 'LBO' THEN 1 END) as lbo_count,
                COUNT(CASE WHEN deal_type = 'Growth' THEN 1 END) as growth_count,
                COUNT(CASE WHEN deal_type = 'Add-on' THEN 1 END) as addon_count,
                COUNT(CASE WHEN deal_type = 'Exit' THEN 1 END) as exit_count,
                SUM(enterprise_value_usd) as total_ev,
                AVG(enterprise_value_usd) as avg_ev,
                AVG(ev_ebitda_multiple) as avg_ebitda_multiple,
                AVG(ev_revenue_multiple) as avg_revenue_multiple
            FROM pe_deals
            {year_filter}
        """)

        result = db.execute(stats_query, params)
        row = result.fetchone()

        return {
            "filter": {"year": year} if year else None,
            "total_deals": row[0],
            "by_type": {
                "lbo": row[1],
                "growth": row[2],
                "add_on": row[3],
                "exit": row[4],
            },
            "valuations": {
                "total_ev_usd": float(row[5]) if row[5] else 0,
                "average_ev_usd": float(row[6]) if row[6] else 0,
            },
            "multiples": {
                "avg_ev_ebitda": float(row[7]) if row[7] else None,
                "avg_ev_revenue": float(row[8]) if row[8] else None,
            },
        }

    except Exception as e:
        logger.error(f"Error fetching deal stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activity/recent")
async def get_recent_deals(
    days: int = Query(30, le=365),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """
    Get recent deal activity.
    """
    try:
        query = text("""
            SELECT
                d.id, d.deal_name, d.deal_type, c.name as company_name,
                d.enterprise_value_usd, d.closed_date, d.announced_date,
                d.buyer_name, d.status
            FROM pe_deals d
            JOIN pe_portfolio_companies c ON d.company_id = c.id
            WHERE (d.closed_date >= CURRENT_DATE - :days::integer
                   OR d.announced_date >= CURRENT_DATE - :days::integer)
            ORDER BY COALESCE(d.closed_date, d.announced_date) DESC
            LIMIT :limit
        """)

        result = db.execute(query, {"days": days, "limit": limit})
        rows = result.fetchall()

        deals = []
        for row in rows:
            deals.append(
                {
                    "id": row[0],
                    "deal_name": row[1],
                    "deal_type": row[2],
                    "company_name": row[3],
                    "enterprise_value_usd": float(row[4]) if row[4] else None,
                    "closed_date": row[5].isoformat() if row[5] else None,
                    "announced_date": row[6].isoformat() if row[6] else None,
                    "buyer": row[7],
                    "status": row[8],
                }
            )

        return {"period_days": days, "count": len(deals), "deals": deals}

    except Exception as e:
        logger.error(f"Error fetching recent deals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
