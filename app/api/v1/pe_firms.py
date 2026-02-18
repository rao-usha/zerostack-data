"""
PE Firms API endpoints.

Endpoints for managing PE/VC firm data including:
- Firm profiles and metadata
- Fund information
- Portfolio companies
- Team members
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pe/firms", tags=["PE Intelligence - Firms"])


# =============================================================================
# Request/Response Models
# =============================================================================


class PEFirmCreate(BaseModel):
    """Request model for creating/updating a PE firm."""

    name: str = Field(..., description="Firm name", examples=["Blackstone"])
    legal_name: Optional[str] = Field(None, examples=["Blackstone Inc."])
    website: Optional[str] = Field(None, examples=["https://www.blackstone.com"])

    headquarters_city: Optional[str] = Field(None, examples=["New York"])
    headquarters_state: Optional[str] = Field(None, examples=["NY"])
    headquarters_country: Optional[str] = Field("USA", examples=["USA"])

    firm_type: Optional[str] = Field(None, examples=["PE"])
    primary_strategy: Optional[str] = Field(None, examples=["Buyout"])
    sector_focus: Optional[List[str]] = Field(
        None, examples=[["Technology", "Healthcare"]]
    )
    geography_focus: Optional[List[str]] = Field(
        None, examples=[["North America", "Europe"]]
    )

    aum_usd_millions: Optional[float] = Field(None, examples=[1000000])
    employee_count: Optional[int] = Field(None, examples=[5000])

    typical_check_size_min: Optional[float] = Field(None, examples=[100])
    typical_check_size_max: Optional[float] = Field(None, examples=[5000])

    cik: Optional[str] = Field(None, examples=["1393818"])
    sec_file_number: Optional[str] = Field(None, examples=["801-12345"])
    crd_number: Optional[str] = Field(None, examples=["12345"])
    is_sec_registered: Optional[bool] = Field(False)

    founded_year: Optional[int] = Field(None, examples=[1985])
    status: Optional[str] = Field("Active", examples=["Active"])

    linkedin_url: Optional[str] = Field(None)
    crunchbase_url: Optional[str] = Field(None)


class PEFirmResponse(BaseModel):
    """Response model for PE firm."""

    id: int
    name: str
    legal_name: Optional[str] = None
    website: Optional[str] = None
    headquarters_city: Optional[str] = None
    headquarters_state: Optional[str] = None
    headquarters_country: Optional[str] = None
    firm_type: Optional[str] = None
    primary_strategy: Optional[str] = None
    aum_usd_millions: Optional[float] = None
    founded_year: Optional[int] = None
    status: Optional[str] = None
    created_at: Optional[str] = None


class PEFundCreate(BaseModel):
    """Request model for creating a fund."""

    name: str = Field(..., examples=["Blackstone Capital Partners IX"])
    fund_number: Optional[int] = Field(None, examples=[9])
    vintage_year: Optional[int] = Field(None, examples=[2023])
    target_size_usd_millions: Optional[float] = Field(None, examples=[25000])
    final_close_usd_millions: Optional[float] = Field(None, examples=[24000])
    strategy: Optional[str] = Field(None, examples=["Buyout"])
    status: Optional[str] = Field("Active", examples=["Active"])


# =============================================================================
# Firm Endpoints
# =============================================================================


@router.get("/")
async def list_pe_firms(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    firm_type: Optional[str] = None,
    strategy: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    List all PE/VC firms with filtering and pagination.

    **Query Parameters:**
    - `limit`: Max results (default: 100, max: 1000)
    - `offset`: Pagination offset
    - `firm_type`: Filter by type (PE, VC, Growth, Credit, etc.)
    - `strategy`: Filter by strategy (Buyout, Venture, etc.)
    - `status`: Filter by status (Active, Inactive)
    - `search`: Search by firm name
    """
    try:
        query = """
            SELECT
                id, name, legal_name, website,
                headquarters_city, headquarters_state, headquarters_country,
                firm_type, primary_strategy, aum_usd_millions,
                employee_count, founded_year, status, cik,
                created_at
            FROM pe_firms
            WHERE 1=1
        """
        params = {"limit": limit, "offset": offset}

        if firm_type:
            query += " AND firm_type = :firm_type"
            params["firm_type"] = firm_type

        if strategy:
            query += " AND primary_strategy ILIKE :strategy"
            params["strategy"] = f"%{strategy}%"

        if status:
            query += " AND status = :status"
            params["status"] = status

        if search:
            query += " AND name ILIKE :search"
            params["search"] = f"%{search}%"

        query += " ORDER BY COALESCE(aum_usd_millions, 0) DESC, name LIMIT :limit OFFSET :offset"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        firms = []
        for row in rows:
            firms.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "legal_name": row[2],
                    "website": row[3],
                    "location": {"city": row[4], "state": row[5], "country": row[6]},
                    "firm_type": row[7],
                    "primary_strategy": row[8],
                    "aum_usd_millions": float(row[9]) if row[9] else None,
                    "employee_count": row[10],
                    "founded_year": row[11],
                    "status": row[12],
                    "cik": row[13],
                    "created_at": row[14].isoformat() if row[14] else None,
                }
            )

        # Get total count
        count_query = "SELECT COUNT(*) FROM pe_firms WHERE 1=1"
        count_params = {}
        if firm_type:
            count_query += " AND firm_type = :firm_type"
            count_params["firm_type"] = firm_type
        if status:
            count_query += " AND status = :status"
            count_params["status"] = status
        if search:
            count_query += " AND name ILIKE :search"
            count_params["search"] = f"%{search}%"

        count_result = db.execute(text(count_query), count_params)
        total = count_result.scalar()

        return {
            "total": total,
            "count": len(firms),
            "limit": limit,
            "offset": offset,
            "firms": firms,
        }

    except Exception as e:
        logger.error(f"Error listing PE firms: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_pe_firms(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """
    Search PE firms by name.

    **Example:**
    ```
    GET /api/v1/pe/firms/search?q=black
    ```
    """
    try:
        query = text("""
            SELECT id, name, firm_type, primary_strategy, aum_usd_millions,
                   headquarters_city, headquarters_state
            FROM pe_firms
            WHERE name ILIKE :search
            ORDER BY
                CASE WHEN name ILIKE :exact THEN 0 ELSE 1 END,
                COALESCE(aum_usd_millions, 0) DESC
            LIMIT :limit
        """)

        result = db.execute(
            query, {"search": f"%{q}%", "exact": f"{q}%", "limit": limit}
        )
        rows = result.fetchall()

        firms = []
        for row in rows:
            firms.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "firm_type": row[2],
                    "strategy": row[3],
                    "aum_usd_millions": float(row[4]) if row[4] else None,
                    "location": f"{row[5]}, {row[6]}" if row[5] else None,
                }
            )

        return {"count": len(firms), "results": firms}

    except Exception as e:
        logger.error(f"Error searching PE firms: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=PEFirmResponse)
async def create_pe_firm(firm: PEFirmCreate, db: Session = Depends(get_db)):
    """
    Create or update a PE firm.

    If a firm with the same name exists, it will be updated.
    """
    try:
        insert_sql = text("""
            INSERT INTO pe_firms (
                name, legal_name, website,
                headquarters_city, headquarters_state, headquarters_country,
                firm_type, primary_strategy,
                aum_usd_millions, employee_count,
                typical_check_size_min, typical_check_size_max,
                cik, sec_file_number, crd_number, is_sec_registered,
                founded_year, status, linkedin_url, crunchbase_url
            ) VALUES (
                :name, :legal_name, :website,
                :headquarters_city, :headquarters_state, :headquarters_country,
                :firm_type, :primary_strategy,
                :aum_usd_millions, :employee_count,
                :typical_check_size_min, :typical_check_size_max,
                :cik, :sec_file_number, :crd_number, :is_sec_registered,
                :founded_year, :status, :linkedin_url, :crunchbase_url
            )
            ON CONFLICT (name) DO UPDATE SET
                legal_name = EXCLUDED.legal_name,
                website = EXCLUDED.website,
                headquarters_city = EXCLUDED.headquarters_city,
                headquarters_state = EXCLUDED.headquarters_state,
                headquarters_country = EXCLUDED.headquarters_country,
                firm_type = EXCLUDED.firm_type,
                primary_strategy = EXCLUDED.primary_strategy,
                aum_usd_millions = EXCLUDED.aum_usd_millions,
                employee_count = EXCLUDED.employee_count,
                typical_check_size_min = EXCLUDED.typical_check_size_min,
                typical_check_size_max = EXCLUDED.typical_check_size_max,
                cik = EXCLUDED.cik,
                sec_file_number = EXCLUDED.sec_file_number,
                crd_number = EXCLUDED.crd_number,
                is_sec_registered = EXCLUDED.is_sec_registered,
                founded_year = EXCLUDED.founded_year,
                status = EXCLUDED.status,
                linkedin_url = EXCLUDED.linkedin_url,
                crunchbase_url = EXCLUDED.crunchbase_url,
                updated_at = NOW()
            RETURNING id, name, legal_name, website, headquarters_city,
                      headquarters_state, headquarters_country, firm_type,
                      primary_strategy, aum_usd_millions, founded_year, status,
                      created_at
        """)

        params = firm.dict(exclude={"sector_focus", "geography_focus"})
        result = db.execute(insert_sql, params)
        row = result.fetchone()

        db.commit()

        return PEFirmResponse(
            id=row[0],
            name=row[1],
            legal_name=row[2],
            website=row[3],
            headquarters_city=row[4],
            headquarters_state=row[5],
            headquarters_country=row[6],
            firm_type=row[7],
            primary_strategy=row[8],
            aum_usd_millions=float(row[9]) if row[9] else None,
            founded_year=row[10],
            status=row[11],
            created_at=row[12].isoformat() if row[12] else None,
        )

    except Exception as e:
        logger.error(f"Error creating PE firm: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{firm_id}")
async def get_pe_firm(firm_id: int, db: Session = Depends(get_db)):
    """
    Get detailed information for a PE firm.

    Returns complete profile including funds, team, and portfolio.
    """
    try:
        # Get firm details
        query = text("""
            SELECT
                id, name, legal_name, website,
                headquarters_city, headquarters_state, headquarters_country,
                firm_type, primary_strategy, sector_focus, geography_focus,
                aum_usd_millions, employee_count, office_locations,
                typical_check_size_min, typical_check_size_max,
                target_company_revenue_min, target_company_revenue_max,
                target_company_ebitda_min, target_company_ebitda_max,
                cik, sec_file_number, crd_number, is_sec_registered,
                founded_year, status, linkedin_url, crunchbase_url, pitchbook_url,
                data_sources, last_verified_date, confidence_score,
                created_at, updated_at
            FROM pe_firms
            WHERE id = :firm_id
        """)

        result = db.execute(query, {"firm_id": firm_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"PE firm {firm_id} not found")

        # Get funds
        funds_query = text("""
            SELECT id, name, fund_number, vintage_year,
                   target_size_usd_millions, final_close_usd_millions,
                   strategy, status
            FROM pe_funds
            WHERE firm_id = :firm_id
            ORDER BY vintage_year DESC
        """)
        funds_result = db.execute(funds_query, {"firm_id": firm_id})
        funds = [
            {
                "id": f[0],
                "name": f[1],
                "fund_number": f[2],
                "vintage_year": f[3],
                "target_size_millions": float(f[4]) if f[4] else None,
                "final_close_millions": float(f[5]) if f[5] else None,
                "strategy": f[6],
                "status": f[7],
            }
            for f in funds_result.fetchall()
        ]

        # Get team members
        team_query = text("""
            SELECT fp.id, p.full_name, fp.title, fp.seniority, fp.is_current
            FROM pe_firm_people fp
            JOIN pe_people p ON fp.person_id = p.id
            WHERE fp.firm_id = :firm_id AND fp.is_current = true
            ORDER BY
                CASE fp.seniority
                    WHEN 'Partner' THEN 1
                    WHEN 'Managing Director' THEN 2
                    WHEN 'Principal' THEN 3
                    WHEN 'VP' THEN 4
                    ELSE 5
                END
            LIMIT 20
        """)
        team_result = db.execute(team_query, {"firm_id": firm_id})
        team = [
            {
                "id": t[0],
                "name": t[1],
                "title": t[2],
                "seniority": t[3],
                "is_current": t[4],
            }
            for t in team_result.fetchall()
        ]

        return {
            "id": row[0],
            "name": row[1],
            "legal_name": row[2],
            "website": row[3],
            "headquarters": {"city": row[4], "state": row[5], "country": row[6]},
            "classification": {
                "type": row[7],
                "strategy": row[8],
                "sector_focus": row[9],
                "geography_focus": row[10],
            },
            "scale": {
                "aum_usd_millions": float(row[11]) if row[11] else None,
                "employee_count": row[12],
                "office_locations": row[13],
            },
            "investment_criteria": {
                "check_size_min_millions": float(row[14]) if row[14] else None,
                "check_size_max_millions": float(row[15]) if row[15] else None,
                "target_revenue_min_millions": float(row[16]) if row[16] else None,
                "target_revenue_max_millions": float(row[17]) if row[17] else None,
                "target_ebitda_min_millions": float(row[18]) if row[18] else None,
                "target_ebitda_max_millions": float(row[19]) if row[19] else None,
            },
            "sec_registration": {
                "cik": row[20],
                "file_number": row[21],
                "crd_number": row[22],
                "is_registered": row[23],
            },
            "founded_year": row[24],
            "status": row[25],
            "social": {
                "linkedin": row[26],
                "crunchbase": row[27],
                "pitchbook": row[28],
            },
            "data_quality": {
                "sources": row[29],
                "last_verified": row[30].isoformat() if row[30] else None,
                "confidence": float(row[31]) if row[31] else None,
            },
            "metadata": {
                "created_at": row[32].isoformat() if row[32] else None,
                "updated_at": row[33].isoformat() if row[33] else None,
            },
            "funds": funds,
            "team": team,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching PE firm: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{firm_id}/portfolio")
async def get_firm_portfolio(
    firm_id: int,
    status: Optional[str] = None,
    limit: int = Query(50, le=500),
    db: Session = Depends(get_db),
):
    """
    Get portfolio companies for a PE firm.

    **Query Parameters:**
    - `status`: Filter by investment status (Active, Exited)
    - `limit`: Max results (default: 50)
    """
    try:
        query = """
            SELECT
                c.id, c.name, c.industry, c.headquarters_city, c.headquarters_state,
                fi.investment_date, fi.investment_type, fi.ownership_pct,
                fi.status, fi.exit_date, fi.exit_type, fi.exit_multiple,
                f.name as fund_name, f.vintage_year
            FROM pe_portfolio_companies c
            JOIN pe_fund_investments fi ON c.id = fi.company_id
            JOIN pe_funds f ON fi.fund_id = f.id
            WHERE f.firm_id = :firm_id
        """
        params = {"firm_id": firm_id, "limit": limit}

        if status:
            query += " AND fi.status = :status"
            params["status"] = status

        query += " ORDER BY fi.investment_date DESC NULLS LAST LIMIT :limit"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        companies = []
        for row in rows:
            companies.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "industry": row[2],
                    "location": f"{row[3]}, {row[4]}" if row[3] else None,
                    "investment": {
                        "date": row[5].isoformat() if row[5] else None,
                        "type": row[6],
                        "ownership_pct": float(row[7]) if row[7] else None,
                        "status": row[8],
                    },
                    "exit": {
                        "date": row[9].isoformat() if row[9] else None,
                        "type": row[10],
                        "multiple": float(row[11]) if row[11] else None,
                    }
                    if row[9]
                    else None,
                    "fund": {"name": row[12], "vintage_year": row[13]},
                }
            )

        return {"firm_id": firm_id, "count": len(companies), "portfolio": companies}

    except Exception as e:
        logger.error(f"Error fetching portfolio: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{firm_id}/funds")
async def get_firm_funds(firm_id: int, db: Session = Depends(get_db)):
    """
    Get all funds for a PE firm.
    """
    try:
        query = text("""
            SELECT
                id, name, fund_number, vintage_year,
                target_size_usd_millions, final_close_usd_millions,
                called_capital_pct, strategy, sector_focus, geography_focus,
                management_fee_pct, carried_interest_pct, preferred_return_pct,
                fund_life_years, investment_period_years,
                status, first_close_date, final_close_date,
                created_at
            FROM pe_funds
            WHERE firm_id = :firm_id
            ORDER BY vintage_year DESC
        """)

        result = db.execute(query, {"firm_id": firm_id})
        rows = result.fetchall()

        funds = []
        for row in rows:
            funds.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "fund_number": row[2],
                    "vintage_year": row[3],
                    "size": {
                        "target_millions": float(row[4]) if row[4] else None,
                        "final_close_millions": float(row[5]) if row[5] else None,
                        "called_pct": float(row[6]) if row[6] else None,
                    },
                    "strategy": row[7],
                    "sector_focus": row[8],
                    "geography_focus": row[9],
                    "terms": {
                        "management_fee_pct": float(row[10]) if row[10] else None,
                        "carried_interest_pct": float(row[11]) if row[11] else None,
                        "preferred_return_pct": float(row[12]) if row[12] else None,
                        "fund_life_years": row[13],
                        "investment_period_years": row[14],
                    },
                    "status": row[15],
                    "first_close_date": row[16].isoformat() if row[16] else None,
                    "final_close_date": row[17].isoformat() if row[17] else None,
                    "created_at": row[18].isoformat() if row[18] else None,
                }
            )

        return {"firm_id": firm_id, "count": len(funds), "funds": funds}

    except Exception as e:
        logger.error(f"Error fetching funds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{firm_id}/team")
async def get_firm_team(
    firm_id: int, current_only: bool = True, db: Session = Depends(get_db)
):
    """
    Get team members for a PE firm.

    **Query Parameters:**
    - `current_only`: Only show current team members (default: true)
    """
    try:
        query = """
            SELECT
                fp.id, p.id as person_id, p.full_name, p.linkedin_url,
                fp.title, fp.seniority, fp.department,
                fp.sector_focus, fp.start_date, fp.end_date, fp.is_current,
                fp.work_email
            FROM pe_firm_people fp
            JOIN pe_people p ON fp.person_id = p.id
            WHERE fp.firm_id = :firm_id
        """
        params = {"firm_id": firm_id}

        if current_only:
            query += " AND fp.is_current = true"

        query += """
            ORDER BY
                CASE fp.seniority
                    WHEN 'Partner' THEN 1
                    WHEN 'Managing Director' THEN 2
                    WHEN 'Principal' THEN 3
                    WHEN 'VP' THEN 4
                    WHEN 'Associate' THEN 5
                    WHEN 'Analyst' THEN 6
                    ELSE 7
                END,
                p.full_name
        """

        result = db.execute(text(query), params)
        rows = result.fetchall()

        team = []
        for row in rows:
            team.append(
                {
                    "id": row[0],
                    "person_id": row[1],
                    "name": row[2],
                    "linkedin": row[3],
                    "title": row[4],
                    "seniority": row[5],
                    "department": row[6],
                    "sector_focus": row[7],
                    "tenure": {
                        "start_date": row[8].isoformat() if row[8] else None,
                        "end_date": row[9].isoformat() if row[9] else None,
                        "is_current": row[10],
                    },
                    "email": row[11],
                }
            )

        return {"firm_id": firm_id, "count": len(team), "team": team}

    except Exception as e:
        logger.error(f"Error fetching team: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/overview")
async def get_pe_firms_stats(db: Session = Depends(get_db)):
    """
    Get overview statistics for PE firms database.
    """
    try:
        stats_query = text("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN firm_type = 'PE' THEN 1 END) as pe_count,
                COUNT(CASE WHEN firm_type = 'VC' THEN 1 END) as vc_count,
                COUNT(CASE WHEN firm_type = 'Growth' THEN 1 END) as growth_count,
                COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_count,
                SUM(aum_usd_millions) as total_aum_millions,
                AVG(aum_usd_millions) as avg_aum_millions,
                COUNT(cik) as with_cik,
                COUNT(website) as with_website
            FROM pe_firms
        """)

        result = db.execute(stats_query)
        row = result.fetchone()

        # Get fund stats
        fund_stats_query = text("""
            SELECT
                COUNT(*) as total_funds,
                SUM(final_close_usd_millions) as total_fund_capital
            FROM pe_funds
        """)
        fund_result = db.execute(fund_stats_query)
        fund_row = fund_result.fetchone()

        return {
            "total_firms": row[0],
            "by_type": {"pe": row[1], "vc": row[2], "growth": row[3]},
            "active_firms": row[4],
            "aum": {
                "total_millions": float(row[5]) if row[5] else 0,
                "average_millions": float(row[6]) if row[6] else 0,
            },
            "data_completeness": {"with_cik": row[7], "with_website": row[8]},
            "funds": {
                "total": fund_row[0],
                "total_capital_millions": float(fund_row[1]) if fund_row[1] else 0,
            },
        }

    except Exception as e:
        logger.error(f"Error fetching stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{firm_id}")
async def delete_pe_firm(firm_id: int, db: Session = Depends(get_db)):
    """
    Delete a PE firm record.
    """
    try:
        sql = text("DELETE FROM pe_firms WHERE id = :firm_id RETURNING name")
        result = db.execute(sql, {"firm_id": firm_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"PE firm {firm_id} not found")

        db.commit()

        return {"message": f"PE firm '{row[0]}' deleted successfully", "id": firm_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting PE firm: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
