"""
PE Portfolio Companies API endpoints.

Endpoints for managing portfolio company data including:
- Company profiles
- Financials and valuations
- Leadership teams
- Competitors
- News
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/pe/companies",
    tags=["PE Intelligence - Portfolio Companies"]
)


# =============================================================================
# Request/Response Models
# =============================================================================

class PortfolioCompanyCreate(BaseModel):
    """Request model for creating a portfolio company."""
    name: str = Field(..., examples=["ServiceTitan"])
    legal_name: Optional[str] = Field(None)
    website: Optional[str] = Field(None, examples=["https://www.servicetitan.com"])
    description: Optional[str] = Field(None)

    headquarters_city: Optional[str] = Field(None, examples=["Glendale"])
    headquarters_state: Optional[str] = Field(None, examples=["CA"])
    headquarters_country: Optional[str] = Field("USA")

    industry: Optional[str] = Field(None, examples=["Software"])
    sub_industry: Optional[str] = Field(None, examples=["Field Service Management"])
    sector: Optional[str] = Field(None)

    founded_year: Optional[int] = Field(None, examples=[2012])
    employee_count: Optional[int] = Field(None, examples=[2000])

    ownership_status: Optional[str] = Field(None, examples=["PE-Backed"])
    current_pe_owner: Optional[str] = Field(None, examples=["Thoma Bravo"])

    linkedin_url: Optional[str] = Field(None)
    status: Optional[str] = Field("Active")


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/")
async def list_portfolio_companies(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    industry: Optional[str] = None,
    ownership_status: Optional[str] = None,
    pe_owner: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    List portfolio companies with filtering.

    **Query Parameters:**
    - `industry`: Filter by industry
    - `ownership_status`: Filter by status (PE-Backed, VC-Backed, etc.)
    - `pe_owner`: Filter by PE firm name
    - `search`: Search by company name
    """
    try:
        query = """
            SELECT
                id, name, website, description,
                headquarters_city, headquarters_state, headquarters_country,
                industry, sub_industry, sector,
                founded_year, employee_count,
                ownership_status, current_pe_owner,
                status, created_at
            FROM pe_portfolio_companies
            WHERE 1=1
        """
        params = {"limit": limit, "offset": offset}

        if industry:
            query += " AND industry ILIKE :industry"
            params["industry"] = f"%{industry}%"

        if ownership_status:
            query += " AND ownership_status = :ownership_status"
            params["ownership_status"] = ownership_status

        if pe_owner:
            query += " AND current_pe_owner ILIKE :pe_owner"
            params["pe_owner"] = f"%{pe_owner}%"

        if search:
            query += " AND name ILIKE :search"
            params["search"] = f"%{search}%"

        query += " ORDER BY name LIMIT :limit OFFSET :offset"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        companies = []
        for row in rows:
            companies.append({
                "id": row[0],
                "name": row[1],
                "website": row[2],
                "description": row[3][:200] if row[3] else None,
                "location": {
                    "city": row[4],
                    "state": row[5],
                    "country": row[6]
                },
                "industry": row[7],
                "sub_industry": row[8],
                "sector": row[9],
                "founded_year": row[10],
                "employee_count": row[11],
                "ownership_status": row[12],
                "current_pe_owner": row[13],
                "status": row[14],
                "created_at": row[15].isoformat() if row[15] else None
            })

        return {
            "count": len(companies),
            "limit": limit,
            "offset": offset,
            "companies": companies
        }

    except Exception as e:
        logger.error(f"Error listing companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_companies(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db)
):
    """
    Search portfolio companies by name.
    """
    try:
        query = text("""
            SELECT id, name, industry, current_pe_owner, ownership_status,
                   headquarters_city, headquarters_state
            FROM pe_portfolio_companies
            WHERE name ILIKE :search
            ORDER BY
                CASE WHEN name ILIKE :exact THEN 0 ELSE 1 END,
                name
            LIMIT :limit
        """)

        result = db.execute(query, {
            "search": f"%{q}%",
            "exact": f"{q}%",
            "limit": limit
        })
        rows = result.fetchall()

        companies = []
        for row in rows:
            companies.append({
                "id": row[0],
                "name": row[1],
                "industry": row[2],
                "pe_owner": row[3],
                "ownership_status": row[4],
                "location": f"{row[5]}, {row[6]}" if row[5] else None
            })

        return {"count": len(companies), "results": companies}

    except Exception as e:
        logger.error(f"Error searching companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_portfolio_company(
    company: PortfolioCompanyCreate,
    db: Session = Depends(get_db)
):
    """
    Create or update a portfolio company.
    """
    try:
        insert_sql = text("""
            INSERT INTO pe_portfolio_companies (
                name, legal_name, website, description,
                headquarters_city, headquarters_state, headquarters_country,
                industry, sub_industry, sector,
                founded_year, employee_count,
                ownership_status, current_pe_owner,
                linkedin_url, status
            ) VALUES (
                :name, :legal_name, :website, :description,
                :headquarters_city, :headquarters_state, :headquarters_country,
                :industry, :sub_industry, :sector,
                :founded_year, :employee_count,
                :ownership_status, :current_pe_owner,
                :linkedin_url, :status
            )
            ON CONFLICT (name) DO UPDATE SET
                legal_name = EXCLUDED.legal_name,
                website = EXCLUDED.website,
                description = EXCLUDED.description,
                headquarters_city = EXCLUDED.headquarters_city,
                headquarters_state = EXCLUDED.headquarters_state,
                headquarters_country = EXCLUDED.headquarters_country,
                industry = EXCLUDED.industry,
                sub_industry = EXCLUDED.sub_industry,
                sector = EXCLUDED.sector,
                founded_year = EXCLUDED.founded_year,
                employee_count = EXCLUDED.employee_count,
                ownership_status = EXCLUDED.ownership_status,
                current_pe_owner = EXCLUDED.current_pe_owner,
                linkedin_url = EXCLUDED.linkedin_url,
                status = EXCLUDED.status,
                updated_at = NOW()
            RETURNING id, name, created_at
        """)

        result = db.execute(insert_sql, company.dict())
        row = result.fetchone()
        db.commit()

        return {
            "id": row[0],
            "name": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
            "message": "Company created/updated successfully"
        }

    except Exception as e:
        logger.error(f"Error creating company: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}")
async def get_portfolio_company(
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed information for a portfolio company.
    """
    try:
        query = text("""
            SELECT
                id, name, legal_name, website, description,
                headquarters_city, headquarters_state, headquarters_country,
                industry, sub_industry, naics_code, sic_code, sector,
                founded_year, employee_count, employee_count_range,
                ownership_status, current_pe_owner, is_platform_company,
                linkedin_url, crunchbase_url, ticker, ein,
                status, created_at, updated_at
            FROM pe_portfolio_companies
            WHERE id = :company_id
        """)

        result = db.execute(query, {"company_id": company_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        # Get investments
        investments_query = text("""
            SELECT
                fi.id, f.name as fund_name, pf.name as firm_name,
                fi.investment_date, fi.investment_type, fi.investment_round,
                fi.invested_amount_usd, fi.ownership_pct,
                fi.entry_ev_usd, fi.entry_ev_ebitda_multiple,
                fi.status, fi.exit_date, fi.exit_type, fi.exit_multiple
            FROM pe_fund_investments fi
            JOIN pe_funds f ON fi.fund_id = f.id
            JOIN pe_firms pf ON f.firm_id = pf.id
            WHERE fi.company_id = :company_id
            ORDER BY fi.investment_date DESC
        """)
        inv_result = db.execute(investments_query, {"company_id": company_id})
        investments = [
            {
                "id": i[0],
                "fund": i[1],
                "firm": i[2],
                "date": i[3].isoformat() if i[3] else None,
                "type": i[4],
                "round": i[5],
                "amount_usd": float(i[6]) if i[6] else None,
                "ownership_pct": float(i[7]) if i[7] else None,
                "entry_ev_usd": float(i[8]) if i[8] else None,
                "entry_multiple": float(i[9]) if i[9] else None,
                "status": i[10],
                "exit_date": i[11].isoformat() if i[11] else None,
                "exit_type": i[12],
                "exit_multiple": float(i[13]) if i[13] else None
            }
            for i in inv_result.fetchall()
        ]

        return {
            "id": row[0],
            "name": row[1],
            "legal_name": row[2],
            "website": row[3],
            "description": row[4],
            "headquarters": {
                "city": row[5],
                "state": row[6],
                "country": row[7]
            },
            "classification": {
                "industry": row[8],
                "sub_industry": row[9],
                "naics_code": row[10],
                "sic_code": row[11],
                "sector": row[12]
            },
            "company_info": {
                "founded_year": row[13],
                "employee_count": row[14],
                "employee_range": row[15]
            },
            "ownership": {
                "status": row[16],
                "current_pe_owner": row[17],
                "is_platform_company": row[18]
            },
            "external_links": {
                "linkedin": row[19],
                "crunchbase": row[20],
                "ticker": row[21],
                "ein": row[22]
            },
            "status": row[23],
            "metadata": {
                "created_at": row[24].isoformat() if row[24] else None,
                "updated_at": row[25].isoformat() if row[25] else None
            },
            "investments": investments
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching company: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/leadership")
async def get_company_leadership(
    company_id: int,
    current_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get leadership team for a portfolio company.
    """
    try:
        query = """
            SELECT
                cl.id, p.id as person_id, p.full_name, p.linkedin_url,
                cl.title, cl.role_category,
                cl.is_ceo, cl.is_cfo, cl.is_board_member, cl.is_board_chair,
                cl.start_date, cl.end_date, cl.is_current,
                cl.appointed_by_pe, cl.pe_firm_affiliation
            FROM pe_company_leadership cl
            JOIN pe_people p ON cl.person_id = p.id
            WHERE cl.company_id = :company_id
        """
        params = {"company_id": company_id}

        if current_only:
            query += " AND cl.is_current = true"

        query += """
            ORDER BY
                cl.is_ceo DESC, cl.is_cfo DESC, cl.is_board_chair DESC,
                CASE cl.role_category
                    WHEN 'C-Suite' THEN 1
                    WHEN 'VP' THEN 2
                    WHEN 'Director' THEN 3
                    WHEN 'Board' THEN 4
                    ELSE 5
                END
        """

        result = db.execute(text(query), params)
        rows = result.fetchall()

        leadership = []
        for row in rows:
            leadership.append({
                "id": row[0],
                "person_id": row[1],
                "name": row[2],
                "linkedin": row[3],
                "title": row[4],
                "role_category": row[5],
                "flags": {
                    "is_ceo": row[6],
                    "is_cfo": row[7],
                    "is_board_member": row[8],
                    "is_board_chair": row[9]
                },
                "tenure": {
                    "start_date": row[10].isoformat() if row[10] else None,
                    "end_date": row[11].isoformat() if row[11] else None,
                    "is_current": row[12]
                },
                "pe_relationship": {
                    "appointed_by_pe": row[13],
                    "firm_affiliation": row[14]
                }
            })

        return {
            "company_id": company_id,
            "count": len(leadership),
            "leadership": leadership
        }

    except Exception as e:
        logger.error(f"Error fetching leadership: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/financials")
async def get_company_financials(
    company_id: int,
    limit: int = Query(10, le=50),
    db: Session = Depends(get_db)
):
    """
    Get financial history for a portfolio company.
    """
    try:
        query = text("""
            SELECT
                id, fiscal_year, fiscal_period, period_end_date,
                revenue_usd, revenue_growth_pct,
                gross_profit_usd, gross_margin_pct,
                ebitda_usd, ebitda_margin_pct,
                ebit_usd, net_income_usd,
                total_assets_usd, total_debt_usd, cash_usd, net_debt_usd,
                operating_cash_flow_usd, capex_usd, free_cash_flow_usd,
                debt_to_ebitda, interest_coverage,
                is_audited, is_estimated, data_source, confidence
            FROM pe_company_financials
            WHERE company_id = :company_id
            ORDER BY fiscal_year DESC, fiscal_period DESC
            LIMIT :limit
        """)

        result = db.execute(query, {"company_id": company_id, "limit": limit})
        rows = result.fetchall()

        financials = []
        for row in rows:
            financials.append({
                "id": row[0],
                "period": {
                    "fiscal_year": row[1],
                    "fiscal_period": row[2],
                    "end_date": row[3].isoformat() if row[3] else None
                },
                "income_statement": {
                    "revenue_usd": float(row[4]) if row[4] else None,
                    "revenue_growth_pct": float(row[5]) if row[5] else None,
                    "gross_profit_usd": float(row[6]) if row[6] else None,
                    "gross_margin_pct": float(row[7]) if row[7] else None,
                    "ebitda_usd": float(row[8]) if row[8] else None,
                    "ebitda_margin_pct": float(row[9]) if row[9] else None,
                    "ebit_usd": float(row[10]) if row[10] else None,
                    "net_income_usd": float(row[11]) if row[11] else None
                },
                "balance_sheet": {
                    "total_assets_usd": float(row[12]) if row[12] else None,
                    "total_debt_usd": float(row[13]) if row[13] else None,
                    "cash_usd": float(row[14]) if row[14] else None,
                    "net_debt_usd": float(row[15]) if row[15] else None
                },
                "cash_flow": {
                    "operating_cash_flow_usd": float(row[16]) if row[16] else None,
                    "capex_usd": float(row[17]) if row[17] else None,
                    "free_cash_flow_usd": float(row[18]) if row[18] else None
                },
                "ratios": {
                    "debt_to_ebitda": float(row[19]) if row[19] else None,
                    "interest_coverage": float(row[20]) if row[20] else None
                },
                "data_quality": {
                    "is_audited": row[21],
                    "is_estimated": row[22],
                    "source": row[23],
                    "confidence": row[24]
                }
            })

        return {
            "company_id": company_id,
            "count": len(financials),
            "financials": financials
        }

    except Exception as e:
        logger.error(f"Error fetching financials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/valuations")
async def get_company_valuations(
    company_id: int,
    limit: int = Query(10, le=50),
    db: Session = Depends(get_db)
):
    """
    Get valuation history for a portfolio company.
    """
    try:
        query = text("""
            SELECT
                id, valuation_date,
                enterprise_value_usd, equity_value_usd, net_debt_usd,
                ev_revenue_multiple, ev_ebitda_multiple, ev_ebit_multiple,
                price_earnings_multiple,
                valuation_type, methodology, event_type,
                data_source, source_url, confidence
            FROM pe_company_valuations
            WHERE company_id = :company_id
            ORDER BY valuation_date DESC
            LIMIT :limit
        """)

        result = db.execute(query, {"company_id": company_id, "limit": limit})
        rows = result.fetchall()

        valuations = []
        for row in rows:
            valuations.append({
                "id": row[0],
                "date": row[1].isoformat() if row[1] else None,
                "values": {
                    "enterprise_value_usd": float(row[2]) if row[2] else None,
                    "equity_value_usd": float(row[3]) if row[3] else None,
                    "net_debt_usd": float(row[4]) if row[4] else None
                },
                "multiples": {
                    "ev_revenue": float(row[5]) if row[5] else None,
                    "ev_ebitda": float(row[6]) if row[6] else None,
                    "ev_ebit": float(row[7]) if row[7] else None,
                    "pe": float(row[8]) if row[8] else None
                },
                "context": {
                    "type": row[9],
                    "methodology": row[10],
                    "event": row[11]
                },
                "source": {
                    "name": row[12],
                    "url": row[13],
                    "confidence": row[14]
                }
            })

        return {
            "company_id": company_id,
            "count": len(valuations),
            "valuations": valuations
        }

    except Exception as e:
        logger.error(f"Error fetching valuations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/competitors")
async def get_company_competitors(
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Get competitors for a portfolio company.
    """
    try:
        query = text("""
            SELECT
                id, competitor_name, competitor_company_id,
                is_public, ticker, is_pe_backed, pe_owner,
                competitor_type, relative_size, market_position,
                notes
            FROM pe_competitor_mappings
            WHERE company_id = :company_id
            ORDER BY
                CASE competitor_type
                    WHEN 'Direct' THEN 1
                    WHEN 'Indirect' THEN 2
                    ELSE 3
                END
        """)

        result = db.execute(query, {"company_id": company_id})
        rows = result.fetchall()

        competitors = []
        for row in rows:
            competitors.append({
                "id": row[0],
                "name": row[1],
                "linked_company_id": row[2],
                "public_info": {
                    "is_public": row[3],
                    "ticker": row[4]
                },
                "pe_info": {
                    "is_pe_backed": row[5],
                    "pe_owner": row[6]
                },
                "competitive_position": {
                    "type": row[7],
                    "relative_size": row[8],
                    "market_position": row[9]
                },
                "notes": row[10]
            })

        return {
            "company_id": company_id,
            "count": len(competitors),
            "competitors": competitors
        }

    except Exception as e:
        logger.error(f"Error fetching competitors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/news")
async def get_company_news(
    company_id: int,
    limit: int = Query(20, le=100),
    news_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get news for a portfolio company.
    """
    try:
        query = """
            SELECT
                id, title, source_name, source_url, author,
                summary, published_date,
                news_type, sentiment, sentiment_score,
                relevance_score, is_primary
            FROM pe_company_news
            WHERE company_id = :company_id
        """
        params = {"company_id": company_id, "limit": limit}

        if news_type:
            query += " AND news_type = :news_type"
            params["news_type"] = news_type

        query += " ORDER BY published_date DESC LIMIT :limit"

        result = db.execute(text(query), params)
        rows = result.fetchall()

        news = []
        for row in rows:
            news.append({
                "id": row[0],
                "title": row[1],
                "source": {
                    "name": row[2],
                    "url": row[3],
                    "author": row[4]
                },
                "summary": row[5],
                "published_date": row[6].isoformat() if row[6] else None,
                "classification": {
                    "type": row[7],
                    "sentiment": row[8],
                    "sentiment_score": float(row[9]) if row[9] else None
                },
                "relevance": {
                    "score": float(row[10]) if row[10] else None,
                    "is_primary": row[11]
                }
            })

        return {
            "company_id": company_id,
            "count": len(news),
            "news": news
        }

    except Exception as e:
        logger.error(f"Error fetching news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
