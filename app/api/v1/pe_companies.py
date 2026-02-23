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
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/pe/companies", tags=["PE Intelligence - Portfolio Companies"]
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
    db: Session = Depends(get_db),
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
            companies.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "website": row[2],
                    "description": row[3][:200] if row[3] else None,
                    "location": {"city": row[4], "state": row[5], "country": row[6]},
                    "industry": row[7],
                    "sub_industry": row[8],
                    "sector": row[9],
                    "founded_year": row[10],
                    "employee_count": row[11],
                    "ownership_status": row[12],
                    "current_pe_owner": row[13],
                    "status": row[14],
                    "created_at": row[15].isoformat() if row[15] else None,
                }
            )

        return {
            "count": len(companies),
            "limit": limit,
            "offset": offset,
            "companies": companies,
        }

    except Exception as e:
        logger.error(f"Error listing companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_companies(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
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

        result = db.execute(
            query, {"search": f"%{q}%", "exact": f"{q}%", "limit": limit}
        )
        rows = result.fetchall()

        companies = []
        for row in rows:
            companies.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "industry": row[2],
                    "pe_owner": row[3],
                    "ownership_status": row[4],
                    "location": f"{row[5]}, {row[6]}" if row[5] else None,
                }
            )

        return {"count": len(companies), "results": companies}

    except Exception as e:
        logger.error(f"Error searching companies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_portfolio_company(
    company: PortfolioCompanyCreate, db: Session = Depends(get_db)
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
            "message": "Company created/updated successfully",
        }

    except Exception as e:
        logger.error(f"Error creating company: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}")
async def get_portfolio_company(company_id: int, db: Session = Depends(get_db)):
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
            raise HTTPException(
                status_code=404, detail=f"Company {company_id} not found"
            )

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
                "exit_multiple": float(i[13]) if i[13] else None,
            }
            for i in inv_result.fetchall()
        ]

        return {
            "id": row[0],
            "name": row[1],
            "legal_name": row[2],
            "website": row[3],
            "description": row[4],
            "headquarters": {"city": row[5], "state": row[6], "country": row[7]},
            "classification": {
                "industry": row[8],
                "sub_industry": row[9],
                "naics_code": row[10],
                "sic_code": row[11],
                "sector": row[12],
            },
            "company_info": {
                "founded_year": row[13],
                "employee_count": row[14],
                "employee_range": row[15],
            },
            "ownership": {
                "status": row[16],
                "current_pe_owner": row[17],
                "is_platform_company": row[18],
            },
            "external_links": {
                "linkedin": row[19],
                "crunchbase": row[20],
                "ticker": row[21],
                "ein": row[22],
            },
            "status": row[23],
            "metadata": {
                "created_at": row[24].isoformat() if row[24] else None,
                "updated_at": row[25].isoformat() if row[25] else None,
            },
            "investments": investments,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching company: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/leadership")
async def get_company_leadership(
    company_id: int, current_only: bool = True, db: Session = Depends(get_db)
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
            leadership.append(
                {
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
                        "is_board_chair": row[9],
                    },
                    "tenure": {
                        "start_date": row[10].isoformat() if row[10] else None,
                        "end_date": row[11].isoformat() if row[11] else None,
                        "is_current": row[12],
                    },
                    "pe_relationship": {
                        "appointed_by_pe": row[13],
                        "firm_affiliation": row[14],
                    },
                }
            )

        return {
            "company_id": company_id,
            "count": len(leadership),
            "leadership": leadership,
        }

    except Exception as e:
        logger.error(f"Error fetching leadership: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/financials")
async def get_company_financials(
    company_id: int, limit: int = Query(10, le=50), db: Session = Depends(get_db)
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
            financials.append(
                {
                    "id": row[0],
                    "period": {
                        "fiscal_year": row[1],
                        "fiscal_period": row[2],
                        "end_date": row[3].isoformat() if row[3] else None,
                    },
                    "income_statement": {
                        "revenue_usd": float(row[4]) if row[4] else None,
                        "revenue_growth_pct": float(row[5]) if row[5] else None,
                        "gross_profit_usd": float(row[6]) if row[6] else None,
                        "gross_margin_pct": float(row[7]) if row[7] else None,
                        "ebitda_usd": float(row[8]) if row[8] else None,
                        "ebitda_margin_pct": float(row[9]) if row[9] else None,
                        "ebit_usd": float(row[10]) if row[10] else None,
                        "net_income_usd": float(row[11]) if row[11] else None,
                    },
                    "balance_sheet": {
                        "total_assets_usd": float(row[12]) if row[12] else None,
                        "total_debt_usd": float(row[13]) if row[13] else None,
                        "cash_usd": float(row[14]) if row[14] else None,
                        "net_debt_usd": float(row[15]) if row[15] else None,
                    },
                    "cash_flow": {
                        "operating_cash_flow_usd": float(row[16]) if row[16] else None,
                        "capex_usd": float(row[17]) if row[17] else None,
                        "free_cash_flow_usd": float(row[18]) if row[18] else None,
                    },
                    "ratios": {
                        "debt_to_ebitda": float(row[19]) if row[19] else None,
                        "interest_coverage": float(row[20]) if row[20] else None,
                    },
                    "data_quality": {
                        "is_audited": row[21],
                        "is_estimated": row[22],
                        "source": row[23],
                        "confidence": row[24],
                    },
                }
            )

        return {
            "company_id": company_id,
            "count": len(financials),
            "financials": financials,
        }

    except Exception as e:
        logger.error(f"Error fetching financials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/valuations")
async def get_company_valuations(
    company_id: int, limit: int = Query(10, le=50), db: Session = Depends(get_db)
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
            valuations.append(
                {
                    "id": row[0],
                    "date": row[1].isoformat() if row[1] else None,
                    "values": {
                        "enterprise_value_usd": float(row[2]) if row[2] else None,
                        "equity_value_usd": float(row[3]) if row[3] else None,
                        "net_debt_usd": float(row[4]) if row[4] else None,
                    },
                    "multiples": {
                        "ev_revenue": float(row[5]) if row[5] else None,
                        "ev_ebitda": float(row[6]) if row[6] else None,
                        "ev_ebit": float(row[7]) if row[7] else None,
                        "pe": float(row[8]) if row[8] else None,
                    },
                    "context": {
                        "type": row[9],
                        "methodology": row[10],
                        "event": row[11],
                    },
                    "source": {"name": row[12], "url": row[13], "confidence": row[14]},
                }
            )

        return {
            "company_id": company_id,
            "count": len(valuations),
            "valuations": valuations,
        }

    except Exception as e:
        logger.error(f"Error fetching valuations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/competitors")
async def get_company_competitors(company_id: int, db: Session = Depends(get_db)):
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
            competitors.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "linked_company_id": row[2],
                    "public_info": {"is_public": row[3], "ticker": row[4]},
                    "pe_info": {"is_pe_backed": row[5], "pe_owner": row[6]},
                    "competitive_position": {
                        "type": row[7],
                        "relative_size": row[8],
                        "market_position": row[9],
                    },
                    "notes": row[10],
                }
            )

        return {
            "company_id": company_id,
            "count": len(competitors),
            "competitors": competitors,
        }

    except Exception as e:
        logger.error(f"Error fetching competitors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{company_id}/news")
async def get_company_news(
    company_id: int,
    limit: int = Query(20, le=100),
    news_type: Optional[str] = None,
    db: Session = Depends(get_db),
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
            news.append(
                {
                    "id": row[0],
                    "title": row[1],
                    "source": {"name": row[2], "url": row[3], "author": row[4]},
                    "summary": row[5],
                    "published_date": row[6].isoformat() if row[6] else None,
                    "classification": {
                        "type": row[7],
                        "sentiment": row[8],
                        "sentiment_score": float(row[9]) if row[9] else None,
                    },
                    "relevance": {
                        "score": float(row[10]) if row[10] else None,
                        "is_primary": row[11],
                    },
                }
            )

        return {"company_id": company_id, "count": len(news), "news": news}

    except Exception as e:
        logger.error(f"Error fetching news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Financial Benchmarking
# =============================================================================


@router.get("/{company_id}/benchmark")
async def get_company_benchmark(
    company_id: int,
    year: Optional[int] = Query(None, description="Fiscal year to benchmark (default: latest)"),
    db: Session = Depends(get_db),
):
    """
    Benchmark a portfolio company's financials against peers.

    Compares revenue growth, margins, multiples, and leverage ratios vs.
    competitor median and percentiles (P25/P75).

    **Peer group construction:**
    1. Direct competitors from `pe_competitor_mappings`
    2. Same-industry companies with financial data

    **Returns:**
    - Company financials for the period
    - Peer group statistics (median, P25, P75)
    - Percentile rank within peer group for each metric
    - Relative performance assessment
    """
    try:
        # 1. Get company info
        company_row = db.execute(
            text("SELECT id, name, industry, sector FROM pe_portfolio_companies WHERE id = :id"),
            {"id": company_id},
        ).fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        company_name = company_row[1]
        company_industry = company_row[2]

        # 2. Get latest fiscal year if not specified
        if not year:
            yr_row = db.execute(
                text("SELECT MAX(fiscal_year) FROM pe_company_financials WHERE company_id = :id"),
                {"id": company_id},
            ).fetchone()
            year = yr_row[0] if yr_row and yr_row[0] else None
            if not year:
                return {
                    "company_id": company_id,
                    "company_name": company_name,
                    "message": "No financial data available for this company",
                    "benchmark": None,
                }

        # 3. Get company financials for the year
        fin_row = db.execute(
            text("""
                SELECT revenue_usd, revenue_growth_pct, gross_margin_pct,
                       ebitda_usd, ebitda_margin_pct, net_income_usd,
                       debt_to_ebitda, interest_coverage,
                       free_cash_flow_usd, total_debt_usd
                FROM pe_company_financials
                WHERE company_id = :id AND fiscal_year = :year AND fiscal_period = 'FY'
            """),
            {"id": company_id, "year": year},
        ).fetchone()

        if not fin_row:
            return {
                "company_id": company_id,
                "company_name": company_name,
                "message": f"No financial data for fiscal year {year}",
                "benchmark": None,
            }

        company_metrics = {
            "revenue_usd": float(fin_row[0]) if fin_row[0] else None,
            "revenue_growth_pct": float(fin_row[1]) if fin_row[1] else None,
            "gross_margin_pct": float(fin_row[2]) if fin_row[2] else None,
            "ebitda_usd": float(fin_row[3]) if fin_row[3] else None,
            "ebitda_margin_pct": float(fin_row[4]) if fin_row[4] else None,
            "net_income_usd": float(fin_row[5]) if fin_row[5] else None,
            "debt_to_ebitda": float(fin_row[6]) if fin_row[6] else None,
            "interest_coverage": float(fin_row[7]) if fin_row[7] else None,
            "free_cash_flow_usd": float(fin_row[8]) if fin_row[8] else None,
        }

        # 4. Get competitor company IDs
        comp_rows = db.execute(
            text("""
                SELECT competitor_name, competitor_company_id, competitor_type, relative_size
                FROM pe_competitor_mappings WHERE company_id = :id
            """),
            {"id": company_id},
        ).fetchall()

        peer_ids = [r[1] for r in comp_rows if r[1] is not None]
        peer_names = [r[0] for r in comp_rows]

        # 5. Also include same-industry and same-sector companies with financials
        company_sector = company_row[3]
        if company_industry or company_sector:
            industry_rows = db.execute(
                text("""
                    SELECT DISTINCT pc.id, pc.name
                    FROM pe_portfolio_companies pc
                    JOIN pe_company_financials f ON pc.id = f.company_id
                    WHERE (pc.industry = :industry OR pc.sector = :sector)
                    AND pc.id != :id AND f.fiscal_year = :year
                    LIMIT 20
                """),
                {
                    "industry": company_industry or "",
                    "sector": company_sector or "",
                    "id": company_id,
                    "year": year,
                },
            ).fetchall()
            for r in industry_rows:
                if r[0] not in peer_ids:
                    peer_ids.append(r[0])
                    if r[1] not in peer_names:
                        peer_names.append(r[1])

        # 6. Get peer financials
        peer_metrics = []
        if peer_ids:
            peer_fin_rows = db.execute(
                text("""
                    SELECT pc.name, f.revenue_usd, f.revenue_growth_pct,
                           f.gross_margin_pct, f.ebitda_margin_pct,
                           f.debt_to_ebitda, f.interest_coverage,
                           f.free_cash_flow_usd
                    FROM pe_company_financials f
                    JOIN pe_portfolio_companies pc ON f.company_id = pc.id
                    WHERE f.company_id = ANY(:ids) AND f.fiscal_year = :year
                    AND f.fiscal_period = 'FY'
                """),
                {"ids": peer_ids, "year": year},
            ).fetchall()

            for pr in peer_fin_rows:
                peer_metrics.append({
                    "name": pr[0],
                    "revenue_usd": float(pr[1]) if pr[1] else None,
                    "revenue_growth_pct": float(pr[2]) if pr[2] else None,
                    "gross_margin_pct": float(pr[3]) if pr[3] else None,
                    "ebitda_margin_pct": float(pr[4]) if pr[4] else None,
                    "debt_to_ebitda": float(pr[5]) if pr[5] else None,
                    "interest_coverage": float(pr[6]) if pr[6] else None,
                    "free_cash_flow_usd": float(pr[7]) if pr[7] else None,
                })

        # 7. Compute percentile stats
        def percentile_stats(values):
            clean = sorted([v for v in values if v is not None])
            if not clean:
                return {"median": None, "p25": None, "p75": None, "min": None, "max": None, "count": 0}
            n = len(clean)
            return {
                "median": clean[n // 2],
                "p25": clean[max(0, n // 4)],
                "p75": clean[min(n - 1, 3 * n // 4)],
                "min": clean[0],
                "max": clean[-1],
                "count": n,
            }

        def percentile_rank(value, values):
            if value is None:
                return None
            clean = sorted([v for v in values if v is not None])
            if not clean:
                return None
            below = sum(1 for v in clean if v < value)
            return round(below / len(clean) * 100, 1)

        metric_keys = ["revenue_growth_pct", "gross_margin_pct", "ebitda_margin_pct",
                       "debt_to_ebitda", "interest_coverage"]

        peer_stats = {}
        company_percentiles = {}
        for key in metric_keys:
            values = [p[key] for p in peer_metrics if p.get(key) is not None]
            peer_stats[key] = percentile_stats(values)
            company_percentiles[key] = percentile_rank(company_metrics.get(key), values)

        # 8. Get valuation multiples comparison
        val_row = db.execute(
            text("""
                SELECT ev_revenue_multiple, ev_ebitda_multiple
                FROM pe_company_valuations
                WHERE company_id = :id
                ORDER BY valuation_date DESC LIMIT 1
            """),
            {"id": company_id},
        ).fetchone()

        company_multiples = {
            "ev_revenue": float(val_row[0]) if val_row and val_row[0] else None,
            "ev_ebitda": float(val_row[1]) if val_row and val_row[1] else None,
        }

        peer_val_multiples = []
        if peer_ids:
            peer_val_rows = db.execute(
                text("""
                    SELECT DISTINCT ON (company_id) company_id,
                           ev_revenue_multiple, ev_ebitda_multiple
                    FROM pe_company_valuations
                    WHERE company_id = ANY(:ids)
                    ORDER BY company_id, valuation_date DESC
                """),
                {"ids": peer_ids},
            ).fetchall()
            peer_val_multiples = [
                {"ev_revenue": float(r[1]) if r[1] else None,
                 "ev_ebitda": float(r[2]) if r[2] else None}
                for r in peer_val_rows
            ]

        multiples_stats = {
            "ev_revenue": percentile_stats([p["ev_revenue"] for p in peer_val_multiples]),
            "ev_ebitda": percentile_stats([p["ev_ebitda"] for p in peer_val_multiples]),
        }

        # 9. Build performance assessment
        assessment = []
        rev_growth = company_metrics.get("revenue_growth_pct")
        peer_med_growth = peer_stats.get("revenue_growth_pct", {}).get("median")
        if rev_growth is not None and peer_med_growth is not None:
            if rev_growth > peer_med_growth * 1.2:
                assessment.append("Revenue growth significantly outpaces peers")
            elif rev_growth > peer_med_growth:
                assessment.append("Revenue growth above peer median")
            else:
                assessment.append("Revenue growth below peer median")

        ebitda_m = company_metrics.get("ebitda_margin_pct")
        peer_med_ebitda = peer_stats.get("ebitda_margin_pct", {}).get("median")
        if ebitda_m is not None and peer_med_ebitda is not None:
            if ebitda_m > peer_med_ebitda * 1.2:
                assessment.append("EBITDA margins significantly above peers - strong profitability")
            elif ebitda_m > peer_med_ebitda:
                assessment.append("EBITDA margins above peer median")
            else:
                assessment.append("EBITDA margins below peer median - margin expansion opportunity")

        dte = company_metrics.get("debt_to_ebitda")
        if dte is not None:
            if dte < 3:
                assessment.append("Conservative leverage profile")
            elif dte < 5:
                assessment.append("Moderate leverage - typical for PE-backed company")
            else:
                assessment.append("Elevated leverage - monitor debt service capacity")

        return {
            "company_id": company_id,
            "company_name": company_name,
            "fiscal_year": year,
            "company_metrics": company_metrics,
            "peer_group": {
                "count": len(peer_metrics),
                "companies": peer_names,
                "source": "Competitors + same-industry companies",
            },
            "peer_statistics": peer_stats,
            "company_percentile_rank": company_percentiles,
            "valuation_multiples": {
                "company": company_multiples,
                "peer_stats": multiples_stats,
            },
            "assessment": assessment,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing benchmark: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Exit Readiness Score
# =============================================================================


@router.get("/{company_id}/exit-readiness")
async def get_exit_readiness(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    **DEPRECATED** — Use `GET /api/v1/exit-readiness/{company_id}` instead.
    The new endpoint adds a 7th signal (hiring/job posting), persists scores
    to DB, and provides rankings + batch compute.

    Calculate an exit readiness score for a portfolio company.

    Aggregates 6 signals into a 0-100 composite score:
    1. **Financial Health** (25%) - Revenue growth, profitability, cash flow
    2. **Financial Trajectory** (20%) - Margin expansion, growth consistency
    3. **Leadership Stability** (15%) - C-suite tenure, PE-appointed roles filled
    4. **Valuation Momentum** (15%) - Multiple expansion, mark-to-market trend
    5. **Market Position** (15%) - Competitive positioning, peer relative performance
    6. **Hold Period** (10%) - Time since PE entry vs. typical hold period

    **Returns:**
    - Composite exit readiness score (0-100)
    - Category breakdown with individual scores
    - Timing recommendation
    - Key strengths and risks for exit
    """
    try:
        # 1. Get company info
        company_row = db.execute(
            text("""
                SELECT id, name, industry, sector, current_pe_owner, founded_year, employee_count
                FROM pe_portfolio_companies WHERE id = :id
            """),
            {"id": company_id},
        ).fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        company_name = company_row[1]
        pe_owner = company_row[4]

        scores = {}
        details = {}
        strengths = []
        risks = []

        # =====================================================================
        # Signal 1: Financial Health (25%)
        # =====================================================================
        fin_rows = db.execute(
            text("""
                SELECT fiscal_year, revenue_usd, revenue_growth_pct,
                       ebitda_usd, ebitda_margin_pct, net_income_usd,
                       free_cash_flow_usd, debt_to_ebitda
                FROM pe_company_financials
                WHERE company_id = :id AND fiscal_period = 'FY'
                ORDER BY fiscal_year DESC LIMIT 5
            """),
            {"id": company_id},
        ).fetchall()

        if fin_rows:
            latest = fin_rows[0]
            revenue = float(latest[1]) if latest[1] else 0
            growth = float(latest[2]) if latest[2] else 0
            ebitda_margin = float(latest[4]) if latest[4] else 0
            fcf = float(latest[6]) if latest[6] else 0
            dte = float(latest[7]) if latest[7] else None

            # Revenue scale score (0-25)
            scale_score = min(25, revenue / 40_000_000)  # Full marks at $1B+
            # Growth score (0-25)
            growth_score = min(25, max(0, growth * 1.0))  # Full marks at 25%+
            # Profitability score (0-25)
            profit_score = min(25, max(0, ebitda_margin * 0.8))  # Full marks at ~30%+
            # Cash flow score (0-25)
            fcf_score = 20 if fcf > 0 else 5

            financial_health = min(100, scale_score + growth_score + profit_score + fcf_score)
            scores["financial_health"] = round(financial_health, 1)
            details["financial_health"] = {
                "latest_revenue_usd": revenue,
                "revenue_growth_pct": growth,
                "ebitda_margin_pct": ebitda_margin,
                "free_cash_flow_positive": fcf > 0,
                "debt_to_ebitda": dte,
            }

            if growth > 15:
                strengths.append(f"Strong revenue growth ({growth:.1f}%)")
            elif growth < 5:
                risks.append(f"Slowing growth ({growth:.1f}%) may reduce buyer interest")

            if ebitda_margin > 20:
                strengths.append(f"Attractive EBITDA margins ({ebitda_margin:.1f}%)")
            elif ebitda_margin < 10:
                risks.append(f"Below-market margins ({ebitda_margin:.1f}%)")

            if fcf > 0:
                strengths.append("Positive free cash flow")
            else:
                risks.append("Negative free cash flow")

            if dte is not None and dte > 5:
                risks.append(f"High leverage ({dte:.1f}x debt/EBITDA)")
        else:
            scores["financial_health"] = 30  # Default for no data
            details["financial_health"] = {"message": "No financial data available"}
            risks.append("No financial data available - limits buyer confidence")

        # =====================================================================
        # Signal 2: Financial Trajectory (20%)
        # =====================================================================
        if len(fin_rows) >= 3:
            margins = [float(r[4]) for r in fin_rows[:3] if r[4] is not None]
            growths = [float(r[2]) for r in fin_rows[:3] if r[2] is not None]

            trajectory = 50  # neutral baseline
            if len(margins) >= 2:
                margin_trend = margins[0] - margins[-1]  # latest - oldest (reversed by DESC)
                if margin_trend > 5:
                    trajectory += 25
                    strengths.append(f"Margin expansion of {margin_trend:.1f}pp over {len(margins)} years")
                elif margin_trend > 0:
                    trajectory += 10
                elif margin_trend < -5:
                    trajectory -= 20
                    risks.append("Margin compression trend")

            if len(growths) >= 2:
                # Consistent growth is better than volatile
                avg_growth = sum(growths) / len(growths)
                if avg_growth > 15 and all(g > 5 for g in growths):
                    trajectory += 25
                    strengths.append("Consistent high growth trajectory")
                elif avg_growth > 10:
                    trajectory += 15
                elif avg_growth < 0:
                    trajectory -= 15
                    risks.append("Revenue declining")

            scores["financial_trajectory"] = min(100, max(0, trajectory))
            details["financial_trajectory"] = {
                "margin_trend": margins,
                "growth_trend": growths,
                "years_of_data": len(fin_rows),
            }
        else:
            scores["financial_trajectory"] = 40
            details["financial_trajectory"] = {"message": "Insufficient historical data"}

        # =====================================================================
        # Signal 3: Leadership Stability (15%)
        # =====================================================================
        lead_rows = db.execute(
            text("""
                SELECT title, role_category, is_ceo, is_cfo, is_board_member,
                       start_date, is_current, appointed_by_pe
                FROM pe_company_leadership
                WHERE company_id = :id AND is_current = true
            """),
            {"id": company_id},
        ).fetchall()

        if lead_rows:
            has_ceo = any(r[2] for r in lead_rows)
            has_cfo = any(r[3] for r in lead_rows)
            c_suite_count = sum(1 for r in lead_rows if r[1] == "C-Suite")
            board_count = sum(1 for r in lead_rows if r[4])
            pe_appointed = sum(1 for r in lead_rows if r[7])

            leadership_score = 30  # base
            if has_ceo:
                leadership_score += 20
            else:
                risks.append("No CEO identified - leadership gap")
            if has_cfo:
                leadership_score += 15
                strengths.append("CFO in place (critical for exit process)")
            else:
                risks.append("No CFO identified - may delay exit process")
            if c_suite_count >= 3:
                leadership_score += 15
            if board_count >= 2:
                leadership_score += 10
            if pe_appointed >= 2:
                leadership_score += 10
                strengths.append(f"{pe_appointed} PE-appointed leaders (professionalized management)")

            scores["leadership_stability"] = min(100, leadership_score)
            details["leadership_stability"] = {
                "has_ceo": has_ceo,
                "has_cfo": has_cfo,
                "c_suite_count": c_suite_count,
                "board_members": board_count,
                "pe_appointed_count": pe_appointed,
                "total_current_leaders": len(lead_rows),
            }
        else:
            scores["leadership_stability"] = 25
            details["leadership_stability"] = {"message": "No leadership data available"}
            risks.append("No leadership data - limits buyer visibility")

        # =====================================================================
        # Signal 4: Valuation Momentum (15%)
        # =====================================================================
        val_rows = db.execute(
            text("""
                SELECT valuation_date, enterprise_value_usd,
                       ev_revenue_multiple, ev_ebitda_multiple, event_type
                FROM pe_company_valuations
                WHERE company_id = :id
                ORDER BY valuation_date DESC LIMIT 5
            """),
            {"id": company_id},
        ).fetchall()

        if len(val_rows) >= 2:
            latest_ev = float(val_rows[0][1]) if val_rows[0][1] else 0
            earliest_ev = float(val_rows[-1][1]) if val_rows[-1][1] else 0

            val_score = 50
            if earliest_ev > 0 and latest_ev > 0:
                ev_growth = (latest_ev / earliest_ev - 1) * 100
                if ev_growth > 50:
                    val_score = 90
                    strengths.append(f"Enterprise value up {ev_growth:.0f}% since entry")
                elif ev_growth > 20:
                    val_score = 70
                    strengths.append(f"Enterprise value up {ev_growth:.0f}%")
                elif ev_growth > 0:
                    val_score = 55
                elif ev_growth < -10:
                    val_score = 20
                    risks.append(f"Enterprise value declined {abs(ev_growth):.0f}%")

            # Multiple expansion
            latest_mult = float(val_rows[0][2]) if val_rows[0][2] else None
            earliest_mult = float(val_rows[-1][2]) if val_rows[-1][2] else None
            if latest_mult and earliest_mult and earliest_mult > 0:
                mult_change = (latest_mult / earliest_mult - 1) * 100
                if mult_change > 0:
                    val_score = min(100, val_score + 10)

            scores["valuation_momentum"] = min(100, max(0, val_score))
            details["valuation_momentum"] = {
                "latest_ev_usd": latest_ev,
                "entry_ev_usd": earliest_ev,
                "ev_growth_pct": round((latest_ev / earliest_ev - 1) * 100, 1) if earliest_ev > 0 else None,
                "latest_ev_revenue": float(val_rows[0][2]) if val_rows[0][2] else None,
                "valuations_count": len(val_rows),
            }
        elif len(val_rows) == 1:
            scores["valuation_momentum"] = 50
            details["valuation_momentum"] = {
                "latest_ev_usd": float(val_rows[0][1]) if val_rows[0][1] else None,
                "message": "Only one valuation point - trend unknown",
            }
        else:
            scores["valuation_momentum"] = 35
            details["valuation_momentum"] = {"message": "No valuation data available"}

        # =====================================================================
        # Signal 5: Market Position (15%)
        # =====================================================================
        comp_rows = db.execute(
            text("""
                SELECT competitor_type, relative_size, market_position
                FROM pe_competitor_mappings WHERE company_id = :id
            """),
            {"id": company_id},
        ).fetchall()

        if comp_rows:
            market_score = 50
            positions = [r[2] for r in comp_rows if r[2]]
            sizes = [r[1] for r in comp_rows if r[1]]

            leader_count = sum(1 for p in positions if p == "Leader")
            if leader_count > 0:
                market_score += 20
            challenger_count = sum(1 for p in positions if p == "Challenger")

            # If most competitors are smaller, company is a market leader
            smaller_count = sum(1 for s in sizes if s == "Smaller")
            if smaller_count > len(sizes) / 2:
                market_score += 20
                strengths.append("Market leader position vs. majority of competitors")
            elif smaller_count > 0:
                market_score += 10

            direct_count = sum(1 for r in comp_rows if r[0] == "Direct")
            if direct_count >= 3:
                market_score += 10  # Active competitive market = buyer interest

            scores["market_position"] = min(100, max(0, market_score))
            details["market_position"] = {
                "total_competitors": len(comp_rows),
                "direct_competitors": direct_count,
                "position_distribution": {
                    "leader": leader_count,
                    "challenger": challenger_count,
                    "niche": sum(1 for p in positions if p == "Niche"),
                },
                "relative_size_distribution": {
                    "larger": sum(1 for s in sizes if s == "Larger"),
                    "similar": sum(1 for s in sizes if s == "Similar"),
                    "smaller": smaller_count,
                },
            }
        else:
            scores["market_position"] = 40
            details["market_position"] = {"message": "No competitor data available"}

        # =====================================================================
        # Signal 6: Hold Period (10%)
        # =====================================================================
        inv_row = db.execute(
            text("""
                SELECT MIN(fi.investment_date), pf.name as firm_name
                FROM pe_fund_investments fi
                JOIN pe_funds f ON fi.fund_id = f.id
                JOIN pe_firms pf ON f.firm_id = pf.id
                WHERE fi.company_id = :id AND fi.status = 'Active'
                GROUP BY pf.name
                ORDER BY MIN(fi.investment_date) ASC LIMIT 1
            """),
            {"id": company_id},
        ).fetchone()

        if inv_row and inv_row[0]:
            from datetime import date as date_type
            entry_date = inv_row[0]
            if isinstance(entry_date, str):
                entry_date = date_type.fromisoformat(entry_date)
            hold_years = (date_type.today() - entry_date).days / 365.25

            # Typical PE hold is 4-7 years. Sweet spot for exit is 3-6 years.
            if 3 <= hold_years <= 6:
                hold_score = 85
                strengths.append(f"Hold period ({hold_years:.1f}y) in optimal exit window")
            elif hold_years < 3:
                hold_score = 40
                risks.append(f"Short hold period ({hold_years:.1f}y) - may be early for exit")
            elif hold_years <= 8:
                hold_score = 65
            else:
                hold_score = 45
                risks.append(f"Extended hold period ({hold_years:.1f}y) - fund may be aging")

            scores["hold_period"] = hold_score
            details["hold_period"] = {
                "entry_date": entry_date.isoformat(),
                "hold_years": round(hold_years, 1),
                "sponsor": inv_row[1],
            }
        else:
            scores["hold_period"] = 50
            details["hold_period"] = {"message": "No investment date found"}

        # =====================================================================
        # Composite Score
        # =====================================================================
        weights = {
            "financial_health": 0.25,
            "financial_trajectory": 0.20,
            "leadership_stability": 0.15,
            "valuation_momentum": 0.15,
            "market_position": 0.15,
            "hold_period": 0.10,
        }

        composite = sum(scores.get(k, 50) * w for k, w in weights.items())
        composite = round(composite, 1)

        # Tier
        if composite >= 80:
            tier = "A"
            timing = "Strong exit candidate - consider initiating process within 6-12 months"
        elif composite >= 65:
            tier = "B"
            timing = "Near exit-ready - address remaining gaps, target 12-18 month exit"
        elif composite >= 50:
            tier = "C"
            timing = "Moderate readiness - 18-24 months of value creation work recommended"
        elif composite >= 35:
            tier = "D"
            timing = "Early stage - significant operational improvements needed before exit"
        else:
            tier = "F"
            timing = "Not exit-ready - fundamental issues must be resolved"

        return {
            "company_id": company_id,
            "company_name": company_name,
            "pe_owner": pe_owner,
            "exit_readiness_score": composite,
            "tier": tier,
            "timing_recommendation": timing,
            "category_scores": {
                k: {"score": scores.get(k, 50), "weight": f"{int(w * 100)}%"}
                for k, w in weights.items()
            },
            "category_details": details,
            "strengths": strengths[:8],
            "risks": risks[:8],
            "scored_at": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing exit readiness: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Buyer Discovery
# =============================================================================


@router.get("/{company_id}/potential-buyers")
async def get_potential_buyers(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Identify potential buyers for a portfolio company.

    Discovers two buyer categories:
    1. **Strategic buyers** — competitors and adjacent companies that could
       acquire the company for product/market synergies
    2. **Financial buyers** — PE firms that invest in the same sector/industry

    Each buyer gets a fit score (0-100) based on size match, sector overlap,
    and strategic rationale.

    **Returns:**
    - Ranked list of strategic buyers with fit scores and rationale
    - Ranked list of financial buyers with fit scores and fund info
    - Summary statistics
    """
    try:
        # 1. Get company info
        company_row = db.execute(
            text("""
                SELECT id, name, industry, sector, current_pe_owner,
                       employee_count, founded_year
                FROM pe_portfolio_companies WHERE id = :id
            """),
            {"id": company_id},
        ).fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        company_name = company_row[1]
        company_industry = company_row[2]
        company_sector = company_row[3]
        current_pe_owner = company_row[4]

        # Get latest revenue for size context
        fin_row = db.execute(
            text("""
                SELECT revenue_usd, enterprise_value_usd
                FROM (
                    SELECT f.revenue_usd, v.enterprise_value_usd
                    FROM pe_company_financials f
                    LEFT JOIN pe_company_valuations v ON v.company_id = f.company_id
                    WHERE f.company_id = :id AND f.fiscal_period = 'FY'
                    ORDER BY f.fiscal_year DESC, v.valuation_date DESC NULLS LAST
                    LIMIT 1
                ) sub
            """),
            {"id": company_id},
        ).fetchone()
        company_revenue = float(fin_row[0]) if fin_row and fin_row[0] else None

        # Get latest EV separately (more reliable)
        ev_row = db.execute(
            text("""
                SELECT enterprise_value_usd FROM pe_company_valuations
                WHERE company_id = :id ORDER BY valuation_date DESC LIMIT 1
            """),
            {"id": company_id},
        ).fetchone()
        company_ev = float(ev_row[0]) if ev_row and ev_row[0] else None

        # =====================================================================
        # Strategic Buyers (from competitor mappings)
        # =====================================================================
        comp_rows = db.execute(
            text("""
                SELECT competitor_name, competitor_company_id, is_public, ticker,
                       is_pe_backed, pe_owner, competitor_type, relative_size,
                       market_position, notes
                FROM pe_competitor_mappings
                WHERE company_id = :id
                ORDER BY
                    CASE competitor_type WHEN 'Direct' THEN 1 WHEN 'Indirect' THEN 2 ELSE 3 END,
                    CASE relative_size WHEN 'Larger' THEN 1 WHEN 'Similar' THEN 2 ELSE 3 END
            """),
            {"id": company_id},
        ).fetchall()

        strategic_buyers = []
        for row in comp_rows:
            fit_score = 50  # base
            rationale = []

            comp_type = row[6]
            rel_size = row[7]
            mkt_pos = row[8]
            is_public = row[2]
            is_pe_backed = row[4]

            # Larger companies are better acquirers
            if rel_size == "Larger":
                fit_score += 20
                rationale.append("Has scale to acquire")
            elif rel_size == "Similar":
                fit_score += 10
                rationale.append("Similar scale - potential merger of equals")

            # Direct competitors have highest strategic rationale
            if comp_type == "Direct":
                fit_score += 15
                rationale.append("Direct competitor - product/customer overlap")
            elif comp_type == "Indirect":
                fit_score += 5
                rationale.append("Adjacent market - expansion opportunity")

            # Public companies have acquisition currency (stock)
            if is_public:
                fit_score += 10
                rationale.append("Public company - can use stock as acquisition currency")

            # Market leaders are more likely acquirers
            if mkt_pos == "Leader":
                fit_score += 5
                rationale.append("Market leader - consolidation play")

            # PE-backed companies might do add-on acquisitions
            if is_pe_backed:
                rationale.append(f"PE-backed by {row[5]} - potential add-on acquisition")

            strategic_buyers.append({
                "name": row[0],
                "linked_company_id": row[1],
                "buyer_type": "Strategic",
                "fit_score": min(100, fit_score),
                "public_info": {
                    "is_public": is_public,
                    "ticker": row[3],
                },
                "pe_info": {
                    "is_pe_backed": is_pe_backed,
                    "pe_owner": row[5],
                },
                "competitive_position": {
                    "type": comp_type,
                    "relative_size": rel_size,
                    "market_position": mkt_pos,
                },
                "rationale": rationale,
            })

        # Sort by fit score
        strategic_buyers.sort(key=lambda x: -x["fit_score"])

        # =====================================================================
        # Financial Buyers (PE firms in same sector/industry)
        # =====================================================================
        financial_buyers = []

        # Find PE firms with matching sector focus or strategy
        pe_rows = db.execute(
            text("""
                SELECT pf.id, pf.name, pf.primary_strategy, pf.sector_focus,
                       pf.aum_usd_millions, pf.typical_check_size_min,
                       pf.typical_check_size_max, pf.firm_type,
                       pf.headquarters_city, pf.headquarters_state
                FROM pe_firms pf
                WHERE pf.status = 'Active'
                AND (
                    pf.primary_strategy ILIKE :industry
                    OR pf.primary_strategy ILIKE :sector
                    OR pf.sector_focus::text ILIKE :industry_pct
                    OR pf.sector_focus::text ILIKE :sector_pct
                    OR pf.primary_strategy IN ('Buyout', 'Growth Equity')
                )
                AND pf.name != :current_owner
                ORDER BY pf.aum_usd_millions DESC NULLS LAST
                LIMIT 20
            """),
            {
                "industry": f"%{company_industry or 'NOMATCH'}%",
                "sector": f"%{company_sector or 'NOMATCH'}%",
                "industry_pct": f"%{company_industry or 'NOMATCH'}%",
                "sector_pct": f"%{company_sector or 'NOMATCH'}%",
                "current_owner": current_pe_owner or "",
            },
        ).fetchall()

        for row in pe_rows:
            fit_score = 40  # base
            rationale = []
            strategy = row[2] or ""
            sector_focus = row[3]
            aum = float(row[4]) if row[4] else None
            check_min = float(row[5]) if row[5] else None
            check_max = float(row[6]) if row[6] else None

            # Strategy match
            if company_industry and company_industry.lower() in strategy.lower():
                fit_score += 25
                rationale.append(f"Strategy directly targets {company_industry}")
            elif company_sector and company_sector.lower() in strategy.lower():
                fit_score += 20
                rationale.append(f"Strategy aligns with {company_sector}")
            elif strategy in ("Buyout", "Growth Equity"):
                fit_score += 10
                rationale.append(f"{strategy} strategy - generalist buyer")

            # Sector focus match
            if sector_focus and isinstance(sector_focus, list):
                for sf in sector_focus:
                    if (company_industry and company_industry.lower() in sf.lower()) or \
                       (company_sector and company_sector.lower() in sf.lower()):
                        fit_score += 15
                        rationale.append(f"Sector focus includes {sf}")
                        break

            # AUM / check size fit
            if aum and company_ev:
                # Typical PE deal is 5-15% of AUM
                if company_ev <= aum * 1e6 * 0.15:
                    fit_score += 10
                    rationale.append("Deal size within typical range for fund")
                elif company_ev > aum * 1e6 * 0.25:
                    fit_score -= 10
                    rationale.append("May be too large for fund size")
            elif aum and aum > 5000:
                fit_score += 5  # Large fund can likely accommodate

            # Count existing investments in similar sectors
            existing = db.execute(
                text("""
                    SELECT COUNT(DISTINCT fi.company_id)
                    FROM pe_fund_investments fi
                    JOIN pe_funds f ON fi.fund_id = f.id
                    JOIN pe_portfolio_companies pc ON fi.company_id = pc.id
                    WHERE f.firm_id = :firm_id
                    AND (pc.industry = :industry OR pc.sector = :sector)
                    AND fi.status = 'Active'
                """),
                {
                    "firm_id": row[0],
                    "industry": company_industry or "",
                    "sector": company_sector or "",
                },
            ).fetchone()
            if existing and existing[0] > 0:
                fit_score += 10
                rationale.append(f"Already has {existing[0]} portfolio companies in sector")

            financial_buyers.append({
                "name": row[1],
                "firm_id": row[0],
                "buyer_type": "Financial",
                "fit_score": min(100, fit_score),
                "firm_info": {
                    "type": row[7],
                    "strategy": strategy,
                    "aum_usd_millions": aum,
                    "check_size": {
                        "min_usd_millions": check_min,
                        "max_usd_millions": check_max,
                    },
                    "location": f"{row[8]}, {row[9]}" if row[8] else None,
                },
                "rationale": rationale,
            })

        # Sort by fit score
        financial_buyers.sort(key=lambda x: -x["fit_score"])
        financial_buyers = financial_buyers[:15]

        return {
            "company_id": company_id,
            "company_name": company_name,
            "company_context": {
                "industry": company_industry,
                "sector": company_sector,
                "current_pe_owner": current_pe_owner,
                "latest_revenue_usd": company_revenue,
                "latest_ev_usd": company_ev,
            },
            "strategic_buyers": {
                "count": len(strategic_buyers),
                "buyers": strategic_buyers,
            },
            "financial_buyers": {
                "count": len(financial_buyers),
                "buyers": financial_buyers,
            },
            "summary": {
                "total_potential_buyers": len(strategic_buyers) + len(financial_buyers),
                "top_strategic": strategic_buyers[0]["name"] if strategic_buyers else None,
                "top_financial": financial_buyers[0]["name"] if financial_buyers else None,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error finding potential buyers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Data Room Package
# =============================================================================


@router.post("/{company_id}/data-room-package")
async def generate_data_room_package(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Assemble a complete data room package for a portfolio company.

    Aggregates all available data into a single structured response suitable
    for export to a virtual data room (VDR). Includes:

    1. **Company Profile** — basic info, classification, ownership
    2. **Financial Summary** — multi-year P&L, balance sheet, cash flow
    3. **Valuation History** — EV, multiples, methodology
    4. **Leadership Team** — executives, board, PE-appointed roles
    5. **Competitive Landscape** — competitors with positioning
    6. **Benchmark Analysis** — financial metrics vs. peer group
    7. **Exit Readiness** — composite score with category breakdown
    8. **Investment History** — fund investments, entry/exit metrics

    **Returns:** Single JSON object with all sections populated.
    """
    try:
        # Verify company exists
        company_row = db.execute(
            text("""
                SELECT id, name, legal_name, website, description,
                       headquarters_city, headquarters_state, headquarters_country,
                       industry, sub_industry, sector,
                       founded_year, employee_count,
                       ownership_status, current_pe_owner, status
                FROM pe_portfolio_companies WHERE id = :id
            """),
            {"id": company_id},
        ).fetchone()
        if not company_row:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        package = {
            "company_id": company_id,
            "generated_at": datetime.utcnow().isoformat(),
            "sections": {},
        }

        # --- Section 1: Company Profile ---
        package["sections"]["company_profile"] = {
            "name": company_row[1],
            "legal_name": company_row[2],
            "website": company_row[3],
            "description": company_row[4],
            "headquarters": {
                "city": company_row[5],
                "state": company_row[6],
                "country": company_row[7],
            },
            "classification": {
                "industry": company_row[8],
                "sub_industry": company_row[9],
                "sector": company_row[10],
            },
            "company_info": {
                "founded_year": company_row[11],
                "employee_count": company_row[12],
            },
            "ownership": {
                "status": company_row[13],
                "current_pe_owner": company_row[14],
            },
        }

        # --- Section 2: Financial Summary ---
        fin_rows = db.execute(
            text("""
                SELECT fiscal_year, fiscal_period,
                       revenue_usd, revenue_growth_pct,
                       gross_profit_usd, gross_margin_pct,
                       ebitda_usd, ebitda_margin_pct,
                       net_income_usd,
                       total_assets_usd, total_debt_usd, cash_usd, net_debt_usd,
                       operating_cash_flow_usd, capex_usd, free_cash_flow_usd,
                       debt_to_ebitda, interest_coverage,
                       is_audited, data_source, confidence
                FROM pe_company_financials
                WHERE company_id = :id AND fiscal_period = 'FY'
                ORDER BY fiscal_year ASC
            """),
            {"id": company_id},
        ).fetchall()

        financials = []
        for r in fin_rows:
            financials.append({
                "fiscal_year": r[0],
                "revenue_usd": float(r[2]) if r[2] else None,
                "revenue_growth_pct": float(r[3]) if r[3] else None,
                "gross_margin_pct": float(r[5]) if r[5] else None,
                "ebitda_usd": float(r[6]) if r[6] else None,
                "ebitda_margin_pct": float(r[7]) if r[7] else None,
                "net_income_usd": float(r[8]) if r[8] else None,
                "total_debt_usd": float(r[10]) if r[10] else None,
                "cash_usd": float(r[11]) if r[11] else None,
                "free_cash_flow_usd": float(r[15]) if r[15] else None,
                "debt_to_ebitda": float(r[16]) if r[16] else None,
                "is_audited": r[18],
                "data_source": r[19],
            })
        package["sections"]["financial_summary"] = {
            "periods": len(financials),
            "data": financials,
        }

        # --- Section 3: Valuation History ---
        val_rows = db.execute(
            text("""
                SELECT valuation_date, enterprise_value_usd, equity_value_usd,
                       ev_revenue_multiple, ev_ebitda_multiple,
                       valuation_type, methodology, event_type,
                       data_source, confidence
                FROM pe_company_valuations
                WHERE company_id = :id ORDER BY valuation_date ASC
            """),
            {"id": company_id},
        ).fetchall()

        valuations = []
        for r in val_rows:
            valuations.append({
                "date": r[0].isoformat() if r[0] else None,
                "enterprise_value_usd": float(r[1]) if r[1] else None,
                "equity_value_usd": float(r[2]) if r[2] else None,
                "ev_revenue_multiple": float(r[3]) if r[3] else None,
                "ev_ebitda_multiple": float(r[4]) if r[4] else None,
                "type": r[5],
                "methodology": r[6],
                "event": r[7],
            })
        package["sections"]["valuation_history"] = {
            "count": len(valuations),
            "data": valuations,
        }

        # --- Section 4: Leadership Team ---
        lead_rows = db.execute(
            text("""
                SELECT p.full_name, cl.title, cl.role_category,
                       cl.is_ceo, cl.is_cfo, cl.is_board_member, cl.is_board_chair,
                       cl.start_date, cl.is_current,
                       cl.appointed_by_pe, cl.pe_firm_affiliation,
                       p.linkedin_url
                FROM pe_company_leadership cl
                JOIN pe_people p ON cl.person_id = p.id
                WHERE cl.company_id = :id
                ORDER BY cl.is_current DESC,
                    cl.is_ceo DESC, cl.is_cfo DESC,
                    CASE cl.role_category
                        WHEN 'C-Suite' THEN 1 WHEN 'VP' THEN 2
                        WHEN 'Director' THEN 3 WHEN 'Board' THEN 4 ELSE 5
                    END
            """),
            {"id": company_id},
        ).fetchall()

        leadership = []
        for r in lead_rows:
            leadership.append({
                "name": r[0],
                "title": r[1],
                "role_category": r[2],
                "is_ceo": r[3],
                "is_cfo": r[4],
                "is_board_member": r[5],
                "is_board_chair": r[6],
                "start_date": r[7].isoformat() if r[7] else None,
                "is_current": r[8],
                "appointed_by_pe": r[9],
                "pe_firm_affiliation": r[10],
                "linkedin_url": r[11],
            })
        package["sections"]["leadership_team"] = {
            "count": len(leadership),
            "data": leadership,
        }

        # --- Section 5: Competitive Landscape ---
        comp_rows = db.execute(
            text("""
                SELECT competitor_name, is_public, ticker, is_pe_backed, pe_owner,
                       competitor_type, relative_size, market_position, notes
                FROM pe_competitor_mappings
                WHERE company_id = :id
                ORDER BY
                    CASE competitor_type WHEN 'Direct' THEN 1 WHEN 'Indirect' THEN 2 ELSE 3 END
            """),
            {"id": company_id},
        ).fetchall()

        competitors = []
        for r in comp_rows:
            competitors.append({
                "name": r[0],
                "is_public": r[1],
                "ticker": r[2],
                "is_pe_backed": r[3],
                "pe_owner": r[4],
                "type": r[5],
                "relative_size": r[6],
                "market_position": r[7],
                "notes": r[8],
            })
        package["sections"]["competitive_landscape"] = {
            "count": len(competitors),
            "data": competitors,
        }

        # --- Section 6: Investment History ---
        inv_rows = db.execute(
            text("""
                SELECT f.name as fund_name, pf.name as firm_name,
                       fi.investment_date, fi.investment_type, fi.investment_round,
                       fi.invested_amount_usd, fi.ownership_pct,
                       fi.entry_ev_usd, fi.entry_ev_ebitda_multiple,
                       fi.status, fi.exit_date, fi.exit_type,
                       fi.exit_amount_usd, fi.exit_multiple, fi.exit_irr_pct
                FROM pe_fund_investments fi
                JOIN pe_funds f ON fi.fund_id = f.id
                JOIN pe_firms pf ON f.firm_id = pf.id
                WHERE fi.company_id = :id
                ORDER BY fi.investment_date DESC
            """),
            {"id": company_id},
        ).fetchall()

        investments = []
        for r in inv_rows:
            investments.append({
                "fund": r[0],
                "firm": r[1],
                "date": r[2].isoformat() if r[2] else None,
                "type": r[3],
                "round": r[4],
                "amount_usd": float(r[5]) if r[5] else None,
                "ownership_pct": float(r[6]) if r[6] else None,
                "entry_ev_usd": float(r[7]) if r[7] else None,
                "entry_multiple": float(r[8]) if r[8] else None,
                "status": r[9],
                "exit_date": r[10].isoformat() if r[10] else None,
                "exit_type": r[11],
                "exit_amount_usd": float(r[12]) if r[12] else None,
                "exit_multiple": float(r[13]) if r[13] else None,
                "exit_irr_pct": float(r[14]) if r[14] else None,
            })
        package["sections"]["investment_history"] = {
            "count": len(investments),
            "data": investments,
        }

        # --- Summary stats ---
        latest_fin = financials[-1] if financials else {}
        latest_val = valuations[-1] if valuations else {}
        package["summary"] = {
            "company_name": company_row[1],
            "industry": company_row[8],
            "pe_owner": company_row[14],
            "latest_revenue_usd": latest_fin.get("revenue_usd"),
            "latest_ebitda_usd": latest_fin.get("ebitda_usd"),
            "latest_ev_usd": latest_val.get("enterprise_value_usd"),
            "financial_periods": len(financials),
            "valuation_points": len(valuations),
            "leadership_count": len(leadership),
            "competitor_count": len(competitors),
            "investment_count": len(investments),
            "data_completeness": {
                "has_financials": len(financials) > 0,
                "has_valuations": len(valuations) > 0,
                "has_leadership": len(leadership) > 0,
                "has_competitors": len(competitors) > 0,
                "has_investments": len(investments) > 0,
            },
        }

        return package

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating data room package: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
