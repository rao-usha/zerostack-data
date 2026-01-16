"""
GraphQL resolvers for Nexdata queries.

Implements all query logic using synchronous database access.
"""
from typing import Optional, List
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.graphql.types import (
    LPFundType,
    FamilyOfficeType,
    PortfolioCompanyType,
    CoInvestorType,
    SearchResultType,
    AnalyticsOverviewType,
    IndustryBreakdownType,
    TopMoverType,
)
from app.graphql.dataloaders import load_portfolio_companies, load_coinvestors


def resolve_lp_fund(
    db: Session,
    id: int,
    include_portfolio: bool = True,
    portfolio_limit: int = 50,
    include_coinvestors: bool = True,
    coinvestor_limit: int = 50
) -> Optional[LPFundType]:
    """Resolve a single LP fund by ID with optional nested data."""
    query = text("""
        SELECT id, name, formal_name, lp_type, jurisdiction, website_url, created_at
        FROM lp_fund
        WHERE id = :id
    """)
    result = db.execute(query, {"id": id})
    row = result.mappings().fetchone()

    if not row:
        return None

    lp = LPFundType(
        id=row["id"],
        name=row["name"],
        formal_name=row.get("formal_name"),
        lp_type=row["lp_type"],
        jurisdiction=row.get("jurisdiction"),
        website_url=row.get("website_url"),
        created_at=row.get("created_at"),
    )

    # Load nested data if requested
    if include_portfolio:
        companies = load_portfolio_companies(db, lp.id, "lp")
        lp.portfolio_companies = companies[:portfolio_limit]
        lp.portfolio_count = len(companies)

    if include_coinvestors:
        coinvestors = load_coinvestors(db, lp.id, "lp")
        lp.co_investors = coinvestors[:coinvestor_limit]

    return lp


def resolve_lp_funds(
    db: Session,
    limit: int = 50,
    offset: int = 0,
    lp_type: Optional[str] = None,
    jurisdiction: Optional[str] = None,
    include_portfolio: bool = False,
    portfolio_limit: int = 10
) -> List[LPFundType]:
    """Resolve list of LP funds with optional filters."""
    conditions = []
    params = {"limit": limit, "offset": offset}

    if lp_type:
        conditions.append("lp_type = :lp_type")
        params["lp_type"] = lp_type

    if jurisdiction:
        conditions.append("jurisdiction = :jurisdiction")
        params["jurisdiction"] = jurisdiction

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = text(f"""
        SELECT id, name, formal_name, lp_type, jurisdiction, website_url, created_at
        FROM lp_fund
        WHERE {where_clause}
        ORDER BY name
        LIMIT :limit OFFSET :offset
    """)

    result = db.execute(query, params)
    lp_funds = []

    for row in result.mappings():
        lp = LPFundType(
            id=row["id"],
            name=row["name"],
            formal_name=row.get("formal_name"),
            lp_type=row["lp_type"],
            jurisdiction=row.get("jurisdiction"),
            website_url=row.get("website_url"),
            created_at=row.get("created_at"),
        )

        # Optionally load portfolio data
        if include_portfolio:
            companies = load_portfolio_companies(db, lp.id, "lp")
            lp.portfolio_companies = companies[:portfolio_limit]
            lp.portfolio_count = len(companies)

        lp_funds.append(lp)

    return lp_funds


def resolve_family_office(
    db: Session,
    id: int,
    include_portfolio: bool = True,
    portfolio_limit: int = 50,
    include_coinvestors: bool = True,
    coinvestor_limit: int = 50
) -> Optional[FamilyOfficeType]:
    """Resolve a single family office by ID with optional nested data."""
    query = text("""
        SELECT id, name, legal_name, region, country, type, city, state_province,
               website, principal_family, principal_name, estimated_wealth,
               investment_focus, sectors_of_interest, geographic_focus,
               stage_preference, check_size_range, estimated_aum, status,
               actively_investing, created_at
        FROM family_offices
        WHERE id = :id
    """)
    result = db.execute(query, {"id": id})
    row = result.mappings().fetchone()

    if not row:
        return None

    fo = FamilyOfficeType(
        id=row["id"],
        name=row["name"],
        legal_name=row.get("legal_name"),
        region=row.get("region"),
        country=row.get("country"),
        type=row.get("type"),
        city=row.get("city"),
        state_province=row.get("state_province"),
        website=row.get("website"),
        principal_family=row.get("principal_family"),
        principal_name=row.get("principal_name"),
        estimated_wealth=row.get("estimated_wealth"),
        investment_focus=row.get("investment_focus"),
        sectors_of_interest=row.get("sectors_of_interest"),
        geographic_focus=row.get("geographic_focus"),
        stage_preference=row.get("stage_preference"),
        check_size_range=row.get("check_size_range"),
        estimated_aum=row.get("estimated_aum"),
        status=row.get("status"),
        actively_investing=row.get("actively_investing"),
        created_at=row.get("created_at"),
    )

    # Load nested data if requested
    if include_portfolio:
        companies = load_portfolio_companies(db, fo.id, "family_office")
        fo.portfolio_companies = companies[:portfolio_limit]
        fo.portfolio_count = len(companies)

    if include_coinvestors:
        coinvestors = load_coinvestors(db, fo.id, "family_office")
        fo.co_investors = coinvestors[:coinvestor_limit]

    return fo


def resolve_family_offices(
    db: Session,
    limit: int = 50,
    offset: int = 0,
    region: Optional[str] = None,
    country: Optional[str] = None,
    type: Optional[str] = None,
    include_portfolio: bool = False,
    portfolio_limit: int = 10
) -> List[FamilyOfficeType]:
    """Resolve list of family offices with optional filters."""
    conditions = []
    params = {"limit": limit, "offset": offset}

    if region:
        conditions.append("region = :region")
        params["region"] = region

    if country:
        conditions.append("country = :country")
        params["country"] = country

    if type:
        conditions.append("type = :type")
        params["type"] = type

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = text(f"""
        SELECT id, name, legal_name, region, country, type, city, state_province,
               website, principal_family, principal_name, estimated_wealth,
               investment_focus, sectors_of_interest, geographic_focus,
               stage_preference, check_size_range, estimated_aum, status,
               actively_investing, created_at
        FROM family_offices
        WHERE {where_clause}
        ORDER BY name
        LIMIT :limit OFFSET :offset
    """)

    result = db.execute(query, params)
    offices = []

    for row in result.mappings():
        fo = FamilyOfficeType(
            id=row["id"],
            name=row["name"],
            legal_name=row.get("legal_name"),
            region=row.get("region"),
            country=row.get("country"),
            type=row.get("type"),
            city=row.get("city"),
            state_province=row.get("state_province"),
            website=row.get("website"),
            principal_family=row.get("principal_family"),
            principal_name=row.get("principal_name"),
            estimated_wealth=row.get("estimated_wealth"),
            investment_focus=row.get("investment_focus"),
            sectors_of_interest=row.get("sectors_of_interest"),
            geographic_focus=row.get("geographic_focus"),
            stage_preference=row.get("stage_preference"),
            check_size_range=row.get("check_size_range"),
            estimated_aum=row.get("estimated_aum"),
            status=row.get("status"),
            actively_investing=row.get("actively_investing"),
            created_at=row.get("created_at"),
        )

        # Optionally load portfolio data
        if include_portfolio:
            companies = load_portfolio_companies(db, fo.id, "family_office")
            fo.portfolio_companies = companies[:portfolio_limit]
            fo.portfolio_count = len(companies)

        offices.append(fo)

    return offices


def resolve_portfolio_company(
    db: Session,
    id: int
) -> Optional[PortfolioCompanyType]:
    """Resolve a single portfolio company by ID."""
    query = text("""
        SELECT id, investor_id, investor_type, company_name, company_website,
               company_industry, company_stage, company_location, company_ticker,
               company_cusip, investment_type, investment_date, investment_amount_usd,
               shares_held, market_value_usd, ownership_percentage, current_holding,
               confidence_level, source_type, source_url, collected_date, created_at
        FROM portfolio_companies
        WHERE id = :id
    """)
    result = db.execute(query, {"id": id})
    row = result.mappings().fetchone()

    if not row:
        return None

    return PortfolioCompanyType(
        id=row["id"],
        investor_id=row["investor_id"],
        investor_type=row["investor_type"],
        company_name=row["company_name"],
        company_website=row.get("company_website"),
        company_industry=row.get("company_industry"),
        company_stage=row.get("company_stage"),
        company_location=row.get("company_location"),
        company_ticker=row.get("company_ticker"),
        company_cusip=row.get("company_cusip"),
        investment_type=row.get("investment_type"),
        investment_date=row.get("investment_date"),
        investment_amount_usd=row.get("investment_amount_usd"),
        shares_held=row.get("shares_held"),
        market_value_usd=row.get("market_value_usd"),
        ownership_percentage=row.get("ownership_percentage"),
        current_holding=bool(row.get("current_holding", 1)),
        confidence_level=row.get("confidence_level"),
        source_type=row.get("source_type"),
        source_url=row.get("source_url"),
        collected_date=row.get("collected_date"),
        created_at=row.get("created_at"),
    )


def resolve_portfolio_companies(
    db: Session,
    investor_id: Optional[int] = None,
    investor_type: Optional[str] = None,
    industry: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[PortfolioCompanyType]:
    """Resolve list of portfolio companies with optional filters."""
    conditions = []
    params = {"limit": limit, "offset": offset}

    if investor_id:
        conditions.append("investor_id = :investor_id")
        params["investor_id"] = investor_id

    if investor_type:
        conditions.append("investor_type = :investor_type")
        params["investor_type"] = investor_type

    if industry:
        conditions.append("LOWER(company_industry) LIKE :industry")
        params["industry"] = f"%{industry.lower()}%"

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = text(f"""
        SELECT id, investor_id, investor_type, company_name, company_website,
               company_industry, company_stage, company_location, company_ticker,
               company_cusip, investment_type, investment_date, investment_amount_usd,
               shares_held, market_value_usd, ownership_percentage, current_holding,
               confidence_level, source_type, source_url, collected_date, created_at
        FROM portfolio_companies
        WHERE {where_clause}
        ORDER BY company_name
        LIMIT :limit OFFSET :offset
    """)

    result = db.execute(query, params)
    companies = []

    for row in result.mappings():
        companies.append(PortfolioCompanyType(
            id=row["id"],
            investor_id=row["investor_id"],
            investor_type=row["investor_type"],
            company_name=row["company_name"],
            company_website=row.get("company_website"),
            company_industry=row.get("company_industry"),
            company_stage=row.get("company_stage"),
            company_location=row.get("company_location"),
            company_ticker=row.get("company_ticker"),
            company_cusip=row.get("company_cusip"),
            investment_type=row.get("investment_type"),
            investment_date=row.get("investment_date"),
            investment_amount_usd=row.get("investment_amount_usd"),
            shares_held=row.get("shares_held"),
            market_value_usd=row.get("market_value_usd"),
            ownership_percentage=row.get("ownership_percentage"),
            current_holding=bool(row.get("current_holding", 1)),
            confidence_level=row.get("confidence_level"),
            source_type=row.get("source_type"),
            source_url=row.get("source_url"),
            collected_date=row.get("collected_date"),
            created_at=row.get("created_at"),
        ))

    return companies


def resolve_search(
    db: Session,
    query_text: str,
    type: Optional[str] = None,
    limit: int = 20
) -> List[SearchResultType]:
    """
    Search investors and companies using full-text search.

    Leverages the search_index table from T12.
    """
    conditions = ["search_vector @@ plainto_tsquery('english', :query)"]
    params = {"query": query_text, "limit": limit}

    if type:
        conditions.append("entity_type = :type")
        params["type"] = type

    where_clause = " AND ".join(conditions)

    search_query = text(f"""
        SELECT entity_id, entity_type, name, description,
               ts_rank(search_vector, plainto_tsquery('english', :query)) as score
        FROM search_index
        WHERE {where_clause}
        ORDER BY score DESC
        LIMIT :limit
    """)

    result = db.execute(search_query, params)
    results = []

    for row in result.mappings():
        results.append(SearchResultType(
            id=row["entity_id"],
            type=row["entity_type"],
            name=row["name"],
            description=row.get("description"),
            score=float(row["score"]),
        ))

    return results


def resolve_analytics_overview(db: Session) -> AnalyticsOverviewType:
    """Get system-wide analytics overview."""
    # Count LPs
    lp_count = db.execute(text("SELECT COUNT(*) FROM lp_fund")).scalar() or 0

    # Count Family Offices
    fo_count = db.execute(text("SELECT COUNT(*) FROM family_offices")).scalar() or 0

    # Count Portfolio Companies
    pc_count = db.execute(text("SELECT COUNT(*) FROM portfolio_companies")).scalar() or 0

    # Count Co-investments
    ci_count = db.execute(text("SELECT COUNT(*) FROM co_investments")).scalar() or 0

    # LPs with portfolio
    lp_with_portfolio = db.execute(text("""
        SELECT COUNT(DISTINCT investor_id)
        FROM portfolio_companies
        WHERE investor_type = 'lp'
    """)).scalar() or 0

    # FOs with portfolio
    fo_with_portfolio = db.execute(text("""
        SELECT COUNT(DISTINCT investor_id)
        FROM portfolio_companies
        WHERE investor_type = 'family_office'
    """)).scalar() or 0

    # Coverage percentage
    total_investors = lp_count + fo_count
    covered = lp_with_portfolio + fo_with_portfolio
    coverage = (covered / total_investors * 100) if total_investors > 0 else 0.0

    return AnalyticsOverviewType(
        total_lp_funds=lp_count,
        total_family_offices=fo_count,
        total_portfolio_companies=pc_count,
        total_co_investments=ci_count,
        coverage_percentage=round(coverage, 1),
        lp_with_portfolio=lp_with_portfolio,
        fo_with_portfolio=fo_with_portfolio,
    )


def resolve_industry_breakdown(
    db: Session,
    investor_id: Optional[int] = None,
    investor_type: Optional[str] = None,
    limit: int = 20
) -> List[IndustryBreakdownType]:
    """Get industry distribution of portfolio companies."""
    conditions = ["company_industry IS NOT NULL", "company_industry != ''"]
    params = {"limit": limit}

    if investor_id:
        conditions.append("investor_id = :investor_id")
        params["investor_id"] = investor_id

    if investor_type:
        conditions.append("investor_type = :investor_type")
        params["investor_type"] = investor_type

    where_clause = " AND ".join(conditions)

    # Get total count for percentage calculation
    total_query = text(f"SELECT COUNT(*) FROM portfolio_companies WHERE {where_clause}")
    total = db.execute(total_query, params).scalar() or 1

    query = text(f"""
        SELECT company_industry as industry, COUNT(*) as count
        FROM portfolio_companies
        WHERE {where_clause}
        GROUP BY company_industry
        ORDER BY count DESC
        LIMIT :limit
    """)

    result = db.execute(query, params)
    breakdown = []

    for row in result.mappings():
        count = row["count"]
        breakdown.append(IndustryBreakdownType(
            industry=row["industry"],
            count=count,
            percentage=round(count / total * 100, 1),
        ))

    return breakdown


def resolve_top_movers(
    db: Session,
    days: int = 30,
    limit: int = 20
) -> List[TopMoverType]:
    """Get recent portfolio changes (new holdings, exits)."""
    query = text("""
        SELECT pc.investor_id, pc.investor_type, pc.company_name,
               pc.collected_date,
               CASE
                   WHEN pc.current_holding = 1 THEN 'new_holding'
                   ELSE 'exit'
               END as change_type,
               CASE
                   WHEN pc.investor_type = 'lp' THEN (SELECT name FROM lp_fund WHERE id = pc.investor_id)
                   ELSE (SELECT name FROM family_offices WHERE id = pc.investor_id)
               END as investor_name
        FROM portfolio_companies pc
        WHERE pc.collected_date >= NOW() - INTERVAL ':days days'
        ORDER BY pc.collected_date DESC
        LIMIT :limit
    """)

    # Can't use interval with parameter directly in some PostgreSQL versions
    # Use a workaround
    query = text(f"""
        SELECT pc.investor_id, pc.investor_type, pc.company_name,
               pc.collected_date,
               CASE
                   WHEN pc.current_holding = 1 THEN 'new_holding'
                   ELSE 'exit'
               END as change_type,
               CASE
                   WHEN pc.investor_type = 'lp' THEN (SELECT name FROM lp_fund WHERE id = pc.investor_id)
                   ELSE (SELECT name FROM family_offices WHERE id = pc.investor_id)
               END as investor_name
        FROM portfolio_companies pc
        WHERE pc.collected_date >= NOW() - INTERVAL '{days} days'
        ORDER BY pc.collected_date DESC
        LIMIT :limit
    """)

    result = db.execute(query, {"limit": limit})
    movers = []

    for row in result.mappings():
        if row.get("investor_name"):  # Skip if investor not found
            movers.append(TopMoverType(
                investor_id=row["investor_id"],
                investor_type=row["investor_type"],
                investor_name=row["investor_name"],
                company_name=row["company_name"],
                change_type=row["change_type"],
                collected_date=row["collected_date"],
            ))

    return movers
