"""
DataLoaders for efficient batch data fetching.

Prevents N+1 query problems by batching database requests.
Uses synchronous database access to match existing codebase patterns.
"""

from typing import List, Dict, Tuple, Any, Callable
from collections import defaultdict
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.graphql.types import PortfolioCompanyType, CoInvestorType


def load_portfolio_companies(
    db: Session, investor_id: int, investor_type: str
) -> List[PortfolioCompanyType]:
    """Load portfolio companies for an investor."""
    query = text("""
        SELECT id, investor_id, investor_type, company_name, company_website,
               company_industry, company_stage, company_location, company_ticker,
               company_cusip, investment_type, investment_date, investment_amount_usd,
               shares_held, market_value_usd, ownership_percentage, current_holding,
               confidence_level, source_type, source_url, collected_date, created_at
        FROM portfolio_companies
        WHERE investor_id = :investor_id AND investor_type = :investor_type
        ORDER BY company_name
    """)
    result = db.execute(
        query, {"investor_id": investor_id, "investor_type": investor_type}
    )

    companies = []
    for row in result.mappings():
        companies.append(_row_to_portfolio_company(row))

    return companies


def load_coinvestors(
    db: Session, investor_id: int, investor_type: str
) -> List[CoInvestorType]:
    """Load co-investors for an investor."""
    query = text("""
        SELECT id, primary_investor_id, primary_investor_type, co_investor_name,
               co_investor_type, deal_name, deal_date, deal_size_usd,
               co_investment_count, source_type, source_url, collected_date
        FROM co_investments
        WHERE primary_investor_id = :investor_id AND primary_investor_type = :investor_type
        ORDER BY co_investor_name
    """)
    result = db.execute(
        query, {"investor_id": investor_id, "investor_type": investor_type}
    )

    coinvestors = []
    for row in result.mappings():
        coinvestors.append(_row_to_coinvestor(row))

    return coinvestors


def _row_to_portfolio_company(row: Dict[str, Any]) -> PortfolioCompanyType:
    """Convert a database row to PortfolioCompanyType."""
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


def _row_to_coinvestor(row: Dict[str, Any]) -> CoInvestorType:
    """Convert a database row to CoInvestorType."""
    return CoInvestorType(
        id=row["id"],
        co_investor_name=row["co_investor_name"],
        co_investor_type=row.get("co_investor_type"),
        deal_name=row.get("deal_name"),
        deal_date=row.get("deal_date"),
        deal_size_usd=row.get("deal_size_usd"),
        co_investment_count=row.get("co_investment_count", 1),
        source_type=row.get("source_type"),
        source_url=row.get("source_url"),
        collected_date=row.get("collected_date"),
    )
