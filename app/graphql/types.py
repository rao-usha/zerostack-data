"""
Strawberry GraphQL type definitions for Nexdata.

Defines all GraphQL types for investors, portfolio companies, and analytics.
"""

import strawberry
from datetime import datetime
from typing import Optional, List


@strawberry.type
class PortfolioCompanyType:
    """Portfolio company/investment holding."""

    id: int
    investor_id: int
    investor_type: str
    company_name: str
    company_website: Optional[str] = None
    company_industry: Optional[str] = None
    company_stage: Optional[str] = None
    company_location: Optional[str] = None
    company_ticker: Optional[str] = None
    company_cusip: Optional[str] = None
    investment_type: Optional[str] = None
    investment_date: Optional[datetime] = None
    investment_amount_usd: Optional[str] = None
    shares_held: Optional[str] = None
    market_value_usd: Optional[str] = None
    ownership_percentage: Optional[str] = None
    current_holding: bool = True
    confidence_level: Optional[str] = None
    source_type: Optional[str] = None
    source_url: Optional[str] = None
    collected_date: Optional[datetime] = None
    created_at: Optional[datetime] = None


@strawberry.type
class CoInvestorType:
    """Co-investor relationship."""

    id: int
    co_investor_name: str
    co_investor_type: Optional[str] = None
    deal_name: Optional[str] = None
    deal_date: Optional[datetime] = None
    deal_size_usd: Optional[str] = None
    co_investment_count: int = 1
    source_type: Optional[str] = None
    source_url: Optional[str] = None
    collected_date: Optional[datetime] = None


@strawberry.type
class LPFundType:
    """Limited Partner (LP) fund - pension funds, endowments, etc."""

    id: int
    name: str
    formal_name: Optional[str] = None
    lp_type: str
    jurisdiction: Optional[str] = None
    website_url: Optional[str] = None
    created_at: Optional[datetime] = None

    # These will be populated by resolvers
    portfolio_companies: List[PortfolioCompanyType] = strawberry.field(
        default_factory=list
    )
    co_investors: List[CoInvestorType] = strawberry.field(default_factory=list)
    portfolio_count: int = 0


@strawberry.type
class FamilyOfficeType:
    """Family office investor."""

    id: int
    name: str
    legal_name: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    type: Optional[str] = None
    city: Optional[str] = None
    state_province: Optional[str] = None
    website: Optional[str] = None
    principal_family: Optional[str] = None
    principal_name: Optional[str] = None
    estimated_wealth: Optional[str] = None
    investment_focus: Optional[List[str]] = None
    sectors_of_interest: Optional[List[str]] = None
    geographic_focus: Optional[List[str]] = None
    stage_preference: Optional[List[str]] = None
    check_size_range: Optional[str] = None
    estimated_aum: Optional[str] = None
    status: Optional[str] = None
    actively_investing: Optional[bool] = None
    created_at: Optional[datetime] = None

    # These will be populated by resolvers
    portfolio_companies: List[PortfolioCompanyType] = strawberry.field(
        default_factory=list
    )
    co_investors: List[CoInvestorType] = strawberry.field(default_factory=list)
    portfolio_count: int = 0


@strawberry.type
class SearchResultType:
    """Search result from full-text search."""

    id: int
    type: str
    name: str
    description: Optional[str] = None
    score: float = 0.0


@strawberry.type
class AnalyticsOverviewType:
    """System-wide analytics overview."""

    total_lp_funds: int
    total_family_offices: int
    total_portfolio_companies: int
    total_co_investments: int
    coverage_percentage: float
    lp_with_portfolio: int
    fo_with_portfolio: int


@strawberry.type
class IndustryBreakdownType:
    """Industry distribution data."""

    industry: str
    count: int
    percentage: float


@strawberry.type
class TopMoverType:
    """Recent portfolio change."""

    investor_id: int
    investor_type: str
    investor_name: str
    company_name: str
    change_type: str
    collected_date: datetime
