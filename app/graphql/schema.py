"""
GraphQL schema and FastAPI integration for Nexdata.

Assembles all types, resolvers, and creates the GraphQL endpoint.
"""
import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import Optional, List
from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.graphql.types import (
    LPFundType,
    FamilyOfficeType,
    PortfolioCompanyType,
    SearchResultType,
    AnalyticsOverviewType,
    IndustryBreakdownType,
    TopMoverType,
)
from app.graphql.resolvers import (
    resolve_lp_fund,
    resolve_lp_funds,
    resolve_family_office,
    resolve_family_offices,
    resolve_portfolio_company,
    resolve_portfolio_companies,
    resolve_search,
    resolve_analytics_overview,
    resolve_industry_breakdown,
    resolve_top_movers,
)


def get_context(db: Session = Depends(get_db)):
    """Create GraphQL context with database session."""
    return {"db": db}


@strawberry.type
class Query:
    """Root GraphQL query type."""

    @strawberry.field(description="Get a single LP fund by ID")
    def lp_fund(
        self,
        info: strawberry.Info,
        id: int,
        include_portfolio: bool = True,
        portfolio_limit: int = 50,
        include_coinvestors: bool = True,
        coinvestor_limit: int = 50,
    ) -> Optional[LPFundType]:
        db = info.context["db"]
        return resolve_lp_fund(
            db, id, include_portfolio, portfolio_limit,
            include_coinvestors, coinvestor_limit
        )

    @strawberry.field(description="List LP funds with optional filters")
    def lp_funds(
        self,
        info: strawberry.Info,
        limit: int = 50,
        offset: int = 0,
        lp_type: Optional[str] = None,
        jurisdiction: Optional[str] = None,
        include_portfolio: bool = False,
        portfolio_limit: int = 10,
    ) -> List[LPFundType]:
        db = info.context["db"]
        return resolve_lp_funds(
            db, limit, offset, lp_type, jurisdiction,
            include_portfolio, portfolio_limit
        )

    @strawberry.field(description="Get a single family office by ID")
    def family_office(
        self,
        info: strawberry.Info,
        id: int,
        include_portfolio: bool = True,
        portfolio_limit: int = 50,
        include_coinvestors: bool = True,
        coinvestor_limit: int = 50,
    ) -> Optional[FamilyOfficeType]:
        db = info.context["db"]
        return resolve_family_office(
            db, id, include_portfolio, portfolio_limit,
            include_coinvestors, coinvestor_limit
        )

    @strawberry.field(description="List family offices with optional filters")
    def family_offices(
        self,
        info: strawberry.Info,
        limit: int = 50,
        offset: int = 0,
        region: Optional[str] = None,
        country: Optional[str] = None,
        type: Optional[str] = None,
        include_portfolio: bool = False,
        portfolio_limit: int = 10,
    ) -> List[FamilyOfficeType]:
        db = info.context["db"]
        return resolve_family_offices(
            db, limit, offset, region, country, type,
            include_portfolio, portfolio_limit
        )

    @strawberry.field(description="Get a single portfolio company by ID")
    def portfolio_company(
        self,
        info: strawberry.Info,
        id: int,
    ) -> Optional[PortfolioCompanyType]:
        db = info.context["db"]
        return resolve_portfolio_company(db, id)

    @strawberry.field(description="List portfolio companies with optional filters")
    def portfolio_companies(
        self,
        info: strawberry.Info,
        investor_id: Optional[int] = None,
        investor_type: Optional[str] = None,
        industry: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[PortfolioCompanyType]:
        db = info.context["db"]
        return resolve_portfolio_companies(
            db, investor_id, investor_type, industry, limit, offset
        )

    @strawberry.field(description="Search investors and companies")
    def search(
        self,
        info: strawberry.Info,
        query: str,
        type: Optional[str] = None,
        limit: int = 20,
    ) -> List[SearchResultType]:
        db = info.context["db"]
        return resolve_search(db, query, type, limit)

    @strawberry.field(description="Get system-wide analytics overview")
    def analytics_overview(
        self,
        info: strawberry.Info,
    ) -> AnalyticsOverviewType:
        db = info.context["db"]
        return resolve_analytics_overview(db)

    @strawberry.field(description="Get industry distribution of portfolio companies")
    def industry_breakdown(
        self,
        info: strawberry.Info,
        investor_id: Optional[int] = None,
        investor_type: Optional[str] = None,
        limit: int = 20,
    ) -> List[IndustryBreakdownType]:
        db = info.context["db"]
        return resolve_industry_breakdown(db, investor_id, investor_type, limit)

    @strawberry.field(description="Get recent portfolio changes")
    def top_movers(
        self,
        info: strawberry.Info,
        days: int = 30,
        limit: int = 20,
    ) -> List[TopMoverType]:
        db = info.context["db"]
        return resolve_top_movers(db, days, limit)


# Create the schema
schema = strawberry.Schema(query=Query)

# Create the FastAPI GraphQL app
graphql_app = GraphQLRouter(
    schema,
    context_getter=get_context,
)
