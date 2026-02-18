"""
Portfolio Comparison API (T17).

Endpoints for comparing investor portfolios side-by-side,
tracking historical changes, and analyzing industry allocations.
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.analytics.comparison import PortfolioComparisonService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/compare", tags=["Portfolio Comparison"])


# =============================================================================
# Request Models
# =============================================================================


class PortfolioCompareRequest(BaseModel):
    """Request to compare two portfolios."""

    investor_a: int = Field(..., gt=0, description="First investor ID")
    investor_b: int = Field(..., gt=0, description="Second investor ID")
    top_holdings: int = Field(
        10, ge=1, le=50, description="Number of top holdings to include"
    )


# =============================================================================
# Response Models
# =============================================================================


class HoldingResponse(BaseModel):
    """A portfolio holding."""

    company_id: int
    company_name: str
    industry: Optional[str]
    market_value_usd: Optional[str]
    shares_held: Optional[str]


class InvestorResponse(BaseModel):
    """Investor summary."""

    id: int
    name: str
    investor_type: str
    total_holdings: int


class IndustryAllocationResponse(BaseModel):
    """Industry allocation comparison."""

    industry: str
    count_a: int
    count_b: int
    percentage_a: float
    percentage_b: float


class PortfolioComparisonResponse(BaseModel):
    """Complete portfolio comparison response."""

    investor_a: InvestorResponse
    investor_b: InvestorResponse

    # Overlap metrics
    overlap_count: int = Field(..., description="Number of shared holdings")
    overlap_percentage_a: float = Field(
        ..., description="% of A's portfolio that overlaps"
    )
    overlap_percentage_b: float = Field(
        ..., description="% of B's portfolio that overlaps"
    )
    jaccard_similarity: float = Field(..., description="Jaccard similarity index (0-1)")
    jaccard_percentage: float = Field(..., description="Jaccard as percentage (0-100)")

    # Holdings
    shared_holdings: List[HoldingResponse] = Field(
        ..., description="Companies held by both"
    )
    unique_to_a: List[HoldingResponse] = Field(..., description="Companies only in A")
    unique_to_b: List[HoldingResponse] = Field(..., description="Companies only in B")

    # Top holdings
    top_holdings_a: List[HoldingResponse] = Field(..., description="Top holdings for A")
    top_holdings_b: List[HoldingResponse] = Field(..., description="Top holdings for B")

    # Industry comparison
    industry_comparison: List[IndustryAllocationResponse]

    # Metadata
    comparison_date: str


class HistoricalDiffResponse(BaseModel):
    """Historical portfolio diff response."""

    investor_id: int
    investor_name: str
    period_start: str
    period_end: str

    # Changes
    additions: List[HoldingResponse] = Field(..., description="New holdings added")
    removals: List[HoldingResponse] = Field(..., description="Holdings removed/exited")
    unchanged_count: int = Field(..., description="Count of unchanged holdings")

    # Summary
    holdings_start: int = Field(..., description="Holdings count at start")
    holdings_end: int = Field(..., description="Holdings count at end")
    net_change: int = Field(..., description="Net change in holdings count")


class IndustryComparisonResponse(BaseModel):
    """Industry-only comparison response."""

    investor_a_id: int
    investor_a_name: str
    investor_b_id: int
    investor_b_name: str
    industry_breakdown: List[IndustryAllocationResponse]


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/portfolios", response_model=PortfolioComparisonResponse)
async def compare_portfolios(
    request: PortfolioCompareRequest, db: Session = Depends(get_db)
):
    """
    Compare two investors' portfolios side-by-side.

    **Returns:**
    - **Overlap metrics**: Jaccard similarity, overlap counts and percentages
    - **Shared holdings**: Companies held by both investors
    - **Unique holdings**: Companies exclusive to each investor
    - **Top holdings**: Largest positions for each investor
    - **Industry comparison**: Sector allocation breakdown

    **Example:**
    ```json
    POST /compare/portfolios
    {
      "investor_a": 1,
      "investor_b": 4,
      "top_holdings": 10
    }
    ```

    **Use Cases:**
    - Due diligence: Compare prospect vs benchmark
    - Competitive analysis: Compare two similar investors
    - Co-investment identification: Find shared holdings
    """
    if request.investor_a == request.investor_b:
        raise HTTPException(
            status_code=400, detail="Cannot compare an investor to itself"
        )

    service = PortfolioComparisonService(db)

    # Validate investors exist
    investor_a = service.get_investor_info(request.investor_a)
    if not investor_a:
        raise HTTPException(
            status_code=404, detail=f"Investor {request.investor_a} not found"
        )

    investor_b = service.get_investor_info(request.investor_b)
    if not investor_b:
        raise HTTPException(
            status_code=404, detail=f"Investor {request.investor_b} not found"
        )

    try:
        comparison = service.compare_portfolios(
            investor_a_id=request.investor_a,
            investor_b_id=request.investor_b,
            top_holdings=request.top_holdings,
        )

        if not comparison:
            raise HTTPException(status_code=500, detail="Failed to generate comparison")

        return PortfolioComparisonResponse(
            investor_a=InvestorResponse(
                id=comparison.investor_a.id,
                name=comparison.investor_a.name,
                investor_type=comparison.investor_a.investor_type,
                total_holdings=comparison.investor_a.total_holdings,
            ),
            investor_b=InvestorResponse(
                id=comparison.investor_b.id,
                name=comparison.investor_b.name,
                investor_type=comparison.investor_b.investor_type,
                total_holdings=comparison.investor_b.total_holdings,
            ),
            overlap_count=comparison.overlap_count,
            overlap_percentage_a=comparison.overlap_percentage_a,
            overlap_percentage_b=comparison.overlap_percentage_b,
            jaccard_similarity=comparison.jaccard_similarity,
            jaccard_percentage=comparison.jaccard_percentage,
            shared_holdings=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in comparison.shared_holdings
            ],
            unique_to_a=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in comparison.unique_to_a
            ],
            unique_to_b=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in comparison.unique_to_b
            ],
            top_holdings_a=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in comparison.top_holdings_a
            ],
            top_holdings_b=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in comparison.top_holdings_b
            ],
            industry_comparison=[
                IndustryAllocationResponse(
                    industry=i.industry,
                    count_a=i.count_a,
                    count_b=i.count_b,
                    percentage_a=i.percentage_a,
                    percentage_b=i.percentage_b,
                )
                for i in comparison.industry_comparison
            ],
            comparison_date=comparison.comparison_date,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing portfolios: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to compare portfolios: {str(e)}"
        )


@router.get("/investor/{investor_id}/history", response_model=HistoricalDiffResponse)
async def compare_investor_history(
    investor_id: int,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """
    Compare an investor's portfolio over time.

    Shows additions (new holdings), removals (exits), and unchanged positions
    between two points in time.

    **Parameters:**
    - **start_date**: Beginning of comparison period (defaults to earliest data)
    - **end_date**: End of comparison period (defaults to latest data)

    **Returns:**
    - **additions**: New companies added to portfolio
    - **removals**: Companies exited from portfolio
    - **unchanged_count**: Number of companies held throughout
    - **net_change**: Overall change in portfolio size

    **Example:**
    ```
    GET /compare/investor/4/history?start_date=2025-01-01&end_date=2025-12-31
    ```
    """
    service = PortfolioComparisonService(db)

    # Validate investor exists
    investor = service.get_investor_info(investor_id)
    if not investor:
        raise HTTPException(status_code=404, detail=f"Investor {investor_id} not found")

    # Parse dates
    parsed_start = None
    parsed_end = None

    if start_date:
        try:
            parsed_start = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD"
            )

    if end_date:
        try:
            parsed_end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD"
            )

    try:
        diff = service.compare_history(
            investor_id=investor_id, start_date=parsed_start, end_date=parsed_end
        )

        if not diff:
            raise HTTPException(
                status_code=500, detail="Failed to generate historical diff"
            )

        return HistoricalDiffResponse(
            investor_id=diff.investor_id,
            investor_name=diff.investor_name,
            period_start=diff.period_start,
            period_end=diff.period_end,
            additions=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in diff.additions
            ],
            removals=[
                HoldingResponse(
                    company_id=h.company_id,
                    company_name=h.company_name,
                    industry=h.industry,
                    market_value_usd=h.market_value_usd,
                    shares_held=h.shares_held,
                )
                for h in diff.removals
            ],
            unchanged_count=diff.unchanged_count,
            holdings_start=diff.holdings_start,
            holdings_end=diff.holdings_end,
            net_change=diff.net_change,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating historical diff: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate historical diff: {str(e)}"
        )


@router.get("/industry", response_model=IndustryComparisonResponse)
async def compare_industry_allocations(
    investor_a: int = Query(..., gt=0, description="First investor ID"),
    investor_b: int = Query(..., gt=0, description="Second investor ID"),
    db: Session = Depends(get_db),
):
    """
    Compare industry/sector allocations between two investors.

    Shows how each investor's portfolio is distributed across industries,
    making it easy to identify strategic differences.

    **Returns:**
    - Per-industry breakdown with counts and percentages for both investors
    - Industries sorted by total holdings (most common first)

    **Example:**
    ```
    GET /compare/industry?investor_a=1&investor_b=4
    ```
    """
    if investor_a == investor_b:
        raise HTTPException(
            status_code=400, detail="Cannot compare an investor to itself"
        )

    service = PortfolioComparisonService(db)

    # Validate investors exist
    inv_a = service.get_investor_info(investor_a)
    if not inv_a:
        raise HTTPException(status_code=404, detail=f"Investor {investor_a} not found")

    inv_b = service.get_investor_info(investor_b)
    if not inv_b:
        raise HTTPException(status_code=404, detail=f"Investor {investor_b} not found")

    try:
        comparison = service.get_industry_comparison(investor_a, investor_b)

        if comparison is None:
            raise HTTPException(
                status_code=500, detail="Failed to generate industry comparison"
            )

        return IndustryComparisonResponse(
            investor_a_id=investor_a,
            investor_a_name=inv_a.name,
            investor_b_id=investor_b,
            investor_b_name=inv_b.name,
            industry_breakdown=[
                IndustryAllocationResponse(
                    industry=i.industry,
                    count_a=i.count_a,
                    count_b=i.count_b,
                    percentage_a=i.percentage_a,
                    percentage_b=i.percentage_b,
                )
                for i in comparison
            ],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing industries: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to compare industries: {str(e)}"
        )
