"""
Investor Similarity & Recommendations API (T18).

Endpoints for finding similar investors and getting company recommendations
based on portfolio overlap using Jaccard similarity.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db

# Import directly to avoid circular/missing dependency issues with analytics.__init__
from app.analytics.recommendations import RecommendationEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discover", tags=["Discovery & Recommendations"])


# =============================================================================
# Response Models
# =============================================================================


class SimilarInvestorResponse(BaseModel):
    """A similar investor with similarity metrics."""

    investor_id: int = Field(..., description="Investor ID")
    investor_type: str = Field(
        ..., description="Type: public_pension, sovereign_wealth, etc."
    )
    name: str = Field(..., description="Investor name")
    similarity_score: float = Field(..., description="Jaccard similarity (0-1)")
    similarity_percentage: float = Field(
        ..., description="Similarity as percentage (0-100)"
    )
    overlap_count: int = Field(..., description="Number of shared holdings")
    sample_overlap: List[str] = Field(
        default_factory=list, description="Sample of shared companies"
    )


class SimilarInvestorsResponse(BaseModel):
    """Response for similar investors query."""

    target_investor_id: int
    target_investor_name: str
    similar_investors: List[SimilarInvestorResponse]
    total_found: int


class CompanyRecommendationResponse(BaseModel):
    """A recommended company."""

    company_name: str = Field(..., description="Company name")
    industry: Optional[str] = Field(None, description="Company industry")
    held_by_similar_count: int = Field(
        ..., description="How many similar investors hold this"
    )
    held_by_investors: List[str] = Field(
        default_factory=list, description="Names of investors who hold this"
    )
    confidence_score: float = Field(
        ..., description="Confidence (0-1) based on similar investor coverage"
    )


class RecommendationsResponse(BaseModel):
    """Response for company recommendations."""

    target_investor_id: int
    target_investor_name: str
    recommendations: List[CompanyRecommendationResponse]
    based_on_similar_count: int = Field(
        ..., description="Number of similar investors used for recommendations"
    )


class InvestorSummary(BaseModel):
    """Summary of an investor for overlap comparison."""

    id: int
    name: str
    total_holdings: int


class OverlapResponse(BaseModel):
    """Portfolio overlap analysis between two investors."""

    investor_a: InvestorSummary
    investor_b: InvestorSummary
    overlap_count: int = Field(..., description="Number of shared holdings")
    overlap_percentage_a: float = Field(
        ..., description="% of investor A's portfolio that overlaps"
    )
    overlap_percentage_b: float = Field(
        ..., description="% of investor B's portfolio that overlaps"
    )
    jaccard_similarity: float = Field(..., description="Jaccard index (0-1)")
    jaccard_percentage: float = Field(..., description="Jaccard as percentage (0-100)")
    shared_companies: List[str] = Field(
        default_factory=list, description="List of shared companies"
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/similar/{investor_id}", response_model=SimilarInvestorsResponse)
async def get_similar_investors(
    investor_id: int,
    investor_type: Optional[str] = Query(
        None, description="Filter by type: public_pension, sovereign_wealth, endowment"
    ),
    limit: int = Query(10, ge=1, le=50, description="Maximum results to return"),
    min_overlap: int = Query(1, ge=1, description="Minimum shared holdings required"),
    db: Session = Depends(get_db),
):
    """
    Find investors with similar portfolios based on Jaccard similarity.

    **How it works:**
    - Calculates overlap between target investor's portfolio and all others
    - Uses Jaccard index: J(A,B) = |A ∩ B| / |A ∪ B|
    - Higher score = more similar portfolios

    **Examples:**
    - `/discover/similar/1` - Find investors similar to CalPERS
    - `/discover/similar/4?investor_type=public_pension` - Similar pension funds to STRS Ohio
    - `/discover/similar/1?limit=5&min_overlap=10` - Top 5 with at least 10 shared holdings
    """
    engine = RecommendationEngine(db)

    # Validate investor exists
    investor_info = engine.get_investor_info(investor_id)
    if not investor_info:
        raise HTTPException(status_code=404, detail=f"Investor {investor_id} not found")

    # Validate investor_type if provided
    if investor_type:
        valid_types = {
            "public_pension",
            "sovereign_wealth",
            "endowment",
            "family_office",
        }
        if investor_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid investor_type. Valid types: {valid_types}",
            )

    try:
        similar = engine.get_similar_investors(
            investor_id=investor_id,
            investor_type=investor_type,
            limit=limit,
            min_overlap=min_overlap,
        )

        results = [
            SimilarInvestorResponse(
                investor_id=s.investor_id,
                investor_type=s.investor_type,
                name=s.name,
                similarity_score=round(s.similarity_score, 4),
                similarity_percentage=round(s.similarity_score * 100, 2),
                overlap_count=s.overlap_count,
                sample_overlap=s.overlap_companies,
            )
            for s in similar
        ]

        return SimilarInvestorsResponse(
            target_investor_id=investor_id,
            target_investor_name=investor_info["name"],
            similar_investors=results,
            total_found=len(results),
        )

    except Exception as e:
        logger.error(f"Error finding similar investors: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to find similar investors: {str(e)}"
        )


@router.get("/recommended/{investor_id}", response_model=RecommendationsResponse)
async def get_recommended_companies(
    investor_id: int,
    similar_count: int = Query(
        10, ge=1, le=50, description="Number of similar investors to consider"
    ),
    limit: int = Query(
        20, ge=1, le=100, description="Maximum recommendations to return"
    ),
    db: Session = Depends(get_db),
):
    """
    Get company recommendations based on what similar investors hold.

    **How it works:**
    - Finds the most similar investors to the target
    - Identifies companies they hold that the target doesn't
    - Ranks by popularity among similar investors

    **Interpretation:**
    - "Investors like X also invest in Y"
    - Higher confidence = more similar investors hold the company

    **Examples:**
    - `/discover/recommended/1` - Recommendations for CalPERS
    - `/discover/recommended/4?similar_count=20&limit=50` - More comprehensive recommendations
    """
    engine = RecommendationEngine(db)

    # Validate investor exists
    investor_info = engine.get_investor_info(investor_id)
    if not investor_info:
        raise HTTPException(status_code=404, detail=f"Investor {investor_id} not found")

    try:
        recommendations = engine.get_recommended_companies(
            investor_id=investor_id, similar_count=similar_count, limit=limit
        )

        results = [
            CompanyRecommendationResponse(
                company_name=r.company_name,
                industry=r.company_industry,
                held_by_similar_count=r.held_by_count,
                held_by_investors=r.held_by_names,
                confidence_score=round(r.confidence, 4),
            )
            for r in recommendations
        ]

        return RecommendationsResponse(
            target_investor_id=investor_id,
            target_investor_name=investor_info["name"],
            recommendations=results,
            based_on_similar_count=similar_count,
        )

    except Exception as e:
        logger.error(f"Error getting recommendations: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get recommendations: {str(e)}"
        )


@router.get("/overlap", response_model=OverlapResponse)
async def get_portfolio_overlap(
    investor_a: int = Query(..., description="First investor ID"),
    investor_b: int = Query(..., description="Second investor ID"),
    db: Session = Depends(get_db),
):
    """
    Analyze portfolio overlap between two investors.

    **Metrics returned:**
    - **overlap_count**: Number of companies both hold
    - **overlap_percentage_a/b**: What % of each portfolio overlaps
    - **jaccard_similarity**: Overall similarity score (0-1)

    **Examples:**
    - `/discover/overlap?investor_a=1&investor_b=4` - Compare CalPERS vs STRS Ohio
    """
    if investor_a == investor_b:
        raise HTTPException(
            status_code=400, detail="Cannot compare an investor to itself"
        )

    engine = RecommendationEngine(db)

    try:
        overlap = engine.get_portfolio_overlap(investor_a, investor_b)

        if not overlap:
            raise HTTPException(
                status_code=404,
                detail=f"One or both investors not found: {investor_a}, {investor_b}",
            )

        return OverlapResponse(
            investor_a=InvestorSummary(
                id=overlap.investor_a_id,
                name=overlap.investor_a_name,
                total_holdings=overlap.investor_a_holdings,
            ),
            investor_b=InvestorSummary(
                id=overlap.investor_b_id,
                name=overlap.investor_b_name,
                total_holdings=overlap.investor_b_holdings,
            ),
            overlap_count=overlap.overlap_count,
            overlap_percentage_a=overlap.overlap_percentage_a,
            overlap_percentage_b=overlap.overlap_percentage_b,
            jaccard_similarity=overlap.jaccard_similarity,
            jaccard_percentage=round(overlap.jaccard_similarity * 100, 2),
            shared_companies=overlap.shared_companies,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculating overlap: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to calculate overlap: {str(e)}"
        )
