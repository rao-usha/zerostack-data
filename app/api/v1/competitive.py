"""
Competitive Intelligence API endpoints.

Provides endpoints for:
- Competitive landscape analysis
- Company comparison
- Movement tracking
- Moat assessment
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.agents.competitive_intel import CompetitiveIntelAgent

router = APIRouter(prefix="/competitive", tags=["Competitive Intelligence"])


# ============================================================================
# Request/Response Models
# ============================================================================

class AnalyzeRequest(BaseModel):
    """Request to start competitive analysis."""
    company_name: str = Field(..., description="Company to analyze")
    max_competitors: int = Field(10, ge=1, le=25, description="Max competitors to find")
    include_movements: bool = Field(True, description="Include recent movements")


class CompareRequest(BaseModel):
    """Request to compare specific companies."""
    companies: List[str] = Field(..., min_length=2, max_length=10, description="Companies to compare")


class CompetitorItem(BaseModel):
    """Competitor info in response."""
    name: str
    similarity_score: float
    relationship: str
    strengths: List[str]
    weaknesses: List[str]


class MoatScores(BaseModel):
    """Moat assessment scores."""
    network_effects: float
    switching_costs: float
    brand: float
    cost_advantages: float
    technology: float


class MoatAssessment(BaseModel):
    """Moat assessment response."""
    overall_moat: str
    overall_score: float
    scores: MoatScores
    summary: str


class MovementItem(BaseModel):
    """Competitive movement item."""
    company: str
    type: str
    description: str
    impact_score: float
    detected_at: Optional[str]


class MovementSummary(BaseModel):
    """Summary of competitor movements."""
    funding_count: int
    hires_announced: int
    products_launched: int
    partnerships: int


class LandscapeResponse(BaseModel):
    """Full competitive landscape response."""
    company: str
    sector: Optional[str]
    market_position: str
    competitors: List[dict]
    comparison_matrix: dict
    moat_assessment: dict
    confidence: float
    data_sources: List[str]
    analyzed_at: str
    cached: Optional[bool] = None


class MovementsResponse(BaseModel):
    """Competitive movements response."""
    company: str
    movements: List[MovementItem]
    summary: MovementSummary


class CompareResponse(BaseModel):
    """Company comparison response."""
    companies: List[str]
    comparison_matrix: dict
    rankings: dict


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/analyze", response_model=LandscapeResponse)
def analyze_competitive_landscape(
    request: AnalyzeRequest,
    db: Session = Depends(get_db)
):
    """
    Start competitive analysis for a company.

    Identifies competitors, builds comparison matrix, assesses competitive moat,
    and optionally tracks recent movements.
    """
    agent = CompetitiveIntelAgent(db)

    # Check cache first
    cached = agent.get_cached_analysis(request.company_name)
    if cached:
        return LandscapeResponse(**cached)

    # Run analysis
    try:
        result = agent.analyze(
            company_name=request.company_name,
            max_competitors=request.max_competitors,
            include_movements=request.include_movements
        )
        return LandscapeResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/{company}", response_model=LandscapeResponse)
def get_competitive_landscape(
    company: str,
    force_refresh: bool = Query(False, description="Force re-analysis"),
    db: Session = Depends(get_db)
):
    """
    Get competitive landscape for a company.

    Returns cached analysis if available, otherwise runs new analysis.
    """
    agent = CompetitiveIntelAgent(db)

    # Check cache unless force refresh
    if not force_refresh:
        cached = agent.get_cached_analysis(company)
        if cached:
            return LandscapeResponse(**cached)

    # Run analysis
    try:
        result = agent.analyze(
            company_name=company,
            max_competitors=10,
            include_movements=True
        )
        return LandscapeResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/{company}/movements", response_model=MovementsResponse)
def get_competitive_movements(
    company: str,
    days: int = Query(30, ge=1, le=365, description="Time range in days"),
    include_competitors: bool = Query(True, description="Include competitor movements"),
    db: Session = Depends(get_db)
):
    """
    Get recent competitive movements.

    Tracks funding, hires, product launches, partnerships, etc.
    for the company and optionally its competitors.
    """
    agent = CompetitiveIntelAgent(db)

    # Get company movements
    movements = agent.detect_movements(company, days)

    # Optionally get competitor movements
    summary = MovementSummary(
        funding_count=0,
        hires_announced=0,
        products_launched=0,
        partnerships=0
    )

    if include_competitors:
        # Get competitors from cache or discover
        cached = agent.get_cached_analysis(company)
        competitor_names = []
        if cached and cached.get("competitors"):
            competitor_names = [c.get("name") for c in cached["competitors"][:5] if c.get("name")]

        if competitor_names:
            all_companies = [company] + competitor_names
            result = agent.track_competitor_movements(all_companies, days)
            movements = [
                MovementItem(
                    company=m["company"],
                    type=m["type"],
                    description=m["description"],
                    impact_score=m.get("impact_score", 0.5),
                    detected_at=m.get("detected_at")
                )
                for m in result["movements"]
            ]
            summary = MovementSummary(**result["summary"])
    else:
        movements = [
            MovementItem(
                company=m["company"],
                type=m["type"],
                description=m["description"],
                impact_score=m.get("impact_score", 0.5),
                detected_at=m.get("detected_at")
            )
            for m in movements
        ]
        # Count types
        for m in movements:
            if m.type == "funding":
                summary.funding_count += 1
            elif m.type == "hiring":
                summary.hires_announced += 1
            elif m.type == "product":
                summary.products_launched += 1
            elif m.type == "partnership":
                summary.partnerships += 1

    return MovementsResponse(
        company=company,
        movements=movements,
        summary=summary
    )


@router.post("/compare", response_model=CompareResponse)
def compare_companies(
    request: CompareRequest,
    db: Session = Depends(get_db)
):
    """
    Compare specific companies directly.

    Builds a comparison matrix and rankings for the specified companies.
    """
    agent = CompetitiveIntelAgent(db)

    try:
        result = agent.compare_companies(request.companies)
        return CompareResponse(
            companies=result["companies"],
            comparison_matrix=result["comparison_matrix"],
            rankings=result.get("rankings", {})
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")


@router.get("/{company}/moat")
def get_moat_assessment(
    company: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed moat assessment for a company.

    Analyzes competitive advantages across five categories:
    - Network effects
    - Switching costs
    - Brand recognition
    - Cost advantages
    - Technology leadership
    """
    agent = CompetitiveIntelAgent(db)

    # First get competitors
    cached = agent.get_cached_analysis(company)
    competitor_names = []
    if cached and cached.get("competitors"):
        competitor_names = [c.get("name") for c in cached["competitors"][:5] if c.get("name")]

    # Assess moat
    moat = agent.assess_moat(company, competitor_names)

    return {
        "company": company,
        "moat_assessment": moat,
    }


@router.get("/{company}/competitors")
def get_competitors(
    company: str,
    max_results: int = Query(10, ge=1, le=25),
    db: Session = Depends(get_db)
):
    """
    Find competitors for a company.

    Uses multiple signals: sector, employee size, funding stage,
    shared investors, and tech stack similarity.
    """
    agent = CompetitiveIntelAgent(db)

    competitors = agent.find_competitors(company, max_results)

    return {
        "company": company,
        "competitors": competitors,
        "count": len(competitors),
    }
