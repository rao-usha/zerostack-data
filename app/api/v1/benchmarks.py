"""
Market Benchmarks API endpoints.

T29: Compare LP performance and allocations against market benchmarks.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.analytics.benchmarks import BenchmarkService


router = APIRouter(prefix="/benchmarks", tags=["benchmarks"])


# Response Models

class PeerInfo(BaseModel):
    """Peer group summary."""
    type: str
    peer_count: int


class SectorComparison(BaseModel):
    """Sector comparison to benchmark."""
    sector: str
    investor_allocation: float
    benchmark_median: float
    benchmark_p25: float
    benchmark_p75: float
    variance: str
    position: str


class DiversificationSummary(BaseModel):
    """Diversification score summary."""
    investor_score: float
    hhi: float
    sector_count: int


class InvestorBenchmarkResponse(BaseModel):
    """Investor vs benchmark comparison."""
    investor_id: int
    investor_name: Optional[str] = None
    investor_type: str
    peer_group: PeerInfo
    sector_comparison: List[SectorComparison]
    diversification: DiversificationSummary


class PeerInvestor(BaseModel):
    """Peer investor info."""
    id: int
    name: str
    subtype: Optional[str] = None
    location: Optional[str] = None


class PeerGroupResponse(BaseModel):
    """Detailed peer group information."""
    investor_id: int
    investor_name: Optional[str] = None
    investor_type: str
    subtype: Optional[str] = None
    peer_count: int
    peers: List[PeerInvestor]


class SectorBenchmark(BaseModel):
    """Sector allocation benchmark."""
    sector: str
    p25: float
    median: float
    p75: float
    sample_size: int


class TypeBenchmark(BaseModel):
    """Benchmarks for an investor type."""
    investor_type: str
    sample_size: int
    sector_allocations: List[SectorBenchmark]


class OverallMarket(BaseModel):
    """Overall market benchmarks."""
    lp_sample_size: int
    family_office_sample_size: int
    lp_sectors: List[SectorBenchmark]
    family_office_sectors: List[SectorBenchmark]


class SectorBenchmarksResponse(BaseModel):
    """All sector benchmarks."""
    benchmarks_by_type: List[TypeBenchmark]
    overall_market: OverallMarket


class DiversificationRanking(BaseModel):
    """Single diversification ranking entry."""
    rank: int
    investor_id: int
    investor_name: str
    investor_type: str
    subtype: Optional[str] = None
    diversification_score: float
    sector_count: int
    holding_count: int
    hhi: float


class ScoreDistribution(BaseModel):
    """Score distribution statistics."""
    mean: float
    median: float
    min: float
    max: float
    std_dev: Optional[float] = None


class DiversificationRankingsResponse(BaseModel):
    """Diversification rankings."""
    rankings: List[DiversificationRanking]
    total_investors: int
    score_distribution: ScoreDistribution


# Endpoints

@router.get(
    "/investor/{investor_id}",
    response_model=InvestorBenchmarkResponse,
    summary="Get investor vs benchmark comparison",
    description="""
    Compare an investor's portfolio allocations against their peer benchmark.

    Returns:
    - Peer group information (type, size)
    - Sector-by-sector comparison with P25/median/P75
    - Diversification score vs peers
    """,
)
def get_investor_benchmark(
    investor_id: int,
    investor_type: str = Query(..., regex="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Compare investor to their peer benchmark."""
    service = BenchmarkService(db)
    result = service.compare_to_benchmark(investor_id, investor_type)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get(
    "/peer-group/{investor_id}",
    response_model=PeerGroupResponse,
    summary="Get investor peer group",
    description="""
    Get the peer group for an investor.

    Peers are determined by:
    - Same investor type (LP subtype or family office type)
    - Similar AUM size bucket (when available)
    """,
)
def get_peer_group(
    investor_id: int,
    investor_type: str = Query(..., regex="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Get peer group for an investor."""
    service = BenchmarkService(db)
    result = service.get_peer_group(investor_id, investor_type)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get(
    "/sectors",
    response_model=SectorBenchmarksResponse,
    summary="Get sector allocation benchmarks",
    description="""
    Get sector allocation benchmarks by investor type.

    Returns median, P25, and P75 allocations for each sector,
    broken down by investor type (LP subtypes and family offices).
    """,
)
def get_sector_benchmarks(
    db: Session = Depends(get_db),
):
    """Get sector allocation benchmarks."""
    service = BenchmarkService(db)
    return service.get_all_sector_benchmarks()


@router.get(
    "/diversification",
    response_model=DiversificationRankingsResponse,
    summary="Get diversification rankings",
    description="""
    Get diversification score rankings for all investors.

    Diversification score (0-100) is based on:
    - HHI (Herfindahl-Hirschman Index) for concentration
    - Number of unique sectors
    - Portfolio size

    Higher score = more diversified portfolio.
    """,
)
def get_diversification_rankings(
    investor_type: Optional[str] = Query(None, regex="^(lp|family_office)$"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get diversification score rankings."""
    service = BenchmarkService(db)
    return service.get_diversification_rankings(investor_type, limit)
