"""
Co-investor Network Graph API endpoints.

Provides network analysis for investor relationships.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.network.graph import NetworkEngine


router = APIRouter(prefix="/network", tags=["network"])


# Response Models


class NodeResponse(BaseModel):
    """Network node (investor)."""

    id: str
    investor_id: Optional[int] = None
    type: str
    name: str
    subtype: Optional[str] = None
    location: Optional[str] = None
    degree: int = 0
    weighted_degree: int = 0
    centrality: float = 0.0
    cluster_id: Optional[int] = None
    is_center: Optional[bool] = None


class EdgeResponse(BaseModel):
    """Network edge (co-investment relationship)."""

    source: str
    target: str
    weight: int
    shared_companies: List[str] = []
    first_date: Optional[str] = None
    last_date: Optional[str] = None


class NetworkStatsResponse(BaseModel):
    """Network statistics."""

    total_nodes: int
    total_edges: int
    total_weight: Optional[int] = None
    avg_degree: float = 0.0
    density: float = 0.0
    direct_connections: Optional[int] = None


class NetworkGraphResponse(BaseModel):
    """Full network graph response."""

    nodes: List[NodeResponse]
    edges: List[EdgeResponse]
    stats: NetworkStatsResponse


class InvestorNetworkResponse(BaseModel):
    """Investor ego network response."""

    center: Optional[NodeResponse] = None
    nodes: List[NodeResponse]
    edges: List[EdgeResponse]
    stats: NetworkStatsResponse


class ClusterMemberResponse(BaseModel):
    """Cluster member."""

    id: str
    name: str
    type: str


class ClusterResponse(BaseModel):
    """Investor cluster."""

    id: int
    size: int
    members: List[ClusterMemberResponse]
    avg_degree: float = 0.0


class PathResponse(BaseModel):
    """Path between two investors."""

    found: bool
    path_length: int
    path: List[NodeResponse]
    edges: List[EdgeResponse]


# Endpoints


@router.get(
    "/graph",
    response_model=NetworkGraphResponse,
    summary="Get full co-investor network",
    description="""
    Returns the complete co-investor network for visualization.

    The network is built from:
    - Direct co-investment records
    - Shared portfolio companies between investors

    Use `limit` to control the number of edges returned (sorted by weight).
    Use `min_weight` to filter out weak connections.
    """,
)
def get_network_graph(
    limit: Optional[int] = Query(
        None, ge=1, le=1000, description="Max edges to return"
    ),
    min_weight: int = Query(1, ge=1, description="Minimum relationship weight"),
    include_external: bool = Query(
        False, description="Include external (non-database) investors"
    ),
    db: Session = Depends(get_db),
):
    """Get full network graph for visualization."""
    engine = NetworkEngine(db)
    result = engine.get_network_graph(
        limit=limit,
        min_weight=min_weight,
        include_external=include_external,
    )
    return result


@router.get(
    "/investor/{investor_id}",
    response_model=InvestorNetworkResponse,
    summary="Get investor's co-investor network",
    description="""
    Returns the ego network for a specific investor showing their co-investors.

    Use `depth` to control how many hops from the center investor:
    - depth=1: Direct co-investors only
    - depth=2: Co-investors and their co-investors
    """,
)
def get_investor_network(
    investor_id: int,
    investor_type: str = Query(
        ..., pattern="^(lp|family_office)$", description="Investor type"
    ),
    depth: int = Query(1, ge=1, le=3, description="Network depth (hops from investor)"),
    min_weight: int = Query(1, ge=1, description="Minimum relationship weight"),
    db: Session = Depends(get_db),
):
    """Get ego network for specific investor."""
    engine = NetworkEngine(db)
    result = engine.get_investor_network(
        investor_id=investor_id,
        investor_type=investor_type,
        depth=depth,
        min_weight=min_weight,
    )

    if not result["nodes"]:
        raise HTTPException(
            status_code=404,
            detail=f"Investor {investor_type}_{investor_id} not found or has no connections",
        )

    return result


@router.get(
    "/central",
    response_model=List[NodeResponse],
    summary="Get most connected investors",
    description="""
    Returns investors ranked by their network centrality (connection strength).

    Centrality is based on:
    - Number of co-investor relationships
    - Total weight of those relationships (# shared investments)
    """,
)
def get_central_investors(
    limit: int = Query(20, ge=1, le=100, description="Number of investors to return"),
    db: Session = Depends(get_db),
):
    """Get most central/connected investors."""
    engine = NetworkEngine(db)
    return engine.get_central_investors(limit=limit)


@router.get(
    "/clusters",
    response_model=List[ClusterResponse],
    summary="Get investor clusters",
    description="""
    Detects and returns investor clusters based on co-investment relationships.

    A cluster is a group of investors who are connected through shared investments.
    Investors with no connections are not included in clusters.
    """,
)
def get_clusters(
    min_cluster_size: int = Query(2, ge=2, le=50, description="Minimum cluster size"),
    db: Session = Depends(get_db),
):
    """Get detected investor clusters."""
    engine = NetworkEngine(db)
    return engine.detect_clusters(min_cluster_size=min_cluster_size)


@router.get(
    "/path",
    response_model=PathResponse,
    summary="Find path between two investors",
    description="""
    Finds the shortest co-investment path connecting two investors.

    Returns the sequence of investors and their shared investments
    that connect the source to the target.

    If no path exists, returns `found: false`.
    """,
)
def find_path(
    source_id: int = Query(..., description="Source investor ID"),
    source_type: str = Query(
        ..., pattern="^(lp|family_office)$", description="Source investor type"
    ),
    target_id: int = Query(..., description="Target investor ID"),
    target_type: str = Query(
        ..., pattern="^(lp|family_office)$", description="Target investor type"
    ),
    db: Session = Depends(get_db),
):
    """Find shortest path between two investors."""
    engine = NetworkEngine(db)
    result = engine.find_path(
        source_id=source_id,
        source_type=source_type,
        target_id=target_id,
        target_type=target_type,
    )

    if result is None:
        raise HTTPException(status_code=404, detail="One or both investors not found")

    return result
