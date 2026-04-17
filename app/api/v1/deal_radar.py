"""
Deal Radar — Convergence Intelligence API.

Endpoints for geographic signal convergence detection, regional scoring,
cluster identification, and AI-powered investment thesis generation.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.convergence_engine import (
    ConvergenceEngine,
    REGION_DEFINITIONS,
    REGION_CONNECTIONS,
    SIGNAL_TYPES,
)

router = APIRouter(prefix="/deal-radar", tags=["Deal Radar"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================


class RegionResponse(BaseModel):
    region_id: str
    label: str
    states: List[str] = []
    map_x: int = 0
    map_y: int = 0
    epa_score: float = 0
    irs_migration_score: float = 0
    trade_score: float = 0
    water_score: float = 0
    macro_score: float = 0
    convergence_score: float = 0
    convergence_grade: str = "F"
    cluster_status: str = "LOW"
    active_signals: List[str] = []
    signal_count: int = 0
    scored_at: Optional[str] = None


class ScanResponse(BaseModel):
    status: str
    regions_scanned: int
    hot_clusters: int
    active_clusters: int
    watch_clusters: int
    regions: List[Dict[str, Any]]


class SignalResponse(BaseModel):
    id: int
    region_id: str
    signal_type: str
    score: float
    description: Optional[str] = None
    detected_at: Optional[str] = None
    batch_id: Optional[str] = None


class StatsResponse(BaseModel):
    total_signals: int = 0
    active_clusters: int = 0
    new_24h: int = 0
    total_records: int = 0


class ThesisResponse(BaseModel):
    region_id: str
    thesis_text: str
    opportunity_score: float = 50
    urgency_score: float = 50
    risk_score: float = 50


class MetadataResponse(BaseModel):
    regions: Dict[str, Any]
    connections: List[List[str]]
    signal_types: Dict[str, Any]


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.post("/scan", response_model=ScanResponse)
def run_convergence_scan(db: Session = Depends(get_db)):
    """Run full convergence scan across all 13 US regions.

    Queries EPA, IRS, Trade, Water, and Macro data sources,
    computes per-signal scores (0-100), and identifies clusters.
    """
    engine = ConvergenceEngine(db)
    results = engine.scan_all_regions()

    hot = sum(1 for r in results if r.cluster_status == "HOT")
    active = sum(1 for r in results if r.cluster_status == "ACTIVE")
    watch = sum(1 for r in results if r.cluster_status == "WATCH")

    return ScanResponse(
        status="completed",
        regions_scanned=len(results),
        hot_clusters=hot,
        active_clusters=active,
        watch_clusters=watch,
        regions=[
            {
                "region_id": r.region_id,
                "label": r.label,
                "convergence_score": r.convergence_score,
                "cluster_status": r.cluster_status,
                "active_signals": r.active_signals,
            }
            for r in results
        ],
    )


@router.get("/regions", response_model=List[RegionResponse])
def get_regions(db: Session = Depends(get_db)):
    """Get all 13 regions with current convergence scores."""
    engine = ConvergenceEngine(db)
    regions = engine.get_all_regions()
    return regions


@router.get("/regions/{region_id}", response_model=RegionResponse)
def get_region(region_id: str, db: Session = Depends(get_db)):
    """Get detailed scores for a single region."""
    if region_id not in REGION_DEFINITIONS:
        raise HTTPException(status_code=404, detail=f"Region '{region_id}' not found")

    engine = ConvergenceEngine(db)
    all_regions = engine.get_all_regions()
    for r in all_regions:
        if r["region_id"] == region_id:
            return r

    # Region exists but hasn't been scored yet
    defn = REGION_DEFINITIONS[region_id]
    return RegionResponse(
        region_id=region_id,
        label=defn["label"],
        states=defn["states"],
        map_x=defn["map_x"],
        map_y=defn["map_y"],
    )


@router.get("/clusters")
def get_clusters(
    min_score: float = Query(44, description="Minimum convergence score"),
    db: Session = Depends(get_db),
):
    """Get active convergence clusters above threshold."""
    engine = ConvergenceEngine(db)
    return engine.get_clusters(min_score=min_score)


@router.get("/signals", response_model=List[SignalResponse])
def get_signals(
    limit: int = Query(20, ge=1, le=100, description="Number of signals to return"),
    db: Session = Depends(get_db),
):
    """Get recent signal events for the live feed."""
    engine = ConvergenceEngine(db)
    return engine.get_recent_signals(limit=limit)


@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Get dashboard stats: total signals, active clusters, new in last scan."""
    engine = ConvergenceEngine(db)
    return engine.get_stats()


@router.post("/thesis/{region_id}", response_model=ThesisResponse)
async def generate_thesis(region_id: str, db: Session = Depends(get_db)):
    """Generate AI-powered investment thesis for a region.

    Uses Claude to analyze convergent signals and produce a
    3-sentence investment thesis with opportunity/urgency/risk scores.
    Results are cached for subsequent requests.
    """
    if region_id not in REGION_DEFINITIONS:
        raise HTTPException(status_code=404, detail=f"Region '{region_id}' not found")

    engine = ConvergenceEngine(db)
    result = await engine.generate_thesis(region_id)

    if not result:
        raise HTTPException(status_code=404, detail="No scores available. Run a scan first.")

    return ThesisResponse(
        region_id=result.region_id,
        thesis_text=result.thesis_text,
        opportunity_score=result.opportunity_score,
        urgency_score=result.urgency_score,
        risk_score=result.risk_score,
    )


@router.get("/metadata", response_model=MetadataResponse)
def get_metadata():
    """Get static metadata: region definitions, connections, signal types.

    Used by the frontend to render the map without hardcoded data.
    """
    return MetadataResponse(
        regions={
            rid: {
                "label": defn["label"],
                "states": defn["states"],
                "map_x": defn["map_x"],
                "map_y": defn["map_y"],
            }
            for rid, defn in REGION_DEFINITIONS.items()
        },
        connections=REGION_CONNECTIONS,
        signal_types=SIGNAL_TYPES,
    )


# =============================================================================
# Dashboard Data (all sources combined)
# =============================================================================


@router.get("/dashboard")
def get_dashboard_data(db: Session = Depends(get_db)):
    """Get enriched dashboard data from all 11 data sources.

    Returns convergence regions plus aggregated stats from banks,
    hospitals, infrastructure grants, business formation, and industry data.
    """
    from sqlalchemy import text

    engine = ConvergenceEngine(db)
    regions = engine.get_all_regions()
    stats = engine.get_stats()
    signals = engine.get_recent_signals(limit=10)

    # Bank health by state
    banks = {}
    try:
        rows = db.execute(text(
            "SELECT state, COUNT(*) as cnt, "
            "COALESCE(AVG(total_assets),0) as avg_assets, "
            "COALESCE(AVG(return_on_assets),0) as avg_roa "
            "FROM ffiec_bank_calls WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY avg_assets DESC"
        )).mappings().fetchall()
        banks = {r["state"]: {"count": r["cnt"], "avg_assets": float(r["avg_assets"]), "avg_roa": float(r["avg_roa"])} for r in rows}
    except Exception:
        pass

    # Hospital stats by state
    hospitals = {}
    try:
        rows = db.execute(text(
            "SELECT state, COUNT(*) as cnt, "
            "COALESCE(AVG(overall_rating),0) as avg_rating "
            "FROM cms_hospitals WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY cnt DESC"
        )).mappings().fetchall()
        hospitals = {r["state"]: {"count": r["cnt"], "avg_rating": float(r["avg_rating"])} for r in rows}
    except Exception:
        pass

    # Infrastructure grants by state
    grants = {}
    try:
        rows = db.execute(text(
            "SELECT state, COALESCE(SUM(aggregated_amount),0) as total "
            "FROM dot_infra_grants GROUP BY state ORDER BY total DESC"
        )).mappings().fetchall()
        grants = {r["state"]: float(r["total"]) for r in rows}
    except Exception:
        pass

    # Business formation trend
    bfs_trend = []
    try:
        rows = db.execute(text(
            "SELECT time_period, business_applications, high_propensity_applications "
            "FROM census_bfs ORDER BY time_period"
        )).mappings().fetchall()
        bfs_trend = [dict(r) for r in rows]
    except Exception:
        pass

    # Top industries
    industries = []
    try:
        rows = db.execute(text(
            "SELECT naics_description, SUM(establishments) as est, SUM(employees) as emp "
            "FROM census_business_patterns "
            "WHERE naics_description IS NOT NULL AND naics_description != 'Total for all sectors' "
            "GROUP BY naics_description ORDER BY emp DESC LIMIT 10"
        )).mappings().fetchall()
        industries = [{"industry": r["naics_description"], "establishments": int(r["est"] or 0), "employees": int(r["emp"] or 0)} for r in rows]
    except Exception:
        pass

    # GHG emitters by state
    ghg = {}
    try:
        rows = db.execute(text(
            "SELECT state, COUNT(*) as facilities, "
            "COALESCE(SUM(total_reported_emissions),0) as total_emissions "
            "FROM epa_ghg_emissions WHERE state IS NOT NULL "
            "GROUP BY state ORDER BY total_emissions DESC"
        )).mappings().fetchall()
        ghg = {r["state"]: {"facilities": r["facilities"], "emissions": float(r["total_emissions"])} for r in rows}
    except Exception:
        pass

    # Record counts
    source_counts = {}
    for tbl in ["epa_echo_facilities", "irs_soi_migration", "us_trade_exports_state",
                 "public_water_system", "irs_soi_zip_income", "ffiec_bank_calls",
                 "cms_hospitals", "dot_infra_grants", "census_bfs",
                 "epa_ghg_emissions", "census_business_patterns"]:
        try:
            cnt = db.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            source_counts[tbl] = cnt
        except Exception:
            source_counts[tbl] = 0

    return {
        "regions": regions,
        "stats": stats,
        "signals": signals,
        "banks": banks,
        "hospitals": hospitals,
        "grants": grants,
        "bfs_trend": bfs_trend,
        "industries": industries,
        "ghg": ghg,
        "source_counts": source_counts,
        "total_records": sum(source_counts.values()),
    }


# =============================================================================
# Natural Language Query
# =============================================================================


class NLQueryRequest(BaseModel):
    query: str = Field("", description="Natural language query string")


class NLQueryResponse(BaseModel):
    query: str
    explanation: str
    filters_applied: List[Dict[str, Any]] = []
    regions: List[Dict[str, Any]] = []
    total_matched: int = 0


@router.post("/query", response_model=NLQueryResponse)
async def natural_language_query(
    request: NLQueryRequest,
    db: Session = Depends(get_db),
):
    """Query convergence regions using natural language.

    Parses queries like 'regions with high EPA violations and population inflow'
    into structured filters, validates against a field whitelist, and returns
    matching regions. Powered by Claude for intent parsing with keyword fallback.
    """
    from app.services.deal_radar_nlq import DealRadarNLQ

    nlq = DealRadarNLQ(db)
    result = await nlq.query(request.query)

    return NLQueryResponse(
        query=result.query,
        explanation=result.explanation,
        filters_applied=result.filters,
        regions=result.regions,
        total_matched=result.region_count,
    )


# =============================================================================
# AI Deal Memo Generator
# =============================================================================


class MemoResponse(BaseModel):
    region_id: str
    title: str
    html: str
    sections: List[str] = []
    generated_at: str = ""
    data_summary: Dict[str, Any] = {}


@router.post("/memo/{region_id}", response_model=MemoResponse)
async def generate_memo(region_id: str, db: Session = Depends(get_db)):
    """Generate an AI-powered investment memo for a convergence cluster.

    Gathers real signal data from EPA, IRS, Trade, Water, and Income tables,
    uses Claude to synthesize 6 memo sections (executive summary, market
    opportunity, signal analysis, target profile, risk factors, recommended
    action), and renders as a styled HTML document.
    """
    if region_id not in REGION_DEFINITIONS:
        raise HTTPException(status_code=404, detail=f"Region '{region_id}' not found")

    from app.services.deal_radar_memo import DealRadarMemoGenerator

    generator = DealRadarMemoGenerator(db)
    result = await generator.generate(region_id)

    if not result:
        raise HTTPException(status_code=404, detail="No scores available. Run a scan first.")

    return MemoResponse(
        region_id=result.region_id,
        title=result.title,
        html=result.html,
        sections=result.sections,
        generated_at=result.generated_at,
        data_summary=result.data_summary,
    )
