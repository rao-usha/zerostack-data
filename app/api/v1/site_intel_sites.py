"""
Site Intelligence Platform - Site Scoring API.

Endpoints for scoring, comparing, and searching potential sites.
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models_site_intel import (
    SiteScoreConfig, SiteScore,
    PowerPlant, Substation, InternetExchange, DataCenterFacility,
    IntermodalTerminal, Port, Airport,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/sites", tags=["Site Intel - Scoring"])


# =============================================================================
# REQUEST MODELS
# =============================================================================

class SiteScoreRequest(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    use_case: str = Field(default="data_center", description="Use case: data_center, warehouse, manufacturing")
    custom_weights: Optional[Dict[str, float]] = None


class SiteCompareRequest(BaseModel):
    locations: List[Dict[str, Any]] = Field(..., description="List of {name, lat, lng}")
    use_case: str = "data_center"
    factors: Optional[List[str]] = None


class SiteSearchRequest(BaseModel):
    use_case: str = "data_center"
    region: Optional[Dict[str, Any]] = None  # {states: [...]} or {bbox: {...}}
    requirements: Optional[Dict[str, Any]] = None
    sort_by: str = "overall_score"
    limit: int = Field(default=20, gt=0, le=100)


# =============================================================================
# SCORING ENDPOINTS
# =============================================================================

@router.post("/score")
async def score_site(
    request: SiteScoreRequest,
    db: Session = Depends(get_db),
):
    """
    Score a specific location for site selection.

    Returns weighted composite score based on:
    - Power infrastructure (substations, capacity, prices)
    - Telecom infrastructure (IX proximity, fiber, data centers)
    - Transportation (intermodal, ports, airports)
    - Labor (workforce size, wages, education)
    - Risk (flood, seismic, climate)
    - Incentives (OZ, FTZ, programs)
    """
    lat, lng = request.latitude, request.longitude

    # Calculate individual factor scores
    factors = {}

    # Power score (0-100)
    substation_count = db.query(func.count(Substation.id)).filter(
        Substation.latitude.between(lat - 0.5, lat + 0.5),
        Substation.longitude.between(lng - 0.5, lng + 0.5),
        Substation.max_voltage_kv >= 69,
    ).scalar() or 0
    factors["power"] = {
        "substations_within_35mi": substation_count,
        "score": min(substation_count * 15 + 10, 100),
    }

    # Telecom score (0-100)
    ix_count = db.query(func.count(InternetExchange.id)).filter(
        InternetExchange.latitude.between(lat - 1.5, lat + 1.5),
        InternetExchange.longitude.between(lng - 1.5, lng + 1.5),
    ).scalar() or 0
    dc_count = db.query(func.count(DataCenterFacility.id)).filter(
        DataCenterFacility.latitude.between(lat - 0.75, lat + 0.75),
        DataCenterFacility.longitude.between(lng - 0.75, lng + 0.75),
    ).scalar() or 0
    factors["telecom"] = {
        "ix_within_100mi": ix_count,
        "dc_within_50mi": dc_count,
        "score": min(ix_count * 15 + dc_count * 5 + 10, 100),
    }

    # Transport score (0-100)
    intermodal_count = db.query(func.count(IntermodalTerminal.id)).filter(
        IntermodalTerminal.latitude.between(lat - 0.75, lat + 0.75),
        IntermodalTerminal.longitude.between(lng - 0.75, lng + 0.75),
    ).scalar() or 0
    port_count = db.query(func.count(Port.id)).filter(
        Port.latitude.between(lat - 1.5, lat + 1.5),
        Port.longitude.between(lng - 1.5, lng + 1.5),
    ).scalar() or 0
    airport_count = db.query(func.count(Airport.id)).filter(
        Airport.latitude.between(lat - 0.5, lat + 0.5),
        Airport.longitude.between(lng - 0.5, lng + 0.5),
        Airport.has_cargo_facility == True,
    ).scalar() or 0
    factors["transport"] = {
        "intermodal_within_50mi": intermodal_count,
        "ports_within_100mi": port_count,
        "cargo_airports_within_35mi": airport_count,
        "score": min(intermodal_count * 15 + port_count * 10 + airport_count * 10 + 20, 100),
    }

    # Default weights by use case
    default_weights = {
        "data_center": {"power": 0.30, "telecom": 0.35, "transport": 0.10, "labor": 0.10, "risk": 0.10, "incentives": 0.05},
        "warehouse": {"power": 0.10, "telecom": 0.05, "transport": 0.40, "labor": 0.20, "risk": 0.15, "incentives": 0.10},
        "manufacturing": {"power": 0.20, "telecom": 0.05, "transport": 0.25, "labor": 0.25, "risk": 0.15, "incentives": 0.10},
    }

    weights = request.custom_weights or default_weights.get(request.use_case, default_weights["data_center"])

    # Calculate weighted overall score
    overall_score = 0
    for factor_name, factor_data in factors.items():
        weight = weights.get(factor_name, 0.1)
        overall_score += factor_data["score"] * weight

    # Add placeholder scores for factors not yet calculated
    for factor_name in ["labor", "risk", "incentives"]:
        if factor_name not in factors:
            factors[factor_name] = {"score": 50, "note": "Placeholder - full calculation pending"}
            overall_score += 50 * weights.get(factor_name, 0.1)

    return {
        "location": {"latitude": lat, "longitude": lng},
        "use_case": request.use_case,
        "overall_score": round(overall_score, 1),
        "grade": (
            "A" if overall_score >= 80 else
            "B" if overall_score >= 65 else
            "C" if overall_score >= 50 else
            "D" if overall_score >= 35 else
            "F"
        ),
        "factors": factors,
        "weights_used": weights,
        "computed_at": datetime.utcnow().isoformat(),
    }


@router.post("/compare")
async def compare_sites(
    request: SiteCompareRequest,
    db: Session = Depends(get_db),
):
    """
    Compare multiple locations side-by-side.

    Returns scores for each location and identifies the best option.
    """
    if len(request.locations) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 locations to compare")
    if len(request.locations) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 locations per comparison")

    results = []

    for loc in request.locations:
        score_request = SiteScoreRequest(
            latitude=loc.get("lat") or loc.get("latitude"),
            longitude=loc.get("lng") or loc.get("longitude"),
            use_case=request.use_case,
        )

        score_result = await score_site(score_request, db)
        score_result["name"] = loc.get("name", f"{loc.get('lat')}, {loc.get('lng')}")
        results.append(score_result)

    # Sort by overall score
    results.sort(key=lambda x: x["overall_score"], reverse=True)

    # Build comparison matrix
    matrix = {}
    for factor in ["power", "telecom", "transport", "labor", "risk", "incentives"]:
        matrix[factor] = {
            r["name"]: r["factors"].get(factor, {}).get("score", 0)
            for r in results
        }

    return {
        "use_case": request.use_case,
        "sites": results,
        "best_overall": results[0]["name"] if results else None,
        "comparison_matrix": matrix,
        "recommendation": f"Based on {request.use_case} criteria, {results[0]['name']} scores highest with {results[0]['overall_score']:.1f}/100" if results else None,
    }


@router.post("/search")
async def search_sites(
    request: SiteSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Search for sites matching criteria.

    Note: This is a placeholder - full implementation requires pre-computed site scores.
    """
    # For now, return guidance on how to use the API
    return {
        "message": "Site search requires pre-computed scores across a grid or site inventory",
        "use_case": request.use_case,
        "alternatives": [
            "Use /sites/score to score specific locations",
            "Use /sites/compare to compare known candidates",
            "Use domain-specific endpoints to find areas with good infrastructure",
        ],
        "suggested_workflow": [
            "1. Identify target states using /incentives/programs/by-state",
            "2. Find areas with good power: /power/substations?min_voltage_kv=115",
            "3. Check telecom: /telecom/data-centers/nearby",
            "4. Verify transport: /transport/intermodal/nearby",
            "5. Score final candidates: /sites/compare",
        ]
    }


# =============================================================================
# REPORT ENDPOINT
# =============================================================================

@router.post("/report")
async def generate_site_report(
    request: SiteScoreRequest,
    db: Session = Depends(get_db),
):
    """
    Generate a detailed site selection report.

    Includes comprehensive analysis across all factors.
    """
    lat, lng = request.latitude, request.longitude

    # Get base score
    score_result = await score_site(request, db)

    # Add detailed infrastructure analysis
    nearby_substations = db.query(Substation).filter(
        Substation.latitude.between(lat - 0.5, lat + 0.5),
        Substation.longitude.between(lng - 0.5, lng + 0.5),
    ).order_by(Substation.max_voltage_kv.desc()).limit(5).all()

    nearby_ix = db.query(InternetExchange).filter(
        InternetExchange.latitude.between(lat - 1.5, lat + 1.5),
    ).order_by(InternetExchange.network_count.desc()).limit(5).all()

    nearby_intermodal = db.query(IntermodalTerminal).filter(
        IntermodalTerminal.latitude.between(lat - 0.75, lat + 0.75),
    ).limit(5).all()

    return {
        "report_type": "Site Selection Analysis",
        "generated_at": datetime.utcnow().isoformat(),
        "location": {"latitude": lat, "longitude": lng},
        "use_case": request.use_case,
        "executive_summary": {
            "overall_score": score_result["overall_score"],
            "grade": score_result["grade"],
            "recommendation": (
                "Highly suitable" if score_result["overall_score"] >= 70 else
                "Suitable with considerations" if score_result["overall_score"] >= 50 else
                "Marginal - significant improvements needed"
            ),
        },
        "factor_analysis": score_result["factors"],
        "infrastructure_details": {
            "power": {
                "nearest_substations": [
                    {
                        "name": s.name,
                        "voltage_kv": float(s.max_voltage_kv) if s.max_voltage_kv else None,
                        "owner": s.owner,
                    }
                    for s in nearby_substations
                ]
            },
            "telecom": {
                "nearest_ix": [
                    {
                        "name": ix.name,
                        "city": ix.city,
                        "networks": ix.network_count,
                    }
                    for ix in nearby_ix
                ]
            },
            "transport": {
                "nearest_intermodal": [
                    {
                        "name": t.name,
                        "railroad": t.railroad,
                        "city": t.city,
                    }
                    for t in nearby_intermodal
                ]
            },
        },
        "next_steps": [
            "Verify utility service territory and rates",
            "Check local zoning and permitting requirements",
            "Conduct Phase I environmental assessment",
            "Engage with local economic development office",
            "Evaluate specific site parcels in the area",
        ],
    }


# =============================================================================
# CONFIGURATION ENDPOINTS
# =============================================================================

@router.get("/configs")
async def list_scoring_configs(db: Session = Depends(get_db)):
    """List available scoring configurations."""
    configs = db.query(SiteScoreConfig).filter(
        SiteScoreConfig.is_active == True
    ).all()

    return [
        {
            "id": c.id,
            "name": c.config_name,
            "use_case": c.use_case,
            "description": c.description,
            "factor_weights": c.factor_weights,
        }
        for c in configs
    ]


@router.get("/summary")
async def get_sites_summary(db: Session = Depends(get_db)):
    """Get summary of site scoring capabilities."""
    return {
        "domain": "scoring",
        "available_use_cases": ["data_center", "warehouse", "manufacturing"],
        "scoring_factors": [
            {"name": "power", "description": "Power infrastructure (substations, capacity)"},
            {"name": "telecom", "description": "Telecom infrastructure (IX, fiber, data centers)"},
            {"name": "transport", "description": "Transportation (intermodal, ports, airports)"},
            {"name": "labor", "description": "Labor market (workforce, wages, education)"},
            {"name": "risk", "description": "Risk factors (flood, seismic, climate)"},
            {"name": "incentives", "description": "Incentives (OZ, FTZ, programs)"},
        ],
        "cached_scores": db.query(func.count(SiteScore.id)).scalar(),
        "available_endpoints": [
            "/site-intel/sites/score",
            "/site-intel/sites/compare",
            "/site-intel/sites/search",
            "/site-intel/sites/report",
            "/site-intel/sites/configs",
        ],
    }
