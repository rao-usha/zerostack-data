"""
Labor Arbitrage Maps — REST API.

Compare labor costs across geographies for PE vertical strategies.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.labor_arbitrage import LaborArbitrageService, VERTICAL_OCCUPATIONS

router = APIRouter(prefix="/labor-arbitrage", tags=["Labor Arbitrage"])


@router.get(
    "/compare",
    summary="Compare wages for an occupation across geographies",
    response_description="Ranked list of areas by wage (lowest first)",
)
def compare_wages(
    occupation_code: str = Query(..., description="SOC code, e.g. 29-1021"),
    base_area: Optional[str] = Query(
        None, description="Area code for baseline comparison (e.g. ST06 for California)"
    ),
    area_type: Optional[str] = Query(
        None, description="Filter by area type: state, msa, national"
    ),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    svc = LaborArbitrageService(db)
    return svc.compare_wages(
        occupation_code=occupation_code,
        base_area=base_area,
        area_type=area_type,
        limit=limit,
    )


@router.get(
    "/vertical/{slug}",
    summary="Labor cost profile for a PE vertical",
    response_description="Wage data for all occupations in the vertical",
)
def vertical_profile(
    slug: str,
    area_codes: Optional[str] = Query(
        None, description="Comma-separated area codes (e.g. ST06,ST48,ST36)"
    ),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    parsed_areas = (
        [a.strip() for a in area_codes.split(",") if a.strip()]
        if area_codes
        else None
    )
    svc = LaborArbitrageService(db)
    return svc.vertical_profile(slug=slug, area_codes=parsed_areas, limit=limit)


@router.get(
    "/occupations",
    summary="List all occupations with wage data",
    response_description="Distinct occupations and vertical mappings",
)
def list_occupations(db: Session = Depends(get_db)):
    svc = LaborArbitrageService(db)
    return svc.list_occupations()


@router.get(
    "/areas",
    summary="List all geographic areas with wage data",
    response_description="Distinct areas grouped by type",
)
def list_areas(
    area_type: Optional[str] = Query(
        None, description="Filter by area type: state, msa, national"
    ),
    db: Session = Depends(get_db),
):
    svc = LaborArbitrageService(db)
    return svc.list_areas(area_type=area_type)


@router.get(
    "/methodology",
    summary="Labor arbitrage methodology",
    response_description="Scoring approach and data sources",
)
def get_methodology():
    return {
        "description": (
            "Labor Arbitrage Maps compare occupational wages across U.S. "
            "geographies using BLS Occupational Employment and Wage Statistics "
            "(OES). Helps PE firms identify lower-cost labor markets for "
            "vertical roll-up strategies."
        ),
        "data_source": "BLS OES (occupational_wage table)",
        "wage_metrics": [
            "mean_hourly_wage",
            "median_hourly_wage",
            "mean_annual_wage",
            "median_annual_wage",
            "pct_10/25/75/90 hourly wages",
        ],
        "area_types": ["state", "msa", "national"],
        "verticals": {
            slug: [{"code": c, "title": t} for c, t in occs]
            for slug, occs in VERTICAL_OCCUPATIONS.items()
        },
        "use_cases": [
            "Compare RN wages across states for medspa roll-up site selection",
            "Find cheapest MSAs for HVAC technicians",
            "Identify labor cost arbitrage between coastal and inland markets",
        ],
    }
