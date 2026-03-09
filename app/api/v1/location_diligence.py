"""
Integrated Due Diligence Package — REST API.

One-call location intelligence from all Nexdata data sources.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.location_diligence import LocationDiligenceService, DD_SECTIONS

router = APIRouter(prefix="/location-diligence", tags=["Location Due Diligence"])


@router.get(
    "/package",
    summary="Full due diligence package for a location",
    response_description="Multi-section DD report from all data sources",
)
def get_package(
    county_fips: Optional[str] = Query(
        None, description="5-digit county FIPS (e.g. 06037 for LA County)"
    ),
    state_fips: Optional[str] = Query(
        None, description="2-digit state FIPS (e.g. 06 for California)"
    ),
    naics_code: Optional[str] = Query(
        None, description="NAICS code for industry-specific context"
    ),
    db: Session = Depends(get_db),
):
    svc = LocationDiligenceService(db)
    return svc.get_package(
        county_fips=county_fips,
        state_fips=state_fips,
        naics_code=naics_code,
    )


@router.get(
    "/compare",
    summary="Compare DD packages across locations",
    response_description="Side-by-side DD comparison",
)
def compare_locations(
    locations: str = Query(
        ..., description="Comma-separated county FIPS codes (e.g. 06037,48201,36061)"
    ),
    naics_code: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    parsed = [f.strip() for f in locations.split(",") if f.strip()]
    svc = LocationDiligenceService(db)
    return svc.compare_locations(locations=parsed, naics_code=naics_code)


@router.get(
    "/sections",
    summary="List all DD sections and their data sources",
    response_description="Available DD sections with table dependencies",
)
def list_sections():
    return {
        "total_sections": len(DD_SECTIONS),
        "sections": DD_SECTIONS,
    }


@router.get(
    "/coverage",
    summary="Check data coverage for a location",
    response_description="Which DD sections have data for the given location",
)
def check_coverage(
    county_fips: Optional[str] = Query(None),
    state: Optional[str] = Query(None, description="2-letter state abbreviation"),
    db: Session = Depends(get_db),
):
    svc = LocationDiligenceService(db)
    return svc.check_coverage(county_fips=county_fips, state=state)
