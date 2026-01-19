"""
SEC Form ADV API Endpoints.

T32: Access investment adviser registration data.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.sec_form_adv import FormADVIngestionService

router = APIRouter(prefix="/form-adv", tags=["form-adv"])


# Response Models

class AdviserSummary(BaseModel):
    """Brief adviser information for search results."""
    crd_number: str
    sec_number: Optional[str] = None
    legal_name: str
    dba_name: Optional[str] = None
    location: Optional[str] = None
    regulatory_aum: Optional[int] = None
    discretionary_aum: Optional[int] = None
    total_employees: Optional[int] = None
    form_of_organization: Optional[str] = None


class SearchResponse(BaseModel):
    """Search results response."""
    total: int
    limit: int
    offset: int
    advisers: List[AdviserSummary]


class LocationInfo(BaseModel):
    """Location details."""
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    zip: Optional[str] = None


class AUMInfo(BaseModel):
    """AUM details."""
    regulatory: Optional[int] = None
    discretionary: Optional[int] = None
    non_discretionary: Optional[int] = None


class ClientBreakdown(BaseModel):
    """Client type percentages."""
    individuals: Optional[int] = 0
    high_net_worth: Optional[int] = 0
    banking_institutions: Optional[int] = 0
    investment_companies: Optional[int] = 0
    pension_plans: Optional[int] = 0
    pooled_investment_vehicles: Optional[int] = 0
    charitable_organizations: Optional[int] = 0
    corporations: Optional[int] = 0
    state_municipal: Optional[int] = 0
    other: Optional[int] = 0


class ClientsInfo(BaseModel):
    """Client information."""
    total_accounts: Optional[int] = None
    discretionary_accounts: Optional[int] = None
    breakdown: ClientBreakdown


class EmployeesInfo(BaseModel):
    """Employee information."""
    total: Optional[int] = None
    investment_advisory: Optional[int] = None
    registered_reps: Optional[int] = None


class RegistrationInfo(BaseModel):
    """Registration details."""
    sec_registered: bool = True
    registration_date: Optional[str] = None
    form_of_organization: Optional[str] = None
    state_of_organization: Optional[str] = None


class CustodyInfo(BaseModel):
    """Custody details."""
    has_custody: bool = False
    client_cash: bool = False
    client_securities: bool = False


class RegulatoryInfo(BaseModel):
    """Regulatory details."""
    has_disciplinary_events: bool = False


class AdviserDetail(BaseModel):
    """Full adviser details."""
    crd_number: str
    sec_number: Optional[str] = None
    legal_name: str
    dba_name: Optional[str] = None
    website: Optional[str] = None
    location: LocationInfo
    aum: AUMInfo
    clients: ClientsInfo
    employees: EmployeesInfo
    registration: RegistrationInfo
    custody: CustodyInfo
    regulatory: RegulatoryInfo


class RankingEntry(BaseModel):
    """AUM ranking entry."""
    rank: int
    crd_number: str
    legal_name: str
    dba_name: Optional[str] = None
    state: Optional[str] = None
    regulatory_aum: Optional[int] = None
    discretionary_aum: Optional[int] = None
    total_employees: Optional[int] = None


class RankingsResponse(BaseModel):
    """AUM rankings response."""
    rankings: List[RankingEntry]
    total_advisers: int
    total_aum: Optional[int] = None


class StateCount(BaseModel):
    """State adviser count."""
    state: str
    count: int
    aum: Optional[int] = None


class OrgCount(BaseModel):
    """Organization type count."""
    type: str
    count: int


class StatsResponse(BaseModel):
    """Aggregate statistics."""
    total_advisers: int
    total_aum: Optional[int] = None
    average_aum: Optional[int] = None
    total_employees: Optional[int] = None
    states_represented: int
    by_state: List[StateCount]
    by_organization: List[OrgCount]


class IngestionResult(BaseModel):
    """Ingestion result."""
    advisers_found: int
    advisers_ingested: int
    advisers_skipped: int
    errors: List[str]


# Endpoints

@router.get(
    "/search",
    response_model=SearchResponse,
    summary="Search investment advisers",
    description="""
    Search for SEC-registered investment advisers.

    Filter by:
    - Name (partial match on legal name or DBA)
    - State (two-letter code, e.g., "NY")
    - AUM range (minimum and/or maximum)
    """,
)
def search_advisers(
    name: Optional[str] = Query(None, description="Adviser name (partial match)"),
    state: Optional[str] = Query(None, description="State code (e.g., NY, CA)"),
    min_aum: Optional[int] = Query(None, description="Minimum regulatory AUM"),
    max_aum: Optional[int] = Query(None, description="Maximum regulatory AUM"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Search investment advisers."""
    service = FormADVIngestionService(db)
    return service.search_advisers(
        name=name,
        state=state,
        min_aum=min_aum,
        max_aum=max_aum,
        limit=limit,
        offset=offset
    )


@router.get(
    "/adviser/{crd_number}",
    response_model=AdviserDetail,
    summary="Get adviser details",
    description="Get detailed information for a specific investment adviser by CRD number.",
)
def get_adviser(
    crd_number: str,
    db: Session = Depends(get_db),
):
    """Get adviser details by CRD number."""
    service = FormADVIngestionService(db)
    adviser = service.get_adviser(crd_number)

    if not adviser:
        raise HTTPException(status_code=404, detail="Adviser not found")

    return adviser


@router.get(
    "/aum-rankings",
    response_model=RankingsResponse,
    summary="Get AUM rankings",
    description="Get top investment advisers ranked by regulatory AUM.",
)
def get_aum_rankings(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get top advisers by AUM."""
    service = FormADVIngestionService(db)
    return service.get_aum_rankings(limit=limit)


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Get Form ADV statistics",
    description="Get aggregate statistics for all investment advisers.",
)
def get_stats(
    db: Session = Depends(get_db),
):
    """Get aggregate statistics."""
    service = FormADVIngestionService(db)
    return service.get_stats()


@router.get(
    "/by-state",
    summary="Get advisers by state",
    description="""
    Get advisers grouped by state.

    If state parameter is provided, returns advisers in that state.
    Otherwise, returns summary counts for all states.
    """,
)
def get_by_state(
    state: Optional[str] = Query(None, description="State code (e.g., NY)"),
    db: Session = Depends(get_db),
):
    """Get advisers by state."""
    service = FormADVIngestionService(db)
    return service.get_by_state(state=state)


@router.post(
    "/ingest",
    response_model=IngestionResult,
    summary="Trigger Form ADV ingestion",
    description="""
    Trigger ingestion of Form ADV data.

    Currently loads sample data. In production, would fetch from SEC quarterly files.
    """,
)
def ingest_data(
    db: Session = Depends(get_db),
):
    """Ingest Form ADV data."""
    service = FormADVIngestionService(db)
    return service.ingest_sample_data()
