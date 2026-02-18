"""
Company and Investor Data Enrichment API endpoints.

Provides endpoints for enriching portfolio company and investor data.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.enrichment.company import CompanyEnrichmentEngine
from app.enrichment.investor import InvestorEnrichmentEngine


router = APIRouter(prefix="/enrichment", tags=["enrichment"])


# Request/Response Models


class EnrichmentTriggerResponse(BaseModel):
    """Response when enrichment is triggered."""

    job_id: int
    company_name: str
    status: str
    message: str


class JobStatusResponse(BaseModel):
    """Enrichment job status."""

    job_id: int
    job_type: str
    company_name: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    results: Optional[dict] = None


class FinancialsResponse(BaseModel):
    """Financial data from SEC."""

    sec_cik: Optional[str] = None
    ticker: Optional[str] = None
    revenue: Optional[int] = None
    assets: Optional[int] = None
    net_income: Optional[int] = None
    filing_date: Optional[str] = None


class FundingResponse(BaseModel):
    """Funding data."""

    total_funding: Optional[int] = None
    last_amount: Optional[int] = None
    last_date: Optional[str] = None
    valuation: Optional[int] = None


class EmployeesResponse(BaseModel):
    """Employee data."""

    count: Optional[int] = None
    date: Optional[str] = None
    growth_yoy: Optional[float] = None


class ClassificationResponse(BaseModel):
    """Industry classification."""

    industry: Optional[str] = None
    sector: Optional[str] = None
    sic_code: Optional[str] = None
    naics_code: Optional[str] = None


class StatusResponse(BaseModel):
    """Company status."""

    current: Optional[str] = None
    acquirer: Optional[str] = None
    ipo_date: Optional[str] = None
    stock_symbol: Optional[str] = None


class EnrichedCompanyResponse(BaseModel):
    """Full enriched company data."""

    company_name: str
    financials: FinancialsResponse
    funding: FundingResponse
    employees: EmployeesResponse
    classification: ClassificationResponse
    status: StatusResponse
    enriched_at: Optional[str] = None
    confidence_score: float = 0.0


class EnrichedCompanyListItem(BaseModel):
    """Summary of enriched company."""

    company_name: str
    industry: Optional[str] = None
    sector: Optional[str] = None
    status: Optional[str] = None
    confidence_score: float = 0.0
    enriched_at: Optional[str] = None


class BatchEnrichmentRequest(BaseModel):
    """Request for batch enrichment."""

    companies: List[str] = Field(..., min_length=1, max_length=50)


class BatchEnrichmentResponse(BaseModel):
    """Response for batch enrichment."""

    total: int
    completed: int
    failed: int
    companies: List[dict]


# Investor Enrichment Models


class InvestorContactResponse(BaseModel):
    """Investor contact information."""

    name: str
    title: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin_url: Optional[str] = None
    role_type: Optional[str] = None
    is_primary: bool = False
    confidence_score: float = 0.5


class InvestorContactsResponse(BaseModel):
    """List of investor contacts."""

    investor_id: int
    investor_type: str
    investor_name: Optional[str] = None
    contacts: List[InvestorContactResponse]


class AumHistoryItem(BaseModel):
    """Single AUM history entry."""

    date: str
    aum_usd: Optional[int] = None
    source: Optional[str] = None


class InvestorAumHistoryResponse(BaseModel):
    """Investor AUM history."""

    investor_id: int
    investor_type: str
    current_aum_usd: Optional[int] = None
    history: List[AumHistoryItem]
    growth_rate_1y: Optional[float] = None


class SectorPreference(BaseModel):
    """Sector preference."""

    sector: str
    weight: float
    company_count: int = 0


class StagePreference(BaseModel):
    """Stage preference."""

    stage: str
    weight: float
    company_count: int = 0


class RegionPreference(BaseModel):
    """Region preference."""

    region: str
    weight: float
    company_count: int = 0


class CommitmentPace(BaseModel):
    """Investment commitment pace."""

    investments_per_year: Optional[float] = None
    last_investment_date: Optional[str] = None
    avg_days_between_investments: Optional[int] = None


class InvestorPreferencesResponse(BaseModel):
    """Investor investment preferences."""

    investor_id: int
    investor_type: str
    sectors: List[SectorPreference]
    stages: List[StagePreference]
    regions: List[RegionPreference]
    commitment_pace: CommitmentPace
    analyzed_at: Optional[str] = None


class InvestorEnrichmentTriggerResponse(BaseModel):
    """Response when investor enrichment is triggered."""

    investor_id: int
    investor_type: str
    investor_name: Optional[str] = None
    status: str
    message: str
    preferences: Optional[dict] = None
    contacts_found: int = 0
    aum_snapshots: int = 0


# Background task for async enrichment
async def run_enrichment(company_name: str, db: Session):
    """Background task to run enrichment."""
    engine = CompanyEnrichmentEngine(db)
    await engine.enrich_company(company_name)


# Endpoints


@router.post(
    "/company/{company_name}",
    response_model=EnrichmentTriggerResponse,
    summary="Trigger company enrichment",
    description="""
    Triggers enrichment for a specific company.

    The enrichment runs in the background and gathers data from:
    - SEC EDGAR (financials for public companies)
    - Funding data (placeholder)
    - Employee data (placeholder)
    - Industry classification

    Use the status endpoint to check progress.
    """,
)
async def trigger_enrichment(
    company_name: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Trigger enrichment for a company."""
    engine = CompanyEnrichmentEngine(db)

    # Run enrichment synchronously for now (can be backgrounded)
    try:
        result = await engine.enrich_company(company_name)
        return {
            "job_id": result.get("job_id", 0),
            "company_name": company_name,
            "status": "completed",
            "message": f"Enrichment completed with confidence {result.get('confidence_score', 0):.2f}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/company/{company_name}/status",
    response_model=JobStatusResponse,
    summary="Get enrichment job status",
    description="Returns the status of the most recent enrichment job for a company.",
)
def get_enrichment_status(
    company_name: str,
    db: Session = Depends(get_db),
):
    """Get enrichment job status."""
    engine = CompanyEnrichmentEngine(db)
    status = engine.get_job_status(company_name)

    if not status:
        raise HTTPException(
            status_code=404, detail=f"No enrichment job found for {company_name}"
        )

    return status


@router.get(
    "/companies/{company_name}",
    response_model=EnrichedCompanyResponse,
    summary="Get enriched company data",
    description="Returns all enriched data for a specific company.",
)
def get_enriched_company(
    company_name: str,
    db: Session = Depends(get_db),
):
    """Get enriched company data."""
    engine = CompanyEnrichmentEngine(db)
    data = engine.get_enriched_company(company_name)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No enriched data found for {company_name}. Trigger enrichment first.",
        )

    return data


@router.get(
    "/companies",
    response_model=List[EnrichedCompanyListItem],
    summary="List enriched companies",
    description="Returns a list of all enriched companies with summary data.",
)
def list_enriched_companies(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
):
    """List all enriched companies."""
    engine = CompanyEnrichmentEngine(db)
    return engine.list_enriched_companies(
        limit=limit,
        offset=offset,
        min_confidence=min_confidence,
    )


@router.post(
    "/batch",
    response_model=BatchEnrichmentResponse,
    summary="Batch enrich companies",
    description="""
    Triggers enrichment for multiple companies at once.

    Maximum 50 companies per batch.
    """,
)
async def batch_enrich(
    request: BatchEnrichmentRequest,
    db: Session = Depends(get_db),
):
    """Batch enrich multiple companies."""
    engine = CompanyEnrichmentEngine(db)
    result = await engine.batch_enrich(request.companies)
    return result


# =============================================================================
# Investor Enrichment Endpoints
# =============================================================================


@router.post(
    "/investor/{investor_id}",
    response_model=InvestorEnrichmentTriggerResponse,
    summary="Trigger investor enrichment",
    description="""
    Triggers full enrichment for an investor (LP or Family Office).

    Enrichment includes:
    - Portfolio-based preference analysis (sectors, stages, regions)
    - Investment commitment pace calculation
    - Contact extraction from available sources
    - AUM history tracking

    Use investor_type query parameter to specify 'lp' or 'family_office'.
    """,
)
async def trigger_investor_enrichment(
    investor_id: int,
    investor_type: str = Query(..., pattern="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Trigger enrichment for an investor."""
    engine = InvestorEnrichmentEngine(db)

    try:
        result = await engine.enrich_investor(investor_id, investor_type)

        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])

        return {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "investor_name": result.get("investor_name"),
            "status": "completed",
            "message": "Investor enrichment completed successfully",
            "preferences": result.get("preferences"),
            "contacts_found": len(result.get("contacts", [])),
            "aum_snapshots": len(result.get("aum_history", [])),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/investor/{investor_id}/status",
    summary="Get investor enrichment status",
    description="Returns the enrichment status for an investor. Shows if enrichment data exists.",
)
def get_investor_enrichment_status(
    investor_id: int,
    investor_type: str = Query(..., pattern="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Get investor enrichment status."""
    engine = InvestorEnrichmentEngine(db)

    investor_info = engine.get_investor_info(investor_id, investor_type)
    if not investor_info:
        raise HTTPException(status_code=404, detail="Investor not found")

    # Check if we have enrichment data
    contacts = engine.get_contacts(investor_id, investor_type)
    prefs = engine.get_preferences(investor_id, investor_type)

    has_prefs = bool(prefs.get("sectors") or prefs.get("stages"))
    has_contacts = len(contacts) > 0

    return {
        "investor_id": investor_id,
        "investor_type": investor_type,
        "investor_name": investor_info.get("name"),
        "enriched": has_prefs or has_contacts,
        "has_preferences": has_prefs,
        "has_contacts": has_contacts,
        "contacts_count": len(contacts),
    }


@router.get(
    "/investors/{investor_id}/contacts",
    response_model=InvestorContactsResponse,
    summary="Get investor contacts",
    description="Returns contact information for an investor.",
)
def get_investor_contacts(
    investor_id: int,
    investor_type: str = Query(..., pattern="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Get investor contacts."""
    engine = InvestorEnrichmentEngine(db)

    investor_info = engine.get_investor_info(investor_id, investor_type)
    if not investor_info:
        raise HTTPException(status_code=404, detail="Investor not found")

    contacts = engine.get_contacts(investor_id, investor_type)

    return {
        "investor_id": investor_id,
        "investor_type": investor_type,
        "investor_name": investor_info.get("name"),
        "contacts": contacts,
    }


@router.get(
    "/investors/{investor_id}/aum-history",
    response_model=InvestorAumHistoryResponse,
    summary="Get investor AUM history",
    description="Returns AUM (Assets Under Management) history for an investor.",
)
def get_investor_aum_history(
    investor_id: int,
    investor_type: str = Query(..., pattern="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Get investor AUM history."""
    engine = InvestorEnrichmentEngine(db)

    investor_info = engine.get_investor_info(investor_id, investor_type)
    if not investor_info:
        raise HTTPException(status_code=404, detail="Investor not found")

    aum_data = engine.get_aum_history(investor_id, investor_type)

    return {
        "investor_id": investor_id,
        "investor_type": investor_type,
        "current_aum_usd": aum_data.get("current_aum_usd"),
        "history": aum_data.get("history", []),
        "growth_rate_1y": aum_data.get("growth_rate_1y"),
    }


@router.get(
    "/investors/{investor_id}/preferences",
    response_model=InvestorPreferencesResponse,
    summary="Get investor investment preferences",
    description="""
    Returns analyzed investment preferences for an investor.

    Preferences are derived from portfolio analysis and include:
    - Sector preferences (Technology, Healthcare, etc.)
    - Stage preferences (Seed, Series A, Growth, etc.)
    - Geographic preferences (North America, Europe, etc.)
    - Investment commitment pace
    """,
)
def get_investor_preferences(
    investor_id: int,
    investor_type: str = Query(..., pattern="^(lp|family_office)$"),
    db: Session = Depends(get_db),
):
    """Get investor investment preferences."""
    engine = InvestorEnrichmentEngine(db)

    investor_info = engine.get_investor_info(investor_id, investor_type)
    if not investor_info:
        raise HTTPException(status_code=404, detail="Investor not found")

    prefs = engine.get_preferences(investor_id, investor_type)

    # Build commitment pace from preferences
    commitment_pace = prefs.get("commitment_pace", {})
    if not commitment_pace:
        pace_data = engine.calculate_commitment_pace(investor_id, investor_type)
        commitment_pace = {
            "investments_per_year": pace_data.get("investments_per_year"),
            "last_investment_date": pace_data.get("last_investment_date"),
            "avg_days_between_investments": pace_data.get(
                "avg_days_between_investments"
            ),
        }

    return {
        "investor_id": investor_id,
        "investor_type": investor_type,
        "sectors": prefs.get("sectors", []),
        "stages": prefs.get("stages", []),
        "regions": prefs.get("regions", []),
        "commitment_pace": commitment_pace,
        "analyzed_at": prefs.get("analyzed_at"),
    }
