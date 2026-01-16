"""
Company Data Enrichment API endpoints.

Provides endpoints for enriching portfolio company data.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.enrichment.company import CompanyEnrichmentEngine


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
            status_code=404,
            detail=f"No enrichment job found for {company_name}"
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
            detail=f"No enriched data found for {company_name}. Trigger enrichment first."
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
