"""
SEC Form D API Endpoints.

T31: Access and search private placement filings.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.sec_form_d import FormDIngestionService

router = APIRouter(prefix="/form-d", tags=["form-d"])


# Response Models


class FilingSummary(BaseModel):
    """Brief filing information for search results."""

    accession_number: str
    cik: str
    submission_type: str
    filed_at: Optional[str] = None
    issuer_name: str
    location: Optional[str] = None
    industry_group: Optional[str] = None
    exemptions: List[str] = []
    total_offering_amount: Optional[int] = None
    total_amount_sold: Optional[int] = None


class SearchResponse(BaseModel):
    """Search results response."""

    total: int
    limit: int
    offset: int
    filings: List[FilingSummary]


class IssuerAddress(BaseModel):
    """Issuer address details."""

    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None


class IssuerInfo(BaseModel):
    """Issuer information."""

    name: str
    address: IssuerAddress
    phone: Optional[str] = None
    entity_type: Optional[str] = None
    jurisdiction: Optional[str] = None
    year_incorporated: Optional[int] = None


class SecuritiesInfo(BaseModel):
    """Securities type information."""

    is_equity: bool = False
    is_debt: bool = False
    is_option: bool = False
    is_pooled_fund: bool = False


class AmountsInfo(BaseModel):
    """Offering amounts."""

    total_offering: Optional[int] = None
    amount_sold: Optional[int] = None
    remaining: Optional[int] = None
    minimum_investment: Optional[int] = None


class OfferingInfo(BaseModel):
    """Offering details."""

    exemptions: List[str] = []
    date_of_first_sale: Optional[str] = None
    more_than_one_year: bool = False
    securities: SecuritiesInfo
    amounts: AmountsInfo


class InvestorsInfo(BaseModel):
    """Investor breakdown."""

    total: Optional[int] = None
    accredited: Optional[int] = None
    non_accredited: Optional[int] = None


class RelatedPerson(BaseModel):
    """Related person info."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    relationship: List[str] = []


class FilingDetail(BaseModel):
    """Full filing details."""

    accession_number: str
    cik: str
    submission_type: str
    filed_at: Optional[str] = None
    issuer: IssuerInfo
    industry: Optional[str] = None
    revenue_range: Optional[str] = None
    offering: OfferingInfo
    investors: InvestorsInfo
    related_persons: List[RelatedPerson] = []
    sales_compensation: List[dict] = []


class IndustryCount(BaseModel):
    """Industry filing count."""

    industry: str
    count: int


class ExemptionCount(BaseModel):
    """Exemption usage count."""

    exemption: str
    count: int


class DateRange(BaseModel):
    """Date range."""

    earliest: Optional[str] = None
    latest: Optional[str] = None


class StatsResponse(BaseModel):
    """Aggregate statistics."""

    total_filings: int
    unique_issuers: int
    total_offering_volume: Optional[int] = None
    total_sold_volume: Optional[int] = None
    fund_filings: int
    date_range: DateRange
    by_industry: List[IndustryCount]
    by_exemption: List[ExemptionCount]


class IngestionResult(BaseModel):
    """Ingestion job result."""

    cik: str
    filings_found: int
    filings_ingested: int
    filings_skipped: int
    errors: List[str]


class IngestionJobResponse(BaseModel):
    """Ingestion job response."""

    status: str
    message: str
    cik: Optional[str] = None
    results: Optional[List[IngestionResult]] = None


# Endpoints


@router.get(
    "/search",
    response_model=SearchResponse,
    summary="Search Form D filings",
    description="""
    Search for Form D private placement filings.

    Filter by:
    - Issuer name (partial match)
    - Industry group
    - Date range
    - Exemption type (e.g., "Rule 506(b)")
    - Minimum offering amount
    """,
)
def search_filings(
    issuer: Optional[str] = Query(None, description="Issuer name (partial match)"),
    industry: Optional[str] = Query(None, description="Industry group"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    exemption: Optional[str] = Query(
        None, description="Exemption type (e.g., 'Rule 506(b)')"
    ),
    min_amount: Optional[int] = Query(None, description="Minimum offering amount"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Search Form D filings in database."""
    service = FormDIngestionService(db)
    return service.search_filings(
        issuer_name=issuer,
        industry=industry,
        start_date=start_date,
        end_date=end_date,
        exemption=exemption,
        min_amount=min_amount,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/issuer/{cik}",
    response_model=List[FilingSummary],
    summary="Get filings by issuer CIK",
    description="Get all Form D filings for a specific issuer by CIK.",
)
def get_filings_by_issuer(
    cik: str,
    db: Session = Depends(get_db),
):
    """Get all Form D filings for an issuer."""
    service = FormDIngestionService(db)
    filings = service.get_filings_by_cik(cik)

    if not filings:
        raise HTTPException(status_code=404, detail="No filings found for this CIK")

    return filings


@router.get(
    "/recent",
    response_model=SearchResponse,
    summary="Get recent Form D filings",
    description="Get the most recent Form D filings from the database.",
)
def get_recent_filings(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get recent Form D filings."""
    from datetime import datetime, timedelta

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    service = FormDIngestionService(db)
    return service.search_filings(start_date=start_date, end_date=end_date, limit=limit)


@router.get(
    "/filing/{accession_number}",
    response_model=FilingDetail,
    summary="Get specific filing details",
    description="Get full details for a specific Form D filing by accession number.",
)
def get_filing(
    accession_number: str,
    db: Session = Depends(get_db),
):
    """Get detailed information for a specific filing."""
    service = FormDIngestionService(db)
    filing = service.get_filing(accession_number)

    if not filing:
        raise HTTPException(status_code=404, detail="Filing not found")

    return filing


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Get Form D statistics",
    description="Get aggregate statistics for Form D filings in the database.",
)
def get_stats(
    db: Session = Depends(get_db),
):
    """Get aggregate Form D statistics."""
    service = FormDIngestionService(db)
    return service.get_stats()


@router.post(
    "/ingest",
    response_model=IngestionJobResponse,
    summary="Trigger Form D ingestion",
    description="""
    Trigger ingestion of Form D filings for specified CIKs.

    If no CIKs provided, ingests from a sample of known issuers.
    """,
)
async def ingest_filings(
    ciks: Optional[List[str]] = Query(None, description="List of CIKs to ingest"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """Ingest Form D filings for specified CIKs."""
    service = FormDIngestionService(db)

    # If no CIKs provided, use sample known Form D filers
    if not ciks:
        # Sample CIKs of known VC/PE funds and startups
        ciks = [
            "0001326801",  # Meta/Facebook
            "0001318605",  # Tesla
            "0001652044",  # Alphabet
            "0001800",  # Abbott
            "0001467373",  # Airbnb
        ]

    results = []
    for cik in ciks[:10]:  # Limit to 10 CIKs per request
        try:
            result = await service.ingest_company_filings(cik)
            results.append(result)
        except Exception as e:
            results.append(
                {
                    "cik": cik,
                    "filings_found": 0,
                    "filings_ingested": 0,
                    "filings_skipped": 0,
                    "errors": [str(e)],
                }
            )

    total_ingested = sum(r["filings_ingested"] for r in results)
    total_found = sum(r["filings_found"] for r in results)

    return {
        "status": "completed",
        "message": f"Ingested {total_ingested} filings from {len(ciks)} issuers ({total_found} found)",
        "results": results,
    }


@router.get(
    "/industries",
    summary="Get available industries",
    description="Get list of industries with Form D filings.",
)
def get_industries(
    db: Session = Depends(get_db),
):
    """Get industries with Form D filing counts."""
    from sqlalchemy import text

    query = text("""
        SELECT industry_group, COUNT(*) as count
        FROM form_d_filings
        WHERE industry_group IS NOT NULL
        GROUP BY industry_group
        ORDER BY count DESC
    """)

    result = db.execute(query).mappings().fetchall()

    return {
        "industries": [
            {"name": r["industry_group"], "count": r["count"]} for r in result
        ]
    }


@router.get(
    "/exemptions",
    summary="Get exemption types",
    description="Get list of exemption types used in Form D filings.",
)
def get_exemptions(
    db: Session = Depends(get_db),
):
    """Get exemption types with counts."""
    from sqlalchemy import text

    query = text("""
        SELECT elem as exemption, COUNT(*) as count
        FROM form_d_filings, jsonb_array_elements_text(federal_exemptions) as elem
        GROUP BY elem
        ORDER BY count DESC
    """)

    result = db.execute(query).mappings().fetchall()

    return {
        "exemptions": [{"type": r["exemption"], "count": r["count"]} for r in result]
    }
