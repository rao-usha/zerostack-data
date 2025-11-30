"""
SEC EDGAR API routes.

Endpoints for ingesting SEC corporate filings.
"""
import logging
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.sec import ingest, metadata, ingest_xbrl, formadv_ingest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sec", tags=["SEC EDGAR"])


# =============================================================================
# Request/Response Models
# =============================================================================


class IngestCompanyRequest(BaseModel):
    """Request model for ingesting a single company's filings."""
    
    cik: str = Field(
        ...,
        description="Company CIK (Central Index Key), e.g., '0000320193' for Apple",
        example="0000320193"
    )
    filing_types: Optional[List[str]] = Field(
        default=None,
        description="Filing types to ingest (defaults to 10-K and 10-Q)",
        example=["10-K", "10-Q", "8-K"]
    )
    start_date: Optional[date] = Field(
        default=None,
        description="Start date for filings (ISO format)",
        example="2020-01-01"
    )
    end_date: Optional[date] = Field(
        default=None,
        description="End date for filings (ISO format)",
        example="2024-12-31"
    )


class IngestMultipleCompaniesRequest(BaseModel):
    """Request model for ingesting multiple companies."""
    
    ciks: List[str] = Field(
        ...,
        description="List of company CIKs",
        example=["0000320193", "0000789019", "0001652044"]
    )
    filing_types: Optional[List[str]] = Field(
        default=None,
        description="Filing types to ingest (defaults to 10-K and 10-Q)",
        example=["10-K", "10-Q"]
    )
    start_date: Optional[date] = Field(
        default=None,
        description="Start date for filings (ISO format)",
        example="2020-01-01"
    )
    end_date: Optional[date] = Field(
        default=None,
        description="End date for filings (ISO format)",
        example="2024-12-31"
    )


class IngestResponse(BaseModel):
    """Response model for ingestion requests."""
    
    job_id: int
    status: str
    message: str
    cik: Optional[str] = None
    company_name: Optional[str] = None


class IngestFormADVRequest(BaseModel):
    """Request model for ingesting Form ADV data."""
    
    family_office_names: List[str] = Field(
        ...,
        description="List of family office names to search and ingest",
        example=["Soros Fund Management", "Pritzker Group", "Cascade Investment"]
    )
    max_concurrency: Optional[int] = Field(
        default=1,
        description="Maximum concurrent API requests (conservative for IAPD)",
        ge=1,
        le=3
    )
    max_requests_per_second: Optional[float] = Field(
        default=2.0,
        description="Rate limit for IAPD API requests",
        gt=0,
        le=5.0
    )


class IngestFormADVByCRDRequest(BaseModel):
    """Request model for ingesting Form ADV by CRD number."""
    
    crd_number: str = Field(
        ...,
        description="Investment adviser CRD (Central Registration Depository) number",
        example="158626"
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/ingest/company", response_model=IngestResponse)
async def ingest_company(
    request: IngestCompanyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest SEC filings for a single company.
    
    This endpoint:
    1. Validates the CIK
    2. Creates an ingestion job
    3. Fetches filings from SEC EDGAR API
    4. Stores filings in appropriate tables
    
    The ingestion runs in the background. Use the `/jobs/{job_id}` endpoint
    to check status.
    
    **Rate Limits:**
    - SEC enforces 10 requests/second per IP
    - This service uses conservative rate limiting (8 req/sec)
    
    **Examples:**
    
    Ingest Apple's 10-K and 10-Q filings from 2020-2024:
    ```json
    {
        "cik": "0000320193",
        "filing_types": ["10-K", "10-Q"],
        "start_date": "2020-01-01",
        "end_date": "2024-12-31"
    }
    ```
    """
    try:
        # Validate CIK
        if not metadata.validate_cik(request.cik):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid CIK format: {request.cik}"
            )
        
        cik_normalized = metadata.normalize_cik(request.cik)
        
        # Create job
        job_config = {
            "source": "sec",
            "cik": cik_normalized,
            "filing_types": request.filing_types or ["10-K", "10-Q"],
            "start_date": request.start_date.isoformat() if request.start_date else None,
            "end_date": request.end_date.isoformat() if request.end_date else None,
        }
        
        job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_company_ingestion,
            job.id,
            cik_normalized,
            request.filing_types,
            request.start_date,
            request.end_date
        )
        
        return IngestResponse(
            job_id=job.id,
            status="pending",
            message=f"Ingestion job created for CIK {cik_normalized}",
            cik=cik_normalized
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingest/multiple", response_model=dict)
async def ingest_multiple_companies(
    request: IngestMultipleCompaniesRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest SEC filings for multiple companies.
    
    This endpoint creates separate jobs for each company and runs them
    in the background.
    
    **Example:**
    
    Ingest filings for Apple, Microsoft, and Google:
    ```json
    {
        "ciks": ["0000320193", "0000789019", "0001652044"],
        "filing_types": ["10-K", "10-Q"],
        "start_date": "2020-01-01",
        "end_date": "2024-12-31"
    }
    ```
    """
    try:
        jobs_created = []
        
        for cik in request.ciks:
            # Validate CIK
            if not metadata.validate_cik(cik):
                logger.warning(f"Skipping invalid CIK: {cik}")
                continue
            
            cik_normalized = metadata.normalize_cik(cik)
            
            # Create job
            job_config = {
                "source": "sec",
                "cik": cik_normalized,
                "filing_types": request.filing_types or ["10-K", "10-Q"],
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
            }
            
            job = IngestionJob(
                source="sec",
                status=JobStatus.PENDING,
                config=job_config
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            
            # Run ingestion in background
            background_tasks.add_task(
                _run_company_ingestion,
                job.id,
                cik_normalized,
                request.filing_types,
                request.start_date,
                request.end_date
            )
            
            jobs_created.append({
                "job_id": job.id,
                "cik": cik_normalized,
                "status": "pending"
            })
        
        return {
            "message": f"Created {len(jobs_created)} ingestion jobs",
            "jobs": jobs_created
        }
    
    except Exception as e:
        logger.error(f"Failed to create ingestion jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supported-filing-types")
async def get_supported_filing_types():
    """
    Get list of supported SEC filing types.
    
    Returns a dictionary mapping filing type codes to descriptions.
    """
    return {
        "filing_types": metadata.SUPPORTED_FILING_TYPES
    }


@router.get("/common-companies")
async def get_common_companies():
    """
    Get CIK numbers for commonly requested companies.
    
    Returns a dictionary of company categories with CIK numbers.
    """
    from app.sources.sec.client import COMMON_COMPANIES
    
    return {
        "companies": COMMON_COMPANIES
    }


@router.post("/ingest/financial-data", response_model=IngestResponse)
async def ingest_financial_data(
    request: IngestCompanyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest structured financial data (XBRL) for a company.
    
    This endpoint fetches and parses:
    - Income statements
    - Balance sheets
    - Cash flow statements
    - Individual financial facts
    
    The data is extracted from SEC's Company Facts API which provides
    structured XBRL data in JSON format.
    
    **Example:**
    
    ```json
    {
        "cik": "0000320193"
    }
    ```
    
    The financial data is stored in normalized tables:
    - `sec_financial_facts` - All financial facts
    - `sec_income_statement` - Income statement line items
    - `sec_balance_sheet` - Balance sheet line items
    - `sec_cash_flow_statement` - Cash flow statement line items
    """
    try:
        # Validate CIK
        if not metadata.validate_cik(request.cik):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid CIK format: {request.cik}"
            )
        
        cik_normalized = metadata.normalize_cik(request.cik)
        
        # Create job
        job_config = {
            "source": "sec",
            "type": "xbrl_financial_data",
            "cik": cik_normalized,
        }
        
        job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_financial_data_ingestion,
            job.id,
            cik_normalized
        )
        
        return IngestResponse(
            job_id=job.id,
            status="pending",
            message=f"XBRL financial data ingestion job created for CIK {cik_normalized}",
            cik=cik_normalized
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create XBRL ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingest/full-company")
async def ingest_full_company(
    request: IngestCompanyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Comprehensive ingestion: Both filings AND financial data.
    
    This endpoint triggers:
    1. Filing metadata ingestion (10-K, 10-Q, etc.)
    2. XBRL financial data ingestion (income statements, balance sheets, cash flows)
    
    **Example:**
    
    ```json
    {
        "cik": "0000320193",
        "filing_types": ["10-K", "10-Q"],
        "start_date": "2020-01-01",
        "end_date": "2024-12-31"
    }
    ```
    """
    try:
        # Validate CIK
        if not metadata.validate_cik(request.cik):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid CIK format: {request.cik}"
            )
        
        cik_normalized = metadata.normalize_cik(request.cik)
        
        jobs_created = []
        
        # Job 1: Filing metadata
        job_config_filings = {
            "source": "sec",
            "type": "filings",
            "cik": cik_normalized,
            "filing_types": request.filing_types or ["10-K", "10-Q"],
            "start_date": request.start_date.isoformat() if request.start_date else None,
            "end_date": request.end_date.isoformat() if request.end_date else None,
        }
        
        job_filings = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config_filings
        )
        db.add(job_filings)
        db.commit()
        db.refresh(job_filings)
        jobs_created.append({"type": "filings", "job_id": job_filings.id})
        
        # Job 2: Financial data
        job_config_xbrl = {
            "source": "sec",
            "type": "xbrl_financial_data",
            "cik": cik_normalized,
        }
        
        job_xbrl = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config_xbrl
        )
        db.add(job_xbrl)
        db.commit()
        db.refresh(job_xbrl)
        jobs_created.append({"type": "financial_data", "job_id": job_xbrl.id})
        
        # Run both in background
        background_tasks.add_task(
            _run_company_ingestion,
            job_filings.id,
            cik_normalized,
            request.filing_types,
            request.start_date,
            request.end_date
        )
        
        background_tasks.add_task(
            _run_financial_data_ingestion,
            job_xbrl.id,
            cik_normalized
        )
        
        return {
            "message": f"Full company ingestion started for CIK {cik_normalized}",
            "cik": cik_normalized,
            "jobs": jobs_created
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create full company ingestion jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Background Task Functions
# =============================================================================


async def _run_company_ingestion(
    job_id: int,
    cik: str,
    filing_types: Optional[List[str]],
    start_date: Optional[date],
    end_date: Optional[date]
):
    """
    Background task for running company ingestion.
    
    This function is called by FastAPI's background tasks system.
    """
    from app.core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        await ingest.ingest_company_filings(
            db=db,
            job_id=job_id,
            cik=cik,
            filing_types=filing_types,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        logger.error(f"Background ingestion failed for job {job_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _run_financial_data_ingestion(
    job_id: int,
    cik: str
):
    """
    Background task for running XBRL financial data ingestion.
    
    This function is called by FastAPI's background tasks system.
    """
    from app.core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        await ingest_xbrl.ingest_company_financial_data(
            db=db,
            job_id=job_id,
            cik=cik
        )
    except Exception as e:
        logger.error(f"Background XBRL ingestion failed for job {job_id}: {e}", exc_info=True)
    finally:
        db.close()


# =============================================================================
# Form ADV Endpoints
# =============================================================================


@router.post("/form-adv/ingest/family-offices", response_model=IngestResponse)
async def ingest_form_adv_family_offices(
    request: IngestFormADVRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest SEC Form ADV data for family offices.
    
    This endpoint:
    1. Searches IAPD (Investment Adviser Public Disclosure) for each firm
    2. Fetches Form ADV data including business contact information
    3. Stores adviser details in `sec_form_adv` table
    4. Stores key personnel in `sec_form_adv_personnel` table
    
    **Data Retrieved:**
    - Business addresses and contact information
    - Phone numbers and email addresses (business-level)
    - Key personnel and their titles
    - Assets under management
    - Registration status
    
    **Important Notes:**
    - Many family offices qualify for regulatory exemptions and may NOT be registered
    - Only registered investment advisers have Form ADV data
    - This retrieves **business contact info only** (not personal PII)
    
    **Rate Limits:**
    - Default: 2 requests/second (conservative for IAPD)
    - Max concurrency: 1-3 (to avoid overwhelming the API)
    
    **Example:**
    
    ```json
    {
        "family_office_names": [
            "Soros Fund Management",
            "Pritzker Group",
            "Cascade Investment"
        ],
        "max_concurrency": 1,
        "max_requests_per_second": 2.0
    }
    ```
    """
    try:
        if not request.family_office_names:
            raise HTTPException(
                status_code=400,
                detail="At least one family office name must be provided"
            )
        
        # Create job
        job_config = {
            "source": "sec",
            "type": "form_adv",
            "family_offices": request.family_office_names,
            "max_concurrency": request.max_concurrency,
            "max_requests_per_second": request.max_requests_per_second,
        }
        
        job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_formadv_ingestion,
            job.id,
            request.family_office_names,
            request.max_concurrency,
            request.max_requests_per_second
        )
        
        return IngestResponse(
            job_id=job.id,
            status="pending",
            message=f"Form ADV ingestion job created for {len(request.family_office_names)} family offices"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Form ADV ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/form-adv/ingest/crd", response_model=IngestResponse)
async def ingest_form_adv_by_crd(
    request: IngestFormADVByCRDRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest SEC Form ADV data for a specific firm by CRD number.
    
    Use this endpoint when you already know the CRD number for a firm.
    CRD numbers can be found via the IAPD website or previous searches.
    
    **Example:**
    
    ```json
    {
        "crd_number": "158626"
    }
    ```
    """
    try:
        if not request.crd_number:
            raise HTTPException(
                status_code=400,
                detail="CRD number is required"
            )
        
        # Create job
        job_config = {
            "source": "sec",
            "type": "form_adv_crd",
            "crd_number": request.crd_number,
        }
        
        job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_formadv_crd_ingestion,
            job.id,
            request.crd_number
        )
        
        return IngestResponse(
            job_id=job.id,
            status="pending",
            message=f"Form ADV ingestion job created for CRD {request.crd_number}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Form ADV CRD ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def _run_formadv_ingestion(
    job_id: int,
    family_office_names: List[str],
    max_concurrency: int,
    max_requests_per_second: float
):
    """
    Background task for running Form ADV ingestion for family offices.
    """
    from app.core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        await formadv_ingest.ingest_family_offices(
            db=db,
            job_id=job_id,
            family_office_names=family_office_names,
            max_concurrency=max_concurrency,
            max_requests_per_second=max_requests_per_second
        )
    except Exception as e:
        logger.error(f"Background Form ADV ingestion failed for job {job_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _run_formadv_crd_ingestion(
    job_id: int,
    crd_number: str
):
    """
    Background task for running Form ADV ingestion by CRD number.
    """
    from app.core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        await formadv_ingest.ingest_firm_by_crd(
            db=db,
            job_id=job_id,
            crd_number=crd_number
        )
    except Exception as e:
        logger.error(f"Background Form ADV CRD ingestion failed for job {job_id}: {e}", exc_info=True)
    finally:
        db.close()

