"""
SEC EDGAR API routes.

Endpoints for ingesting SEC corporate filings.
"""
import logging
import asyncio
from typing import List, Optional
from datetime import date, datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.sec import ingest, metadata, ingest_xbrl, formadv_ingest

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sec", 
    tags=["SEC EDGAR"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)


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


@router.post("/ingest/industrial-companies")
async def ingest_industrial_companies(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest SEC XBRL financial data for all industrial companies with CIK numbers.

    Reads CIKs from the `industrial_companies` table and ingests income statements,
    balance sheets, and cash flow statements for each company.

    Returns a master job ID and the list of company CIKs queued for ingestion.
    """
    try:
        # Query all industrial companies with CIK numbers
        result = db.execute(text(
            "SELECT id, name, cik FROM industrial_companies WHERE cik IS NOT NULL AND cik != '' ORDER BY name"
        ))
        companies = result.fetchall()

        if not companies:
            raise HTTPException(
                status_code=404,
                detail="No industrial companies with CIK numbers found"
            )

        # Create a master job
        job_config = {
            "source": "sec",
            "type": "industrial_companies_batch",
            "company_count": len(companies),
        }

        master_job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(master_job)
        db.commit()
        db.refresh(master_job)

        company_list = [
            {"id": row[0], "name": row[1], "cik": row[2]}
            for row in companies
        ]

        # Run batch ingestion in background
        background_tasks.add_task(
            _run_industrial_companies_batch,
            master_job.id,
            company_list
        )

        return {
            "job_id": master_job.id,
            "status": "pending",
            "message": f"Batch ingestion started for {len(companies)} industrial companies",
            "companies": company_list
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start industrial companies batch: {e}", exc_info=True)
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
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
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
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
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


async def _run_industrial_companies_batch(
    master_job_id: int,
    companies: List[dict]
):
    """
    Background task: sequentially ingest financial data for all industrial companies.

    Processes companies one at a time with a small delay between each to respect
    SEC rate limits (10 req/sec). Each company creates its own sub-job.
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    results = []
    succeeded = 0
    failed = 0

    try:
        # Mark master job as running
        master_job = db.query(IngestionJob).filter(IngestionJob.id == master_job_id).first()
        if master_job:
            master_job.status = JobStatus.RUNNING
            master_job.started_at = datetime.utcnow()
            db.commit()

        for i, company in enumerate(companies):
            cik = company["cik"]
            name = company["name"]
            cik_normalized = metadata.normalize_cik(cik)

            logger.info(f"[{i+1}/{len(companies)}] Ingesting financials for {name} (CIK {cik_normalized})")

            # Create a sub-job for this company
            sub_job = IngestionJob(
                source="sec",
                status=JobStatus.PENDING,
                config={
                    "source": "sec",
                    "type": "xbrl_financial_data",
                    "cik": cik_normalized,
                    "company_name": name,
                    "parent_job_id": master_job_id,
                }
            )
            db.add(sub_job)
            db.commit()
            db.refresh(sub_job)

            try:
                result = await ingest_xbrl.ingest_company_financial_data(
                    db=db,
                    job_id=sub_job.id,
                    cik=cik_normalized,
                    skip_facts=True,  # Skip raw facts for batch — much faster
                )
                results.append({"company": name, "cik": cik_normalized, "status": "success", **result})
                succeeded += 1
                logger.info(f"  -> {name}: {result['total_rows']} rows ingested")
            except Exception as e:
                results.append({"company": name, "cik": cik_normalized, "status": "failed", "error": str(e)})
                failed += 1
                logger.error(f"  -> {name}: FAILED - {e}")

            # Brief delay between companies to stay well under SEC rate limits
            if i < len(companies) - 1:
                await asyncio.sleep(1.0)

        # Update master job
        master_job = db.query(IngestionJob).filter(IngestionJob.id == master_job_id).first()
        if master_job:
            master_job.status = JobStatus.SUCCESS if failed == 0 else JobStatus.FAILED
            master_job.completed_at = datetime.utcnow()
            master_job.rows_inserted = sum(r.get("total_rows", 0) for r in results if r["status"] == "success")
            master_job.error_message = f"{succeeded} succeeded, {failed} failed" if failed > 0 else None
            db.commit()

        logger.info(f"Industrial companies batch complete: {succeeded} succeeded, {failed} failed")

    except Exception as e:
        logger.error(f"Industrial companies batch failed: {e}", exc_info=True)
        try:
            master_job = db.query(IngestionJob).filter(IngestionJob.id == master_job_id).first()
            if master_job:
                master_job.status = JobStatus.FAILED
                master_job.completed_at = datetime.utcnow()
                master_job.error_message = str(e)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# =============================================================================
# Form ADV Endpoints
# =============================================================================


@router.post("/form-adv/ingest/family-offices", response_model=IngestResponse, tags=["Form ADV - Ingestion"])
async def ingest_form_adv_family_offices(
    request: IngestFormADVRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    🏛️ Ingest SEC Form ADV data for family offices.
    
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


@router.post("/form-adv/ingest/crd", response_model=IngestResponse, tags=["Form ADV - Ingestion"])
async def ingest_form_adv_by_crd(
    request: IngestFormADVByCRDRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    🏛️ Ingest SEC Form ADV data for a specific firm by CRD number.
    
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
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
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
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
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


# =============================================================================
# Form ADV Query Endpoints
# =============================================================================


@router.get("/form-adv/firms", tags=["Form ADV - Query"])
async def query_form_adv_firms(
    limit: int = 100,
    offset: int = 0,
    family_office_only: bool = False,
    state: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    📊 Query Form ADV firms from the database.
    
    Returns business contact information for registered investment advisers.
    
    **Parameters:**
    - `limit`: Maximum number of results (default: 100, max: 1000)
    - `offset`: Pagination offset (default: 0)
    - `family_office_only`: Filter to only family offices (default: false)
    - `state`: Filter by state (e.g., "NY", "CA")
    
    **Example:**
    ```
    GET /api/v1/sec/form-adv/firms?limit=50&family_office_only=true&state=NY
    ```
    """
    try:
        from sqlalchemy import text
        
        # Validate limit
        limit = min(limit, 1000)
        
        # Build query
        query = """
            SELECT 
                crd_number,
                sec_number,
                firm_name,
                legal_name,
                business_address_street1,
                business_address_city,
                business_address_state,
                business_address_zip,
                business_phone,
                business_email,
                website,
                assets_under_management,
                is_family_office,
                registration_status,
                registration_date,
                ingested_at
            FROM sec_form_adv
            WHERE 1=1
        """
        
        params = {}
        
        if family_office_only:
            query += " AND is_family_office = :family_office"
            params["family_office"] = True
        
        if state:
            query += " AND business_address_state = :state"
            params["state"] = state.upper()
        
        query += """
            ORDER BY assets_under_management DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
        params["limit"] = limit
        params["offset"] = offset
        
        result = db.execute(text(query), params)
        rows = result.fetchall()
        
        # Convert to dict
        firms = []
        for row in rows:
            firms.append({
                "crd_number": row[0],
                "sec_number": row[1],
                "firm_name": row[2],
                "legal_name": row[3],
                "business_address": {
                    "street": row[4],
                    "city": row[5],
                    "state": row[6],
                    "zip": row[7]
                },
                "contact": {
                    "phone": row[8],
                    "email": row[9],
                    "website": row[10]
                },
                "assets_under_management": float(row[11]) if row[11] else None,
                "is_family_office": row[12],
                "registration_status": row[13],
                "registration_date": row[14].isoformat() if row[14] else None,
                "ingested_at": row[15].isoformat() if row[15] else None
            })
        
        return {
            "count": len(firms),
            "limit": limit,
            "offset": offset,
            "firms": firms
        }
    
    except Exception as e:
        logger.error(f"Error querying Form ADV firms: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/form-adv/firms/{crd_number}", tags=["Form ADV - Query"])
async def get_form_adv_firm(
    crd_number: str,
    db: Session = Depends(get_db)
):
    """
    📋 Get detailed information for a specific firm by CRD number.
    
    Includes personnel information if available.
    
    **Example:**
    ```
    GET /api/v1/sec/form-adv/firms/158626
    ```
    """
    try:
        from sqlalchemy import text
        
        # Query firm
        firm_query = text("""
            SELECT 
                crd_number,
                sec_number,
                firm_name,
                legal_name,
                doing_business_as,
                business_address_street1,
                business_address_street2,
                business_address_city,
                business_address_state,
                business_address_zip,
                business_address_country,
                business_phone,
                business_fax,
                business_email,
                website,
                mailing_address_street1,
                mailing_address_city,
                mailing_address_state,
                mailing_address_zip,
                registration_status,
                registration_date,
                state_registrations,
                assets_under_management,
                aum_date,
                total_client_count,
                is_family_office,
                form_adv_url,
                filing_date,
                last_amended_date,
                ingested_at
            FROM sec_form_adv
            WHERE crd_number = :crd_number
        """)
        
        result = db.execute(firm_query, {"crd_number": crd_number})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Firm with CRD {crd_number} not found")
        
        # Query personnel
        personnel_query = text("""
            SELECT 
                full_name,
                title,
                position_type,
                email,
                phone
            FROM sec_form_adv_personnel
            WHERE crd_number = :crd_number
            ORDER BY full_name
        """)
        
        personnel_result = db.execute(personnel_query, {"crd_number": crd_number})
        personnel_rows = personnel_result.fetchall()
        
        personnel = []
        for p_row in personnel_rows:
            personnel.append({
                "name": p_row[0],
                "title": p_row[1],
                "position_type": p_row[2],
                "email": p_row[3],
                "phone": p_row[4]
            })
        
        # Build response
        firm = {
            "crd_number": row[0],
            "sec_number": row[1],
            "firm_name": row[2],
            "legal_name": row[3],
            "doing_business_as": row[4],
            "business_address": {
                "street1": row[5],
                "street2": row[6],
                "city": row[7],
                "state": row[8],
                "zip": row[9],
                "country": row[10]
            },
            "contact": {
                "phone": row[11],
                "fax": row[12],
                "email": row[13],
                "website": row[14]
            },
            "mailing_address": {
                "street": row[15],
                "city": row[16],
                "state": row[17],
                "zip": row[18]
            },
            "registration": {
                "status": row[19],
                "date": row[20].isoformat() if row[20] else None,
                "states": row[21] if row[21] else []
            },
            "assets_under_management": {
                "amount": float(row[22]) if row[22] else None,
                "date": row[23].isoformat() if row[23] else None
            },
            "client_count": row[24],
            "is_family_office": row[25],
            "form_adv": {
                "url": row[26],
                "filing_date": row[27].isoformat() if row[27] else None,
                "last_amended": row[28].isoformat() if row[28] else None
            },
            "personnel": personnel,
            "metadata": {
                "ingested_at": row[29].isoformat() if row[29] else None
            }
        }
        
        return firm
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching Form ADV firm {crd_number}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/form-adv/stats", tags=["Form ADV - Query"])
async def get_form_adv_stats(db: Session = Depends(get_db)):
    """
    📈 Get statistics about ingested Form ADV data.
    
    Returns counts by various dimensions.
    
    **Example:**
    ```
    GET /api/v1/sec/form-adv/stats
    ```
    """
    try:
        from sqlalchemy import text
        
        stats_query = text("""
            SELECT 
                COUNT(*) as total_firms,
                COUNT(CASE WHEN is_family_office = true THEN 1 END) as family_offices,
                COUNT(DISTINCT business_address_state) as states_represented,
                SUM(assets_under_management) as total_aum,
                AVG(assets_under_management) as avg_aum,
                MAX(assets_under_management) as max_aum,
                COUNT(business_email) as firms_with_email,
                COUNT(business_phone) as firms_with_phone,
                COUNT(website) as firms_with_website
            FROM sec_form_adv
        """)
        
        result = db.execute(stats_query)
        row = result.fetchone()
        
        personnel_query = text("""
            SELECT COUNT(*) FROM sec_form_adv_personnel
        """)
        personnel_result = db.execute(personnel_query)
        personnel_count = personnel_result.scalar()
        
        return {
            "firms": {
                "total": row[0],
                "family_offices": row[1],
                "states_represented": row[2]
            },
            "assets_under_management": {
                "total": float(row[3]) if row[3] else 0,
                "average": float(row[4]) if row[4] else 0,
                "maximum": float(row[5]) if row[5] else 0
            },
            "contact_info_availability": {
                "email": row[6],
                "phone": row[7],
                "website": row[8]
            },
            "personnel": {
                "total_records": personnel_count
            }
        }
    
    except Exception as e:
        logger.error(f"Error fetching Form ADV stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

