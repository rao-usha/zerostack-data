"""
FDIC BankFind API endpoints.

Provides HTTP endpoints for ingesting and querying FDIC bank data.

Datasets:
- Bank Financials: Balance sheets, income statements, 1,100+ metrics
- Institutions: Bank demographics, locations, charter info
- Failed Banks: Historical bank failures
- Summary of Deposits: Branch-level deposit data
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.fdic import ingest, metadata
from app.sources.fdic.client import FDICClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fdic", tags=["FDIC BankFind"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class FinancialsIngestRequest(BaseModel):
    """Request model for bank financials ingestion."""
    cert: Optional[int] = Field(
        None,
        description="FDIC Certificate Number (optional - for specific bank)"
    )
    report_date: Optional[str] = Field(
        None,
        description="Report date filter (YYYYMMDD format, e.g., '20230630')"
    )
    year: Optional[int] = Field(
        None,
        description="Year filter (e.g., 2023)"
    )
    limit: Optional[int] = Field(
        None,
        description="Maximum records to fetch (optional)"
    )


class InstitutionsIngestRequest(BaseModel):
    """Request model for institutions ingestion."""
    active_only: bool = Field(
        True,
        description="Only fetch active institutions"
    )
    state: Optional[str] = Field(
        None,
        description="State filter (2-letter code, e.g., 'CA')"
    )
    limit: Optional[int] = Field(
        None,
        description="Maximum records to fetch (optional)"
    )


class FailedBanksIngestRequest(BaseModel):
    """Request model for failed banks ingestion."""
    year_start: Optional[int] = Field(
        None,
        description="Start year filter"
    )
    year_end: Optional[int] = Field(
        None,
        description="End year filter"
    )
    limit: Optional[int] = Field(
        None,
        description="Maximum records to fetch (optional)"
    )


class DepositsIngestRequest(BaseModel):
    """Request model for Summary of Deposits ingestion."""
    year: Optional[int] = Field(
        None,
        description="Year filter (e.g., 2023)"
    )
    cert: Optional[int] = Field(
        None,
        description="FDIC Certificate Number (optional)"
    )
    state: Optional[str] = Field(
        None,
        description="State filter (2-letter code)"
    )
    limit: Optional[int] = Field(
        None,
        description="Maximum records to fetch (optional)"
    )


class AllDatasetsIngestRequest(BaseModel):
    """Request model for all FDIC datasets ingestion."""
    include_financials: bool = Field(
        True,
        description="Include bank financials"
    )
    include_institutions: bool = Field(
        True,
        description="Include institutions"
    )
    include_failed_banks: bool = Field(
        True,
        description="Include failed banks"
    )
    include_deposits: bool = Field(
        False,
        description="Include Summary of Deposits (large dataset!)"
    )
    year: Optional[int] = Field(
        None,
        description="Year filter for financials and deposits"
    )


class BankSearchRequest(BaseModel):
    """Request model for bank search."""
    query: str = Field(
        ...,
        description="Search term (bank name, city, etc.)"
    )
    active_only: bool = Field(
        True,
        description="Only return active banks"
    )
    limit: int = Field(
        100,
        description="Maximum results to return",
        le=1000
    )


class MetricInfo(BaseModel):
    """Financial metric information."""
    code: str
    description: str
    type: str


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================

@router.post("/financials/ingest")
async def ingest_bank_financials(
    request: FinancialsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest bank financial data from FDIC BankFind API.
    
    **What's Included:**
    - Balance sheet data (assets, liabilities, equity)
    - Income statement (net income, interest income/expense)
    - Performance ratios (ROA, ROE, NIM, efficiency ratio)
    - Capital ratios (Tier 1, total risk-based)
    - Asset quality metrics (NPL, charge-offs, reserves)
    - Loan and deposit composition
    
    **1,100+ financial variables available.**
    
    **Example Filters:**
    - `cert=3511` - JPMorgan Chase
    - `cert=628` - Bank of America
    - `year=2023` - All banks for 2023
    - `report_date=20230630` - Q2 2023 data
    
    **Note:** No API key required. Data is free and public.
    """
    try:
        # Create job
        job_config = {
            "dataset": "financials",
            "cert": request.cert,
            "report_date": request.report_date,
            "year": request.year,
            "limit": request.limit
        }
        
        job = IngestionJob(
            source="fdic",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_financials_ingestion,
            job.id,
            request.cert,
            request.report_date,
            request.year,
            request.limit
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "FDIC bank financials ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create financials ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/institutions/ingest")
async def ingest_institutions(
    request: InstitutionsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest bank institution demographics from FDIC BankFind API.
    
    **What's Included:**
    - Bank name, FDIC certificate number
    - Address, city, state, ZIP code
    - Charter type, bank class
    - Primary regulator (OCC, FRB, FDIC)
    - CBSA/metro area codes
    - Summary financials (assets, deposits, equity)
    - Website URL
    
    **Coverage:** ~4,700 active FDIC-insured institutions
    
    **Example Filters:**
    - `state=CA` - California banks only
    - `active_only=true` - Only active banks (default)
    
    **Note:** No API key required.
    """
    try:
        # Create job
        job_config = {
            "dataset": "institutions",
            "active_only": request.active_only,
            "state": request.state,
            "limit": request.limit
        }
        
        job = IngestionJob(
            source="fdic",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_institutions_ingestion,
            job.id,
            request.active_only,
            request.state,
            request.limit
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "FDIC institutions ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create institutions ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/failed-banks/ingest")
async def ingest_failed_banks(
    request: FailedBanksIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest failed banks list from FDIC BankFind API.
    
    **What's Included:**
    - Bank name, location, FDIC cert
    - Failure date
    - Acquiring institution (if any)
    - Resolution type
    - Estimated assets and deposits at failure
    - Estimated cost to FDIC
    
    **Coverage:** All FDIC-insured bank failures since 1934
    
    **Use Cases:**
    - Crisis indicators (failure trends)
    - Resolution analysis
    - Historical patterns
    
    **Example Filters:**
    - `year_start=2008&year_end=2012` - Financial crisis failures
    - `year_start=2020` - COVID-era failures
    
    **Note:** No API key required.
    """
    try:
        # Create job
        job_config = {
            "dataset": "failed_banks",
            "year_start": request.year_start,
            "year_end": request.year_end,
            "limit": request.limit
        }
        
        job = IngestionJob(
            source="fdic",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_failed_banks_ingestion,
            job.id,
            request.year_start,
            request.year_end,
            request.limit
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "FDIC failed banks ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create failed banks ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deposits/ingest")
async def ingest_summary_of_deposits(
    request: DepositsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Summary of Deposits (SOD) data from FDIC BankFind API.
    
    **What's Included:**
    - Branch-level deposit data
    - Branch address and location
    - Geographic coordinates (lat/long)
    - Main office vs. branch flag
    - CBSA/metro area codes
    
    **⚠️ Warning:** This is a LARGE dataset (~85,000+ branches per year).
    Consider filtering by year, cert, or state.
    
    **Use Cases:**
    - Bank footprint analysis
    - Market share by geography
    - Branch network visualization
    
    **Example Filters:**
    - `year=2023` - Most recent data
    - `cert=3511` - JPMorgan Chase branches
    - `state=NY` - New York branches
    
    **Note:** No API key required.
    """
    try:
        # Create job
        job_config = {
            "dataset": "summary_deposits",
            "year": request.year,
            "cert": request.cert,
            "state": request.state,
            "limit": request.limit
        }
        
        job = IngestionJob(
            source="fdic",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_deposits_ingestion,
            job.id,
            request.year,
            request.cert,
            request.state,
            request.limit
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "FDIC Summary of Deposits ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create deposits ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/all/ingest")
async def ingest_all_datasets(
    request: AllDatasetsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest all FDIC datasets at once.
    
    **Datasets Included:**
    - Bank Financials (if enabled)
    - Institutions (if enabled)
    - Failed Banks (if enabled)
    - Summary of Deposits (disabled by default - very large!)
    
    Creates separate jobs for each dataset.
    
    **Note:** Consider disabling deposits unless specifically needed.
    """
    try:
        job_ids = []
        datasets = []
        
        if request.include_financials:
            job = IngestionJob(
                source="fdic",
                status=JobStatus.PENDING,
                config={"dataset": "financials", "year": request.year}
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)
            datasets.append("financials")
            background_tasks.add_task(
                _run_financials_ingestion,
                job.id, None, None, request.year, None
            )
        
        if request.include_institutions:
            job = IngestionJob(
                source="fdic",
                status=JobStatus.PENDING,
                config={"dataset": "institutions"}
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)
            datasets.append("institutions")
            background_tasks.add_task(
                _run_institutions_ingestion,
                job.id, True, None, None
            )
        
        if request.include_failed_banks:
            job = IngestionJob(
                source="fdic",
                status=JobStatus.PENDING,
                config={"dataset": "failed_banks"}
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)
            datasets.append("failed_banks")
            background_tasks.add_task(
                _run_failed_banks_ingestion,
                job.id, None, None, None
            )
        
        if request.include_deposits:
            job = IngestionJob(
                source="fdic",
                status=JobStatus.PENDING,
                config={"dataset": "summary_deposits", "year": request.year}
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)
            datasets.append("summary_deposits")
            background_tasks.add_task(
                _run_deposits_ingestion,
                job.id, request.year, None, None, None
            )
        
        return {
            "job_ids": job_ids,
            "datasets": datasets,
            "status": "pending",
            "message": f"Created {len(job_ids)} FDIC ingestion jobs"
        }
    
    except Exception as e:
        logger.error(f"Failed to create all datasets ingestion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# REFERENCE ENDPOINTS
# =============================================================================

@router.get("/reference/metrics")
async def get_financial_metrics():
    """
    Get list of available financial metrics with descriptions.
    
    Returns common financial metrics from FDIC Call Reports including:
    - Balance sheet items (ASSET, DEP, EQ, etc.)
    - Income statement items (NETINC, INTINC, etc.)
    - Ratios (ROA, ROE, NIM, capital ratios, etc.)
    - Asset quality metrics
    - Loan and deposit composition
    """
    metrics = []
    for code, info in metadata.COMMON_FINANCIAL_METRICS.items():
        metrics.append({
            "code": code,
            "description": info["description"],
            "type": info["type"]
        })
    
    return {
        "metrics": metrics,
        "total": len(metrics),
        "note": "These are the most common metrics. The FDIC API provides 1,100+ variables."
    }


@router.get("/reference/datasets")
async def get_available_datasets():
    """
    Get information about available FDIC datasets.
    """
    return {
        "datasets": [
            {
                "name": "financials",
                "display_name": "Bank Financials",
                "description": "Balance sheets, income statements, 1,100+ financial metrics",
                "endpoint": "/api/v1/fdic/financials/ingest",
                "table": "fdic_bank_financials"
            },
            {
                "name": "institutions",
                "display_name": "Bank Institutions",
                "description": "Bank demographics, locations, charter info, ~4,700 active banks",
                "endpoint": "/api/v1/fdic/institutions/ingest",
                "table": "fdic_institutions"
            },
            {
                "name": "failed_banks",
                "display_name": "Failed Banks",
                "description": "Historical bank failures since 1934",
                "endpoint": "/api/v1/fdic/failed-banks/ingest",
                "table": "fdic_failed_banks"
            },
            {
                "name": "summary_deposits",
                "display_name": "Summary of Deposits",
                "description": "Branch-level deposit data, ~85,000+ branches",
                "endpoint": "/api/v1/fdic/deposits/ingest",
                "table": "fdic_summary_deposits"
            }
        ],
        "api_docs": "https://banks.data.fdic.gov/docs/",
        "api_key_required": False
    }


@router.get("/reference/major-banks")
async def get_major_banks():
    """
    Get FDIC certificate numbers for major U.S. banks.
    
    Useful for targeting specific banks in ingestion requests.
    """
    return {
        "major_banks": [
            {"name": "JPMorgan Chase Bank", "cert": 628, "city": "Columbus", "state": "OH"},
            {"name": "Bank of America", "cert": 3510, "city": "Charlotte", "state": "NC"},
            {"name": "Wells Fargo Bank", "cert": 3511, "city": "Sioux Falls", "state": "SD"},
            {"name": "Citibank", "cert": 7213, "city": "Sioux Falls", "state": "SD"},
            {"name": "U.S. Bank", "cert": 6548, "city": "Cincinnati", "state": "OH"},
            {"name": "PNC Bank", "cert": 6384, "city": "Wilmington", "state": "DE"},
            {"name": "Truist Bank", "cert": 9846, "city": "Charlotte", "state": "NC"},
            {"name": "Goldman Sachs Bank USA", "cert": 33124, "city": "New York", "state": "NY"},
            {"name": "TD Bank", "cert": 17100, "city": "Wilmington", "state": "DE"},
            {"name": "Capital One", "cert": 33954, "city": "McLean", "state": "VA"},
            {"name": "Silicon Valley Bank", "cert": 24735, "city": "Santa Clara", "state": "CA", "note": "Failed March 2023"},
            {"name": "Signature Bank", "cert": 57053, "city": "New York", "state": "NY", "note": "Failed March 2023"},
            {"name": "First Republic Bank", "cert": 59017, "city": "San Francisco", "state": "CA", "note": "Failed May 2023"},
        ],
        "note": "Use 'cert' parameter to fetch data for specific banks"
    }


# =============================================================================
# SEARCH ENDPOINT
# =============================================================================

@router.get("/search")
async def search_banks(
    query: str,
    active_only: bool = True,
    limit: int = 100
):
    """
    Search for banks by name, city, or other text.
    
    **Examples:**
    - `/fdic/search?query=Chase` - Find banks with "Chase" in name
    - `/fdic/search?query=California` - Find banks in California
    - `/fdic/search?query=Silicon` - Find Silicon Valley Bank
    
    **Note:** This makes a real-time call to the FDIC API.
    """
    if not query or len(query) < 2:
        raise HTTPException(
            status_code=400,
            detail="Query must be at least 2 characters"
        )
    
    if limit > 1000:
        limit = 1000
    
    try:
        client = FDICClient()
        
        try:
            results = await client.search_banks(
                query=query,
                active_only=active_only,
                limit=limit
            )
            
            # Format results
            banks = []
            for r in results:
                data = r.get("data", r)
                banks.append({
                    "cert": data.get("CERT"),
                    "name": data.get("NAME"),
                    "city": data.get("CITY"),
                    "state": data.get("STALP"),
                    "active": data.get("ACTIVE") == 1,
                    "asset": data.get("ASSET"),
                    "charter": data.get("CHARTER"),
                    "bkclass": data.get("BKCLASS"),
                    "regulator": data.get("REGAGNT"),
                })
            
            return {
                "query": query,
                "results": banks,
                "count": len(banks),
                "active_only": active_only
            }
        
        finally:
            await client.close()
    
    except Exception as e:
        logger.error(f"Bank search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# BACKGROUND TASK FUNCTIONS
# =============================================================================

async def _run_financials_ingestion(
    job_id: int,
    cert: Optional[int],
    report_date: Optional[str],
    year: Optional[int],
    limit: Optional[int]
):
    """Run bank financials ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_bank_financials(
            db=db,
            job_id=job_id,
            cert=cert,
            report_date=report_date,
            year=year,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Background financials ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_institutions_ingestion(
    job_id: int,
    active_only: bool,
    state: Optional[str],
    limit: Optional[int]
):
    """Run institutions ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_institutions(
            db=db,
            job_id=job_id,
            active_only=active_only,
            state=state,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Background institutions ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_failed_banks_ingestion(
    job_id: int,
    year_start: Optional[int],
    year_end: Optional[int],
    limit: Optional[int]
):
    """Run failed banks ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_failed_banks(
            db=db,
            job_id=job_id,
            year_start=year_start,
            year_end=year_end,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Background failed banks ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_deposits_ingestion(
    job_id: int,
    year: Optional[int],
    cert: Optional[int],
    state: Optional[str],
    limit: Optional[int]
):
    """Run Summary of Deposits ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_summary_of_deposits(
            db=db,
            job_id=job_id,
            year=year,
            cert=cert,
            state=state,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Background deposits ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
