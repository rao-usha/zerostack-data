"""
Treasury FiscalData API endpoints.

Provides HTTP endpoints for ingesting Treasury fiscal data.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.treasury import ingest, metadata
from app.sources.treasury.client import TREASURY_DATASETS, SECURITY_TYPES, AUCTION_SECURITY_TYPES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Treasury FiscalData"])


class TreasuryIngestRequest(BaseModel):
    """Request model for Treasury ingestion."""
    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format (defaults to 5 years ago)"
    )
    end_date: Optional[str] = Field(
        None,
        description="End date in YYYY-MM-DD format (defaults to today)"
    )


class TreasuryInterestRatesRequest(TreasuryIngestRequest):
    """Request model for interest rates ingestion."""
    security_type: Optional[str] = Field(
        None,
        description="Filter by security type (e.g., 'Treasury Bills', 'Treasury Notes')"
    )


class TreasuryMonthlyStatementRequest(TreasuryIngestRequest):
    """Request model for monthly statement ingestion."""
    classification: Optional[str] = Field(
        None,
        description="Filter by classification (e.g., 'Receipts', 'Outlays')"
    )


class TreasuryAuctionsRequest(TreasuryIngestRequest):
    """Request model for auctions ingestion."""
    security_type: Optional[str] = Field(
        None,
        description="Filter by security type (e.g., 'Bill', 'Note', 'Bond', 'TIPS')"
    )


class TreasuryDatasetsResponse(BaseModel):
    """Response model for available datasets."""
    datasets: list


@router.post("/treasury/debt/ingest")
async def ingest_debt_data(
    request: TreasuryIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Federal Debt Outstanding data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data Includes:**
    - Total public debt outstanding
    - Debt held by the public
    - Intragovernmental holdings
    - Historical data back to 1993
    
    **Source:** Treasury FiscalData v2/accounting/od/debt_outstanding
    
    **API Key:** ❌ NOT REQUIRED
    """
    try:
        # Validate date formats if provided
        if request.start_date and not metadata.validate_date_format(request.start_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
        if request.end_date and not metadata.validate_date_format(request.end_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
        
        # Create job
        job_config = {
            "dataset": "debt_outstanding",
            "start_date": request.start_date,
            "end_date": request.end_date
        }
        
        job = IngestionJob(
            source="treasury",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_treasury_ingestion,
            job.id,
            "debt_outstanding",
            request.start_date,
            request.end_date
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "Treasury debt outstanding ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Treasury debt ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/treasury/interest-rates/ingest")
async def ingest_interest_rates_data(
    request: TreasuryInterestRatesRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Treasury Interest Rates data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data Includes:**
    - Average interest rates on Treasury securities
    - Treasury Bills, Notes, Bonds rates
    - TIPS and FRN rates
    - Historical monthly data
    
    **Security Types:**
    - Treasury Bills
    - Treasury Notes
    - Treasury Bonds
    - Treasury Inflation-Protected Securities (TIPS)
    - Treasury Floating Rate Notes (FRN)
    
    **Source:** Treasury FiscalData v2/accounting/od/avg_interest_rates
    
    **API Key:** ❌ NOT REQUIRED
    """
    try:
        # Validate date formats
        if request.start_date and not metadata.validate_date_format(request.start_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
        if request.end_date and not metadata.validate_date_format(request.end_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
        
        # Validate security type if provided
        if request.security_type and request.security_type not in SECURITY_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid security_type. Must be one of: {', '.join(SECURITY_TYPES)}"
            )
        
        # Create job
        job_config = {
            "dataset": "interest_rates",
            "start_date": request.start_date,
            "end_date": request.end_date,
            "security_type": request.security_type
        }
        
        job = IngestionJob(
            source="treasury",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_treasury_interest_rates_ingestion,
            job.id,
            request.start_date,
            request.end_date,
            request.security_type
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "Treasury interest rates ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Treasury interest rates ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/treasury/revenue-spending/ingest")
async def ingest_revenue_spending_data(
    request: TreasuryMonthlyStatementRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Monthly Treasury Statement (Revenue & Spending) data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data Includes:**
    - Federal receipts (tax revenue)
    - Federal outlays (spending)
    - Budget surplus/deficit
    - Current month and year-to-date figures
    
    **Source:** Treasury FiscalData v1/accounting/mts/mts_table_4
    
    **API Key:** ❌ NOT REQUIRED
    """
    try:
        # Validate date formats
        if request.start_date and not metadata.validate_date_format(request.start_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
        if request.end_date and not metadata.validate_date_format(request.end_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
        
        # Create job
        job_config = {
            "dataset": "monthly_statement",
            "start_date": request.start_date,
            "end_date": request.end_date,
            "classification": request.classification
        }
        
        job = IngestionJob(
            source="treasury",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_treasury_monthly_statement_ingestion,
            job.id,
            request.start_date,
            request.end_date,
            request.classification
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "Treasury revenue/spending ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Treasury monthly statement ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/treasury/auctions/ingest")
async def ingest_auctions_data(
    request: TreasuryAuctionsRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Treasury Auction Results data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Data Includes:**
    - Bond, Note, Bill auction results
    - High yield, bid-to-cover ratio
    - Competitive vs non-competitive bids
    - Primary dealer, direct/indirect bidder breakdown
    - CUSIP, maturity dates, terms
    
    **Security Types:**
    - Bill (Treasury Bills)
    - Note (Treasury Notes)
    - Bond (Treasury Bonds)
    - TIPS (Treasury Inflation-Protected Securities)
    - FRN (Floating Rate Notes)
    - CMB (Cash Management Bills)
    
    **Source:** Treasury FiscalData v1/accounting/od/auctions_query
    
    **API Key:** ❌ NOT REQUIRED
    """
    try:
        # Validate date formats
        if request.start_date and not metadata.validate_date_format(request.start_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
        if request.end_date and not metadata.validate_date_format(request.end_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
        
        # Validate security type if provided
        if request.security_type and request.security_type not in AUCTION_SECURITY_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid security_type. Must be one of: {', '.join(AUCTION_SECURITY_TYPES)}"
            )
        
        # Create job
        job_config = {
            "dataset": "auctions",
            "start_date": request.start_date,
            "end_date": request.end_date,
            "security_type": request.security_type
        }
        
        job = IngestionJob(
            source="treasury",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_treasury_auctions_ingestion,
            job.id,
            request.start_date,
            request.end_date,
            request.security_type
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "Treasury auctions ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create Treasury auctions ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/treasury/all/ingest")
async def ingest_all_treasury_data(
    request: TreasuryIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest ALL Treasury FiscalData datasets at once.
    
    This endpoint creates ingestion jobs for all 5 datasets:
    1. Debt Outstanding
    2. Interest Rates
    3. Monthly Statement (Revenue & Spending)
    4. Auction Results
    5. Daily Treasury Balance
    
    **Note:** This is a batch operation and may take several minutes.
    
    **API Key:** ❌ NOT REQUIRED
    """
    try:
        # Validate date formats
        if request.start_date and not metadata.validate_date_format(request.start_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
        if request.end_date and not metadata.validate_date_format(request.end_date):
            raise HTTPException(
                status_code=400,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
        
        # Create jobs for each dataset
        datasets = ["debt_outstanding", "interest_rates", "monthly_statement", "auctions", "daily_balance"]
        job_ids = []
        
        for dataset in datasets:
            job_config = {
                "dataset": dataset,
                "start_date": request.start_date,
                "end_date": request.end_date,
                "batch": True
            }
            
            job = IngestionJob(
                source="treasury",
                status=JobStatus.PENDING,
                config=job_config
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)
        
        # Run batch ingestion in background
        background_tasks.add_task(
            _run_all_treasury_ingestion,
            request.start_date,
            request.end_date
        )
        
        return {
            "job_ids": job_ids,
            "status": "pending",
            "message": f"Created {len(job_ids)} Treasury ingestion jobs",
            "datasets": datasets
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create batch Treasury ingestion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/treasury/reference/datasets", response_model=TreasuryDatasetsResponse)
async def get_available_datasets():
    """
    Get available Treasury FiscalData datasets.
    
    Returns all available datasets with their descriptions.
    """
    try:
        datasets_info = []
        for dataset_name, dataset_info in TREASURY_DATASETS.items():
            datasets_info.append({
                "name": dataset_name,
                "table_name": dataset_info["table_name"],
                "description": dataset_info["description"],
                "endpoint": dataset_info["endpoint"],
                "date_field": dataset_info["date_field"]
            })
        
        return {
            "datasets": datasets_info
        }
    
    except Exception as e:
        logger.error(f"Failed to get Treasury datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/treasury/reference/security-types")
async def get_security_types():
    """
    Get available security types for Treasury data.
    
    Returns security types for:
    - Interest rates (Treasury Bills, Notes, Bonds, TIPS, FRN)
    - Auctions (Bill, Note, Bond, TIPS, FRN, CMB)
    """
    return {
        "interest_rate_security_types": SECURITY_TYPES,
        "auction_security_types": AUCTION_SECURITY_TYPES
    }


# Background task functions

async def _run_treasury_ingestion(
    job_id: int,
    dataset: str,
    start_date: Optional[str],
    end_date: Optional[str]
):
    """Run Treasury ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        ingest_funcs = {
            "daily_balance": ingest.ingest_treasury_daily_balance,
            "debt_outstanding": ingest.ingest_treasury_debt_outstanding,
        }
        
        await ingest_funcs[dataset](
            db=db,
            job_id=job_id,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        logger.error(f"Background Treasury {dataset} ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_treasury_interest_rates_ingestion(
    job_id: int,
    start_date: Optional[str],
    end_date: Optional[str],
    security_type: Optional[str]
):
    """Run Treasury interest rates ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_treasury_interest_rates(
            db=db,
            job_id=job_id,
            start_date=start_date,
            end_date=end_date,
            security_type=security_type
        )
    except Exception as e:
        logger.error(f"Background Treasury interest rates ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_treasury_monthly_statement_ingestion(
    job_id: int,
    start_date: Optional[str],
    end_date: Optional[str],
    classification: Optional[str]
):
    """Run Treasury monthly statement ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_treasury_monthly_statement(
            db=db,
            job_id=job_id,
            start_date=start_date,
            end_date=end_date,
            classification=classification
        )
    except Exception as e:
        logger.error(f"Background Treasury monthly statement ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_treasury_auctions_ingestion(
    job_id: int,
    start_date: Optional[str],
    end_date: Optional[str],
    security_type: Optional[str]
):
    """Run Treasury auctions ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_treasury_auctions(
            db=db,
            job_id=job_id,
            start_date=start_date,
            end_date=end_date,
            security_type=security_type
        )
    except Exception as e:
        logger.error(f"Background Treasury auctions ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_all_treasury_ingestion(
    start_date: Optional[str],
    end_date: Optional[str]
):
    """Run all Treasury ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_all_treasury_data(
            db=db,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        logger.error(f"Background batch Treasury ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
