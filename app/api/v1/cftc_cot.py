"""
CFTC Commitments of Traders (COT) API endpoints.

Provides access to weekly futures positioning data.
"""
import logging
from datetime import datetime
from typing import Optional
from enum import Enum

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db, get_session_factory
from app.core.models import IngestionJob, JobStatus
from app.sources.cftc_cot import (
    ingest_cot_legacy,
    ingest_cot_disaggregated,
    ingest_cot_tff,
    ingest_cot_all_reports,
    MAJOR_CONTRACTS,
    COMMODITY_GROUPS,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/cftc-cot", tags=["CFTC COT"])


class ReportType(str, Enum):
    """COT report types."""
    LEGACY = "legacy"
    DISAGGREGATED = "disaggregated"
    TFF = "tff"
    ALL = "all"


class COTIngestRequest(BaseModel):
    """Request model for COT data ingestion."""
    year: int = Field(default_factory=lambda: datetime.now().year, description="Year to ingest")
    report_type: ReportType = Field(default=ReportType.LEGACY, description="Type of COT report")
    combined: bool = Field(default=True, description="Include futures + options combined (vs futures only)")


# ========== Ingestion Endpoints ==========

@router.post("/ingest")
async def ingest_cot_data(
    request: COTIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest CFTC COT data for a given year.
    
    Report types:
    - **legacy**: Commercial vs Non-commercial positions
    - **disaggregated**: Producer, Swap Dealer, Managed Money, Other
    - **tff**: Traders in Financial Futures (Dealer, Asset Manager, Leveraged)
    - **all**: All report types
    
    Data is released weekly on Tuesday afternoons.
    No API key required (public data).
    """
    try:
        job_config = {
            "dataset": "cot",
            "year": request.year,
            "report_type": request.report_type.value,
            "combined": request.combined,
        }
        
        job = IngestionJob(
            source="cftc_cot",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_cot_ingestion,
            job.id,
            request.year,
            request.report_type.value,
            request.combined
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"CFTC COT {request.report_type.value} ingestion job created for {request.year}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create COT job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def _run_cot_ingestion(
    job_id: int,
    year: int,
    report_type: str,
    combined: bool
):
    """Background task to run COT ingestion."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        # Update job to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        # Get raw psycopg2 connection for the ingest functions
        conn = db.connection().connection
        
        if report_type == "all":
            results = await ingest_cot_all_reports(conn, year, combined)
            total_rows = sum(results.values())
        elif report_type == "legacy":
            total_rows = await ingest_cot_legacy(conn, year, combined)
        elif report_type == "disaggregated":
            total_rows = await ingest_cot_disaggregated(conn, year, combined)
        elif report_type == "tff":
            total_rows = await ingest_cot_tff(conn, year, combined)
        else:
            raise ValueError(f"Unknown report type: {report_type}")
        
        # Update job to success
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.SUCCESS
            job.rows_inserted = total_rows
            job.completed_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"COT ingestion job {job_id} completed: {total_rows} rows")
    
    except Exception as e:
        logger.error(f"COT ingestion job {job_id} failed: {e}", exc_info=True)
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
    
    finally:
        db.close()


# ========== Reference Endpoints ==========

@router.get("/reference/contracts")
async def get_major_contracts():
    """
    Get list of major futures contracts tracked in COT reports.
    
    Returns common contract names with their full exchange names.
    """
    return {
        "contracts": [
            {"full_name": name, "short_name": short}
            for name, short in MAJOR_CONTRACTS.items()
        ]
    }


@router.get("/reference/commodity-groups")
async def get_commodity_groups():
    """
    Get commodity groupings for analysis.
    
    Groups: energy, metals, grains, softs, livestock, financials, currencies, rates
    """
    return {"groups": COMMODITY_GROUPS}


@router.get("/reference/report-types")
async def get_report_types():
    """
    Get available COT report types with descriptions.
    """
    return {
        "report_types": [
            {
                "type": "legacy",
                "description": "Legacy report - Commercial (hedgers) vs Non-commercial (speculators)",
                "categories": ["Commercial", "Non-Commercial", "Non-Reportable"],
            },
            {
                "type": "disaggregated",
                "description": "Disaggregated report - Detailed trader categories",
                "categories": ["Producer/Merchant", "Swap Dealers", "Managed Money", "Other Reportables"],
            },
            {
                "type": "tff",
                "description": "Traders in Financial Futures - For financial contracts",
                "categories": ["Dealer/Intermediary", "Asset Manager", "Leveraged Funds", "Other Reportables"],
            },
        ]
    }
