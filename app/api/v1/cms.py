"""
CMS / HHS API endpoints.

Provides REST API endpoints for ingesting and querying CMS healthcare data.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.cms import ingest, metadata

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/cms",
    tags=["cms"],
    responses={404: {"description": "Not found"}},
)


# Request/Response Models

class MedicareUtilizationRequest(BaseModel):
    """Request to ingest Medicare Utilization data."""
    year: Optional[int] = Field(None, description="Optional year filter")
    state: Optional[str] = Field(None, description="Two-letter state code (e.g., 'CA')")
    limit: Optional[int] = Field(None, description="Maximum records to ingest (for testing)")


class HospitalCostReportRequest(BaseModel):
    """Request to ingest Hospital Cost Report data."""
    year: Optional[int] = Field(None, description="Optional year filter")
    limit: Optional[int] = Field(None, description="Maximum records to ingest")


class DrugPricingRequest(BaseModel):
    """Request to ingest Drug Pricing data."""
    year: Optional[int] = Field(None, description="Optional year filter")
    brand_name: Optional[str] = Field(None, description="Optional filter by brand name")
    limit: Optional[int] = Field(None, description="Maximum records to ingest (for testing)")


class CMSDatasetInfo(BaseModel):
    """Information about a CMS dataset."""
    dataset_type: str
    table_name: str
    display_name: str
    description: str
    source_url: str
    column_count: int


# Endpoints

@router.get(
    "/datasets",
    response_model=list[CMSDatasetInfo],
    summary="List CMS Datasets",
    description="""
    List all available CMS datasets that can be ingested.
    
    **Available Datasets:**
    - **medicare_utilization**: Medicare Provider Utilization and Payment Data
    - **hospital_cost_reports**: Hospital Cost Reporting Information System (HCRIS)
    - **drug_pricing**: Medicare Part D Drug Spending Data
    """
)
def list_datasets():
    """List all available CMS datasets."""
    datasets = []
    
    for dataset_type, meta in metadata.DATASETS.items():
        datasets.append(CMSDatasetInfo(
            dataset_type=dataset_type,
            table_name=meta["table_name"],
            display_name=meta["display_name"],
            description=meta["description"],
            source_url=meta["source_url"],
            column_count=len(meta["columns"])
        ))
    
    return datasets


@router.get(
    "/datasets/{dataset_type}/schema",
    summary="Get Dataset Schema",
    description="""
    Get the database schema for a specific CMS dataset.
    
    Returns column names, types, and descriptions for the specified dataset.
    """
)
def get_dataset_schema(dataset_type: str):
    """Get schema for a specific CMS dataset."""
    try:
        meta = metadata.get_dataset_metadata(dataset_type)
        return {
            "dataset_type": dataset_type,
            "table_name": meta["table_name"],
            "display_name": meta["display_name"],
            "description": meta["description"],
            "columns": meta["columns"]
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post(
    "/ingest/medicare-utilization",
    summary="Ingest Medicare Utilization Data",
    description="""
    Ingest Medicare Provider Utilization and Payment Data.
    
    **Data Source:** data.cms.gov (Socrata API)
    
    **Contains:**
    - Provider information (NPI, name, address, credentials)
    - Service utilization (HCPCS codes, service counts)
    - Payment information (submitted charges, Medicare payments)
    - Beneficiary counts
    
    **Optional Filters:**
    - `year`: Filter by year
    - `state`: Filter by state (e.g., "CA", "NY")
    - `limit`: Limit number of records (useful for testing)
    
    **Example Use Cases:**
    - Analyze provider billing patterns
    - Compare costs across states
    - Identify high-volume procedures
    - Track Medicare spending trends
    """,
    response_model=dict
)
async def ingest_medicare_utilization(
    request: MedicareUtilizationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Start Medicare Utilization data ingestion job."""
    # Create ingestion job
    job = IngestionJob(
        source="cms",
        status=JobStatus.PENDING,
        config={
            "dataset_type": "medicare_utilization",
            "year": request.year,
            "state": request.state,
            "limit": request.limit
        }
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    # Start ingestion in background
    background_tasks.add_task(
        _run_medicare_utilization_ingestion,
        job.id,
        request.year,
        request.state,
        request.limit
    )
    
    return {
        "job_id": job.id,
        "status": job.status.value,
        "message": "Medicare Utilization ingestion job started",
        "dataset_type": "medicare_utilization"
    }


@router.post(
    "/ingest/hospital-cost-reports",
    summary="Ingest Hospital Cost Report Data",
    description="""
    Ingest Hospital Cost Reporting Information System (HCRIS) data.
    
    **Data Source:** CMS bulk download
    
    **Contains:**
    - Financial information
    - Utilization data
    - Cost reports
    - Provider characteristics
    
    **Note:** HCRIS data is available as large bulk files and may take significant
    time to download and process.
    
    **Optional Filters:**
    - `year`: Filter by fiscal year
    - `limit`: Limit number of records
    
    **Example Use Cases:**
    - Hospital financial analysis
    - Cost benchmarking
    - Utilization trends
    - Provider comparison
    """,
    response_model=dict
)
async def ingest_hospital_cost_reports(
    request: HospitalCostReportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Start Hospital Cost Report data ingestion job."""
    # Create ingestion job
    job = IngestionJob(
        source="cms",
        status=JobStatus.PENDING,
        config={
            "dataset_type": "hospital_cost_reports",
            "year": request.year,
            "limit": request.limit
        }
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    # Start ingestion in background
    background_tasks.add_task(
        _run_hospital_cost_report_ingestion,
        job.id,
        request.year,
        request.limit
    )
    
    return {
        "job_id": job.id,
        "status": job.status.value,
        "message": "Hospital Cost Report ingestion job started",
        "dataset_type": "hospital_cost_reports"
    }


@router.post(
    "/ingest/drug-pricing",
    summary="Ingest Drug Pricing Data",
    description="""
    Ingest Medicare Part D Drug Spending data.
    
    **Data Source:** data.cms.gov (Socrata API)
    
    **Contains:**
    - Brand and generic drug names
    - Total spending
    - Total claims and beneficiaries
    - Per-unit and per-claim costs
    - Dosage units
    - Outlier flags
    
    **Optional Filters:**
    - `year`: Filter by year
    - `brand_name`: Filter by specific brand name
    - `limit`: Limit number of records (useful for testing)
    
    **Example Use Cases:**
    - Track drug price trends
    - Compare brand vs generic costs
    - Identify high-cost medications
    - Analyze Medicare drug spending
    - Price benchmarking
    """,
    response_model=dict
)
async def ingest_drug_pricing(
    request: DrugPricingRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Start Drug Pricing data ingestion job."""
    # Create ingestion job
    job = IngestionJob(
        source="cms",
        status=JobStatus.PENDING,
        config={
            "dataset_type": "drug_pricing",
            "year": request.year,
            "brand_name": request.brand_name,
            "limit": request.limit
        }
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    # Start ingestion in background
    background_tasks.add_task(
        _run_drug_pricing_ingestion,
        job.id,
        request.year,
        request.brand_name,
        request.limit
    )
    
    return {
        "job_id": job.id,
        "status": job.status.value,
        "message": "Drug Pricing ingestion job started",
        "dataset_type": "drug_pricing"
    }


# Background task functions

async def _run_medicare_utilization_ingestion(
    job_id: int,
    year: Optional[int],
    state: Optional[str],
    limit: Optional[int]
):
    """Background task to run Medicare Utilization ingestion."""
    from app.core.database import get_session_factory
    from datetime import datetime
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()
        
        try:
            # Run ingestion
            result = await ingest.ingest_medicare_utilization(
                db=db,
                job_id=job_id,
                year=year,
                state=state,
                limit=limit
            )
            
            # Update job with success
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_affected = result.get("rows_inserted", 0)
            job.result_metadata = result
            db.commit()
            
            logger.info(f"Job {job_id} completed successfully")
        
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()


async def _run_hospital_cost_report_ingestion(
    job_id: int,
    year: Optional[int],
    limit: Optional[int]
):
    """Background task to run Hospital Cost Report ingestion."""
    from app.core.database import get_session_factory
    from datetime import datetime
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()
        
        try:
            result = await ingest.ingest_hospital_cost_reports(
                db=db,
                job_id=job_id,
                year=year,
                limit=limit
            )
            
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_affected = result.get("rows_inserted", 0)
            job.result_metadata = result
            db.commit()
            
            logger.info(f"Job {job_id} completed successfully")
        
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()


async def _run_drug_pricing_ingestion(
    job_id: int,
    year: Optional[int],
    brand_name: Optional[str],
    limit: Optional[int]
):
    """Background task to run Drug Pricing ingestion."""
    from app.core.database import get_session_factory
    from datetime import datetime
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()
        
        try:
            result = await ingest.ingest_drug_pricing(
                db=db,
                job_id=job_id,
                year=year,
                brand_name=brand_name,
                limit=limit
            )
            
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_affected = result.get("rows_inserted", 0)
            job.result_metadata = result
            db.commit()
            
            logger.info(f"Job {job_id} completed successfully")
        
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()
