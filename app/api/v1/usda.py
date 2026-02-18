"""
USDA NASS QuickStats API endpoints.

Provides access to agricultural statistics.
Requires USDA_API_KEY environment variable.
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
from app.sources.usda import (
    ingest_crop_production,
    ingest_crop_all_stats,
    ingest_livestock_inventory,
    ingest_annual_crops,
    ingest_all_major_crops,
    MAJOR_CROP_STATES,
    COMMODITY_CATEGORIES,
    STATE_FIPS,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/usda", tags=["USDA Agriculture"])


class CropIngestRequest(BaseModel):
    """Request model for crop data ingestion."""

    commodity: str = Field(
        ..., description="Commodity name (CORN, SOYBEANS, WHEAT, etc.)"
    )
    year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year to ingest"
    )
    state: Optional[str] = Field(None, description="State name (optional, e.g., IOWA)")
    all_stats: bool = Field(
        default=True,
        description="Include all statistics (production, yield, area, prices)",
    )


class LivestockIngestRequest(BaseModel):
    """Request model for livestock data ingestion."""

    commodity: str = Field(..., description="Livestock type (CATTLE, HOGS, etc.)")
    year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year to ingest"
    )
    state: Optional[str] = Field(None, description="State name (optional)")


class AnnualCropsIngestRequest(BaseModel):
    """Request model for annual crops summary."""

    year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year to ingest"
    )


class AllMajorCropsIngestRequest(BaseModel):
    """Request model for all major crops."""

    year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year to ingest"
    )


# ========== Ingestion Endpoints ==========


def _create_usda_job(db, background_tasks, config, run_func, run_args, message):
    """Helper to create a USDA ingestion job with custom background task."""
    try:
        job = IngestionJob(source="usda", status=JobStatus.PENDING, config=config)
        db.add(job)
        db.commit()
        db.refresh(job)
        background_tasks.add_task(run_func, job.id, *run_args)
        return {
            "job_id": job.id,
            "status": "pending",
            "message": message,
            "check_status": f"/api/v1/jobs/{job.id}",
        }
    except Exception as e:
        logger.error("Failed to create USDA job: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/crop/ingest")
async def ingest_crop_data(
    request: CropIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest crop data for a specific commodity.

    Available commodities: CORN, SOYBEANS, WHEAT, COTTON, RICE, OATS, BARLEY, SORGHUM

    **Requires USDA_API_KEY environment variable.**
    """
    return _create_usda_job(
        db,
        background_tasks,
        config={
            "dataset": "crop",
            "commodity": request.commodity.upper(),
            "year": request.year,
            "state": request.state,
            "all_stats": request.all_stats,
        },
        run_func=_run_crop_ingestion,
        run_args=(
            request.commodity.upper(),
            request.year,
            request.state,
            request.all_stats,
        ),
        message=f"USDA {request.commodity.upper()} ingestion job created for {request.year}",
    )


@router.post("/livestock/ingest")
async def ingest_livestock_data(
    request: LivestockIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest livestock inventory data.

    Available types: CATTLE, HOGS, SHEEP, CHICKENS, TURKEYS

    **Requires USDA_API_KEY environment variable.**
    """
    return _create_usda_job(
        db,
        background_tasks,
        config={
            "dataset": "livestock",
            "commodity": request.commodity.upper(),
            "year": request.year,
            "state": request.state,
        },
        run_func=_run_livestock_ingestion,
        run_args=(request.commodity.upper(), request.year, request.state),
        message=f"USDA {request.commodity.upper()} livestock ingestion job created",
    )


@router.post("/annual-summary/ingest")
async def ingest_annual_summary(
    request: AnnualCropsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest annual crop production summary for all crops.

    **Requires USDA_API_KEY environment variable.**
    """
    return _create_usda_job(
        db,
        background_tasks,
        config={"dataset": "annual_summary", "year": request.year},
        run_func=_run_annual_ingestion,
        run_args=(request.year,),
        message=f"USDA annual summary ingestion job created for {request.year}",
    )


@router.post("/all-major-crops/ingest")
async def ingest_all_major_crops_endpoint(
    request: AllMajorCropsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest data for all major crops (CORN, SOYBEANS, WHEAT, COTTON, RICE).

    **Requires USDA_API_KEY environment variable.**
    """
    return _create_usda_job(
        db,
        background_tasks,
        config={"dataset": "all_major_crops", "year": request.year},
        run_func=_run_all_crops_ingestion,
        run_args=(request.year,),
        message=f"USDA all major crops ingestion job created for {request.year}",
    )


# ========== Background Tasks ==========


async def _run_crop_ingestion(job_id, commodity, year, state, all_stats):
    """Background task for crop ingestion."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        conn = db.connection().connection
        if all_stats:
            total_rows = await ingest_crop_all_stats(conn, commodity, year, state)
        else:
            total_rows = await ingest_crop_production(conn, commodity, year, state)

        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.SUCCESS
            job.rows_inserted = total_rows
            job.completed_at = datetime.utcnow()
            db.commit()
        logger.info("USDA crop job %d completed: %d rows", job_id, total_rows)
    except Exception as e:
        logger.error("USDA crop job %d failed: %s", job_id, e, exc_info=True)
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


async def _run_livestock_ingestion(job_id, commodity, year, state):
    """Background task for livestock ingestion."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        conn = db.connection().connection
        total_rows = await ingest_livestock_inventory(conn, commodity, year, state)

        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.SUCCESS
            job.rows_inserted = total_rows
            job.completed_at = datetime.utcnow()
            db.commit()
        logger.info("USDA livestock job %d completed: %d rows", job_id, total_rows)
    except Exception as e:
        logger.error("USDA livestock job %d failed: %s", job_id, e, exc_info=True)
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


async def _run_annual_ingestion(job_id, year):
    """Background task for annual summary ingestion."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        conn = db.connection().connection
        total_rows = await ingest_annual_crops(conn, year)

        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.SUCCESS
            job.rows_inserted = total_rows
            job.completed_at = datetime.utcnow()
            db.commit()
        logger.info("USDA annual job %d completed: %d rows", job_id, total_rows)
    except Exception as e:
        logger.error("USDA annual job %d failed: %s", job_id, e, exc_info=True)
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


async def _run_all_crops_ingestion(job_id, year):
    """Background task for all major crops ingestion."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        conn = db.connection().connection
        results = await ingest_all_major_crops(conn, year)
        total_rows = sum(results.values())

        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.SUCCESS
            job.rows_inserted = total_rows
            job.completed_at = datetime.utcnow()
            db.commit()
        logger.info("USDA all crops job %d completed: %d rows", job_id, total_rows)
    except Exception as e:
        logger.error("USDA all crops job %d failed: %s", job_id, e, exc_info=True)
        job = db.get(IngestionJob, job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


# ========== Reference Endpoints ==========


@router.get("/reference/commodities")
async def get_commodities():
    """Get available commodities by category."""
    return {"commodities": COMMODITY_CATEGORIES}


@router.get("/reference/crop-states")
async def get_major_crop_states():
    """Get top producing states for major crops."""
    return {"crop_states": MAJOR_CROP_STATES}


@router.get("/reference/state-fips")
async def get_state_fips_codes():
    """Get state FIPS codes for filtering."""
    return {"states": STATE_FIPS}
