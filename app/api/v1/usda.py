"""
USDA NASS QuickStats API endpoints.

Provides access to agricultural statistics.
Requires USDA_API_KEY environment variable.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.usda import (
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
    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="usda",
        config={
            "dataset": "crop",
            "commodity": request.commodity.upper(),
            "year": request.year,
            "state": request.state,
            "all_stats": request.all_stats,
        },
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
    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="usda",
        config={
            "dataset": "livestock",
            "commodity": request.commodity.upper(),
            "year": request.year,
            "state": request.state,
        },
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
    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="usda",
        config={
            "dataset": "annual_summary",
            "year": request.year,
        },
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
    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="usda",
        config={
            "dataset": "all_major_crops",
            "year": request.year,
        },
        message=f"USDA all major crops ingestion job created for {request.year}",
    )


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
