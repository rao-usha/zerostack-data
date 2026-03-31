"""
AFDC (Alternative Fuels Data Center) API endpoints.

Provides ingestion of EV charging station data from the NREL AFDC API.
Useful for tracking EV infrastructure density by state — a key leading
indicator for EV fleet penetration and automotive service disruption analysis.

API key: Set DATA_GOV_API in environment (or use DEMO_KEY for testing).
"""

import logging
from enum import Enum
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.afdc.metadata import DATASETS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/afdc", tags=["AFDC — EV Infrastructure"])


class AFDCDataset(str, Enum):
    EV_STATIONS = "ev_stations"  # EV charging stations by state


class AFDCIngestRequest(BaseModel):
    """Request model for AFDC dataset ingestion."""

    api_key: Optional[str] = Field(
        default=None,
        description="NREL / data.gov API key. Falls back to DATA_GOV_API env var.",
    )


@router.post("/ingest/{dataset}", summary="Ingest an AFDC dataset")
async def ingest_afdc(
    dataset: AFDCDataset,
    request: AFDCIngestRequest = AFDCIngestRequest(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger ingestion of an AFDC dataset.

    Datasets:
    - **ev_stations**: EV charging station counts by US state (Level 1, Level 2, DC Fast).
      Sourced from the NREL AFDC `/count-by-state.json?fuel_type=ELEC` endpoint.
      Useful as a proxy for EV infrastructure density and fleet readiness by geography.

    Returns a job_id immediately; ingestion runs in the background.
    """
    config: dict = {"dataset": dataset.value}
    if request.api_key:
        config["api_key"] = request.api_key

    return create_and_dispatch_job(
        db=db,
        background_tasks=background_tasks,
        source="afdc",
        config=config,
        message=f"AFDC {dataset.value} ingestion queued",
    )


@router.get("/datasets", summary="List available AFDC datasets")
async def list_datasets():
    """Return metadata for all available AFDC datasets."""
    return {
        slug: {
            "slug": slug,
            "display_name": info["display_name"],
            "description": info["description"],
            "table": info["table"],
        }
        for slug, info in DATASETS.items()
    }


@router.get("/coverage/{dataset}", summary="Check data coverage for an AFDC dataset")
async def get_coverage(
    dataset: AFDCDataset,
    db: Session = Depends(get_db),
):
    """Return row count and latest ingestion date for an AFDC dataset."""
    from app.sources.afdc.metadata import get_table_name
    from sqlalchemy import text

    table = get_table_name(dataset.value)
    result = db.execute(
        text(f"SELECT COUNT(*) as cnt, MAX(as_of_date) as latest FROM {table}")
    ).fetchone()
    return {
        "dataset": dataset.value,
        "table": table,
        "row_count": result.cnt if result else 0,
        "latest_date": str(result.latest) if result and result.latest else None,
    }
