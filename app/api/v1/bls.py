"""
BLS (Bureau of Labor Statistics) API endpoints.

Provides access to labor statistics data including:
- CES (Current Employment Statistics) - Employment, hours, earnings
- CPS (Current Population Survey) - Unemployment, labor force
- JOLTS (Job Openings and Labor Turnover) - Job openings, quits, hires
- CPI (Consumer Price Index) - Inflation measures
- PPI (Producer Price Index) - Producer prices

API Key: Optional but recommended for higher rate limits
- Without key: 25 queries/day, 10 years per query
- With key: 500 queries/day, 20 years per query
Get free key at: https://data.bls.gov/registrationEngine/
"""
import logging
from datetime import datetime
from typing import List, Optional
from enum import Enum

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.bls import (
    get_series_for_dataset,
    get_series_reference,
    get_default_date_range,
    COMMON_SERIES,
    ALL_SERIES_REFERENCE,
)
from app.core.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/bls", tags=["BLS Labor Statistics"])


# =============================================================================
# ENUMS AND REQUEST MODELS
# =============================================================================

class BLSDataset(str, Enum):
    """Available BLS datasets."""
    CES = "ces"  # Current Employment Statistics
    CPS = "cps"  # Current Population Survey
    JOLTS = "jolts"  # Job Openings and Labor Turnover Survey
    CPI = "cpi"  # Consumer Price Index
    PPI = "ppi"  # Producer Price Index
    OES = "oes"  # Occupational Employment Statistics


class BLSDatasetIngestRequest(BaseModel):
    """Request model for dataset ingestion."""
    start_year: Optional[int] = Field(
        default=None,
        description="Start year (defaults based on API key: 10 years without, 20 with)"
    )
    end_year: Optional[int] = Field(
        default=None,
        description="End year (defaults to current year)"
    )
    series_ids: Optional[List[str]] = Field(
        default=None,
        description="Specific series IDs to ingest (defaults to common series for the dataset)"
    )


class BLSSeriesIngestRequest(BaseModel):
    """Request model for custom series ingestion."""
    series_ids: List[str] = Field(
        ...,
        description="List of BLS series IDs to ingest",
        min_length=1
    )
    start_year: int = Field(
        default_factory=lambda: datetime.now().year - 10,
        description="Start year",
        ge=1900
    )
    end_year: int = Field(
        default_factory=lambda: datetime.now().year,
        description="End year"
    )
    dataset: BLSDataset = Field(
        ...,
        description="Target dataset/table for storage"
    )


class BLSAllDatasetsIngestRequest(BaseModel):
    """Request model for ingesting all BLS datasets."""
    datasets: Optional[List[BLSDataset]] = Field(
        default=None,
        description="List of datasets to ingest (defaults to all)"
    )
    start_year: Optional[int] = Field(
        default=None,
        description="Start year"
    )
    end_year: Optional[int] = Field(
        default=None,
        description="End year"
    )


# =============================================================================
# INGESTION ENDPOINTS
# =============================================================================

@router.post("/{dataset}/ingest")
async def ingest_bls_dataset_endpoint(
    dataset: BLSDataset,
    request: BLSDatasetIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BLS data for a specific dataset.

    **Available Datasets:**
    - **ces**: Current Employment Statistics (employment, hours, earnings by industry)
    - **cps**: Current Population Survey (unemployment rate, labor force participation)
    - **jolts**: Job Openings and Labor Turnover (job openings, hires, quits, separations)
    - **cpi**: Consumer Price Index (inflation measures, CPI-U, Core CPI)
    - **ppi**: Producer Price Index (wholesale prices, final demand, intermediate demand)
    - **oes**: Occupational Employment Statistics (wages by occupation)

    **API Key:** Optional but recommended
    - Without key: 25 queries/day, 10 years max
    - With key: 500 queries/day, 20 years max
    - Set `BLS_API_KEY` environment variable

    **Example Request:**
    ```json
    {
        "start_year": 2020,
        "end_year": 2024
    }
    ```
    """
    try:
        settings = get_settings()
        api_key_present = settings.get_bls_api_key() is not None

        # Set defaults
        if request.start_year is None or request.end_year is None:
            default_start, default_end = get_default_date_range(api_key_present)
            start_year = request.start_year or default_start
            end_year = request.end_year or default_end
        else:
            start_year = request.start_year
            end_year = request.end_year

        # Get series IDs
        series_ids = request.series_ids
        if not series_ids:
            series_ids = get_series_for_dataset(dataset.value)

        return create_and_dispatch_job(
            db, background_tasks, source="bls",
            config={
                "dataset": dataset.value,
                "start_year": start_year,
                "end_year": end_year,
                "series_count": len(series_ids),
                "series_ids": series_ids[:10],  # Store first 10 for reference
                "api_key_configured": api_key_present,
            },
            message=f"BLS {dataset.value.upper()} ingestion job created",
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create BLS {dataset.value} job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/series/ingest")
async def ingest_custom_series(
    request: BLSSeriesIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest specific BLS series by ID.

    Use this endpoint to ingest custom series that aren't in the default sets.

    **Finding Series IDs:**
    - Visit https://www.bls.gov/data/ and search for your series
    - Use the reference endpoints below to see common series IDs

    **Example Request:**
    ```json
    {
        "series_ids": ["LNS14000000", "CUUR0000SA0"],
        "start_year": 2020,
        "end_year": 2024,
        "dataset": "cps"
    }
    ```
    """
    try:
        settings = get_settings()
        api_key_present = settings.get_bls_api_key() is not None

        return create_and_dispatch_job(
            db, background_tasks, source="bls",
            config={
                "dataset": request.dataset.value,
                "start_year": request.start_year,
                "end_year": request.end_year,
                "series_ids": request.series_ids,
                "series_count": len(request.series_ids),
                "api_key_configured": api_key_present,
            },
            message="BLS custom series ingestion job created",
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create BLS series job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/all/ingest")
async def ingest_all_datasets(
    request: BLSAllDatasetsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest multiple BLS datasets at once.

    Creates separate jobs for each dataset. Useful for initial data load.

    **Example Request:**
    ```json
    {
        "datasets": ["cps", "ces", "cpi"],
        "start_year": 2020,
        "end_year": 2024
    }
    ```

    **Note:** This creates one job per dataset. Monitor each job separately.
    """
    try:
        settings = get_settings()
        api_key_present = settings.get_bls_api_key() is not None

        # Determine datasets to ingest
        if request.datasets:
            datasets = [d.value for d in request.datasets]
        else:
            datasets = list(COMMON_SERIES.keys())

        # Set defaults
        if request.start_year is None or request.end_year is None:
            default_start, default_end = get_default_date_range(api_key_present)
            start_year = request.start_year or default_start
            end_year = request.end_year or default_end
        else:
            start_year = request.start_year
            end_year = request.end_year

        jobs = []
        for dataset in datasets:
            series_ids = get_series_for_dataset(dataset)

            result = create_and_dispatch_job(
                db, background_tasks, source="bls",
                config={
                    "dataset": dataset,
                    "start_year": start_year,
                    "end_year": end_year,
                    "series_count": len(series_ids),
                    "api_key_configured": api_key_present,
                    "batch_job": True,
                },
                message=f"BLS {dataset.upper()} ingestion job created",
            )

            jobs.append({
                "job_id": result["job_id"],
                "dataset": dataset,
                "series_count": len(series_ids),
            })

        return {
            "status": "pending",
            "message": f"Created {len(jobs)} BLS ingestion jobs",
            "jobs": jobs,
            "year_range": f"{start_year}-{end_year}",
            "api_key_configured": api_key_present,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create BLS batch jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# REFERENCE ENDPOINTS
# =============================================================================

@router.get("/reference/datasets")
async def get_available_datasets():
    """
    Get list of available BLS datasets with descriptions.
    """
    datasets = {}
    for dataset, series in COMMON_SERIES.items():
        datasets[dataset] = {
            "name": dataset.upper(),
            "series_count": len(series),
            "description": ALL_SERIES_REFERENCE.get(dataset, {}).get("description", ""),
        }

    return {"datasets": datasets}


@router.get("/reference/series")
async def get_common_series(dataset: Optional[BLSDataset] = None):
    """
    Get commonly used BLS series IDs.

    **Query Parameters:**
    - `dataset`: Filter by dataset (optional)

    Returns series organized by dataset with descriptions.
    """
    try:
        if dataset:
            return get_series_reference(dataset.value)
        return get_series_reference()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/reference/series/{dataset}")
async def get_dataset_series(dataset: BLSDataset):
    """
    Get series IDs for a specific BLS dataset.

    **Available Datasets:** ces, cps, jolts, cpi, ppi, oes
    """
    try:
        reference = get_series_reference(dataset.value)
        return reference
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/reference/quick")
async def get_quick_reference():
    """
    Get quick reference for most popular BLS series.

    Returns the most commonly used series for economic analysis.
    """
    return {
        "unemployment": {
            "LNS14000000": "Unemployment Rate (seasonally adjusted)",
            "LNS11300000": "Labor Force Participation Rate",
        },
        "employment": {
            "CES0000000001": "Total Nonfarm Employment",
            "CES0500000003": "Average Hourly Earnings",
        },
        "inflation": {
            "CUUR0000SA0": "CPI-U All Items",
            "CUUR0000SA0L1E": "Core CPI (less food and energy)",
            "CUSR0000SA0": "CPI-U All Items (seasonally adjusted)",
        },
        "job_market": {
            "JTS000000000000000JOL": "Job Openings Level",
            "JTS000000000000000QUL": "Quits Level",
            "JTS000000000000000QUR": "Quits Rate",
        },
        "producer_prices": {
            "WPSFD4": "PPI Final Demand",
            "WPSFD41": "PPI Final Demand Goods",
        },
        "api_key_info": {
            "register_url": "https://data.bls.gov/registrationEngine/",
            "without_key": "25 queries/day, 10 years, 25 series per query",
            "with_key": "500 queries/day, 20 years, 50 series per query",
        }
    }
