"""
FRED API endpoints.

Provides HTTP endpoints for ingesting FRED data.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.job_helpers import create_and_dispatch_job
from app.sources.fred import ingest, metadata
from app.sources.fred.client import COMMON_SERIES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fred"])


class FREDIngestRequest(BaseModel):
    """Request model for FRED ingestion."""

    category: str = Field(
        ...,
        description="FRED category (interest_rates, monetary_aggregates, industrial_production, economic_indicators)",
    )
    series_ids: Optional[List[str]] = Field(
        None, description="List of FRED series IDs (uses defaults if not provided)"
    )
    observation_start: Optional[str] = Field(
        None, description="Start date in YYYY-MM-DD format (defaults to 10 years ago)"
    )
    observation_end: Optional[str] = Field(
        None, description="End date in YYYY-MM-DD format (defaults to today)"
    )


class FREDBatchIngestRequest(BaseModel):
    """Request model for batch FRED ingestion."""

    categories: List[str] = Field(..., description="List of FRED categories to ingest")
    observation_start: Optional[str] = Field(
        None, description="Start date for all categories"
    )
    observation_end: Optional[str] = Field(
        None, description="End date for all categories"
    )


class FREDCategoriesResponse(BaseModel):
    """Response model for available categories."""

    categories: List[dict]


class FREDSeriesListResponse(BaseModel):
    """Response model for series in a category."""

    category: str
    series: List[dict]


@router.post("/fred/ingest")
async def ingest_fred_data(
    request: FREDIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest FRED category data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Supported Categories:**
    - interest_rates: Federal Funds Rate, Treasury rates, Prime Rate (H.15)
    - monetary_aggregates: M1, M2, Monetary Base
    - industrial_production: Total, Manufacturing, Mining, Utilities
    - economic_indicators: GDP, Unemployment, CPI, PCE, Retail Sales

    **Example Series IDs:**
    - DFF: Federal Funds Rate
    - DGS10: 10-Year Treasury Rate
    - M1SL: M1 Money Stock
    - GDP: Gross Domestic Product
    - UNRATE: Unemployment Rate

    **Note:** FRED API key is optional but recommended.
    Set FRED_API_KEY environment variable to get higher rate limits.
    Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html
    """
    # Validate category
    valid_categories = list(COMMON_SERIES.keys())
    if request.category.lower() not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {', '.join(valid_categories)}",
        )

    # Validate date formats if provided
    if request.observation_start and not metadata.validate_date_format(
        request.observation_start
    ):
        raise HTTPException(
            status_code=400, detail="Invalid observation_start format. Use YYYY-MM-DD"
        )
    if request.observation_end and not metadata.validate_date_format(
        request.observation_end
    ):
        raise HTTPException(
            status_code=400, detail="Invalid observation_end format. Use YYYY-MM-DD"
        )

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="fred",
        config={
            "category": request.category,
            "series_ids": request.series_ids,
            "observation_start": request.observation_start,
            "observation_end": request.observation_end,
        },
        message="FRED ingestion job created",
    )


@router.post("/fred/ingest/batch")
async def ingest_fred_batch(
    request: FREDBatchIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest multiple FRED categories at once.

    This endpoint creates multiple ingestion jobs and runs them in the background.

    **Example:**
    ```json
    {
        "categories": ["interest_rates", "monetary_aggregates", "economic_indicators"],
        "observation_start": "2020-01-01",
        "observation_end": "2023-12-31"
    }
    ```
    """
    try:
        valid_categories = list(COMMON_SERIES.keys())

        # Validate all categories
        invalid_categories = [
            c for c in request.categories if c.lower() not in valid_categories
        ]
        if invalid_categories:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid categories: {', '.join(invalid_categories)}. "
                f"Must be one of: {', '.join(valid_categories)}",
            )

        # Validate date formats if provided
        if request.observation_start and not metadata.validate_date_format(
            request.observation_start
        ):
            raise HTTPException(
                status_code=400,
                detail="Invalid observation_start format. Use YYYY-MM-DD",
            )
        if request.observation_end and not metadata.validate_date_format(
            request.observation_end
        ):
            raise HTTPException(
                status_code=400, detail="Invalid observation_end format. Use YYYY-MM-DD"
            )

        # Create jobs for each category
        job_ids = []
        for category in request.categories:
            job_config = {
                "category": category,
                "observation_start": request.observation_start,
                "observation_end": request.observation_end,
                "batch": True,
            }

            job = IngestionJob(
                source="fred", status=JobStatus.PENDING, config=job_config
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)

        # Run batch ingestion in background (no job_id — kept as-is)
        background_tasks.add_task(
            _run_fred_batch_ingestion,
            request.categories,
            request.observation_start,
            request.observation_end,
        )

        return {
            "job_ids": job_ids,
            "status": "pending",
            "message": f"Created {len(job_ids)} FRED ingestion jobs",
            "categories": request.categories,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create batch FRED ingestion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fred/categories", response_model=FREDCategoriesResponse)
async def get_available_categories():
    """
    Get available FRED categories.

    Returns all available categories with their descriptions.
    """
    try:
        categories_info = []
        for category_name in COMMON_SERIES.keys():
            categories_info.append(
                {
                    "name": category_name,
                    "display_name": metadata.get_category_display_name(category_name),
                    "description": metadata.get_category_description(category_name),
                    "series_count": len(COMMON_SERIES[category_name]),
                }
            )

        return {"categories": categories_info}

    except Exception as e:
        logger.error(f"Failed to get FRED categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fred/series/{category}", response_model=FREDSeriesListResponse)
async def get_series_for_category(category: str):
    """
    Get available series for a FRED category.

    Returns the default series IDs that will be used if no explicit
    series_ids are provided in an ingestion request.
    """
    try:
        category_lower = category.lower()
        valid_categories = list(COMMON_SERIES.keys())

        if category_lower not in valid_categories:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid category. Must be one of: {', '.join(valid_categories)}",
            )

        series_dict = COMMON_SERIES[category_lower]

        # Build detailed response
        series_info = []
        for series_name, series_id in series_dict.items():
            series_info.append(
                {
                    "series_id": series_id,
                    "name": series_name.replace("_", " ").title(),
                    "description": f"FRED series {series_id}",
                }
            )

        return {"category": category_lower, "series": series_info}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get series for {category}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Background Task Functions (batch only — no job_id)
# ============================================


async def _run_fred_batch_ingestion(
    categories: List[str],
    observation_start: Optional[str],
    observation_end: Optional[str],
):
    """Run batch FRED ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        # Try to get FRED API key from environment
        api_key = getattr(settings, "fred_api_key", None)

        await ingest.ingest_all_fred_categories(
            db=db,
            categories=categories,
            observation_start=observation_start,
            observation_end=observation_end,
            api_key=api_key,
        )
    except Exception as e:
        logger.error(f"Background batch FRED ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
