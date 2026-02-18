"""
Kaggle API endpoints.

Provides HTTP endpoints for ingesting Kaggle competition datasets.

Currently supported:
- M5 Forecasting (Walmart-style retail demand)

IMPORTANT:
- Requires Kaggle API credentials (KAGGLE_USERNAME, KAGGLE_KEY or ~/.kaggle/kaggle.json)
- Some competitions require accepting terms on kaggle.com first
- Large datasets (like M5) may take significant time to download and ingest
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.kaggle import ingest, m5_metadata
from app.sources.kaggle.client import KaggleClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["kaggle"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class M5IngestRequest(BaseModel):
    """Request model for M5 dataset ingestion."""

    force_download: bool = Field(
        False, description="Force re-download even if files exist locally"
    )
    limit_items: Optional[int] = Field(
        None,
        description="Limit number of items to process (for testing). None = all items (~30K)",
    )


class M5StatusResponse(BaseModel):
    """Response model for M5 dataset status."""

    dataset: str
    competition: str
    description: str
    tables: dict
    hierarchy: dict
    date_range: str
    license: str


class KaggleFilesResponse(BaseModel):
    """Response model for listing competition files."""

    competition: str
    files: list


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/kaggle/m5/info", response_model=M5StatusResponse)
async def get_m5_info():
    """
    Get information about the M5 Forecasting dataset.

    Returns metadata about the M5 competition dataset including:
    - Dataset description
    - Table schema summaries
    - Hierarchy information (states, stores, categories)
    - Date range and licensing info

    **No credentials required for this endpoint.**
    """
    try:
        summary = m5_metadata.get_m5_summary()
        return {
            "dataset": summary["name"],
            "competition": summary["competition"],
            "description": summary["description"],
            "tables": summary["tables"],
            "hierarchy": summary["hierarchy"],
            "date_range": summary["date_range"],
            "license": summary["license"],
        }
    except Exception as e:
        logger.error(f"Failed to get M5 info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kaggle/m5/files")
async def list_m5_files():
    """
    List available files in the M5 competition.

    Returns the files available for download from the M5 Forecasting competition.

    **Requires Kaggle credentials:**
    - Set KAGGLE_USERNAME and KAGGLE_KEY in environment
    - Or configure ~/.kaggle/kaggle.json

    **Note:** You must accept the competition rules at kaggle.com first.
    """
    from app.core.config import get_settings

    try:
        settings = get_settings()

        # Try to get credentials
        try:
            username, key = settings.require_kaggle_credentials()
        except ValueError:
            username, key = None, None

        client = KaggleClient(
            username=username, key=key, data_dir=settings.kaggle_data_dir
        )

        files = await client.list_competition_files(KaggleClient.M5_COMPETITION)

        return {
            "competition": KaggleClient.M5_COMPETITION,
            "files": files,
            "note": "Accept competition rules at kaggle.com before downloading",
        }

    except Exception as e:
        logger.error(f"Failed to list M5 files: {e}")

        if "403" in str(e).lower() or "forbidden" in str(e).lower():
            raise HTTPException(
                status_code=403,
                detail="Access forbidden. Have you accepted the competition rules at kaggle.com?",
            )
        elif "401" in str(e).lower() or "unauthorized" in str(e).lower():
            raise HTTPException(
                status_code=401,
                detail="Kaggle authentication failed. Check KAGGLE_USERNAME and KAGGLE_KEY.",
            )
        else:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/kaggle/m5/ingest")
async def ingest_m5_dataset(
    request: M5IngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest the M5 Forecasting dataset.

    This endpoint:
    1. Downloads the M5 files from Kaggle (if not cached)
    2. Creates database tables (m5_calendar, m5_items, m5_prices, m5_sales)
    3. Parses and loads all data into PostgreSQL

    **Important Notes:**
    - First download is ~500MB+, may take several minutes
    - Full ingestion creates ~60M+ rows (sales data is in long format)
    - Use `limit_items` parameter for testing with smaller subset
    - Uses background task - check job status via /api/v1/jobs/{job_id}

    **Requires Kaggle credentials:**
    - Set KAGGLE_USERNAME and KAGGLE_KEY in environment
    - Or configure ~/.kaggle/kaggle.json

    **Prerequisites:**
    - Accept M5 competition rules at: https://www.kaggle.com/competitions/m5-forecasting-accuracy

    **Example Request:**
    ```json
    {
        "force_download": false,
        "limit_items": 100
    }
    ```
    Set `limit_items` to null (or omit) to process all ~30K items.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="kaggle",
        config={
            "dataset": "m5-forecasting",
            "competition": KaggleClient.M5_COMPETITION,
            "force_download": request.force_download,
            "limit_items": request.limit_items,
        },
        message="M5 ingestion job created. This may take a while for full dataset.",
    )


@router.post("/kaggle/m5/prepare-tables")
async def prepare_m5_tables_endpoint(db: Session = Depends(get_db)):
    """
    Create M5 database tables without downloading/ingesting data.

    Creates:
    - m5_calendar: Calendar dimension with dates, events, SNAP indicators
    - m5_items: Item dimension with hierarchy (category, department, store, state)
    - m5_prices: Price data at store/item/week level
    - m5_sales: Daily sales data in long format

    This is idempotent - safe to call multiple times.

    **Use cases:**
    - Pre-create tables before ingestion
    - Verify schema creation works
    - Inspect table structure before loading data
    """
    try:
        result = await ingest.prepare_m5_tables(db)

        return {"status": "success", "message": "M5 tables created/verified", **result}

    except Exception as e:
        logger.error(f"Failed to prepare M5 tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kaggle/m5/schema")
async def get_m5_schema():
    """
    Get the database schema for M5 tables.

    Returns the SQL CREATE TABLE statements that would be used
    to create the M5 tables.

    **Tables:**
    - m5_calendar: Calendar dimension
    - m5_items: Item dimension
    - m5_prices: Price data
    - m5_sales: Daily sales (long format)
    """
    try:
        sql = m5_metadata.get_all_create_table_sql()

        return {
            "tables": ["m5_calendar", "m5_items", "m5_prices", "m5_sales"],
            "schema_sql": sql,
            "schemas": {
                "m5_calendar": m5_metadata.M5_CALENDAR_SCHEMA,
                "m5_items": m5_metadata.M5_ITEMS_SCHEMA,
                "m5_prices": m5_metadata.M5_PRICES_SCHEMA,
                "m5_sales": m5_metadata.M5_SALES_SCHEMA,
            },
        }

    except Exception as e:
        logger.error(f"Failed to get M5 schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))
