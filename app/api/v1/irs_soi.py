"""
IRS Statistics of Income (SOI) API endpoints.

Provides HTTP endpoints for ingesting IRS SOI data:
- Individual Income by ZIP Code
- Individual Income by County
- Migration Data (county-to-county flows)
- Business Income by ZIP Code

No API key required - all data is public domain.
"""
import logging
from enum import Enum
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.irs_soi.client import AVAILABLE_YEARS, DEFAULT_YEAR
from app.sources.irs_soi.metadata import AGI_BRACKETS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["irs-soi"])


# ========== Enums for validation ==========

class FlowType(str, Enum):
    INFLOW = "inflow"
    OUTFLOW = "outflow"
    BOTH = "both"


# ========== Request Models ==========

class ZipIncomeIngestRequest(BaseModel):
    """Request model for IRS SOI ZIP income ingestion."""
    year: Optional[int] = Field(
        None,
        description=f"Tax year (defaults to {DEFAULT_YEAR})",
        examples=[2021]
    )
    use_cache: bool = Field(
        True,
        description="Use cached downloads if available"
    )


class CountyIncomeIngestRequest(BaseModel):
    """Request model for IRS SOI county income ingestion."""
    year: Optional[int] = Field(
        None,
        description=f"Tax year (defaults to {DEFAULT_YEAR})",
        examples=[2021]
    )
    use_cache: bool = Field(
        True,
        description="Use cached downloads if available"
    )


class MigrationIngestRequest(BaseModel):
    """Request model for IRS SOI migration data ingestion."""
    year: Optional[int] = Field(
        None,
        description=f"Tax year (defaults to {DEFAULT_YEAR})",
        examples=[2021]
    )
    flow_type: FlowType = Field(
        FlowType.BOTH,
        description="Migration flow type: inflow, outflow, or both"
    )
    use_cache: bool = Field(
        True,
        description="Use cached downloads if available"
    )


class BusinessIncomeIngestRequest(BaseModel):
    """Request model for IRS SOI business income ingestion."""
    year: Optional[int] = Field(
        None,
        description=f"Tax year (defaults to {DEFAULT_YEAR})",
        examples=[2021]
    )
    use_cache: bool = Field(
        True,
        description="Use cached downloads if available"
    )


class AllDatasetsIngestRequest(BaseModel):
    """Request model for ingesting all IRS SOI datasets."""
    year: Optional[int] = Field(
        None,
        description=f"Tax year (defaults to {DEFAULT_YEAR})",
        examples=[2021]
    )
    use_cache: bool = Field(
        True,
        description="Use cached downloads if available"
    )


# ========== Endpoints ==========

@router.post("/irs-soi/zip-income/ingest")
async def ingest_zip_income_data(
    request: ZipIncomeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest IRS SOI individual income by ZIP code data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /api/v1/jobs/{job_id} to check progress.

    **Data includes:**
    - Number of returns by AGI bracket
    - Total AGI, wages, dividends, capital gains
    - Tax liability, credits, deductions
    - Breakdown by filing status

    **No API key required** (public government data)

    **Available years:** 2017-2021
    """
    year = request.year or DEFAULT_YEAR

    # Validate year
    if year not in AVAILABLE_YEARS["zip_income"]:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available. Available: {AVAILABLE_YEARS['zip_income']}"
        )

    return create_and_dispatch_job(
        db, background_tasks, source="irs_soi",
        config={
            "dataset": "zip_income",
            "year": year,
            "use_cache": request.use_cache,
        },
        message=f"IRS SOI ZIP income ingestion job created for year {year}",
    )


@router.post("/irs-soi/county-income/ingest")
async def ingest_county_income_data(
    request: CountyIncomeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest IRS SOI individual income by county data.

    Similar to ZIP data but aggregated at the county level with FIPS codes.

    **Data includes:**
    - Number of returns by AGI bracket
    - Total AGI, wages, dividends, capital gains
    - Tax liability and income sources
    - County FIPS codes for geographic joins

    **No API key required** (public government data)

    **Available years:** 2017-2021
    """
    year = request.year or DEFAULT_YEAR

    if year not in AVAILABLE_YEARS["county_income"]:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available. Available: {AVAILABLE_YEARS['county_income']}"
        )

    return create_and_dispatch_job(
        db, background_tasks, source="irs_soi",
        config={
            "dataset": "county_income",
            "year": year,
            "use_cache": request.use_cache,
        },
        message=f"IRS SOI county income ingestion job created for year {year}",
    )


@router.post("/irs-soi/migration/ingest")
async def ingest_migration_data(
    request: MigrationIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest IRS SOI county-to-county migration data.

    Migration patterns are derived from tax return address changes.

    **Data includes:**
    - County-to-county migration flows
    - Number of returns/exemptions migrating
    - Aggregate income of migrants
    - Inflow and outflow perspectives

    **Use cases:**
    - Track population movements
    - Analyze income migration patterns
    - Identify growth/decline areas

    **No API key required** (public government data)

    **Available years:** 2017-2021
    """
    year = request.year or DEFAULT_YEAR

    if year not in AVAILABLE_YEARS["migration"]:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available. Available: {AVAILABLE_YEARS['migration']}"
        )

    return create_and_dispatch_job(
        db, background_tasks, source="irs_soi",
        config={
            "dataset": "migration",
            "year": year,
            "flow_type": request.flow_type.value,
            "use_cache": request.use_cache,
        },
        message=f"IRS SOI migration ingestion job created for year {year} ({request.flow_type.value})",
    )


@router.post("/irs-soi/business-income/ingest")
async def ingest_business_income_data(
    request: BusinessIncomeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest IRS SOI business income by ZIP code data.

    Business and self-employment income statistics.

    **Data includes:**
    - Schedule C (sole proprietorships)
    - Partnership/S-corp income
    - Rental real estate income
    - Farm income
    - Self-employment tax

    **No API key required** (public government data)

    **Available years:** 2017-2021
    """
    year = request.year or DEFAULT_YEAR

    if year not in AVAILABLE_YEARS["business_income"]:
        raise HTTPException(
            status_code=400,
            detail=f"Year {year} not available. Available: {AVAILABLE_YEARS['business_income']}"
        )

    return create_and_dispatch_job(
        db, background_tasks, source="irs_soi",
        config={
            "dataset": "business_income",
            "year": year,
            "use_cache": request.use_cache,
        },
        message=f"IRS SOI business income ingestion job created for year {year}",
    )


@router.post("/irs-soi/all/ingest")
async def ingest_all_soi_datasets(
    request: AllDatasetsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest all IRS SOI datasets for a given year.

    This is a convenience endpoint that ingests:
    - ZIP code income data
    - County income data
    - Migration data (inflow + outflow)
    - Business income data

    **Note:** This downloads multiple large files. May take 10+ minutes.

    **No API key required** (public government data)
    """
    year = request.year or DEFAULT_YEAR

    return create_and_dispatch_job(
        db, background_tasks, source="irs_soi",
        config={
            "dataset": "all",
            "year": year,
            "use_cache": request.use_cache,
        },
        message=f"IRS SOI all datasets ingestion job created for year {year}",
    )


# ========== Reference Endpoints ==========

@router.get("/irs-soi/reference/agi-brackets")
async def get_agi_brackets():
    """
    Get AGI (Adjusted Gross Income) bracket definitions.

    These brackets are used to categorize income levels in IRS SOI data.
    """
    return {
        "agi_brackets": AGI_BRACKETS,
        "note": "Bracket '0' represents totals across all income levels."
    }


@router.get("/irs-soi/reference/years")
async def get_available_years():
    """
    Get available tax years for each IRS SOI dataset.
    """
    return {
        "available_years": AVAILABLE_YEARS,
        "default_year": DEFAULT_YEAR,
        "note": "Data typically lags 2-3 years from current date."
    }


@router.get("/irs-soi/datasets")
async def list_soi_datasets():
    """
    List available IRS SOI datasets and their descriptions.
    """
    return {
        "datasets": [
            {
                "id": "zip_income",
                "name": "Individual Income by ZIP Code",
                "description": "Income statistics aggregated by ZIP code and AGI bracket",
                "endpoint": "/irs-soi/zip-income/ingest",
                "table_name": "irs_soi_zip_income",
                "available_years": AVAILABLE_YEARS["zip_income"]
            },
            {
                "id": "county_income",
                "name": "Individual Income by County",
                "description": "Income statistics aggregated by county FIPS and AGI bracket",
                "endpoint": "/irs-soi/county-income/ingest",
                "table_name": "irs_soi_county_income",
                "available_years": AVAILABLE_YEARS["county_income"]
            },
            {
                "id": "migration",
                "name": "County-to-County Migration",
                "description": "Migration flows derived from tax return address changes",
                "endpoint": "/irs-soi/migration/ingest",
                "table_name": "irs_soi_migration",
                "available_years": AVAILABLE_YEARS["migration"]
            },
            {
                "id": "business_income",
                "name": "Business Income by ZIP Code",
                "description": "Business/self-employment income (Schedule C, partnerships, rental)",
                "endpoint": "/irs-soi/business-income/ingest",
                "table_name": "irs_soi_business_income",
                "available_years": AVAILABLE_YEARS["business_income"]
            }
        ],
        "note": "No API key required for any IRS SOI data (public domain)"
    }
