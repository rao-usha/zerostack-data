"""
US Trade API endpoints.

Provides HTTP endpoints for ingesting US Census Bureau International Trade data:
- Exports by HS code
- Imports by HS code
- State-level exports
- Port/district-level trade
- Trade summaries by country
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from enum import Enum

from app.core.database import get_db
from app.core.config import get_settings
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)

router = APIRouter(tags=["us_trade"])


# ========== Enums for validation ==========

class TradeType(str, Enum):
    EXPORT = "export"
    IMPORT = "import"


# ========== Request Models ==========

class ExportsHSIngestRequest(BaseModel):
    """Request model for US exports by HS code ingestion."""
    year: int = Field(
        ...,
        description="Data year (2013 to present)",
        ge=2013,
        examples=[2024]
    )
    month: Optional[int] = Field(
        None,
        description="Month (1-12). Leave empty for annual totals.",
        ge=1,
        le=12,
        examples=[6]
    )
    hs_code: Optional[str] = Field(
        None,
        description="HS code filter (2, 4, 6, or 10 digit). Example: '84' for machinery.",
        examples=["84"]
    )
    country: Optional[str] = Field(
        None,
        description="Census country code filter. Example: '5700' for China.",
        examples=["5700"]
    )


class ImportsHSIngestRequest(BaseModel):
    """Request model for US imports by HS code ingestion."""
    year: int = Field(
        ...,
        description="Data year (2013 to present)",
        ge=2013,
        examples=[2024]
    )
    month: Optional[int] = Field(
        None,
        description="Month (1-12). Leave empty for annual totals.",
        ge=1,
        le=12,
        examples=[6]
    )
    hs_code: Optional[str] = Field(
        None,
        description="HS code filter (2, 4, 6, or 10 digit). Example: '85' for electronics.",
        examples=["85"]
    )
    country: Optional[str] = Field(
        None,
        description="Census country code filter. Example: '5700' for China.",
        examples=["5700"]
    )


class StateExportsIngestRequest(BaseModel):
    """Request model for US state-level exports ingestion."""
    year: int = Field(
        ...,
        description="Data year (2013 to present)",
        ge=2013,
        examples=[2024]
    )
    month: Optional[int] = Field(
        None,
        description="Month (1-12). Leave empty for annual totals.",
        ge=1,
        le=12
    )
    state: Optional[str] = Field(
        None,
        description="State FIPS code. Example: '48' for Texas.",
        examples=["48"]
    )
    hs_code: Optional[str] = Field(
        None,
        description="HS code filter. Example: '27' for mineral fuels."
    )
    country: Optional[str] = Field(
        None,
        description="Census country code filter."
    )


class PortTradeIngestRequest(BaseModel):
    """Request model for US port-level trade ingestion."""
    year: int = Field(
        ...,
        description="Data year (2013 to present)",
        ge=2013,
        examples=[2024]
    )
    trade_type: TradeType = Field(
        ...,
        description="Export or import"
    )
    month: Optional[int] = Field(
        None,
        description="Month (1-12). Leave empty for annual totals.",
        ge=1,
        le=12
    )
    district: Optional[str] = Field(
        None,
        description="Customs district code. Example: '55' for Houston.",
        examples=["55"]
    )
    hs_code: Optional[str] = Field(
        None,
        description="HS code filter."
    )
    country: Optional[str] = Field(
        None,
        description="Census country code filter."
    )


class TradeSummaryIngestRequest(BaseModel):
    """Request model for US trade summary ingestion."""
    year: int = Field(
        ...,
        description="Data year (2013 to present)",
        ge=2013,
        examples=[2024]
    )
    month: Optional[int] = Field(
        None,
        description="Month (1-12). Leave empty for annual totals.",
        ge=1,
        le=12
    )


# ========== Endpoints ==========

@router.post("/us-trade/exports/hs/ingest")
async def ingest_exports_by_hs(
    request: ExportsHSIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest US export data by HS code (Harmonized System).

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Data includes:**
    - Export value (USD) by commodity and country
    - Monthly and year-to-date values
    - Quantity with units

    **No API key required** (Census Bureau public API)

    **HS Code Examples:**
    - '84': Machinery, mechanical appliances
    - '85': Electrical machinery, equipment
    - '87': Vehicles
    - '27': Mineral fuels, oils
    - '30': Pharmaceutical products

    **Common Country Codes:**
    - '5700': China
    - '2010': Mexico
    - '1220': Canada
    - '5880': Japan
    - '4280': Germany
    """
    settings = get_settings()
    api_key = getattr(settings, 'census_survey_api_key', None)
    return create_and_dispatch_job(
        db, background_tasks, source="us_trade",
        config={
            "dataset": "exports_hs",
            "year": request.year,
            "month": request.month,
            "hs_code": request.hs_code,
            "country": request.country,
            "api_key": api_key,
        },
        message="US exports by HS ingestion job created",
    )


@router.post("/us-trade/imports/hs/ingest")
async def ingest_imports_by_hs(
    request: ImportsHSIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest US import data by HS code (Harmonized System).

    **Data includes:**
    - General import value (total value of goods entering)
    - Consumption value (value for domestic use)
    - Duty-free and dutiable value breakdown
    - Monthly and year-to-date values
    - Quantity with units

    **No API key required** (Census Bureau public API)
    """
    settings = get_settings()
    api_key = getattr(settings, 'census_survey_api_key', None)
    return create_and_dispatch_job(
        db, background_tasks, source="us_trade",
        config={
            "dataset": "imports_hs",
            "year": request.year,
            "month": request.month,
            "hs_code": request.hs_code,
            "country": request.country,
            "api_key": api_key,
        },
        message="US imports by HS ingestion job created",
    )


@router.post("/us-trade/exports/state/ingest")
async def ingest_state_exports(
    request: StateExportsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest US state-level export data.

    Shows which states export what commodities to which countries.

    **Data includes:**
    - State-level export values by commodity and destination
    - Monthly and year-to-date values

    **State FIPS Code Examples:**
    - '48': Texas
    - '06': California
    - '36': New York
    - '17': Illinois
    - '39': Ohio

    **No API key required** (Census Bureau public API)
    """
    settings = get_settings()
    api_key = getattr(settings, 'census_survey_api_key', None)
    return create_and_dispatch_job(
        db, background_tasks, source="us_trade",
        config={
            "dataset": "state_exports",
            "year": request.year,
            "month": request.month,
            "state": request.state,
            "hs_code": request.hs_code,
            "country": request.country,
            "api_key": api_key,
        },
        message="US state exports ingestion job created",
    )


@router.post("/us-trade/port/ingest")
async def ingest_port_trade(
    request: PortTradeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest US trade data by customs district (port of entry).

    Shows trade volumes through each port by commodity and country.

    **District Code Examples:**
    - '55': Houston, TX
    - '68': Los Angeles, CA
    - '10': New York, NY
    - '60': Seattle, WA
    - '39': Miami, FL
    - '57': Laredo, TX

    **No API key required** (Census Bureau public API)
    """
    settings = get_settings()
    api_key = getattr(settings, 'census_survey_api_key', None)
    return create_and_dispatch_job(
        db, background_tasks, source="us_trade",
        config={
            "dataset": "port_trade",
            "year": request.year,
            "trade_type": request.trade_type.value,
            "month": request.month,
            "district": request.district,
            "hs_code": request.hs_code,
            "country": request.country,
            "api_key": api_key,
        },
        message=f"US port {request.trade_type.value}s ingestion job created",
    )


@router.post("/us-trade/summary/ingest")
async def ingest_trade_summary(
    request: TradeSummaryIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest aggregated US trade summary by country.

    Fetches exports and imports, then aggregates by country to show
    total trade and trade balance.

    **Data includes:**
    - Total exports by country
    - Total imports by country
    - Total trade (exports + imports)
    - Trade balance (exports - imports)

    **Use cases:**
    - Identify top trading partners
    - Analyze trade deficits/surpluses
    - Track trade concentration

    **No API key required** (Census Bureau public API)
    """
    settings = get_settings()
    api_key = getattr(settings, 'census_survey_api_key', None)
    return create_and_dispatch_job(
        db, background_tasks, source="us_trade",
        config={
            "dataset": "summary",
            "year": request.year,
            "month": request.month,
            "api_key": api_key,
        },
        message="US trade summary ingestion job created",
    )


@router.get("/us-trade/datasets")
async def list_us_trade_datasets():
    """
    List available US Trade datasets and their descriptions.
    """
    return {
        "datasets": [
            {
                "id": "exports_hs",
                "name": "US Exports by HS Code",
                "description": "Export values by Harmonized System commodity code",
                "endpoint": "/us-trade/exports/hs/ingest",
                "source": "Census Bureau International Trade API",
                "filters": ["year", "month", "hs_code", "country"]
            },
            {
                "id": "imports_hs",
                "name": "US Imports by HS Code",
                "description": "Import values with duty breakdown by HS code",
                "endpoint": "/us-trade/imports/hs/ingest",
                "source": "Census Bureau International Trade API",
                "filters": ["year", "month", "hs_code", "country"]
            },
            {
                "id": "exports_state",
                "name": "US State Exports",
                "description": "State-level exports by commodity and destination",
                "endpoint": "/us-trade/exports/state/ingest",
                "source": "Census Bureau International Trade API",
                "filters": ["year", "month", "state", "hs_code", "country"]
            },
            {
                "id": "port_trade",
                "name": "US Trade by Port/District",
                "description": "Trade by customs district (port of entry)",
                "endpoint": "/us-trade/port/ingest",
                "source": "Census Bureau International Trade API",
                "filters": ["year", "month", "trade_type", "district", "hs_code", "country"]
            },
            {
                "id": "trade_summary",
                "name": "US Trade Summary by Country",
                "description": "Aggregated exports, imports, and balance by country",
                "endpoint": "/us-trade/summary/ingest",
                "source": "Census Bureau International Trade API",
                "filters": ["year", "month"]
            }
        ],
        "reference_codes": {
            "hs_chapters": {
                "84": "Machinery, mechanical appliances",
                "85": "Electrical machinery, equipment",
                "87": "Vehicles",
                "27": "Mineral fuels, oils",
                "30": "Pharmaceutical products",
                "90": "Optical, measuring instruments",
                "39": "Plastics",
                "29": "Organic chemicals"
            },
            "top_countries": {
                "5700": "China",
                "2010": "Mexico",
                "1220": "Canada",
                "5880": "Japan",
                "4280": "Germany",
                "5800": "South Korea",
                "5830": "Taiwan",
                "4120": "United Kingdom"
            },
            "top_districts": {
                "55": "Houston, TX",
                "68": "Los Angeles, CA",
                "10": "New York, NY",
                "60": "Seattle, WA",
                "39": "Miami, FL",
                "57": "Laredo, TX"
            }
        },
        "note": "No API key required. Data available from 2013 to present."
    }


@router.get("/us-trade/reference/hs-chapters")
async def get_hs_chapters():
    """
    Get list of HS (Harmonized System) commodity chapters (2-digit codes).
    """
    from app.sources.us_trade.client import HS_CHAPTERS
    return {
        "hs_chapters": [
            {"code": code, "description": desc}
            for code, desc in sorted(HS_CHAPTERS.items())
        ],
        "note": "Use 2-digit code as hs_code filter for chapter-level data"
    }


@router.get("/us-trade/reference/countries")
async def get_trading_partners():
    """
    Get list of Census country codes for major US trading partners.
    """
    from app.sources.us_trade.client import TOP_TRADING_PARTNERS
    return {
        "countries": [
            {"code": code, "name": name}
            for code, name in sorted(TOP_TRADING_PARTNERS.items(), key=lambda x: x[1])
        ],
        "note": "Use country code as country filter"
    }


@router.get("/us-trade/reference/districts")
async def get_customs_districts():
    """
    Get list of US Customs Districts (port codes).
    """
    from app.sources.us_trade.client import CUSTOMS_DISTRICTS
    return {
        "districts": [
            {"code": code, "name": name}
            for code, name in sorted(CUSTOMS_DISTRICTS.items())
        ],
        "note": "Use district code as district filter in port trade queries"
    }


@router.get("/us-trade/reference/states")
async def get_state_codes():
    """
    Get list of state FIPS codes for state export queries.
    """
    from app.sources.us_trade.client import STATE_FIPS
    return {
        "states": [
            {"fips_code": code, "name": name}
            for code, name in sorted(STATE_FIPS.items(), key=lambda x: x[1])
        ],
        "note": "Use FIPS code as state filter in state export queries"
    }
