"""
International Economic Data API endpoints.

Provides HTTP endpoints for ingesting data from:
- World Bank Open Data (WDI)
- International Monetary Fund (IMF)
- OECD
- Bank for International Settlements (BIS)

All sources are free, no API key required.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.international_econ.client import (
    COMMON_WDI_INDICATORS,
    MAJOR_ECONOMIES,
    G7_COUNTRIES,
    G20_COUNTRIES,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["international_econ"])


# ============================================================================
# Request Models
# ============================================================================


class WorldBankWDIIngestRequest(BaseModel):
    """Request model for World Bank WDI ingestion."""

    indicators: List[str] = Field(
        default=["NY.GDP.MKTP.CD", "NY.GDP.MKTP.KD.ZG", "SP.POP.TOTL"],
        description="List of WDI indicator codes (e.g., NY.GDP.MKTP.CD for GDP)",
        examples=[["NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL"]],
    )
    countries: Optional[List[str]] = Field(
        None,
        description="List of country codes (None for all countries). Use ISO3 codes like USA, GBR, JPN",
        examples=[["USA", "GBR", "JPN", "DEU", "FRA"]],
    )
    start_year: int = Field(
        default=2015, description="Start year for data", ge=1960, le=2025
    )
    end_year: Optional[int] = Field(
        None, description="End year for data (None for current year)", ge=1960, le=2025
    )


class WorldBankCountriesIngestRequest(BaseModel):
    """Request model for World Bank countries metadata ingestion."""

    pass  # No parameters needed


class WorldBankIndicatorsIngestRequest(BaseModel):
    """Request model for World Bank indicators metadata ingestion."""

    search: Optional[str] = Field(
        None, description="Optional search term to filter indicators"
    )
    max_results: int = Field(
        default=1000, description="Maximum number of indicators to fetch", ge=1, le=5000
    )


class IMFIFSIngestRequest(BaseModel):
    """Request model for IMF IFS data ingestion."""

    indicator: str = Field(default="NGDP_R_XDC", description="IFS indicator code")
    countries: Optional[List[str]] = Field(
        None, description="List of country codes (None for all)"
    )
    start_year: str = Field(default="2015", description="Start year")
    end_year: Optional[str] = Field(
        None, description="End year (None for current year)"
    )


class BISEERIngestRequest(BaseModel):
    """Request model for BIS Effective Exchange Rate data ingestion."""

    countries: Optional[List[str]] = Field(
        None, description="List of country codes (None for all)"
    )
    eer_type: str = Field(
        default="R", description="Exchange rate type: R (Real) or N (Nominal)"
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class BISPropertyIngestRequest(BaseModel):
    """Request model for BIS property price data ingestion."""

    countries: Optional[List[str]] = Field(
        None, description="List of country codes (None for all)"
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class OECDMEIIngestRequest(BaseModel):
    """Request model for OECD Main Economic Indicators ingestion."""

    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes. Common: USA, GBR, DEU, FRA, JPN, CAN, ITA, AUS",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]],
    )
    subjects: Optional[List[str]] = Field(
        None,
        description="List of subject codes (economic indicators). None for all available.",
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class OECDKEIIngestRequest(BaseModel):
    """Request model for OECD Key Economic Indicators ingestion."""

    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]],
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class OECDLaborIngestRequest(BaseModel):
    """Request model for OECD Labour Force Statistics ingestion."""

    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]],
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class OECDTradeIngestRequest(BaseModel):
    """Request model for OECD Trade in Services ingestion."""

    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of reporter country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]],
    )
    start_period: str = Field(default="2015", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


class OECDTaxIngestRequest(BaseModel):
    """Request model for OECD Tax Revenue Statistics ingestion."""

    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]],
    )
    start_period: str = Field(default="2000", description="Start period (year)")
    end_period: Optional[str] = Field(
        None, description="End period (year, None for current)"
    )


# ============================================================================
# World Bank Endpoints
# ============================================================================


@router.post("/international/worldbank/wdi/ingest")
async def ingest_worldbank_wdi_data(
    request: WorldBankWDIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest World Bank World Development Indicators (WDI) data.

    This is the primary World Bank data endpoint providing access to 1,600+
    indicators for 200+ countries covering:
    - GDP and economic growth
    - Population and demographics
    - Health and education
    - Trade and finance
    - Environment and climate

    **No API key required** - World Bank data is free and open.

    **Common Indicators:**
    - `NY.GDP.MKTP.CD` - GDP (current US$)
    - `NY.GDP.MKTP.KD.ZG` - GDP growth (annual %)
    - `NY.GDP.PCAP.CD` - GDP per capita (current US$)
    - `SP.POP.TOTL` - Population, total
    - `SP.POP.GROW` - Population growth (annual %)
    - `FP.CPI.TOTL.ZG` - Inflation, consumer prices (annual %)
    - `SL.UEM.TOTL.ZS` - Unemployment, total (% of labor force)

    **Example Request:**
    ```json
    {
        "indicators": ["NY.GDP.MKTP.CD", "SP.POP.TOTL"],
        "countries": ["USA", "CHN", "JPN"],
        "start_year": 2015,
        "end_year": 2023
    }
    ```
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_worldbank",
        config={
            "source": "worldbank",
            "dataset": "wdi",
            "indicators": request.indicators,
            "countries": request.countries,
            "start_year": request.start_year,
            "end_year": request.end_year,
        },
        message="World Bank WDI ingestion job created",
    )


@router.post("/international/worldbank/countries/ingest")
async def ingest_worldbank_countries_data(
    background_tasks: BackgroundTasks, db: Session = Depends(get_db)
):
    """
    Ingest World Bank countries metadata.

    Fetches metadata for all 200+ countries/regions including:
    - Country names and ISO codes
    - Regions (e.g., East Asia, Europe)
    - Income levels (e.g., High income, Low income)
    - Capital cities and coordinates

    **No API key required.**

    This is useful as reference data for joining with WDI indicator data.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_worldbank",
        config={
            "source": "worldbank",
            "dataset": "countries",
        },
        message="World Bank countries metadata ingestion job created",
    )


@router.post("/international/worldbank/indicators/ingest")
async def ingest_worldbank_indicators_metadata(
    request: WorldBankIndicatorsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest World Bank indicators metadata.

    Fetches metadata for 1,600+ available indicators including:
    - Indicator codes and names
    - Descriptions and source information
    - Topic classifications

    **No API key required.**

    Use the `search` parameter to filter indicators (e.g., "GDP", "population").
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_worldbank",
        config={
            "source": "worldbank",
            "dataset": "indicators",
            "search": request.search,
            "max_results": request.max_results,
        },
        message="World Bank indicators metadata ingestion job created",
    )


@router.get("/international/worldbank/indicators/common")
async def get_common_wdi_indicators():
    """
    Get list of commonly used World Development Indicators.

    Returns categorized list of popular indicators for:
    - Economic (GDP, trade)
    - Population
    - Labor market
    - Prices/Inflation
    - Poverty
    - Health
    - Education
    - Environment

    Use these codes with the `/international/worldbank/wdi/ingest` endpoint.
    """
    from app.sources.international_econ.metadata import WDI_INDICATOR_CATEGORIES

    return {
        "categories": WDI_INDICATOR_CATEGORIES,
        "common_indicators": COMMON_WDI_INDICATORS,
        "country_groups": {
            "major_economies": MAJOR_ECONOMIES,
            "g7": G7_COUNTRIES,
            "g20": G20_COUNTRIES,
        },
    }


# ============================================================================
# IMF Endpoints
# ============================================================================


@router.post("/international/imf/ifs/ingest")
async def ingest_imf_ifs_data(
    request: IMFIFSIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest IMF International Financial Statistics (IFS) data.

    IFS provides comprehensive macroeconomic data including:
    - Exchange rates
    - Interest rates
    - Prices and production
    - Government finance
    - National accounts

    **No API key required.**

    **Note:** IMF API uses SDMX format which can be complex.
    Results may vary based on data availability.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_imf",
        config={
            "source": "imf",
            "dataset": "ifs",
            "indicator": request.indicator,
            "countries": request.countries,
            "start_year": request.start_year,
            "end_year": request.end_year,
        },
        message="IMF IFS ingestion job created",
    )


# ============================================================================
# OECD Endpoints
# ============================================================================


@router.post("/international/oecd/mei/ingest")
async def ingest_oecd_mei_data(
    request: OECDMEIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OECD Main Economic Indicators (MEI) data.

    OECD MEI provides key economic indicators for OECD member countries including:
    - Industrial production
    - Consumer prices (inflation)
    - Unemployment rates
    - Trade statistics
    - Retail trade
    - Business confidence
    - Leading indicators

    **No API key required.**

    **Coverage:** 38 OECD member countries + key partners

    **Common Country Codes:**
    - USA, GBR, DEU, FRA, JPN, CAN, ITA, AUS, KOR, MEX

    **Example Request:**
    ```json
    {
        "countries": ["USA", "GBR", "DEU", "FRA", "JPN"],
        "start_period": "2020",
        "end_period": "2023"
    }
    ```
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_oecd",
        config={
            "source": "oecd",
            "dataset": "mei",
            "countries": request.countries,
            "subjects": request.subjects,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="OECD MEI ingestion job created",
    )


@router.post("/international/oecd/kei/ingest")
async def ingest_oecd_kei_data(
    request: OECDKEIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OECD Key Economic Indicators (KEI) data.

    KEI is the OECD's Main Economic Indicators dataset including:
    - Industrial Production indices
    - Consumer Price indices (inflation)
    - Unemployment rates
    - Retail trade
    - Manufacturing output

    **No API key required.**
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_oecd",
        config={
            "source": "oecd",
            "dataset": "kei",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="OECD KEI ingestion job created",
    )


@router.post("/international/oecd/labor/ingest")
async def ingest_oecd_labor_data(
    request: OECDLaborIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OECD Annual Labour Force Statistics (ALFS) data.

    ALFS provides comprehensive labor market data including:
    - Employment and unemployment rates
    - Labor force participation
    - Employment by sector
    - Demographics breakdowns (age, sex)

    **No API key required.**
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_oecd",
        config={
            "source": "oecd",
            "dataset": "alfs",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="OECD Labor statistics ingestion job created",
    )


@router.post("/international/oecd/trade/ingest")
async def ingest_oecd_trade_data(
    request: OECDTradeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OECD Balanced Trade in Services (BATIS) data.

    BATIS provides bilateral trade in services data:
    - Exports and imports of services
    - By service category
    - Between trading partners

    **No API key required.**
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_oecd",
        config={
            "source": "oecd",
            "dataset": "batis",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="OECD Trade statistics ingestion job created",
    )


@router.post("/international/oecd/tax/ingest")
async def ingest_oecd_tax_data(
    request: OECDTaxIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OECD Tax Revenue Statistics data.

    Revenue Statistics provides comprehensive tax data including:
    - Total tax revenue
    - Tax breakdown by type (income, VAT, property, etc.)
    - Government level (central, state, local)
    - As percentage of GDP
    - Historical trends since 1965

    **No API key required.**
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_oecd",
        config={
            "source": "oecd",
            "dataset": "tax",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="OECD Tax revenue statistics ingestion job created",
    )


# ============================================================================
# BIS Endpoints
# ============================================================================


@router.post("/international/bis/eer/ingest")
async def ingest_bis_eer_data(
    request: BISEERIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest BIS Effective Exchange Rate data.

    BIS publishes nominal and real effective exchange rates for
    60+ economies. These indices are key indicators of:
    - Currency competitiveness
    - Trade-weighted exchange rate movements
    - Monetary policy transmission

    **No API key required.**

    **Types:**
    - `R` - Real Effective Exchange Rate (inflation-adjusted)
    - `N` - Nominal Effective Exchange Rate
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_bis",
        config={
            "source": "bis",
            "dataset": "eer",
            "countries": request.countries,
            "eer_type": request.eer_type,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="BIS Effective Exchange Rate ingestion job created",
    )


@router.post("/international/bis/property/ingest")
async def ingest_bis_property_data(
    request: BISPropertyIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest BIS residential property price data.

    BIS publishes selected residential property price statistics
    covering 60+ countries. Data includes:
    - Residential property price indices
    - Real (inflation-adjusted) prices
    - Historical trends

    **No API key required.**

    Useful for:
    - Real estate market analysis
    - Cross-country comparisons
    - Financial stability monitoring
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="international_econ_bis",
        config={
            "source": "bis",
            "dataset": "property",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period,
        },
        message="BIS property price ingestion job created",
    )
