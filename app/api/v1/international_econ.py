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
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.international_econ import ingest
from app.sources.international_econ.client import (
    COMMON_WDI_INDICATORS,
    MAJOR_ECONOMIES,
    G7_COUNTRIES,
    G20_COUNTRIES
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
        examples=[["NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL"]]
    )
    countries: Optional[List[str]] = Field(
        None,
        description="List of country codes (None for all countries). Use ISO3 codes like USA, GBR, JPN",
        examples=[["USA", "GBR", "JPN", "DEU", "FRA"]]
    )
    start_year: int = Field(
        default=2015,
        description="Start year for data",
        ge=1960,
        le=2025
    )
    end_year: Optional[int] = Field(
        None,
        description="End year for data (None for current year)",
        ge=1960,
        le=2025
    )


class WorldBankCountriesIngestRequest(BaseModel):
    """Request model for World Bank countries metadata ingestion."""
    pass  # No parameters needed


class WorldBankIndicatorsIngestRequest(BaseModel):
    """Request model for World Bank indicators metadata ingestion."""
    search: Optional[str] = Field(
        None,
        description="Optional search term to filter indicators"
    )
    max_results: int = Field(
        default=1000,
        description="Maximum number of indicators to fetch",
        ge=1,
        le=5000
    )


class IMFIFSIngestRequest(BaseModel):
    """Request model for IMF IFS data ingestion."""
    indicator: str = Field(
        default="NGDP_R_XDC",
        description="IFS indicator code"
    )
    countries: Optional[List[str]] = Field(
        None,
        description="List of country codes (None for all)"
    )
    start_year: str = Field(
        default="2015",
        description="Start year"
    )
    end_year: Optional[str] = Field(
        None,
        description="End year (None for current year)"
    )


class BISEERIngestRequest(BaseModel):
    """Request model for BIS Effective Exchange Rate data ingestion."""
    countries: Optional[List[str]] = Field(
        None,
        description="List of country codes (None for all)"
    )
    eer_type: str = Field(
        default="R",
        description="Exchange rate type: R (Real) or N (Nominal)"
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class BISPropertyIngestRequest(BaseModel):
    """Request model for BIS property price data ingestion."""
    countries: Optional[List[str]] = Field(
        None,
        description="List of country codes (None for all)"
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class OECDMEIIngestRequest(BaseModel):
    """Request model for OECD Main Economic Indicators ingestion."""
    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes. Common: USA, GBR, DEU, FRA, JPN, CAN, ITA, AUS",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]]
    )
    subjects: Optional[List[str]] = Field(
        None,
        description="List of subject codes (economic indicators). None for all available."
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class OECDKEIIngestRequest(BaseModel):
    """Request model for OECD Key Economic Indicators ingestion."""
    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]]
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class OECDLaborIngestRequest(BaseModel):
    """Request model for OECD Labour Force Statistics ingestion."""
    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]]
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class OECDTradeIngestRequest(BaseModel):
    """Request model for OECD Trade in Services ingestion."""
    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of reporter country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]]
    )
    start_period: str = Field(
        default="2015",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


class OECDTaxIngestRequest(BaseModel):
    """Request model for OECD Tax Revenue Statistics ingestion."""
    countries: Optional[List[str]] = Field(
        default=["USA", "GBR", "DEU", "FRA", "JPN"],
        description="List of OECD country codes",
        examples=[["USA", "GBR", "DEU", "FRA", "JPN"]]
    )
    start_period: str = Field(
        default="2000",
        description="Start period (year)"
    )
    end_period: Optional[str] = Field(
        None,
        description="End period (year, None for current)"
    )


# ============================================================================
# World Bank Endpoints
# ============================================================================

@router.post("/international/worldbank/wdi/ingest")
async def ingest_worldbank_wdi_data(
    request: WorldBankWDIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "worldbank",
            "dataset": "wdi",
            "indicators": request.indicators,
            "countries": request.countries,
            "start_year": request.start_year,
            "end_year": request.end_year
        }
        
        job = IngestionJob(
            source="international_econ_worldbank",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_worldbank_wdi_ingestion,
            job.id,
            request.indicators,
            request.countries,
            request.start_year,
            request.end_year
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "World Bank WDI ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "indicators_requested": len(request.indicators),
            "countries": request.countries or "all"
        }
    
    except Exception as e:
        logger.error(f"Failed to create World Bank WDI ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/worldbank/countries/ingest")
async def ingest_worldbank_countries_data(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "worldbank",
            "dataset": "countries"
        }
        
        job = IngestionJob(
            source="international_econ_worldbank",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_worldbank_countries_ingestion,
            job.id
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "World Bank countries metadata ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create World Bank countries ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/worldbank/indicators/ingest")
async def ingest_worldbank_indicators_metadata(
    request: WorldBankIndicatorsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "worldbank",
            "dataset": "indicators",
            "search": request.search,
            "max_results": request.max_results
        }
        
        job = IngestionJob(
            source="international_econ_worldbank",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_worldbank_indicators_ingestion,
            job.id,
            request.search,
            request.max_results
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "World Bank indicators metadata ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "search_term": request.search,
            "max_results": request.max_results
        }
    
    except Exception as e:
        logger.error(f"Failed to create World Bank indicators ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
            "g20": G20_COUNTRIES
        }
    }


# ============================================================================
# IMF Endpoints
# ============================================================================

@router.post("/international/imf/ifs/ingest")
async def ingest_imf_ifs_data(
    request: IMFIFSIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "imf",
            "dataset": "ifs",
            "indicator": request.indicator,
            "countries": request.countries,
            "start_year": request.start_year,
            "end_year": request.end_year
        }
        
        job = IngestionJob(
            source="international_econ_imf",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_imf_ifs_ingestion,
            job.id,
            request.indicator,
            request.countries,
            request.start_year,
            request.end_year
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "IMF IFS ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "indicator": request.indicator
        }
    
    except Exception as e:
        logger.error(f"Failed to create IMF IFS ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# OECD Endpoints
# ============================================================================

@router.post("/international/oecd/mei/ingest")
async def ingest_oecd_mei_data(
    request: OECDMEIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "oecd",
            "dataset": "mei",
            "countries": request.countries,
            "subjects": request.subjects,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_oecd",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_oecd_mei_ingestion,
            job.id,
            request.countries,
            request.subjects,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "OECD MEI ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "countries": request.countries
        }
    
    except Exception as e:
        logger.error(f"Failed to create OECD MEI ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/oecd/kei/ingest")
async def ingest_oecd_kei_data(
    request: OECDKEIIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "oecd",
            "dataset": "kei",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_oecd",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_oecd_kei_ingestion,
            job.id,
            request.countries,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "OECD KEI ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "countries": request.countries
        }
    
    except Exception as e:
        logger.error(f"Failed to create OECD KEI ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/oecd/labor/ingest")
async def ingest_oecd_labor_data(
    request: OECDLaborIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "oecd",
            "dataset": "alfs",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_oecd",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_oecd_labor_ingestion,
            job.id,
            request.countries,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "OECD Labor statistics ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "countries": request.countries
        }
    
    except Exception as e:
        logger.error(f"Failed to create OECD Labor ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/oecd/trade/ingest")
async def ingest_oecd_trade_data(
    request: OECDTradeIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest OECD Balanced Trade in Services (BATIS) data.
    
    BATIS provides bilateral trade in services data:
    - Exports and imports of services
    - By service category
    - Between trading partners
    
    **No API key required.**
    """
    try:
        job_config = {
            "source": "oecd",
            "dataset": "batis",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_oecd",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_oecd_trade_ingestion,
            job.id,
            request.countries,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "OECD Trade statistics ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "countries": request.countries
        }
    
    except Exception as e:
        logger.error(f"Failed to create OECD Trade ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/oecd/tax/ingest")
async def ingest_oecd_tax_data(
    request: OECDTaxIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "oecd",
            "dataset": "tax",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_oecd",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_oecd_tax_ingestion,
            job.id,
            request.countries,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "OECD Tax revenue statistics ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "countries": request.countries
        }
    
    except Exception as e:
        logger.error(f"Failed to create OECD Tax ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# BIS Endpoints
# ============================================================================

@router.post("/international/bis/eer/ingest")
async def ingest_bis_eer_data(
    request: BISEERIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "bis",
            "dataset": "eer",
            "countries": request.countries,
            "eer_type": request.eer_type,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_bis",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_bis_eer_ingestion,
            job.id,
            request.countries,
            request.eer_type,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "BIS Effective Exchange Rate ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
            "eer_type": request.eer_type
        }
    
    except Exception as e:
        logger.error(f"Failed to create BIS EER ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/international/bis/property/ingest")
async def ingest_bis_property_data(
    request: BISPropertyIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
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
    try:
        job_config = {
            "source": "bis",
            "dataset": "property",
            "countries": request.countries,
            "start_period": request.start_period,
            "end_period": request.end_period
        }
        
        job = IngestionJob(
            source="international_econ_bis",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_bis_property_ingestion,
            job.id,
            request.countries,
            request.start_period,
            request.end_period
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "BIS property price ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create BIS property price ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Background Task Functions
# ============================================================================

async def _run_worldbank_wdi_ingestion(
    job_id: int,
    indicators: List[str],
    countries: Optional[List[str]],
    start_year: int,
    end_year: Optional[int]
):
    """Run World Bank WDI ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_worldbank_wdi(
            db=db,
            job_id=job_id,
            indicators=indicators,
            countries=countries,
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        logger.error(f"Background World Bank WDI ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_worldbank_countries_ingestion(job_id: int):
    """Run World Bank countries ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_worldbank_countries(
            db=db,
            job_id=job_id
        )
    except Exception as e:
        logger.error(f"Background World Bank countries ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_worldbank_indicators_ingestion(
    job_id: int,
    search: Optional[str],
    max_results: int
):
    """Run World Bank indicators ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_worldbank_indicators(
            db=db,
            job_id=job_id,
            search=search,
            max_results=max_results
        )
    except Exception as e:
        logger.error(f"Background World Bank indicators ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_imf_ifs_ingestion(
    job_id: int,
    indicator: str,
    countries: Optional[List[str]],
    start_year: str,
    end_year: Optional[str]
):
    """Run IMF IFS ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_imf_ifs(
            db=db,
            job_id=job_id,
            indicator=indicator,
            countries=countries,
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        logger.error(f"Background IMF IFS ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_oecd_mei_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    subjects: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run OECD MEI ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_oecd_mei(
            db=db,
            job_id=job_id,
            countries=countries,
            subjects=subjects,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background OECD MEI ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_oecd_kei_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run OECD KEI ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_oecd_kei(
            db=db,
            job_id=job_id,
            countries=countries,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background OECD KEI ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_oecd_labor_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run OECD Labor ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_oecd_labor(
            db=db,
            job_id=job_id,
            countries=countries,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background OECD Labor ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_oecd_trade_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run OECD Trade ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_oecd_trade(
            db=db,
            job_id=job_id,
            countries=countries,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background OECD Trade ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_oecd_tax_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run OECD Tax ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_oecd_tax(
            db=db,
            job_id=job_id,
            countries=countries,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background OECD Tax ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_bis_eer_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    eer_type: str,
    start_period: str,
    end_period: Optional[str]
):
    """Run BIS EER ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_bis_eer(
            db=db,
            job_id=job_id,
            countries=countries,
            eer_type=eer_type,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background BIS EER ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_bis_property_ingestion(
    job_id: int,
    countries: Optional[List[str]],
    start_period: str,
    end_period: Optional[str]
):
    """Run BIS property price ingestion in background."""
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        await ingest.ingest_bis_property_prices(
            db=db,
            job_id=job_id,
            countries=countries,
            start_period=start_period,
            end_period=end_period
        )
    except Exception as e:
        logger.error(f"Background BIS property price ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
