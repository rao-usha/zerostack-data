"""
BEA (Bureau of Economic Analysis) API endpoints.

Provides HTTP endpoints for ingesting BEA data:
- NIPA (National Income and Product Accounts) - GDP, PCE, Investment
- Regional Economic Accounts - GDP by state/county, Personal Income
- GDP by Industry - Value added, gross output by industry
- International Transactions - Trade balance, foreign investment
"""
import logging
from typing import Dict, Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from enum import Enum

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.bea import ingest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["bea"])


# ========== Enums for validation ==========

class NIPAFrequency(str, Enum):
    ANNUAL = "A"
    QUARTERLY = "Q"
    MONTHLY = "M"


class RegionalGeoFips(str, Enum):
    STATE = "STATE"
    COUNTY = "COUNTY"
    MSA = "MSA"


class GDPIndustryFrequency(str, Enum):
    ANNUAL = "A"
    QUARTERLY = "Q"


# ========== Request Models ==========

class NIPAIngestRequest(BaseModel):
    """Request model for BEA NIPA ingestion."""
    table_name: str = Field(
        default="T10101",
        description="NIPA table name (e.g., T10101 for GDP, T20100 for Personal Income)",
        example="T10101"
    )
    frequency: NIPAFrequency = Field(
        default=NIPAFrequency.ANNUAL,
        description="Data frequency: A (annual), Q (quarterly), M (monthly)"
    )
    year: Optional[str] = Field(
        None,
        description="Year(s) to retrieve - 'ALL', single year, or comma-separated. Defaults to last 10 years.",
        example="2020,2021,2022,2023,2024"
    )


class RegionalIngestRequest(BaseModel):
    """Request model for BEA Regional Economic Accounts ingestion."""
    table_name: str = Field(
        default="SAGDP2N",
        description="Regional table name (e.g., SAGDP2N for GDP by state, SAINC1 for Personal Income)",
        example="SAGDP2N"
    )
    line_code: str = Field(
        default="1",
        description="Line code for specific measure within table",
        example="1"
    )
    geo_fips: str = Field(
        default="STATE",
        description="Geographic area: STATE, COUNTY, MSA, or specific FIPS code",
        example="STATE"
    )
    year: Optional[str] = Field(
        None,
        description="Year(s) to retrieve. Defaults to last 10 years.",
        example="2020,2021,2022,2023,2024"
    )


class GDPIndustryIngestRequest(BaseModel):
    """Request model for BEA GDP by Industry ingestion."""
    table_id: str = Field(
        default="1",
        description="Table ID (1=Value Added, 5=% of GDP, 6=Real Value Added, 10=Gross Output)",
        example="1"
    )
    frequency: GDPIndustryFrequency = Field(
        default=GDPIndustryFrequency.ANNUAL,
        description="Data frequency: A (annual), Q (quarterly)"
    )
    year: Optional[str] = Field(
        None,
        description="Year(s) to retrieve. Defaults to last 5 years.",
        example="2020,2021,2022,2023,2024"
    )
    industry: str = Field(
        default="ALL",
        description="Industry code or 'ALL' for all industries",
        example="ALL"
    )


class InternationalIngestRequest(BaseModel):
    """Request model for BEA International Transactions ingestion."""
    indicator: str = Field(
        default="BalGds",
        description="Transaction indicator (e.g., BalGds for Balance on Goods)",
        example="BalGds"
    )
    area_or_country: str = Field(
        default="AllCountries",
        description="Geographic area or 'AllCountries'",
        example="AllCountries"
    )
    frequency: GDPIndustryFrequency = Field(
        default=GDPIndustryFrequency.ANNUAL,
        description="Data frequency: A (annual), Q (quarterly)"
    )
    year: Optional[str] = Field(
        None,
        description="Year(s) to retrieve. Defaults to ALL.",
        example="ALL"
    )


# ========== Endpoints ==========

@router.post("/bea/nipa/ingest")
async def ingest_nipa_data(
    request: NIPAIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BEA NIPA (National Income and Product Accounts) data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Common NIPA Tables:**
    - **T10101**: Gross Domestic Product (GDP)
    - **T10105**: GDP Percent Change
    - **T10106**: Real GDP (Chained Dollars)
    - **T20100**: Personal Income and Its Disposition
    - **T20200**: Personal Consumption Expenditures (PCE)
    - **T30100**: Government Receipts and Expenditures
    - **T50100**: Saving and Investment
    - **T60100**: Corporate Profits by Industry
    
    **API Key Required:** Set BEA_API_KEY in environment variables.
    Get a free key at: https://apps.bea.gov/api/signup/
    """
    try:
        job_config = {
            "dataset": "nipa",
            "table_name": request.table_name,
            "frequency": request.frequency.value,
            "year": request.year
        }
        
        job = IngestionJob(
            source="bea",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_nipa_ingestion,
            job.id,
            request.table_name,
            request.frequency.value,
            request.year
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"BEA NIPA ingestion job created for table {request.table_name}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create BEA NIPA job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bea/regional/ingest")
async def ingest_regional_data(
    request: RegionalIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BEA Regional Economic Accounts data.
    
    **Common Regional Tables:**
    - **SAGDP2N**: GDP by State (all industries)
    - **SAGDP9N**: Real GDP by State
    - **SAINC1**: Personal Income by State
    - **SAINC4**: Personal Income and Employment by State
    - **SAINC51**: Per Capita Personal Income by State
    - **CAINC1**: Personal Income by County
    - **CAGDP2**: GDP by County
    - **MAGDP2**: GDP by Metro Area (MSA)
    
    **Geographic Options:**
    - `STATE`: All 50 states + DC
    - `COUNTY`: All US counties
    - `MSA`: Metropolitan Statistical Areas
    - Specific FIPS code (e.g., "06000" for California)
    
    **API Key Required:** Set BEA_API_KEY in environment variables.
    """
    try:
        job_config = {
            "dataset": "regional",
            "table_name": request.table_name,
            "line_code": request.line_code,
            "geo_fips": request.geo_fips,
            "year": request.year
        }
        
        job = IngestionJob(
            source="bea",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_regional_ingestion,
            job.id,
            request.table_name,
            request.line_code,
            request.geo_fips,
            request.year
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"BEA Regional ingestion job created for table {request.table_name}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create BEA Regional job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bea/gdp-industry/ingest")
async def ingest_gdp_industry_data(
    request: GDPIndustryIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BEA GDP by Industry data.
    
    **Table IDs:**
    - **1**: Value Added by Industry
    - **5**: Value Added by Industry as Percentage of GDP
    - **6**: Real Value Added by Industry
    - **10**: Gross Output by Industry
    - **11**: Intermediate Inputs by Industry
    
    Provides breakdown of economic output by industry sector (NAICS codes).
    
    **API Key Required:** Set BEA_API_KEY in environment variables.
    """
    try:
        job_config = {
            "dataset": "gdp_industry",
            "table_id": request.table_id,
            "frequency": request.frequency.value,
            "year": request.year,
            "industry": request.industry
        }
        
        job = IngestionJob(
            source="bea",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_gdp_industry_ingestion,
            job.id,
            request.table_id,
            request.frequency.value,
            request.year,
            request.industry
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"BEA GDP by Industry ingestion job created for table {request.table_id}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create BEA GDP by Industry job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bea/international/ingest")
async def ingest_international_data(
    request: InternationalIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BEA International Transactions data.
    
    **Common Indicators:**
    - **BalGds**: Balance on Goods
    - **BalServ**: Balance on Services
    - **BalCurAcct**: Current Account Balance
    - **ExpGds**: Exports of Goods
    - **ImpGds**: Imports of Goods
    
    Provides data on US trade balance and international investment.
    
    **API Key Required:** Set BEA_API_KEY in environment variables.
    """
    try:
        job_config = {
            "dataset": "international",
            "indicator": request.indicator,
            "area_or_country": request.area_or_country,
            "frequency": request.frequency.value,
            "year": request.year
        }
        
        job = IngestionJob(
            source="bea",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_international_ingestion,
            job.id,
            request.indicator,
            request.area_or_country,
            request.frequency.value,
            request.year
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"BEA International ingestion job created for indicator {request.indicator}",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create BEA International job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bea/datasets")
async def list_bea_datasets():
    """
    List available BEA datasets and common tables.
    """
    return {
        "datasets": [
            {
                "id": "nipa",
                "name": "National Income and Product Accounts (NIPA)",
                "description": "GDP, Personal Income, PCE, Government spending, Saving & Investment",
                "endpoint": "/bea/nipa/ingest",
                "common_tables": {
                    "T10101": "Gross Domestic Product",
                    "T10106": "Real GDP (Chained Dollars)",
                    "T20100": "Personal Income",
                    "T20200": "Personal Consumption Expenditures",
                    "T30100": "Government Receipts & Expenditures",
                    "T50100": "Saving and Investment",
                    "T60100": "Corporate Profits"
                }
            },
            {
                "id": "regional",
                "name": "Regional Economic Accounts",
                "description": "State, county, and metro area economic data",
                "endpoint": "/bea/regional/ingest",
                "common_tables": {
                    "SAGDP2N": "GDP by State",
                    "SAGDP9N": "Real GDP by State",
                    "SAINC1": "Personal Income by State",
                    "SAINC51": "Per Capita Income by State",
                    "CAINC1": "Personal Income by County",
                    "CAGDP2": "GDP by County"
                }
            },
            {
                "id": "gdp_industry",
                "name": "GDP by Industry",
                "description": "Value added and gross output by industry sector",
                "endpoint": "/bea/gdp-industry/ingest",
                "table_ids": {
                    "1": "Value Added by Industry",
                    "5": "Value Added as % of GDP",
                    "6": "Real Value Added",
                    "10": "Gross Output by Industry"
                }
            },
            {
                "id": "international",
                "name": "International Transactions",
                "description": "Trade balance, exports, imports, foreign investment",
                "endpoint": "/bea/international/ingest",
                "common_indicators": {
                    "BalGds": "Balance on Goods",
                    "BalServ": "Balance on Services",
                    "ExpGds": "Exports of Goods",
                    "ImpGds": "Imports of Goods"
                }
            }
        ],
        "api_key_info": {
            "required": True,
            "env_variable": "BEA_API_KEY",
            "signup_url": "https://apps.bea.gov/api/signup/"
        }
    }


# ========== Background Task Functions ==========

async def _run_nipa_ingestion(
    job_id: int,
    table_name: str,
    frequency: str,
    year: Optional[str]
):
    """Run BEA NIPA ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_bea_api_key()
        
        if not api_key:
            raise ValueError(
                "BEA_API_KEY is required. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )
        
        await ingest.ingest_nipa_data(
            db=db,
            job_id=job_id,
            table_name=table_name,
            frequency=frequency,
            year=year,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background BEA NIPA ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_regional_ingestion(
    job_id: int,
    table_name: str,
    line_code: str,
    geo_fips: str,
    year: Optional[str]
):
    """Run BEA Regional ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_bea_api_key()
        
        if not api_key:
            raise ValueError(
                "BEA_API_KEY is required. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )
        
        await ingest.ingest_regional_data(
            db=db,
            job_id=job_id,
            table_name=table_name,
            line_code=line_code,
            geo_fips=geo_fips,
            year=year,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background BEA Regional ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_gdp_industry_ingestion(
    job_id: int,
    table_id: str,
    frequency: str,
    year: Optional[str],
    industry: str
):
    """Run BEA GDP by Industry ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_bea_api_key()
        
        if not api_key:
            raise ValueError(
                "BEA_API_KEY is required. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )
        
        await ingest.ingest_gdp_by_industry_data(
            db=db,
            job_id=job_id,
            table_id=table_id,
            frequency=frequency,
            year=year,
            industry=industry,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background BEA GDP by Industry ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_international_ingestion(
    job_id: int,
    indicator: str,
    area_or_country: str,
    frequency: str,
    year: Optional[str]
):
    """Run BEA International ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_bea_api_key()
        
        if not api_key:
            raise ValueError(
                "BEA_API_KEY is required. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )
        
        await ingest.ingest_international_data(
            db=db,
            job_id=job_id,
            indicator=indicator,
            area_or_country=area_or_country,
            frequency=frequency,
            year=year,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background BEA International ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
