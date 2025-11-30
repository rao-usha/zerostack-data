"""
EIA API endpoints.

Provides HTTP endpoints for ingesting EIA data.
"""
import logging
from typing import Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.eia import ingest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["eia"])


class EIAPetroleumIngestRequest(BaseModel):
    """Request model for EIA petroleum ingestion."""
    subcategory: str = Field(
        default="consumption",
        description="Subcategory (consumption, production, imports, exports, stocks)"
    )
    route: Optional[str] = Field(
        None,
        description="Optional custom API route"
    )
    frequency: str = Field(
        default="annual",
        description="Data frequency (annual, monthly, weekly, daily)"
    )
    start: Optional[str] = Field(
        None,
        description="Start date (format depends on frequency)"
    )
    end: Optional[str] = Field(
        None,
        description="End date (format depends on frequency)"
    )
    facets: Optional[Dict[str, str]] = Field(
        None,
        description="Optional facet filters (e.g., {'process': 'VPP', 'product': 'EPP0'})"
    )


class EIANaturalGasIngestRequest(BaseModel):
    """Request model for EIA natural gas ingestion."""
    subcategory: str = Field(
        default="consumption",
        description="Subcategory (consumption, production, storage, prices)"
    )
    route: Optional[str] = Field(
        None,
        description="Optional custom API route"
    )
    frequency: str = Field(
        default="annual",
        description="Data frequency (annual, monthly)"
    )
    start: Optional[str] = Field(
        None,
        description="Start date"
    )
    end: Optional[str] = Field(
        None,
        description="End date"
    )
    facets: Optional[Dict[str, str]] = Field(
        None,
        description="Optional facet filters"
    )


class EIAElectricityIngestRequest(BaseModel):
    """Request model for EIA electricity ingestion."""
    subcategory: str = Field(
        default="retail_sales",
        description="Subcategory (retail_sales, generation, revenue, customers)"
    )
    route: Optional[str] = Field(
        None,
        description="Optional custom API route"
    )
    frequency: str = Field(
        default="annual",
        description="Data frequency (annual, monthly)"
    )
    start: Optional[str] = Field(
        None,
        description="Start date"
    )
    end: Optional[str] = Field(
        None,
        description="End date"
    )
    facets: Optional[Dict[str, str]] = Field(
        None,
        description="Optional facet filters (e.g., {'sectorid': 'RES', 'stateid': 'CA'})"
    )


class EIARetailGasPricesIngestRequest(BaseModel):
    """Request model for EIA retail gas prices ingestion."""
    frequency: str = Field(
        default="weekly",
        description="Data frequency (weekly, daily)"
    )
    start: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM-DD)"
    )
    end: Optional[str] = Field(
        None,
        description="End date (YYYY-MM-DD)"
    )
    facets: Optional[Dict[str, str]] = Field(
        None,
        description="Optional facet filters (e.g., {'product': 'EPM0', 'area': 'NUS'})"
    )


class EIASTEOIngestRequest(BaseModel):
    """Request model for EIA STEO ingestion."""
    frequency: str = Field(
        default="monthly",
        description="Data frequency (monthly)"
    )
    start: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM)"
    )
    end: Optional[str] = Field(
        None,
        description="End date (YYYY-MM)"
    )
    facets: Optional[Dict[str, str]] = Field(
        None,
        description="Optional facet filters"
    )


@router.post("/eia/petroleum/ingest")
async def ingest_petroleum_data(
    request: EIAPetroleumIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest EIA petroleum data.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **Subcategories:**
    - consumption: Petroleum consumption by product
    - production: Petroleum production by product
    - imports: Petroleum imports by product
    - exports: Petroleum exports by product
    - stocks: Petroleum stocks by product
    
    **Note:** EIA API key is REQUIRED.
    Set EIA_API_KEY environment variable.
    Get a free key at: https://www.eia.gov/opendata/register.php
    """
    try:
        # Create job
        job_config = {
            "category": "petroleum",
            "subcategory": request.subcategory,
            "route": request.route,
            "frequency": request.frequency,
            "start": request.start,
            "end": request.end,
            "facets": request.facets
        }
        
        job = IngestionJob(
            source="eia",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Run ingestion in background
        background_tasks.add_task(
            _run_petroleum_ingestion,
            job.id,
            request.subcategory,
            request.route,
            request.frequency,
            request.start,
            request.end,
            request.facets
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "EIA petroleum ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create EIA petroleum ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/eia/natural-gas/ingest")
async def ingest_natural_gas_data(
    request: EIANaturalGasIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest EIA natural gas data.
    
    **Subcategories:**
    - consumption: Natural gas consumption by sector
    - production: Natural gas production
    - storage: Natural gas storage levels
    - prices: Natural gas prices
    """
    try:
        job_config = {
            "category": "natural_gas",
            "subcategory": request.subcategory,
            "route": request.route,
            "frequency": request.frequency,
            "start": request.start,
            "end": request.end,
            "facets": request.facets
        }
        
        job = IngestionJob(
            source="eia",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_natural_gas_ingestion,
            job.id,
            request.subcategory,
            request.route,
            request.frequency,
            request.start,
            request.end,
            request.facets
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "EIA natural gas ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create EIA natural gas ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/eia/electricity/ingest")
async def ingest_electricity_data(
    request: EIAElectricityIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest EIA electricity data.
    
    **Subcategories:**
    - retail_sales: Electricity retail sales by sector and state
    - generation: Electricity generation by fuel type
    - revenue: Electricity revenue by sector
    - customers: Number of electricity customers
    """
    try:
        job_config = {
            "category": "electricity",
            "subcategory": request.subcategory,
            "route": request.route,
            "frequency": request.frequency,
            "start": request.start,
            "end": request.end,
            "facets": request.facets
        }
        
        job = IngestionJob(
            source="eia",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_electricity_ingestion,
            job.id,
            request.subcategory,
            request.route,
            request.frequency,
            request.start,
            request.end,
            request.facets
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "EIA electricity ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create EIA electricity ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/eia/retail-gas-prices/ingest")
async def ingest_retail_gas_prices(
    request: EIARetailGasPricesIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest EIA retail gasoline prices.
    
    Includes regular, midgrade, premium, and diesel prices by region.
    """
    try:
        job_config = {
            "category": "retail_gas_prices",
            "frequency": request.frequency,
            "start": request.start,
            "end": request.end,
            "facets": request.facets
        }
        
        job = IngestionJob(
            source="eia",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_retail_gas_prices_ingestion,
            job.id,
            request.frequency,
            request.start,
            request.end,
            request.facets
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "EIA retail gas prices ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create EIA retail gas prices ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/eia/steo/ingest")
async def ingest_steo_projections(
    request: EIASTEOIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest EIA Short-Term Energy Outlook (STEO) projections.
    
    Includes monthly projections for energy supply, demand, and prices.
    """
    try:
        job_config = {
            "category": "steo",
            "frequency": request.frequency,
            "start": request.start,
            "end": request.end,
            "facets": request.facets
        }
        
        job = IngestionJob(
            source="eia",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_steo_ingestion,
            job.id,
            request.frequency,
            request.start,
            request.end,
            request.facets
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "EIA STEO ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create EIA STEO ingestion job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Background task functions

async def _run_petroleum_ingestion(
    job_id: int,
    subcategory: str,
    route: Optional[str],
    frequency: str,
    start: Optional[str],
    end: Optional[str],
    facets: Optional[Dict[str, str]]
):
    """Run EIA petroleum ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_eia_api_key()
        
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        
        await ingest.ingest_eia_petroleum_data(
            db=db,
            job_id=job_id,
            subcategory=subcategory,
            route=route,
            frequency=frequency,
            start=start,
            end=end,
            facets=facets,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background EIA petroleum ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_natural_gas_ingestion(
    job_id: int,
    subcategory: str,
    route: Optional[str],
    frequency: str,
    start: Optional[str],
    end: Optional[str],
    facets: Optional[Dict[str, str]]
):
    """Run EIA natural gas ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_eia_api_key()
        
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        
        await ingest.ingest_eia_natural_gas_data(
            db=db,
            job_id=job_id,
            subcategory=subcategory,
            route=route,
            frequency=frequency,
            start=start,
            end=end,
            facets=facets,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background EIA natural gas ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_electricity_ingestion(
    job_id: int,
    subcategory: str,
    route: Optional[str],
    frequency: str,
    start: Optional[str],
    end: Optional[str],
    facets: Optional[Dict[str, str]]
):
    """Run EIA electricity ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_eia_api_key()
        
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        
        await ingest.ingest_eia_electricity_data(
            db=db,
            job_id=job_id,
            subcategory=subcategory,
            route=route,
            frequency=frequency,
            start=start,
            end=end,
            facets=facets,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background EIA electricity ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_retail_gas_prices_ingestion(
    job_id: int,
    frequency: str,
    start: Optional[str],
    end: Optional[str],
    facets: Optional[Dict[str, str]]
):
    """Run EIA retail gas prices ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_eia_api_key()
        
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        
        await ingest.ingest_eia_retail_gas_prices(
            db=db,
            job_id=job_id,
            frequency=frequency,
            start=start,
            end=end,
            facets=facets,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background EIA retail gas prices ingestion failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_steo_ingestion(
    job_id: int,
    frequency: str,
    start: Optional[str],
    end: Optional[str],
    facets: Optional[Dict[str, str]]
):
    """Run EIA STEO ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_eia_api_key()
        
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        
        await ingest.ingest_eia_steo_projections(
            db=db,
            job_id=job_id,
            frequency=frequency,
            start=start,
            end=end,
            facets=facets,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background EIA STEO ingestion failed: {e}", exc_info=True)
    finally:
        db.close()

