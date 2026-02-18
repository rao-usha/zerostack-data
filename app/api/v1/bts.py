"""
BTS (Bureau of Transportation Statistics) API endpoints.

Provides HTTP endpoints for ingesting BTS data:
- Border Crossing Entry Data
- Freight Analysis Framework (FAF5) regional data
- Vehicle Miles Traveled (VMT)
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

router = APIRouter(tags=["bts"])


# ========== Enums for validation ==========

class BorderType(str, Enum):
    US_CANADA = "US-Canada Border"
    US_MEXICO = "US-Mexico Border"


class BorderMeasure(str, Enum):
    TRUCKS = "Trucks"
    LOADED_TRUCK_CONTAINERS = "Loaded Truck Containers"
    EMPTY_TRUCK_CONTAINERS = "Empty Truck Containers"
    TRAINS = "Trains"
    LOADED_RAIL_CONTAINERS = "Loaded Rail Containers"
    EMPTY_RAIL_CONTAINERS = "Empty Rail Containers"
    TRAIN_PASSENGERS = "Train Passengers"
    BUSES = "Buses"
    BUS_PASSENGERS = "Bus Passengers"
    PERSONAL_VEHICLES = "Personal Vehicles"
    PERSONAL_VEHICLE_PASSENGERS = "Personal Vehicle Passengers"
    PEDESTRIANS = "Pedestrians"


class FAFVersion(str, Enum):
    REGIONAL_2018_2024 = "regional_2018_2024"
    REGIONAL_FORECASTS = "regional_forecasts"
    STATE_2018_2024 = "state_2018_2024"


# ========== Request Models ==========

class BorderCrossingIngestRequest(BaseModel):
    """Request model for BTS border crossing ingestion."""
    start_date: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM format). Defaults to 5 years ago.",
        examples=["2020-01"]
    )
    end_date: Optional[str] = Field(
        None,
        description="End date (YYYY-MM format). Defaults to current month.",
        examples=["2024-12"]
    )
    state: Optional[str] = Field(
        None,
        description="Filter by state code (e.g., 'TX', 'CA', 'NY')",
        examples=["TX"]
    )
    border: Optional[BorderType] = Field(
        None,
        description="Filter by border type"
    )
    measure: Optional[BorderMeasure] = Field(
        None,
        description="Filter by measure type (Trucks, Containers, etc.)"
    )


class VMTIngestRequest(BaseModel):
    """Request model for BTS Vehicle Miles Traveled ingestion."""
    start_date: Optional[str] = Field(
        None,
        description="Start date (YYYY-MM format). Defaults to 3 years ago.",
        examples=["2022-01"]
    )
    end_date: Optional[str] = Field(
        None,
        description="End date (YYYY-MM format). Defaults to current month.",
        examples=["2024-12"]
    )
    state: Optional[str] = Field(
        None,
        description="Filter by state name (e.g., 'Texas', 'California')",
        examples=["Texas"]
    )


class FAFIngestRequest(BaseModel):
    """Request model for BTS FAF5 Freight data ingestion."""
    version: FAFVersion = Field(
        default=FAFVersion.REGIONAL_2018_2024,
        description="FAF version to download"
    )


# ========== Endpoints ==========

@router.post("/bts/border-crossing/ingest")
async def ingest_border_crossing_data(
    request: BorderCrossingIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BTS border crossing entry data.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Data includes:**
    - Monthly statistics at all US ports of entry
    - Trucks, containers, trains, buses, personal vehicles, pedestrians
    - US-Canada and US-Mexico borders

    **No API key required** (public data via Socrata)

    **Example filters:**
    - Get Texas border crossings: `state="TX"`
    - Get Mexico border only: `border="US-Mexico Border"`
    - Get truck traffic only: `measure="Trucks"`
    """
    settings = get_settings()
    app_token = settings.get_bts_app_token()
    return create_and_dispatch_job(
        db, background_tasks, source="bts",
        config={
            "dataset": "border_crossing",
            "start_date": request.start_date,
            "end_date": request.end_date,
            "state": request.state,
            "border": request.border.value if request.border else None,
            "measure": request.measure.value if request.measure else None,
            "app_token": app_token,
        },
        message="BTS border crossing ingestion job created",
    )


@router.post("/bts/vmt/ingest")
async def ingest_vmt_data(
    request: VMTIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BTS Vehicle Miles Traveled (VMT) data.

    VMT is a key economic indicator measuring traffic volume on all public roads.
    Often used as a proxy for consumer activity and economic health.

    **Data includes:**
    - Monthly VMT by state
    - Seasonally adjusted values
    - Year-over-year percent change

    **No API key required** (public data via Socrata)
    """
    settings = get_settings()
    app_token = settings.get_bts_app_token()
    return create_and_dispatch_job(
        db, background_tasks, source="bts",
        config={
            "dataset": "vmt",
            "start_date": request.start_date,
            "end_date": request.end_date,
            "state": request.state,
            "app_token": app_token,
        },
        message="BTS VMT ingestion job created",
    )


@router.post("/bts/faf/ingest")
async def ingest_faf_data(
    request: FAFIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest BTS Freight Analysis Framework (FAF5) data.

    FAF5 provides comprehensive freight flow data for freight planning and analysis.

    **Data includes:**
    - Freight tonnage, value, and ton-miles
    - Origin-destination pairs (132 FAF zones)
    - 43 commodity types (SCTG2 codes)
    - Transport modes (Truck, Rail, Water, Air, Pipeline)
    - Trade types (Domestic, Import, Export)

    **Versions available:**
    - `regional_2018_2024`: Regional database 2018-2024 (recommended)
    - `regional_forecasts`: Regional with forecasts to 2050
    - `state_2018_2024`: State-level 2018-2024

    **Note:** This downloads a large CSV file (~100MB+). May take several minutes.
    """
    settings = get_settings()
    app_token = settings.get_bts_app_token()
    return create_and_dispatch_job(
        db, background_tasks, source="bts",
        config={
            "dataset": "faf",
            "version": request.version.value,
            "app_token": app_token,
        },
        message="BTS FAF5 ingestion job created (large download, may take several minutes)",
    )


@router.get("/bts/datasets")
async def list_bts_datasets():
    """
    List available BTS datasets and their descriptions.
    """
    return {
        "datasets": [
            {
                "id": "border_crossing",
                "name": "Border Crossing Entry Data",
                "description": "Monthly statistics on crossings at US ports of entry",
                "endpoint": "/bts/border-crossing/ingest",
                "source": "Socrata API",
                "filters": ["state", "border", "measure", "date_range"]
            },
            {
                "id": "vmt",
                "name": "Vehicle Miles Traveled (VMT)",
                "description": "Monthly traffic volume on all public roads by state",
                "endpoint": "/bts/vmt/ingest",
                "source": "Socrata API",
                "filters": ["state", "date_range"]
            },
            {
                "id": "faf_regional",
                "name": "Freight Analysis Framework (FAF5)",
                "description": "Freight tonnage, value, ton-miles by O-D, commodity, mode",
                "endpoint": "/bts/faf/ingest",
                "source": "CSV Download",
                "versions": ["regional_2018_2024", "regional_forecasts", "state_2018_2024"]
            }
        ],
        "note": "No API key required for BTS public data"
    }
