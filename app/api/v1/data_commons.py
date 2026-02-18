"""
Google Data Commons API endpoints.

Provides HTTP endpoints for ingesting Data Commons data:
- Statistical observations for places
- US state-level data for common variables
- Custom variable/place combinations
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.sources.data_commons.client import STATISTICAL_VARIABLES, PLACE_DCIDS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["data_commons"])


# ========== Request Models ==========


class StatVarIngestRequest(BaseModel):
    """Request model for statistical variable ingestion."""

    variable_dcid: str = Field(
        default="Count_Person",
        description="Statistical variable DCID (e.g., Count_Person, Median_Income_Household)",
        examples=["Count_Person"],
    )
    places: List[str] = Field(
        default=["geoId/06", "geoId/48", "geoId/36"],
        description="List of place DCIDs to fetch data for",
        examples=[["geoId/06", "geoId/48"]],
    )


class PlaceStatsIngestRequest(BaseModel):
    """Request model for place statistics ingestion."""

    place_dcid: str = Field(
        default="geoId/06",
        description="Place DCID (e.g., geoId/06 for California)",
        examples=["geoId/06"],
    )
    variables: Optional[List[str]] = Field(
        None,
        description="List of variable DCIDs (defaults to common variables)",
        examples=[["Count_Person", "Median_Income_Household"]],
    )


class USStateDataRequest(BaseModel):
    """Request model for US state data ingestion."""

    variables: Optional[List[str]] = Field(
        None,
        description="List of variable DCIDs (defaults to common demographics)",
        examples=[
            ["Count_Person", "Median_Income_Household", "UnemploymentRate_Person"]
        ],
    )


# ========== Endpoints ==========


@router.post("/data-commons/stat-var/ingest")
async def ingest_stat_var_data(
    request: StatVarIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Data Commons statistical variable data for specified places.

    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.

    **Common Statistical Variables:**
    - **Count_Person**: Total population
    - **Median_Income_Household**: Median household income
    - **UnemploymentRate_Person**: Unemployment rate
    - **Median_Age_Person**: Median age
    - **Count_Household**: Number of households
    - **Count_CriminalActivities_CombinedCrime**: Total crimes

    **Place DCID Format:**
    - US States: geoId/XX (e.g., geoId/06 for California)
    - US Counties: geoId/XXXXX (5-digit FIPS)
    - Countries: country/XXX (e.g., country/USA)

    **API Key:** Optional but recommended for higher rate limits.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="data_commons",
        config={
            "dataset": "observations",
            "variable_dcid": request.variable_dcid,
            "places": request.places,
        },
        message=f"Data Commons ingestion job created for {request.variable_dcid}",
    )


@router.post("/data-commons/place-stats/ingest")
async def ingest_place_stats_data(
    request: PlaceStatsIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest multiple statistical variables for a single place.

    Fetches data for many variables at once for a given location.
    Useful for building a complete statistical profile of a place.

    **Place Examples:**
    - geoId/06 - California
    - geoId/48 - Texas
    - geoId/36 - New York
    - country/USA - United States
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="data_commons",
        config={
            "dataset": "place_stats",
            "place_dcid": request.place_dcid,
            "variables": request.variables,
        },
        message=f"Data Commons place stats job created for {request.place_dcid}",
    )


@router.post("/data-commons/us-states/ingest")
async def ingest_us_state_data(
    request: USStateDataRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest statistical data for all US states.

    Fetches specified variables for all 50 US states plus DC.
    Great for building comparative state-level datasets.

    **Default Variables:**
    - Count_Person (Population)
    - Median_Income_Household
    - UnemploymentRate_Person
    - Median_Age_Person
    - Count_Household
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="data_commons",
        config={
            "dataset": "us_states",
            "variables": request.variables,
        },
        message="Data Commons US states ingestion job created",
    )


@router.get("/data-commons/variables")
async def list_statistical_variables():
    """
    List available statistical variables and their descriptions.
    """
    return {
        "variables": STATISTICAL_VARIABLES,
        "categories": {
            "demographics": [
                "Count_Person",
                "Count_Person_Male",
                "Count_Person_Female",
                "Median_Age_Person",
                "Count_Household",
                "Count_HousingUnit",
            ],
            "income": [
                "Median_Income_Person",
                "Median_Income_Household",
                "Mean_Income_Person",
                "Count_Person_BelowPovertyLine",
            ],
            "employment": [
                "Count_Person_Employed",
                "Count_Person_Unemployed",
                "UnemploymentRate_Person",
            ],
            "crime": [
                "Count_CriminalActivities_CombinedCrime",
                "Count_CriminalActivities_ViolentCrime",
                "Count_CriminalActivities_PropertyCrime",
            ],
            "health": ["Count_Death", "LifeExpectancy_Person"],
            "economy": [
                "Amount_EconomicActivity_GrossDomesticProduction_RealValue",
                "GrowthRate_Amount_EconomicActivity_GrossDomesticProduction",
            ],
        },
    }


@router.get("/data-commons/places")
async def list_common_places():
    """
    List common place DCIDs for reference.
    """
    return {
        "us_states": {
            "Alabama": "geoId/01",
            "Alaska": "geoId/02",
            "Arizona": "geoId/04",
            "Arkansas": "geoId/05",
            "California": "geoId/06",
            "Colorado": "geoId/08",
            "Connecticut": "geoId/09",
            "Delaware": "geoId/10",
            "District of Columbia": "geoId/11",
            "Florida": "geoId/12",
            "Georgia": "geoId/13",
            "Hawaii": "geoId/15",
            "Idaho": "geoId/16",
            "Illinois": "geoId/17",
            "Indiana": "geoId/18",
            "Iowa": "geoId/19",
            "Kansas": "geoId/20",
            "Kentucky": "geoId/21",
            "Louisiana": "geoId/22",
            "Maine": "geoId/23",
            "Maryland": "geoId/24",
            "Massachusetts": "geoId/25",
            "Michigan": "geoId/26",
            "Minnesota": "geoId/27",
            "Mississippi": "geoId/28",
            "Missouri": "geoId/29",
            "Montana": "geoId/30",
            "Nebraska": "geoId/31",
            "Nevada": "geoId/32",
            "New Hampshire": "geoId/33",
            "New Jersey": "geoId/34",
            "New Mexico": "geoId/35",
            "New York": "geoId/36",
            "North Carolina": "geoId/37",
            "North Dakota": "geoId/38",
            "Ohio": "geoId/39",
            "Oklahoma": "geoId/40",
            "Oregon": "geoId/41",
            "Pennsylvania": "geoId/42",
            "Rhode Island": "geoId/44",
            "South Carolina": "geoId/45",
            "South Dakota": "geoId/46",
            "Tennessee": "geoId/47",
            "Texas": "geoId/48",
            "Utah": "geoId/49",
            "Vermont": "geoId/50",
            "Virginia": "geoId/51",
            "Washington": "geoId/53",
            "West Virginia": "geoId/54",
            "Wisconsin": "geoId/55",
            "Wyoming": "geoId/56",
        },
        "countries": {
            "United States": "country/USA",
            "Canada": "country/CAN",
            "Mexico": "country/MEX",
            "United Kingdom": "country/GBR",
            "Germany": "country/DEU",
            "France": "country/FRA",
            "Japan": "country/JPN",
            "China": "country/CHN",
            "India": "country/IND",
            "Brazil": "country/BRA",
        },
        "place_format_info": {
            "us_state": "geoId/XX (2-digit FIPS)",
            "us_county": "geoId/XXXXX (5-digit FIPS)",
            "country": "country/XXX (ISO 3166-1 alpha-3)",
        },
    }
