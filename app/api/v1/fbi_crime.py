"""
FBI Crime Data Explorer API endpoints.

Provides HTTP endpoints for ingesting FBI crime data including:
- UCR Crime Estimates (national and state)
- Summarized Agency Data
- NIBRS Incident-Based Data
- Hate Crime Statistics
- LEOKA (Law Enforcement Officers Killed and Assaulted)

API Documentation: https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi
API Key: Free from https://api.data.gov/signup/
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.job_helpers import create_and_dispatch_job
from app.sources.fbi_crime import ingest, metadata
from app.sources.fbi_crime.client import FBICrimeClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fbi_crime"])


# ============================================
# Request/Response Models
# ============================================

class FBIEstimatesRequest(BaseModel):
    """Request model for FBI crime estimates ingestion."""
    scope: str = Field(
        default="national",
        description="Data scope: 'national' or 'state'"
    )
    offenses: Optional[List[str]] = Field(
        None,
        description="List of offense types (uses all if not provided)"
    )
    states: Optional[List[str]] = Field(
        None,
        description="List of state abbreviations (for state-level data)"
    )


class FBISummarizedRequest(BaseModel):
    """Request model for FBI summarized data ingestion."""
    states: Optional[List[str]] = Field(
        None,
        description="List of state abbreviations (defaults to all)"
    )
    offenses: Optional[List[str]] = Field(
        None,
        description="List of offense types"
    )
    since: int = Field(
        default=2010,
        description="Start year"
    )
    until: int = Field(
        default=2023,
        description="End year"
    )


class FBINIBRSRequest(BaseModel):
    """Request model for FBI NIBRS data ingestion."""
    states: Optional[List[str]] = Field(
        None,
        description="List of state abbreviations"
    )
    variables: Optional[List[str]] = Field(
        None,
        description="List of NIBRS variables (count, offense, etc.)"
    )


class FBIHateCrimeRequest(BaseModel):
    """Request model for FBI hate crime data ingestion."""
    states: Optional[List[str]] = Field(
        None,
        description="List of state abbreviations (None for national only)"
    )


class FBILEOKARequest(BaseModel):
    """Request model for FBI LEOKA data ingestion."""
    states: Optional[List[str]] = Field(
        None,
        description="List of state abbreviations (None for national only)"
    )


class FBIBatchIngestRequest(BaseModel):
    """Request model for batch FBI crime data ingestion."""
    datasets: List[str] = Field(
        ...,
        description="List of datasets to ingest (estimates_national, estimates_state, summarized, nibrs, hate_crime, leoka)"
    )
    include_states: bool = Field(
        default=False,
        description="Include state-level data (increases API calls)"
    )


class FBIDatasetsResponse(BaseModel):
    """Response model for available datasets."""
    datasets: List[dict]


class FBIOffensesResponse(BaseModel):
    """Response model for available offense types."""
    offenses: List[dict]


class FBIStatesResponse(BaseModel):
    """Response model for available states."""
    states: List[dict]


# ============================================
# Information Endpoints
# ============================================

@router.get("/fbi-crime/datasets", response_model=FBIDatasetsResponse)
async def get_available_datasets():
    """
    Get available FBI crime datasets.

    Returns all datasets that can be ingested from the FBI Crime Data Explorer.
    """
    try:
        datasets_info = []
        for dataset_type in metadata.AVAILABLE_DATASETS:
            datasets_info.append({
                "name": dataset_type,
                "display_name": metadata.get_dataset_display_name(dataset_type.split("_")[0]),
                "description": metadata.get_dataset_description(dataset_type.split("_")[0])
            })

        return {"datasets": datasets_info}

    except Exception as e:
        logger.error(f"Failed to get FBI crime datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fbi-crime/offenses", response_model=FBIOffensesResponse)
async def get_available_offenses():
    """
    Get available offense types for FBI crime data.

    Returns all offense types that can be queried from the API.
    """
    try:
        offenses_info = []
        for offense in FBICrimeClient.OFFENSE_TYPES:
            offenses_info.append({
                "code": offense,
                "display_name": offense.replace("-", " ").title(),
                "category": "violent" if offense in ["violent-crime", "homicide", "rape", "robbery", "aggravated-assault"] else "property"
            })

        return {"offenses": offenses_info}

    except Exception as e:
        logger.error(f"Failed to get offense types: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fbi-crime/states", response_model=FBIStatesResponse)
async def get_available_states():
    """
    Get available state abbreviations for FBI crime data.

    Returns all U.S. states that can be queried.
    """
    try:
        states_info = []
        state_names = {
            "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
            "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
            "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
            "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
            "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
            "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
            "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
            "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
            "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
            "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
            "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
            "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
            "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
        }

        for abbr in FBICrimeClient.STATE_ABBRS:
            states_info.append({
                "abbreviation": abbr,
                "name": state_names.get(abbr, abbr)
            })

        return {"states": states_info}

    except Exception as e:
        logger.error(f"Failed to get states: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Ingestion Endpoints
# ============================================

@router.post("/fbi-crime/estimates/ingest")
async def ingest_fbi_estimates(
    request: FBIEstimatesRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FBI crime estimates data.

    **Scope Options:**
    - `national`: National-level crime estimates (default)
    - `state`: State-level crime estimates

    **Available Offenses:**
    - violent-crime, property-crime, homicide, rape, robbery
    - aggravated-assault, burglary, larceny, motor-vehicle-theft, arson

    **Example:**
    ```json
    {
        "scope": "national",
        "offenses": ["violent-crime", "property-crime", "homicide"]
    }
    ```

    **Note:** Requires FBI_CRIME_API_KEY environment variable.
    Get a free key at: https://api.data.gov/signup/
    """
    # Validate scope
    if request.scope not in ["national", "state"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid scope. Must be 'national' or 'state'"
        )

    # Validate offenses if provided
    if request.offenses:
        invalid_offenses = [o for o in request.offenses if o not in FBICrimeClient.OFFENSE_TYPES]
        if invalid_offenses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid offenses: {', '.join(invalid_offenses)}"
            )

    return create_and_dispatch_job(
        db, background_tasks, source="fbi_crime",
        config={
            "dataset": "estimates",
            "scope": request.scope,
            "offenses": request.offenses,
            "states": request.states,
        },
        message="FBI Crime estimates ingestion job created",
    )


@router.post("/fbi-crime/summarized/ingest")
async def ingest_fbi_summarized(
    request: FBISummarizedRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FBI summarized agency crime data.

    Summarized data provides crime counts by agency over time.

    **Example:**
    ```json
    {
        "states": ["CA", "TX", "NY"],
        "offenses": ["violent-crime", "property-crime"],
        "since": 2015,
        "until": 2023
    }
    ```
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fbi_crime",
        config={
            "dataset": "summarized",
            "states": request.states,
            "offenses": request.offenses,
            "since": request.since,
            "until": request.until,
        },
        message="FBI Crime summarized ingestion job created",
    )


@router.post("/fbi-crime/nibrs/ingest")
async def ingest_fbi_nibrs(
    request: FBINIBRSRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FBI NIBRS (National Incident-Based Reporting) data.

    NIBRS provides detailed incident-level crime data.

    **Available Variables:**
    - count, offense, victim/age, victim/race, victim/sex
    - offender/age, offender/race, offender/sex, relationship

    **Example:**
    ```json
    {
        "states": ["CA", "TX", "NY", "FL"],
        "variables": ["count", "offense"]
    }
    ```
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fbi_crime",
        config={
            "dataset": "nibrs",
            "states": request.states,
            "variables": request.variables,
        },
        message="FBI NIBRS ingestion job created",
    )


@router.post("/fbi-crime/hate-crime/ingest")
async def ingest_fbi_hate_crime(
    request: FBIHateCrimeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FBI hate crime statistics.

    Includes national and optionally state-level hate crime data.

    **Example:**
    ```json
    {
        "states": ["CA", "TX", "NY"]
    }
    ```

    Or for national data only:
    ```json
    {}
    ```
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fbi_crime",
        config={
            "dataset": "hate_crime",
            "states": request.states,
        },
        message="FBI Hate Crime ingestion job created",
    )


@router.post("/fbi-crime/leoka/ingest")
async def ingest_fbi_leoka(
    request: FBILEOKARequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest FBI LEOKA (Law Enforcement Officers Killed and Assaulted) data.

    Includes national and optionally state-level LEOKA data.

    **Example:**
    ```json
    {
        "states": ["CA", "TX", "NY"]
    }
    ```
    """
    return create_and_dispatch_job(
        db, background_tasks, source="fbi_crime",
        config={
            "dataset": "leoka",
            "states": request.states,
        },
        message="FBI LEOKA ingestion job created",
    )


@router.post("/fbi-crime/ingest/all")
async def ingest_all_fbi_crime_data(
    request: FBIBatchIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest multiple FBI crime datasets at once.

    **Available Datasets:**
    - estimates_national: National crime estimates
    - estimates_state: State-level crime estimates
    - summarized: Summarized agency data
    - nibrs: NIBRS incident-based data
    - hate_crime: Hate crime statistics
    - leoka: Law enforcement officers killed/assaulted

    **Example:**
    ```json
    {
        "datasets": ["estimates_national", "hate_crime", "leoka"],
        "include_states": false
    }
    ```
    """
    try:
        valid_datasets = [
            "estimates_national", "estimates_state", "summarized",
            "nibrs", "hate_crime", "leoka"
        ]

        invalid_datasets = [d for d in request.datasets if d not in valid_datasets]
        if invalid_datasets:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid datasets: {', '.join(invalid_datasets)}. "
                       f"Valid options: {', '.join(valid_datasets)}"
            )

        # Create jobs for each dataset
        job_ids = []
        for dataset in request.datasets:
            job_config = {
                "dataset": dataset,
                "include_states": request.include_states,
                "batch": True
            }

            job = IngestionJob(
                source="fbi_crime",
                status=JobStatus.PENDING,
                config=job_config
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            job_ids.append(job.id)

        # Run batch ingestion (no job_id — kept as-is)
        background_tasks.add_task(
            _run_batch_ingestion,
            request.datasets,
            request.include_states
        )

        return {
            "job_ids": job_ids,
            "status": "pending",
            "message": f"Created {len(job_ids)} FBI Crime ingestion jobs",
            "datasets": request.datasets
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create batch FBI Crime jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Background Task Functions (batch only — no job_id)
# ============================================

async def _run_batch_ingestion(
    datasets: List[str],
    include_states: bool
):
    """Run batch FBI crime ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.get_fbi_crime_api_key()

        await ingest.ingest_all_fbi_crime_data(
            db=db,
            api_key=api_key,
            include_states=include_states
        )
    except Exception as e:
        logger.error(f"Background batch FBI crime ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
