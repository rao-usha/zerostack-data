"""
NOAA API endpoints for data ingestion.

Provides HTTP API for:
- Triggering NOAA data ingestion
- Querying available datasets
- Fetching dataset metadata
- Checking ingestion job status
"""
import logging
from datetime import date
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.sources.noaa.ingest import ingest_noaa_data, ingest_noaa_by_chunks
from app.sources.noaa.metadata import NOAA_DATASETS, NOAADataset
from app.sources.noaa.client import NOAAClient
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/noaa", tags=["noaa"])


# Pydantic models for request/response
class NOAAIngestionRequest(BaseModel):
    """Request model for NOAA data ingestion."""
    
    token: str = Field(..., description="NOAA CDO API token")
    dataset_key: str = Field(..., description="Dataset key (e.g., 'ghcnd_daily')")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    location_id: Optional[str] = Field(None, description="Location filter (e.g., 'FIPS:06' for California)")
    station_id: Optional[str] = Field(None, description="Station filter (e.g., 'GHCND:USW00023174')")
    data_type_ids: Optional[List[str]] = Field(None, description="Data types to fetch (e.g., ['TMAX', 'TMIN'])")
    max_results: Optional[int] = Field(None, description="Maximum total results")
    max_concurrency: int = Field(3, description="Maximum concurrent API requests")
    requests_per_second: float = Field(4.0, description="Rate limit (requests per second)")
    use_chunking: bool = Field(False, description="Split large date ranges into chunks")
    chunk_days: int = Field(30, description="Days per chunk if use_chunking=True")
    
    class Config:
        json_schema_extra = {
            "example": {
                "token": "YOUR_NOAA_TOKEN",
                "dataset_key": "ghcnd_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "location_id": "FIPS:06",
                "data_type_ids": ["TMAX", "TMIN", "PRCP"],
                "max_results": 10000,
                "use_chunking": False
            }
        }


class NOAAIngestionResponse(BaseModel):
    """Response model for NOAA ingestion."""
    
    job_id: int
    status: str
    dataset_key: str
    rows_fetched: int
    rows_inserted: int
    table_name: str
    message: str


class NOAADatasetInfo(BaseModel):
    """Information about a NOAA dataset."""
    
    dataset_key: str
    dataset_id: str
    name: str
    description: str
    data_types: List[str]
    table_name: str
    start_date: str
    end_date: str
    update_frequency: str


class NOAADatasetsResponse(BaseModel):
    """Response listing available NOAA datasets."""
    
    datasets: List[NOAADatasetInfo]
    count: int


# API Endpoints

@router.post("/ingest", response_model=NOAAIngestionResponse)
async def ingest_noaa(
    request: NOAAIngestionRequest
) -> NOAAIngestionResponse:
    """
    Ingest NOAA climate/weather data.
    
    This endpoint triggers ingestion of data from NOAA Climate Data Online (CDO) API.
    
    **Required:**
    - NOAA CDO API token (get from https://www.ncdc.noaa.gov/cdo-web/token)
    
    **Dataset Keys:**
    - `ghcnd_daily`: Daily weather observations
    - `normal_daily`: Daily climate normals
    - `normal_monthly`: Monthly climate normals
    - `gsom`: Monthly summaries
    - `precip_hourly`: Hourly precipitation
    
    **Location IDs:**
    - FIPS codes: `FIPS:06` (California), `FIPS:36` (New York), etc.
    - ZIP codes: `ZIP:10001`, `ZIP:90210`, etc.
    
    **Station IDs:**
    - Format: `GHCND:USW00023174` (LAX airport), etc.
    - Use `/noaa/stations` endpoint to find stations
    
    **Rate Limits:**
    - NOAA CDO API: 5 requests/second, 10,000 requests/day
    - This endpoint respects these limits automatically
    """
    try:
        # Validate dataset exists
        if request.dataset_key not in NOAA_DATASETS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown dataset: {request.dataset_key}. "
                       f"Available: {list(NOAA_DATASETS.keys())}"
            )
        
        # Choose ingestion method based on use_chunking
        if request.use_chunking:
            logger.info(f"Starting chunked ingestion for {request.dataset_key}")
            results = await ingest_noaa_by_chunks(
                token=request.token,
                dataset_key=request.dataset_key,
                start_date=request.start_date,
                end_date=request.end_date,
                chunk_days=request.chunk_days,
                location_id=request.location_id,
                station_id=request.station_id,
                data_type_ids=request.data_type_ids,
                max_results=request.max_results,
                max_concurrency=request.max_concurrency,
                requests_per_second=request.requests_per_second
            )
            
            # Aggregate results
            total_fetched = sum(r["rows_fetched"] for r in results)
            total_inserted = sum(r["rows_inserted"] for r in results)
            job_ids = [r["job_id"] for r in results]
            
            return NOAAIngestionResponse(
                job_id=job_ids[0],  # Return first job ID
                status="success",
                dataset_key=request.dataset_key,
                rows_fetched=total_fetched,
                rows_inserted=total_inserted,
                table_name=results[0]["table_name"],
                message=f"Successfully ingested {total_inserted} rows in {len(results)} chunks. "
                        f"Job IDs: {job_ids}"
            )
        else:
            logger.info(f"Starting ingestion for {request.dataset_key}")
            result = await ingest_noaa_data(
                token=request.token,
                dataset_key=request.dataset_key,
                start_date=request.start_date,
                end_date=request.end_date,
                location_id=request.location_id,
                station_id=request.station_id,
                data_type_ids=request.data_type_ids,
                max_results=request.max_results,
                max_concurrency=request.max_concurrency,
                requests_per_second=request.requests_per_second
            )
            
            return NOAAIngestionResponse(
                **result,
                message=f"Successfully ingested {result['rows_inserted']} rows"
            )
            
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@router.get("/datasets", response_model=NOAADatasetsResponse)
async def list_datasets() -> NOAADatasetsResponse:
    """
    List available NOAA datasets.
    
    Returns metadata about all configured NOAA datasets that can be ingested.
    """
    dataset_infos = []
    
    for key, dataset in NOAA_DATASETS.items():
        info = NOAADatasetInfo(
            dataset_key=key,
            dataset_id=dataset.dataset_id,
            name=dataset.name,
            description=dataset.description,
            data_types=dataset.data_types,
            table_name=dataset.table_name,
            start_date=dataset.start_date.isoformat(),
            end_date=dataset.end_date.isoformat(),
            update_frequency=dataset.update_frequency
        )
        dataset_infos.append(info)
    
    return NOAADatasetsResponse(
        datasets=dataset_infos,
        count=len(dataset_infos)
    )


@router.get("/datasets/{dataset_key}")
async def get_dataset(dataset_key: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific NOAA dataset.
    
    Args:
        dataset_key: Dataset identifier (e.g., 'ghcnd_daily')
    """
    if dataset_key not in NOAA_DATASETS:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset not found: {dataset_key}"
        )
    
    dataset = NOAA_DATASETS[dataset_key]
    return dataset.to_dict()


@router.get("/locations")
async def get_locations(
    token: str = Query(..., description="NOAA CDO API token"),
    dataset_id: str = Query("GHCND", description="Dataset ID (e.g., GHCND)"),
    location_category_id: Optional[str] = Query(None, description="Location category (e.g., ST for states)"),
    limit: int = Query(100, description="Maximum results", le=1000)
) -> Dict[str, Any]:
    """
    Get available locations (geographic areas) from NOAA CDO API.
    
    This endpoint queries the NOAA CDO API directly to get location information.
    
    **Location Categories:**
    - `CITY`: Cities
    - `ST`: States
    - `CNTRY`: Countries
    - `ZIP`: ZIP codes
    
    **Example:**
    - Get all US states: `?dataset_id=GHCND&location_category_id=ST`
    """
    try:
        client = NOAAClient(token=token)
        
        try:
            locations = await client.get_locations(
                dataset_id=dataset_id,
                location_category_id=location_category_id,
                limit=limit
            )
            
            return {
                "locations": locations,
                "count": len(locations)
            }
        finally:
            await client.close()
            
    except Exception as e:
        logger.error(f"Failed to fetch locations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stations")
async def get_stations(
    token: str = Query(..., description="NOAA CDO API token"),
    dataset_id: str = Query("GHCND", description="Dataset ID"),
    location_id: Optional[str] = Query(None, description="Location filter (e.g., FIPS:06)"),
    limit: int = Query(100, description="Maximum results", le=1000)
) -> Dict[str, Any]:
    """
    Get available weather stations from NOAA CDO API.
    
    This endpoint queries the NOAA CDO API directly to get station information.
    
    **Example:**
    - Get California stations: `?dataset_id=GHCND&location_id=FIPS:06`
    """
    try:
        client = NOAAClient(token=token)
        
        try:
            stations = await client.get_stations(
                dataset_id=dataset_id,
                location_id=location_id,
                limit=limit
            )
            
            return {
                "stations": stations,
                "count": len(stations)
            }
        finally:
            await client.close()
            
    except Exception as e:
        logger.error(f"Failed to fetch stations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-types")
async def get_data_types(
    token: str = Query(..., description="NOAA CDO API token"),
    dataset_id: str = Query("GHCND", description="Dataset ID"),
    limit: int = Query(100, description="Maximum results", le=1000)
) -> Dict[str, Any]:
    """
    Get available data types for a dataset from NOAA CDO API.
    
    Data types represent different measurements (e.g., TMAX, TMIN, PRCP).
    
    **Example:**
    - Get GHCND data types: `?dataset_id=GHCND`
    """
    try:
        client = NOAAClient(token=token)
        
        try:
            data_types = await client.get_data_types(
                dataset_id=dataset_id,
                limit=limit
            )
            
            return {
                "data_types": data_types,
                "count": len(data_types)
            }
        finally:
            await client.close()
            
    except Exception as e:
        logger.error(f"Failed to fetch data types: {e}")
        raise HTTPException(status_code=500, detail=str(e))














