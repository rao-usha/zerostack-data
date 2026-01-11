"""
Foot Traffic & Location Intelligence API endpoints.

Provides endpoints for:
- Location discovery for brands
- Foot traffic data collection
- Traffic analytics and trends
- Competitive benchmarking
"""
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    Location,
    FootTrafficObservation,
    LocationMetadata,
    FootTrafficCollectionJob,
)
from app.agentic.foot_traffic_agent import (
    FootTrafficAgent,
    quick_location_discovery,
    quick_traffic_collection,
)
from app.sources.foot_traffic.ingest import (
    discover_brand_locations,
    collect_traffic_for_location,
    enrich_location_metadata,
    get_brand_traffic_summary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/foot-traffic", tags=["Foot Traffic"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class LocationDiscoveryRequest(BaseModel):
    """Request model for location discovery."""
    brand_name: str = Field(..., description="Brand name to discover locations for")
    city: Optional[str] = Field(None, description="City to search in")
    state: Optional[str] = Field(None, description="State code (e.g., 'CA')")
    latitude: Optional[float] = Field(None, description="Center latitude for geographic search")
    longitude: Optional[float] = Field(None, description="Center longitude")
    limit: int = Field(50, ge=1, le=200, description="Maximum locations to discover")
    strategies: Optional[List[str]] = Field(
        None, 
        description="Specific strategies to use (foursquare, safegraph, etc.)"
    )


class TrafficCollectionRequest(BaseModel):
    """Request model for traffic collection."""
    location_id: Optional[int] = Field(None, description="Specific location ID")
    brand_name: Optional[str] = Field(None, description="Brand name (collect for all locations)")
    city: Optional[str] = Field(None, description="City filter when using brand_name")
    start_date: Optional[date] = Field(None, description="Start date (default: 90 days ago)")
    end_date: Optional[date] = Field(None, description="End date (default: today)")
    strategies: Optional[List[str]] = Field(None, description="Specific strategies to use")


class LocationResponse(BaseModel):
    """Response model for a single location."""
    id: int
    location_name: str
    brand_name: Optional[str]
    street_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    latitude: Optional[str]
    longitude: Optional[str]
    category: Optional[str]
    is_active: bool


class TrafficSummaryResponse(BaseModel):
    """Response model for traffic summary."""
    brand_name: str
    location_count: int
    observation_count: int
    avg_weekly_visits: Optional[float]
    total_visits: Optional[int]
    date_range: Dict[str, Optional[str]]


# =============================================================================
# LOCATION DISCOVERY ENDPOINTS
# =============================================================================


@router.post("/locations/discover")
async def discover_locations(
    request: LocationDiscoveryRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Discover and track locations for a brand.
    
    Uses multiple data sources (Foursquare, SafeGraph, etc.) to find
    all locations for a retail/restaurant brand.
    
    **Example:**
    ```json
    {
        "brand_name": "Chipotle",
        "city": "San Francisco",
        "state": "CA",
        "limit": 50
    }
    ```
    
    **Returns:**
    - Discovered locations with addresses, coordinates
    - Which data sources were used
    - Agent reasoning for decisions made
    """
    try:
        # Create job record
        job_result = db.execute(
            text("""
                INSERT INTO foot_traffic_collection_jobs (
                    job_type, target_brand, geographic_scope, status, started_at, created_at
                ) VALUES (
                    'discover_locations', :brand, :scope, 'running', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                ) RETURNING id
            """),
            {
                "brand": request.brand_name,
                "scope": f"{request.city or ''}, {request.state or ''}".strip(", ") or "national",
            }
        )
        job_id = job_result.fetchone()[0]
        db.commit()
        
        # Run discovery
        agent = FootTrafficAgent(db)
        result = await agent.discover_locations(
            brand_name=request.brand_name,
            city=request.city,
            state=request.state,
            latitude=request.latitude,
            longitude=request.longitude,
            strategies_to_use=request.strategies
        )
        
        # Update job
        status = "success" if result.get("locations_found", 0) > 0 else "partial_success"
        db.execute(
            text("""
                UPDATE foot_traffic_collection_jobs 
                SET status = :status,
                    completed_at = CURRENT_TIMESTAMP,
                    locations_found = :found,
                    reasoning_log = CAST(:reasoning AS jsonb),
                    errors = CAST(:errors AS jsonb),
                    requests_made = :requests
                WHERE id = :job_id
            """),
            {
                "job_id": job_id,
                "status": status,
                "found": result.get("locations_found", 0),
                "reasoning": json.dumps(result.get("reasoning_log")) if result.get("reasoning_log") else None,
                "errors": json.dumps(result.get("errors")) if result.get("errors") else None,
                "requests": result.get("requests_made", 0),
            }
        )
        db.commit()
        
        return {
            "data": result,
            "meta": {
                "job_id": job_id,
                "source": "foot_traffic_agent",
            }
        }
        
    except Exception as e:
        logger.error(f"Location discovery failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/locations")
async def list_locations(
    brand_name: Optional[str] = Query(None, description="Filter by brand name"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    category: Optional[str] = Query(None, description="Filter by category (restaurant, retail, etc.)"),
    is_active: bool = Query(True, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    List tracked locations with optional filters.
    
    **Query Parameters:**
    - `brand_name`: Filter by brand (e.g., "Starbucks")
    - `city`: Filter by city
    - `state`: Filter by state code
    - `category`: Filter by category (restaurant, retail, office, venue)
    - `is_active`: Show only active locations (default: true)
    """
    # Build query
    query = "SELECT * FROM locations WHERE 1=1"
    count_query = "SELECT COUNT(*) FROM locations WHERE 1=1"
    params = {}
    
    if brand_name:
        query += " AND brand_name = :brand_name"
        count_query += " AND brand_name = :brand_name"
        params["brand_name"] = brand_name
    if city:
        query += " AND city = :city"
        count_query += " AND city = :city"
        params["city"] = city
    if state:
        query += " AND state = :state"
        count_query += " AND state = :state"
        params["state"] = state
    if category:
        query += " AND category = :category"
        count_query += " AND category = :category"
        params["category"] = category
    if is_active is not None:
        query += " AND is_active = :is_active"
        count_query += " AND is_active = :is_active"
        params["is_active"] = 1 if is_active else 0
    
    query += " ORDER BY brand_name, city LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset
    
    # Execute
    locations = db.execute(text(query), params).fetchall()
    total = db.execute(text(count_query), params).fetchone()[0]
    
    return {
        "data": [dict(row._mapping) for row in locations],
        "meta": {
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    }


@router.get("/locations/{location_id}")
async def get_location(
    location_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get details for a specific location.
    
    Returns location data including metadata and recent traffic observations.
    """
    location = db.execute(
        text("SELECT * FROM locations WHERE id = :id"),
        {"id": location_id}
    ).fetchone()
    
    if not location:
        raise HTTPException(status_code=404, detail=f"Location {location_id} not found")
    
    # Get metadata
    metadata = db.execute(
        text("SELECT * FROM location_metadata WHERE location_id = :id"),
        {"id": location_id}
    ).fetchone()
    
    # Get recent observations
    observations = db.execute(
        text("""
            SELECT * FROM foot_traffic_observations 
            WHERE location_id = :id 
            ORDER BY observation_date DESC 
            LIMIT 10
        """),
        {"id": location_id}
    ).fetchall()
    
    return {
        "data": {
            "location": dict(location._mapping),
            "metadata": dict(metadata._mapping) if metadata else None,
            "recent_observations": [dict(o._mapping) for o in observations],
        }
    }


# =============================================================================
# TRAFFIC COLLECTION ENDPOINTS
# =============================================================================


@router.post("/locations/{location_id}/collect")
async def collect_traffic_for_single_location(
    location_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Collect foot traffic data for a specific location.
    
    Uses available data sources (SafeGraph, Placer, city data) to gather
    traffic observations.
    
    **Query Parameters:**
    - `start_date`: Start of date range (default: 90 days ago)
    - `end_date`: End of date range (default: today)
    """
    # Verify location exists
    location = db.execute(
        text("SELECT * FROM locations WHERE id = :id"),
        {"id": location_id}
    ).fetchone()
    
    if not location:
        raise HTTPException(status_code=404, detail=f"Location {location_id} not found")
    
    try:
        result = await collect_traffic_for_location(
            db=db,
            location_id=location_id,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "data": result,
            "meta": {"location_id": location_id}
        }
        
    except Exception as e:
        logger.error(f"Traffic collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collect")
async def collect_traffic_batch(
    request: TrafficCollectionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Collect foot traffic data for multiple locations.
    
    Can collect for:
    - A specific location (by location_id)
    - All locations for a brand (by brand_name)
    - All locations in a city (by brand_name + city)
    
    **Example:**
    ```json
    {
        "brand_name": "Chipotle",
        "city": "San Francisco",
        "start_date": "2024-01-01",
        "end_date": "2024-03-31"
    }
    ```
    """
    if not request.location_id and not request.brand_name:
        raise HTTPException(
            status_code=400, 
            detail="Must provide either location_id or brand_name"
        )
    
    try:
        agent = FootTrafficAgent(db)
        result = await agent.collect_traffic(
            location_id=request.location_id,
            brand_name=request.brand_name,
            city=request.city,
            start_date=request.start_date,
            end_date=request.end_date,
            strategies_to_use=request.strategies
        )
        
        return {
            "data": result,
            "meta": {"source": "foot_traffic_agent"}
        }
        
    except Exception as e:
        logger.error(f"Batch traffic collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# TRAFFIC QUERY ENDPOINTS
# =============================================================================


@router.get("/locations/{location_id}/traffic")
async def get_location_traffic(
    location_id: int,
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    granularity: str = Query("weekly", description="Granularity: daily, weekly, monthly"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get foot traffic time series for a location.
    
    Returns traffic observations within the date range at the specified granularity.
    """
    query = """
        SELECT * FROM foot_traffic_observations
        WHERE location_id = :location_id
        AND observation_date >= :start_date
        AND observation_date <= :end_date
        AND observation_period = :period
        ORDER BY observation_date
    """
    
    observations = db.execute(
        text(query),
        {
            "location_id": location_id,
            "start_date": start_date,
            "end_date": end_date,
            "period": granularity,
        }
    ).fetchall()
    
    return {
        "data": [dict(o._mapping) for o in observations],
        "meta": {
            "location_id": location_id,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "granularity": granularity,
            "observation_count": len(observations),
        }
    }


@router.get("/brands/{brand_name}/aggregate")
async def get_brand_aggregate_traffic(
    brand_name: str,
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get aggregated traffic across all locations for a brand.
    
    Returns summary statistics including:
    - Total locations tracked
    - Average weekly visits
    - Total visits in period
    - Date range of available data
    """
    result = await get_brand_traffic_summary(
        db=db,
        brand_name=brand_name,
        city=city,
        state=state,
        start_date=start_date,
        end_date=end_date
    )
    
    return {
        "data": result,
        "meta": {"brand_name": brand_name}
    }


@router.get("/compare")
async def compare_brands(
    brand_names: List[str] = Query(..., description="Brands to compare"),
    city: Optional[str] = Query(None, description="City filter"),
    state: Optional[str] = Query(None, description="State filter"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Compare foot traffic across multiple brands.
    
    **Query Parameters:**
    - `brand_names`: List of brands to compare (e.g., `?brand_names=Chipotle&brand_names=Panera`)
    - `city`: Optional city filter
    - `state`: Optional state filter
    
    **Example:**
    ```
    GET /foot-traffic/compare?brand_names=Chipotle&brand_names=Panera&brand_names=Sweetgreen&city=San%20Francisco
    ```
    """
    results = []
    
    for brand in brand_names:
        summary = await get_brand_traffic_summary(
            db=db,
            brand_name=brand,
            city=city,
            state=state,
            start_date=start_date,
            end_date=end_date
        )
        results.append(summary)
    
    return {
        "data": results,
        "meta": {
            "brands_compared": brand_names,
            "city": city,
            "state": state,
        }
    }


# =============================================================================
# ENRICHMENT ENDPOINTS
# =============================================================================


@router.post("/locations/{location_id}/enrich")
async def enrich_location(
    location_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Enrich a location with additional metadata.
    
    Fetches:
    - Trade area data (population, income)
    - Competitive set (nearby competitors)
    - Ratings and reviews
    """
    result = await enrich_location_metadata(
        db=db,
        location_id=location_id
    )
    
    return {
        "data": result,
        "meta": {"location_id": location_id}
    }


# =============================================================================
# JOB TRACKING ENDPOINTS
# =============================================================================


@router.get("/jobs")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    List foot traffic collection jobs.
    """
    query = "SELECT * FROM foot_traffic_collection_jobs WHERE 1=1"
    params = {}
    
    if status:
        query += " AND status = :status"
        params["status"] = status
    if brand:
        query += " AND target_brand = :brand"
        params["brand"] = brand
    
    query += " ORDER BY created_at DESC LIMIT :limit"
    params["limit"] = limit
    
    jobs = db.execute(text(query), params).fetchall()
    
    return {
        "data": [dict(j._mapping) for j in jobs],
        "meta": {"count": len(jobs)}
    }


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get details for a specific collection job.
    """
    job = db.execute(
        text("SELECT * FROM foot_traffic_collection_jobs WHERE id = :id"),
        {"id": job_id}
    ).fetchone()
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return {"data": dict(job._mapping)}


# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================


@router.get("/sources")
async def get_available_sources() -> Dict[str, Any]:
    """
    Get list of available data sources and their status.
    
    Returns which APIs are configured and available for use.
    """
    from app.core.config import get_settings
    settings = get_settings()
    
    sources = {
        "safegraph": {
            "name": "SafeGraph Patterns",
            "configured": bool(settings.get_safegraph_api_key()),
            "confidence": "high",
            "data_type": "Weekly foot traffic from mobile location data",
            "cost": "$100-500/month",
        },
        "foursquare": {
            "name": "Foursquare Places",
            "configured": bool(settings.get_foursquare_api_key()),
            "confidence": "medium",
            "data_type": "POI metadata and check-in data",
            "cost": "Free tier + $0.01-0.05/call",
        },
        "placer": {
            "name": "Placer.ai Analytics",
            "configured": bool(settings.get_placer_api_key()),
            "confidence": "high",
            "data_type": "Retail analytics and trade area data",
            "cost": "$500-2,000+/month",
        },
        "google": {
            "name": "Google Popular Times",
            "configured": settings.is_google_scraping_enabled(),
            "confidence": "medium",
            "data_type": "Peak hours patterns (scraping, ToS risk)",
            "cost": "Free",
            "warning": "Scraping may violate Google ToS",
        },
        "city_data": {
            "name": "City Pedestrian Counters",
            "configured": True,  # Always available (no API key)
            "confidence": "high",
            "data_type": "Public pedestrian counter data",
            "cost": "Free",
            "supported_cities": ["Seattle", "New York", "San Francisco", "Chicago"],
        },
    }
    
    return {"data": sources}


@router.get("/categories")
async def get_location_categories() -> Dict[str, Any]:
    """
    Get available location categories and subcategories.
    """
    from app.sources.foot_traffic.metadata import LOCATION_CATEGORIES
    return {"data": LOCATION_CATEGORIES}
