"""
Yelp Fusion API endpoints.

Provides HTTP endpoints for ingesting Yelp business data:
- Business listings by location
- Business categories
- Multi-location business search

IMPORTANT: Yelp has strict daily API limits (500 calls/day for free tier).
Plan your data collection accordingly.
"""
import logging
from typing import Dict, Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from enum import Enum

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.sources.yelp import ingest
from app.sources.yelp.client import YELP_CATEGORIES
from app.sources.yelp.metadata import US_CITIES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["yelp"])


# ========== Enums ==========

class SortBy(str, Enum):
    BEST_MATCH = "best_match"
    RATING = "rating"
    REVIEW_COUNT = "review_count"
    DISTANCE = "distance"


# ========== Request Models ==========

class BusinessSearchRequest(BaseModel):
    """Request model for business search ingestion."""
    location: str = Field(
        ...,
        description="Location string (e.g., 'San Francisco, CA', 'NYC')",
        examples=["San Francisco, CA"]
    )
    term: Optional[str] = Field(
        None,
        description="Search term (e.g., 'restaurants', 'coffee')",
        examples=["restaurants"]
    )
    categories: Optional[str] = Field(
        None,
        description="Category filter (comma-separated, e.g., 'restaurants,bars')",
        examples=["restaurants"]
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=50,
        description="Number of results (max 50)"
    )


class MultiLocationRequest(BaseModel):
    """Request model for multi-location business search."""
    locations: List[str] = Field(
        ...,
        description="List of location strings",
        examples=[["San Francisco, CA", "Los Angeles, CA", "New York, NY"]]
    )
    term: Optional[str] = Field(
        None,
        description="Search term",
        examples=["restaurants"]
    )
    categories: Optional[str] = Field(
        None,
        description="Category filter",
        examples=["restaurants"]
    )
    limit_per_location: int = Field(
        default=20,
        ge=1,
        le=50,
        description="Results per location (max 50)"
    )


# ========== Endpoints ==========

@router.post("/yelp/businesses/ingest")
async def ingest_businesses(
    request: BusinessSearchRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Yelp business listings for a location.
    
    This endpoint creates an ingestion job and runs it in the background.
    Use GET /jobs/{job_id} to check progress.
    
    **IMPORTANT:** Uses 1 API call. Daily limit is 500 calls.
    
    **Common Categories:**
    - restaurants, food, bars, coffee
    - shopping, grocery, fashion
    - health, dentists, physicians
    - auto, autorepair
    - hotels, travel
    
    **API Key Required:** Set YELP_API_KEY in environment variables.
    Get a free key at: https://www.yelp.com/developers/v3/manage_app
    """
    try:
        job_config = {
            "dataset": "businesses",
            "location": request.location,
            "term": request.term,
            "categories": request.categories,
            "limit": request.limit
        }
        
        job = IngestionJob(
            source="yelp",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_business_search_ingestion,
            job.id,
            request.location,
            request.term,
            request.categories,
            request.limit
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"Yelp business search job created for {request.location}",
            "api_calls_used": 1,
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create Yelp job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/yelp/businesses/multi-location/ingest")
async def ingest_multi_location_businesses(
    request: MultiLocationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest Yelp business listings for multiple locations.
    
    **WARNING:** Uses 1 API call per location.
    For n locations, this uses n API calls.
    Daily limit is 500 calls total.
    
    Example: 25 locations = 25 API calls (5% of daily limit)
    """
    # Warn about API usage
    if len(request.locations) > 100:
        raise HTTPException(
            status_code=400,
            detail=f"Too many locations ({len(request.locations)}). "
            f"Maximum 100 locations per request to stay within daily limits."
        )
    
    try:
        job_config = {
            "dataset": "businesses_multi",
            "locations": request.locations,
            "term": request.term,
            "categories": request.categories,
            "limit_per_location": request.limit_per_location
        }
        
        job = IngestionJob(
            source="yelp",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_multi_location_ingestion,
            job.id,
            request.locations,
            request.term,
            request.categories,
            request.limit_per_location
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": f"Yelp multi-location job created for {len(request.locations)} locations",
            "api_calls_estimate": len(request.locations),
            "daily_limit_usage": f"{len(request.locations)}/500 ({len(request.locations)/5:.1f}%)",
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create Yelp multi-location job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/yelp/categories/ingest")
async def ingest_categories(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Ingest all Yelp business categories.
    
    Fetches the complete category hierarchy.
    Uses 1 API call.
    
    **API Key Required:** Set YELP_API_KEY in environment variables.
    """
    try:
        job_config = {
            "dataset": "categories"
        }
        
        job = IngestionJob(
            source="yelp",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        background_tasks.add_task(
            _run_categories_ingestion,
            job.id
        )
        
        return {
            "job_id": job.id,
            "status": "pending",
            "message": "Yelp categories ingestion job created",
            "api_calls_used": 1,
            "check_status": f"/api/v1/jobs/{job.id}"
        }
    
    except Exception as e:
        logger.error(f"Failed to create Yelp categories job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/yelp/categories")
async def list_categories():
    """
    List common Yelp business categories.
    """
    return {
        "categories": YELP_CATEGORIES,
        "category_groups": {
            "food_dining": [
                "restaurants", "food", "bars", "coffee", "breakfast_brunch",
                "pizza", "mexican", "chinese", "italian", "japanese"
            ],
            "retail": [
                "shopping", "grocery", "fashion", "electronics"
            ],
            "services": [
                "localservices", "homeservices", "professional",
                "financialservices", "realestate"
            ],
            "health": [
                "health", "dentists", "physicians"
            ],
            "auto": [
                "auto", "autorepair"
            ],
            "entertainment": [
                "active", "arts", "nightlife"
            ],
            "travel": [
                "hotels", "travel"
            ]
        }
    }


@router.get("/yelp/cities")
async def list_major_cities():
    """
    List major US cities for business data collection.
    """
    return {
        "cities": US_CITIES,
        "usage_note": (
            "Each city search uses 1 API call. "
            "Daily limit is 500 calls for free tier."
        )
    }


@router.get("/yelp/api-limits")
async def get_api_limits():
    """
    Get Yelp API rate limit information.
    """
    return {
        "daily_limit": 500,
        "max_results_per_search": 50,
        "max_offset": 1000,
        "reviews_per_business": 3,
        "recommendations": {
            "conservative": "10-20 searches per day",
            "moderate": "50-100 searches per day",
            "aggressive": "200-400 searches per day (use sparingly)"
        },
        "api_key_info": {
            "required": True,
            "env_variable": "YELP_API_KEY",
            "signup_url": "https://www.yelp.com/developers/v3/manage_app"
        }
    }


# ========== Background Task Functions ==========

async def _run_business_search_ingestion(
    job_id: int,
    location: str,
    term: Optional[str],
    categories: Optional[str],
    limit: int
):
    """Run Yelp business search ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.require_yelp_api_key()
        
        await ingest.ingest_businesses_by_location(
            db=db,
            job_id=job_id,
            location=location,
            term=term,
            categories=categories,
            limit=limit,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background Yelp business search failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_multi_location_ingestion(
    job_id: int,
    locations: List[str],
    term: Optional[str],
    categories: Optional[str],
    limit_per_location: int
):
    """Run Yelp multi-location ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.require_yelp_api_key()
        
        await ingest.ingest_multiple_locations(
            db=db,
            job_id=job_id,
            locations=locations,
            term=term,
            categories=categories,
            limit_per_location=limit_per_location,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background Yelp multi-location search failed: {e}", exc_info=True)
    finally:
        db.close()


async def _run_categories_ingestion(job_id: int):
    """Run Yelp categories ingestion in background."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        settings = get_settings()
        api_key = settings.require_yelp_api_key()
        
        await ingest.ingest_business_categories(
            db=db,
            job_id=job_id,
            api_key=api_key
        )
    except Exception as e:
        logger.error(f"Background Yelp categories ingestion failed: {e}", exc_info=True)
    finally:
        db.close()
