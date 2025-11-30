"""
GeoJSON boundary endpoints.

Query stored geographic boundaries for mapping and visualization.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models import GeoJSONBoundaries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/geojson", tags=["geojson"])


class BoundaryInfo(BaseModel):
    """Basic boundary information without full GeoJSON."""
    id: int
    dataset_id: str
    geo_level: str
    geo_id: str
    geo_name: Optional[str]
    bbox_minx: Optional[str]
    bbox_miny: Optional[str]
    bbox_maxx: Optional[str]
    bbox_maxy: Optional[str]
    
    model_config = {"from_attributes": True}


class BoundaryWithGeoJSON(BoundaryInfo):
    """Full boundary with GeoJSON feature."""
    geojson: dict
    
    model_config = {"from_attributes": True}


@router.get("/datasets", response_model=List[dict])
def list_boundary_datasets(db: Session = Depends(get_db)) -> List[dict]:
    """
    List all available GeoJSON boundary datasets.
    
    Returns summary of each dataset including count and geographic level.
    """
    from sqlalchemy import func
    
    results = db.query(
        GeoJSONBoundaries.dataset_id,
        GeoJSONBoundaries.geo_level,
        func.count(GeoJSONBoundaries.id).label('feature_count')
    ).group_by(
        GeoJSONBoundaries.dataset_id,
        GeoJSONBoundaries.geo_level
    ).order_by(GeoJSONBoundaries.dataset_id).all()
    
    return [
        {
            "dataset_id": r.dataset_id,
            "geo_level": r.geo_level,
            "feature_count": r.feature_count
        }
        for r in results
    ]


@router.get("/boundaries/{dataset_id}", response_model=List[BoundaryInfo])
def list_boundaries(
    dataset_id: str,
    limit: int = Query(100, ge=1, le=10000),
    db: Session = Depends(get_db)
) -> List[BoundaryInfo]:
    """
    List boundaries in a dataset (without full GeoJSON).
    
    Returns boundary metadata including bounding boxes but not full geometry.
    Useful for getting a list of available geographies.
    """
    boundaries = db.query(GeoJSONBoundaries).filter(
        GeoJSONBoundaries.dataset_id == dataset_id
    ).limit(limit).all()
    
    if not boundaries:
        raise HTTPException(status_code=404, detail=f"No boundaries found for dataset: {dataset_id}")
    
    return [BoundaryInfo.model_validate(b) for b in boundaries]


@router.get("/boundary/{dataset_id}/{geo_id}", response_model=BoundaryWithGeoJSON)
def get_boundary(
    dataset_id: str,
    geo_id: str,
    db: Session = Depends(get_db)
) -> BoundaryWithGeoJSON:
    """
    Get a specific boundary with full GeoJSON.
    
    Args:
        dataset_id: Dataset identifier (e.g., "census_states_2021")
        geo_id: Geographic identifier (e.g., "06" for California)
    
    Returns:
        Complete GeoJSON feature for the boundary
    
    Example:
        GET /api/v1/geojson/boundary/census_states_2021/06
    """
    boundary = db.query(GeoJSONBoundaries).filter(
        GeoJSONBoundaries.dataset_id == dataset_id,
        GeoJSONBoundaries.geo_id == geo_id
    ).first()
    
    if not boundary:
        raise HTTPException(
            status_code=404,
            detail=f"Boundary not found: {geo_id} in dataset {dataset_id}"
        )
    
    return BoundaryWithGeoJSON.model_validate(boundary)


@router.get("/featurecollection/{dataset_id}")
def get_feature_collection(
    dataset_id: str,
    geo_ids: Optional[str] = Query(None, description="Comma-separated list of geo_ids to include"),
    db: Session = Depends(get_db)
) -> dict:
    """
    Get a GeoJSON FeatureCollection for a dataset.
    
    Args:
        dataset_id: Dataset identifier
        geo_ids: Optional comma-separated list of geo_ids to filter (e.g., "06,36,48")
    
    Returns:
        GeoJSON FeatureCollection
    
    Example:
        GET /api/v1/geojson/featurecollection/census_states_2021
        GET /api/v1/geojson/featurecollection/census_states_2021?geo_ids=06,36,48
    """
    query = db.query(GeoJSONBoundaries).filter(
        GeoJSONBoundaries.dataset_id == dataset_id
    )
    
    # Filter by specific geo_ids if provided
    if geo_ids:
        geo_id_list = [g.strip() for g in geo_ids.split(",")]
        query = query.filter(GeoJSONBoundaries.geo_id.in_(geo_id_list))
    
    boundaries = query.all()
    
    if not boundaries:
        raise HTTPException(
            status_code=404,
            detail=f"No boundaries found for dataset: {dataset_id}"
        )
    
    # Build FeatureCollection
    features = [b.geojson for b in boundaries]
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "dataset_id": dataset_id,
            "feature_count": len(features)
        }
    }


@router.get("/search")
def search_boundaries(
    query: str = Query(..., min_length=2),
    dataset_id: Optional[str] = None,
    db: Session = Depends(get_db)
) -> List[BoundaryInfo]:
    """
    Search for boundaries by name.
    
    Args:
        query: Search term (matches geo_name)
        dataset_id: Optional dataset filter
    
    Returns:
        List of matching boundaries
    
    Example:
        GET /api/v1/geojson/search?query=california
        GET /api/v1/geojson/search?query=angeles&dataset_id=census_counties_us_2021
    """
    search_term = f"%{query}%"
    
    query_obj = db.query(GeoJSONBoundaries).filter(
        GeoJSONBoundaries.geo_name.ilike(search_term)
    )
    
    if dataset_id:
        query_obj = query_obj.filter(GeoJSONBoundaries.dataset_id == dataset_id)
    
    boundaries = query_obj.limit(50).all()
    
    return [BoundaryInfo.model_validate(b) for b in boundaries]

