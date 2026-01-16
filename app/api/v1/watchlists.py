"""
Watchlists and Saved Searches API (T20).

Endpoints for managing user watchlists and saved search queries.
"""

import logging
from typing import List, Optional, Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.users.watchlists import WatchlistService
from app.search.engine import SearchEngine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Watchlists & Saved Searches"])


# =============================================================================
# Request Models
# =============================================================================


class WatchlistCreate(BaseModel):
    """Request to create a watchlist."""
    name: str = Field(..., min_length=1, max_length=255, description="Watchlist name")
    user_id: str = Field(..., min_length=1, description="User identifier (email)")
    description: Optional[str] = Field(None, description="Optional description")


class WatchlistUpdate(BaseModel):
    """Request to update a watchlist."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None


class WatchlistItemCreate(BaseModel):
    """Request to add an item to a watchlist."""
    entity_type: str = Field(..., pattern="^(investor|company)$", description="Entity type")
    entity_id: int = Field(..., gt=0, description="Entity ID")
    note: Optional[str] = Field(None, description="Optional note about this item")


class SavedSearchCreate(BaseModel):
    """Request to save a search."""
    name: str = Field(..., min_length=1, max_length=255, description="Search name")
    user_id: str = Field(..., min_length=1, description="User identifier")
    query: Optional[str] = Field("", description="Search query text")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Search filters")


class SavedSearchUpdate(BaseModel):
    """Request to update a saved search."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    query: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None


# =============================================================================
# Response Models
# =============================================================================


class WatchlistResponse(BaseModel):
    """Watchlist response."""
    id: int
    user_id: str
    name: str
    description: Optional[str]
    is_public: bool
    item_count: int
    created_at: str
    updated_at: str


class WatchlistItemResponse(BaseModel):
    """Watchlist item response."""
    id: int
    entity_type: str
    entity_id: int
    entity_name: str
    entity_details: Dict[str, Any]
    note: Optional[str]
    added_at: str


class WatchlistItemsResponse(BaseModel):
    """Paginated list of watchlist items."""
    items: List[WatchlistItemResponse]
    total: int
    page: int
    page_size: int


class SavedSearchResponse(BaseModel):
    """Saved search response."""
    id: int
    user_id: str
    name: str
    query: Optional[str]
    filters: Dict[str, Any]
    execution_count: int
    last_executed_at: Optional[str]
    created_at: str
    updated_at: str


# =============================================================================
# Watchlist Endpoints
# =============================================================================


@router.post("/watchlists", response_model=WatchlistResponse, status_code=201)
async def create_watchlist(
    request: WatchlistCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new watchlist.

    **Example:**
    ```json
    {
      "name": "Tech Companies",
      "user_id": "analyst@example.com",
      "description": "Companies to track in the tech sector"
    }
    ```
    """
    service = WatchlistService(db)

    try:
        watchlist = service.create_watchlist(
            user_id=request.user_id,
            name=request.name,
            description=request.description
        )

        return WatchlistResponse(
            id=watchlist.id,
            user_id=watchlist.user_id,
            name=watchlist.name,
            description=watchlist.description,
            is_public=watchlist.is_public,
            item_count=watchlist.item_count,
            created_at=watchlist.created_at.isoformat(),
            updated_at=watchlist.updated_at.isoformat()
        )
    except Exception as e:
        logger.error(f"Error creating watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create watchlist: {str(e)}")


@router.get("/watchlists", response_model=List[WatchlistResponse])
async def list_watchlists(
    user_id: str = Query(..., description="User identifier"),
    db: Session = Depends(get_db)
):
    """
    List all watchlists for a user.

    **Example:** `GET /watchlists?user_id=analyst@example.com`
    """
    service = WatchlistService(db)

    try:
        watchlists = service.list_watchlists(user_id)

        return [
            WatchlistResponse(
                id=w.id,
                user_id=w.user_id,
                name=w.name,
                description=w.description,
                is_public=w.is_public,
                item_count=w.item_count,
                created_at=w.created_at.isoformat(),
                updated_at=w.updated_at.isoformat()
            )
            for w in watchlists
        ]
    except Exception as e:
        logger.error(f"Error listing watchlists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list watchlists: {str(e)}")


@router.get("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
async def get_watchlist(
    watchlist_id: int,
    db: Session = Depends(get_db)
):
    """Get a watchlist by ID."""
    service = WatchlistService(db)

    watchlist = service.get_watchlist(watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail=f"Watchlist {watchlist_id} not found")

    return WatchlistResponse(
        id=watchlist.id,
        user_id=watchlist.user_id,
        name=watchlist.name,
        description=watchlist.description,
        is_public=watchlist.is_public,
        item_count=watchlist.item_count,
        created_at=watchlist.created_at.isoformat(),
        updated_at=watchlist.updated_at.isoformat()
    )


@router.patch("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
async def update_watchlist(
    watchlist_id: int,
    request: WatchlistUpdate,
    db: Session = Depends(get_db)
):
    """Update a watchlist."""
    service = WatchlistService(db)

    # Check exists
    existing = service.get_watchlist(watchlist_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Watchlist {watchlist_id} not found")

    watchlist = service.update_watchlist(
        watchlist_id=watchlist_id,
        name=request.name,
        description=request.description
    )

    return WatchlistResponse(
        id=watchlist.id,
        user_id=watchlist.user_id,
        name=watchlist.name,
        description=watchlist.description,
        is_public=watchlist.is_public,
        item_count=watchlist.item_count,
        created_at=watchlist.created_at.isoformat(),
        updated_at=watchlist.updated_at.isoformat()
    )


@router.delete("/watchlists/{watchlist_id}", status_code=204)
async def delete_watchlist(
    watchlist_id: int,
    db: Session = Depends(get_db)
):
    """Delete a watchlist and all its items."""
    service = WatchlistService(db)

    deleted = service.delete_watchlist(watchlist_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Watchlist {watchlist_id} not found")

    return None


# =============================================================================
# Watchlist Items Endpoints
# =============================================================================


@router.post("/watchlists/{watchlist_id}/items", response_model=WatchlistItemResponse, status_code=201)
async def add_watchlist_item(
    watchlist_id: int,
    request: WatchlistItemCreate,
    db: Session = Depends(get_db)
):
    """
    Add an item to a watchlist.

    **Entity types:** `investor`, `company`

    **Example:**
    ```json
    {
      "entity_type": "investor",
      "entity_id": 1,
      "note": "Key competitor to watch"
    }
    ```
    """
    service = WatchlistService(db)

    # Check watchlist exists
    watchlist = service.get_watchlist(watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail=f"Watchlist {watchlist_id} not found")

    item = service.add_item(
        watchlist_id=watchlist_id,
        entity_type=request.entity_type,
        entity_id=request.entity_id,
        note=request.note
    )

    if not item:
        raise HTTPException(
            status_code=409,
            detail=f"Item already exists in watchlist (entity_type={request.entity_type}, entity_id={request.entity_id})"
        )

    return WatchlistItemResponse(
        id=item.id,
        entity_type=item.entity_type,
        entity_id=item.entity_id,
        entity_name=item.entity_name,
        entity_details=item.entity_details,
        note=item.note,
        added_at=item.added_at.isoformat()
    )


@router.get("/watchlists/{watchlist_id}/items", response_model=WatchlistItemsResponse)
async def list_watchlist_items(
    watchlist_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db)
):
    """List items in a watchlist with pagination."""
    service = WatchlistService(db)

    # Check watchlist exists
    watchlist = service.get_watchlist(watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail=f"Watchlist {watchlist_id} not found")

    items, total = service.list_items(watchlist_id, page=page, page_size=page_size)

    return WatchlistItemsResponse(
        items=[
            WatchlistItemResponse(
                id=item.id,
                entity_type=item.entity_type,
                entity_id=item.entity_id,
                entity_name=item.entity_name,
                entity_details=item.entity_details,
                note=item.note,
                added_at=item.added_at.isoformat()
            )
            for item in items
        ],
        total=total,
        page=page,
        page_size=page_size
    )


@router.delete("/watchlists/{watchlist_id}/items/{item_id}", status_code=204)
async def remove_watchlist_item(
    watchlist_id: int,
    item_id: int,
    db: Session = Depends(get_db)
):
    """Remove an item from a watchlist by item ID."""
    service = WatchlistService(db)

    removed = service.remove_item(watchlist_id, item_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found in watchlist {watchlist_id}")

    return None


# =============================================================================
# Saved Searches Endpoints
# =============================================================================


@router.post("/searches/saved", response_model=SavedSearchResponse, status_code=201)
async def create_saved_search(
    request: SavedSearchCreate,
    db: Session = Depends(get_db)
):
    """
    Save a search query for later re-execution.

    **Example:**
    ```json
    {
      "name": "Healthcare Pension Funds",
      "user_id": "analyst@example.com",
      "query": "healthcare",
      "filters": {
        "types": ["investor"],
        "investor_type": "public_pension"
      }
    }
    ```
    """
    service = WatchlistService(db)

    try:
        search = service.create_saved_search(
            user_id=request.user_id,
            name=request.name,
            query=request.query,
            filters=request.filters
        )

        return SavedSearchResponse(
            id=search.id,
            user_id=search.user_id,
            name=search.name,
            query=search.query,
            filters=search.filters,
            execution_count=search.execution_count,
            last_executed_at=search.last_executed_at.isoformat() if search.last_executed_at else None,
            created_at=search.created_at.isoformat(),
            updated_at=search.updated_at.isoformat()
        )
    except Exception as e:
        logger.error(f"Error creating saved search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save search: {str(e)}")


@router.get("/searches/saved", response_model=List[SavedSearchResponse])
async def list_saved_searches(
    user_id: str = Query(..., description="User identifier"),
    name: Optional[str] = Query(None, description="Filter by name (substring match)"),
    db: Session = Depends(get_db)
):
    """
    List saved searches for a user.

    **Example:** `GET /searches/saved?user_id=analyst@example.com&name=healthcare`
    """
    service = WatchlistService(db)

    try:
        searches = service.list_saved_searches(user_id, name_filter=name)

        return [
            SavedSearchResponse(
                id=s.id,
                user_id=s.user_id,
                name=s.name,
                query=s.query,
                filters=s.filters,
                execution_count=s.execution_count,
                last_executed_at=s.last_executed_at.isoformat() if s.last_executed_at else None,
                created_at=s.created_at.isoformat(),
                updated_at=s.updated_at.isoformat()
            )
            for s in searches
        ]
    except Exception as e:
        logger.error(f"Error listing saved searches: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list saved searches: {str(e)}")


@router.get("/searches/saved/{search_id}", response_model=SavedSearchResponse)
async def get_saved_search(
    search_id: int,
    db: Session = Depends(get_db)
):
    """Get a saved search by ID."""
    service = WatchlistService(db)

    search = service.get_saved_search(search_id)
    if not search:
        raise HTTPException(status_code=404, detail=f"Saved search {search_id} not found")

    return SavedSearchResponse(
        id=search.id,
        user_id=search.user_id,
        name=search.name,
        query=search.query,
        filters=search.filters,
        execution_count=search.execution_count,
        last_executed_at=search.last_executed_at.isoformat() if search.last_executed_at else None,
        created_at=search.created_at.isoformat(),
        updated_at=search.updated_at.isoformat()
    )


@router.get("/searches/saved/{search_id}/execute")
async def execute_saved_search(
    search_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Execute a saved search and return results.

    Uses the T12 search engine to perform the search with the saved parameters.
    Increments the execution count and updates last_executed_at.
    """
    service = WatchlistService(db)

    # Get saved search
    search = service.get_saved_search(search_id)
    if not search:
        raise HTTPException(status_code=404, detail=f"Saved search {search_id} not found")

    # Record execution
    service.record_execution(search_id)

    # Execute via T12 search engine
    try:
        engine = SearchEngine(db)

        # Extract filters
        filters = search.filters or {}
        types = filters.get("types")
        industry = filters.get("industry")
        investor_type = filters.get("investor_type")
        location = filters.get("location")

        response = engine.search(
            query=search.query or "",
            result_types=types,
            industry=industry,
            investor_type=investor_type,
            location=location,
            page=page,
            page_size=page_size
        )

        return {
            "saved_search_id": search_id,
            "saved_search_name": search.name,
            "query": search.query,
            "filters": search.filters,
            "results": [
                {
                    "type": r.result_type,
                    "id": r.entity_id,
                    "name": r.name,
                    "industry": r.metadata.get("industry"),
                    "investor_type": r.metadata.get("investor_type"),
                    "location": r.metadata.get("location"),
                    "rank": round(r.relevance_score, 4)
                }
                for r in response.results
            ],
            "total": response.total,
            "page": page,
            "page_size": page_size
        }
    except Exception as e:
        logger.error(f"Error executing saved search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to execute search: {str(e)}")


@router.patch("/searches/saved/{search_id}", response_model=SavedSearchResponse)
async def update_saved_search(
    search_id: int,
    request: SavedSearchUpdate,
    db: Session = Depends(get_db)
):
    """Update a saved search."""
    service = WatchlistService(db)

    # Check exists
    existing = service.get_saved_search(search_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Saved search {search_id} not found")

    search = service.update_saved_search(
        search_id=search_id,
        name=request.name,
        query=request.query,
        filters=request.filters
    )

    return SavedSearchResponse(
        id=search.id,
        user_id=search.user_id,
        name=search.name,
        query=search.query,
        filters=search.filters,
        execution_count=search.execution_count,
        last_executed_at=search.last_executed_at.isoformat() if search.last_executed_at else None,
        created_at=search.created_at.isoformat(),
        updated_at=search.updated_at.isoformat()
    )


@router.delete("/searches/saved/{search_id}", status_code=204)
async def delete_saved_search(
    search_id: int,
    db: Session = Depends(get_db)
):
    """Delete a saved search."""
    service = WatchlistService(db)

    deleted = service.delete_saved_search(search_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Saved search {search_id} not found")

    return None
