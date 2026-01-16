"""
Full-Text Search API (T12).

Unified search across investors, portfolio companies, and co-investors
with fuzzy matching, faceted filtering, and autocomplete.
"""

import logging
from typing import List, Optional, Dict, Any
from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.search.engine import SearchEngine, SearchResultType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Search"])


# =============================================================================
# Request/Response Models
# =============================================================================


class SearchResultResponse(BaseModel):
    """A single search result."""
    id: int = Field(..., description="Search index record ID")
    entity_id: int = Field(..., description="Original entity ID in source table")
    type: str = Field(..., description="Entity type: investor, company, co_investor")
    name: str = Field(..., description="Entity name")
    description: Optional[str] = Field(None, description="Entity description")
    relevance_score: float = Field(..., description="Relevance score (0-1+)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional entity data")
    highlight: Optional[str] = Field(None, description="Highlighted match text")


class SearchFacetsResponse(BaseModel):
    """Aggregated facet counts for filtering."""
    result_types: Dict[str, int] = Field(default_factory=dict, description="Count by entity type")
    industries: Dict[str, int] = Field(default_factory=dict, description="Count by industry")
    investor_types: Dict[str, int] = Field(default_factory=dict, description="Count by investor type")
    locations: Dict[str, int] = Field(default_factory=dict, description="Count by location")


class SearchResponse(BaseModel):
    """Complete search response."""
    results: List[SearchResultResponse] = Field(..., description="Search results")
    facets: SearchFacetsResponse = Field(..., description="Facet counts for filtering")
    total: int = Field(..., description="Total matching results")
    page: int = Field(..., description="Current page (1-indexed)")
    page_size: int = Field(..., description="Results per page")
    query: str = Field(..., description="Original search query")
    search_time_ms: float = Field(..., description="Search execution time in milliseconds")


class SuggestionResponse(BaseModel):
    """An autocomplete suggestion."""
    text: str = Field(..., description="Suggested text")
    type: str = Field(..., description="Entity type")
    id: int = Field(..., description="Search index record ID")
    entity_id: int = Field(..., description="Original entity ID")
    score: float = Field(..., description="Match score")


class SuggestResponse(BaseModel):
    """Autocomplete suggestions response."""
    suggestions: List[SuggestionResponse]
    prefix: str


class ReindexResponse(BaseModel):
    """Response from reindex operation."""
    success: bool
    counts: Dict[str, int] = Field(..., description="Records indexed per entity type")
    total: int = Field(..., description="Total records indexed")


class SearchStatsResponse(BaseModel):
    """Search index statistics."""
    total_indexed: int
    by_type: Dict[str, int]
    last_updated: Optional[str]
    error: Optional[str] = None


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=0, description="Search query"),
    types: Optional[List[str]] = Query(None, description="Filter by entity types: investor, company, co_investor"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    investor_type: Optional[str] = Query(None, description="Filter by investor type (e.g., public_pension)"),
    location: Optional[str] = Query(None, description="Filter by location"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    fuzzy: bool = Query(True, description="Enable fuzzy matching for typos"),
    db: Session = Depends(get_db)
):
    """
    Search across investors, portfolio companies, and co-investors.

    **Features:**
    - Full-text search with relevance ranking
    - Fuzzy matching for typo tolerance (e.g., "calprs" finds "CalPERS")
    - Faceted filtering by type, industry, location
    - Pagination support

    **Examples:**
    - `/search?q=calpers` - Find CalPERS investor
    - `/search?q=tech&types=company` - Search only companies for "tech"
    - `/search?q=pension&investor_type=public_pension` - Find public pension funds
    - `/search?q=calprs&fuzzy=true` - Fuzzy search with typo
    """
    try:
        engine = SearchEngine(db)

        # Validate types if provided
        if types:
            valid_types = {"investor", "company", "co_investor"}
            invalid = set(types) - valid_types
            if invalid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid types: {invalid}. Valid types: {valid_types}"
                )

        result = engine.search(
            query=q,
            result_types=types,
            industry=industry,
            investor_type=investor_type,
            location=location,
            page=page,
            page_size=page_size,
            fuzzy=fuzzy
        )

        # Convert to response models
        results_response = [
            SearchResultResponse(
                id=r.id,
                entity_id=r.entity_id,
                type=r.result_type,
                name=r.name,
                description=r.description,
                relevance_score=r.relevance_score,
                metadata=r.metadata,
                highlight=r.highlight
            )
            for r in result.results
        ]

        facets_response = SearchFacetsResponse(
            result_types=result.facets.result_types,
            industries=result.facets.industries,
            investor_types=result.facets.investor_types,
            locations=result.facets.locations
        )

        return SearchResponse(
            results=results_response,
            facets=facets_response,
            total=result.total,
            page=result.page,
            page_size=result.page_size,
            query=result.query,
            search_time_ms=result.search_time_ms
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/suggest", response_model=SuggestResponse)
async def suggest(
    prefix: str = Query(..., min_length=1, description="Search prefix for autocomplete"),
    limit: int = Query(10, ge=1, le=50, description="Maximum suggestions to return"),
    types: Optional[List[str]] = Query(None, description="Filter by entity types"),
    db: Session = Depends(get_db)
):
    """
    Get autocomplete suggestions for a search prefix.

    **Examples:**
    - `/search/suggest?prefix=cal` - Suggests "CalPERS", "CalSTRS", etc.
    - `/search/suggest?prefix=seq&types=co_investor` - Suggests co-investors starting with "seq"
    """
    try:
        engine = SearchEngine(db)

        # Validate types if provided
        if types:
            valid_types = {"investor", "company", "co_investor"}
            invalid = set(types) - valid_types
            if invalid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid types: {invalid}. Valid types: {valid_types}"
                )

        suggestions = engine.suggest(
            prefix=prefix,
            limit=limit,
            result_types=types
        )

        suggestions_response = [
            SuggestionResponse(
                text=s.text,
                type=s.type,
                id=s.id,
                entity_id=s.entity_id,
                score=s.score
            )
            for s in suggestions
        ]

        return SuggestResponse(
            suggestions=suggestions_response,
            prefix=prefix
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Suggest error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Autocomplete failed: {str(e)}")


@router.post("/reindex", response_model=ReindexResponse)
async def reindex(
    type: Optional[str] = Query(None, description="Reindex only specific type: investor, company, co_investor"),
    db: Session = Depends(get_db)
):
    """
    Trigger reindexing of search data from source tables.

    This populates the search index from lp_fund, portfolio_companies, and co_investments.
    Safe to call multiple times (idempotent).

    **Note:** This is an admin operation. In production, consider protecting with authentication.
    """
    try:
        engine = SearchEngine(db)

        # Validate type if provided
        entity_type = None
        if type:
            type_map = {
                "investor": SearchResultType.INVESTOR,
                "company": SearchResultType.COMPANY,
                "co_investor": SearchResultType.CO_INVESTOR
            }
            if type not in type_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid type: {type}. Valid types: {list(type_map.keys())}"
                )
            entity_type = type_map[type]

        counts = engine.reindex(entity_type=entity_type)
        total = sum(counts.values())

        logger.info(f"Reindex complete: {counts}, total={total}")

        return ReindexResponse(
            success=True,
            counts=counts,
            total=total
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reindex error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Reindex failed: {str(e)}")


@router.get("/stats", response_model=SearchStatsResponse)
async def get_stats(db: Session = Depends(get_db)):
    """
    Get search index statistics.

    Returns counts of indexed entities and last update timestamp.
    """
    try:
        engine = SearchEngine(db)
        stats = engine.get_stats()

        return SearchStatsResponse(
            total_indexed=stats.get("total_indexed", 0),
            by_type=stats.get("by_type", {}),
            last_updated=stats.get("last_updated"),
            error=stats.get("error")
        )

    except Exception as e:
        logger.error(f"Stats error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not get stats: {str(e)}")
