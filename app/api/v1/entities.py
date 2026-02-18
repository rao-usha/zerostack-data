"""
Entity Resolution API Endpoints (T37)

Provides intelligent entity matching and deduplication across data sources.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.entity_resolver import (
    EntityResolver,
    CanonicalEntity,
    EntityAlias,
)

router = APIRouter(prefix="/entities", tags=["Entity Resolution"])


# =============================================================================
# SCHEMAS
# =============================================================================


class EntityIdentifiers(BaseModel):
    """Known identifiers for entity resolution."""

    cik: Optional[str] = Field(None, description="SEC CIK number")
    crd: Optional[str] = Field(None, description="SEC CRD number (advisers)")
    ticker: Optional[str] = Field(None, description="Stock ticker symbol")
    cusip: Optional[str] = Field(None, description="CUSIP identifier")
    lei: Optional[str] = Field(None, description="Legal Entity Identifier")


class ResolveRequest(BaseModel):
    """Request to resolve an entity."""

    name: str = Field(..., description="Entity name to resolve")
    entity_type: str = Field(..., description="Entity type: 'company' or 'investor'")
    identifiers: Optional[EntityIdentifiers] = None
    website: Optional[str] = Field(None, description="Entity website URL")
    state: Optional[str] = Field(None, description="State/province")
    country: Optional[str] = Field(None, description="Country")
    industry: Optional[str] = Field(None, description="Industry classification")
    source_type: Optional[str] = Field(None, description="Source of this reference")
    source_id: Optional[str] = Field(None, description="ID in source system")
    auto_create: bool = Field(True, description="Create new entity if no match found")


class AlternativeMatch(BaseModel):
    """An alternative match candidate."""

    id: int
    canonical_name: str
    confidence: float
    needs_review: Optional[bool] = None


class CanonicalEntityResponse(BaseModel):
    """Canonical entity details."""

    id: int
    entity_type: str
    canonical_name: str
    normalized_name: str
    cik: Optional[str] = None
    crd: Optional[str] = None
    ticker: Optional[str] = None
    cusip: Optional[str] = None
    lei: Optional[str] = None
    website: Optional[str] = None
    domain: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    industry: Optional[str] = None
    entity_subtype: Optional[str] = None
    alias_count: int
    source_count: int
    is_verified: bool
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class ResolveResponse(BaseModel):
    """Response from entity resolution."""

    canonical_entity: Optional[CanonicalEntityResponse] = None
    match_confidence: float
    match_method: str
    is_new: bool
    alternatives: List[AlternativeMatch] = []


class AliasResponse(BaseModel):
    """An entity alias."""

    id: int
    alias_name: str
    normalized_alias: str
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    match_confidence: Optional[float] = None
    match_method: Optional[str] = None
    is_manual_override: bool
    is_primary: bool
    created_at: str

    class Config:
        from_attributes = True


class AliasListResponse(BaseModel):
    """List of aliases for an entity."""

    canonical_entity_id: int
    canonical_name: str
    aliases: List[AliasResponse]
    total_aliases: int


class AddAliasRequest(BaseModel):
    """Request to add a manual alias."""

    alias: str = Field(..., description="Alias name to add")
    source: str = Field("manual", description="Source of this alias")


class MergeRequest(BaseModel):
    """Request to merge entities."""

    source_entity_id: int = Field(
        ..., description="Entity to merge FROM (will be deleted)"
    )
    target_entity_id: int = Field(
        ..., description="Entity to merge INTO (will be kept)"
    )
    reason: Optional[str] = Field(None, description="Reason for merge")


class MergeResponse(BaseModel):
    """Response from merge operation."""

    success: bool
    merged_entity_id: int
    aliases_transferred: int
    merge_history_id: int
    error: Optional[str] = None


class SplitRequest(BaseModel):
    """Request to split an entity."""

    aliases_to_split: List[str] = Field(
        ..., description="Alias names to move to new entity"
    )
    new_entity_name: str = Field(..., description="Name for the new entity")
    reason: Optional[str] = Field(None, description="Reason for split")


class SplitResponse(BaseModel):
    """Response from split operation."""

    success: bool
    new_entity_id: int
    new_entity_name: str
    aliases_moved: int
    merge_history_id: int


class DuplicatePair(BaseModel):
    """A potential duplicate pair."""

    entity_a: Dict[str, Any]
    entity_b: Dict[str, Any]
    confidence: float
    match_method: str


class DuplicatesResponse(BaseModel):
    """Response with potential duplicates."""

    duplicates: List[DuplicatePair]
    total_found: int


class EntityStatsResponse(BaseModel):
    """Entity resolution statistics."""

    total_entities: int
    by_type: Dict[str, int]
    total_aliases: int
    total_merges: int
    avg_aliases_per_entity: float


class EntitySearchResponse(BaseModel):
    """Search results."""

    entities: List[CanonicalEntityResponse]
    total: int


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def entity_to_response(entity: CanonicalEntity) -> CanonicalEntityResponse:
    """Convert entity to response model."""
    return CanonicalEntityResponse(
        id=entity.id,
        entity_type=entity.entity_type,
        canonical_name=entity.canonical_name,
        normalized_name=entity.normalized_name,
        cik=entity.cik,
        crd=entity.crd,
        ticker=entity.ticker,
        cusip=entity.cusip,
        lei=entity.lei,
        website=entity.website,
        domain=entity.domain,
        city=entity.city,
        state=entity.state,
        country=entity.country,
        industry=entity.industry,
        entity_subtype=entity.entity_subtype,
        alias_count=entity.alias_count,
        source_count=entity.source_count,
        is_verified=entity.is_verified,
        created_at=entity.created_at.isoformat(),
        updated_at=entity.updated_at.isoformat(),
    )


def alias_to_response(alias: EntityAlias) -> AliasResponse:
    """Convert alias to response model."""
    return AliasResponse(
        id=alias.id,
        alias_name=alias.alias_name,
        normalized_alias=alias.normalized_alias,
        source_type=alias.source_type,
        source_id=alias.source_id,
        match_confidence=alias.match_confidence,
        match_method=alias.match_method,
        is_manual_override=alias.is_manual_override,
        is_primary=alias.is_primary,
        created_at=alias.created_at.isoformat(),
    )


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/resolve", response_model=ResolveResponse)
async def resolve_entity(
    name: str = Query(..., description="Entity name to resolve"),
    entity_type: str = Query(..., description="Entity type: 'company' or 'investor'"),
    cik: Optional[str] = Query(None, description="SEC CIK number"),
    crd: Optional[str] = Query(None, description="SEC CRD number"),
    ticker: Optional[str] = Query(None, description="Stock ticker"),
    cusip: Optional[str] = Query(None, description="CUSIP identifier"),
    lei: Optional[str] = Query(None, description="Legal Entity Identifier"),
    website: Optional[str] = Query(None, description="Entity website"),
    state: Optional[str] = Query(None, description="State/province"),
    country: Optional[str] = Query(None, description="Country"),
    industry: Optional[str] = Query(None, description="Industry"),
    source_type: Optional[str] = Query(None, description="Source of reference"),
    source_id: Optional[str] = Query(None, description="ID in source system"),
    auto_create: bool = Query(True, description="Create if not found"),
    db: Session = Depends(get_db),
):
    """
    Resolve an entity name to a canonical entity.

    Attempts matching in order of confidence:
    1. Exact identifier match (CIK, CRD, ticker, CUSIP, LEI)
    2. Domain match (same website)
    3. Name + location match (fuzzy name + same state/country)
    4. Name-only match (fuzzy name)

    If no confident match is found and auto_create=True, creates a new entity.
    """
    if entity_type not in ["company", "investor", "person"]:
        raise HTTPException(
            status_code=400,
            detail="entity_type must be 'company', 'investor', or 'person'",
        )

    resolver = EntityResolver(db)
    result = resolver.resolve(
        name=name,
        entity_type=entity_type,
        cik=cik,
        crd=crd,
        ticker=ticker,
        cusip=cusip,
        lei=lei,
        website=website,
        state=state,
        country=country,
        industry=industry,
        source_type=source_type,
        source_id=source_id,
        auto_create=auto_create,
    )

    # Get full entity if found
    canonical_entity = None
    if result.canonical_entity_id:
        entity = resolver.get_entity(result.canonical_entity_id)
        if entity:
            canonical_entity = entity_to_response(entity)

    return ResolveResponse(
        canonical_entity=canonical_entity,
        match_confidence=result.match_confidence,
        match_method=result.match_method,
        is_new=result.is_new,
        alternatives=[AlternativeMatch(**alt) for alt in result.alternatives],
    )


@router.post("/resolve", response_model=ResolveResponse)
async def resolve_entity_post(request: ResolveRequest, db: Session = Depends(get_db)):
    """
    Resolve an entity name to a canonical entity (POST version).

    Same as GET but accepts a JSON body for more complex requests.
    """
    if request.entity_type not in ["company", "investor", "person"]:
        raise HTTPException(
            status_code=400,
            detail="entity_type must be 'company', 'investor', or 'person'",
        )

    resolver = EntityResolver(db)

    identifiers = request.identifiers or EntityIdentifiers()

    result = resolver.resolve(
        name=request.name,
        entity_type=request.entity_type,
        cik=identifiers.cik,
        crd=identifiers.crd,
        ticker=identifiers.ticker,
        cusip=identifiers.cusip,
        lei=identifiers.lei,
        website=request.website,
        state=request.state,
        country=request.country,
        industry=request.industry,
        source_type=request.source_type,
        source_id=request.source_id,
        auto_create=request.auto_create,
    )

    # Get full entity if found
    canonical_entity = None
    if result.canonical_entity_id:
        entity = resolver.get_entity(result.canonical_entity_id)
        if entity:
            canonical_entity = entity_to_response(entity)

    return ResolveResponse(
        canonical_entity=canonical_entity,
        match_confidence=result.match_confidence,
        match_method=result.match_method,
        is_new=result.is_new,
        alternatives=[AlternativeMatch(**alt) for alt in result.alternatives],
    )


@router.post("/merge", response_model=MergeResponse)
async def merge_entities(request: MergeRequest, db: Session = Depends(get_db)):
    """
    Merge two entities.

    The source entity is merged INTO the target entity.
    All aliases from source are transferred to target.
    The source entity is deleted.
    """
    resolver = EntityResolver(db)

    result = resolver.merge_entities(
        source_entity_id=request.source_entity_id,
        target_entity_id=request.target_entity_id,
        reason=request.reason,
        performed_by="api",
    )

    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)

    return MergeResponse(
        success=result.success,
        merged_entity_id=result.merged_entity_id,
        aliases_transferred=result.aliases_transferred,
        merge_history_id=result.merge_history_id,
    )


@router.get("/duplicates", response_model=DuplicatesResponse)
async def get_duplicates(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    min_confidence: float = Query(0.70, description="Minimum match confidence"),
    max_confidence: float = Query(0.90, description="Maximum match confidence"),
    limit: int = Query(50, description="Maximum results", le=200),
    db: Session = Depends(get_db),
):
    """
    Find potential duplicate entities for review.

    Returns pairs of entities with similarity scores in the specified range.
    Default range (0.70-0.90) captures matches that need human review.
    """
    resolver = EntityResolver(db)

    duplicates = resolver.find_duplicates(
        entity_type=entity_type,
        min_confidence=min_confidence,
        max_confidence=max_confidence,
        limit=limit,
    )

    pairs = [
        DuplicatePair(
            entity_a={"id": d.entity_a_id, "canonical_name": d.entity_a_name},
            entity_b={"id": d.entity_b_id, "canonical_name": d.entity_b_name},
            confidence=round(d.confidence, 3),
            match_method=d.match_method,
        )
        for d in duplicates
    ]

    return DuplicatesResponse(duplicates=pairs, total_found=len(pairs))


@router.get("/search", response_model=EntitySearchResponse)
async def search_entities(
    q: str = Query(..., description="Search query"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(20, description="Maximum results", le=100),
    db: Session = Depends(get_db),
):
    """Search entities by name."""
    resolver = EntityResolver(db)

    entities = resolver.search_entities(query=q, entity_type=entity_type, limit=limit)

    return EntitySearchResponse(
        entities=[entity_to_response(e) for e in entities], total=len(entities)
    )


@router.get("/stats", response_model=EntityStatsResponse)
async def get_entity_stats(db: Session = Depends(get_db)):
    """Get entity resolution statistics."""
    resolver = EntityResolver(db)
    stats = resolver.get_stats()

    return EntityStatsResponse(**stats)


@router.get(
    "/by-identifier/{identifier_type}/{identifier_value}",
    response_model=CanonicalEntityResponse,
)
async def get_entity_by_identifier(
    identifier_type: str,
    identifier_value: str,
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    db: Session = Depends(get_db),
):
    """
    Get an entity by a specific identifier.

    Supported identifier types: cik, crd, ticker, cusip, lei
    """
    if identifier_type not in ["cik", "crd", "ticker", "cusip", "lei"]:
        raise HTTPException(
            status_code=400,
            detail="identifier_type must be one of: cik, crd, ticker, cusip, lei",
        )

    resolver = EntityResolver(db)

    try:
        entity = resolver.get_entity_by_identifier(
            identifier_type=identifier_type,
            identifier_value=identifier_value,
            entity_type=entity_type,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    return entity_to_response(entity)


# =============================================================================
# PARAMETERIZED ROUTES (must be after static routes to avoid conflicts)
# =============================================================================


@router.get("/{entity_id}", response_model=CanonicalEntityResponse)
async def get_entity(entity_id: int, db: Session = Depends(get_db)):
    """Get a canonical entity by ID."""
    resolver = EntityResolver(db)
    entity = resolver.get_entity(entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    return entity_to_response(entity)


@router.get("/{entity_id}/aliases", response_model=AliasListResponse)
async def get_entity_aliases(entity_id: int, db: Session = Depends(get_db)):
    """Get all aliases for a canonical entity."""
    resolver = EntityResolver(db)
    entity = resolver.get_entity(entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    aliases = resolver.get_aliases(entity_id)

    return AliasListResponse(
        canonical_entity_id=entity_id,
        canonical_name=entity.canonical_name,
        aliases=[alias_to_response(a) for a in aliases],
        total_aliases=len(aliases),
    )


@router.post("/{entity_id}/aliases", response_model=AliasResponse)
async def add_entity_alias(
    entity_id: int, request: AddAliasRequest, db: Session = Depends(get_db)
):
    """Manually add an alias to an entity."""
    resolver = EntityResolver(db)
    entity = resolver.get_entity(entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    try:
        alias = resolver.add_manual_alias(
            entity_id=entity_id, alias_name=request.alias, performed_by=request.source
        )
        return alias_to_response(alias)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.post("/{entity_id}/split", response_model=SplitResponse)
async def split_entity(
    entity_id: int, request: SplitRequest, db: Session = Depends(get_db)
):
    """
    Split aliases from an entity into a new entity.

    Creates a new entity with the specified aliases.
    Those aliases are removed from the original entity.
    """
    resolver = EntityResolver(db)

    try:
        new_entity, history_id = resolver.split_entity(
            entity_id=entity_id,
            aliases_to_split=request.aliases_to_split,
            new_entity_name=request.new_entity_name,
            reason=request.reason,
            performed_by="api",
        )

        return SplitResponse(
            success=True,
            new_entity_id=new_entity.id,
            new_entity_name=new_entity.canonical_name,
            aliases_moved=len(request.aliases_to_split),
            merge_history_id=history_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
