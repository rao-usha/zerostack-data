"""
Family Office Collection API endpoints.

Provides:
- Seed FO registry to database
- Run collection jobs
- Check collection status
- Coverage reports
"""

import logging
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db
from app.core.family_office_models import FamilyOffice
from app.sources.family_office_collection.config import (
    get_fo_registry,
    get_fo_by_name,
    get_registry_stats,
)
from app.sources.family_office_collection.types import (
    FoCollectionConfig,
    FoCollectionSource,
)
from app.sources.family_office_collection.runner import FoCollectionOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fo-collection", tags=["Family Office Collection"])


# ============================================================================
# Request/Response Models
# ============================================================================


class FoCollectionJobRequest(BaseModel):
    """Request to start a FO collection job."""

    fo_types: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    sources: List[str] = ["website", "news", "deals"]
    max_concurrent_fos: int = 5
    rate_limit_delay: float = 2.0


class FoCollectionJobResponse(BaseModel):
    """Response from a collection job."""

    status: str
    total_fos: int
    completed_fos: int
    successful_fos: int = 0
    failed_fos: int = 0
    total_items: int = 0
    message: str = ""


class FoCoverageResponse(BaseModel):
    """Family Office coverage statistics."""

    total_in_registry: int
    total_in_database: int
    coverage_pct: float
    by_type: dict
    by_region: dict


class FoSeedResponse(BaseModel):
    """Response from seeding FOs."""

    status: str
    total_in_registry: int
    created: int
    updated: int
    skipped: int
    message: str


# ============================================================================
# Seed Endpoints
# ============================================================================


@router.post("/seed-fos", response_model=FoSeedResponse)
async def seed_fos_from_registry(
    db: Session = Depends(get_db),
):
    """
    Seed the database with Family Offices from the expanded registry.

    This endpoint loads all 300+ family offices from the registry JSON
    and creates/updates FamilyOffice records in the database.

    Safe to run multiple times - uses upsert logic.
    """
    registry = get_fo_registry()

    created = 0
    updated = 0
    skipped = 0

    for entry in registry:
        try:
            # Check if FO already exists
            existing = (
                db.query(FamilyOffice).filter(FamilyOffice.name == entry.name).first()
            )

            # Convert AUM to string for estimated_wealth field
            estimated_wealth = None
            if entry.estimated_aum_billions:
                estimated_wealth = f"${entry.estimated_aum_billions}B+"

            if existing:
                # Update existing record
                existing.principal_name = entry.principal_name
                existing.principal_family = entry.principal_family
                existing.website = entry.website_url
                existing.type = entry.fo_type
                existing.region = entry.region
                existing.country = (
                    entry.country_code
                )  # FoRegistryEntry uses country_code
                existing.city = entry.city
                existing.state_province = entry.state_province
                existing.estimated_wealth = estimated_wealth
                existing.investment_focus = entry.investment_focus
                existing.sectors_of_interest = entry.sectors_of_interest
                existing.geographic_focus = entry.geographic_focus
                existing.check_size_range = entry.check_size_range
                existing.updated_at = datetime.utcnow()
                updated += 1
            else:
                # Create new FO
                fo = FamilyOffice(
                    name=entry.name,
                    principal_name=entry.principal_name,
                    principal_family=entry.principal_family,
                    website=entry.website_url,
                    type=entry.fo_type,
                    region=entry.region,
                    country=entry.country_code,
                    city=entry.city,
                    state_province=entry.state_province,
                    estimated_wealth=estimated_wealth,
                    investment_focus=entry.investment_focus,
                    sectors_of_interest=entry.sectors_of_interest,
                    geographic_focus=entry.geographic_focus,
                    check_size_range=entry.check_size_range,
                    status="Active",
                    data_sources=["registry"],
                )
                db.add(fo)
                created += 1

            # Commit after each FO to avoid bulk insert issues
            db.commit()

        except Exception as e:
            db.rollback()
            logger.warning(f"Error seeding FO {entry.name}: {e}")
            skipped += 1
            continue

    return FoSeedResponse(
        status="success",
        total_in_registry=len(registry),
        created=created,
        updated=updated,
        skipped=skipped,
        message=f"Seeded {created} new FOs, updated {updated}, skipped {skipped}",
    )


# ============================================================================
# Collection Job Endpoints
# ============================================================================


@router.post("/jobs", response_model=FoCollectionJobResponse)
async def create_collection_job(
    request: FoCollectionJobRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Create and run a Family Office collection job.

    Collects data from configured sources for FOs matching filters.
    """
    # Build config
    sources = []
    for s in request.sources:
        try:
            sources.append(FoCollectionSource(s))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid source: {s}. Valid: website, news, deals",
            )

    config = FoCollectionConfig(
        fo_types=request.fo_types,
        regions=request.regions,
        sources=sources,
        max_concurrent_fos=request.max_concurrent_fos,
        rate_limit_delay=request.rate_limit_delay,
    )

    from app.core.job_queue_service import submit_job, WORKER_MODE

    if WORKER_MODE:
        submit_result = submit_job(
            db=db,
            job_type="fo",
            payload={
                "fo_types": request.fo_types,
                "regions": request.regions,
                "sources": request.sources,
                "max_concurrent_fos": request.max_concurrent_fos,
                "rate_limit_delay": request.rate_limit_delay,
            },
        )
        return FoCollectionJobResponse(
            status="queued",
            total_fos=0,
            completed_fos=0,
            message=f"Job queued (id={submit_result['job_queue_id']})",
        )

    # Legacy: run collection synchronously with database session
    orchestrator = FoCollectionOrchestrator(config=config, db=db)
    result = await orchestrator.run_collection()

    return FoCollectionJobResponse(
        status=result.get("status", "unknown"),
        total_fos=result.get("total_fos", 0),
        completed_fos=result.get("completed_fos", 0),
        successful_fos=result.get("successful_fos", 0),
        failed_fos=result.get("failed_fos", 0),
        total_items=result.get("total_items", 0),
        message=f"Collected {result.get('total_items', 0)} items from {result.get('successful_fos', 0)} FOs",
    )


@router.post("/collect/{fo_name}")
async def collect_single_fo(
    fo_name: str,
    sources: List[str] = Query(default=["website", "news"]),
    db: Session = Depends(get_db),
):
    """
    Collect data for a single Family Office by name.
    """
    # Check FO exists in registry
    fo = get_fo_by_name(fo_name)
    if not fo:
        raise HTTPException(
            status_code=404, detail=f"FO not found in registry: {fo_name}"
        )

    # Build config
    source_enums = []
    for s in sources:
        try:
            source_enums.append(FoCollectionSource(s))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid source: {s}")

    config = FoCollectionConfig(sources=source_enums)
    orchestrator = FoCollectionOrchestrator(config=config, db=db)

    results = await orchestrator.collect_single_fo(fo_name)

    return {
        "status": "success",
        "fo_name": fo_name,
        "results": [r.to_dict() for r in results],
    }


# ============================================================================
# Coverage & Status Endpoints
# ============================================================================


@router.get("/coverage", response_model=FoCoverageResponse)
async def get_fo_coverage(
    db: Session = Depends(get_db),
):
    """
    Get Family Office coverage statistics.

    Shows how many FOs from registry are in database and breakdown by type/region.
    """
    registry = get_fo_registry()
    get_registry_stats()

    from sqlalchemy import func

    # Count in database
    db_count = db.query(FamilyOffice).count()

    # By type in database
    db_by_type = {}
    type_counts = (
        db.query(FamilyOffice.type, func.count(FamilyOffice.id))
        .group_by(FamilyOffice.type)
        .all()
    )
    for fo_type, count in type_counts:
        db_by_type[fo_type or "unknown"] = count

    # By region in database
    db_by_region = {}
    region_counts = (
        db.query(FamilyOffice.region, func.count(FamilyOffice.id))
        .group_by(FamilyOffice.region)
        .all()
    )
    for region, count in region_counts:
        db_by_region[region or "unknown"] = count

    coverage_pct = (db_count / len(registry) * 100) if registry else 0

    return FoCoverageResponse(
        total_in_registry=len(registry),
        total_in_database=db_count,
        coverage_pct=round(coverage_pct, 1),
        by_type=db_by_type,
        by_region=db_by_region,
    )


@router.get("/registry-stats")
async def get_registry_statistics():
    """
    Get statistics about the Family Office registry.

    Returns counts by type, region, and other metadata.
    """
    return get_registry_stats()


@router.get("/registry")
async def list_registry_fos(
    fo_type: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    """
    List Family Offices from the registry.

    Use this to browse available FOs before running collection.
    """
    registry = get_fo_registry()

    # Apply filters
    if fo_type:
        registry = [e for e in registry if e.fo_type == fo_type]
    if region:
        registry = [e for e in registry if e.region == region]

    # Paginate
    total = len(registry)
    registry = registry[offset : offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [
            {
                "name": e.name,
                "principal_name": e.principal_name,
                "fo_type": e.fo_type,
                "region": e.region,
                "country_code": e.country_code,
                "website_url": e.website_url,
                "estimated_aum_billions": e.estimated_aum_billions,
            }
            for e in registry
        ],
    }


# ============================================================================
# FO Query Endpoints
# ============================================================================


@router.get("/family-offices/{fo_id}/contacts")
async def get_fo_contacts(
    fo_id: int,
    db: Session = Depends(get_db),
):
    """
    Get contacts for a specific Family Office.

    Returns key personnel including principals, investment team, and operations.
    """
    from app.core.family_office_models import FamilyOfficeContact

    fo = db.query(FamilyOffice).filter(FamilyOffice.id == fo_id).first()
    if not fo:
        raise HTTPException(status_code=404, detail="Family Office not found")

    contacts = (
        db.query(FamilyOfficeContact)
        .filter(FamilyOfficeContact.family_office_id == fo_id)
        .all()
    )

    return {
        "fo_id": fo_id,
        "fo_name": fo.name,
        "contacts": [
            {
                "id": c.id,
                "full_name": c.full_name,
                "title": c.title,
                "role": c.role,
                "email": c.email,
                "phone": c.phone,
                "linkedin_url": c.linkedin_url,
                "is_primary_contact": c.is_primary_contact,
                "status": c.status,
            }
            for c in contacts
        ],
        "key_contacts": fo.key_contacts or [],
    }


@router.get("/family-offices/by-sector")
async def get_fos_by_sector(
    sector: str = Query(
        ..., description="Sector of interest (e.g., 'AI', 'Healthcare', 'Fintech')"
    ),
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db),
):
    """
    Find Family Offices interested in a specific sector.

    Searches sectors_of_interest field for matches.
    """

    # Search for FOs with matching sector in their sectors_of_interest array
    fos = (
        db.query(FamilyOffice)
        .filter(FamilyOffice.sectors_of_interest.any(sector))
        .limit(limit)
        .all()
    )

    return {
        "sector": sector,
        "count": len(fos),
        "family_offices": [
            {
                "id": fo.id,
                "name": fo.name,
                "principal_name": fo.principal_name,
                "region": fo.region,
                "estimated_wealth": fo.estimated_wealth,
                "investment_focus": fo.investment_focus,
                "sectors_of_interest": fo.sectors_of_interest,
            }
            for fo in fos
        ],
    }


@router.get("/family-offices/active-investors")
async def get_active_fo_investors(
    investment_focus: Optional[str] = Query(
        None,
        description="Filter by investment focus (e.g., 'Venture Capital', 'Real Estate')",
    ),
    min_aum_billions: Optional[float] = Query(
        None, description="Minimum estimated AUM in billions"
    ),
    region: Optional[str] = Query(None, description="Filter by region"),
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db),
):
    """
    Find actively investing Family Offices.

    Returns FOs marked as actively investing with optional filters.
    """
    query = db.query(FamilyOffice).filter(
        FamilyOffice.actively_investing == True, FamilyOffice.status == "Active"
    )

    if investment_focus:
        query = query.filter(FamilyOffice.investment_focus.any(investment_focus))

    if region:
        query = query.filter(FamilyOffice.region == region)

    # Note: estimated_wealth is stored as string, so exact filtering is approximate
    fos = query.limit(limit).all()

    return {
        "filters": {
            "investment_focus": investment_focus,
            "min_aum_billions": min_aum_billions,
            "region": region,
        },
        "count": len(fos),
        "family_offices": [
            {
                "id": fo.id,
                "name": fo.name,
                "principal_name": fo.principal_name,
                "region": fo.region,
                "country": fo.country,
                "estimated_wealth": fo.estimated_wealth,
                "investment_focus": fo.investment_focus,
                "sectors_of_interest": fo.sectors_of_interest,
                "check_size_range": fo.check_size_range,
                "website": fo.website,
            }
            for fo in fos
        ],
    }


@router.get("/family-offices/{fo_id}/summary")
async def get_fo_summary(
    fo_id: int,
    db: Session = Depends(get_db),
):
    """
    Get a comprehensive summary of a Family Office.

    Includes all available data about the FO.
    """
    fo = db.query(FamilyOffice).filter(FamilyOffice.id == fo_id).first()
    if not fo:
        raise HTTPException(status_code=404, detail="Family Office not found")

    return {
        "id": fo.id,
        "name": fo.name,
        "legal_name": fo.legal_name,
        "type": fo.type,
        "region": fo.region,
        "country": fo.country,
        "city": fo.city,
        "state_province": fo.state_province,
        "principal_family": fo.principal_family,
        "principal_name": fo.principal_name,
        "estimated_wealth": fo.estimated_wealth,
        "estimated_aum": fo.estimated_aum,
        "investment_focus": fo.investment_focus,
        "sectors_of_interest": fo.sectors_of_interest,
        "geographic_focus": fo.geographic_focus,
        "stage_preference": fo.stage_preference,
        "check_size_range": fo.check_size_range,
        "investment_thesis": fo.investment_thesis,
        "notable_investments": fo.notable_investments,
        "website": fo.website,
        "linkedin": fo.linkedin,
        "key_contacts": fo.key_contacts,
        "data_sources": fo.data_sources,
        "sec_registered": fo.sec_registered,
        "sec_crd_number": fo.sec_crd_number,
        "actively_investing": fo.actively_investing,
        "accepts_outside_capital": fo.accepts_outside_capital,
        "status": fo.status,
        "last_updated_date": fo.last_updated_date,
    }
