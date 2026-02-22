"""
PE Collection API endpoints.

Trigger and monitor PE data collection jobs.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/pe/collection",
    tags=["PE Intelligence - Collection"],
)


class CollectRequest(BaseModel):
    entity_type: str = Field(
        default="firm", description="firm, company, person, or deal"
    )
    sources: List[str] = Field(
        default=["firm_website", "bio_extractor"],
        description="Collection sources (sec_adv, firm_website, bio_extractor, sec_form_d, etc.)",
    )
    firm_id: Optional[int] = Field(default=None, description="Single firm ID")
    firm_ids: Optional[List[int]] = Field(default=None, description="Multiple firm IDs")
    company_id: Optional[int] = Field(default=None, description="Single company ID")
    company_ids: Optional[List[int]] = Field(
        default=None, description="Multiple company IDs"
    )
    max_concurrent: int = Field(default=5, ge=1, le=20)
    rate_limit_delay: float = Field(default=2.0, ge=0.1, le=30.0)


class CollectResponse(BaseModel):
    status: str
    message: str
    results: Optional[List[dict]] = None


async def _run_collection(config_dict: dict, db: Session):
    """Background task to run PE collection."""
    # Import here to ensure collectors are registered
    import app.sources.pe_collection  # noqa: F401
    from app.sources.pe_collection.orchestrator import PECollectionOrchestrator
    from app.sources.pe_collection.persister import PEPersister
    from app.sources.pe_collection.types import PECollectionConfig

    config = PECollectionConfig.from_dict(config_dict)
    orchestrator = PECollectionOrchestrator(db_session=db)
    results = await orchestrator.run_collection(config)

    # Persist collected items to DB
    persister = PEPersister(db)
    persist_stats = persister.persist_results(results)

    total_items = sum(r.items_found for r in results)
    logger.info(
        f"PE collection complete: {len(results)} results, {total_items} items found, "
        f"persisted={persist_stats['persisted']}, updated={persist_stats['updated']}, "
        f"failed={persist_stats['failed']}"
    )


@router.post("/collect", response_model=CollectResponse)
async def trigger_collection(
    request: CollectRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Trigger PE data collection for specified entities and sources.

    Runs collection in the background and returns immediately.
    """
    # Import to ensure collectors are registered
    import app.sources.pe_collection  # noqa: F401
    from app.sources.pe_collection.orchestrator import PECollectionOrchestrator

    # Validate sources
    valid_sources = {s.value for s in PECollectionOrchestrator._collectors.keys()}
    for src in request.sources:
        if src not in valid_sources and src not in [
            "sec_adv",
            "firm_website",
            "sec_form_d",
            "sec_13d",
            "linkedin_firm",
            "linkedin_people",
            "crunchbase",
            "news_api",
            "public_comps",
        ]:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown source '{src}'. Registered: {sorted(valid_sources)}",
            )

    config_dict = {
        "entity_type": request.entity_type,
        "sources": request.sources,
        "max_concurrent": request.max_concurrent,
        "rate_limit_delay": request.rate_limit_delay,
        "firm_id": request.firm_id,
        "firm_ids": request.firm_ids,
        "company_id": request.company_id,
        "company_ids": request.company_ids,
    }

    from app.core.job_queue_service import submit_job

    result = submit_job(
        db=db,
        job_type="pe",
        payload=config_dict,
        background_tasks=background_tasks,
        background_func=_run_collection,
        background_args=(config_dict, db),
    )

    return CollectResponse(
        status="started",
        message=(
            f"Collection started for entity_type={request.entity_type}, "
            f"sources={request.sources} (mode={result['mode']})"
        ),
    )


@router.get("/sources")
async def list_registered_sources():
    """List all registered collection sources."""
    import app.sources.pe_collection  # noqa: F401
    from app.sources.pe_collection.orchestrator import PECollectionOrchestrator

    registered = {
        src.value: cls.__name__
        for src, cls in PECollectionOrchestrator._collectors.items()
    }
    return {
        "registered_collectors": registered,
        "total": len(registered),
    }
