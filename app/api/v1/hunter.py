"""
Data Hunter API endpoints.

Provides AI-powered autonomous data gap filling:
- Scan for missing data fields
- Start hunt jobs to fill gaps
- Track job progress and results
- View statistics and queue
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.core.database import get_db
from app.agents.data_hunter import DataHunterAgent

router = APIRouter(tags=["Data Hunter"])


# =============================================================================
# Request/Response Models
# =============================================================================


class StartHuntRequest(BaseModel):
    """Request to start a hunt job."""

    entity_type: Optional[str] = Field(
        None, description="Filter by entity type (company)"
    )
    fields: Optional[List[str]] = Field(None, description="Specific fields to hunt")
    limit: int = Field(50, ge=1, le=500, description="Max gaps to process")
    min_priority: float = Field(
        0.0, ge=0.0, le=1.0, description="Minimum priority score"
    )


class HuntJobResponse(BaseModel):
    """Response for hunt job operations."""

    job_id: str
    status: str
    total_gaps: int
    processed: int = 0
    filled: int = 0
    failed: int = 0
    fill_rate: Optional[float] = None
    duration_seconds: Optional[float] = None
    results: Optional[List[Dict[str, Any]]] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class GapItem(BaseModel):
    """A single data gap."""

    id: int
    entity_type: str
    entity_name: str
    field_name: str
    priority_score: float
    status: str
    attempts: int
    current_value: Optional[str] = None
    filled_value: Optional[str] = None
    filled_source: Optional[str] = None
    confidence: Optional[float] = None


class GapQueueResponse(BaseModel):
    """Response for gap queue listing."""

    gaps: List[Dict[str, Any]]
    total: int
    by_field: Dict[str, int]
    by_status: Dict[str, int]


class ScanResponse(BaseModel):
    """Response for gap scan operation."""

    scanned_records: int
    new_gaps_found: int
    gaps_by_field: Dict[str, int]


class HuntStatsResponse(BaseModel):
    """Response for hunt statistics."""

    total_gaps: int
    gaps_by_status: Dict[str, int]
    fill_rate: float
    by_field: Dict[str, Dict[str, Any]]
    source_performance: List[Dict[str, Any]]
    recent_fills: List[Dict[str, Any]]


class EntityHuntResponse(BaseModel):
    """Response for single entity hunt."""

    entity: str
    gaps_found: int
    gaps_filled: int
    results: List[Dict[str, Any]]


# =============================================================================
# API Endpoints
# =============================================================================


@router.post("/hunter/start", response_model=HuntJobResponse)
async def start_hunt_job(
    request: StartHuntRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Start a new hunt job to fill data gaps.

    The job runs in the background, processing gaps by priority.
    Use GET /hunter/job/{job_id} to check progress.
    """
    hunter = DataHunterAgent(db)

    # Start the job
    job = hunter.start_hunt_job(
        entity_type=request.entity_type,
        fields=request.fields,
        limit=request.limit,
        min_priority=request.min_priority,
    )

    if not job:
        raise HTTPException(status_code=400, detail="Failed to start hunt job")

    # Process in background
    job_id = job["job_id"]
    background_tasks.add_task(_process_hunt_job, db, job_id)

    return HuntJobResponse(
        job_id=job["job_id"],
        status=job["status"],
        total_gaps=job["total_gaps"],
        processed=0,
        filled=0,
        failed=0,
        created_at=job.get("created_at"),
    )


def _process_hunt_job(db: Session, job_id: str):
    """Background task to process hunt job."""
    hunter = DataHunterAgent(db)
    hunter.process_hunt_job(job_id)


@router.get("/hunter/job/{job_id}", response_model=HuntJobResponse)
async def get_job_status(
    job_id: str,
    include_results: bool = Query(True, description="Include detailed results"),
    db: Session = Depends(get_db),
):
    """
    Get the status and results of a hunt job.
    """
    hunter = DataHunterAgent(db)
    job = hunter.get_job_status(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    # Calculate fill rate and duration
    fill_rate = None
    if job.get("processed", 0) > 0:
        fill_rate = job.get("filled", 0) / job["processed"]

    duration = None
    if job.get("started_at") and job.get("completed_at"):
        started = job["started_at"]
        completed = job["completed_at"]
        if isinstance(started, datetime) and isinstance(completed, datetime):
            duration = (completed - started).total_seconds()

    return HuntJobResponse(
        job_id=job["job_id"],
        status=job["status"],
        total_gaps=job.get("total_gaps", 0),
        processed=job.get("processed", 0),
        filled=job.get("filled", 0),
        failed=job.get("failed", 0),
        fill_rate=fill_rate,
        duration_seconds=duration,
        results=job.get("results") if include_results else None,
        created_at=job.get("created_at"),
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at"),
    )


@router.get("/hunter/queue", response_model=GapQueueResponse)
async def get_gap_queue(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    field: Optional[str] = Query(None, description="Filter by field name"),
    status: Optional[str] = Query(None, description="Filter by status"),
    min_priority: float = Query(0.0, ge=0.0, le=1.0, description="Minimum priority"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db),
):
    """
    View the queue of data gaps waiting to be filled.
    """
    hunter = DataHunterAgent(db)
    result = hunter.get_gap_queue(
        entity_type=entity_type,
        field=field,
        status=status,
        min_priority=min_priority,
        limit=limit,
        offset=offset,
    )

    return GapQueueResponse(
        gaps=result.get("gaps", []),
        total=result.get("total", 0),
        by_field=result.get("by_field", {}),
        by_status=result.get("by_status", {}),
    )


@router.get("/hunter/stats", response_model=HuntStatsResponse)
async def get_hunt_stats(db: Session = Depends(get_db)):
    """
    Get overall statistics for the data hunter.

    Includes:
    - Total gaps and fill rates
    - Performance by field type
    - Source reliability scores
    - Recent successful fills
    """
    hunter = DataHunterAgent(db)
    stats = hunter.get_stats()

    return HuntStatsResponse(
        total_gaps=stats.get("total_gaps", 0),
        gaps_by_status=stats.get("gaps_by_status", {}),
        fill_rate=stats.get("fill_rate", 0.0),
        by_field=stats.get("by_field", {}),
        source_performance=stats.get("source_performance", []),
        recent_fills=stats.get("recent_fills", []),
    )


@router.post("/hunter/scan", response_model=ScanResponse)
async def scan_for_gaps(
    entity_type: Optional[str] = Query("company", description="Entity type to scan"),
    db: Session = Depends(get_db),
):
    """
    Scan for new data gaps in the database.

    Identifies records with missing fields and adds them to the gap queue.
    """
    hunter = DataHunterAgent(db)
    result = hunter.scan_for_gaps(entity_type=entity_type)

    return ScanResponse(
        scanned_records=result.get("scanned_records", 0),
        new_gaps_found=result.get("new_gaps_found", 0),
        gaps_by_field=result.get("gaps_by_field", {}),
    )


@router.post("/hunter/entity/{name}", response_model=EntityHuntResponse)
async def hunt_entity(name: str, db: Session = Depends(get_db)):
    """
    Hunt missing data for a specific entity.

    Scans the entity for gaps and attempts to fill them immediately.
    """
    hunter = DataHunterAgent(db)
    result = hunter.hunt_entity(name)

    return EntityHuntResponse(
        entity=name,
        gaps_found=result.get("gaps_found", 0),
        gaps_filled=result.get("gaps_filled", 0),
        results=result.get("results", []),
    )


@router.get("/hunter/provenance/{entity_name}")
async def get_provenance(
    entity_name: str,
    field: Optional[str] = Query(None, description="Filter by field"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Get data provenance (audit trail) for an entity.

    Shows history of data updates with sources and confidence scores.
    """
    hunter = DataHunterAgent(db)
    provenance = hunter.get_provenance(entity_name, field=field, limit=limit)

    return {"entity": entity_name, "records": provenance, "total": len(provenance)}


@router.get("/hunter/sources")
async def get_source_reliability(db: Session = Depends(get_db)):
    """
    Get reliability scores for all data sources.

    Shows success rates per source per field type.
    """
    hunter = DataHunterAgent(db)
    sources = hunter.get_source_reliability()

    return {"sources": sources, "total": len(sources)}
