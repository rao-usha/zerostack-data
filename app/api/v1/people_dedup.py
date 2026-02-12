"""
People Deduplication API endpoints.

Provides endpoints for scanning, reviewing, and merging duplicate person records:
- Trigger dedup scans (company-scoped or global)
- Review pending merge candidates
- Approve or reject merges
- View merge decision history
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.services.dedup_service import DedupService


router = APIRouter(prefix="/people-dedup", tags=["People Deduplication"])


# =============================================================================
# Request/Response Models
# =============================================================================

class ScanRequest(BaseModel):
    """Request to trigger a dedup scan."""
    company_id: Optional[int] = Field(None, description="Scope scan to a specific company")
    limit: Optional[int] = Field(1000, ge=1, le=10000, description="Max people to scan")


class MergeRequest(BaseModel):
    """Request to approve a merge."""
    candidate_id: int = Field(..., description="The merge candidate record ID")
    canonical_person_id: int = Field(..., description="Which person ID to keep")


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/scan")
async def scan_for_duplicates(
    request: ScanRequest,
    db: Session = Depends(get_db),
):
    """
    Trigger a deduplication scan.

    Groups people by last name and compares within groups using fuzzy matching.
    High-confidence matches (≥0.95 similarity + shared company) are auto-merged.
    Ambiguous matches (≥0.80 similarity) are queued for manual review.
    """
    service = DedupService(db)
    stats = service.scan_for_duplicates(
        company_id=request.company_id,
        limit=request.limit or 1000,
    )
    return stats


@router.get("/candidates")
async def get_candidates(
    status: str = Query("pending", description="Filter by status: pending, auto_merged, approved, rejected"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List merge candidates with full person details.

    Returns pairs of potentially duplicate people with similarity scores
    and evidence notes.
    """
    valid_statuses = {"pending", "auto_merged", "approved", "rejected"}
    if status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of: {valid_statuses}",
        )

    service = DedupService(db)
    candidates = service.get_pending_candidates(
        limit=limit,
        offset=offset,
        status=status,
    )
    return candidates


@router.post("/merge")
async def approve_merge(
    request: MergeRequest,
    db: Session = Depends(get_db),
):
    """
    Approve and execute a merge from the review queue.

    Transfers missing data from the duplicate to the canonical person,
    reassigns all FK references, and marks the duplicate as non-canonical.
    """
    service = DedupService(db)
    result = service.manual_merge(
        candidate_id=request.candidate_id,
        canonical_person_id=request.canonical_person_id,
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(
    candidate_id: int,
    db: Session = Depends(get_db),
):
    """
    Reject a merge candidate.

    Marks the pair as 'rejected' so they won't appear in the review queue again.
    """
    service = DedupService(db)
    result = service.reject_merge(candidate_id)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@router.get("/history")
async def get_merge_history(
    person_id: Optional[int] = Query(None, description="Filter by person ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    View merge decision history.

    Shows past auto-merges, approved merges, and rejections.
    """
    service = DedupService(db)
    history = service.get_merge_history(
        person_id=person_id,
        limit=limit,
        offset=offset,
    )
    return history
