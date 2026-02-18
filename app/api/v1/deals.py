"""
Deal Flow Tracker API endpoints.

Provides endpoints for managing investment deal pipeline.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

from app.core.database import get_db
from app.deals.tracker import DealTracker, PIPELINE_STAGES

router = APIRouter(prefix="/deals", tags=["deals"])


# Request/Response Models


class CreateDealRequest(BaseModel):
    company_name: str = Field(..., description="Name of the target company")
    company_sector: Optional[str] = Field(None, description="Industry sector")
    company_stage: Optional[str] = Field(
        None, description="Company stage (seed, series_a, etc.)"
    )
    company_location: Optional[str] = Field(None, description="Company headquarters")
    company_website: Optional[str] = Field(None, description="Company website URL")
    deal_type: Optional[str] = Field(
        None, description="Deal type (primary, secondary, co-invest)"
    )
    deal_size_millions: Optional[float] = Field(
        None, description="Expected deal size in millions"
    )
    valuation_millions: Optional[float] = Field(
        None, description="Company valuation in millions"
    )
    pipeline_stage: Optional[str] = Field(
        "sourced", description="Initial pipeline stage"
    )
    priority: Optional[int] = Field(
        3, ge=1, le=5, description="Priority (1=highest, 5=lowest)"
    )
    fit_score: Optional[float] = Field(
        None, ge=0, le=100, description="Fit score 0-100"
    )
    source: Optional[str] = Field(
        None, description="Deal source (referral, inbound, etc.)"
    )
    source_contact: Optional[str] = Field(None, description="Source contact name")
    assigned_to: Optional[str] = Field(None, description="Assigned team member")
    tags: Optional[List[str]] = Field(
        default_factory=list, description="Tags for categorization"
    )


class UpdateDealRequest(BaseModel):
    company_name: Optional[str] = None
    company_sector: Optional[str] = None
    company_stage: Optional[str] = None
    company_location: Optional[str] = None
    company_website: Optional[str] = None
    deal_type: Optional[str] = None
    deal_size_millions: Optional[float] = None
    valuation_millions: Optional[float] = None
    pipeline_stage: Optional[str] = None
    priority: Optional[int] = Field(None, ge=1, le=5)
    fit_score: Optional[float] = Field(None, ge=0, le=100)
    source: Optional[str] = None
    source_contact: Optional[str] = None
    assigned_to: Optional[str] = None
    tags: Optional[List[str]] = None


class AddActivityRequest(BaseModel):
    activity_type: str = Field(
        ..., description="Type: note, meeting, call, email, document"
    )
    title: Optional[str] = Field(None, description="Activity title")
    description: Optional[str] = Field(None, description="Activity details")
    meeting_date: Optional[datetime] = Field(
        None, description="Meeting date/time if applicable"
    )
    attendees: Optional[List[str]] = Field(
        default_factory=list, description="Meeting attendees"
    )
    created_by: Optional[str] = Field(None, description="User who created the activity")


# Endpoints


@router.post("")
def create_deal(request: CreateDealRequest):
    """Create a new deal in the pipeline."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)
        result = tracker.create_deal(request.model_dump())
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.get("")
def list_deals(
    pipeline_stage: Optional[str] = Query(None, description="Filter by pipeline stage"),
    company_sector: Optional[str] = Query(
        None, description="Filter by sector (partial match)"
    ),
    assigned_to: Optional[str] = Query(None, description="Filter by assignee"),
    priority: Optional[int] = Query(None, ge=1, le=5, description="Filter by priority"),
    limit: int = Query(50, ge=1, le=200, description="Results per page"),
    offset: int = Query(0, ge=0, description="Results offset"),
):
    """List deals with optional filters."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)
        return tracker.list_deals(
            pipeline_stage=pipeline_stage,
            company_sector=company_sector,
            assigned_to=assigned_to,
            priority=priority,
            limit=limit,
            offset=offset,
        )
    finally:
        db.close()


@router.get("/pipeline")
def get_pipeline_summary():
    """Get pipeline summary with counts by stage and priority."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)
        return tracker.get_pipeline_summary()
    finally:
        db.close()


@router.get("/stages")
def get_pipeline_stages():
    """Get list of valid pipeline stages."""
    return {
        "stages": [
            {
                "id": "sourced",
                "name": "Sourced",
                "description": "Initial opportunity identified",
            },
            {
                "id": "reviewing",
                "name": "Reviewing",
                "description": "Initial review/screening",
            },
            {
                "id": "due_diligence",
                "name": "Due Diligence",
                "description": "Active due diligence",
            },
            {
                "id": "negotiation",
                "name": "Negotiation",
                "description": "Terms negotiation",
            },
            {
                "id": "closed_won",
                "name": "Closed Won",
                "description": "Investment made",
            },
            {
                "id": "closed_lost",
                "name": "Closed Lost",
                "description": "Did not invest",
            },
            {"id": "passed", "name": "Passed", "description": "Decided not to pursue"},
        ]
    }


@router.get("/{deal_id}")
def get_deal(deal_id: int):
    """Get deal details by ID."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)
        deal = tracker.get_deal(deal_id)
        if not deal:
            raise HTTPException(status_code=404, detail="Deal not found")
        return deal
    finally:
        db.close()


@router.patch("/{deal_id}")
def update_deal(deal_id: int, request: UpdateDealRequest):
    """Update deal fields."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)

        # Check if deal exists
        existing = tracker.get_deal(deal_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Deal not found")

        # Filter out None values
        updates = {k: v for k, v in request.model_dump().items() if v is not None}

        result = tracker.update_deal(deal_id, updates)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.delete("/{deal_id}")
def delete_deal(deal_id: int):
    """Delete a deal and all its activities."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)
        deleted = tracker.delete_deal(deal_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Deal not found")
        return {"message": "Deal deleted successfully", "id": deal_id}
    finally:
        db.close()


@router.post("/{deal_id}/activities")
def add_activity(deal_id: int, request: AddActivityRequest):
    """Add an activity to a deal."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)

        # Check if deal exists
        existing = tracker.get_deal(deal_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Deal not found")

        result = tracker.add_activity(deal_id, request.model_dump())
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.get("/{deal_id}/activities")
def get_activities(
    deal_id: int,
    limit: int = Query(50, ge=1, le=200, description="Max activities to return"),
):
    """Get activities for a deal."""
    db = next(get_db())
    try:
        tracker = DealTracker(db)

        # Check if deal exists
        existing = tracker.get_deal(deal_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Deal not found")

        activities = tracker.get_activities(deal_id, limit=limit)
        return {
            "deal_id": deal_id,
            "activities": activities,
            "count": len(activities),
        }
    finally:
        db.close()
