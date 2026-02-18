"""
Due Diligence API endpoints.

Provides access to automated due diligence reports and risk analysis.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional, List

from app.core.database import get_db
from app.agents.due_diligence import DueDiligenceAgent

router = APIRouter(prefix="/diligence", tags=["Due Diligence"])


# Request/Response Models
class StartDiligenceRequest(BaseModel):
    """Request to start due diligence."""

    company_name: str = Field(..., description="Company name to analyze")
    domain: Optional[str] = Field(None, description="Company domain for enrichment")
    template: str = Field("standard", description="DD template: standard, quick, deep")
    focus_areas: Optional[List[str]] = Field(
        None,
        description="Specific areas to focus on: financial, team, legal, competitive, market, operational",
    )


@router.get("/templates")
def list_templates(db: Session = Depends(get_db)):
    """
    List available due diligence templates.

    Templates define which sections are included in the DD report:
    - standard: Comprehensive coverage of all areas
    - quick: Fast risk screening (financial, legal, team)
    - deep: Exhaustive analysis for major investments
    """
    agent = DueDiligenceAgent(db)
    templates = agent.get_templates()

    return {"count": len(templates), "templates": templates}


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """
    Get due diligence statistics.

    Returns job counts, average risk scores, and risk level distribution.
    """
    agent = DueDiligenceAgent(db)
    return agent.get_stats()


@router.get("/jobs")
def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    List recent due diligence jobs.

    Optionally filter by status: pending, researching, analyzing, generating, completed, failed
    """
    agent = DueDiligenceAgent(db)
    jobs = agent.list_jobs(status=status, limit=limit)

    return {"count": len(jobs), "jobs": jobs}


@router.post("/start")
def start_diligence(request: StartDiligenceRequest, db: Session = Depends(get_db)):
    """
    Start due diligence process for a company.

    The agent will:
    1. Run company research using T41 agent
    2. Analyze risk across all categories (financial, team, legal, competitive, market, operational)
    3. Detect red flags from news and data signals
    4. Calculate risk score (0-100) and risk level
    5. Generate structured DD memo with recommendations

    Returns job_id to track progress. Use GET /diligence/{job_id} to check status.
    """
    # Validate template
    valid_templates = ["standard", "quick", "deep"]
    if request.template not in valid_templates:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid template. Must be one of: {', '.join(valid_templates)}",
        )

    # Validate focus areas if provided
    valid_areas = ["financial", "team", "legal", "competitive", "market", "operational"]
    if request.focus_areas:
        invalid = [a for a in request.focus_areas if a not in valid_areas]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid focus areas: {', '.join(invalid)}. Valid: {', '.join(valid_areas)}",
            )

    agent = DueDiligenceAgent(db)

    job_id = agent.start_diligence(
        company_name=request.company_name,
        domain=request.domain,
        template=request.template,
        focus_areas=request.focus_areas,
    )

    return {
        "status": "started",
        "job_id": job_id,
        "company_name": request.company_name,
        "template": request.template,
        "message": "Due diligence started. Poll GET /diligence/{job_id} for results.",
    }


@router.get("/company/{company_name}")
def get_cached_diligence(company_name: str, db: Session = Depends(get_db)):
    """
    Get cached due diligence report for a company.

    Returns the most recent DD report if available (cached for 30 days).
    Use POST /diligence/start to run new DD if not cached.
    """
    agent = DueDiligenceAgent(db)
    result = agent.get_cached_diligence(company_name)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No cached due diligence for '{company_name}'. Use POST /diligence/start to begin.",
        )

    return result


@router.get("/{job_id}")
def get_diligence_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get status and results of a due diligence job.

    Status values:
    - pending: Job queued
    - researching: Running company research (T41)
    - analyzing: Analyzing risk categories
    - generating: Generating DD memo
    - completed: Analysis complete, memo ready
    - failed: Process failed (check error field)

    Risk levels:
    - low (0-25): Strong fundamentals, no red flags
    - moderate (26-50): Some concerns, manageable
    - high (51-75): Significant concerns
    - critical (76-100): Major red flags
    """
    agent = DueDiligenceAgent(db)
    result = agent.get_job_status(job_id)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Due diligence job {job_id} not found"
        )

    return result
