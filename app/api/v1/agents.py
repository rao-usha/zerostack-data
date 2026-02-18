"""
Agentic Data Intelligence API endpoints.

Provides access to autonomous AI research agents for comprehensive data synthesis.
"""

from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional, List

from app.core.database import get_db
from app.agents.company_researcher import CompanyResearchAgent, ResearchStatus
from app.agents.deep_researcher import DeepResearchAgent

router = APIRouter(prefix="/agents", tags=["Agentic Intelligence"])


# Request/Response Models
class CompanyResearchRequest(BaseModel):
    """Request to start company research."""

    company_name: str = Field(..., description="Company name to research")
    domain: Optional[str] = Field(None, description="Company domain for enrichment")
    ticker: Optional[str] = Field(None, description="Stock ticker if public")
    priority_sources: Optional[List[str]] = Field(
        None,
        description="Priority data sources: enrichment, github, glassdoor, app_store, web_traffic, news, sec_filings, corporate_registry, scoring",
    )
    force_refresh: bool = Field(False, description="Force new research even if cached")


class ResearchJobResponse(BaseModel):
    """Response for research job status."""

    job_id: str
    company_name: str
    status: str
    progress: float
    sources_completed: List[str]
    sources_pending: List[str]
    sources_failed: List[str]
    created_at: str
    completed_at: Optional[str] = None
    result: Optional[dict] = None
    error: Optional[str] = None


class DeepResearchRequest(BaseModel):
    """Request for deep multi-turn research."""

    company_name: str = Field(..., description="Company name to deeply research")
    include_follow_ups: bool = Field(True, description="Run follow-up analysis prompts")


# ============================================================================
# DEEP RESEARCH ENDPOINTS (Multi-turn LLM Analysis)
# ============================================================================


@router.post("/deep-research")
async def start_deep_research(
    request: DeepResearchRequest, db: Session = Depends(get_db)
):
    """
    Start deep multi-turn research on a company.

    This performs comprehensive investment analysis using structured prompts:
    1. Collects data from all sources (GitHub, SEC, News, etc.)
    2. Runs initial investment analysis prompt through LLM
    3. Performs follow-up analyses: competitive positioning, risk deep-dive
    4. Generates final investment recommendation

    Returns job_id to track progress. Use GET /agents/deep-research/{job_id} for status.

    **Note**: Requires OPENAI_API_KEY or ANTHROPIC_API_KEY configured.
    """
    agent = DeepResearchAgent(db)

    job_id = await agent.start_deep_research(
        company_name=request.company_name, include_follow_ups=request.include_follow_ups
    )

    return {
        "status": "started",
        "job_id": job_id,
        "company_name": request.company_name,
        "message": "Deep research started. Poll GET /agents/deep-research/{job_id} for status.",
        "phases": ["collecting", "analyzing", "synthesizing", "complete"],
        "include_follow_ups": request.include_follow_ups,
    }


@router.get("/deep-research/{job_id}")
def get_deep_research_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get status and results of a deep research job.

    Phases:
    - collecting: Gathering data from all sources
    - analyzing: Running LLM analysis prompts
    - synthesizing: Combining analyses into final report
    - complete: Research finished
    - failed: Research failed (check error_message)
    """
    agent = DeepResearchAgent(db)
    result = agent.get_job_status(job_id)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Deep research job {job_id} not found"
        )

    return result


# ============================================================================
# STANDARD RESEARCH ENDPOINTS
# ============================================================================


@router.get("/sources")
def list_available_sources(db: Session = Depends(get_db)):
    """
    List available data sources for research.

    Shows which sources are configured and available.
    """
    agent = CompanyResearchAgent(db)
    return agent.get_available_sources()


@router.post("/research/company")
async def start_company_research(
    request: CompanyResearchRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Start autonomous company research across all data sources.

    The agent will:
    1. Query all available data sources in parallel
    2. Synthesize findings into a unified company profile
    3. Identify data gaps and confidence levels
    4. Cache results for 7 days

    Returns job_id to track progress. Use GET /agents/research/{job_id} to check status.
    """
    agent = CompanyResearchAgent(db)

    # Check cache first unless force refresh
    if not request.force_refresh:
        cached = agent.get_cached_research(request.company_name)
        if cached:
            return {
                "status": "cached",
                "job_id": None,
                "company_name": request.company_name,
                "message": "Found cached research",
                "cache_age_hours": cached.get("cache_age_hours"),
                "result": cached,
            }

    # Start new research job
    job_id = agent.start_research(
        company_name=request.company_name,
        domain=request.domain,
        ticker=request.ticker,
        priority_sources=request.priority_sources,
    )

    return {
        "status": "started",
        "job_id": job_id,
        "company_name": request.company_name,
        "message": "Research job started. Poll GET /agents/research/{job_id} for status.",
        "estimated_sources": 9,
    }


@router.post("/research/batch")
async def start_batch_research(
    companies: List[CompanyResearchRequest], db: Session = Depends(get_db)
):
    """
    Start research for multiple companies.

    Maximum 10 companies per batch. Returns list of job_ids.
    """
    if len(companies) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 companies per batch")

    if len(companies) < 1:
        raise HTTPException(status_code=400, detail="At least 1 company required")

    agent = CompanyResearchAgent(db)
    results = []

    for company in companies:
        # Check cache first
        if not company.force_refresh:
            cached = agent.get_cached_research(company.company_name)
            if cached:
                results.append(
                    {
                        "company_name": company.company_name,
                        "status": "cached",
                        "job_id": None,
                    }
                )
                continue

        # Start new research
        job_id = agent.start_research(
            company_name=company.company_name,
            domain=company.domain,
            ticker=company.ticker,
            priority_sources=company.priority_sources,
        )
        results.append(
            {
                "company_name": company.company_name,
                "status": "started",
                "job_id": job_id,
            }
        )

    return {
        "batch_size": len(companies),
        "started": sum(1 for r in results if r["status"] == "started"),
        "cached": sum(1 for r in results if r["status"] == "cached"),
        "results": results,
    }


# IMPORTANT: These specific routes MUST come before the {job_id} parameter route
@router.get("/research/jobs")
def list_research_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    List recent research jobs.

    Optionally filter by status: pending, running, completed, failed
    """
    agent = CompanyResearchAgent(db)
    jobs = agent.list_jobs(status=status, limit=limit)

    return {"count": len(jobs), "jobs": jobs}


@router.get("/research/stats")
def get_research_stats(db: Session = Depends(get_db)):
    """
    Get research agent statistics.

    Returns job counts, cache stats, and source success rates.
    """
    agent = CompanyResearchAgent(db)
    return agent.get_stats()


@router.get("/research/company/{company_name}")
def get_company_research(
    company_name: str,
    max_age_hours: int = Query(
        168, description="Max cache age in hours (default 7 days)"
    ),
    db: Session = Depends(get_db),
):
    """
    Get cached research for a company.

    Returns the most recent research results if available and within max_age_hours.
    Use POST /agents/research/company to start new research if not cached.
    """
    agent = CompanyResearchAgent(db)
    result = agent.get_cached_research(company_name, max_age_hours=max_age_hours)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No cached research for '{company_name}'. Use POST /agents/research/company to start research.",
        )

    return result


# Parameterized route MUST come after specific routes
@router.get("/research/{job_id}")
def get_research_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get status and results of a research job.

    Status values:
    - pending: Job queued but not started
    - running: Research in progress
    - completed: All sources queried, profile synthesized
    - failed: Research failed (check error field)
    """
    agent = CompanyResearchAgent(db)
    result = agent.get_job_status(job_id)

    if not result:
        raise HTTPException(status_code=404, detail=f"Research job {job_id} not found")

    return result


@router.delete("/research/{job_id}")
def cancel_research_job(job_id: str, db: Session = Depends(get_db)):
    """
    Cancel a pending or running research job.
    """
    agent = CompanyResearchAgent(db)
    result = agent.cancel_job(job_id)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Research job {job_id} not found or already completed",
        )

    return {"status": "cancelled", "job_id": job_id}
