"""
People Collection Jobs API endpoints.

Provides endpoints for job management, monitoring, and alerts:
- Collection job status and history
- Scheduled job management
- Change alerts and digests
"""

from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import PeopleCollectionJob
from app.jobs.people_collection_scheduler import (
    PeopleCollectionScheduler,
    process_pending_jobs,
    get_people_schedule_status,
    register_people_collection_schedules,
)
from app.jobs.change_monitor import ChangeMonitor, AlertDigestGenerator


router = APIRouter(prefix="/people-jobs", tags=["People Collection Jobs"])


# =============================================================================
# Response Models
# =============================================================================


class JobSummary(BaseModel):
    """Summary of a collection job."""

    id: int
    job_type: str
    company_id: Optional[int] = None
    company_count: int = 0
    status: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    changes_detected: int = 0


class JobDetail(BaseModel):
    """Detailed job information."""

    id: int
    job_type: str
    company_id: Optional[int] = None
    company_ids: Optional[List[int]] = None
    config: Optional[dict] = None
    status: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    people_found: int = 0
    people_created: int = 0
    people_updated: int = 0
    changes_detected: int = 0
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None


class JobStatsResponse(BaseModel):
    """Job statistics."""

    period_days: int
    total_jobs: int
    by_status: dict
    by_type: dict
    total_people_found: int
    total_people_created: int
    total_changes_detected: int
    success_rate: float


class AgentMetrics(BaseModel):
    """Metrics for a single collection agent."""

    agent: str
    total_jobs: int
    successful_jobs: int
    failed_jobs: int
    zero_people_jobs: int
    success_rate: float
    avg_people_found: float
    total_people_found: int
    common_errors: List[dict]


class CollectionMetricsResponse(BaseModel):
    """Aggregate collection metrics across all agents."""

    period_days: int
    generated_at: str
    summary: dict
    by_agent: List[AgentMetrics]
    failure_analysis: dict
    recommendations: List[str]


class ScheduleJobRequest(BaseModel):
    """Request to schedule a collection job."""

    job_type: str = Field(..., description="website_crawl, sec_parse, news_scan")
    company_ids: Optional[List[int]] = Field(
        None, description="Specific companies to collect"
    )
    priority: str = Field("all", description="all, portfolio, public")
    limit: int = Field(50, ge=1, le=200, description="Max companies to process")


class ChangeAlertItem(BaseModel):
    """A change alert."""

    change_id: int
    person_name: str
    company_id: int
    company_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    announced_date: Optional[str] = None
    detected_date: Optional[str] = None
    is_c_suite: bool = False
    significance_score: Optional[int] = None


class AlertsResponse(BaseModel):
    """Change alerts response."""

    filter_type: Optional[str] = None
    filter_id: Optional[int] = None
    period_days: int
    total_alerts: int
    alerts: List[ChangeAlertItem]


class DigestSummary(BaseModel):
    """Summary stats in digest."""

    period_days: int
    total_changes: int
    by_type: dict
    c_suite_changes: int
    board_changes: int
    high_significance: int
    companies_affected: int


class DigestResponse(BaseModel):
    """Weekly digest response."""

    generated_at: str
    period: str
    filter: Optional[dict] = None
    summary: DigestSummary
    highlights: List[dict]
    all_changes: List[dict]


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/", response_model=List[JobSummary])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    job_type: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    List collection jobs.

    Returns recent jobs with summary information.
    """
    query = db.query(PeopleCollectionJob)

    if status:
        query = query.filter(PeopleCollectionJob.status == status)
    if job_type:
        query = query.filter(PeopleCollectionJob.job_type == job_type)

    jobs = query.order_by(PeopleCollectionJob.created_at.desc()).limit(limit).all()

    return [
        JobSummary(
            id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            company_count=len(job.company_ids)
            if job.company_ids
            else (1 if job.company_id else 0),
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            people_found=job.people_found or 0,
            people_created=job.people_created or 0,
            changes_detected=job.changes_detected or 0,
        )
        for job in jobs
    ]


@router.get("/stats", response_model=JobStatsResponse)
async def get_job_stats(
    days: int = Query(7, ge=1, le=90, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get collection job statistics.

    Returns summary stats for the specified period.
    """
    scheduler = PeopleCollectionScheduler(db)
    stats = scheduler.get_job_stats(days=days)

    return JobStatsResponse(**stats)


@router.get("/metrics", response_model=CollectionMetricsResponse)
async def get_collection_metrics(
    days: int = Query(7, ge=1, le=90, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get detailed collection metrics for debugging extraction issues.

    Returns:
    - Success/failure rates per agent (website, sec, news)
    - Most common failure reasons
    - Zero-extraction analysis
    - Recommendations for improvement
    """
    from datetime import datetime, timedelta
    from sqlalchemy import func, case
    from collections import Counter

    cutoff = datetime.utcnow() - timedelta(days=days)

    # Query all jobs in period
    jobs = (
        db.query(PeopleCollectionJob)
        .filter(PeopleCollectionJob.created_at >= cutoff)
        .all()
    )

    if not jobs:
        return CollectionMetricsResponse(
            period_days=days,
            generated_at=datetime.utcnow().isoformat(),
            summary={"total_jobs": 0, "message": "No jobs found in period"},
            by_agent=[],
            failure_analysis={},
            recommendations=["Run some collection jobs to generate metrics"],
        )

    # Aggregate by job type (agent)
    agent_stats = {}
    all_errors = []

    for job in jobs:
        job_type = job.job_type or "unknown"

        if job_type not in agent_stats:
            agent_stats[job_type] = {
                "total": 0,
                "success": 0,
                "failed": 0,
                "zero_people": 0,
                "people_found": 0,
                "errors": [],
            }

        stats = agent_stats[job_type]
        stats["total"] += 1
        stats["people_found"] += job.people_found or 0

        if job.status in ("success", "completed_with_errors"):
            stats["success"] += 1
        elif job.status == "failed":
            stats["failed"] += 1

        if (job.people_found or 0) == 0 and job.status != "pending":
            stats["zero_people"] += 1

        # Collect errors
        if job.errors:
            for err in job.errors:
                stats["errors"].append(err)
                all_errors.append({"job_type": job_type, "error": err})

        if job.warnings:
            for warn in job.warnings:
                stats["errors"].append(warn)

    # Build agent metrics
    agent_metrics = []
    for agent, stats in agent_stats.items():
        # Count common errors
        error_counts = Counter(stats["errors"])
        common_errors = [
            {"error": err[:100], "count": count}
            for err, count in error_counts.most_common(5)
        ]

        agent_metrics.append(
            AgentMetrics(
                agent=agent,
                total_jobs=stats["total"],
                successful_jobs=stats["success"],
                failed_jobs=stats["failed"],
                zero_people_jobs=stats["zero_people"],
                success_rate=round(stats["success"] / stats["total"] * 100, 1)
                if stats["total"] > 0
                else 0,
                avg_people_found=round(stats["people_found"] / stats["total"], 1)
                if stats["total"] > 0
                else 0,
                total_people_found=stats["people_found"],
                common_errors=common_errors,
            )
        )

    # Overall failure analysis
    total_jobs = len(jobs)
    total_zero_people = sum(
        1 for j in jobs if (j.people_found or 0) == 0 and j.status != "pending"
    )
    total_failed = sum(1 for j in jobs if j.status == "failed")

    # Categorize error types
    error_categories = {
        "no_leadership_page": 0,
        "js_rendering": 0,
        "llm_extraction": 0,
        "network_error": 0,
        "no_website": 0,
        "no_cik": 0,
        "other": 0,
    }

    for err_info in all_errors:
        err = err_info["error"].lower()
        if "no leadership page" in err or "no pages found" in err:
            error_categories["no_leadership_page"] += 1
        elif "javascript" in err or "js-rendered" in err:
            error_categories["js_rendering"] += 1
        elif "llm" in err or "extraction" in err:
            error_categories["llm_extraction"] += 1
        elif "timeout" in err or "network" in err or "fetch" in err:
            error_categories["network_error"] += 1
        elif "no website" in err:
            error_categories["no_website"] += 1
        elif "no cik" in err:
            error_categories["no_cik"] += 1
        else:
            error_categories["other"] += 1

    # Generate recommendations
    recommendations = []

    if error_categories["no_leadership_page"] > 0:
        recommendations.append(
            f"Expand URL patterns in PageFinder - {error_categories['no_leadership_page']} jobs couldn't find leadership pages"
        )
    if error_categories["js_rendering"] > 0:
        recommendations.append(
            f"Add JavaScript rendering support - {error_categories['js_rendering']} pages require JS"
        )
    if error_categories["no_website"] > 0:
        recommendations.append(
            f"Enrich company data - {error_categories['no_website']} companies missing website URLs"
        )
    if error_categories["no_cik"] > 0:
        recommendations.append(
            f"Add CIK lookup - {error_categories['no_cik']} companies missing SEC CIK"
        )
    if total_zero_people > total_jobs * 0.5:
        recommendations.append(
            "High zero-extraction rate (>50%) - review LLM prompts and HTML cleaning"
        )

    if not recommendations:
        recommendations.append(
            "Collection metrics look healthy - no urgent issues detected"
        )

    return CollectionMetricsResponse(
        period_days=days,
        generated_at=datetime.utcnow().isoformat(),
        summary={
            "total_jobs": total_jobs,
            "total_successful": sum(
                1 for j in jobs if j.status in ("success", "completed_with_errors")
            ),
            "total_failed": total_failed,
            "total_zero_people": total_zero_people,
            "zero_people_rate": round(total_zero_people / total_jobs * 100, 1)
            if total_jobs > 0
            else 0,
            "total_people_found": sum(j.people_found or 0 for j in jobs),
        },
        by_agent=agent_metrics,
        failure_analysis=error_categories,
        recommendations=recommendations,
    )


@router.get("/{job_id}", response_model=JobDetail)
async def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a specific job.
    """
    job = db.get(PeopleCollectionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobDetail(
        id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        company_ids=job.company_ids,
        config=job.config,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        people_found=job.people_found or 0,
        people_created=job.people_created or 0,
        people_updated=job.people_updated or 0,
        changes_detected=job.changes_detected or 0,
        errors=job.errors,
        warnings=job.warnings,
    )


@router.post("/schedule", response_model=JobSummary)
async def schedule_job(
    request: ScheduleJobRequest,
    db: Session = Depends(get_db),
):
    """
    Schedule a new collection job.

    Creates a pending job for the specified companies.
    """
    valid_types = {"website_crawl", "sec_parse", "news_scan", "sec_8k_check"}
    if request.job_type not in valid_types:
        raise HTTPException(
            status_code=400, detail=f"job_type must be one of: {valid_types}"
        )

    scheduler = PeopleCollectionScheduler(db)

    if request.company_ids:
        company_ids = request.company_ids
    else:
        # Get companies based on priority
        companies = scheduler.get_companies_for_refresh(
            job_type=request.job_type,
            limit=request.limit,
            priority=request.priority,
        )
        company_ids = [c.id for c in companies]

    if not company_ids:
        raise HTTPException(
            status_code=400, detail="No companies found for the specified criteria"
        )

    job = scheduler.create_batch_job(
        job_type=request.job_type,
        company_ids=company_ids,
        config={
            "source": "api",
            "priority": request.priority,
        },
    )

    return JobSummary(
        id=job.id,
        job_type=job.job_type,
        company_count=len(company_ids),
        status=job.status,
        created_at=job.created_at,
    )


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Cancel a pending job.

    Only pending jobs can be cancelled.
    """
    job = db.get(PeopleCollectionJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "pending":
        raise HTTPException(
            status_code=400, detail=f"Cannot cancel job with status: {job.status}"
        )

    job.status = "cancelled"
    job.completed_at = datetime.utcnow()
    db.commit()

    return {"status": "cancelled", "job_id": job_id}


@router.post("/cleanup-stuck")
async def cleanup_stuck_jobs(
    max_age_hours: int = Query(4, ge=1, le=24),
    db: Session = Depends(get_db),
):
    """
    Mark stuck jobs as failed.

    Jobs running longer than max_age_hours are considered stuck.
    """
    scheduler = PeopleCollectionScheduler(db)
    count = scheduler.cleanup_stuck_jobs(max_age_hours=max_age_hours)

    return {
        "cleaned_up": count,
        "max_age_hours": max_age_hours,
    }


@router.post("/process")
async def process_jobs(
    max_jobs: int = Query(5, ge=1, le=20, description="Max jobs to process"),
):
    """
    Manually trigger processing of pending collection jobs.

    This picks up pending jobs and executes them using the
    PeopleCollectionOrchestrator.
    """
    result = await process_pending_jobs(max_jobs=max_jobs)
    return result


@router.get("/schedules/status")
async def get_schedule_status():
    """
    Get status of all people collection scheduled jobs.

    Shows next run times for:
    - Job processor (every 10 min)
    - Weekly website refresh (Sundays 2 AM)
    - Daily SEC check (weekdays 6 PM)
    - Daily news scan (8 AM)
    - Stuck job cleanup (every 2 hours)
    """
    return get_people_schedule_status()


@router.post("/schedules/register")
async def register_schedules():
    """
    Register people collection schedules with APScheduler.

    Call this if schedules are not running after a restart.
    """
    results = register_people_collection_schedules()
    registered = sum(1 for v in results.values() if v)
    return {
        "registered": registered,
        "total": len(results),
        "details": results,
    }


# =============================================================================
# Alert Endpoints
# =============================================================================


@router.get("/alerts/recent", response_model=AlertsResponse)
async def get_recent_alerts(
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Get recent leadership change alerts across all companies.
    """
    monitor = ChangeMonitor(db)
    changes = monitor.get_recent_changes(days=days, c_suite_only=c_suite_only)

    from app.core.people_models import IndustrialCompany

    alerts = []
    for change in changes:
        company = db.get(IndustrialCompany, change.company_id)
        alerts.append(
            ChangeAlertItem(
                change_id=change.id,
                person_name=change.person_name,
                company_id=change.company_id,
                company_name=company.name if company else "Unknown",
                change_type=change.change_type,
                old_title=change.old_title,
                new_title=change.new_title,
                announced_date=change.announced_date.isoformat()
                if change.announced_date
                else None,
                detected_date=change.detected_date.isoformat()
                if change.detected_date
                else None,
                is_c_suite=change.is_c_suite,
                significance_score=change.significance_score,
            )
        )

    return AlertsResponse(
        period_days=days,
        total_alerts=len(alerts),
        alerts=alerts,
    )


@router.get("/alerts/portfolio/{portfolio_id}", response_model=AlertsResponse)
async def get_portfolio_alerts(
    portfolio_id: int,
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Get leadership change alerts for a specific portfolio.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_portfolio_alerts(
        portfolio_id=portfolio_id,
        days=days,
        c_suite_only=c_suite_only,
    )

    return AlertsResponse(
        filter_type="portfolio",
        filter_id=portfolio_id,
        period_days=days,
        total_alerts=len(alerts),
        alerts=[ChangeAlertItem(**a) for a in alerts],
    )


@router.get("/alerts/watchlist/{watchlist_id}")
async def get_watchlist_alerts(
    watchlist_id: int,
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """
    Get alerts for people on a specific watchlist.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_watchlist_alerts(watchlist_id=watchlist_id, days=days)

    return {
        "filter_type": "watchlist",
        "filter_id": watchlist_id,
        "period_days": days,
        "total_alerts": len(alerts),
        "alerts": alerts,
    }


@router.get("/alerts/industry/{industry}", response_model=AlertsResponse)
async def get_industry_alerts(
    industry: str,
    days: int = Query(7, ge=1, le=90),
    c_suite_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    """
    Get leadership change alerts for a specific industry.
    """
    monitor = ChangeMonitor(db)
    alerts = monitor.get_industry_alerts(
        industry=industry,
        days=days,
        c_suite_only=c_suite_only,
    )

    return AlertsResponse(
        filter_type="industry",
        period_days=days,
        total_alerts=len(alerts),
        alerts=[ChangeAlertItem(**a) for a in alerts],
    )


# =============================================================================
# Digest Endpoints
# =============================================================================


@router.get("/digest/weekly")
async def get_weekly_digest(
    portfolio_id: Optional[int] = Query(None, description="Filter to portfolio"),
    industry: Optional[str] = Query(None, description="Filter to industry"),
    db: Session = Depends(get_db),
):
    """
    Generate weekly digest of leadership changes.

    Can be filtered to a specific portfolio or industry.
    """
    generator = AlertDigestGenerator(db)
    digest = generator.generate_weekly_digest(
        portfolio_id=portfolio_id,
        industry=industry,
    )

    return digest


@router.get("/digest/watchlist/{watchlist_id}")
async def get_watchlist_digest(
    watchlist_id: int,
    days: int = Query(7, ge=1, le=30),
    db: Session = Depends(get_db),
):
    """
    Generate digest for a specific watchlist.
    """
    generator = AlertDigestGenerator(db)
    digest = generator.generate_watchlist_digest(
        watchlist_id=watchlist_id,
        days=days,
    )

    if "error" in digest:
        raise HTTPException(status_code=404, detail=digest["error"])

    return digest


@router.get("/digest/summary")
async def get_change_summary(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """
    Get summary of all leadership changes in period.
    """
    monitor = ChangeMonitor(db)
    summary = monitor.get_change_summary(days=days)

    return summary


# =============================================================================
# Deep Collection Endpoint
# =============================================================================


class RecursiveCollectRequest(BaseModel):
    """Request body for recursive collection."""

    # Structure discovery
    discover_structure: bool = Field(
        True, description="Auto-discover subsidiaries and divisions"
    )
    max_units: int = Field(25, ge=1, le=50, description="Max business units to process")

    # Per-unit collection
    run_sec_per_unit: bool = Field(
        True, description="Run SEC EDGAR per public subsidiary"
    )
    run_website_per_unit: bool = Field(True, description="Run website crawl per unit")
    run_news_per_unit: bool = Field(False, description="Run news scan per unit (slow)")
    max_crawl_pages_per_unit: int = Field(
        20, ge=5, le=100, description="Max pages per unit"
    )

    # LinkedIn discovery
    run_linkedin: bool = Field(True, description="Run LinkedIn Google search discovery")
    max_linkedin_searches: int = Field(
        100, ge=0, le=500, description="Total LinkedIn searches"
    )

    # Functional org mapping
    map_functions: List[str] = Field(
        default=["technology"],
        description="Functions to map (technology, finance, legal)",
    )
    function_depth: int = Field(
        3, ge=1, le=5, description="Levels below C-suite to map"
    )

    # Org chart
    build_master_org_chart: bool = Field(
        True, description="Build master org chart after collection"
    )


class DeepCollectRequest(BaseModel):
    """Request body for deep collection."""

    seed_urls: Optional[List[str]] = Field(None, description="Seed URLs for deep crawl")
    allowed_domains: Optional[List[str]] = Field(
        None, description="Allowed domains for crawling"
    )
    subsidiary_names: Optional[List[str]] = Field(
        None, description="Subsidiary names for news search"
    )
    newsroom_url: Optional[str] = Field(None, description="Direct newsroom URL")
    division_context: Optional[str] = Field(
        None, description="Context about company divisions for org chart"
    )
    run_sec: bool = Field(True, description="Run SEC EDGAR collection")
    run_website: bool = Field(True, description="Run deep website crawl")
    run_news: bool = Field(True, description="Run deep news scan")
    build_org_chart: bool = Field(True, description="Build org chart after collection")
    max_crawl_pages: int = Field(50, ge=5, le=200, description="Max pages to crawl")
    news_days_back: int = Field(1825, ge=30, le=3650, description="News lookback days")


@router.post("/recursive-collect/{company_id}")
async def recursive_collect(
    company_id: int,
    request: Optional[RecursiveCollectRequest] = None,
    db: Session = Depends(get_db),
):
    """
    Run recursive corporate structure discovery and deep people collection.

    This is the most comprehensive collection pipeline. It:
    1. Discovers corporate structure (subsidiaries via SEC Exhibit 21, website, LLM)
    2. Runs deep collection for each business unit (SEC + website crawl)
    3. Discovers additional people via LinkedIn Google search
    4. Builds functional org maps (e.g., technology org 3 levels deep)
    5. Constructs master cross-subsidiary org chart

    Designed for Fortune 500 companies with complex corporate structures.

    Expected yield: 100-200+ people with organizational hierarchy.
    Expected LLM cost: ~$12-17.
    """
    from app.sources.people_collection.recursive_collector import (
        RecursiveCollector,
        RecursiveCollectConfig,
    )

    # Build config from request
    config = RecursiveCollectConfig()
    if request:
        config.discover_structure = request.discover_structure
        config.max_units = request.max_units
        config.run_sec_per_unit = request.run_sec_per_unit
        config.run_website_per_unit = request.run_website_per_unit
        config.run_news_per_unit = request.run_news_per_unit
        config.max_crawl_pages_per_unit = request.max_crawl_pages_per_unit
        config.run_linkedin = request.run_linkedin
        config.max_linkedin_searches = request.max_linkedin_searches
        config.map_functions = request.map_functions
        config.function_depth = request.function_depth
        config.build_master_org_chart = request.build_master_org_chart

    collector = RecursiveCollector(db_session=db)
    result = await collector.collect(company_id, config)

    return result.to_dict()


@router.post("/deep-collect/{company_id}")
async def deep_collect(
    company_id: int,
    request: Optional[DeepCollectRequest] = None,
    db: Session = Depends(get_db),
):
    """
    Run deep, multi-phase people collection for a company.

    This is an intensive collection pipeline designed for Fortune 500 companies.
    It runs:
    1. SEC EDGAR (proxy + 10-K + Form 4 + 8-K)
    2. Deep website crawl (BFS across multiple domains)
    3. Deep news scan (5-year lookback, multi-query)
    4. LLM-powered org chart construction

    Expected yield: 100-250+ people with organizational hierarchy.
    Expected duration: 5-15 minutes.
    Expected LLM cost: ~$5-10.

    The endpoint runs synchronously and returns the full result. For very large
    companies, consider running as a background task.
    """
    from app.sources.people_collection.deep_collection_orchestrator import (
        DeepCollectionOrchestrator,
        DeepCollectionConfig,
    )

    # Build config from request
    config = DeepCollectionConfig()
    if request:
        config.seed_urls = request.seed_urls
        config.allowed_domains = request.allowed_domains
        config.subsidiary_names = request.subsidiary_names
        config.newsroom_url = request.newsroom_url
        config.division_context = request.division_context
        config.run_sec = request.run_sec
        config.run_website = request.run_website
        config.run_news = request.run_news
        config.build_org_chart = request.build_org_chart
        config.max_crawl_pages = request.max_crawl_pages
        config.news_days_back = request.news_days_back

    orchestrator = DeepCollectionOrchestrator(db_session=db)
    result = await orchestrator.deep_collect(company_id, config)

    return result.to_dict()


# =============================================================================
# Diagnostic Endpoints
# =============================================================================


@router.post("/test/{company_id}")
async def test_collection(
    company_id: int,
    sources: str = Query(
        "website", description="Comma-separated sources: website,sec,news"
    ),
    db: Session = Depends(get_db),
):
    """
    Run collection for a single company with full diagnostics.

    This endpoint is for debugging why collections may not find people.
    Returns detailed information about:
    - Company data availability (website URL, CIK)
    - Which agents ran and their results
    - Pages checked and extraction results
    - Errors at each step

    Use this to understand why a specific company returned 0 people.
    """
    from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator

    source_list = [s.strip() for s in sources.split(",")]

    orchestrator = PeopleCollectionOrchestrator(db_session=db)
    diagnostics = await orchestrator.collect_company_with_diagnostics(
        company_id=company_id,
        sources=source_list,
    )

    return diagnostics.to_dict()


@router.get("/test/company-check/{company_id}")
async def check_company_data(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Check if a company has the required data for collection.

    Returns information about:
    - Whether company exists
    - Whether website URL is configured
    - Whether SEC CIK is configured
    - Last crawl dates
    """
    from app.core.people_models import IndustrialCompany

    company = (
        db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    )

    if not company:
        return {
            "found": False,
            "company_id": company_id,
            "error": "Company not found in database",
        }

    return {
        "found": True,
        "company_id": company_id,
        "name": company.name,
        "data_availability": {
            "has_website": bool(company.website),
            "website_url": company.website,
            "has_cik": bool(company.cik),
            "cik": company.cik,
        },
        "crawl_history": {
            "last_crawled_date": company.last_crawled_date.isoformat()
            if company.last_crawled_date
            else None,
            "leadership_last_updated": company.leadership_last_updated.isoformat()
            if company.leadership_last_updated
            else None,
        },
        "collection_ready": {
            "website_collection": bool(company.website),
            "sec_collection": bool(company.cik),
            "news_collection": True,  # Always available
        },
        "recommendations": _get_collection_recommendations(company),
    }


def _get_collection_recommendations(company) -> List[str]:
    """Generate recommendations for improving collection for a company."""
    recommendations = []

    if not company.website:
        recommendations.append(
            "Add website URL to enable website-based leadership collection"
        )
    if not company.cik:
        recommendations.append(
            "Add SEC CIK to enable SEC filing-based leadership collection"
        )
    if company.website and company.last_crawled_date is None:
        recommendations.append("Website configured but never crawled - run collection")
    if company.cik and company.leadership_last_updated is None:
        recommendations.append(
            "CIK configured but SEC filings never parsed - run SEC collection"
        )

    if not recommendations:
        recommendations.append("Company is fully configured for collection")

    return recommendations


@router.get("/test/batch-check")
async def check_batch_readiness(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Check collection readiness for multiple companies.

    Returns summary of how many companies are ready for each collection type.
    """
    from app.core.people_models import IndustrialCompany
    from sqlalchemy import func

    # Count totals
    total = db.query(func.count(IndustrialCompany.id)).scalar()
    with_website = (
        db.query(func.count(IndustrialCompany.id))
        .filter(
            IndustrialCompany.website.isnot(None),
            IndustrialCompany.website != "",
        )
        .scalar()
    )
    with_cik = (
        db.query(func.count(IndustrialCompany.id))
        .filter(
            IndustrialCompany.cik.isnot(None),
            IndustrialCompany.cik != "",
        )
        .scalar()
    )
    never_crawled = (
        db.query(func.count(IndustrialCompany.id))
        .filter(
            IndustrialCompany.last_crawled_date.is_(None),
        )
        .scalar()
    )

    # Get sample companies without website
    missing_website = (
        db.query(IndustrialCompany)
        .filter(
            IndustrialCompany.website.is_(None) | (IndustrialCompany.website == ""),
        )
        .limit(5)
        .all()
    )

    return {
        "summary": {
            "total_companies": total,
            "with_website_url": with_website,
            "with_sec_cik": with_cik,
            "never_crawled": never_crawled,
        },
        "percentages": {
            "website_ready": round(with_website / total * 100, 1) if total > 0 else 0,
            "sec_ready": round(with_cik / total * 100, 1) if total > 0 else 0,
        },
        "sample_missing_website": [
            {"id": c.id, "name": c.name} for c in missing_website
        ],
    }
