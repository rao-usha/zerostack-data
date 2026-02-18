"""
GitHub Repository Analytics API Endpoints.

T34: Track developer activity as a proxy for tech company health.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.github import GitHubAnalyticsService

router = APIRouter(prefix="/github", tags=["github"])


# Response Models


class OrgMetrics(BaseModel):
    """Organization metrics."""

    total_stars: int
    total_forks: int
    repo_count: Optional[int] = None
    top_repos: List[str]
    primary_languages: List[str]


class OrganizationResponse(BaseModel):
    """Organization overview response."""

    login: str
    name: Optional[str] = None
    description: Optional[str] = None
    blog: Optional[str] = None
    location: Optional[str] = None
    email: Optional[str] = None
    twitter_username: Optional[str] = None
    public_repos: Optional[int] = None
    followers: Optional[int] = None
    github_created_at: Optional[str] = None
    metrics: OrgMetrics
    velocity_score: Optional[int] = None
    last_fetched_at: Optional[str] = None


class RepositorySummary(BaseModel):
    """Repository summary."""

    name: str
    full_name: str
    description: Optional[str] = None
    language: Optional[str] = None
    stars: int
    forks: int
    open_issues: Optional[int] = None
    is_fork: bool = False
    is_archived: bool = False
    topics: Optional[List[str]] = None
    pushed_at: Optional[str] = None
    github_created_at: Optional[str] = None


class ReposResponse(BaseModel):
    """Repository list response."""

    org: str
    total: int
    limit: int
    offset: int
    repositories: List[RepositorySummary]


class WeeklyActivity(BaseModel):
    """Weekly activity data."""

    week: str
    commits: int
    repos_active: Optional[int] = None


class ActivityTrends(BaseModel):
    """Activity trends."""

    commit_trend: str
    total_commits: Optional[int] = None


class ActivityResponse(BaseModel):
    """Activity trends response."""

    org: str
    period: str
    weekly_activity: List[WeeklyActivity]
    trends: ActivityTrends
    top_repos_analyzed: List[str]


class ContributorSummary(BaseModel):
    """Contributor summary."""

    username: str
    avatar_url: Optional[str] = None
    total_contributions: int
    repos_contributed: int


class ContributorsResponse(BaseModel):
    """Contributors response."""

    org: str
    total_contributors: int
    contributors: List[ContributorSummary]


class VelocityBreakdown(BaseModel):
    """Velocity score breakdown."""

    commit_frequency: int
    pr_velocity: int
    issue_resolution: int
    contributor_growth: int
    release_cadence: int


class VelocityResponse(BaseModel):
    """Velocity score response."""

    org: str
    velocity_score: int
    breakdown: VelocityBreakdown
    percentile: int
    comparison: str


class RepositoryDetail(BaseModel):
    """Full repository details."""

    name: str
    full_name: str
    description: Optional[str] = None
    homepage: Optional[str] = None
    language: Optional[str] = None
    languages: Optional[dict] = None
    stars: int
    forks: int
    watchers: int
    open_issues: int
    size_kb: Optional[int] = None
    default_branch: Optional[str] = None
    is_fork: bool
    is_archived: bool
    topics: Optional[List[str]] = None
    license_name: Optional[str] = None
    github_created_at: Optional[str] = None
    pushed_at: Optional[str] = None
    last_fetched_at: Optional[str] = None


class LanguageStat(BaseModel):
    """Language statistics."""

    language: str
    count: int
    stars: int


class StatsResponse(BaseModel):
    """Aggregate statistics response."""

    total_organizations: int
    total_repositories: int
    total_stars: Optional[int] = None
    total_forks: Optional[int] = None
    languages_tracked: int
    top_languages: List[LanguageStat]


class FetchResponse(BaseModel):
    """Fetch result response."""

    status: str
    org: str
    message: str


# Endpoints


@router.get(
    "/org/{org}",
    response_model=OrganizationResponse,
    summary="Get organization overview",
    description="""
    Get overview for a GitHub organization including repos, stars, and velocity score.

    Returns cached data if available. Use POST /github/org/{org}/fetch to refresh.
    """,
)
def get_organization(
    org: str,
    db: Session = Depends(get_db),
):
    """Get organization overview."""
    service = GitHubAnalyticsService(db)
    result = service.get_organization(org)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Organization '{org}' not found. Use POST /github/org/{org}/fetch to fetch data.",
        )

    return result


@router.post(
    "/org/{org}/fetch",
    response_model=OrganizationResponse,
    summary="Fetch organization data",
    description="""
    Fetch fresh data for a GitHub organization from the GitHub API.

    This will update all metrics including repositories, stars, and velocity score.
    Requires GITHUB_TOKEN environment variable for authenticated requests.
    """,
)
async def fetch_organization(
    org: str,
    db: Session = Depends(get_db),
):
    """Fetch organization data from GitHub API."""
    service = GitHubAnalyticsService(db)
    result = await service.fetch_organization(org)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Organization '{org}' not found on GitHub"
        )

    return result


@router.get(
    "/org/{org}/repos",
    response_model=ReposResponse,
    summary="Get organization repositories",
    description="Get list of repositories for an organization with metrics.",
)
def get_org_repos(
    org: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort: str = Query("stars", description="Sort by: stars, forks, updated, name"),
    db: Session = Depends(get_db),
):
    """Get repositories for an organization."""
    service = GitHubAnalyticsService(db)
    return service.get_org_repos(org, limit=limit, offset=offset, sort_by=sort)


@router.get(
    "/org/{org}/activity",
    response_model=ActivityResponse,
    summary="Get activity trends",
    description="""
    Get weekly activity trends for an organization.

    Analyzes commit activity from top repositories.
    """,
)
async def get_org_activity(
    org: str,
    weeks: int = Query(12, ge=4, le=52),
    db: Session = Depends(get_db),
):
    """Get activity trends for an organization."""
    service = GitHubAnalyticsService(db)
    return await service.get_org_activity(org, weeks=weeks)


@router.get(
    "/org/{org}/contributors",
    response_model=ContributorsResponse,
    summary="Get top contributors",
    description="Get top contributors for an organization.",
)
def get_org_contributors(
    org: str,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get contributors for an organization."""
    service = GitHubAnalyticsService(db)
    return service.get_org_contributors(org, limit=limit)


@router.get(
    "/org/{org}/score",
    response_model=VelocityResponse,
    summary="Get velocity score",
    description="""
    Get developer velocity score for an organization.

    Score (0-100) based on:
    - Commit frequency (30%)
    - PR velocity (25%)
    - Issue resolution (20%)
    - Contributor growth (15%)
    - Release cadence (10%)
    """,
)
def get_velocity_score(
    org: str,
    db: Session = Depends(get_db),
):
    """Get velocity score for an organization."""
    service = GitHubAnalyticsService(db)
    result = service.get_velocity_breakdown(org)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Organization '{org}' not found. Fetch data first."
        )

    return result


@router.get(
    "/repo/{owner}/{repo}",
    response_model=RepositoryDetail,
    summary="Get repository details",
    description="Get detailed information for a specific repository.",
)
def get_repo_details(
    owner: str,
    repo: str,
    db: Session = Depends(get_db),
):
    """Get repository details."""
    service = GitHubAnalyticsService(db)
    result = service.get_repo_details(owner, repo)

    if not result:
        raise HTTPException(status_code=404, detail="Repository not found")

    return result


@router.get(
    "/search",
    summary="Search repositories",
    description="Search repositories by name or description.",
)
def search_repos(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Search repositories."""
    service = GitHubAnalyticsService(db)
    return service.search_repos(q, limit=limit)


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Get aggregate statistics",
    description="Get aggregate statistics for all tracked organizations and repositories.",
)
def get_stats(
    db: Session = Depends(get_db),
):
    """Get aggregate statistics."""
    service = GitHubAnalyticsService(db)
    return service.get_stats()
