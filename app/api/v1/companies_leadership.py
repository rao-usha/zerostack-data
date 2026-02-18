"""
Company Leadership API endpoints.

Provides endpoints for viewing and managing company leadership:
- Get company leadership team
- View leadership history/changes
- Compare leadership across companies (benchmarking)
"""

from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.people_models import (
    Person,
    CompanyPerson,
    IndustrialCompany,
    LeadershipChange,
    PeopleCollectionJob,
)

router = APIRouter(prefix="/companies", tags=["Company Leadership"])


# =============================================================================
# Response Models
# =============================================================================


class LeadershipMember(BaseModel):
    """A member of the leadership team."""

    person_id: int
    full_name: str
    title: str
    title_normalized: Optional[str] = None
    title_level: Optional[str] = None
    department: Optional[str] = None
    is_board_member: bool = False
    is_board_chair: bool = False
    is_executive: bool = True
    linkedin_url: Optional[str] = None
    photo_url: Optional[str] = None
    bio: Optional[str] = None
    start_date: Optional[date] = None
    reports_to_id: Optional[int] = None

    class Config:
        from_attributes = True


class CompanyLeadershipResponse(BaseModel):
    """Company leadership team."""

    company_id: int
    company_name: str
    executives: List[LeadershipMember] = []
    board_members: List[LeadershipMember] = []
    total_leadership_count: int = 0
    last_updated: Optional[date] = None


class LeadershipChangeItem(BaseModel):
    """Leadership change for a company."""

    id: int
    person_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    announced_date: Optional[date] = None
    effective_date: Optional[date] = None
    is_c_suite: bool = False
    is_board: bool = False
    source_type: str

    class Config:
        from_attributes = True


class LeadershipHistoryResponse(BaseModel):
    """Leadership change history for a company."""

    company_id: int
    company_name: str
    changes: List[LeadershipChangeItem] = []
    total_changes: int = 0


class LeadershipComparisonItem(BaseModel):
    """Leadership comparison for a single company."""

    company_id: int
    company_name: str
    ceo_name: Optional[str] = None
    ceo_tenure_years: Optional[float] = None
    cfo_name: Optional[str] = None
    executive_count: int = 0
    board_size: int = 0
    c_suite_count: int = 0
    recent_changes_count: int = 0


class LeadershipComparisonResponse(BaseModel):
    """Leadership comparison across companies."""

    companies: List[LeadershipComparisonItem]


class CollectionTriggerResponse(BaseModel):
    """Response when triggering data collection."""

    job_id: int
    status: str
    message: str


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/{company_id}/leadership", response_model=CompanyLeadershipResponse)
async def get_company_leadership(
    company_id: int,
    include_board: bool = Query(True, description="Include board members"),
    db: Session = Depends(get_db),
):
    """
    Get the current leadership team for a company.

    Returns executives and optionally board members with their details.
    """
    company = (
        db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Get current leadership
    query = (
        db.query(CompanyPerson, Person)
        .join(Person, CompanyPerson.person_id == Person.id)
        .filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        )
    )

    results = query.all()

    executives = []
    board_members = []

    for cp, person in results:
        member = LeadershipMember(
            person_id=person.id,
            full_name=person.full_name,
            title=cp.title,
            title_normalized=cp.title_normalized,
            title_level=cp.title_level,
            department=cp.department,
            is_board_member=cp.is_board_member,
            is_board_chair=cp.is_board_chair,
            is_executive=not cp.is_board_member
            or cp.title_level in ["c_suite", "president"],
            linkedin_url=person.linkedin_url,
            photo_url=person.photo_url,
            bio=person.bio[:200] if person.bio else None,
            start_date=cp.start_date,
            reports_to_id=cp.reports_to_id,
        )

        if cp.is_board_member and not member.is_executive:
            if include_board:
                board_members.append(member)
        else:
            executives.append(member)

    # Sort executives by title level
    level_order = {
        "c_suite": 0,
        "president": 1,
        "evp": 2,
        "svp": 3,
        "vp": 4,
        "director": 5,
    }
    executives.sort(key=lambda x: level_order.get(x.title_level or "", 99))

    return CompanyLeadershipResponse(
        company_id=company.id,
        company_name=company.name,
        executives=executives,
        board_members=board_members,
        total_leadership_count=len(executives) + len(board_members),
        last_updated=company.leadership_last_updated,
    )


@router.get(
    "/{company_id}/leadership/history", response_model=LeadershipHistoryResponse
)
async def get_leadership_history(
    company_id: int,
    since_date: Optional[date] = Query(None, description="Changes since date"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Get leadership change history for a company.

    Shows all recorded leadership changes ordered by date.
    """
    company = (
        db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    query = db.query(LeadershipChange).filter(LeadershipChange.company_id == company_id)

    if since_date:
        query = query.filter(
            (LeadershipChange.announced_date >= since_date)
            | (LeadershipChange.effective_date >= since_date)
        )

    query = query.order_by(
        LeadershipChange.announced_date.desc().nullslast(),
        LeadershipChange.created_at.desc(),
    )

    total = query.count()
    changes = query.limit(limit).all()

    items = [
        LeadershipChangeItem(
            id=c.id,
            person_name=c.person_name,
            change_type=c.change_type,
            old_title=c.old_title,
            new_title=c.new_title,
            announced_date=c.announced_date,
            effective_date=c.effective_date,
            is_c_suite=c.is_c_suite,
            is_board=c.is_board,
            source_type=c.source_type,
        )
        for c in changes
    ]

    return LeadershipHistoryResponse(
        company_id=company.id,
        company_name=company.name,
        changes=items,
        total_changes=total,
    )


@router.get(
    "/{company_id}/leadership/compare", response_model=LeadershipComparisonResponse
)
async def compare_leadership(
    company_id: int,
    compare_with: List[int] = Query(..., description="Company IDs to compare with"),
    db: Session = Depends(get_db),
):
    """
    Compare leadership structure across companies.

    Useful for benchmarking team size, C-suite composition, and turnover.
    """
    # Include the main company in comparison
    all_company_ids = [company_id] + compare_with

    companies = (
        db.query(IndustrialCompany)
        .filter(IndustrialCompany.id.in_(all_company_ids))
        .all()
    )

    if not companies:
        raise HTTPException(status_code=404, detail="No companies found")

    results = []
    for company in companies:
        # Get leadership stats
        leadership = (
            db.query(CompanyPerson, Person)
            .join(Person, CompanyPerson.person_id == Person.id)
            .filter(
                CompanyPerson.company_id == company.id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        ceo = None
        cfo = None
        executive_count = 0
        board_size = 0
        c_suite_count = 0

        for cp, person in leadership:
            if cp.is_board_member:
                board_size += 1

            if cp.title_level == "c_suite":
                c_suite_count += 1
                executive_count += 1

                title_lower = (cp.title or "").lower()
                if "ceo" in title_lower or "chief executive" in title_lower:
                    ceo = person.full_name
                elif "cfo" in title_lower or "chief financial" in title_lower:
                    cfo = person.full_name
            elif cp.title_level in ["president", "evp", "svp", "vp"]:
                executive_count += 1

        # Count recent changes (last 12 months)
        from datetime import timedelta

        one_year_ago = date.today() - timedelta(days=365)
        recent_changes = (
            db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id == company.id,
                LeadershipChange.announced_date >= one_year_ago,
            )
            .count()
        )

        results.append(
            LeadershipComparisonItem(
                company_id=company.id,
                company_name=company.name,
                ceo_name=ceo,
                cfo_name=cfo,
                executive_count=executive_count,
                board_size=board_size,
                c_suite_count=c_suite_count,
                recent_changes_count=recent_changes,
            )
        )

    return LeadershipComparisonResponse(companies=results)


@router.post(
    "/{company_id}/leadership/refresh", response_model=CollectionTriggerResponse
)
async def refresh_leadership(
    company_id: int,
    sources: List[str] = Query(["website"], description="Sources to collect from"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Trigger a refresh of leadership data for a company.

    Runs collection in the background and returns a job ID for tracking.
    """
    company = (
        db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Create collection job
    job = PeopleCollectionJob(
        job_type="refresh",
        company_id=company_id,
        config={"sources": sources},
        status="pending",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Note: Background task execution would be handled by a worker
    # For now, we just create the job record

    return CollectionTriggerResponse(
        job_id=job.id,
        status="pending",
        message=f"Collection job created for {company.name}. Sources: {', '.join(sources)}",
    )


@router.get("/leadership/stats")
async def get_leadership_stats(
    db: Session = Depends(get_db),
):
    """
    Get aggregate leadership statistics across all companies.

    Returns counts and trends for the platform.
    """
    # Total people
    total_people = db.query(Person).count()

    # Active leadership positions
    active_positions = (
        db.query(CompanyPerson).filter(CompanyPerson.is_current == True).count()
    )

    # Companies with leadership data
    companies_with_data = (
        db.query(func.count(func.distinct(CompanyPerson.company_id)))
        .filter(CompanyPerson.is_current == True)
        .scalar()
    )

    # C-suite count
    c_suite_count = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.is_current == True,
            CompanyPerson.title_level == "c_suite",
        )
        .count()
    )

    # Board members
    board_count = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.is_current == True,
            CompanyPerson.is_board_member == True,
        )
        .count()
    )

    # Recent changes (last 30 days)
    from datetime import timedelta

    thirty_days_ago = date.today() - timedelta(days=30)
    recent_changes = (
        db.query(LeadershipChange)
        .filter(
            LeadershipChange.announced_date >= thirty_days_ago,
        )
        .count()
    )

    # Changes by type
    change_counts = (
        db.query(LeadershipChange.change_type, func.count(LeadershipChange.id))
        .group_by(LeadershipChange.change_type)
        .all()
    )

    changes_by_type = {ct: count for ct, count in change_counts}

    return {
        "total_people": total_people,
        "active_positions": active_positions,
        "companies_with_leadership_data": companies_with_data,
        "c_suite_executives": c_suite_count,
        "board_members": board_count,
        "changes_last_30_days": recent_changes,
        "changes_by_type": changes_by_type,
    }
