"""
People/Leadership API endpoints.

Provides CRUD operations for people and leadership data:
- Search and list people
- Get person details with experience/education
- Leadership changes feed
"""

from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import (
    Person,
    CompanyPerson,
    PersonExperience,
    PersonEducation,
    LeadershipChange,
    IndustrialCompany,
)

router = APIRouter(prefix="/people", tags=["People & Leadership"])


# =============================================================================
# Response Models
# =============================================================================


class PersonSummary(BaseModel):
    """Summary of a person for list views."""

    id: int
    full_name: str
    current_title: Optional[str] = None
    current_company: Optional[str] = None
    current_company_id: Optional[int] = None
    linkedin_url: Optional[str] = None
    photo_url: Optional[str] = None
    is_board_member: bool = False
    is_executive: bool = True

    class Config:
        from_attributes = True


class ExperienceItem(BaseModel):
    """Work experience entry."""

    id: int
    company_name: str
    title: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    is_current: bool = False
    description: Optional[str] = None

    class Config:
        from_attributes = True


class EducationItem(BaseModel):
    """Education entry."""

    id: int
    institution: str
    degree: Optional[str] = None
    field_of_study: Optional[str] = None
    graduation_year: Optional[int] = None

    class Config:
        from_attributes = True


class CurrentRole(BaseModel):
    """Current role at a company."""

    company_id: int
    company_name: str
    title: str
    title_normalized: Optional[str] = None
    title_level: Optional[str] = None
    department: Optional[str] = None
    is_board_member: bool = False
    is_board_chair: bool = False
    start_date: Optional[date] = None

    class Config:
        from_attributes = True


class PersonDetail(BaseModel):
    """Full person details."""

    id: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    photo_url: Optional[str] = None
    bio: Optional[str] = None
    current_roles: List[CurrentRole] = []
    experience: List[ExperienceItem] = []
    education: List[EducationItem] = []
    data_sources: List[str] = []
    last_verified_date: Optional[date] = None

    class Config:
        from_attributes = True


class LeadershipChangeItem(BaseModel):
    """Leadership change entry."""

    id: int
    person_name: str
    person_id: Optional[int] = None
    company_id: int
    company_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    old_company: Optional[str] = None
    announced_date: Optional[date] = None
    effective_date: Optional[date] = None
    reason: Optional[str] = None
    is_c_suite: bool = False
    is_board: bool = False
    significance_score: int = 5
    source_type: str
    source_url: Optional[str] = None

    class Config:
        from_attributes = True


class PeopleListResponse(BaseModel):
    """Paginated list of people."""

    items: List[PersonSummary]
    total: int
    page: int
    page_size: int
    pages: int


class ChangesListResponse(BaseModel):
    """Paginated list of leadership changes."""

    items: List[LeadershipChangeItem]
    total: int
    page: int
    page_size: int


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=PeopleListResponse)
async def list_people(
    search: Optional[str] = Query(None, description="Search by name"),
    company_id: Optional[int] = Query(None, description="Filter by company"),
    title_level: Optional[str] = Query(
        None, description="Filter by title level (c_suite, vp, director, etc.)"
    ),
    is_board_member: Optional[bool] = Query(None, description="Filter board members"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    List people with optional filters.

    Supports searching by name and filtering by company, title level, or board membership.
    """
    query = db.query(Person)

    # Apply search filter
    if search:
        search_term = f"%{search}%"
        query = query.filter(Person.full_name.ilike(search_term))

    # Filter by company
    if company_id:
        query = query.join(CompanyPerson).filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        )

    # Filter by title level
    if title_level:
        query = query.join(CompanyPerson).filter(
            CompanyPerson.title_level == title_level,
            CompanyPerson.is_current == True,
        )

    # Filter board members
    if is_board_member is not None:
        query = query.join(CompanyPerson).filter(
            CompanyPerson.is_board_member == is_board_member,
            CompanyPerson.is_current == True,
        )

    # Get total count (use subquery to avoid DISTINCT on JSON)
    from sqlalchemy import func

    subquery = query.with_entities(Person.id).distinct().subquery()
    total = db.query(func.count()).select_from(subquery).scalar()

    # Paginate - get distinct person IDs first, then fetch full records
    offset = (page - 1) * page_size
    person_ids = [
        p[0]
        for p in query.with_entities(Person.id)
        .distinct()
        .offset(offset)
        .limit(page_size)
        .all()
    ]
    people = (
        db.query(Person).filter(Person.id.in_(person_ids)).all() if person_ids else []
    )

    # Build response
    items = []
    for person in people:
        # Get current role
        current_role = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.person_id == person.id,
                CompanyPerson.is_current == True,
            )
            .first()
        )

        company_name = None
        company_id_val = None
        if current_role:
            company = db.get(IndustrialCompany, current_role.company_id)
            if company:
                company_name = company.name
                company_id_val = company.id

        items.append(
            PersonSummary(
                id=person.id,
                full_name=person.full_name,
                current_title=current_role.title if current_role else None,
                current_company=company_name,
                current_company_id=company_id_val,
                linkedin_url=person.linkedin_url,
                photo_url=person.photo_url,
                is_board_member=current_role.is_board_member if current_role else False,
                is_executive=not (
                    current_role.is_board_member if current_role else False
                ),
            )
        )

    pages = (total + page_size - 1) // page_size

    return PeopleListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.get("/{person_id}", response_model=PersonDetail)
async def get_person(
    person_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a person.

    Includes current roles, work experience, and education.
    """
    person = db.query(Person).filter(Person.id == person_id).first()

    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    # Get current roles
    current_roles = []
    company_persons = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.person_id == person_id,
            CompanyPerson.is_current == True,
        )
        .all()
    )

    for cp in company_persons:
        company = db.get(IndustrialCompany, cp.company_id)
        if company:
            current_roles.append(
                CurrentRole(
                    company_id=company.id,
                    company_name=company.name,
                    title=cp.title,
                    title_normalized=cp.title_normalized,
                    title_level=cp.title_level,
                    department=cp.department,
                    is_board_member=cp.is_board_member,
                    is_board_chair=cp.is_board_chair,
                    start_date=cp.start_date,
                )
            )

    # Get experience
    experience = []
    exp_records = (
        db.query(PersonExperience)
        .filter(PersonExperience.person_id == person_id)
        .order_by(PersonExperience.start_date.desc().nullslast())
        .all()
    )

    for exp in exp_records:
        experience.append(
            ExperienceItem(
                id=exp.id,
                company_name=exp.company_name,
                title=exp.title,
                start_date=exp.start_date,
                end_date=exp.end_date,
                is_current=exp.is_current,
                description=exp.description,
            )
        )

    # Get education
    education = []
    edu_records = (
        db.query(PersonEducation)
        .filter(PersonEducation.person_id == person_id)
        .order_by(PersonEducation.graduation_year.desc().nullslast())
        .all()
    )

    for edu in edu_records:
        education.append(
            EducationItem(
                id=edu.id,
                institution=edu.institution,
                degree=edu.degree,
                field_of_study=edu.field_of_study,
                graduation_year=edu.graduation_year,
            )
        )

    return PersonDetail(
        id=person.id,
        full_name=person.full_name,
        first_name=person.first_name,
        last_name=person.last_name,
        linkedin_url=person.linkedin_url,
        email=person.email,
        phone=person.phone,
        photo_url=person.photo_url,
        bio=person.bio,
        current_roles=current_roles,
        experience=experience,
        education=education,
        data_sources=person.data_sources or [],
        last_verified_date=person.last_verified_date,
    )


@router.get("/changes/feed", response_model=ChangesListResponse)
async def get_leadership_changes(
    company_id: Optional[int] = Query(None, description="Filter by company"),
    change_type: Optional[str] = Query(None, description="Filter by change type"),
    is_c_suite: Optional[bool] = Query(None, description="Filter C-suite changes"),
    min_significance: Optional[int] = Query(
        None, ge=1, le=10, description="Minimum significance score"
    ),
    since_date: Optional[date] = Query(None, description="Changes since date"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get leadership changes feed.

    Returns recent leadership changes across companies, with filtering options.
    """
    query = db.query(LeadershipChange)

    # Apply filters
    if company_id:
        query = query.filter(LeadershipChange.company_id == company_id)

    if change_type:
        query = query.filter(LeadershipChange.change_type == change_type)

    if is_c_suite is not None:
        query = query.filter(LeadershipChange.is_c_suite == is_c_suite)

    if min_significance:
        query = query.filter(LeadershipChange.significance_score >= min_significance)

    if since_date:
        query = query.filter(
            (LeadershipChange.announced_date >= since_date)
            | (LeadershipChange.effective_date >= since_date)
        )

    # Order by date (most recent first)
    query = query.order_by(
        LeadershipChange.announced_date.desc().nullslast(),
        LeadershipChange.created_at.desc(),
    )

    # Get total
    total = query.count()

    # Paginate
    offset = (page - 1) * page_size
    changes = query.offset(offset).limit(page_size).all()

    # Build response
    items = []
    for change in changes:
        company = db.get(IndustrialCompany, change.company_id)
        company_name = company.name if company else "Unknown"

        items.append(
            LeadershipChangeItem(
                id=change.id,
                person_name=change.person_name,
                person_id=change.person_id,
                company_id=change.company_id,
                company_name=company_name,
                change_type=change.change_type,
                old_title=change.old_title,
                new_title=change.new_title,
                old_company=change.old_company,
                announced_date=change.announced_date,
                effective_date=change.effective_date,
                reason=change.reason,
                is_c_suite=change.is_c_suite,
                is_board=change.is_board,
                significance_score=change.significance_score,
                source_type=change.source_type,
                source_url=change.source_url,
            )
        )

    return ChangesListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/search/executives", response_model=List[PersonSummary])
async def search_executives(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Quick search for executives by name.

    Returns top matches for autocomplete/search functionality.
    """
    search_term = f"%{q}%"

    # Search people with current executive roles
    results = (
        db.query(Person)
        .join(CompanyPerson)
        .filter(
            Person.full_name.ilike(search_term),
            CompanyPerson.is_current == True,
        )
        .distinct()
        .limit(limit)
        .all()
    )

    items = []
    for person in results:
        current_role = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.person_id == person.id,
                CompanyPerson.is_current == True,
            )
            .first()
        )

        company_name = None
        company_id = None
        if current_role:
            company = db.get(IndustrialCompany, current_role.company_id)
            if company:
                company_name = company.name
                company_id = company.id

        items.append(
            PersonSummary(
                id=person.id,
                full_name=person.full_name,
                current_title=current_role.title if current_role else None,
                current_company=company_name,
                current_company_id=company_id,
                linkedin_url=person.linkedin_url,
                photo_url=person.photo_url,
                is_board_member=current_role.is_board_member if current_role else False,
                is_executive=True,
            )
        )

    return items
