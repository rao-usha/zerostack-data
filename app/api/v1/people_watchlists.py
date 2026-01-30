"""
People Watchlists API endpoints.

Provides endpoints for tracking specific executives:
- Create watchlists to monitor key people
- Get alerts when tracked people change roles
- Track executive career movements
"""

from typing import Optional, List
from datetime import date, datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import (
    PeopleWatchlist,
    PeopleWatchlistPerson,
    Person,
    CompanyPerson,
    IndustrialCompany,
    LeadershipChange,
)

router = APIRouter(prefix="/people-watchlists", tags=["People Watchlists"])


# =============================================================================
# Request/Response Models
# =============================================================================

class WatchlistCreate(BaseModel):
    """Request to create a watchlist."""
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None


class WatchlistUpdate(BaseModel):
    """Request to update a watchlist."""
    name: Optional[str] = None
    description: Optional[str] = None


class WatchlistPersonAdd(BaseModel):
    """Request to add a person to watchlist."""
    person_id: int
    notes: Optional[str] = None
    alert_on_change: bool = Field(True, description="Receive alerts when this person changes roles")


class WatchlistSummary(BaseModel):
    """Summary of a watchlist."""
    id: int
    name: str
    description: Optional[str] = None
    person_count: int = 0
    recent_changes: int = 0
    created_at: date

    class Config:
        from_attributes = True


class WatchlistPersonItem(BaseModel):
    """A person on a watchlist."""
    person_id: int
    full_name: str
    current_title: Optional[str] = None
    current_company: Optional[str] = None
    current_company_id: Optional[int] = None
    linkedin_url: Optional[str] = None
    photo_url: Optional[str] = None
    notes: Optional[str] = None
    alert_on_change: bool = True
    added_at: datetime
    last_change_date: Optional[date] = None

    class Config:
        from_attributes = True


class WatchlistDetail(BaseModel):
    """Detailed watchlist information."""
    id: int
    name: str
    description: Optional[str] = None
    people: List[WatchlistPersonItem] = []
    created_at: date

    class Config:
        from_attributes = True


class PersonChangeAlert(BaseModel):
    """An alert for a tracked person."""
    person_id: int
    person_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    old_company: Optional[str] = None
    new_company: Optional[str] = None
    announced_date: Optional[date] = None
    watchlist_id: int
    watchlist_name: str

    class Config:
        from_attributes = True


class WatchlistAlertsResponse(BaseModel):
    """Alerts for a watchlist."""
    watchlist_id: int
    watchlist_name: str
    alerts: List[PersonChangeAlert]
    total_alerts: int


# =============================================================================
# Endpoints
# =============================================================================

@router.get("", response_model=List[WatchlistSummary])
async def list_watchlists(
    db: Session = Depends(get_db),
):
    """
    List all people watchlists.

    Watchlists help you track specific executives across companies.
    """
    watchlists = db.query(PeopleWatchlist).order_by(PeopleWatchlist.name).all()

    results = []
    thirty_days_ago = date.today() - timedelta(days=30)

    for wl in watchlists:
        # Count people
        person_count = db.query(PeopleWatchlistPerson).filter(
            PeopleWatchlistPerson.watchlist_id == wl.id
        ).count()

        # Count recent changes for watched people
        person_ids = [wp.person_id for wp in db.query(PeopleWatchlistPerson).filter(
            PeopleWatchlistPerson.watchlist_id == wl.id
        ).all()]

        recent_changes = 0
        if person_ids:
            # Get person names
            people = db.query(Person).filter(Person.id.in_(person_ids)).all()
            person_names = [p.full_name for p in people]

            if person_names:
                recent_changes = db.query(LeadershipChange).filter(
                    LeadershipChange.person_name.in_(person_names),
                    LeadershipChange.announced_date >= thirty_days_ago,
                ).count()

        results.append(WatchlistSummary(
            id=wl.id,
            name=wl.name,
            description=wl.description,
            person_count=person_count,
            recent_changes=recent_changes,
            created_at=wl.created_at.date() if wl.created_at else date.today(),
        ))

    return results


@router.post("", response_model=WatchlistSummary)
async def create_watchlist(
    request: WatchlistCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new people watchlist.

    Use watchlists to track key executives and get notified of changes.
    """
    watchlist = PeopleWatchlist(
        name=request.name,
        description=request.description,
    )
    db.add(watchlist)
    db.commit()
    db.refresh(watchlist)

    return WatchlistSummary(
        id=watchlist.id,
        name=watchlist.name,
        description=watchlist.description,
        person_count=0,
        recent_changes=0,
        created_at=watchlist.created_at.date() if watchlist.created_at else date.today(),
    )


@router.get("/{watchlist_id}", response_model=WatchlistDetail)
async def get_watchlist(
    watchlist_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed watchlist information.

    Includes all tracked people with their current roles.
    """
    watchlist = db.query(PeopleWatchlist).filter(
        PeopleWatchlist.id == watchlist_id
    ).first()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    # Get watched people
    watched = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist_id
    ).all()

    people = []
    for wp in watched:
        person = db.query(Person).get(wp.person_id)
        if not person:
            continue

        # Get current role
        current_role = db.query(CompanyPerson).filter(
            CompanyPerson.person_id == person.id,
            CompanyPerson.is_current == True,
        ).first()

        current_title = None
        current_company = None
        current_company_id = None
        if current_role:
            current_title = current_role.title
            company = db.query(IndustrialCompany).get(current_role.company_id)
            if company:
                current_company = company.name
                current_company_id = company.id

        # Get last change date
        last_change = db.query(LeadershipChange).filter(
            LeadershipChange.person_name == person.full_name
        ).order_by(LeadershipChange.announced_date.desc()).first()

        people.append(WatchlistPersonItem(
            person_id=person.id,
            full_name=person.full_name,
            current_title=current_title,
            current_company=current_company,
            current_company_id=current_company_id,
            linkedin_url=person.linkedin_url,
            photo_url=person.photo_url,
            notes=wp.notes,
            alert_on_change=wp.alert_on_change,
            added_at=wp.added_at,
            last_change_date=last_change.announced_date if last_change else None,
        ))

    return WatchlistDetail(
        id=watchlist.id,
        name=watchlist.name,
        description=watchlist.description,
        people=people,
        created_at=watchlist.created_at.date() if watchlist.created_at else date.today(),
    )


@router.put("/{watchlist_id}", response_model=WatchlistSummary)
async def update_watchlist(
    watchlist_id: int,
    request: WatchlistUpdate,
    db: Session = Depends(get_db),
):
    """Update watchlist name or description."""
    watchlist = db.query(PeopleWatchlist).filter(
        PeopleWatchlist.id == watchlist_id
    ).first()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    if request.name:
        watchlist.name = request.name
    if request.description is not None:
        watchlist.description = request.description

    db.commit()
    db.refresh(watchlist)

    person_count = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist.id
    ).count()

    return WatchlistSummary(
        id=watchlist.id,
        name=watchlist.name,
        description=watchlist.description,
        person_count=person_count,
        recent_changes=0,
        created_at=watchlist.created_at.date() if watchlist.created_at else date.today(),
    )


@router.delete("/{watchlist_id}")
async def delete_watchlist(
    watchlist_id: int,
    db: Session = Depends(get_db),
):
    """Delete a watchlist and remove all tracked people."""
    watchlist = db.query(PeopleWatchlist).filter(
        PeopleWatchlist.id == watchlist_id
    ).first()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    # Delete watched people
    db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist_id
    ).delete()

    db.delete(watchlist)
    db.commit()

    return {"status": "deleted", "watchlist_id": watchlist_id}


@router.post("/{watchlist_id}/people", response_model=WatchlistPersonItem)
async def add_person_to_watchlist(
    watchlist_id: int,
    request: WatchlistPersonAdd,
    db: Session = Depends(get_db),
):
    """Add a person to a watchlist."""
    watchlist = db.query(PeopleWatchlist).filter(
        PeopleWatchlist.id == watchlist_id
    ).first()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    person = db.query(Person).filter(
        Person.id == request.person_id
    ).first()

    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    # Check if already watched
    existing = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist_id,
        PeopleWatchlistPerson.person_id == request.person_id,
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="Person already on watchlist")

    wp = PeopleWatchlistPerson(
        watchlist_id=watchlist_id,
        person_id=request.person_id,
        notes=request.notes,
        alert_on_change=request.alert_on_change,
    )
    db.add(wp)
    db.commit()
    db.refresh(wp)

    # Get current role
    current_role = db.query(CompanyPerson).filter(
        CompanyPerson.person_id == person.id,
        CompanyPerson.is_current == True,
    ).first()

    current_title = None
    current_company = None
    current_company_id = None
    if current_role:
        current_title = current_role.title
        company = db.query(IndustrialCompany).get(current_role.company_id)
        if company:
            current_company = company.name
            current_company_id = company.id

    return WatchlistPersonItem(
        person_id=person.id,
        full_name=person.full_name,
        current_title=current_title,
        current_company=current_company,
        current_company_id=current_company_id,
        linkedin_url=person.linkedin_url,
        photo_url=person.photo_url,
        notes=wp.notes,
        alert_on_change=wp.alert_on_change,
        added_at=wp.added_at,
        last_change_date=None,
    )


@router.delete("/{watchlist_id}/people/{person_id}")
async def remove_person_from_watchlist(
    watchlist_id: int,
    person_id: int,
    db: Session = Depends(get_db),
):
    """Remove a person from a watchlist."""
    wp = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist_id,
        PeopleWatchlistPerson.person_id == person_id,
    ).first()

    if not wp:
        raise HTTPException(status_code=404, detail="Person not on watchlist")

    db.delete(wp)
    db.commit()

    return {"status": "removed", "watchlist_id": watchlist_id, "person_id": person_id}


@router.get("/{watchlist_id}/alerts", response_model=WatchlistAlertsResponse)
async def get_watchlist_alerts(
    watchlist_id: int,
    days: int = Query(30, ge=1, le=365, description="Days to look back"),
    db: Session = Depends(get_db),
):
    """
    Get alerts for tracked people who have changed roles.

    Shows recent leadership changes for people on your watchlist.
    """
    watchlist = db.query(PeopleWatchlist).filter(
        PeopleWatchlist.id == watchlist_id
    ).first()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    # Get watched people with alerts enabled
    watched = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.watchlist_id == watchlist_id,
        PeopleWatchlistPerson.alert_on_change == True,
    ).all()

    if not watched:
        return WatchlistAlertsResponse(
            watchlist_id=watchlist.id,
            watchlist_name=watchlist.name,
            alerts=[],
            total_alerts=0,
        )

    # Get person names
    person_ids = [wp.person_id for wp in watched]
    people = db.query(Person).filter(Person.id.in_(person_ids)).all()
    person_map = {p.id: p for p in people}
    person_names = [p.full_name for p in people]

    # Find changes
    cutoff_date = date.today() - timedelta(days=days)
    changes = db.query(LeadershipChange).filter(
        LeadershipChange.person_name.in_(person_names),
        LeadershipChange.announced_date >= cutoff_date,
    ).order_by(LeadershipChange.announced_date.desc()).all()

    alerts = []
    for change in changes:
        # Find matching person
        person = next((p for p in people if p.full_name == change.person_name), None)

        # Get company names
        old_company = None
        new_company = None
        if change.old_company:
            old_company = change.old_company
        if change.company_id:
            company = db.query(IndustrialCompany).get(change.company_id)
            new_company = company.name if company else None

        alerts.append(PersonChangeAlert(
            person_id=person.id if person else 0,
            person_name=change.person_name,
            change_type=change.change_type,
            old_title=change.old_title,
            new_title=change.new_title,
            old_company=old_company,
            new_company=new_company,
            announced_date=change.announced_date,
            watchlist_id=watchlist.id,
            watchlist_name=watchlist.name,
        ))

    return WatchlistAlertsResponse(
        watchlist_id=watchlist.id,
        watchlist_name=watchlist.name,
        alerts=alerts,
        total_alerts=len(alerts),
    )


@router.get("/alerts/all", response_model=List[PersonChangeAlert])
async def get_all_alerts(
    days: int = Query(30, ge=1, le=365, description="Days to look back"),
    db: Session = Depends(get_db),
):
    """
    Get alerts across all watchlists.

    Aggregated view of all tracked people who have changed roles.
    """
    # Get all watched people with alerts enabled
    watched = db.query(PeopleWatchlistPerson).filter(
        PeopleWatchlistPerson.alert_on_change == True,
    ).all()

    if not watched:
        return []

    # Get person names and watchlist info
    person_ids = list(set([wp.person_id for wp in watched]))
    people = db.query(Person).filter(Person.id.in_(person_ids)).all()
    person_names = [p.full_name for p in people]

    # Map person to watchlist
    person_watchlist_map = {}
    for wp in watched:
        if wp.person_id not in person_watchlist_map:
            person_watchlist_map[wp.person_id] = []
        watchlist = db.query(PeopleWatchlist).get(wp.watchlist_id)
        if watchlist:
            person_watchlist_map[wp.person_id].append(watchlist)

    # Find changes
    cutoff_date = date.today() - timedelta(days=days)
    changes = db.query(LeadershipChange).filter(
        LeadershipChange.person_name.in_(person_names),
        LeadershipChange.announced_date >= cutoff_date,
    ).order_by(LeadershipChange.announced_date.desc()).all()

    alerts = []
    for change in changes:
        person = next((p for p in people if p.full_name == change.person_name), None)
        if not person:
            continue

        watchlists = person_watchlist_map.get(person.id, [])
        if not watchlists:
            continue

        # Create alert for first watchlist (could expand to multiple)
        wl = watchlists[0]

        alerts.append(PersonChangeAlert(
            person_id=person.id,
            person_name=change.person_name,
            change_type=change.change_type,
            old_title=change.old_title,
            new_title=change.new_title,
            old_company=change.old_company,
            new_company=None,
            announced_date=change.announced_date,
            watchlist_id=wl.id,
            watchlist_name=wl.name,
        ))

    return alerts
