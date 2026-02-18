"""
People Portfolios API endpoints.

Provides endpoints for managing PE portfolios for leadership tracking:
- Create and manage portfolios
- Add/remove companies from portfolios
- View portfolio leadership overview
- Track leadership changes across portfolio
"""

from typing import Optional, List
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import (
    PeoplePortfolio,
    PeoplePortfolioCompany,
    IndustrialCompany,
    CompanyPerson,
    LeadershipChange,
)

router = APIRouter(prefix="/people-portfolios", tags=["People Portfolios"])


# =============================================================================
# Request/Response Models
# =============================================================================


class PortfolioCreate(BaseModel):
    """Request to create a portfolio."""

    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    portfolio_type: str = Field(
        "pe_portfolio", description="pe_portfolio, watchlist, peer_group"
    )


class PortfolioUpdate(BaseModel):
    """Request to update a portfolio."""

    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None


class PortfolioCompanyAdd(BaseModel):
    """Request to add a company to portfolio."""

    company_id: int
    investment_date: Optional[date] = None
    exit_date: Optional[date] = None
    notes: Optional[str] = None


class PortfolioSummary(BaseModel):
    """Summary of a portfolio."""

    id: int
    name: str
    description: Optional[str] = None
    portfolio_type: str
    company_count: int = 0
    total_executives: int = 0
    recent_changes: int = 0
    created_at: date

    class Config:
        from_attributes = True


class PortfolioCompanyItem(BaseModel):
    """Company in a portfolio."""

    company_id: int
    company_name: str
    industry: Optional[str] = None
    website: Optional[str] = None
    investment_date: Optional[date] = None
    exit_date: Optional[date] = None
    is_active: bool = True
    executive_count: int = 0
    recent_changes: int = 0
    notes: Optional[str] = None

    class Config:
        from_attributes = True


class PortfolioDetail(BaseModel):
    """Detailed portfolio information."""

    id: int
    name: str
    description: Optional[str] = None
    portfolio_type: str
    companies: List[PortfolioCompanyItem] = []
    total_executives: int = 0
    total_board_members: int = 0
    c_suite_count: int = 0
    recent_changes_30d: int = 0
    created_at: date
    updated_at: date

    class Config:
        from_attributes = True


class LeadershipChangeItem(BaseModel):
    """Leadership change in portfolio context."""

    id: int
    company_id: int
    company_name: str
    person_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    announced_date: Optional[date] = None
    is_c_suite: bool = False
    is_board: bool = False

    class Config:
        from_attributes = True


class PortfolioChangesResponse(BaseModel):
    """Leadership changes across a portfolio."""

    portfolio_id: int
    portfolio_name: str
    changes: List[LeadershipChangeItem]
    total_changes: int


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=List[PortfolioSummary])
async def list_portfolios(
    portfolio_type: Optional[str] = Query(None, description="Filter by type"),
    db: Session = Depends(get_db),
):
    """
    List all portfolios.

    Returns summary information for each portfolio.
    """
    query = db.query(PeoplePortfolio)

    if portfolio_type:
        query = query.filter(PeoplePortfolio.portfolio_type == portfolio_type)

    portfolios = query.order_by(PeoplePortfolio.name).all()

    results = []
    for portfolio in portfolios:
        # Count companies
        company_count = (
            db.query(PeoplePortfolioCompany)
            .filter(
                PeoplePortfolioCompany.portfolio_id == portfolio.id,
                PeoplePortfolioCompany.is_active == True,
            )
            .count()
        )

        # Count executives across portfolio
        company_ids = [
            pc.company_id
            for pc in db.query(PeoplePortfolioCompany)
            .filter(
                PeoplePortfolioCompany.portfolio_id == portfolio.id,
                PeoplePortfolioCompany.is_active == True,
            )
            .all()
        ]

        exec_count = 0
        recent_changes = 0
        if company_ids:
            exec_count = (
                db.query(CompanyPerson)
                .filter(
                    CompanyPerson.company_id.in_(company_ids),
                    CompanyPerson.is_current == True,
                )
                .count()
            )

            # Recent changes (30 days)
            thirty_days_ago = date.today() - timedelta(days=30)
            recent_changes = (
                db.query(LeadershipChange)
                .filter(
                    LeadershipChange.company_id.in_(company_ids),
                    LeadershipChange.announced_date >= thirty_days_ago,
                )
                .count()
            )

        results.append(
            PortfolioSummary(
                id=portfolio.id,
                name=portfolio.name,
                description=portfolio.description,
                portfolio_type=portfolio.portfolio_type,
                company_count=company_count,
                total_executives=exec_count,
                recent_changes=recent_changes,
                created_at=portfolio.created_at.date()
                if portfolio.created_at
                else date.today(),
            )
        )

    return results


@router.post("", response_model=PortfolioSummary)
async def create_portfolio(
    request: PortfolioCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new portfolio.

    Portfolios can be PE portfolios, watchlists, or peer groups.
    """
    # Check for duplicate name
    existing = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.name == request.name).first()
    )

    if existing:
        raise HTTPException(
            status_code=400, detail="Portfolio with this name already exists"
        )

    portfolio = PeoplePortfolio(
        name=request.name,
        description=request.description,
        portfolio_type=request.portfolio_type,
    )
    db.add(portfolio)
    db.commit()
    db.refresh(portfolio)

    return PortfolioSummary(
        id=portfolio.id,
        name=portfolio.name,
        description=portfolio.description,
        portfolio_type=portfolio.portfolio_type,
        company_count=0,
        total_executives=0,
        recent_changes=0,
        created_at=portfolio.created_at.date()
        if portfolio.created_at
        else date.today(),
    )


@router.get("/{portfolio_id}", response_model=PortfolioDetail)
async def get_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed portfolio information.

    Includes all companies and leadership statistics.
    """
    portfolio = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.id == portfolio_id).first()
    )

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    # Get portfolio companies
    portfolio_companies = (
        db.query(PeoplePortfolioCompany)
        .filter(
            PeoplePortfolioCompany.portfolio_id == portfolio_id,
        )
        .all()
    )

    companies = []
    total_executives = 0
    total_board = 0
    c_suite_count = 0
    thirty_days_ago = date.today() - timedelta(days=30)

    for pc in portfolio_companies:
        company = db.get(IndustrialCompany, pc.company_id)
        if not company:
            continue

        # Count executives for this company
        exec_count = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company.id,
                CompanyPerson.is_current == True,
            )
            .count()
        )

        # Count recent changes
        recent = (
            db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id == company.id,
                LeadershipChange.announced_date >= thirty_days_ago,
            )
            .count()
        )

        companies.append(
            PortfolioCompanyItem(
                company_id=company.id,
                company_name=company.name,
                industry=company.industry_segment,
                website=company.website,
                investment_date=pc.investment_date,
                exit_date=pc.exit_date,
                is_active=pc.is_active,
                executive_count=exec_count,
                recent_changes=recent,
                notes=pc.notes,
            )
        )

        if pc.is_active:
            total_executives += exec_count

            # Count C-suite and board
            leadership = (
                db.query(CompanyPerson)
                .filter(
                    CompanyPerson.company_id == company.id,
                    CompanyPerson.is_current == True,
                )
                .all()
            )

            for cp in leadership:
                if cp.title_level == "c_suite":
                    c_suite_count += 1
                if cp.is_board_member:
                    total_board += 1

    # Count recent changes across portfolio
    active_company_ids = [c.company_id for c in companies if c.is_active]
    recent_changes_30d = 0
    if active_company_ids:
        recent_changes_30d = (
            db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id.in_(active_company_ids),
                LeadershipChange.announced_date >= thirty_days_ago,
            )
            .count()
        )

    return PortfolioDetail(
        id=portfolio.id,
        name=portfolio.name,
        description=portfolio.description,
        portfolio_type=portfolio.portfolio_type,
        companies=companies,
        total_executives=total_executives,
        total_board_members=total_board,
        c_suite_count=c_suite_count,
        recent_changes_30d=recent_changes_30d,
        created_at=portfolio.created_at.date()
        if portfolio.created_at
        else date.today(),
        updated_at=portfolio.updated_at.date()
        if portfolio.updated_at
        else date.today(),
    )


@router.put("/{portfolio_id}", response_model=PortfolioSummary)
async def update_portfolio(
    portfolio_id: int,
    request: PortfolioUpdate,
    db: Session = Depends(get_db),
):
    """Update portfolio name or description."""
    portfolio = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.id == portfolio_id).first()
    )

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    if request.name:
        portfolio.name = request.name
    if request.description is not None:
        portfolio.description = request.description

    db.commit()
    db.refresh(portfolio)

    # Get counts
    company_count = (
        db.query(PeoplePortfolioCompany)
        .filter(
            PeoplePortfolioCompany.portfolio_id == portfolio.id,
            PeoplePortfolioCompany.is_active == True,
        )
        .count()
    )

    return PortfolioSummary(
        id=portfolio.id,
        name=portfolio.name,
        description=portfolio.description,
        portfolio_type=portfolio.portfolio_type,
        company_count=company_count,
        total_executives=0,
        recent_changes=0,
        created_at=portfolio.created_at.date()
        if portfolio.created_at
        else date.today(),
    )


@router.delete("/{portfolio_id}")
async def delete_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
):
    """Delete a portfolio and its company associations."""
    portfolio = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.id == portfolio_id).first()
    )

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    # Delete company associations
    db.query(PeoplePortfolioCompany).filter(
        PeoplePortfolioCompany.portfolio_id == portfolio_id
    ).delete()

    # Delete portfolio
    db.delete(portfolio)
    db.commit()

    return {"status": "deleted", "portfolio_id": portfolio_id}


@router.post("/{portfolio_id}/companies", response_model=PortfolioCompanyItem)
async def add_company_to_portfolio(
    portfolio_id: int,
    request: PortfolioCompanyAdd,
    db: Session = Depends(get_db),
):
    """Add a company to a portfolio."""
    portfolio = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.id == portfolio_id).first()
    )

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    company = (
        db.query(IndustrialCompany)
        .filter(IndustrialCompany.id == request.company_id)
        .first()
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Check if already in portfolio
    existing = (
        db.query(PeoplePortfolioCompany)
        .filter(
            PeoplePortfolioCompany.portfolio_id == portfolio_id,
            PeoplePortfolioCompany.company_id == request.company_id,
        )
        .first()
    )

    if existing:
        raise HTTPException(status_code=400, detail="Company already in portfolio")

    pc = PeoplePortfolioCompany(
        portfolio_id=portfolio_id,
        company_id=request.company_id,
        investment_date=request.investment_date,
        exit_date=request.exit_date,
        is_active=request.exit_date is None,
        notes=request.notes,
    )
    db.add(pc)
    db.commit()

    # Get executive count
    exec_count = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == company.id,
            CompanyPerson.is_current == True,
        )
        .count()
    )

    return PortfolioCompanyItem(
        company_id=company.id,
        company_name=company.name,
        industry=company.industry_segment,
        website=company.website,
        investment_date=pc.investment_date,
        exit_date=pc.exit_date,
        is_active=pc.is_active,
        executive_count=exec_count,
        recent_changes=0,
        notes=pc.notes,
    )


@router.delete("/{portfolio_id}/companies/{company_id}")
async def remove_company_from_portfolio(
    portfolio_id: int,
    company_id: int,
    db: Session = Depends(get_db),
):
    """Remove a company from a portfolio."""
    pc = (
        db.query(PeoplePortfolioCompany)
        .filter(
            PeoplePortfolioCompany.portfolio_id == portfolio_id,
            PeoplePortfolioCompany.company_id == company_id,
        )
        .first()
    )

    if not pc:
        raise HTTPException(status_code=404, detail="Company not in portfolio")

    db.delete(pc)
    db.commit()

    return {"status": "removed", "portfolio_id": portfolio_id, "company_id": company_id}


@router.get("/{portfolio_id}/changes", response_model=PortfolioChangesResponse)
async def get_portfolio_changes(
    portfolio_id: int,
    days: int = Query(30, ge=1, le=365, description="Days to look back"),
    change_type: Optional[str] = Query(None, description="Filter by change type"),
    db: Session = Depends(get_db),
):
    """
    Get leadership changes across all portfolio companies.

    Great for monitoring portfolio company leadership stability.
    """
    portfolio = (
        db.query(PeoplePortfolio).filter(PeoplePortfolio.id == portfolio_id).first()
    )

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    # Get active company IDs
    company_ids = [
        pc.company_id
        for pc in db.query(PeoplePortfolioCompany)
        .filter(
            PeoplePortfolioCompany.portfolio_id == portfolio_id,
            PeoplePortfolioCompany.is_active == True,
        )
        .all()
    ]

    if not company_ids:
        return PortfolioChangesResponse(
            portfolio_id=portfolio.id,
            portfolio_name=portfolio.name,
            changes=[],
            total_changes=0,
        )

    # Query changes
    cutoff_date = date.today() - timedelta(days=days)
    query = db.query(LeadershipChange).filter(
        LeadershipChange.company_id.in_(company_ids),
        LeadershipChange.announced_date >= cutoff_date,
    )

    if change_type:
        query = query.filter(LeadershipChange.change_type == change_type)

    query = query.order_by(LeadershipChange.announced_date.desc())

    changes = query.all()

    items = []
    for change in changes:
        company = db.get(IndustrialCompany, change.company_id)
        company_name = company.name if company else "Unknown"

        items.append(
            LeadershipChangeItem(
                id=change.id,
                company_id=change.company_id,
                company_name=company_name,
                person_name=change.person_name,
                change_type=change.change_type,
                old_title=change.old_title,
                new_title=change.new_title,
                announced_date=change.announced_date,
                is_c_suite=change.is_c_suite,
                is_board=change.is_board,
            )
        )

    return PortfolioChangesResponse(
        portfolio_id=portfolio.id,
        portfolio_name=portfolio.name,
        changes=items,
        total_changes=len(items),
    )
