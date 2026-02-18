"""
Peer Sets API endpoints.

Provides endpoints for managing peer sets for leadership benchmarking:
- Create peer groups of similar companies
- Compare leadership structures across peers
- Benchmark team sizes, turnover, and composition
"""

from typing import Optional, List
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.people_models import (
    PeoplePeerSet,
    PeoplePeerSetMember,
    IndustrialCompany,
    CompanyPerson,
    LeadershipChange,
)

router = APIRouter(prefix="/peer-sets", tags=["Peer Sets & Benchmarking"])


# =============================================================================
# Request/Response Models
# =============================================================================


class PeerSetCreate(BaseModel):
    """Request to create a peer set."""

    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    industry: Optional[str] = None
    criteria: Optional[dict] = Field(
        None, description="Selection criteria (revenue range, employee count, etc.)"
    )


class PeerSetUpdate(BaseModel):
    """Request to update a peer set."""

    name: Optional[str] = None
    description: Optional[str] = None
    criteria: Optional[dict] = None


class PeerSetMemberAdd(BaseModel):
    """Request to add a company to peer set."""

    company_id: int
    is_primary: bool = Field(
        False, description="Is this the primary company being compared"
    )


class PeerSetSummary(BaseModel):
    """Summary of a peer set."""

    id: int
    name: str
    description: Optional[str] = None
    industry: Optional[str] = None
    member_count: int = 0
    primary_company_id: Optional[int] = None
    primary_company_name: Optional[str] = None
    created_at: date

    class Config:
        from_attributes = True


class PeerMemberItem(BaseModel):
    """A member of a peer set."""

    company_id: int
    company_name: str
    industry: Optional[str] = None
    revenue: Optional[float] = None
    employee_count: Optional[int] = None
    is_primary: bool = False
    executive_count: int = 0
    c_suite_count: int = 0
    board_size: int = 0

    class Config:
        from_attributes = True


class PeerSetDetail(BaseModel):
    """Detailed peer set information."""

    id: int
    name: str
    description: Optional[str] = None
    industry: Optional[str] = None
    criteria: Optional[dict] = None
    members: List[PeerMemberItem] = []
    created_at: date

    class Config:
        from_attributes = True


class BenchmarkMetric(BaseModel):
    """A single benchmark metric for a company."""

    company_id: int
    company_name: str
    is_primary: bool = False
    value: float
    rank: int
    percentile: float


class BenchmarkResult(BaseModel):
    """Benchmark comparison result."""

    metric_name: str
    description: str
    metrics: List[BenchmarkMetric]
    peer_set_average: float
    peer_set_median: float


class BenchmarkResponse(BaseModel):
    """Full benchmark comparison response."""

    peer_set_id: int
    peer_set_name: str
    primary_company: Optional[str] = None
    benchmarks: List[BenchmarkResult]


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=List[PeerSetSummary])
async def list_peer_sets(
    industry: Optional[str] = Query(None, description="Filter by industry"),
    db: Session = Depends(get_db),
):
    """
    List all peer sets.

    Peer sets group similar companies for leadership benchmarking.
    """
    query = db.query(PeoplePeerSet)

    if industry:
        query = query.filter(PeoplePeerSet.industry == industry)

    peer_sets = query.order_by(PeoplePeerSet.name).all()

    results = []
    for ps in peer_sets:
        # Count members
        member_count = (
            db.query(PeoplePeerSetMember)
            .filter(PeoplePeerSetMember.peer_set_id == ps.id)
            .count()
        )

        # Find primary company
        primary = (
            db.query(PeoplePeerSetMember)
            .filter(
                PeoplePeerSetMember.peer_set_id == ps.id,
                PeoplePeerSetMember.is_primary == True,
            )
            .first()
        )

        primary_name = None
        primary_id = None
        if primary:
            company = db.get(IndustrialCompany, primary.company_id)
            if company:
                primary_name = company.name
                primary_id = company.id

        results.append(
            PeerSetSummary(
                id=ps.id,
                name=ps.name,
                description=ps.description,
                industry=ps.industry,
                member_count=member_count,
                primary_company_id=primary_id,
                primary_company_name=primary_name,
                created_at=ps.created_at.date() if ps.created_at else date.today(),
            )
        )

    return results


@router.post("", response_model=PeerSetSummary)
async def create_peer_set(
    request: PeerSetCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new peer set.

    Use peer sets to benchmark leadership against similar companies.
    """
    peer_set = PeoplePeerSet(
        name=request.name,
        description=request.description,
        industry=request.industry,
        criteria=request.criteria,
    )
    db.add(peer_set)
    db.commit()
    db.refresh(peer_set)

    return PeerSetSummary(
        id=peer_set.id,
        name=peer_set.name,
        description=peer_set.description,
        industry=peer_set.industry,
        member_count=0,
        primary_company_id=None,
        primary_company_name=None,
        created_at=peer_set.created_at.date() if peer_set.created_at else date.today(),
    )


@router.get("/{peer_set_id}", response_model=PeerSetDetail)
async def get_peer_set(
    peer_set_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed peer set information.

    Includes all member companies with leadership stats.
    """
    peer_set = db.query(PeoplePeerSet).filter(PeoplePeerSet.id == peer_set_id).first()

    if not peer_set:
        raise HTTPException(status_code=404, detail="Peer set not found")

    # Get members
    members_db = (
        db.query(PeoplePeerSetMember)
        .filter(PeoplePeerSetMember.peer_set_id == peer_set_id)
        .all()
    )

    members = []
    for member in members_db:
        company = db.get(IndustrialCompany, member.company_id)
        if not company:
            continue

        # Get leadership stats
        leadership = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company.id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        exec_count = len(leadership)
        c_suite_count = len([cp for cp in leadership if cp.title_level == "c_suite"])
        board_size = len([cp for cp in leadership if cp.is_board_member])

        members.append(
            PeerMemberItem(
                company_id=company.id,
                company_name=company.name,
                industry=company.industry_segment,
                revenue=company.revenue_usd,
                employee_count=company.employee_count,
                is_primary=member.is_primary,
                executive_count=exec_count,
                c_suite_count=c_suite_count,
                board_size=board_size,
            )
        )

    return PeerSetDetail(
        id=peer_set.id,
        name=peer_set.name,
        description=peer_set.description,
        industry=peer_set.industry,
        criteria=peer_set.criteria,
        members=members,
        created_at=peer_set.created_at.date() if peer_set.created_at else date.today(),
    )


@router.delete("/{peer_set_id}")
async def delete_peer_set(
    peer_set_id: int,
    db: Session = Depends(get_db),
):
    """Delete a peer set and its members."""
    peer_set = db.query(PeoplePeerSet).filter(PeoplePeerSet.id == peer_set_id).first()

    if not peer_set:
        raise HTTPException(status_code=404, detail="Peer set not found")

    # Delete members
    db.query(PeoplePeerSetMember).filter(
        PeoplePeerSetMember.peer_set_id == peer_set_id
    ).delete()

    db.delete(peer_set)
    db.commit()

    return {"status": "deleted", "peer_set_id": peer_set_id}


@router.post("/{peer_set_id}/members", response_model=PeerMemberItem)
async def add_peer_member(
    peer_set_id: int,
    request: PeerSetMemberAdd,
    db: Session = Depends(get_db),
):
    """Add a company to a peer set."""
    peer_set = db.query(PeoplePeerSet).filter(PeoplePeerSet.id == peer_set_id).first()

    if not peer_set:
        raise HTTPException(status_code=404, detail="Peer set not found")

    company = (
        db.query(IndustrialCompany)
        .filter(IndustrialCompany.id == request.company_id)
        .first()
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Check if already in peer set
    existing = (
        db.query(PeoplePeerSetMember)
        .filter(
            PeoplePeerSetMember.peer_set_id == peer_set_id,
            PeoplePeerSetMember.company_id == request.company_id,
        )
        .first()
    )

    if existing:
        raise HTTPException(status_code=400, detail="Company already in peer set")

    # If setting as primary, unset existing primary
    if request.is_primary:
        db.query(PeoplePeerSetMember).filter(
            PeoplePeerSetMember.peer_set_id == peer_set_id,
            PeoplePeerSetMember.is_primary == True,
        ).update({"is_primary": False})

    member = PeoplePeerSetMember(
        peer_set_id=peer_set_id,
        company_id=request.company_id,
        is_primary=request.is_primary,
    )
    db.add(member)
    db.commit()

    # Get stats
    leadership = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == company.id,
            CompanyPerson.is_current == True,
        )
        .all()
    )

    return PeerMemberItem(
        company_id=company.id,
        company_name=company.name,
        industry=company.industry,
        revenue=company.revenue,
        employee_count=company.employee_count,
        is_primary=request.is_primary,
        executive_count=len(leadership),
        c_suite_count=len([cp for cp in leadership if cp.title_level == "c_suite"]),
        board_size=len([cp for cp in leadership if cp.is_board_member]),
    )


@router.delete("/{peer_set_id}/members/{company_id}")
async def remove_peer_member(
    peer_set_id: int,
    company_id: int,
    db: Session = Depends(get_db),
):
    """Remove a company from a peer set."""
    member = (
        db.query(PeoplePeerSetMember)
        .filter(
            PeoplePeerSetMember.peer_set_id == peer_set_id,
            PeoplePeerSetMember.company_id == company_id,
        )
        .first()
    )

    if not member:
        raise HTTPException(status_code=404, detail="Company not in peer set")

    db.delete(member)
    db.commit()

    return {"status": "removed", "peer_set_id": peer_set_id, "company_id": company_id}


@router.get("/{peer_set_id}/benchmark", response_model=BenchmarkResponse)
async def benchmark_peer_set(
    peer_set_id: int,
    db: Session = Depends(get_db),
):
    """
    Benchmark leadership across peer set companies.

    Compares executive team size, C-suite composition, board size, and turnover.
    """
    peer_set = db.query(PeoplePeerSet).filter(PeoplePeerSet.id == peer_set_id).first()

    if not peer_set:
        raise HTTPException(status_code=404, detail="Peer set not found")

    # Get members
    members = (
        db.query(PeoplePeerSetMember)
        .filter(PeoplePeerSetMember.peer_set_id == peer_set_id)
        .all()
    )

    if not members:
        raise HTTPException(status_code=400, detail="Peer set has no members")

    # Collect metrics for each company
    company_metrics = []
    primary_company = None

    for member in members:
        company = db.get(IndustrialCompany, member.company_id)
        if not company:
            continue

        leadership = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company.id,
                CompanyPerson.is_current == True,
            )
            .all()
        )

        # Count leadership changes in last year
        one_year_ago = date.today() - timedelta(days=365)
        turnover = (
            db.query(LeadershipChange)
            .filter(
                LeadershipChange.company_id == company.id,
                LeadershipChange.announced_date >= one_year_ago,
            )
            .count()
        )

        metrics = {
            "company_id": company.id,
            "company_name": company.name,
            "is_primary": member.is_primary,
            "executive_count": len(leadership),
            "c_suite_count": len(
                [cp for cp in leadership if cp.title_level == "c_suite"]
            ),
            "board_size": len([cp for cp in leadership if cp.is_board_member]),
            "vp_count": len(
                [cp for cp in leadership if cp.title_level in ["vp", "svp", "evp"]]
            ),
            "turnover_12m": turnover,
        }
        company_metrics.append(metrics)

        if member.is_primary:
            primary_company = company.name

    # Build benchmark results
    benchmarks = []

    metric_definitions = [
        (
            "executive_count",
            "Total Executives",
            "Total number of executives in leadership team",
        ),
        ("c_suite_count", "C-Suite Size", "Number of C-level executives"),
        ("board_size", "Board Size", "Number of board members"),
        (
            "vp_count",
            "VP-Level Executives",
            "Number of VP, SVP, and EVP level executives",
        ),
        (
            "turnover_12m",
            "Leadership Changes (12M)",
            "Number of leadership changes in last 12 months",
        ),
    ]

    for metric_key, metric_name, description in metric_definitions:
        values = [m[metric_key] for m in company_metrics]

        if not values:
            continue

        # Sort and rank
        sorted_companies = sorted(
            company_metrics, key=lambda x: x[metric_key], reverse=True
        )

        metrics = []
        for rank, m in enumerate(sorted_companies, 1):
            percentile = (
                (len(sorted_companies) - rank + 1) / len(sorted_companies)
            ) * 100
            metrics.append(
                BenchmarkMetric(
                    company_id=m["company_id"],
                    company_name=m["company_name"],
                    is_primary=m["is_primary"],
                    value=m[metric_key],
                    rank=rank,
                    percentile=round(percentile, 1),
                )
            )

        # Calculate stats
        avg = sum(values) / len(values) if values else 0
        sorted_vals = sorted(values)
        median = sorted_vals[len(sorted_vals) // 2] if sorted_vals else 0

        benchmarks.append(
            BenchmarkResult(
                metric_name=metric_name,
                description=description,
                metrics=metrics,
                peer_set_average=round(avg, 1),
                peer_set_median=median,
            )
        )

    return BenchmarkResponse(
        peer_set_id=peer_set.id,
        peer_set_name=peer_set.name,
        primary_company=primary_company,
        benchmarks=benchmarks,
    )
