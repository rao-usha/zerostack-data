"""
Board Interlock API endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.database import get_db

router = APIRouter(prefix="/board-interlocks", tags=["Board Interlocks"])


@router.get("/companies")
async def list_companies_with_board_seats(
    db: Session = Depends(get_db),
):
    """List all companies that have board seats in the DB."""
    from app.core.people_models import BoardSeat, IndustrialCompany

    rows = (
        db.query(IndustrialCompany.id, IndustrialCompany.name)
        .join(BoardSeat, BoardSeat.company_id == IndustrialCompany.id)
        .filter(BoardSeat.is_current == True)
        .distinct()
        .order_by(IndustrialCompany.name)
        .all()
    )
    return [{"id": r[0], "name": r[1]} for r in rows]


@router.get("/person/{person_id}/seats")
async def get_person_board_seats(
    person_id: int,
    current_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    """All board seats held by a person."""
    from app.core.people_models import BoardSeat, Person
    person = db.query(Person).filter(Person.id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    q = db.query(BoardSeat).filter(BoardSeat.person_id == person_id)
    if current_only:
        q = q.filter(BoardSeat.is_current == True)
    seats = q.order_by(BoardSeat.start_date.desc().nullslast()).all()
    return {
        "person_id": person_id,
        "full_name": person.full_name,
        "board_seats": [
            {"company_name": s.company_name, "company_type": s.company_type,
             "role": s.role, "committee": s.committee, "is_chair": s.is_chair,
             "start_date": s.start_date.isoformat() if s.start_date else None,
             "end_date": s.end_date.isoformat() if s.end_date else None,
             "is_current": s.is_current}
            for s in seats
        ],
    }


@router.get("/person/{person_id}/co-directors")
async def get_co_directors(
    person_id: int,
    db: Session = Depends(get_db),
):
    """People who currently sit on any board with this person."""
    from app.core.people_models import BoardInterlock, Person
    from sqlalchemy import or_
    interlocks = (
        db.query(BoardInterlock)
        .filter(
            or_(BoardInterlock.person_id_a == person_id, BoardInterlock.person_id_b == person_id),
            BoardInterlock.is_current == True,
        )
        .all()
    )
    if not interlocks:
        return {"person_id": person_id, "co_directors": [], "shared_boards": 0}

    co_ids = {(il.person_id_b if il.person_id_a == person_id else il.person_id_a): il.shared_company
              for il in interlocks}
    people = {p.id: p for p in db.query(Person).filter(Person.id.in_(co_ids.keys())).all()}
    return {
        "person_id": person_id,
        "co_directors": [
            {"person_id": pid, "full_name": people[pid].full_name if pid in people else None,
             "shared_board": company}
            for pid, company in co_ids.items()
        ],
        "shared_boards": len(interlocks),
    }


@router.get("/company/{company_id}/network")
async def get_company_board_network(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Board network graph for a company — nodes (directors) and edges (co-directorships)."""
    from app.services.board_interlock_service import BoardInterlockService
    service = BoardInterlockService()
    return service.get_network_graph(company_id, db)


@router.post("/compute/{company_id}")
async def compute_interlocks(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Trigger interlock computation for a company's board members."""
    from app.core.people_models import IndustrialCompany
    from app.services.board_interlock_service import BoardInterlockService
    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    service = BoardInterlockService()
    count = service.compute_interlocks_for_company(company.name, db)
    return {"company_id": company_id, "interlocks_computed": count}


@router.post("/seed-from-company-people")
async def seed_board_seats_from_company_people(
    db: Session = Depends(get_db),
):
    """
    Seed board_seats from company_people.is_board_member = True.
    Safe to run multiple times — upserts on (person_id, company_id).
    """
    from app.core.people_models import CompanyPerson, BoardSeat, IndustrialCompany

    board_members = (
        db.query(CompanyPerson)
        .filter(CompanyPerson.is_board_member == True)
        .all()
    )

    # Build company_id → name map to avoid N+1 queries
    company_ids = {cp.company_id for cp in board_members if cp.company_id}
    companies_map = {
        c.id: c.name for c in
        db.query(IndustrialCompany).filter(IndustrialCompany.id.in_(company_ids)).all()
    }

    # Track which (person_id, company_name, is_current) combos we're inserting this run
    seen_keys = set()

    created = 0
    skipped = 0
    for cp in board_members:
        company_name = companies_map.get(cp.company_id, f"Company {cp.company_id}")
        is_current = cp.is_current if cp.is_current is not None else True
        dedup_key = (cp.person_id, company_name, is_current)

        # Skip duplicates within this batch
        if dedup_key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(dedup_key)

        # Skip if already in DB
        existing = (
            db.query(BoardSeat)
            .filter(
                BoardSeat.person_id == cp.person_id,
                BoardSeat.company_name == company_name,
                BoardSeat.is_current == is_current,
            )
            .first()
        )
        if existing:
            skipped += 1
            continue

        seat = BoardSeat(
            person_id=cp.person_id,
            company_id=cp.company_id,
            company_name=company_name,
            role=cp.title or "Director",
            is_current=is_current,
            source="company_people",
        )
        db.add(seat)
        created += 1

    db.commit()
    return {
        "seats_created": created,
        "seats_skipped": skipped,
        "total_board_members": len(board_members),
    }


@router.post("/compute-all")
async def compute_all_interlocks(
    db: Session = Depends(get_db),
):
    """
    Run BoardInterlockService for every company that has board seats.
    Returns summary of companies processed and interlocks created.
    """
    from app.core.people_models import BoardSeat
    from app.services.board_interlock_service import BoardInterlockService

    company_names = [
        r[0] for r in
        db.query(BoardSeat.company_name)
        .filter(BoardSeat.is_current == True, BoardSeat.company_name.isnot(None))
        .distinct()
        .all()
    ]

    if not company_names:
        return {"companies_processed": 0, "interlocks_created": 0, "message": "No board seats found — run seed-from-company-people first"}

    service = BoardInterlockService()
    total_interlocks = 0
    for name in company_names:
        count = service.compute_interlocks_for_company(name, db)
        total_interlocks += count

    return {
        "companies_processed": len(company_names),
        "interlocks_created": total_interlocks,
    }


@router.post("/collect/{company_id}")
async def collect_board_seats(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Run BoardAgent to collect board seat data for a company via SEC DEF 14A.
    Requires company to have a CIK set in industrial_companies.
    """
    from app.core.people_models import IndustrialCompany, BoardSeat, Person
    from app.sources.people_collection.board_agent import BoardAgent

    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    if not company.cik:
        raise HTTPException(status_code=400, detail="Company has no CIK — cannot fetch SEC proxy")

    agent = BoardAgent()
    try:
        results = await agent.collect_from_proxy(cik=company.cik, company_name=company.name)
    finally:
        await agent.close()

    created = 0
    for item in results:
        person_name = item.get("director_name") or item.get("person_name")
        if not person_name:
            continue
        person = db.query(Person).filter(Person.full_name.ilike(f"%{person_name}%")).first()
        person_id = person.id if person else None

        seat = db.query(BoardSeat).filter(
            BoardSeat.company_name == company.name,
            BoardSeat.person_id == person_id if person_id else False,
        ).first() if person_id else None

        if not seat:
            seat = BoardSeat(
                person_id=person_id,
                company_name=company.name,
                company_id=company_id,
                role=item.get("role"),
                committee=item.get("committee"),
                is_current=True,
                source="sec_proxy",
            )
            db.add(seat)
            created += 1

        for other in item.get("other_companies", []):
            other_name = other.get("company_name")
            if not other_name or not person_id:
                continue
            existing_other = db.query(BoardSeat).filter(
                BoardSeat.person_id == person_id,
                BoardSeat.company_name == other_name,
                BoardSeat.is_current == True,
            ).first()
            if not existing_other:
                db.add(BoardSeat(
                    person_id=person_id,
                    company_name=other_name,
                    company_type="public",
                    role=other.get("role", "Director"),
                    is_current=True,
                    source="sec_proxy",
                ))
                created += 1

    db.commit()
    return {"company_id": company_id, "seats_created": created, "directors_found": len(results)}
