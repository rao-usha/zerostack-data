"""
PE Firm Org Snapshot builder.

Takes a point-in-time snapshot of a PE firm's org structure,
computes diff vs prior snapshot, and persists to pe_firm_org_snapshots.
"""
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def build_org_snapshot(db: Session, firm_id: int) -> dict:
    """
    Build and persist an org snapshot for a PE firm.

    Runs classification first, then captures the full org hierarchy and
    computes a diff against the most recent prior snapshot.
    Returns snapshot summary dict.
    """
    from app.core.pe_models import (
        PEFirmPeople,
        PEPerson,
        PEInvestmentCommittee,
        PEFirmOrgSnapshot,
    )
    from app.services.pe_org_classifier import classify_firm_people

    # Run classification first (updates role_type + IC records)
    classify_firm_people(db, firm_id)

    # Load current team ordered by seniority
    rows = (
        db.query(PEFirmPeople, PEPerson)
        .join(PEPerson, PEFirmPeople.person_id == PEPerson.id)
        .filter(PEFirmPeople.firm_id == firm_id, PEFirmPeople.is_current == True)  # noqa: E712
        .order_by(PEFirmPeople.seniority)
        .all()
    )

    # Load IC member IDs
    ic_member_ids = {
        row.person_id
        for row in db.query(PEInvestmentCommittee)
        .filter(
            PEInvestmentCommittee.firm_id == firm_id,
            PEInvestmentCommittee.is_current == True,  # noqa: E712
        )
        .all()
    }

    # Build org JSON hierarchy
    org_json: dict = {
        "investment_committee": [],
        "operating_partners": [],
        "investment_team": [],
        "other": [],
    }

    for firm_person, person in rows:
        entry = {
            "person_id": person.id,
            "name": person.full_name,
            "title": firm_person.title or person.current_title,
            "seniority": firm_person.seniority,
            "role_type": firm_person.role_type,
            "department": firm_person.department,
            "sector_focus": firm_person.sector_focus,
        }
        if person.id in ic_member_ids:
            org_json["investment_committee"].append(entry)
        elif firm_person.role_type == "operating_partner":
            org_json["operating_partners"].append(entry)
        elif firm_person.role_type == "investment_team":
            org_json["investment_team"].append(entry)
        else:
            org_json["other"].append(entry)

    # Load prior snapshot for diff
    prior = (
        db.query(PEFirmOrgSnapshot)
        .filter(PEFirmOrgSnapshot.firm_id == firm_id)
        .order_by(PEFirmOrgSnapshot.snapshot_date.desc())
        .first()
    )

    changes = _compute_diff(prior.org_json if prior else None, org_json)

    total = len(rows)
    snapshot = PEFirmOrgSnapshot(
        firm_id=firm_id,
        snapshot_date=datetime.utcnow(),
        org_json=org_json,
        ic_member_count=len(org_json["investment_committee"]),
        op_partner_count=len(org_json["operating_partners"]),
        investment_team_count=len(org_json["investment_team"]),
        total_headcount=total,
        changes_from_prior=changes,
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)

    return {
        "snapshot_id": snapshot.id,
        "snapshot_date": snapshot.snapshot_date.isoformat(),
        "ic_member_count": snapshot.ic_member_count,
        "op_partner_count": snapshot.op_partner_count,
        "investment_team_count": snapshot.investment_team_count,
        "total_headcount": snapshot.total_headcount,
        "changes_from_prior": changes,
    }


def _compute_diff(prior_json: Optional[dict], current_json: dict) -> dict:
    """Compute simple people-set diff between two org JSONs."""
    if not prior_json:
        return {"note": "first snapshot", "additions": [], "departures": []}

    def get_ids(json_dict: dict) -> set:
        ids: set = set()
        for section in json_dict.values():
            if isinstance(section, list):
                for p in section:
                    pid = p.get("person_id")
                    if pid is not None:
                        ids.add(pid)
        return ids

    prior_ids = get_ids(prior_json)
    current_ids = get_ids(current_json)

    added = current_ids - prior_ids
    departed = prior_ids - current_ids

    def get_name(json_dict: dict, pid: int) -> str:
        for section in json_dict.values():
            if isinstance(section, list):
                for p in section:
                    if p.get("person_id") == pid:
                        return p.get("name", str(pid))
        return str(pid)

    return {
        "additions": [
            {"person_id": pid, "name": get_name(current_json, pid)} for pid in added
        ],
        "departures": [
            {"person_id": pid, "name": get_name(prior_json, pid)} for pid in departed
        ],
    }
