"""
PE Org Classifier — classifies PE firm team members into role types.

Uses title heuristics first; falls back to LLM for ambiguous cases.
Builds Investment Committee membership records from IC classification.
"""
import re
import logging
from typing import Optional
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Title patterns for operating partners
OP_TITLE_PATTERNS = [
    r"\boperating partner\b",
    r"\bvalue creation\b",
    r"\bportfolio operations\b",
    r"\bportfolio support\b",
    r"\boperational advisor\b",
    r"\boperating advisor\b",
    r"\bentrepreneur.in.residence\b",
    r"\bexecutive.in.residence\b",
    r"\bsenior advisor\b",
]

# Title patterns for IR/fundraising
IR_TITLE_PATTERNS = [
    r"\binvestor relations\b",
    r"\bfundraising\b",
    r"\bcapital formation\b",
    r"\bclient relations\b",
    r"\bclient services\b",
    r"\bmarketing\b",
]

# Title patterns for finance/ops
FINANCE_OPS_PATTERNS = [
    r"\bchief financial officer\b",
    r"\bcfo\b",
    r"\bchief operating officer\b",
    r"\bcoo\b",
    r"\bcontroller\b",
    r"\bcompliance\b",
    r"\bgeneral counsel\b",
    r"\bhuman resources\b",
    r"\bhr\b",
    r"\badmin\b",
    r"\boffice manager\b",
]

# IC-eligible seniority levels
IC_SENIORITY = {"Partner", "Managing Director", "Managing Partner", "Senior Managing Director"}


def classify_role_type(
    title: str,
    seniority: Optional[str] = None,
    department: Optional[str] = None,
) -> str:
    """Classify a PE firm team member's role_type from title/seniority/department."""
    title_lower = (title or "").lower()
    dept_lower = (department or "").lower()

    # Operating partner check (highest priority — distinct role)
    for pattern in OP_TITLE_PATTERNS:
        if re.search(pattern, title_lower):
            return "operating_partner"

    # Advisory board
    if re.search(r"\badvisor(y)?\b", title_lower) or "advisory" in dept_lower:
        return "advisory_board"

    # IR/fundraising
    for pattern in IR_TITLE_PATTERNS:
        if re.search(pattern, title_lower) or re.search(pattern, dept_lower):
            return "ir_fundraising"

    # Finance/ops
    for pattern in FINANCE_OPS_PATTERNS:
        if re.search(pattern, title_lower):
            return "finance_ops"

    # Default: investment team
    return "investment_team"


def is_ic_member(
    role_type: str,
    seniority: Optional[str],
    title: Optional[str],
) -> bool:
    """Determine if a person should be on the Investment Committee."""
    if role_type != "investment_team":
        return False
    if seniority in IC_SENIORITY:
        return True
    title_lower = (title or "").lower()
    return bool(
        re.search(
            r"\bmanaging (director|partner)\b|\bgeneral partner\b|\bfounding partner\b",
            title_lower,
        )
    )


def classify_firm_people(db: Session, firm_id: int) -> dict:
    """
    Classify all current team members at a PE firm.

    Updates role_type on PEFirmPeople and builds IC membership records.
    Returns summary counts.
    """
    from app.core.pe_models import PEFirmPeople, PEPerson, PEInvestmentCommittee

    # Load current team
    rows = (
        db.query(PEFirmPeople, PEPerson)
        .join(PEPerson, PEFirmPeople.person_id == PEPerson.id)
        .filter(PEFirmPeople.firm_id == firm_id, PEFirmPeople.is_current == True)  # noqa: E712
        .all()
    )

    counts = {
        "investment_team": 0,
        "operating_partner": 0,
        "advisory_board": 0,
        "ir_fundraising": 0,
        "finance_ops": 0,
        "lpac_member": 0,
        "ic_members": 0,
    }

    for firm_person, person in rows:
        role_type = classify_role_type(
            title=person.current_title or firm_person.title or "",
            seniority=firm_person.seniority,
            department=firm_person.department,
        )
        counts[role_type] = counts.get(role_type, 0) + 1

        # Update role_type on the PEFirmPeople record
        firm_person.role_type = role_type

        # Build IC membership
        if is_ic_member(role_type, firm_person.seniority, person.current_title or firm_person.title):
            counts["ic_members"] += 1
            existing = (
                db.query(PEInvestmentCommittee)
                .filter(
                    PEInvestmentCommittee.firm_id == firm_id,
                    PEInvestmentCommittee.person_id == person.id,
                )
                .one_or_none()
            )
            if not existing:
                title_lower = (person.current_title or "").lower()
                role = (
                    "chair"
                    if "managing partner" in title_lower or "managing director" in title_lower
                    else "voting_member"
                )
                db.add(
                    PEInvestmentCommittee(
                        firm_id=firm_id,
                        person_id=person.id,
                        role=role,
                        is_current=True,
                    )
                )

    db.commit()
    return counts
