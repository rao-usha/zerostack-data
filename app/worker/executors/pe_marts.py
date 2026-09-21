"""PE mart build executor (SPEC_117): firms from Form ADV, then funds from Form D.

Firms run first: the fund build looks up `pe_firms.id` by CRD to set `firm_id`.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)


def run_pe_marts(skip_firms: bool = False, skip_funds: bool = False,
                 skip_people: bool = False, dry_run: bool = False) -> dict:
    from app.marts import adv_private_funds, pe_firms_sec, pe_funds_sec, pe_people_sec

    engine = get_engine()
    summary = {}
    if not skip_firms:
        with engine.begin() as conn:
            summary["firms"] = pe_firms_sec.build(conn)
    if not skip_funds:
        # Current state from the Schedule D filing-grain tables first: it is
        # what the ADV attribution tiers read, and an empty table would make
        # them contribute nothing without failing anything.
        with engine.begin() as conn:
            summary["adv_private_funds"] = adv_private_funds.build(conn)
        with engine.begin() as conn:
            summary["funds"] = pe_funds_sec.build(conn, dry_run=dry_run)
    if not skip_people:
        # People last: the pair key carries firm_id and the tiers read
        # pe_funds.firm_link_method, so both must already be current.
        with engine.begin() as conn:
            summary["people"] = pe_people_sec.build(conn, dry_run=dry_run)
    return summary


async def execute(job: JobQueue, db: Session):
    payload = job.payload or {}
    job.progress_pct = 1.0
    job.progress_message = "Building PE marts from SEC data"
    db.commit()

    summary = await asyncio.to_thread(
        run_pe_marts,
        skip_firms=bool(payload.get("skip_firms")),
        skip_funds=bool(payload.get("skip_funds")),
        skip_people=bool(payload.get("skip_people")),
        dry_run=bool(payload.get("dry_run")),
    )

    firms = summary.get("firms", {})
    funds = summary.get("funds", {})
    people = summary.get("people", {})
    job.progress_message = (
        f"firms +{firms.get('inserted', 0)}/~{firms.get('updated', 0)} updated; "
        f"funds +{funds.get('inserted', 0)}, linked "
        f"{funds.get('linked_name_core', 0) + funds.get('linked_related_person', 0)}; "
        f"people +{people.get('inserted_people', 0)} over "
        f"{people.get('firms_covered', 0)} firms"
    )
    db.commit()
    logger.info(f"pe_mart_build summary: {summary}")
