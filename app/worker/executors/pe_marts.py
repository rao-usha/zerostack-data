"""PE mart build executor (SPEC_117): firms from Form ADV, then funds from Form D.

Firms run first: the fund build looks up `pe_firms.id` by CRD to set `firm_id`.
"""

import asyncio
import logging
from contextlib import nullcontext

from sqlalchemy.orm import Session

from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)


def _run_stages(conn_for, skip_firms: bool, skip_funds: bool, skip_people: bool,
                dry_run: bool) -> dict:
    from app.marts import adv_private_funds, pe_firms_sec, pe_funds_sec, pe_people_sec

    summary = {}
    if not skip_firms:
        with conn_for() as conn:
            summary["firms"] = pe_firms_sec.build(conn)
    if not skip_funds:
        # Current state from the Schedule D filing-grain tables first: it is
        # what the ADV attribution tiers read, and an empty table would make
        # them contribute nothing without failing anything.
        with conn_for() as conn:
            summary["adv_private_funds"] = adv_private_funds.build(conn)
        with conn_for() as conn:
            summary["funds"] = pe_funds_sec.build(conn, dry_run=dry_run)
    if not skip_people:
        # People last: the pair key carries firm_id and the tiers read
        # pe_funds.firm_link_method, so both must already be current.
        with conn_for() as conn:
            summary["people"] = pe_people_sec.build(conn, dry_run=dry_run)
    return summary


def run_pe_marts(skip_firms: bool = False, skip_funds: bool = False,
                 skip_people: bool = False, dry_run: bool = False) -> dict:
    engine = get_engine()
    if not dry_run:
        # each stage commits on its own, as before
        return _run_stages(engine.begin, skip_firms, skip_funds, skip_people, dry_run=False)

    # Dry run (SPEC_129): firms and ADV private funds have no dry-run mode of
    # their own, and used to write for real. Run every stage on ONE connection
    # inside ONE transaction and roll it back: firms and ADV funds report
    # exactly what a real run would change, the funds/people stages see the
    # would-be firm ids and ADV index, and nothing is kept.
    with engine.connect() as conn:
        tx = conn.begin()
        try:
            summary = _run_stages(lambda: nullcontext(conn), skip_firms, skip_funds,
                                  skip_people, dry_run=True)
        finally:
            tx.rollback()
    summary["dry_run"] = True
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
    # every attribution tier (adv_exact, adv_family, adv_platform, name_core,
    # related_person), not just the two that predate the ADV tiers
    linked = sum(v for k, v in funds.items() if k.startswith("linked_") and isinstance(v, int))
    job.progress_message = (
        f"{'DRY RUN (nothing written): ' if summary.get('dry_run') else ''}"
        f"firms +{firms.get('inserted', 0)}/~{firms.get('updated', 0)} updated; "
        f"funds +{funds.get('inserted', 0)}, linked {linked}; "
        f"people +{people.get('inserted_people', 0)} over "
        f"{people.get('firms_covered', 0)} firms"
    )
    db.commit()
    logger.info(f"pe_mart_build summary: {summary}")
