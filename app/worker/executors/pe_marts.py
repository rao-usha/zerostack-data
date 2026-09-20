"""PE mart build executor (SPEC_117): firms from Form ADV, then funds from Form D.

Firms run first: the fund build looks up `pe_firms.id` by CRD to set `firm_id`.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)


def run_pe_marts(skip_firms: bool = False, skip_funds: bool = False) -> dict:
    from app.marts import pe_firms_sec, pe_funds_sec

    engine = get_engine()
    summary = {}
    if not skip_firms:
        with engine.begin() as conn:
            summary["firms"] = pe_firms_sec.build(conn)
    if not skip_funds:
        with engine.begin() as conn:
            summary["funds"] = pe_funds_sec.build(conn)
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
    )

    firms = summary.get("firms", {})
    funds = summary.get("funds", {})
    job.progress_message = (
        f"firms +{firms.get('inserted', 0)}/~{firms.get('updated', 0)} updated; "
        f"funds +{funds.get('inserted', 0)}, linked "
        f"{funds.get('linked_name_core', 0) + funds.get('linked_related_person', 0)}"
    )
    db.commit()
    logger.info(f"pe_mart_build summary: {summary}")
