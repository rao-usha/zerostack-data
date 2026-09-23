"""PE mart build executor (SPEC_117): firms from Form ADV, then funds from Form D.

Firms run first: the fund build looks up `pe_firms.id` by CRD to set `firm_id`.

Payload:
    skip_firms / skip_funds / skip_people: bool
    dry_run: bool                 build, gate and ledger; keep nothing
    publish_guard_override        (SPEC_129) accept a publish-guard trip
    input_override: true | [src]  (SPEC_126a, admin) build on failed/stale inputs
    input_max_age_days: {src: n}  (SPEC_126a, admin) per-input max age
    gate_override: true | [gate]  (SPEC_126a, admin) commit despite failed gates

The worker runs the build guarded (SPEC_126a): inputs asserted first, every
stage in ONE transaction, ship gates before commit, a `core.mart_build` row
for every run. See app/marts/build_ledger.py.
"""

import asyncio
import logging
from contextlib import nullcontext

from sqlalchemy.orm import Session

from app.core.copy_loader import publish_guard_override
from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)

MART = "pe_marts"


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


def _stages(skip_firms: bool, skip_funds: bool, skip_people: bool) -> list:
    out = []
    if not skip_firms:
        out.append("firms")
    if not skip_funds:
        out += ["adv_private_funds", "funds"]
    if not skip_people:
        out.append("people")
    return out


def run_pe_marts(skip_firms: bool = False, skip_funds: bool = False,
                 skip_people: bool = False, dry_run: bool = False, *,
                 guard: bool = False, input_override=None, input_max_age_days=None,
                 gate_override=None, ingestion_job_id=None, job_queue_id=None) -> dict:
    """Build the PE marts on ONE connection inside ONE transaction.

    Every stage shares the transaction (SPEC_126a): a failure in `people` no
    longer leaves `firms` and `funds` rebuilt. A dry run rolls it back (SPEC_129:
    firms and ADV private funds have no dry-run mode of their own; here they
    report exactly what a real run would change, and the funds/people stages
    see the would-be firm ids and ADV index).

    `guard=True` (the worker) adds the input assertions, ship gates and ledger
    row; direct calls (tests, scripts) build without them.
    """
    engine = get_engine()

    def build(conn) -> dict:
        return _run_stages(lambda: nullcontext(conn), skip_firms, skip_funds,
                           skip_people, dry_run=dry_run)

    if guard:
        from app.marts import build_ledger, inputs

        summary = build_ledger.run_guarded(
            engine,
            mart=MART,
            sources=inputs.sources_for(inputs.PE_MART_STAGE_INPUTS,
                                       _stages(skip_firms, skip_funds, skip_people)),
            build=build,
            dry_run=dry_run,
            input_override=input_override,
            input_max_age_days=input_max_age_days,
            gate_override=gate_override,
            ingestion_job_id=ingestion_job_id,
            job_queue_id=job_queue_id,
        )
    else:
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                summary = build(conn)
            except BaseException:
                tx.rollback()
                raise
            if dry_run:
                tx.rollback()
            else:
                tx.commit()
    if dry_run:
        summary["dry_run"] = True
    return summary


async def execute(job: JobQueue, db: Session):
    payload = job.payload or {}
    job.progress_pct = 1.0
    job.progress_message = "Building PE marts from SEC data"
    db.commit()

    # publish_guard_override (SPEC_129): accept a guard trip for this job only
    with publish_guard_override(payload.get("publish_guard_override")):
        summary = await asyncio.to_thread(
            run_pe_marts,
            skip_firms=bool(payload.get("skip_firms")),
            skip_funds=bool(payload.get("skip_funds")),
            skip_people=bool(payload.get("skip_people")),
            dry_run=bool(payload.get("dry_run")),
            guard=True,
            input_override=payload.get("input_override"),
            input_max_age_days=payload.get("input_max_age_days"),
            gate_override=payload.get("gate_override"),
            ingestion_job_id=payload.get("ingestion_job_id"),
            job_queue_id=getattr(job, "id", None),
        )

    firms = summary.get("firms", {})
    funds = summary.get("funds", {})
    people = summary.get("people", {})
    build = summary.get("mart_build") or {}
    # every attribution tier (adv_exact, adv_family, adv_platform, name_core,
    # related_person), not just the two that predate the ADV tiers
    linked = sum(v for k, v in funds.items() if k.startswith("linked_") and isinstance(v, int))
    job.progress_message = (
        f"{'DRY RUN (nothing written): ' if summary.get('dry_run') else ''}"
        f"{'build #' + str(build['id']) + ': ' if build.get('id') else ''}"
        f"firms +{firms.get('inserted', 0)}/~{firms.get('updated', 0)} updated; "
        f"funds +{funds.get('inserted', 0)}, linked {linked}; "
        f"people +{people.get('inserted_people', 0)} over "
        f"{people.get('firms_covered', 0)} firms"
        f"{'; gates overridden: ' + ', '.join(build['gates_overridden']) if build.get('gates_overridden') else ''}"
    )[:500]
    db.commit()
    logger.info(f"pe_mart_build summary: {summary}")
