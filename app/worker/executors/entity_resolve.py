"""Entity master executor (SPEC_116): feeds -> CIK/CRD bridge -> resolve.

Payload:
    skip_feeds: bool      reuse core.source_record as-is
    skip_bridge: bool     keep the existing bridge
    include_name_tier: bool (default True) name+state bridge tier
    dry_run: bool         build, gate and ledger; keep nothing (one rolled-back tx)
    input_override / input_max_age_days / gate_override
                          admin overrides of the SPEC_126a input and ship gates

The blocking work runs in a thread so the worker heartbeat keeps ticking.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)

MART = "entity_resolve"


def _stages(skip_feeds: bool, skip_bridge: bool) -> list:
    out = []
    if not skip_feeds:
        out.append("feeds")
    if not skip_bridge:
        out.append("bridge")
    return out + ["resolve"]


def run_entity_master(
    skip_feeds: bool = False,
    skip_bridge: bool = False,
    include_name_tier: bool = True,
    dry_run: bool = False,
    progress=None,
    *,
    guard: bool = False,
    input_override=None,
    input_max_age_days=None,
    gate_override=None,
    ingestion_job_id=None,
    job_queue_id=None,
) -> dict:
    """Refresh the entity master on ONE connection inside ONE transaction.

    The stages used to commit one by one, so a failed `resolve` left the feeds
    and bridge rewritten, and `dry_run` (honoured only by `resolve`) still
    wrote them. Now a failure or a dry run keeps nothing (SPEC_126a).
    `guard=True` (the worker) adds input assertions, ship gates and the
    `core.mart_build` ledger row.
    """
    from app.entities import cik_crd_bridge, feeds, resolve

    engine = get_engine()

    def build(conn) -> dict:
        summary = {}
        if not skip_feeds:
            if progress:
                progress("feeding core.source_record", 5.0)
            summary["feeds"] = feeds.run_feeds(conn, progress=progress)
        if not skip_bridge:
            if progress:
                progress("building CIK/CRD bridge", 40.0)
            summary["bridge"] = cik_crd_bridge.build(conn, include_name_tier=include_name_tier)
        if progress:
            progress("resolving entities", 60.0)
        summary["resolve"] = resolve.resolve(conn, dry_run=dry_run)
        return summary

    if guard:
        from app.marts import build_ledger, inputs

        return build_ledger.run_guarded(
            engine,
            mart=MART,
            sources=inputs.sources_for(inputs.ENTITY_STAGE_INPUTS,
                                       _stages(skip_feeds, skip_bridge)),
            build=build,
            dry_run=dry_run,
            input_override=input_override,
            input_max_age_days=input_max_age_days,
            gate_override=gate_override,
            ingestion_job_id=ingestion_job_id,
            job_queue_id=job_queue_id,
        )

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
    return summary


async def execute(job: JobQueue, db: Session):
    payload = job.payload or {}
    job.progress_pct = 1.0
    job.progress_message = "Starting entity master refresh"
    db.commit()

    def progress(message: str, pct: float) -> None:
        # separate session: the executor's own session must stay clean
        from app.core.database import get_session_factory

        session = get_session_factory()()
        try:
            row = session.get(JobQueue, job.id)
            if row is not None:
                row.progress_message = message[:500]
                row.progress_pct = round(pct, 1)
                session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()

    summary = await asyncio.to_thread(
        run_entity_master,
        skip_feeds=bool(payload.get("skip_feeds")),
        skip_bridge=bool(payload.get("skip_bridge")),
        include_name_tier=payload.get("include_name_tier", True),
        dry_run=bool(payload.get("dry_run")),
        progress=progress,
        guard=True,
        input_override=payload.get("input_override"),
        input_max_age_days=payload.get("input_max_age_days"),
        gate_override=payload.get("gate_override"),
        ingestion_job_id=payload.get("ingestion_job_id"),
        job_queue_id=getattr(job, "id", None),
    )

    resolved = summary.get("resolve", {})
    build = summary.get("mart_build") or {}
    job.progress_message = (
        f"{'build #' + str(build['id']) + ': ' if build.get('id') else ''}"
        f"entities: {resolved.get('components_materialized', 0)} components, "
        f"{resolved.get('entities_new', 0)} new, {resolved.get('entities_written', 0)} written, "
        f"bridge {summary.get('bridge', {}).get('accepted', 0)}"
    )
    db.commit()
    logger.info(f"entity_resolve summary: {summary}")
