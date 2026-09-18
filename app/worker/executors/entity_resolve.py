"""Entity master executor (SPEC_116): feeds -> CIK/CRD bridge -> resolve.

Payload:
    skip_feeds: bool      reuse core.source_record as-is
    skip_bridge: bool     keep the existing bridge
    include_name_tier: bool (default True) name+state bridge tier
    dry_run: bool         plan and ledger only, no entity writes

The blocking work runs in a thread so the worker heartbeat keeps ticking.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.database import get_engine
from app.core.models_queue import JobQueue

logger = logging.getLogger(__name__)


def run_entity_master(
    skip_feeds: bool = False,
    skip_bridge: bool = False,
    include_name_tier: bool = True,
    dry_run: bool = False,
    progress=None,
) -> dict:
    """Refresh the entity master. Each stage commits on its own."""
    from app.entities import cik_crd_bridge, feeds, resolve

    engine = get_engine()
    summary = {}

    if not skip_feeds:
        if progress:
            progress("feeding core.source_record", 5.0)
        with engine.begin() as conn:
            summary["feeds"] = feeds.run_feeds(conn, progress=progress)

    if not skip_bridge:
        if progress:
            progress("building CIK/CRD bridge", 40.0)
        with engine.begin() as conn:
            summary["bridge"] = cik_crd_bridge.build(conn, include_name_tier=include_name_tier)

    if progress:
        progress("resolving entities", 60.0)
    with engine.begin() as conn:
        summary["resolve"] = resolve.resolve(conn, dry_run=dry_run)
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
    )

    resolved = summary.get("resolve", {})
    job.progress_message = (
        f"entities: {resolved.get('components_materialized', 0)} components, "
        f"{resolved.get('entities_new', 0)} new, {resolved.get('entities_written', 0)} written, "
        f"bridge {summary.get('bridge', {}).get('accepted', 0)}"
    )
    db.commit()
    logger.info(f"entity_resolve summary: {summary}")
