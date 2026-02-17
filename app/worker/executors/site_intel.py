"""Site Intel collection executor for the worker queue."""
import logging
from typing import List, Optional

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute a site_intel job by delegating to the existing orchestrator."""
    from app.core.database import get_session_factory
    from app.core.config import get_settings
    from app.sources.site_intel.runner import SiteIntelOrchestrator
    from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource

    # Import all collectors so they register
    import app.sources.site_intel.power           # noqa: F401
    import app.sources.site_intel.telecom         # noqa: F401
    import app.sources.site_intel.transport       # noqa: F401
    import app.sources.site_intel.labor           # noqa: F401
    import app.sources.site_intel.risk            # noqa: F401
    import app.sources.site_intel.incentives      # noqa: F401
    import app.sources.site_intel.logistics       # noqa: F401
    import app.sources.site_intel.water_utilities # noqa: F401

    payload = job.payload or {}
    domains: Optional[List[str]] = payload.get("domains")
    sources: Optional[List[str]] = payload.get("sources")
    states: Optional[List[str]] = payload.get("states")

    settings = get_settings()

    # Build API keys
    api_keys = {}
    key_mapping = {
        "eia": "eia_api_key",
        "eia_electricity": "eia_api_key",
        "eia_gas": "eia_api_key",
        "bls": "bls_api_key",
        "bls_oes": "bls_api_key",
        "bls_qcew": "bls_api_key",
    }
    for source_val, field_name in key_mapping.items():
        key_val = getattr(settings, field_name, None)
        if key_val:
            api_keys[source_val] = key_val

    # Use a fresh session for the long-running work
    SessionLocal = get_session_factory()
    work_db = SessionLocal()

    try:
        orchestrator = SiteIntelOrchestrator(work_db, api_keys=api_keys)

        # Send progress
        _progress(db, job, 5.0, "Resolving targets")

        # Resolve targets
        target_domains = None
        if domains:
            target_domains = []
            for d in domains:
                try:
                    target_domains.append(SiteIntelDomain(d))
                except ValueError:
                    logger.warning(f"Unknown domain: {d}")

        target_sources = None
        if sources:
            target_sources = []
            for s in sources:
                try:
                    target_sources.append(SiteIntelSource(s))
                except ValueError:
                    logger.warning(f"Unknown source: {s}")

        kwargs = {}
        if states:
            kwargs["states"] = states

        _progress(db, job, 10.0, "Collecting")

        if target_sources:
            for source in target_sources:
                domain = None
                for d in SiteIntelDomain:
                    if source in orchestrator.get_sources_for_domain(d):
                        domain = d
                        break
                if domain:
                    await orchestrator.collect(domain, source, **kwargs)
        elif target_domains:
            for domain in target_domains:
                await orchestrator.collect_domain(domain, **kwargs)
        else:
            await orchestrator.full_sync(**kwargs)

        _progress(db, job, 100.0, "Completed")
        logger.info("Site intel collection completed")

    finally:
        work_db.close()


def _progress(db: Session, job: JobQueue, pct: float, msg: str):
    """Update progress on the job and send a PG notification."""
    job.progress_pct = pct
    job.progress_message = msg
    db.commit()
    send_job_event(db, "job_progress", {
        "job_id": job.id,
        "job_type": "site_intel",
        "progress_pct": pct,
        "progress_message": msg,
    })
    db.commit()
