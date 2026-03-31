"""
Post-ingestion DQ hook.

Lightweight, non-blocking hook called after complete_job() marks a job as
SUCCESS. Runs four steps in sequence:
  1. Profile the ingested table
  2. Detect anomalies from the new profile
  3. Evaluate matching DQ rules
  4. Run the domain-specific BaseQualityProvider for the affected entity

Step 4 is new (PLAN_039). It routes by source prefix:
  people_* → PeopleQAService (company_id from job.dataset_id)
  pe_*     → PEDQService (firm_id)
  site_intel_* → SiteIntelDQService (job_id)
  three_pl_*   → ThreePLDQService (company_id)

Design: fire-and-forget via asyncio.create_task. Failures in the hook
never affect the ingestion job status.
"""

import asyncio
import logging
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source → provider routing
# ---------------------------------------------------------------------------

def _extract_entity_id(job, source: Optional[str]) -> Optional[int]:
    """Extract the domain entity ID from job.config JSON.

    Mapping:
      people_*    → config["company_id"]
      pe_*        → config["firm_id"] or config["company_id"]
      site_intel_* → job.id (the SiteIntelCollectionJob id IS the entity)
      three_pl_*  → config["company_id"]
    """
    if not job:
        return None
    config = job.config or {}
    s = (source or "").lower()
    if s.startswith("people"):
        return config.get("company_id")
    if s.startswith("pe"):
        return config.get("firm_id") or config.get("company_id")
    if s.startswith("site_intel"):
        # For site intel the entity IS the ingestion job id — but
        # SiteIntelCollectionJob uses a separate table/id. Return None
        # to skip until a site_intel_job_id is threaded through config.
        return config.get("site_intel_job_id")
    if s.startswith("three_pl") or s.startswith("3pl"):
        return config.get("company_id")
    return None


def _get_domain_provider(source: Optional[str]):
    """Return the appropriate BaseQualityProvider for a source prefix, or None."""
    if not source:
        return None
    s = source.lower()
    if s.startswith("people"):
        from app.services.people_qa_service import PeopleQAService
        return PeopleQAService()
    if s.startswith("pe"):
        from app.services.pe_dq_service import PEDQService
        return PEDQService()
    if s.startswith("site_intel"):
        from app.services.site_intel_dq_service import SiteIntelDQService
        return SiteIntelDQService()
    if s.startswith("three_pl") or s.startswith("3pl"):
        from app.services.three_pl_dq_service import ThreePLDQService
        return ThreePLDQService()
    return None


# Economic source prefixes — routed to EconDQService (table-scoped, not entity-scoped)
_ECON_PREFIXES = ("fred_", "bls_", "bea_", "acs5_")


def _get_econ_provider(source: Optional[str], table_name: str):
    """Return an EconDQService if source matches an economic data prefix."""
    if not source:
        return None
    s = source.lower()
    if any(s.startswith(p) for p in _ECON_PREFIXES):
        from app.services.econ_dq_service import EconDQService
        return EconDQService(db=None, table_name=table_name)  # db injected at call site
    return None


async def _run_post_ingestion_checks(
    job_id: int,
    table_name: str,
    source: Optional[str] = None,
) -> None:
    """
    Async post-ingestion DQ pipeline:
    1. Profile the ingested table
    2. Detect anomalies from the new profile
    3. Evaluate matching DQ rules
    """
    from app.core.database import get_session_factory

    SessionFactory = get_session_factory()
    db: Session = SessionFactory()

    try:
        # 1. Profile the table
        from app.core.data_profiling_service import profile_table

        logger.info(f"[DQ Hook] Profiling table '{table_name}' (job {job_id})")
        snapshot = profile_table(db, table_name, job_id=job_id, source=source)

        if not snapshot:
            logger.info(f"[DQ Hook] Profiling skipped for '{table_name}' (lock or error)")
            return

        # 2. Detect anomalies against new profile
        from app.core.anomaly_detection_service import detect_anomalies

        logger.info(f"[DQ Hook] Running anomaly detection for '{table_name}'")
        alerts = detect_anomalies(db, snapshot, table_name)

        critical_alerts = [a for a in alerts if a.severity and a.severity.value == "error"]
        if critical_alerts:
            logger.warning(
                f"[DQ Hook] {len(critical_alerts)} CRITICAL anomalies detected "
                f"on '{table_name}' after job {job_id}"
            )

        # 3. Evaluate matching DQ rules
        from app.core.data_quality_service import evaluate_rules_for_job
        from app.core.models import IngestionJob

        logger.info(f"[DQ Hook] Evaluating DQ rules for job {job_id}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        report = None
        if job:
            report = evaluate_rules_for_job(db, job, table_name)

        if report and report.rules_failed and report.rules_failed > 0:
            logger.warning(
                f"[DQ Hook] {report.rules_failed} rule failures "
                f"on '{table_name}' after job {job_id}"
            )

        # 4. Domain-specific quality provider (PLAN_039)
        # entity_id is stored in job.config JSON under source-specific keys
        qa_score: Optional[int] = None
        provider = _get_domain_provider(source)
        entity_id = _extract_entity_id(job, source) if job else None
        if provider and entity_id is not None:
            try:
                logger.info(
                    f"[DQ Hook] Running {provider.dataset} quality check "
                    f"for entity {entity_id} (job {job_id})"
                )
                qa_report = provider.run(entity_id, db)
                qa_score = qa_report.quality_score
                if qa_score < 60:
                    logger.warning(
                        f"[DQ Hook] Low quality score {qa_score}/100 for "
                        f"{provider.dataset} entity {entity_id}"
                    )
                else:
                    logger.info(
                        f"[DQ Hook] Quality score {qa_score}/100 for "
                        f"{provider.dataset} entity {entity_id}"
                    )
            except Exception as qa_exc:
                logger.warning(
                    f"[DQ Hook] Domain QA check failed for job {job_id}: {qa_exc}"
                )

        # 4b. Economic data quality provider (PLAN_047)
        # Table-scoped — runs when source prefix matches fred_/bls_/bea_/acs5_
        if qa_score is None:
            econ_provider = _get_econ_provider(source, table_name)
            if econ_provider is not None:
                try:
                    econ_provider.db = db  # inject session
                    logger.info(
                        f"[DQ Hook] Running econ quality check for table "
                        f"'{table_name}' (job {job_id})"
                    )
                    econ_report = econ_provider.run_checks()
                    qa_score = econ_report.quality_score
                    if qa_score < 60:
                        logger.warning(
                            f"[DQ Hook] Low econ quality score {qa_score}/100 "
                            f"for table '{table_name}'"
                        )
                    else:
                        logger.info(
                            f"[DQ Hook] Econ quality score {qa_score}/100 "
                            f"for table '{table_name}'"
                        )
                except Exception as econ_exc:
                    logger.warning(
                        f"[DQ Hook] Econ QA check failed for job {job_id}: {econ_exc}"
                    )

        logger.info(
            f"[DQ Hook] Post-ingestion checks complete for '{table_name}' "
            f"(job {job_id}): profile OK, {len(alerts)} anomalies, "
            f"{report.rules_failed if report and report.rules_failed else 0} rule failures, "
            f"quality score {qa_score if qa_score is not None else 'n/a'}"
        )

    except Exception as e:
        # Never propagate — the ingestion job must not be affected
        logger.error(f"[DQ Hook] Error in post-ingestion checks for job {job_id}: {e}")
    finally:
        db.close()


def schedule_post_ingestion_check(
    job_id: int,
    table_name: str,
    source: Optional[str] = None,
) -> None:
    """
    Schedule a post-ingestion DQ check as a fire-and-forget background task.

    Safe to call from synchronous code — gets or creates an event loop.
    If no event loop is running, logs a warning and skips (tests, scripts).
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_run_post_ingestion_checks(job_id, table_name, source))
        logger.debug(f"[DQ Hook] Scheduled post-ingestion check for job {job_id}")
    except RuntimeError:
        # No running event loop — likely called from sync context or tests
        logger.debug(
            f"[DQ Hook] No event loop — skipping async post-ingestion check "
            f"for job {job_id}"
        )
