"""
Bulk SEC XBRL ingest orchestrator (PLAN_062 W1.A).

Loops through a CIK universe (default: top 2,500 by EDGAR filing prevalence)
calling the existing XBRL fetch + parse + upsert pipeline for each company.
Designed to populate `sec_financial_facts`, `sec_income_statement`,
`sec_balance_sheet`, and `sec_cash_flow_statement` with enough breadth to
support Phase A1 TabDDPM training.

Differences from the single-CIK endpoint:
- One parent orchestrator IngestionJob row (not 2,500 child rows)
- Single SECClient instance reused across all CIKs (vs. creating one per call)
- Continues on per-CIK failures rather than aborting the whole job
- Per-CIK errors logged with `error_count` aggregated on the parent job
- Built-in rate-limit respect via the SECClient's BaseAPIClient semaphore
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.sources.sec import xbrl_parser
from app.sources.sec.client import SECClient
from app.sources.sec.ingest_xbrl import (
    FACT_CONFLICT_COLUMNS,
    STATEMENT_CONFLICT_COLUMNS,
    _upsert_financial_statements,
)
from app.sources.sec.models import (
    SECBalanceSheet,
    SECCashFlowStatement,
    SECFinancialFact,
    SECIncomeStatement,
)

logger = logging.getLogger(__name__)


async def _ingest_one_cik(
    db: Session,
    client: SECClient,
    cik: str,
    skip_facts: bool = False,
) -> Dict[str, Any]:
    """
    Run XBRL fetch + parse + upsert for a single CIK, reusing the supplied client.

    Mirrors the body of `ingest_company_financial_data` but without per-CIK job
    ceremony (no IngestionJob row, no SECClient lifecycle).

    Raises on fetch/parse/upsert failures so caller can count + continue.
    """
    facts_data = await client.get_company_facts(cik)
    parsed_data = xbrl_parser.parse_company_facts(facts_data, cik)

    facts_count = 0
    if not skip_facts and parsed_data.get("financial_facts"):
        facts_count = _upsert_financial_statements(
            db,
            parsed_data["financial_facts"],
            SECFinancialFact,
            conflict_columns=FACT_CONFLICT_COLUMNS,
            batch_size=500,
        )

    income_count = 0
    if parsed_data.get("income_statement"):
        income_count = _upsert_financial_statements(
            db,
            parsed_data["income_statement"],
            SECIncomeStatement,
            conflict_columns=STATEMENT_CONFLICT_COLUMNS[SECIncomeStatement],
        )

    balance_count = 0
    if parsed_data.get("balance_sheet"):
        balance_count = _upsert_financial_statements(
            db,
            parsed_data["balance_sheet"],
            SECBalanceSheet,
            conflict_columns=STATEMENT_CONFLICT_COLUMNS[SECBalanceSheet],
        )

    cashflow_count = 0
    if parsed_data.get("cash_flow"):
        cashflow_count = _upsert_financial_statements(
            db,
            parsed_data["cash_flow"],
            SECCashFlowStatement,
            conflict_columns=STATEMENT_CONFLICT_COLUMNS[SECCashFlowStatement],
        )

    return {
        "cik": cik,
        "financial_facts": facts_count,
        "income_statements": income_count,
        "balance_sheets": balance_count,
        "cash_flow_statements": cashflow_count,
        "total_rows": facts_count + income_count + balance_count + cashflow_count,
    }


async def bulk_ingest_xbrl(
    db: Session,
    job_id: int,
    ciks: List[str],
    skip_facts: bool = True,
    log_every: int = 25,
) -> Dict[str, Any]:
    """
    Run XBRL ingest for many CIKs sequentially, aggregating into the parent job.

    Args:
        db: Database session for upserts
        job_id: Parent orchestrator IngestionJob id (status + aggregate stats)
        ciks: List of 10-digit normalized CIKs
        skip_facts: Skip raw financial_facts upsert (default True — 10x faster,
                    and `sec_financial_facts` is only needed for D&A derivation
                    which can be backfilled later from sec_financial_facts on
                    a small subset)
        log_every: Log progress every N CIKs

    Returns:
        Aggregated stats dict.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job:
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()

    client = SECClient()

    aggregates = {
        "ciks_requested": len(ciks),
        "ciks_succeeded": 0,
        "ciks_failed": 0,
        "financial_facts": 0,
        "income_statements": 0,
        "balance_sheets": 0,
        "cash_flow_statements": 0,
        "errors_sample": [],
    }

    try:
        for idx, cik in enumerate(ciks, start=1):
            try:
                result = await _ingest_one_cik(db, client, cik, skip_facts=skip_facts)
                aggregates["ciks_succeeded"] += 1
                aggregates["financial_facts"] += result["financial_facts"]
                aggregates["income_statements"] += result["income_statements"]
                aggregates["balance_sheets"] += result["balance_sheets"]
                aggregates["cash_flow_statements"] += result["cash_flow_statements"]
            except Exception as exc:
                aggregates["ciks_failed"] += 1
                if len(aggregates["errors_sample"]) < 20:
                    aggregates["errors_sample"].append(
                        {"cik": cik, "error": str(exc)[:200]}
                    )
                logger.warning("XBRL ingest failed for CIK %s: %s", cik, exc)
                try:
                    db.rollback()
                except Exception:
                    pass

            if idx % log_every == 0 or idx == len(ciks):
                logger.info(
                    "Bulk XBRL progress: %d/%d CIKs (%d ok, %d failed, %d income statements so far)",
                    idx,
                    len(ciks),
                    aggregates["ciks_succeeded"],
                    aggregates["ciks_failed"],
                    aggregates["income_statements"],
                )

        # Update parent job to SUCCESS
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = (
                aggregates["income_statements"]
                + aggregates["balance_sheets"]
                + aggregates["cash_flow_statements"]
                + aggregates["financial_facts"]
            )
            db.commit()

    except Exception as e:
        logger.error("Bulk XBRL ingest fatal error: %s", e, exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)[:500]
            db.commit()
        raise

    finally:
        await client.close()

    return aggregates


def schedule_bulk_xbrl_ingest(
    db: Session,
    ciks: List[str],
    skip_facts: bool = True,
) -> int:
    """
    Create the parent orchestrator IngestionJob row and return its id.
    Caller is responsible for scheduling the async run (e.g., via BackgroundTasks
    or a direct asyncio.create_task in a controlled async context).
    """
    job = IngestionJob(
        source="sec",
        status=JobStatus.PENDING,
        config={
            "source": "sec",
            "type": "xbrl_bulk_orchestrator",
            "cik_count": len(ciks),
            "skip_facts": skip_facts,
        },
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job.id
