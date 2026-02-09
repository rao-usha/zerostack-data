"""
SEC XBRL financial data ingestion.

Fetches and parses structured financial data from SEC Company Facts API.
Uses upsert (ON CONFLICT) to safely handle re-ingestion.
"""
import logging
from typing import Dict, Any, List, Set
from datetime import datetime
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus
from app.sources.sec.client import SECClient
from app.sources.sec import xbrl_parser
from app.sources.sec.models import (
    SECFinancialFact,
    SECIncomeStatement,
    SECBalanceSheet,
    SECCashFlowStatement
)

logger = logging.getLogger(__name__)


def _get_model_columns(model_class) -> Set[str]:
    """Get the set of valid column names for a SQLAlchemy model."""
    mapper = inspect(model_class)
    return {col.key for col in mapper.column_attrs}


def _filter_to_model_columns(record: Dict[str, Any], valid_columns: Set[str]) -> Dict[str, Any]:
    """Filter a record dict to only include keys that are valid model columns."""
    return {k: v for k, v in record.items() if k in valid_columns}


def _deduplicate_records(records: List[Dict[str, Any]], conflict_columns: List[str]) -> List[Dict[str, Any]]:
    """
    Deduplicate records by conflict columns, keeping the last occurrence.

    PostgreSQL ON CONFLICT cannot handle duplicate conflict keys within the
    same INSERT statement, so we must deduplicate before batching.
    """
    seen = {}
    for record in records:
        # Build a key from the conflict columns; use str() to handle None
        key = tuple(str(record.get(col)) for col in conflict_columns)
        seen[key] = record  # last occurrence wins
    return list(seen.values())


def _upsert_financial_statements(
    db: Session,
    records: List[Dict[str, Any]],
    model_class,
    conflict_columns: List[str],
    batch_size: int = 500
) -> int:
    """
    Upsert financial statement records using PostgreSQL ON CONFLICT.

    Uses multi-row INSERT for better performance.

    Args:
        db: Database session
        records: List of record dicts to upsert
        model_class: SQLAlchemy model class
        conflict_columns: Columns that form the unique constraint for ON CONFLICT
        batch_size: Number of records per batch INSERT

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    valid_columns = _get_model_columns(model_class)
    table = model_class.__table__
    count = 0

    # Pre-filter all records to valid columns
    filtered_records = []
    for record in records:
        filtered = _filter_to_model_columns(record, valid_columns)
        filtered.pop("id", None)
        if filtered:
            filtered_records.append(filtered)

    if not filtered_records:
        return 0

    # Deduplicate by conflict columns to avoid ON CONFLICT batch errors
    filtered_records = _deduplicate_records(filtered_records, conflict_columns)

    # Determine update columns from the first record
    sample = filtered_records[0]
    update_col_names = [k for k in sample if k not in conflict_columns and k != "id"]

    for i in range(0, len(filtered_records), batch_size):
        batch = filtered_records[i:i + batch_size]

        stmt = pg_insert(table).values(batch)

        if update_col_names:
            update_cols = {col: stmt.excluded[col] for col in update_col_names}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_cols,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=conflict_columns,
            )

        db.execute(stmt)
        count += len(batch)
        db.commit()

    return count


async def ingest_company_financial_data(
    db: Session,
    job_id: int,
    cik: str,
    skip_facts: bool = False
) -> Dict[str, Any]:
    """
    Ingest structured financial data from SEC XBRL.

    Fetches from /api/xbrl/companyfacts/CIK{cik}.json

    Args:
        db: Database session
        job_id: Ingestion job ID
        cik: Company CIK
        skip_facts: If True, skip inserting raw financial facts (much faster)

    Returns:
        Dictionary with ingestion results
    """
    client = SECClient()

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(f"Fetching financial facts for CIK {cik}")

        # Fetch company facts from SEC
        facts_data = await client.get_company_facts(cik)

        # Parse financial data
        parsed_data = xbrl_parser.parse_company_facts(facts_data, cik)

        # Upsert financial facts (optional — slowest part)
        facts_count = 0
        if not skip_facts and parsed_data["financial_facts"]:
            logger.info(f"Upserting {len(parsed_data['financial_facts'])} financial facts")
            facts_count = _upsert_financial_statements(
                db,
                parsed_data["financial_facts"],
                SECFinancialFact,
                conflict_columns=["cik", "fact_name", "period_end_date", "fiscal_year", "fiscal_period", "unit"],
                batch_size=500,
            )
            logger.info(f"Upserted {facts_count} financial facts")
        elif skip_facts:
            logger.info(f"Skipping {len(parsed_data.get('financial_facts', []))} financial facts (skip_facts=True)")

        # Upsert income statements
        income_count = 0
        if parsed_data["income_statement"]:
            logger.info(f"Upserting {len(parsed_data['income_statement'])} income statements")
            income_count = _upsert_financial_statements(
                db,
                parsed_data["income_statement"],
                SECIncomeStatement,
                conflict_columns=["cik", "period_end_date", "fiscal_year", "fiscal_period"],
            )
            logger.info(f"Upserted {income_count} income statements")

        # Upsert balance sheets
        balance_count = 0
        if parsed_data["balance_sheet"]:
            logger.info(f"Upserting {len(parsed_data['balance_sheet'])} balance sheets")
            balance_count = _upsert_financial_statements(
                db,
                parsed_data["balance_sheet"],
                SECBalanceSheet,
                conflict_columns=["cik", "period_end_date", "fiscal_year", "fiscal_period"],
            )
            logger.info(f"Upserted {balance_count} balance sheets")

        # Upsert cash flow statements
        cashflow_count = 0
        if parsed_data["cash_flow"]:
            logger.info(f"Upserting {len(parsed_data['cash_flow'])} cash flow statements")
            cashflow_count = _upsert_financial_statements(
                db,
                parsed_data["cash_flow"],
                SECCashFlowStatement,
                conflict_columns=["cik", "period_end_date", "fiscal_year", "fiscal_period"],
            )
            logger.info(f"Upserted {cashflow_count} cash flow statements")

        total_rows = facts_count + income_count + balance_count + cashflow_count

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()

        return {
            "cik": cik,
            "financial_facts": facts_count,
            "income_statements": income_count,
            "balance_sheets": balance_count,
            "cash_flow_statements": cashflow_count,
            "total_rows": total_rows
        }

    except Exception as e:
        logger.error(f"XBRL ingestion failed for CIK {cik}: {e}", exc_info=True)
        db.rollback()

        # Update job status to failed
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)
                db.commit()
        except Exception:
            logger.error("Failed to update job status after error", exc_info=True)

        raise

    finally:
        await client.close()
