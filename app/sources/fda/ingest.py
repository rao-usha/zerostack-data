"""
openFDA Device Registration ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
Paginates by US state to stay within the openFDA skip limit of 26,000.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.core.batch_operations import batch_insert
from app.sources.fda.client import OpenFDAClient
from app.sources.fda import metadata

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE PREPARATION
# =============================================================================


def prepare_table(db: Session) -> str:
    """
    Prepare the fda_device_registrations table.

    Creates the table if it does not exist and registers it
    in the dataset registry.

    Args:
        db: Database session

    Returns:
        Table name
    """
    table_name = metadata.TABLE_NAME

    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_create_table_sql()
    # Execute each statement separately (CREATE TABLE + CREATE INDEX)
    for statement in create_sql.split(";"):
        statement = statement.strip()
        if statement:
            db.execute(text(statement))
    db.commit()

    # Register dataset
    _register_dataset(db, table_name)

    return table_name


def _register_dataset(db: Session, table_name: str):
    """Register dataset in dataset_registry."""
    dataset_id = "fda_device_registrations"

    existing = (
        db.query(DatasetRegistry)
        .filter(DatasetRegistry.table_name == table_name)
        .first()
    )

    if existing:
        existing.last_updated_at = datetime.utcnow()
        db.commit()
        logger.info(f"Updated dataset {dataset_id} registration")
    else:
        entry = DatasetRegistry(
            source="fda",
            dataset_id=dataset_id,
            table_name=table_name,
            display_name=metadata.get_display_name(),
            description=metadata.get_description(),
            source_metadata={
                "api": "https://open.fda.gov/apis/device/registrationlisting/",
                "api_key_required": False,
                "max_skip": 26000,
            },
        )
        db.add(entry)
        db.commit()
        logger.info(f"Registered dataset {dataset_id}")


# =============================================================================
# INGESTION
# =============================================================================


async def ingest_device_registrations(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    search_query: Optional[str] = None,
    limit_per_state: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Ingest FDA device registration data from the openFDA API.

    Iterates over US states (or a specified subset) and fetches all
    device registrations for each state, respecting the openFDA
    skip limit of 26,000 per query partition.

    Args:
        db: Database session
        job_id: Ingestion job ID for status tracking
        states: Optional list of state codes to ingest (defaults to all US states)
        search_query: Optional additional Lucene search filter
        limit_per_state: Optional limit on records per state (for testing)

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OpenFDAClient(
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        # Prepare table
        table_name = prepare_table(db)

        # Determine which states to process
        target_states = states or metadata.US_STATES

        total_inserted = 0
        total_fetched = 0
        state_stats = {}
        errors = []

        for state in target_states:
            try:
                logger.info(f"Fetching FDA device registrations for {state}")

                if limit_per_state:
                    # Single request with limit (for testing)
                    response = await client.search_device_registrations(
                        state=state,
                        search_query=search_query,
                        limit=min(limit_per_state, client.MAX_LIMIT),
                    )
                    raw_results = response.get("results", [])
                else:
                    # Full pagination for the state
                    raw_results = await client.fetch_all_for_state(
                        state=state, search_query=search_query
                    )

                if not raw_results:
                    logger.debug(f"No registrations for state {state}")
                    state_stats[state] = 0
                    continue

                # Parse records
                parsed_rows = []
                for result in raw_results:
                    row = metadata.parse_registration_record(result)
                    if row:
                        # Ensure all keys are present for batch insert consistency
                        for col in metadata.COLUMNS:
                            row.setdefault(col, None)
                        parsed_rows.append(row)

                total_fetched += len(raw_results)

                if parsed_rows:
                    # Batch insert with upsert
                    insert_result = batch_insert(
                        db=db,
                        table_name=table_name,
                        rows=parsed_rows,
                        columns=metadata.COLUMNS,
                        batch_size=500,
                        conflict_columns=metadata.CONFLICT_COLUMNS,
                        update_columns=metadata.UPDATE_COLUMNS,
                        job_id=job_id,
                    )
                    inserted = insert_result.total_rows
                    total_inserted += inserted
                    state_stats[state] = inserted
                    logger.info(
                        f"State {state}: {inserted} rows upserted "
                        f"from {len(raw_results)} API results"
                    )
                else:
                    state_stats[state] = 0

            except Exception as e:
                logger.error(
                    f"Failed to ingest state {state}: {e}", exc_info=True
                )
                errors.append({"state": state, "error": str(e)})
                state_stats[state] = 0

        # Update job status
        if job:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_inserted
            if errors:
                job.error_details = {"state_errors": errors}
            db.commit()

        result = {
            "table_name": table_name,
            "rows_inserted": total_inserted,
            "records_fetched": total_fetched,
            "states_processed": len(target_states),
            "state_stats": state_stats,
        }
        if errors:
            result["errors"] = errors

        logger.info(
            f"FDA device registration ingestion complete: "
            f"{total_inserted} rows from {len(target_states)} states"
        )

        return result

    except Exception as e:
        logger.error(
            f"FDA device registration ingestion failed: {e}", exc_info=True
        )

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise
    finally:
        await client.close()
