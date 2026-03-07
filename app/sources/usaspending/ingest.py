"""
USAspending.gov ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.usaspending.client import USASpendingClient, NAICS_CODES_OF_INTEREST
from app.sources.usaspending import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_usaspending_data(db: Session) -> Dict[str, Any]:
    """
    Prepare database table for USAspending data ingestion.

    Steps:
    1. Generate table name
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry

    Args:
        db: Database session

    Returns:
        Dictionary with table_name

    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name()

        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for USAspending award data")
        create_sql = metadata.generate_create_table_sql(table_name)

        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()

        # 4. Register in dataset_registry
        dataset_id = metadata.DATASET_ID

        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "api_base_url": USASpendingClient.BASE_URL,
                "endpoint": "search/spending_by_award/",
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="usaspending",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "api_base_url": USASpendingClient.BASE_URL,
                    "endpoint": "search/spending_by_award/",
                },
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")

        return {"table_name": table_name}

    except Exception as e:
        logger.error(f"Failed to prepare table for USAspending data: {e}")
        raise


async def ingest_usaspending_awards(
    db: Session,
    job_id: int,
    naics_codes: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    award_type_codes: Optional[List[str]] = None,
    min_amount: Optional[float] = None,
    max_pages: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Ingest USAspending award data into Postgres.

    Paginates through the search API and batch inserts results.

    Args:
        db: Database session
        job_id: Ingestion job ID
        naics_codes: NAICS codes to filter by (defaults to NAICS_CODES_OF_INTEREST)
        states: State codes to filter by (optional)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        award_type_codes: Award type codes to filter by
        min_amount: Minimum award amount
        max_pages: Maximum number of pages to fetch (for testing/limiting)

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    # Initialize client
    client = USASpendingClient(
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

        # Set default date range if not provided
        if not start_date or not end_date:
            default_start, default_end = metadata.get_default_date_range()
            start_date = start_date or default_start
            end_date = end_date or default_end

        # Validate date formats
        if not metadata.validate_date_format(start_date):
            raise ValueError(f"Invalid start date format: {start_date}. Use YYYY-MM-DD")
        if not metadata.validate_date_format(end_date):
            raise ValueError(f"Invalid end date format: {end_date}. Use YYYY-MM-DD")

        # Use default NAICS codes if not provided
        if not naics_codes:
            naics_codes = list(NAICS_CODES_OF_INTEREST.keys())

        logger.info(
            f"Ingesting USAspending awards: "
            f"NAICS={naics_codes}, states={states}, "
            f"{start_date} to {end_date}"
        )

        # Prepare table
        table_info = await prepare_table_for_usaspending_data(db)
        table_name = table_info["table_name"]

        # Build filters
        filters = USASpendingClient._build_filters(
            naics_codes=naics_codes,
            states=states,
            start_date=start_date,
            end_date=end_date,
            award_type_codes=award_type_codes,
            min_amount=min_amount,
        )

        # Fetch and insert data with pagination
        all_parsed_data = []
        page = 1
        limit = 100  # USAspending max per page

        insert_sql = metadata.get_insert_sql(table_name)
        rows_inserted = 0

        while True:
            # Fetch page
            logger.info(f"Fetching USAspending awards page {page}")
            api_response = await client.search_awards(
                filters=filters,
                page=page,
                limit=limit,
            )

            # Parse response
            parsed = metadata.parse_awards_response(api_response)

            if not parsed:
                logger.info(f"No more results at page {page}")
                break

            all_parsed_data.extend(parsed)

            # Ensure all records have identical keys
            for rec in parsed:
                for col in metadata.COLUMNS:
                    rec.setdefault(col, None)

            # Batch insert this page
            db.execute(text(insert_sql), parsed)
            rows_inserted += len(parsed)
            db.commit()

            logger.info(
                f"Inserted {len(parsed)} awards (page {page}, "
                f"total: {rows_inserted})"
            )

            # Check pagination
            pagination = metadata.get_response_pagination(api_response)
            if not pagination["has_next"]:
                logger.info(
                    f"Reached last page. Total available: {pagination['total']}"
                )
                break

            # Check max pages limit
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages limit ({max_pages})")
                break

            page += 1

        logger.info(f"Successfully inserted {rows_inserted} USAspending awards")

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "pages_fetched": page,
            "naics_codes": naics_codes,
            "states": states,
            "date_range": f"{start_date} to {end_date}",
        }

    except Exception as e:
        logger.error(f"USAspending ingestion failed: {e}", exc_info=True)

        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()
