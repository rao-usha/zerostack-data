"""
Kaggle M5 Forecasting dataset ingestion orchestration.

High-level functions that coordinate:
1. Data download from Kaggle
2. Table creation in PostgreSQL
3. Data parsing and insertion
4. Job tracking via IngestionJob model

IMPORTANT:
- All ingestion runs are tracked via ingestion_jobs table (MANDATORY per RULES)
- Uses bounded concurrency via batch processing
- Parameterized queries only (no SQL string concatenation)
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.kaggle.client import KaggleClient
from app.sources.kaggle import m5_metadata

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE PREPARATION
# =============================================================================


async def prepare_m5_tables(db: Session) -> Dict[str, Any]:
    """
    Create all M5 tables in the database (idempotent).

    Creates:
    - m5_calendar: Calendar dimension
    - m5_items: Item dimension
    - m5_prices: Price data
    - m5_sales: Daily sales (long format)

    Args:
        db: Database session

    Returns:
        Dictionary with created table names

    Raises:
        Exception: On table creation failure
    """
    try:
        logger.info("Preparing M5 tables...")

        # Create tables one by one to ensure proper ordering
        schemas = [
            m5_metadata.M5_CALENDAR_SCHEMA,
            m5_metadata.M5_ITEMS_SCHEMA,
            m5_metadata.M5_PRICES_SCHEMA,
            m5_metadata.M5_SALES_SCHEMA,
        ]

        tables = []
        for schema in schemas:
            table_name = schema["table_name"]
            logger.info(f"Creating table: {table_name}")

            # Generate and execute CREATE TABLE
            create_sql = m5_metadata.generate_create_table_sql(schema)

            # Split and execute each statement separately
            for statement in create_sql.split(";"):
                statement = statement.strip()
                # Skip empty statements and comments
                if statement and not statement.startswith("--"):
                    logger.debug(f"Executing: {statement[:80]}...")
                    db.execute(text(statement))

            # Commit after each table to ensure it exists before indexes
            db.commit()
            tables.append(table_name)

        logger.info(f"Created M5 tables: {tables}")

        # Register datasets
        for table_type in ["calendar", "items", "prices", "sales"]:
            table_name = m5_metadata.generate_table_name(table_type)
            dataset_id = f"m5_{table_type}"

            existing = (
                db.query(DatasetRegistry)
                .filter(DatasetRegistry.table_name == table_name)
                .first()
            )

            if existing:
                existing.last_updated_at = datetime.utcnow()
                existing.source_metadata = m5_metadata.get_m5_summary()
            else:
                entry = DatasetRegistry(
                    source="kaggle",
                    dataset_id=dataset_id,
                    table_name=table_name,
                    display_name=m5_metadata.get_table_display_name(table_type),
                    description=m5_metadata.get_table_description(table_type),
                    source_metadata=m5_metadata.get_m5_summary(),
                )
                db.add(entry)

        db.commit()

        return {"tables": tables, "status": "created"}

    except Exception as e:
        logger.error(f"Failed to prepare M5 tables: {e}")
        db.rollback()
        raise


# =============================================================================
# DATA INGESTION
# =============================================================================


async def ingest_m5_calendar(
    db: Session, file_path: Path, batch_size: int = 500
) -> int:
    """
    Ingest calendar.csv into m5_calendar table.

    Args:
        db: Database session
        file_path: Path to calendar.csv
        batch_size: Rows per batch insert

    Returns:
        Number of rows inserted
    """
    logger.info(f"Ingesting calendar from: {file_path}")

    insert_sql = """
        INSERT INTO m5_calendar 
        (date, d, wm_yr_wk, weekday, wday, month, year, 
         event_name_1, event_type_1, event_name_2, event_type_2,
         snap_ca, snap_tx, snap_wi)
        VALUES 
        (:date, :d, :wm_yr_wk, :weekday, :wday, :month, :year,
         :event_name_1, :event_type_1, :event_name_2, :event_type_2,
         :snap_ca, :snap_tx, :snap_wi)
        ON CONFLICT (date) DO UPDATE SET
            d = EXCLUDED.d,
            wm_yr_wk = EXCLUDED.wm_yr_wk,
            event_name_1 = EXCLUDED.event_name_1,
            event_type_1 = EXCLUDED.event_type_1,
            event_name_2 = EXCLUDED.event_name_2,
            event_type_2 = EXCLUDED.event_type_2,
            snap_ca = EXCLUDED.snap_ca,
            snap_tx = EXCLUDED.snap_tx,
            snap_wi = EXCLUDED.snap_wi,
            ingested_at = NOW()
    """

    rows_inserted = 0
    batch = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            parsed = m5_metadata.parse_calendar_row(row)
            batch.append(parsed)

            if len(batch) >= batch_size:
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                batch = []

        # Insert remaining rows
        if batch:
            db.execute(text(insert_sql), batch)
            rows_inserted += len(batch)

    db.commit()
    logger.info(f"Inserted {rows_inserted} calendar rows")

    return rows_inserted


async def ingest_m5_prices(db: Session, file_path: Path, batch_size: int = 5000) -> int:
    """
    Ingest sell_prices.csv into m5_prices table.

    Args:
        db: Database session
        file_path: Path to sell_prices.csv
        batch_size: Rows per batch insert

    Returns:
        Number of rows inserted
    """
    logger.info(f"Ingesting prices from: {file_path}")

    insert_sql = """
        INSERT INTO m5_prices 
        (store_id, item_id, wm_yr_wk, sell_price)
        VALUES 
        (:store_id, :item_id, :wm_yr_wk, :sell_price)
        ON CONFLICT (store_id, item_id, wm_yr_wk) DO UPDATE SET
            sell_price = EXCLUDED.sell_price,
            ingested_at = NOW()
    """

    rows_inserted = 0
    batch = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            parsed = m5_metadata.parse_price_row(row)
            batch.append(parsed)

            if len(batch) >= batch_size:
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                batch = []

                if rows_inserted % 100000 == 0:
                    logger.info(f"Inserted {rows_inserted} price rows...")
                    db.commit()  # Commit periodically to avoid long transactions

        if batch:
            db.execute(text(insert_sql), batch)
            rows_inserted += len(batch)

    db.commit()
    logger.info(f"Inserted {rows_inserted} price rows")

    return rows_inserted


async def ingest_m5_sales(
    db: Session,
    file_path: Path,
    calendar_path: Optional[Path] = None,
    batch_size: int = 10000,
    limit_items: Optional[int] = None,
) -> Dict[str, int]:
    """
    Ingest sales_train_validation.csv into m5_items and m5_sales tables.

    This transforms the wide-format sales data (d_1 to d_1969 columns)
    into long format for efficient querying.

    Args:
        db: Database session
        file_path: Path to sales_train_validation.csv
        calendar_path: Optional path to calendar.csv for date lookup
        batch_size: Rows per batch insert
        limit_items: Optional limit on number of items to process (for testing)

    Returns:
        Dictionary with items_inserted and sales_inserted counts
    """
    logger.info(f"Ingesting sales from: {file_path}")

    # Build calendar lookup for d -> date mapping
    calendar_lookup = {}
    if calendar_path and calendar_path.exists():
        logger.info("Building calendar lookup for date mapping...")
        with open(calendar_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                calendar_lookup[row["d"]] = row["date"]

    # SQL for items
    items_sql = """
        INSERT INTO m5_items 
        (id, item_id, dept_id, cat_id, store_id, state_id)
        VALUES 
        (:id, :item_id, :dept_id, :cat_id, :store_id, :state_id)
        ON CONFLICT (id) DO NOTHING
    """

    # SQL for sales
    sales_sql = """
        INSERT INTO m5_sales 
        (item_store_id, d, item_id, store_id, date, sales)
        VALUES 
        (:item_store_id, :d, :item_id, :store_id, :date, :sales)
        ON CONFLICT (item_store_id, d) DO UPDATE SET
            sales = EXCLUDED.sales,
            date = EXCLUDED.date,
            ingested_at = NOW()
    """

    items_inserted = 0
    sales_inserted = 0
    items_batch = []
    sales_batch = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Get day columns (d_1, d_2, ..., d_1969)
        fieldnames = reader.fieldnames
        day_columns = [col for col in fieldnames if col.startswith("d_")]
        logger.info(f"Found {len(day_columns)} day columns")

        item_count = 0
        for row in reader:
            # Check limit
            if limit_items and item_count >= limit_items:
                break

            item_count += 1

            # Extract item dimension data
            item_data = m5_metadata.parse_sales_row_to_items(row)
            items_batch.append(item_data)

            # Transform to long format sales data
            sales_rows = m5_metadata.parse_sales_row_to_long_format(
                row, day_columns, calendar_lookup
            )
            sales_batch.extend(sales_rows)

            # Batch insert items
            if len(items_batch) >= batch_size:
                db.execute(text(items_sql), items_batch)
                items_inserted += len(items_batch)
                items_batch = []

            # Batch insert sales
            if len(sales_batch) >= batch_size:
                db.execute(text(sales_sql), sales_batch)
                sales_inserted += len(sales_batch)
                sales_batch = []

                if sales_inserted % 500000 == 0:
                    logger.info(
                        f"Inserted {sales_inserted} sales rows, {items_inserted} items..."
                    )
                    db.commit()  # Commit periodically

        # Insert remaining batches
        if items_batch:
            db.execute(text(items_sql), items_batch)
            items_inserted += len(items_batch)

        if sales_batch:
            db.execute(text(sales_sql), sales_batch)
            sales_inserted += len(sales_batch)

    db.commit()
    logger.info(f"Inserted {items_inserted} items, {sales_inserted} sales rows")

    return {"items_inserted": items_inserted, "sales_inserted": sales_inserted}


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================


async def ingest_m5_dataset(
    db: Session,
    job_id: int,
    force_download: bool = False,
    limit_items: Optional[int] = None,
    kaggle_username: Optional[str] = None,
    kaggle_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orchestrate full M5 dataset ingestion.

    Steps:
    1. Update job status to RUNNING
    2. Download files from Kaggle (if not cached)
    3. Create database tables
    4. Ingest calendar data
    5. Ingest price data
    6. Ingest sales data (transforms wide to long format)
    7. Update job status to SUCCESS/FAILED

    Args:
        db: Database session
        job_id: IngestionJob ID for tracking
        force_download: Force re-download even if files exist
        limit_items: Optional limit on items to process (for testing)
        kaggle_username: Kaggle username (uses settings if not provided)
        kaggle_key: Kaggle API key (uses settings if not provided)

    Returns:
        Dictionary with ingestion results

    Raises:
        Exception: On ingestion failure
    """
    settings = get_settings()

    # Get Kaggle credentials
    if not kaggle_username or not kaggle_key:
        try:
            kaggle_username, kaggle_key = settings.require_kaggle_credentials()
        except ValueError:
            # Try without credentials - might work if ~/.kaggle/kaggle.json exists
            logger.warning(
                "Kaggle credentials not in config. "
                "Will try using ~/.kaggle/kaggle.json if available."
            )
            kaggle_username = None
            kaggle_key = None

    # Initialize client
    client = KaggleClient(
        username=kaggle_username, key=kaggle_key, data_dir=settings.kaggle_data_dir
    )

    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        results = {
            "competition": KaggleClient.M5_COMPETITION,
            "tables": [],
            "rows": {},
        }

        # Step 1: Download files from Kaggle
        logger.info("Step 1: Downloading M5 files from Kaggle...")
        downloaded_files = await client.download_m5_files(force=force_download)

        results["files_downloaded"] = list(downloaded_files.keys())
        logger.info(f"Downloaded files: {results['files_downloaded']}")

        # Get file paths
        calendar_path = client.get_local_file_path(
            KaggleClient.M5_COMPETITION, "calendar.csv"
        )
        prices_path = client.get_local_file_path(
            KaggleClient.M5_COMPETITION, "sell_prices.csv"
        )
        sales_path = client.get_local_file_path(
            KaggleClient.M5_COMPETITION, "sales_train_validation.csv"
        )

        # Validate all required files exist
        if not calendar_path or not calendar_path.exists():
            raise FileNotFoundError("calendar.csv not found after download")
        if not prices_path or not prices_path.exists():
            raise FileNotFoundError("sell_prices.csv not found after download")
        if not sales_path or not sales_path.exists():
            raise FileNotFoundError(
                "sales_train_validation.csv not found after download"
            )

        # Step 2: Create tables
        logger.info("Step 2: Creating M5 tables...")
        table_result = await prepare_m5_tables(db)
        results["tables"] = table_result["tables"]

        # Step 3: Ingest calendar
        logger.info("Step 3: Ingesting calendar data...")
        calendar_rows = await ingest_m5_calendar(db, calendar_path)
        results["rows"]["calendar"] = calendar_rows

        # Step 4: Ingest prices
        logger.info("Step 4: Ingesting price data...")
        price_rows = await ingest_m5_prices(db, prices_path)
        results["rows"]["prices"] = price_rows

        # Step 5: Ingest sales (this is the big one!)
        logger.info("Step 5: Ingesting sales data (this may take a while)...")
        sales_result = await ingest_m5_sales(
            db, sales_path, calendar_path=calendar_path, limit_items=limit_items
        )
        results["rows"]["items"] = sales_result["items_inserted"]
        results["rows"]["sales"] = sales_result["sales_inserted"]

        # Calculate total rows
        total_rows = (
            calendar_rows
            + price_rows
            + sales_result["items_inserted"]
            + sales_result["sales_inserted"]
        )
        results["total_rows"] = total_rows

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()

        logger.info(f"M5 ingestion complete: {total_rows} total rows")
        return results

    except Exception as e:
        logger.error(f"M5 ingestion failed: {e}", exc_info=True)

        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise


async def create_m5_ingestion_job(db: Session, config: Dict[str, Any]) -> IngestionJob:
    """
    Create a new ingestion job for M5 dataset.

    Args:
        db: Database session
        config: Job configuration

    Returns:
        Created IngestionJob instance
    """
    job = IngestionJob(
        source="kaggle",
        status=JobStatus.PENDING,
        config={
            "dataset": "m5-forecasting",
            "competition": KaggleClient.M5_COMPETITION,
            **config,
        },
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    logger.info(f"Created M5 ingestion job: {job.id}")
    return job
