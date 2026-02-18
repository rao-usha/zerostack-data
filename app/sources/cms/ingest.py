"""
CMS ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
import csv
import io
import zipfile

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.cms.client import CMSClient
from app.sources.cms import metadata

logger = logging.getLogger(__name__)


async def ingest_medicare_utilization(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    state: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest Medicare Provider Utilization and Payment Data.

    This dataset contains Medicare Part B claims data for physicians and
    other healthcare practitioners.

    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        year: Optional year filter (defaults to latest available)
        state: Optional state filter (two-letter abbreviation)
        limit: Optional limit on number of records (for testing)

    Returns:
        Dictionary with ingestion results
    """
    start_time = datetime.utcnow()
    dataset_type = "medicare_utilization"

    settings = get_settings()

    # Update job to RUNNING
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job:
        job.status = JobStatus.RUNNING
        job.started_at = start_time
        db.commit()

    try:
        # 1. Get dataset metadata
        meta = metadata.get_dataset_metadata(dataset_type)
        table_name = meta["table_name"]
        dataset_id = meta.get("socrata_dataset_id")

        # Check if dataset is available via Socrata API
        # CMS migrated from Socrata to DKAN format in 2024
        if dataset_id is None:
            error_msg = (
                f"CMS dataset '{dataset_type}' is no longer available via Socrata API. "
                f"CMS has migrated to a new DKAN format. Please visit {meta.get('source_url', 'data.cms.gov')} "
                f"to download the data directly. See https://downloads.cms.gov/files/Socrata-DKAN-API-Endpoints-Mapping.pdf "
                f"for API migration details."
            )
            logger.error(error_msg)
            if job:
                job.status = JobStatus.FAILED
                job.error_message = error_msg
                job.completed_at = datetime.utcnow()
                db.commit()
            raise ValueError(error_msg)

        # 2. Create table if not exists
        logger.info(f"Preparing table {table_name}")
        create_sql = metadata.generate_create_table_sql(dataset_type)
        db.execute(text(create_sql))
        db.commit()

        # 3. Register dataset
        _register_dataset(db, dataset_type, meta)

        # 4. Build SoQL WHERE clause for filters
        where_clauses = []
        if state:
            where_clauses.append(f"rndrng_prvdr_state_abrvtn='{state.upper()}'")

        where_clause = " AND ".join(where_clauses) if where_clauses else None

        # 5. Initialize CMS client
        client = CMSClient(
            max_concurrency=settings.max_concurrency,
            max_retries=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor
        )

        try:
            # 6. Fetch data from Socrata API
            logger.info(f"Fetching data from Socrata dataset {dataset_id}")
            records = await client.fetch_socrata_data(
                dataset_id=dataset_id,
                limit=1000,  # Records per page
                where=where_clause,
                max_records=limit
            )

            logger.info(f"Fetched {len(records)} records")

            # 7. Insert data
            if records:
                await _batch_insert_data(db, table_name, records, meta["columns"])

            # 8. Calculate results
            rows_inserted = len(records)
            duration = (datetime.utcnow() - start_time).total_seconds()

            # 9. Update job status
            if job:
                if rows_inserted == 0:
                    job.status = JobStatus.FAILED
                    job.error_message = "Ingestion completed but no rows were inserted"
                    logger.warning(f"Job {job_id}: No CMS Medicare utilization data returned")
                else:
                    job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = rows_inserted
                db.commit()

            logger.info(
                f"Successfully ingested {rows_inserted} rows into {table_name} "
                f"in {duration:.2f}s"
            )

            return {
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "duration_seconds": duration,
                "dataset_id": dataset_id
            }

        finally:
            await client.close()

    except Exception as e:
        logger.error(f"CMS Medicare utilization ingestion failed: {e}", exc_info=True)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        raise


async def ingest_hospital_cost_reports(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest Hospital Cost Report (HCRIS) data.
    
    Hospital Cost Reporting Information System data includes financial information,
    utilization data, and cost reports submitted by hospitals.
    
    Note: HCRIS data is typically available as bulk CSV downloads.
    This implementation fetches a sample/recent extract.
    
    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        year: Optional year filter
        limit: Optional limit on number of records
        
    Returns:
        Dictionary with ingestion results
    """
    start_time = datetime.utcnow()
    dataset_type = "hospital_cost_reports"
    
    settings = get_settings()
    
    # 1. Get dataset metadata
    meta = metadata.get_dataset_metadata(dataset_type)
    table_name = meta["table_name"]
    
    # 2. Create table if not exists
    logger.info(f"Preparing table {table_name}")
    create_sql = metadata.generate_create_table_sql(dataset_type)
    db.execute(text(create_sql))
    db.commit()
    
    # 3. Register dataset
    _register_dataset(db, dataset_type, meta)
    
    # Update job to RUNNING
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job:
        job.status = JobStatus.RUNNING
        job.started_at = start_time
        db.commit()

    try:
        # 4. Build HCRIS URL for requested year
        hcris_year = year or 2022
        hcris_url = f"https://www.cms.gov/files/zip/hosp{hcris_year}alphav26.zip"

        # 5. Download the ZIP file
        client = CMSClient(
            max_concurrency=settings.max_concurrency,
            max_retries=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor,
        )

        try:
            logger.info(f"Downloading HCRIS ZIP from {hcris_url}")
            zip_bytes = await client.fetch_bulk_file(hcris_url)
            logger.info(f"Downloaded {len(zip_bytes)} bytes")
        finally:
            await client.close()

        # 6. Extract and parse CSV from ZIP
        records = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV files found in HCRIS ZIP archive: {zf.namelist()}")

            for csv_name in csv_names:
                logger.info(f"Parsing {csv_name}")
                raw = zf.read(csv_name)
                # HCRIS files use CP1252 encoding
                text_data = raw.decode("cp1252", errors="replace")
                reader = csv.DictReader(io.StringIO(text_data))

                for row in reader:
                    records.append(row)
                    if limit and len(records) >= limit:
                        break
                if limit and len(records) >= limit:
                    break

        logger.info(f"Parsed {len(records)} records from HCRIS ZIP")

        # 7. Insert data
        if records:
            await _batch_insert_data(db, table_name, records, meta["columns"])

        rows_inserted = len(records)
        duration = (datetime.utcnow() - start_time).total_seconds()

        # 8. Update job status
        if job:
            job.status = JobStatus.SUCCESS if rows_inserted > 0 else JobStatus.FAILED
            if rows_inserted == 0:
                job.error_message = "No rows parsed from HCRIS ZIP"
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        logger.info(f"Ingested {rows_inserted} HCRIS rows into {table_name} in {duration:.2f}s")

        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "duration_seconds": duration,
        }

    except Exception as e:
        logger.error(f"CMS HCRIS ingestion failed: {e}", exc_info=True)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        raise


async def ingest_drug_pricing(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    brand_name: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest Medicare Part D Drug Spending data.

    This dataset contains prescription drug costs and utilization
    for brand name and generic drugs under Medicare Part D.

    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        year: Optional year filter
        brand_name: Optional filter by brand name
        limit: Optional limit on number of records

    Returns:
        Dictionary with ingestion results
    """
    start_time = datetime.utcnow()
    dataset_type = "drug_pricing"

    settings = get_settings()

    # Update job to RUNNING
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job:
        job.status = JobStatus.RUNNING
        job.started_at = start_time
        db.commit()

    try:
        # 1. Get dataset metadata
        meta = metadata.get_dataset_metadata(dataset_type)
        table_name = meta["table_name"]
        dataset_id = meta.get("socrata_dataset_id")

        # Check if dataset is available via Socrata API
        # CMS migrated from Socrata to DKAN format in 2024
        if dataset_id is None:
            error_msg = (
                f"CMS dataset '{dataset_type}' is no longer available via Socrata API. "
                f"CMS has migrated to a new DKAN format. Please visit {meta.get('source_url', 'data.cms.gov')} "
                f"to download the data directly. See https://downloads.cms.gov/files/Socrata-DKAN-API-Endpoints-Mapping.pdf "
                f"for API migration details."
            )
            logger.error(error_msg)
            if job:
                job.status = JobStatus.FAILED
                job.error_message = error_msg
                job.completed_at = datetime.utcnow()
                db.commit()
            raise ValueError(error_msg)

        # 2. Create table if not exists
        logger.info(f"Preparing table {table_name}")
        create_sql = metadata.generate_create_table_sql(dataset_type)
        db.execute(text(create_sql))
        db.commit()

        # 3. Register dataset
        _register_dataset(db, dataset_type, meta)

        # 4. Build SoQL WHERE clause for filters
        where_clauses = []
        if year:
            where_clauses.append(f"year={year}")
        if brand_name:
            # Escape single quotes in brand name
            safe_brand = brand_name.replace("'", "''")
            where_clauses.append(f"brnd_name='{safe_brand}'")

        where_clause = " AND ".join(where_clauses) if where_clauses else None

        # 5. Initialize CMS client
        client = CMSClient(
            max_concurrency=settings.max_concurrency,
            max_retries=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor
        )

        try:
            # 6. Fetch data from Socrata API
            logger.info(f"Fetching data from Socrata dataset {dataset_id}")
            records = await client.fetch_socrata_data(
                dataset_id=dataset_id,
                limit=1000,  # Records per page
                where=where_clause,
                max_records=limit
            )

            logger.info(f"Fetched {len(records)} records")

            # 7. Insert data
            if records:
                await _batch_insert_data(db, table_name, records, meta["columns"])

            # 8. Calculate results
            rows_inserted = len(records)
            duration = (datetime.utcnow() - start_time).total_seconds()

            # 9. Update job status
            if job:
                if rows_inserted == 0:
                    job.status = JobStatus.FAILED
                    job.error_message = "Ingestion completed but no rows were inserted"
                    logger.warning(f"Job {job_id}: No CMS drug pricing data returned")
                else:
                    job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = rows_inserted
                db.commit()

            logger.info(
                f"Successfully ingested {rows_inserted} rows into {table_name} "
                f"in {duration:.2f}s"
            )

            return {
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "duration_seconds": duration,
                "dataset_id": dataset_id
            }

        finally:
            await client.close()

    except Exception as e:
        logger.error(f"CMS drug pricing ingestion failed: {e}", exc_info=True)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        raise


async def _batch_insert_data(
    db: Session,
    table_name: str,
    records: list,
    column_defs: Dict[str, Dict[str, str]],
    batch_size: int = 1000
) -> None:
    """
    Batch insert data into Postgres using parameterized queries.
    
    Args:
        db: Database session
        table_name: Target table name
        records: List of records from CMS API
        column_defs: Column definitions from metadata
        batch_size: Number of rows per batch
    """
    if not records:
        return
    
    # Get column names from first record
    all_columns = list(column_defs.keys())
    
    # Build INSERT statement with parameterized values
    columns_sql = ", ".join(all_columns)
    placeholders = ", ".join([f":{col}" for col in all_columns])
    insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
    
    # Process in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        # Normalize batch data
        normalized_batch = []
        for record in batch:
            normalized = {}
            
            for col_name in all_columns:
                raw_value = record.get(col_name)
                normalized[col_name] = _normalize_value(raw_value, column_defs[col_name]["type"])
            
            normalized_batch.append(normalized)
        
        # Execute batch insert using parameterized query
        db.execute(text(insert_sql), normalized_batch)
        db.commit()
        
        logger.debug(f"Inserted batch of {len(batch)} rows into {table_name}")


def _normalize_value(value: Any, col_type: str) -> Any:
    """
    Normalize a CMS API value for database insertion.
    
    Handles null values, empty strings, type conversions.
    
    Args:
        value: Raw value from API
        col_type: PostgreSQL column type
        
    Returns:
        Normalized value
    """
    if value is None or value == "" or value == "null":
        return None
    
    # Handle numeric types
    if col_type in ("INTEGER", "NUMERIC"):
        try:
            if col_type == "INTEGER":
                return int(float(value))  # Handle cases like "123.0"
            else:
                return float(value)
        except (ValueError, TypeError):
            return None
    
    # Handle date types
    if col_type == "DATE":
        try:
            # CMS dates are typically in format: YYYY-MM-DD or MM/DD/YYYY
            from dateutil import parser
            return parser.parse(str(value)).date()
        except (ValueError, TypeError):
            return None

    # Text types - return as-is
    return str(value)


def _register_dataset(
    db: Session,
    dataset_type: str,
    meta: Dict[str, Any]
) -> None:
    """
    Register dataset in dataset_registry if not already registered.
    
    Args:
        db: Database session
        dataset_type: Type of dataset
        meta: Dataset metadata
    """
    table_name = meta["table_name"]
    
    # Check if already registered
    existing = db.query(DatasetRegistry).filter(
        DatasetRegistry.table_name == table_name
    ).first()
    
    if existing:
        logger.info(f"Dataset {table_name} already registered")
        existing.last_updated_at = datetime.utcnow()
        db.commit()
    else:
        dataset = DatasetRegistry(
            source="cms",
            dataset_id=dataset_type,
            table_name=table_name,
            display_name=meta["display_name"],
            description=meta["description"],
            source_metadata={
                "dataset_type": dataset_type,
                "source_url": meta["source_url"],
                "socrata_dataset_id": meta.get("socrata_dataset_id")
            }
        )
        db.add(dataset)
        db.commit()
        logger.info(f"Registered dataset {table_name}")

