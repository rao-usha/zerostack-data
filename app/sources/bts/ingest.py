"""
BTS ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for Bureau of Transportation Statistics datasets.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.bts.client import BTSClient
from app.sources.bts import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_bts_data(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for BTS data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: Dataset identifier (border_crossing, faf_regional, vmt)
        
    Returns:
        Dictionary with:
        - table_name: Generated Postgres table name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for BTS {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"bts_{dataset}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {"dataset": dataset}
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="bts",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_dataset_display_name(dataset),
                description=metadata.get_dataset_description(dataset),
                source_metadata={"dataset": dataset}
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {"table_name": table_name}
    
    except Exception as e:
        logger.error(f"Failed to prepare table for BTS data: {e}")
        raise


async def ingest_border_crossing_data(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    state: Optional[str] = None,
    border: Optional[str] = None,
    measure: Optional[str] = None,
    app_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest BTS border crossing data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date filter (YYYY-MM)
        end_date: End date filter (YYYY-MM)
        state: Filter by state code
        border: Filter by border (US-Canada Border, US-Mexico Border)
        measure: Filter by measure type (Trucks, Containers, etc.)
        app_token: Optional Socrata app token for higher rate limits
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    # Initialize BTS client
    client = BTSClient(
        app_token=app_token,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
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
            default_start, default_end = metadata.get_default_date_range("border_crossing")
            start_date = start_date or default_start
            end_date = end_date or default_end
        
        logger.info(
            f"Ingesting BTS border crossing data: "
            f"{start_date} to {end_date}, state={state}, border={border}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_bts_data(db, "border_crossing")
        table_name = table_info["table_name"]
        
        # Fetch data from Socrata API with pagination
        all_records = []
        offset = 0
        batch_size = 50000  # Socrata limit
        
        while True:
            logger.info(f"Fetching border crossing data (offset={offset})")
            
            records = await client.get_border_crossing_data(
                limit=batch_size,
                offset=offset,
                state=state,
                border=border,
                start_date=start_date,
                end_date=end_date,
                measure=measure
            )
            
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            if len(records) < batch_size:
                break
            
            offset += batch_size
        
        # Parse records
        parsed_records = metadata.parse_border_crossing_response(all_records)
        
        if not parsed_records:
            logger.warning("No border crossing data to insert")
            rows_inserted = 0
        else:
            # Insert data
            rows_inserted = await _insert_border_crossing_data(
                db, table_name, parsed_records
            )
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "border_crossing",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}",
            "filters": {
                "state": state,
                "border": border,
                "measure": measure
            }
        }
    
    except Exception as e:
        logger.error(f"BTS border crossing ingestion failed: {e}", exc_info=True)
        
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


async def _insert_border_crossing_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert border crossing data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "port_name", "state", "port_code", "border", "date",
        "measure", "value", "latitude", "longitude"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("port_code", "date", "measure")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (port_code, date, measure)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    # Execute in batches
    batch_size = 1000
    rows_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if (i + batch_size) % 10000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} rows")
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_vmt_data(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    state: Optional[str] = None,
    app_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest BTS Vehicle Miles Traveled (VMT) data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date filter (YYYY-MM)
        end_date: End date filter (YYYY-MM)
        state: Filter by state name
        app_token: Optional Socrata app token
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = BTSClient(
        app_token=app_token,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        # Set default date range
        if not start_date or not end_date:
            default_start, default_end = metadata.get_default_date_range("vmt")
            start_date = start_date or default_start
            end_date = end_date or default_end
        
        logger.info(
            f"Ingesting BTS VMT data: "
            f"{start_date} to {end_date}, state={state}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_bts_data(db, "vmt")
        table_name = table_info["table_name"]
        
        # Fetch data with pagination
        all_records = []
        offset = 0
        batch_size = 50000
        
        while True:
            logger.info(f"Fetching VMT data (offset={offset})")
            
            records = await client.get_vmt_data(
                limit=batch_size,
                offset=offset,
                state=state,
                start_date=start_date,
                end_date=end_date
            )
            
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            if len(records) < batch_size:
                break
            
            offset += batch_size
        
        # Parse records
        parsed_records = metadata.parse_vmt_response(all_records)
        
        if not parsed_records:
            logger.warning("No VMT data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_vmt_data(db, table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "vmt",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}",
            "filters": {"state": state}
        }
    
    except Exception as e:
        logger.error(f"BTS VMT ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_vmt_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert VMT data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "date", "state", "state_fips", "vmt", "vmt_sa",
        "percent_change", "functional_system"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("date", "state", "functional_system")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (date, state, COALESCE(functional_system, ''))
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    batch_size = 1000
    rows_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_faf_regional_data(
    db: Session,
    job_id: int,
    version: str = "regional_2018_2024",
    app_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest BTS Freight Analysis Framework (FAF5) regional data into Postgres.
    
    Downloads bulk CSV from BTS and loads into database.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        version: FAF version to download:
            - "regional_2018_2024": Regional database 2018-2024 (default)
            - "regional_forecasts": Regional with forecasts to 2050
            - "state_2018_2024": State-level 2018-2024
        app_token: Optional Socrata app token (not used for CSV downloads)
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = BTSClient(
        app_token=app_token,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"Ingesting BTS FAF5 regional data: version={version}")
        
        # Prepare table
        table_info = await prepare_table_for_bts_data(db, "faf_regional")
        table_name = table_info["table_name"]
        
        # Download and parse FAF data
        logger.info("Downloading FAF5 data (this may take a few minutes)...")
        raw_records = await client.download_faf_data(version=version)
        
        # Parse records
        parsed_records = metadata.parse_faf_records(raw_records)
        
        if not parsed_records:
            logger.warning("No FAF data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_faf_data(db, table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "faf_regional",
            "version": version,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"BTS FAF ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_faf_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert FAF data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "fr_orig", "dms_orig", "dms_dest", "fr_dest",
        "fr_inmode", "dms_mode", "fr_outmode",
        "sctg2", "trade_type", "tons", "value", "tmiles", "curval", "year"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("year", "fr_orig", "fr_dest", "sctg2", "dms_mode", "trade_type")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (year, fr_orig, fr_dest, sctg2, dms_mode, trade_type)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    batch_size = 5000  # Larger batches for bulk data
    rows_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if (i + batch_size) % 50000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} rows")
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted
