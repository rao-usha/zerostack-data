"""
FRED ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.fred.client import FREDClient
from app.sources.fred import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_fred_category(
    db: Session,
    category: str,
    series_ids: List[str]
) -> Dict[str, Any]:
    """
    Prepare database table for FRED data ingestion.
    
    Steps:
    1. Generate table name based on category
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        category: Category name (e.g., "interest_rates", "monetary_aggregates")
        series_ids: List of series IDs to be stored
        
    Returns:
        Dictionary with:
        - table_name: Generated Postgres table name
        - series_count: Number of series
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(category)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for {len(series_ids)} series")
        create_sql = metadata.generate_create_table_sql(table_name, series_ids)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"fred_{category.lower()}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "category": category,
                "series_ids": series_ids,
                "series_count": len(series_ids)
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="fred",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_category_display_name(category),
                description=metadata.get_category_description(category),
                source_metadata={
                    "category": category,
                    "series_ids": series_ids,
                    "series_count": len(series_ids)
                }
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {
            "table_name": table_name,
            "series_count": len(series_ids)
        }
    
    except Exception as e:
        logger.error(f"Failed to prepare table for FRED category: {e}")
        raise


async def ingest_fred_category(
    db: Session,
    job_id: int,
    category: str,
    series_ids: Optional[List[str]] = None,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FRED category data into Postgres.
    
    Steps:
    1. Validate parameters
    2. Prepare table
    3. Fetch data from FRED API
    4. Parse and insert data
    5. Update job status
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        category: Category name (e.g., "interest_rates", "monetary_aggregates")
        series_ids: Optional list of specific series IDs (uses defaults if not provided)
        observation_start: Start date in YYYY-MM-DD format (optional)
        observation_end: End date in YYYY-MM-DD format (optional)
        api_key: Optional FRED API key
        
    Returns:
        Dictionary with ingestion results
        
    Raises:
        Exception: On ingestion errors
    """
    settings = get_settings()
    
    # Initialize FRED client
    client = FREDClient(
        api_key=api_key,
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
        
        # 1. Validate and set defaults
        if not series_ids:
            try:
                series_ids = metadata.get_series_for_category(category)
                logger.info(f"Using default series for {category}: {series_ids}")
            except ValueError as e:
                raise ValueError(str(e))
        
        if not observation_end or not observation_start:
            default_start, default_end = metadata.get_default_date_range()
            observation_start = observation_start or default_start
            observation_end = observation_end or default_end
        
        # Validate date formats
        if not metadata.validate_date_format(observation_start):
            raise ValueError(f"Invalid start date format: {observation_start}. Use YYYY-MM-DD")
        if not metadata.validate_date_format(observation_end):
            raise ValueError(f"Invalid end date format: {observation_end}. Use YYYY-MM-DD")
        
        logger.info(
            f"Ingesting FRED {category}: "
            f"{len(series_ids)} series, {observation_start} to {observation_end}"
        )
        
        # 2. Prepare table
        table_info = await prepare_table_for_fred_category(
            db, category, series_ids
        )
        table_name = table_info["table_name"]
        
        # 3. Fetch data from FRED API
        logger.info(f"Fetching {len(series_ids)} series from FRED API")
        
        all_parsed_data = {}
        
        # Fetch each series individually (FRED API doesn't support batch requests)
        for i, series_id in enumerate(series_ids, 1):
            logger.info(f"Fetching series {i}/{len(series_ids)}: {series_id}")
            
            try:
                api_response = await client.get_series_observations(
                    series_id=series_id,
                    observation_start=observation_start,
                    observation_end=observation_end
                )
                
                # 4. Parse data
                parsed = metadata.parse_observations(api_response, series_id)
                all_parsed_data[series_id] = parsed
                
                logger.info(f"Parsed {len(parsed)} observations for {series_id}")
                
            except Exception as e:
                logger.error(f"Failed to fetch series {series_id}: {e}")
                # Continue with other series instead of failing completely
                all_parsed_data[series_id] = []
        
        # 5. Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        
        if not rows:
            logger.warning("No data to insert")
            rows_inserted = 0
        else:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            # Build parameterized INSERT with ON CONFLICT
            insert_sql = f"""
                INSERT INTO {table_name} 
                (series_id, date, value, realtime_start, realtime_end)
                VALUES 
                (:series_id, :date, :value, :realtime_start, :realtime_end)
                ON CONFLICT (series_id, date) 
                DO UPDATE SET
                    value = EXCLUDED.value,
                    realtime_start = EXCLUDED.realtime_start,
                    realtime_end = EXCLUDED.realtime_end,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 1000
            rows_inserted = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                
                # Commit each batch
                db.commit()
                
                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(rows)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # 6. Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": category,
            "series_count": len(series_ids),
            "rows_inserted": rows_inserted,
            "date_range": f"{observation_start} to {observation_end}"
        }
    
    except Exception as e:
        logger.error(f"FRED ingestion failed: {e}", exc_info=True)
        
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


async def ingest_all_fred_categories(
    db: Session,
    categories: List[str],
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest multiple FRED categories.
    
    This is a convenience function for ingesting multiple FRED categories
    at once (interest_rates, monetary_aggregates, industrial_production, etc.).
    
    Args:
        db: Database session
        categories: List of category names to ingest
        observation_start: Start date in YYYY-MM-DD format (optional)
        observation_end: End date in YYYY-MM-DD format (optional)
        api_key: Optional FRED API key
        
    Returns:
        Dictionary with results for each category
    """
    results = {}
    
    for category in categories:
        logger.info(f"Starting ingestion for FRED category: {category}")
        
        # Create job
        job_config = {
            "source": "fred",
            "category": category,
            "observation_start": observation_start,
            "observation_end": observation_end,
        }
        
        job = IngestionJob(
            source="fred",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        try:
            result = await ingest_fred_category(
                db=db,
                job_id=job.id,
                category=category,
                observation_start=observation_start,
                observation_end=observation_end,
                api_key=api_key
            )
            
            results[category] = {
                "status": "success",
                "job_id": job.id,
                **result
            }
            
        except Exception as e:
            logger.error(f"Failed to ingest {category}: {e}")
            results[category] = {
                "status": "failed",
                "job_id": job.id,
                "error": str(e)
            }
    
    return results

