"""
NOAA data ingestion logic.

This module orchestrates the ingestion of NOAA climate and weather data
following the service's plugin pattern and safety rules.
"""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.core.models import IngestionJob, DatasetRegistry
from app.sources.noaa.client import NOAAClient
from app.sources.noaa.metadata import NOAA_DATASETS, get_table_schema, get_primary_key

logger = logging.getLogger(__name__)


async def create_noaa_table(
    session: AsyncSession,
    dataset_key: str
) -> None:
    """
    Create NOAA dataset table if it doesn't exist.
    
    This function is idempotent - safe to call multiple times.
    
    Args:
        session: Database session
        dataset_key: Key in NOAA_DATASETS dictionary
    """
    dataset = NOAA_DATASETS.get(dataset_key)
    if not dataset:
        raise ValueError(f"Unknown dataset: {dataset_key}")
    
    table_name = dataset.table_name
    schema = get_table_schema(dataset_key)
    primary_keys = get_primary_key(dataset_key)
    
    # Build CREATE TABLE statement
    columns_def = []
    for col_name, col_type in schema.items():
        columns_def.append(f"{col_name} {col_type}")
    
    # Add primary key constraint
    pk_constraint = f"PRIMARY KEY ({', '.join(primary_keys)})"
    columns_def.append(pk_constraint)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_def)}
    )
    """
    
    logger.info(f"Creating table {table_name}")
    await session.execute(text(create_table_sql))
    await session.commit()
    
    # Create indexes for common query patterns
    await create_noaa_indexes(session, table_name)


async def create_noaa_indexes(
    session: AsyncSession,
    table_name: str
) -> None:
    """
    Create indexes for efficient querying.
    
    Args:
        session: Database session
        table_name: Name of the table
    """
    indexes = [
        # Index on date for time-series queries
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date)",
        
        # Index on station for station-specific queries
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_station ON {table_name}(station)",
        
        # Index on datatype for type-specific queries
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_datatype ON {table_name}(datatype)",
        
        # Index on location_id for location queries
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_location ON {table_name}(location_id)",
        
        # Composite index for common query pattern
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date_type_station ON {table_name}(date, datatype, station)",
    ]
    
    for index_sql in indexes:
        try:
            await session.execute(text(index_sql))
        except Exception as e:
            logger.warning(f"Failed to create index: {e}")
    
    await session.commit()
    logger.info(f"Created indexes for {table_name}")


async def register_noaa_dataset(
    session: AsyncSession,
    dataset_key: str,
    start_date: date,
    end_date: date,
    location_id: Optional[str] = None
) -> None:
    """
    Register NOAA dataset in dataset_registry.
    
    Args:
        session: Database session
        dataset_key: Key in NOAA_DATASETS dictionary
        start_date: Start date of data
        end_date: End date of data
        location_id: Optional location filter applied
    """
    dataset = NOAA_DATASETS.get(dataset_key)
    if not dataset:
        raise ValueError(f"Unknown dataset: {dataset_key}")
    
    # Check if already registered
    existing = await session.execute(
        text("""
            SELECT id FROM dataset_registry 
            WHERE source = 'noaa' AND dataset_id = :dataset_id
        """),
        {"dataset_id": dataset_key}
    )
    
    if existing.fetchone():
        logger.info(f"Dataset {dataset_key} already registered")
        return
    
    # Register new dataset
    metadata = {
        "dataset_id": dataset.dataset_id,
        "name": dataset.name,
        "description": dataset.description,
        "data_types": dataset.data_types,
        "update_frequency": dataset.update_frequency,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "location_filter": location_id
    }
    
    await session.execute(
        text("""
            INSERT INTO dataset_registry (source, dataset_id, table_name, metadata)
            VALUES (:source, :dataset_id, :table_name, :metadata)
        """),
        {
            "source": "noaa",
            "dataset_id": dataset_key,
            "table_name": dataset.table_name,
            "metadata": str(metadata)
        }
    )
    await session.commit()
    
    logger.info(f"Registered dataset: {dataset_key}")


async def ingest_noaa_data(
    token: str,
    dataset_key: str,
    start_date: date,
    end_date: date,
    location_id: Optional[str] = None,
    station_id: Optional[str] = None,
    data_type_ids: Optional[List[str]] = None,
    max_results: Optional[int] = None,
    max_concurrency: int = 3,
    requests_per_second: float = 4.0
) -> Dict[str, Any]:
    """
    Ingest NOAA climate/weather data.
    
    This function:
    1. Creates an ingestion_job record
    2. Creates the target table if needed
    3. Fetches data from NOAA CDO API
    4. Inserts data into PostgreSQL
    5. Updates job status
    
    Args:
        token: NOAA CDO API token
        dataset_key: Key in NOAA_DATASETS dictionary
        start_date: Start date for data
        end_date: End date for data
        location_id: Optional location filter (e.g., "FIPS:06" for California)
        station_id: Optional station filter
        data_type_ids: Optional list of data types to fetch
        max_results: Optional maximum total results
        max_concurrency: Maximum concurrent API requests
        requests_per_second: Rate limit (default 4 to stay under 5 req/sec)
        
    Returns:
        Dictionary with ingestion results and job info
    """
    dataset = NOAA_DATASETS.get(dataset_key)
    if not dataset:
        raise ValueError(f"Unknown dataset: {dataset_key}")
    
    # Use dataset's default data types if not specified
    if not data_type_ids:
        data_type_ids = dataset.data_types
    
    # Create database session
    async for session in get_async_session():
        try:
            # Step 1: Create ingestion job
            job_id = await create_ingestion_job(
                session=session,
                dataset_key=dataset_key,
                start_date=start_date,
                end_date=end_date,
                location_id=location_id
            )
            
            logger.info(f"Started ingestion job {job_id} for {dataset_key}")
            
            # Step 2: Create table if needed
            await create_noaa_table(session, dataset_key)
            
            # Step 3: Register dataset
            await register_noaa_dataset(
                session=session,
                dataset_key=dataset_key,
                start_date=start_date,
                end_date=end_date,
                location_id=location_id
            )
            
            # Step 4: Fetch data from NOAA API
            client = NOAAClient(
                token=token,
                max_concurrency=max_concurrency,
                requests_per_second=requests_per_second
            )
            
            try:
                logger.info(
                    f"Fetching data from NOAA: dataset={dataset.dataset_id}, "
                    f"dates={start_date} to {end_date}, types={data_type_ids}"
                )
                
                data = await client.get_all_data_paginated(
                    dataset_id=dataset.dataset_id,
                    start_date=start_date,
                    end_date=end_date,
                    data_type_ids=data_type_ids,
                    location_id=location_id,
                    station_id=station_id,
                    max_results=max_results
                )
                
                logger.info(f"Fetched {len(data)} records from NOAA API")
                
                # Step 5: Insert data into database
                if data:
                    rows_inserted = await insert_noaa_data(
                        session=session,
                        table_name=dataset.table_name,
                        data=data
                    )
                    
                    logger.info(f"Inserted {rows_inserted} rows into {dataset.table_name}")
                else:
                    rows_inserted = 0
                    logger.warning("No data returned from NOAA API")
                
                # Step 6: Update job status to success
                await update_job_status(
                    session=session,
                    job_id=job_id,
                    status="success",
                    rows_inserted=rows_inserted
                )
                
                return {
                    "job_id": job_id,
                    "status": "success",
                    "dataset_key": dataset_key,
                    "rows_fetched": len(data),
                    "rows_inserted": rows_inserted,
                    "table_name": dataset.table_name
                }
                
            except Exception as e:
                # Update job status to failed
                await update_job_status(
                    session=session,
                    job_id=job_id,
                    status="failed",
                    error_message=str(e)
                )
                logger.error(f"Ingestion failed: {e}")
                raise
                
            finally:
                await client.close()
                
        except Exception as e:
            logger.error(f"Ingestion error: {e}")
            raise


async def create_ingestion_job(
    session: AsyncSession,
    dataset_key: str,
    start_date: date,
    end_date: date,
    location_id: Optional[str]
) -> int:
    """
    Create an ingestion_jobs record.
    
    Args:
        session: Database session
        dataset_key: Dataset identifier
        start_date: Start date
        end_date: End date
        location_id: Optional location filter
        
    Returns:
        Job ID
    """
    config = {
        "dataset_key": dataset_key,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "location_id": location_id
    }
    
    result = await session.execute(
        text("""
            INSERT INTO ingestion_jobs (source, dataset_id, status, config)
            VALUES (:source, :dataset_id, :status, :config)
            RETURNING id
        """),
        {
            "source": "noaa",
            "dataset_id": dataset_key,
            "status": "running",
            "config": str(config)
        }
    )
    await session.commit()
    
    job_id = result.fetchone()[0]
    return job_id


async def update_job_status(
    session: AsyncSession,
    job_id: int,
    status: str,
    rows_inserted: int = 0,
    error_message: Optional[str] = None
) -> None:
    """
    Update ingestion_jobs status.
    
    Args:
        session: Database session
        job_id: Job ID
        status: New status (success, failed)
        rows_inserted: Number of rows inserted
        error_message: Optional error message
    """
    result = {
        "rows_inserted": rows_inserted
    }
    if error_message:
        result["error"] = error_message
    
    await session.execute(
        text("""
            UPDATE ingestion_jobs
            SET status = :status,
                result = :result,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = :job_id
        """),
        {
            "job_id": job_id,
            "status": status,
            "result": str(result)
        }
    )
    await session.commit()


async def insert_noaa_data(
    session: AsyncSession,
    table_name: str,
    data: List[Dict[str, Any]]
) -> int:
    """
    Insert NOAA data into database table.
    
    Uses parameterized queries for SQL safety.
    Handles conflicts by updating existing records.
    
    Args:
        session: Database session
        table_name: Target table name
        data: List of data records from NOAA API
        
    Returns:
        Number of rows inserted/updated
    """
    if not data:
        return 0
    
    # Build INSERT statement with ON CONFLICT handling
    insert_sql = f"""
        INSERT INTO {table_name} (
            date, datatype, station, value, attributes,
            location_id, location_name, latitude, longitude, elevation
        ) VALUES (
            :date, :datatype, :station, :value, :attributes,
            :location_id, :location_name, :latitude, :longitude, :elevation
        )
        ON CONFLICT (date, datatype, station)
        DO UPDATE SET
            value = EXCLUDED.value,
            attributes = EXCLUDED.attributes,
            ingestion_timestamp = CURRENT_TIMESTAMP
    """
    
    # Transform NOAA API data to database records
    records = []
    for item in data:
        record = {
            "date": item.get("date", "").split("T")[0],  # Extract date part
            "datatype": item.get("datatype", ""),
            "station": item.get("station", ""),
            "value": item.get("value"),
            "attributes": item.get("attributes"),
            "location_id": None,  # May not be in response
            "location_name": None,
            "latitude": None,
            "longitude": None,
            "elevation": None
        }
        records.append(record)
    
    # Batch insert with parameterized queries
    await session.execute(text(insert_sql), records)
    await session.commit()
    
    return len(records)


async def ingest_noaa_by_chunks(
    token: str,
    dataset_key: str,
    start_date: date,
    end_date: date,
    chunk_days: int = 30,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Ingest NOAA data in date-range chunks to handle large requests.
    
    NOAA CDO API limits results per request, so for large date ranges,
    we break into smaller chunks.
    
    Args:
        token: NOAA CDO API token
        dataset_key: Key in NOAA_DATASETS dictionary
        start_date: Start date
        end_date: End date
        chunk_days: Days per chunk (default 30)
        **kwargs: Additional arguments passed to ingest_noaa_data
        
    Returns:
        List of ingestion results for each chunk
    """
    results = []
    current_date = start_date
    
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_days - 1), end_date)
        
        logger.info(f"Ingesting chunk: {current_date} to {chunk_end}")
        
        result = await ingest_noaa_data(
            token=token,
            dataset_key=dataset_key,
            start_date=current_date,
            end_date=chunk_end,
            **kwargs
        )
        
        results.append(result)
        current_date = chunk_end + timedelta(days=1)
    
    return results














