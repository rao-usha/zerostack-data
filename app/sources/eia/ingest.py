"""
EIA ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.eia.client import EIAClient
from app.sources.eia import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_eia_data(
    db: Session,
    category: str,
    subcategory: Optional[str] = None
) -> Dict[str, Any]:
    """
    Prepare database table for EIA data ingestion.
    
    Steps:
    1. Generate table name based on category
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        category: Category name (e.g., "petroleum", "natural_gas", "electricity")
        subcategory: Optional subcategory (e.g., "consumption", "production")
        
    Returns:
        Dictionary with:
        - table_name: Generated Postgres table name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(category, subcategory)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for EIA {category} data")
        create_sql = metadata.generate_create_table_sql(table_name, category)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"eia_{category.lower()}"
        if subcategory:
            dataset_id += f"_{subcategory.lower()}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "category": category,
                "subcategory": subcategory
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="eia",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_category_display_name(
                    f"{category}_{subcategory}" if subcategory else category
                ),
                description=metadata.get_category_description(
                    f"{category}_{subcategory}" if subcategory else category
                ),
                source_metadata={
                    "category": category,
                    "subcategory": subcategory
                }
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {
            "table_name": table_name
        }
    
    except Exception as e:
        logger.error(f"Failed to prepare table for EIA data: {e}")
        raise


async def ingest_eia_petroleum_data(
    db: Session,
    job_id: int,
    subcategory: str = "consumption",
    route: Optional[str] = None,
    frequency: str = "annual",
    start: Optional[str] = None,
    end: Optional[str] = None,
    facets: Optional[Dict[str, str]] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest EIA petroleum data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        subcategory: Subcategory (e.g., "consumption", "production")
        route: Optional custom API route
        frequency: Data frequency (annual, monthly, weekly)
        start: Start date
        end: End date
        facets: Optional facet filters
        api_key: EIA API key (required)
        
    Returns:
        Dictionary with ingestion results
    """
    if not api_key:
        raise ValueError(
            "EIA_API_KEY is required for EIA operations. "
            "Get a free key at: https://www.eia.gov/opendata/register.php"
        )
    
    settings = get_settings()
    
    # Initialize EIA client
    client = EIAClient(
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
        
        # Set default date range if not provided
        if not start or not end:
            default_start, default_end = metadata.get_default_date_range(frequency)
            start = start or default_start
            end = end or default_end
        
        logger.info(
            f"Ingesting EIA petroleum {subcategory}: "
            f"frequency={frequency}, {start} to {end}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_eia_data(
            db, "petroleum", subcategory
        )
        table_name = table_info["table_name"]
        
        # Fetch data from EIA API (with pagination)
        logger.info(f"Fetching petroleum {subcategory} data from EIA API")
        
        all_parsed_data = []
        offset = 0
        batch_size = 5000  # EIA API max
        
        while True:
            api_response = await client.get_petroleum_data(
                route=route if route else f"pet/{subcategory}/a",
                frequency=frequency,
                start=start,
                end=end,
                facets=facets,
                offset=offset,
                length=batch_size
            )
            
            # Parse data
            parsed = metadata.parse_eia_response(api_response, "petroleum")
            all_parsed_data.extend(parsed)
            
            logger.info(f"Parsed {len(parsed)} records (offset={offset})")
            
            # Check if we got all data
            if len(parsed) < batch_size:
                break
            
            offset += batch_size
        
        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        
        if not rows:
            logger.warning("No data to insert")
            rows_inserted = 0
        else:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            # Build parameterized INSERT with ON CONFLICT
            # Dynamically build column list based on what's present
            columns = [
                "period", "value", "units", "series_id", "product", "process",
                "area_code", "area_name", "state_code", "sector", "frequency",
                "duoarea", "product_name", "process_name"
            ]
            
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)
            
            update_set = ", ".join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != "period"
            ])
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (period, COALESCE(series_id, ''), COALESCE(area_code, ''), COALESCE(product, ''))
                DO UPDATE SET
                    {update_set},
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
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": "petroleum",
            "subcategory": subcategory,
            "rows_inserted": rows_inserted,
            "date_range": f"{start} to {end}"
        }
    
    except Exception as e:
        logger.error(f"EIA petroleum ingestion failed: {e}", exc_info=True)
        
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


async def ingest_eia_natural_gas_data(
    db: Session,
    job_id: int,
    subcategory: str = "consumption",
    route: Optional[str] = None,
    frequency: str = "annual",
    start: Optional[str] = None,
    end: Optional[str] = None,
    facets: Optional[Dict[str, str]] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest EIA natural gas data into Postgres.
    
    Similar structure to petroleum ingestion.
    """
    if not api_key:
        raise ValueError(
            "EIA_API_KEY is required for EIA operations. "
            "Get a free key at: https://www.eia.gov/opendata/register.php"
        )
    
    settings = get_settings()
    client = EIAClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not start or not end:
            default_start, default_end = metadata.get_default_date_range(frequency)
            start = start or default_start
            end = end or default_end
        
        logger.info(
            f"Ingesting EIA natural gas {subcategory}: "
            f"frequency={frequency}, {start} to {end}"
        )
        
        table_info = await prepare_table_for_eia_data(
            db, "natural_gas", subcategory
        )
        table_name = table_info["table_name"]
        
        # Fetch data with pagination
        all_parsed_data = []
        offset = 0
        batch_size = 5000
        
        while True:
            api_response = await client.get_natural_gas_data(
                route=route if route else f"natural-gas/{subcategory}/sum/a",
                frequency=frequency,
                start=start,
                end=end,
                facets=facets,
                offset=offset,
                length=batch_size
            )
            
            parsed = metadata.parse_eia_response(api_response, "natural_gas")
            all_parsed_data.extend(parsed)
            
            logger.info(f"Parsed {len(parsed)} records (offset={offset})")
            
            if len(parsed) < batch_size:
                break
            
            offset += batch_size
        
        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        rows_inserted = 0
        
        if rows:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            columns = [
                "period", "value", "units", "series_id", "product", "process",
                "area_code", "area_name", "state_code", "sector", "frequency",
                "duoarea", "process_name"
            ]
            
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)
            update_set = ", ".join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != "period"
            ])
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (period, COALESCE(series_id, ''), COALESCE(area_code, ''), COALESCE(process, ''))
                DO UPDATE SET
                    {update_set},
                    ingested_at = NOW()
            """
            
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": "natural_gas",
            "subcategory": subcategory,
            "rows_inserted": rows_inserted,
            "date_range": f"{start} to {end}"
        }
    
    except Exception as e:
        logger.error(f"EIA natural gas ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_eia_electricity_data(
    db: Session,
    job_id: int,
    subcategory: str = "retail_sales",
    route: Optional[str] = None,
    frequency: str = "annual",
    start: Optional[str] = None,
    end: Optional[str] = None,
    facets: Optional[Dict[str, str]] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest EIA electricity data into Postgres.
    """
    if not api_key:
        raise ValueError(
            "EIA_API_KEY is required for EIA operations. "
            "Get a free key at: https://www.eia.gov/opendata/register.php"
        )
    
    settings = get_settings()
    client = EIAClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not start or not end:
            default_start, default_end = metadata.get_default_date_range(frequency)
            start = start or default_start
            end = end or default_end
        
        logger.info(
            f"Ingesting EIA electricity {subcategory}: "
            f"frequency={frequency}, {start} to {end}"
        )
        
        table_info = await prepare_table_for_eia_data(
            db, "electricity", subcategory
        )
        table_name = table_info["table_name"]
        
        # Fetch data with pagination
        all_parsed_data = []
        offset = 0
        batch_size = 5000
        
        while True:
            api_response = await client.get_electricity_data(
                route=route if route else f"electricity/{subcategory.replace('_', '-')}",
                frequency=frequency,
                start=start,
                end=end,
                facets=facets,
                offset=offset,
                length=batch_size
            )
            
            parsed = metadata.parse_eia_response(api_response, "electricity")
            all_parsed_data.extend(parsed)
            
            logger.info(f"Parsed {len(parsed)} records (offset={offset})")
            
            if len(parsed) < batch_size:
                break
            
            offset += batch_size
        
        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        rows_inserted = 0
        
        if rows:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            columns = [
                "period", "value", "units", "series_id", "sector",
                "area_code", "area_name", "state_code", "frequency",
                "sectorid", "sector_name", "stateid", "state_name"
            ]
            
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)
            update_set = ", ".join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != "period"
            ])
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (period, COALESCE(series_id, ''), COALESCE(state_code, ''), COALESCE(sector, ''))
                DO UPDATE SET
                    {update_set},
                    ingested_at = NOW()
            """
            
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": "electricity",
            "subcategory": subcategory,
            "rows_inserted": rows_inserted,
            "date_range": f"{start} to {end}"
        }
    
    except Exception as e:
        logger.error(f"EIA electricity ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_eia_retail_gas_prices(
    db: Session,
    job_id: int,
    frequency: str = "weekly",
    start: Optional[str] = None,
    end: Optional[str] = None,
    facets: Optional[Dict[str, str]] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest EIA retail gas prices into Postgres.
    """
    if not api_key:
        raise ValueError(
            "EIA_API_KEY is required for EIA operations. "
            "Get a free key at: https://www.eia.gov/opendata/register.php"
        )
    
    settings = get_settings()
    client = EIAClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not start or not end:
            default_start, default_end = metadata.get_default_date_range(frequency)
            start = start or default_start
            end = end or default_end
        
        logger.info(
            f"Ingesting EIA retail gas prices: "
            f"frequency={frequency}, {start} to {end}"
        )
        
        table_info = await prepare_table_for_eia_data(
            db, "retail_gas_prices"
        )
        table_name = table_info["table_name"]
        
        # Fetch data with pagination
        all_parsed_data = []
        offset = 0
        batch_size = 5000
        
        while True:
            api_response = await client.get_retail_gas_prices(
                frequency=frequency,
                start=start,
                end=end,
                facets=facets,
                offset=offset,
                length=batch_size
            )
            
            parsed = metadata.parse_eia_response(api_response, "retail_gas_prices")
            all_parsed_data.extend(parsed)
            
            logger.info(f"Parsed {len(parsed)} records (offset={offset})")
            
            if len(parsed) < batch_size:
                break
            
            offset += batch_size
        
        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        rows_inserted = 0
        
        if rows:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            columns = [
                "period", "value", "units", "series_id", "product",
                "area_code", "area_name", "frequency", "grade", "formulation"
            ]
            
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)
            update_set = ", ".join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != "period"
            ])
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (period, COALESCE(series_id, ''), COALESCE(area_code, ''), COALESCE(product, ''))
                DO UPDATE SET
                    {update_set},
                    ingested_at = NOW()
            """
            
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": "retail_gas_prices",
            "rows_inserted": rows_inserted,
            "date_range": f"{start} to {end}"
        }
    
    except Exception as e:
        logger.error(f"EIA retail gas prices ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_eia_steo_projections(
    db: Session,
    job_id: int,
    frequency: str = "monthly",
    start: Optional[str] = None,
    end: Optional[str] = None,
    facets: Optional[Dict[str, str]] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest EIA STEO (Short-Term Energy Outlook) projections into Postgres.
    """
    if not api_key:
        raise ValueError(
            "EIA_API_KEY is required for EIA operations. "
            "Get a free key at: https://www.eia.gov/opendata/register.php"
        )
    
    settings = get_settings()
    client = EIAClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not start or not end:
            default_start, default_end = metadata.get_default_date_range(frequency)
            start = start or default_start
            end = end or default_end
        
        logger.info(
            f"Ingesting EIA STEO projections: "
            f"frequency={frequency}, {start} to {end}"
        )
        
        table_info = await prepare_table_for_eia_data(
            db, "steo"
        )
        table_name = table_info["table_name"]
        
        # Fetch data with pagination
        all_parsed_data = []
        offset = 0
        batch_size = 5000
        
        while True:
            api_response = await client.get_steo_projections(
                frequency=frequency,
                start=start,
                end=end,
                facets=facets,
                offset=offset,
                length=batch_size
            )
            
            parsed = metadata.parse_eia_response(api_response, "steo")
            all_parsed_data.extend(parsed)
            
            logger.info(f"Parsed {len(parsed)} records (offset={offset})")
            
            if len(parsed) < batch_size:
                break
            
            offset += batch_size
        
        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        rows_inserted = 0
        
        if rows:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            columns = [
                "period", "value", "units", "series_id", "frequency",
                "series_name", "series_description"
            ]
            
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)
            update_set = ", ".join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != "period"
            ])
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (period, COALESCE(series_id, ''))
                DO UPDATE SET
                    {update_set},
                    ingested_at = NOW()
            """
            
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "category": "steo",
            "rows_inserted": rows_inserted,
            "date_range": f"{start} to {end}"
        }
    
    except Exception as e:
        logger.error(f"EIA STEO ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()

