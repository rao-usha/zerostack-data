"""
Yelp Fusion ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for Yelp business data.

IMPORTANT: Yelp has strict daily API limits (500 calls/day for free tier).
Use sparingly and plan your data collection strategy accordingly.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.yelp.client import YelpClient, YELP_CATEGORIES
from app.sources.yelp import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_yelp_data(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for Yelp data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: Dataset identifier (businesses, categories, reviews)
        
    Returns:
        Dictionary with table_name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for Yelp {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"yelp_{dataset}"
        
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
                source="yelp",
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
        logger.error(f"Failed to prepare table for Yelp data: {e}")
        raise


async def ingest_businesses_by_location(
    db: Session,
    job_id: int,
    location: str,
    term: Optional[str] = None,
    categories: Optional[str] = None,
    limit: int = 50,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest Yelp business listings for a specific location.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        location: Location string (e.g., "San Francisco, CA")
        term: Optional search term (e.g., "restaurants")
        categories: Optional category filter (e.g., "restaurants,bars")
        limit: Maximum number of businesses to retrieve (max 50 per call)
        api_key: Yelp API key (required)
        
    Returns:
        Dictionary with ingestion results
        
    IMPORTANT: Each call uses 1 API call. Daily limit is 500 calls.
    """
    if not api_key:
        raise ValueError(
            "YELP_API_KEY is required for Yelp operations. "
            "Get a free key at: https://www.yelp.com/developers/v3/manage_app"
        )
    
    settings = get_settings()
    
    client = YelpClient(
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
        
        logger.info(
            f"Ingesting Yelp businesses: location={location}, "
            f"term={term}, categories={categories}, limit={limit}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_yelp_data(db, "businesses")
        db_table_name = table_info["table_name"]
        
        # Fetch data from Yelp API
        api_response = await client.search_businesses(
            location=location,
            term=term,
            categories=categories,
            limit=min(limit, 50)
        )
        
        # Parse records
        parsed_records = metadata.parse_business_search_response(
            api_response, location, term
        )
        
        if not parsed_records:
            logger.warning("No Yelp businesses to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_businesses(db, db_table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "businesses",
            "location": location,
            "term": term,
            "categories": categories,
            "total_found": api_response.get("total", 0),
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"Yelp businesses ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_business_categories(
    db: Session,
    job_id: int,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest all Yelp business categories.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        api_key: Yelp API key (required)
        
    Returns:
        Dictionary with ingestion results
        
    NOTE: This uses 1 API call.
    """
    if not api_key:
        raise ValueError(
            "YELP_API_KEY is required for Yelp operations. "
            "Get a free key at: https://www.yelp.com/developers/v3/manage_app"
        )
    
    settings = get_settings()
    
    client = YelpClient(
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
        
        logger.info("Ingesting Yelp business categories")
        
        # Prepare table
        table_info = await prepare_table_for_yelp_data(db, "categories")
        db_table_name = table_info["table_name"]
        
        # Fetch data from Yelp API
        api_response = await client.get_all_categories()
        
        # Parse records
        parsed_records = metadata.parse_categories_response(api_response)
        
        if not parsed_records:
            logger.warning("No Yelp categories to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_categories(db, db_table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "categories",
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"Yelp categories ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_multiple_locations(
    db: Session,
    job_id: int,
    locations: List[str],
    term: Optional[str] = None,
    categories: Optional[str] = None,
    limit_per_location: int = 20,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest Yelp business listings for multiple locations.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        locations: List of location strings
        term: Optional search term
        categories: Optional category filter
        limit_per_location: Maximum businesses per location (max 50)
        api_key: Yelp API key (required)
        
    Returns:
        Dictionary with ingestion results
        
    WARNING: Each location uses 1 API call.
    For n locations, this uses n API calls.
    Daily limit is 500 calls total.
    """
    if not api_key:
        raise ValueError(
            "YELP_API_KEY is required for Yelp operations. "
            "Get a free key at: https://www.yelp.com/developers/v3/manage_app"
        )
    
    # Warn about API usage
    if len(locations) > 50:
        logger.warning(
            f"Requesting {len(locations)} locations will use {len(locations)} API calls. "
            f"Daily limit is 500 calls."
        )
    
    settings = get_settings()
    
    client = YelpClient(
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
        
        logger.info(
            f"Ingesting Yelp businesses for {len(locations)} locations "
            f"(term={term}, categories={categories})"
        )
        
        # Prepare table
        table_info = await prepare_table_for_yelp_data(db, "businesses")
        db_table_name = table_info["table_name"]
        
        # Fetch data for all locations
        all_records = []
        locations_processed = 0
        errors = []
        
        for location in locations:
            try:
                api_response = await client.search_businesses(
                    location=location,
                    term=term,
                    categories=categories,
                    limit=min(limit_per_location, 50)
                )
                
                parsed = metadata.parse_business_search_response(
                    api_response, location, term
                )
                all_records.extend(parsed)
                locations_processed += 1
                
            except Exception as e:
                logger.warning(f"Failed to fetch businesses for {location}: {e}")
                errors.append({"location": location, "error": str(e)})
        
        if not all_records:
            logger.warning("No Yelp businesses to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_businesses(db, db_table_name, all_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS if not errors else JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            if errors:
                job.error_message = f"Partial success. {len(errors)} locations failed."
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "businesses",
            "locations_requested": len(locations),
            "locations_processed": locations_processed,
            "term": term,
            "categories": categories,
            "rows_inserted": rows_inserted,
            "errors": errors if errors else None,
        }
    
    except Exception as e:
        logger.error(f"Yelp multi-location ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_businesses(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert business data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "yelp_id", "name", "alias", "image_url", "is_closed", "url",
        "review_count", "rating", "latitude", "longitude", "price",
        "phone", "display_phone", "distance",
        "address1", "address2", "address3", "city", "state", "zip_code", "country",
        "categories", "category_titles", "transactions",
        "search_location", "search_term"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col != "yelp_id"
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (yelp_id)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def _insert_categories(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert category data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "alias", "title", "parent_aliases",
        "country_whitelist", "country_blacklist"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col != "alias"
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (alias)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted
