"""
BEA ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for Bureau of Economic Analysis datasets.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.bea.client import BEAClient
from app.sources.bea import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_bea_data(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for BEA data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: Dataset identifier (nipa, regional, gdp_industry, international)
        
    Returns:
        Dictionary with table_name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for BEA {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"bea_{dataset}"
        
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
                source="bea",
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
        logger.error(f"Failed to prepare table for BEA data: {e}")
        raise


async def ingest_nipa_data(
    db: Session,
    job_id: int,
    table_name: str = "T10101",
    frequency: str = "A",
    year: Optional[str] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest BEA NIPA (National Income and Product Accounts) data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        table_name: NIPA table name (e.g., "T10101" for GDP)
        frequency: A (annual), Q (quarterly), M (monthly)
        year: Year(s) to retrieve - "ALL", single year, or comma-separated
        api_key: BEA API key (required)
        
    Returns:
        Dictionary with ingestion results
    """
    if not api_key:
        raise ValueError(
            "BEA_API_KEY is required for BEA operations. "
            "Get a free key at: https://apps.bea.gov/api/signup/"
        )
    
    settings = get_settings()
    
    client = BEAClient(
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
        
        # Set default year range if not provided
        if not year:
            year = metadata.get_default_year_range("nipa")
        
        logger.info(
            f"Ingesting BEA NIPA data: table={table_name}, "
            f"frequency={frequency}, year={year}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_bea_data(db, "nipa")
        db_table_name = table_info["table_name"]
        
        # Fetch data from BEA API
        api_response = await client.get_nipa_data(
            table_name=table_name,
            frequency=frequency,
            year=year
        )
        
        # Parse records
        parsed_records = metadata.parse_nipa_response(api_response, table_name)
        
        if not parsed_records:
            logger.warning("No NIPA data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_nipa_data(db, db_table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "nipa",
            "bea_table": table_name,
            "frequency": frequency,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"BEA NIPA ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_nipa_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert NIPA data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "table_name", "series_code", "line_number", "line_description",
        "time_period", "cl_unit", "unit_mult", "data_value", "notes"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("table_name", "series_code", "time_period")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (table_name, series_code, time_period)
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


async def ingest_regional_data(
    db: Session,
    job_id: int,
    table_name: str = "SAGDP2N",
    line_code: str = "1",
    geo_fips: str = "STATE",
    year: Optional[str] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest BEA Regional Economic Accounts data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        table_name: Regional table name (e.g., "SAGDP2N" for GDP by state)
        line_code: Line code for specific measure
        geo_fips: Geographic area - "STATE", "COUNTY", "MSA", or specific FIPS
        year: Year(s) to retrieve
        api_key: BEA API key (required)
        
    Returns:
        Dictionary with ingestion results
    """
    if not api_key:
        raise ValueError(
            "BEA_API_KEY is required for BEA operations. "
            "Get a free key at: https://apps.bea.gov/api/signup/"
        )
    
    settings = get_settings()
    
    client = BEAClient(
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
        
        if not year:
            year = metadata.get_default_year_range("regional")
        
        logger.info(
            f"Ingesting BEA Regional data: table={table_name}, "
            f"geo_fips={geo_fips}, line_code={line_code}, year={year}"
        )
        
        table_info = await prepare_table_for_bea_data(db, "regional")
        db_table_name = table_info["table_name"]
        
        api_response = await client.get_regional_data(
            table_name=table_name,
            line_code=line_code,
            geo_fips=geo_fips,
            year=year
        )
        
        parsed_records = metadata.parse_regional_response(api_response, table_name)
        
        if not parsed_records:
            logger.warning("No Regional data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_regional_data(db, db_table_name, parsed_records)
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "regional",
            "bea_table": table_name,
            "geo_fips": geo_fips,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"BEA Regional ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_regional_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert Regional data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "table_name", "geo_fips", "geo_name", "line_code", "line_description",
        "time_period", "cl_unit", "unit_mult", "data_value"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("table_name", "geo_fips", "line_code", "time_period")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (table_name, geo_fips, line_code, time_period)
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


async def ingest_gdp_by_industry_data(
    db: Session,
    job_id: int,
    table_id: str = "1",
    frequency: str = "A",
    year: Optional[str] = None,
    industry: str = "ALL",
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest BEA GDP by Industry data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        table_id: Table ID (1-15)
        frequency: A (annual), Q (quarterly)
        year: Year(s) to retrieve
        industry: Industry code or "ALL"
        api_key: BEA API key (required)
        
    Returns:
        Dictionary with ingestion results
    """
    if not api_key:
        raise ValueError(
            "BEA_API_KEY is required for BEA operations. "
            "Get a free key at: https://apps.bea.gov/api/signup/"
        )
    
    settings = get_settings()
    
    client = BEAClient(
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
        
        if not year:
            year = metadata.get_default_year_range("gdp_industry")
        
        logger.info(
            f"Ingesting BEA GDP by Industry: table_id={table_id}, "
            f"frequency={frequency}, industry={industry}, year={year}"
        )
        
        table_info = await prepare_table_for_bea_data(db, "gdp_industry")
        db_table_name = table_info["table_name"]
        
        api_response = await client.get_gdp_by_industry(
            table_id=table_id,
            frequency=frequency,
            year=year,
            industry=industry
        )
        
        parsed_records = metadata.parse_gdp_industry_response(api_response, table_id)
        
        if not parsed_records:
            logger.warning("No GDP by Industry data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_gdp_industry_data(db, db_table_name, parsed_records)
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "gdp_industry",
            "table_id": table_id,
            "frequency": frequency,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"BEA GDP by Industry ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_gdp_industry_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert GDP by Industry data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "table_id", "industry_id", "industry_description",
        "frequency", "time_period", "data_value", "notes"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("table_id", "industry_id", "time_period")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (table_id, industry_id, time_period)
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


async def ingest_international_data(
    db: Session,
    job_id: int,
    indicator: str = "BalGds",
    area_or_country: str = "AllCountries",
    frequency: str = "A",
    year: Optional[str] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Ingest BEA International Transactions data into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        indicator: Transaction indicator
        area_or_country: Geographic area or "AllCountries"
        frequency: A (annual), Q (quarterly)
        year: Year(s) to retrieve
        api_key: BEA API key (required)
        
    Returns:
        Dictionary with ingestion results
    """
    if not api_key:
        raise ValueError(
            "BEA_API_KEY is required for BEA operations. "
            "Get a free key at: https://apps.bea.gov/api/signup/"
        )
    
    settings = get_settings()
    
    client = BEAClient(
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
        
        if not year:
            year = "ALL"
        
        logger.info(
            f"Ingesting BEA International data: indicator={indicator}, "
            f"area={area_or_country}, frequency={frequency}, year={year}"
        )
        
        table_info = await prepare_table_for_bea_data(db, "international")
        db_table_name = table_info["table_name"]
        
        api_response = await client.get_international_transactions(
            indicator=indicator,
            area_or_country=area_or_country,
            frequency=frequency,
            year=year
        )
        
        parsed_records = metadata.parse_international_response(api_response, indicator)
        
        if not parsed_records:
            logger.warning("No International data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_international_data(db, db_table_name, parsed_records)
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "international",
            "indicator": indicator,
            "area_or_country": area_or_country,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"BEA International ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_international_data(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert International data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "indicator", "indicator_description", "area_or_country",
        "frequency", "time_period", "cl_unit", "unit_mult", "data_value"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("indicator", "area_or_country", "time_period")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (indicator, area_or_country, time_period)
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
