"""
Data Commons ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for Google Data Commons datasets.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.data_commons.client import DataCommonsClient, STATISTICAL_VARIABLES, PLACE_DCIDS
from app.sources.data_commons import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_data_commons(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for Data Commons data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: Dataset identifier (observations, place_stats, time_series)
        
    Returns:
        Dictionary with table_name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for Data Commons {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"data_commons_{dataset}"
        
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
                source="data_commons",
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
        logger.error(f"Failed to prepare table for Data Commons data: {e}")
        raise


async def ingest_statistical_variable(
    db: Session,
    job_id: int,
    variable_dcid: str,
    places: List[str],
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest Data Commons statistical variable observations for specified places.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        variable_dcid: Statistical variable DCID (e.g., "Count_Person")
        places: List of place DCIDs (e.g., ["geoId/06", "geoId/48"])
        api_key: Optional API key for higher rate limits
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = DataCommonsClient(
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
        
        # Get variable display name
        variable_name = STATISTICAL_VARIABLES.get(variable_dcid, variable_dcid)
        
        logger.info(
            f"Ingesting Data Commons data: variable={variable_dcid}, "
            f"places={len(places)}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_data_commons(db, "observations")
        db_table_name = table_info["table_name"]
        
        # Fetch data from Data Commons API
        # Process in batches to avoid API limits
        batch_size = 50
        all_records = []
        
        for i in range(0, len(places), batch_size):
            batch_places = places[i:i + batch_size]
            
            api_response = await client.bulk_observations(
                variables=[variable_dcid],
                entities=batch_places
            )
            
            # Parse records
            parsed_records = metadata.parse_observation_response(
                api_response, variable_dcid, variable_name
            )
            all_records.extend(parsed_records)
        
        if not all_records:
            logger.warning("No Data Commons data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_observations(db, db_table_name, all_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "observations",
            "variable_dcid": variable_dcid,
            "variable_name": variable_name,
            "places_count": len(places),
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"Data Commons ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_place_statistics(
    db: Session,
    job_id: int,
    place_dcid: str,
    variables: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest multiple statistical variables for a single place.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        place_dcid: Place DCID (e.g., "geoId/06" for California)
        variables: List of variable DCIDs (defaults to common variables)
        api_key: Optional API key for higher rate limits
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    # Default to common variables
    if not variables:
        variables = list(STATISTICAL_VARIABLES.keys())[:20]  # Top 20
    
    client = DataCommonsClient(
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
            f"Ingesting Data Commons data: place={place_dcid}, "
            f"variables={len(variables)}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_data_commons(db, "observations")
        db_table_name = table_info["table_name"]
        
        # Fetch data for all variables
        # Process in batches to avoid API limits
        batch_size = 10
        all_records = []
        
        for i in range(0, len(variables), batch_size):
            batch_vars = variables[i:i + batch_size]
            
            api_response = await client.bulk_observations(
                variables=batch_vars,
                entities=[place_dcid]
            )
            
            # Parse records for each variable
            for var_dcid in batch_vars:
                var_name = STATISTICAL_VARIABLES.get(var_dcid, var_dcid)
                parsed = metadata.parse_observation_response(
                    api_response, var_dcid, var_name
                )
                all_records.extend(parsed)
        
        if not all_records:
            logger.warning("No Data Commons data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_observations(db, db_table_name, all_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "observations",
            "place_dcid": place_dcid,
            "variables_count": len(variables),
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"Data Commons place statistics ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_us_state_data(
    db: Session,
    job_id: int,
    variables: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest statistical data for all US states.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        variables: List of variable DCIDs (defaults to common variables)
        api_key: Optional API key for higher rate limits
        
    Returns:
        Dictionary with ingestion results
    """
    # Get all US states
    us_state_dcids = [
        "geoId/01",  # Alabama
        "geoId/02",  # Alaska
        "geoId/04",  # Arizona
        "geoId/05",  # Arkansas
        "geoId/06",  # California
        "geoId/08",  # Colorado
        "geoId/09",  # Connecticut
        "geoId/10",  # Delaware
        "geoId/11",  # District of Columbia
        "geoId/12",  # Florida
        "geoId/13",  # Georgia
        "geoId/15",  # Hawaii
        "geoId/16",  # Idaho
        "geoId/17",  # Illinois
        "geoId/18",  # Indiana
        "geoId/19",  # Iowa
        "geoId/20",  # Kansas
        "geoId/21",  # Kentucky
        "geoId/22",  # Louisiana
        "geoId/23",  # Maine
        "geoId/24",  # Maryland
        "geoId/25",  # Massachusetts
        "geoId/26",  # Michigan
        "geoId/27",  # Minnesota
        "geoId/28",  # Mississippi
        "geoId/29",  # Missouri
        "geoId/30",  # Montana
        "geoId/31",  # Nebraska
        "geoId/32",  # Nevada
        "geoId/33",  # New Hampshire
        "geoId/34",  # New Jersey
        "geoId/35",  # New Mexico
        "geoId/36",  # New York
        "geoId/37",  # North Carolina
        "geoId/38",  # North Dakota
        "geoId/39",  # Ohio
        "geoId/40",  # Oklahoma
        "geoId/41",  # Oregon
        "geoId/42",  # Pennsylvania
        "geoId/44",  # Rhode Island
        "geoId/45",  # South Carolina
        "geoId/46",  # South Dakota
        "geoId/47",  # Tennessee
        "geoId/48",  # Texas
        "geoId/49",  # Utah
        "geoId/50",  # Vermont
        "geoId/51",  # Virginia
        "geoId/53",  # Washington
        "geoId/54",  # West Virginia
        "geoId/55",  # Wisconsin
        "geoId/56",  # Wyoming
    ]
    
    # Default variables
    if not variables:
        variables = [
            "Count_Person",
            "Median_Income_Household",
            "UnemploymentRate_Person",
            "Median_Age_Person",
            "Count_Household",
        ]
    
    settings = get_settings()
    
    client = DataCommonsClient(
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
            f"Ingesting Data Commons US state data: "
            f"variables={len(variables)}, states={len(us_state_dcids)}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_data_commons(db, "observations")
        db_table_name = table_info["table_name"]
        
        all_records = []
        
        # Fetch data - batch by variables
        for var_dcid in variables:
            var_name = STATISTICAL_VARIABLES.get(var_dcid, var_dcid)
            
            api_response = await client.bulk_observations(
                variables=[var_dcid],
                entities=us_state_dcids
            )
            
            parsed = metadata.parse_observation_response(
                api_response, var_dcid, var_name
            )
            all_records.extend(parsed)
        
        if not all_records:
            logger.warning("No Data Commons data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_observations(db, db_table_name, all_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": db_table_name,
            "dataset": "observations",
            "states_count": len(us_state_dcids),
            "variables_count": len(variables),
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"Data Commons US state ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_observations(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert observation data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "variable_dcid", "variable_name", "entity_dcid", "entity_name",
        "observation_date", "observation_value", "unit",
        "measurement_method", "provenance_url"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("variable_dcid", "entity_dcid", "observation_date")
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (variable_dcid, entity_dcid, observation_date)
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
