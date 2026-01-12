"""
FBI Crime Data ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.fbi_crime.client import FBICrimeClient
from app.sources.fbi_crime import metadata

logger = logging.getLogger(__name__)


# ============================================
# Table Preparation Functions
# ============================================

async def prepare_estimates_table(
    db: Session,
    scope: str = "national"
) -> Dict[str, Any]:
    """
    Prepare database table for FBI crime estimates.
    
    Args:
        db: Database session
        scope: Data scope (national, state, regional)
        
    Returns:
        Dictionary with table info
    """
    table_name = metadata.generate_table_name("estimates", scope)
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_estimates_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    # Register in dataset_registry
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id=f"fbi_crime_estimates_{scope}",
        table_name=table_name,
        display_name=f"FBI Crime Estimates ({scope.title()})",
        description=metadata.get_dataset_description("estimates"),
        metadata_extra={"scope": scope, "dataset_type": "estimates"}
    )
    
    return {"table_name": table_name}


async def prepare_summarized_table(db: Session) -> Dict[str, Any]:
    """Prepare database table for FBI summarized crime data."""
    table_name = metadata.generate_table_name("summarized", "agency")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_summarized_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id="fbi_crime_summarized",
        table_name=table_name,
        display_name="FBI Summarized Crime Data",
        description=metadata.get_dataset_description("summarized"),
        metadata_extra={"dataset_type": "summarized"}
    )
    
    return {"table_name": table_name}


async def prepare_nibrs_table(db: Session) -> Dict[str, Any]:
    """Prepare database table for NIBRS data."""
    table_name = metadata.generate_table_name("nibrs", "state")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_nibrs_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id="fbi_crime_nibrs",
        table_name=table_name,
        display_name="FBI NIBRS Data",
        description=metadata.get_dataset_description("nibrs"),
        metadata_extra={"dataset_type": "nibrs"}
    )
    
    return {"table_name": table_name}


async def prepare_hate_crime_table(db: Session) -> Dict[str, Any]:
    """Prepare database table for hate crime statistics."""
    table_name = metadata.generate_table_name("hate_crime", "national")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_hate_crime_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id="fbi_crime_hate_crime",
        table_name=table_name,
        display_name="FBI Hate Crime Statistics",
        description=metadata.get_dataset_description("hate_crime"),
        metadata_extra={"dataset_type": "hate_crime"}
    )
    
    return {"table_name": table_name}


async def prepare_leoka_table(db: Session) -> Dict[str, Any]:
    """Prepare database table for LEOKA data."""
    table_name = metadata.generate_table_name("leoka", "national")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_leoka_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id="fbi_crime_leoka",
        table_name=table_name,
        display_name="FBI LEOKA Data",
        description=metadata.get_dataset_description("leoka"),
        metadata_extra={"dataset_type": "leoka"}
    )
    
    return {"table_name": table_name}


async def prepare_participation_table(db: Session) -> Dict[str, Any]:
    """Prepare database table for participation data."""
    table_name = metadata.generate_table_name("participation", "national")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_participation_table_sql(table_name)
    
    db.execute(text(create_sql))
    db.commit()
    
    await _register_dataset(
        db=db,
        source="fbi_crime",
        dataset_id="fbi_crime_participation",
        table_name=table_name,
        display_name="FBI Agency Participation",
        description=metadata.get_dataset_description("participation"),
        metadata_extra={"dataset_type": "participation"}
    )
    
    return {"table_name": table_name}


# ============================================
# Main Ingestion Functions
# ============================================

async def ingest_fbi_crime_estimates(
    db: Session,
    job_id: int,
    scope: str = "national",
    offenses: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FBI crime estimates data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        scope: Data scope (national or state)
        offenses: List of offense types (uses all if not provided)
        states: List of state abbreviations for state-level data
        api_key: FBI Crime API key
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    if not api_key:
        api_key = settings.get_fbi_crime_api_key()
    
    if not api_key:
        raise ValueError(
            "FBI Crime API key is required. "
            "Set FBI_CRIME_API_KEY environment variable. "
            "Get a free key at: https://api.data.gov/signup/"
        )
    
    client = FBICrimeClient(
        api_key=api_key,
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
        
        # Use all offenses if not specified
        if not offenses:
            offenses = client.OFFENSE_TYPES
        
        # Prepare table
        table_info = await prepare_estimates_table(db, scope)
        table_name = table_info["table_name"]
        
        all_rows = []
        
        if scope == "national":
            # Fetch national estimates for all offenses
            logger.info(f"Fetching national estimates for {len(offenses)} offense types")
            
            for offense in offenses:
                try:
                    logger.info(f"Fetching national estimates for {offense}")
                    response = await client.get_national_estimates(offense)
                    parsed = metadata.parse_national_estimates(response, offense)
                    all_rows.extend(parsed)
                    logger.info(f"Parsed {len(parsed)} rows for {offense}")
                except Exception as e:
                    logger.error(f"Failed to fetch {offense}: {e}")
        
        elif scope == "state":
            # Fetch state estimates
            target_states = states or client.STATE_ABBRS
            logger.info(f"Fetching state estimates for {len(target_states)} states")
            
            for state in target_states:
                for offense in offenses:
                    try:
                        logger.info(f"Fetching {offense} estimates for {state}")
                        response = await client.get_state_estimates(state, offense)
                        parsed = metadata.parse_state_estimates(response, state, offense)
                        all_rows.extend(parsed)
                    except Exception as e:
                        logger.error(f"Failed to fetch {offense} for {state}: {e}")
        
        # Insert data
        rows_inserted = await _insert_estimates_data(db, table_name, all_rows)
        
        # Update job status
        if job:
            if rows_inserted == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(f"Job {job_id}: No FBI crime estimates data returned")
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "scope": scope,
            "offenses_fetched": len(offenses),
            "rows_inserted": rows_inserted
        }
    
    except Exception as e:
        logger.error(f"FBI Crime estimates ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_fbi_crime_summarized(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    offenses: Optional[List[str]] = None,
    since: int = 2010,
    until: int = 2023,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FBI summarized crime data by state.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        states: List of state abbreviations (defaults to all)
        offenses: List of offense types
        since: Start year
        until: End year
        api_key: FBI Crime API key
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    if not api_key:
        api_key = settings.get_fbi_crime_api_key()
    
    if not api_key:
        raise ValueError("FBI Crime API key is required")
    
    client = FBICrimeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not states:
            states = client.STATE_ABBRS
        if not offenses:
            offenses = ["violent-crime", "property-crime"]
        
        table_info = await prepare_summarized_table(db)
        table_name = table_info["table_name"]
        
        all_rows = []
        
        for state in states:
            for offense in offenses:
                try:
                    logger.info(f"Fetching summarized {offense} for {state} ({since}-{until})")
                    response = await client.get_summarized_data(state, offense, since, until)
                    parsed = metadata.parse_summarized_data(response, state, offense)
                    all_rows.extend(parsed)
                except Exception as e:
                    logger.error(f"Failed to fetch summarized data for {state}/{offense}: {e}")
        
        rows_inserted = await _insert_summarized_data(db, table_name, all_rows)
        
        if job:
            if rows_inserted == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(f"Job {job_id}: No FBI summarized crime data returned")
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "states_fetched": len(states),
            "offenses_fetched": len(offenses),
            "rows_inserted": rows_inserted
        }
    
    except Exception as e:
        logger.error(f"FBI Crime summarized ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_fbi_crime_nibrs(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    variables: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FBI NIBRS data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        states: List of state abbreviations
        variables: List of NIBRS variables
        api_key: FBI Crime API key
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    if not api_key:
        api_key = settings.get_fbi_crime_api_key()
    
    if not api_key:
        raise ValueError("FBI Crime API key is required")
    
    client = FBICrimeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        if not states:
            # Start with a subset of major states
            states = ["CA", "TX", "NY", "FL", "PA", "IL", "OH", "MI", "GA", "NC"]
        if not variables:
            variables = ["count", "offense"]
        
        table_info = await prepare_nibrs_table(db)
        table_name = table_info["table_name"]
        
        all_rows = []
        
        for state in states:
            for variable in variables:
                try:
                    logger.info(f"Fetching NIBRS {variable} for {state}")
                    response = await client.get_nibrs_offense_data(state, variable)
                    parsed = metadata.parse_nibrs_data(response, state, variable)
                    all_rows.extend(parsed)
                except Exception as e:
                    logger.error(f"Failed to fetch NIBRS for {state}/{variable}: {e}")
        
        rows_inserted = await _insert_nibrs_data(db, table_name, all_rows)
        
        if job:
            if rows_inserted == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(f"Job {job_id}: No FBI NIBRS data returned")
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "states_fetched": len(states),
            "variables_fetched": len(variables),
            "rows_inserted": rows_inserted
        }
    
    except Exception as e:
        logger.error(f"FBI Crime NIBRS ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_fbi_hate_crime(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FBI hate crime statistics.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        states: List of state abbreviations (None for national only)
        api_key: FBI Crime API key
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    if not api_key:
        api_key = settings.get_fbi_crime_api_key()
    
    if not api_key:
        raise ValueError("FBI Crime API key is required")
    
    client = FBICrimeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        table_info = await prepare_hate_crime_table(db)
        table_name = table_info["table_name"]
        
        all_rows = []
        
        # Always fetch national data
        try:
            logger.info("Fetching national hate crime data")
            response = await client.get_hate_crime_national()
            parsed = metadata.parse_hate_crime_data(response)
            all_rows.extend(parsed)
        except Exception as e:
            logger.error(f"Failed to fetch national hate crime data: {e}")
        
        # Fetch state-level data if requested
        if states:
            for state in states:
                try:
                    logger.info(f"Fetching hate crime data for {state}")
                    response = await client.get_hate_crime_by_state(state)
                    parsed = metadata.parse_hate_crime_data(response, state)
                    all_rows.extend(parsed)
                except Exception as e:
                    logger.error(f"Failed to fetch hate crime for {state}: {e}")
        
        rows_inserted = await _insert_hate_crime_data(db, table_name, all_rows)
        
        if job:
            if rows_inserted == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(f"Job {job_id}: No FBI hate crime data returned")
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "states_fetched": len(states) if states else 0,
            "rows_inserted": rows_inserted
        }

    except Exception as e:
        logger.error(f"FBI Hate Crime ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_fbi_leoka(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FBI LEOKA (Law Enforcement Officers Killed and Assaulted) data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        states: List of state abbreviations (None for national only)
        api_key: FBI Crime API key
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    if not api_key:
        api_key = settings.get_fbi_crime_api_key()
    
    if not api_key:
        raise ValueError("FBI Crime API key is required")
    
    client = FBICrimeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries
    )
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        table_info = await prepare_leoka_table(db)
        table_name = table_info["table_name"]
        
        all_rows = []
        
        # Fetch national data
        try:
            logger.info("Fetching national LEOKA data")
            response = await client.get_leoka_national()
            parsed = metadata.parse_leoka_data(response)
            all_rows.extend(parsed)
        except Exception as e:
            logger.error(f"Failed to fetch national LEOKA data: {e}")
        
        # Fetch state data if requested
        if states:
            for state in states:
                try:
                    logger.info(f"Fetching LEOKA data for {state}")
                    response = await client.get_leoka_by_state(state)
                    parsed = metadata.parse_leoka_data(response, state)
                    all_rows.extend(parsed)
                except Exception as e:
                    logger.error(f"Failed to fetch LEOKA for {state}: {e}")
        
        rows_inserted = await _insert_leoka_data(db, table_name, all_rows)
        
        if job:
            if rows_inserted == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(f"Job {job_id}: No FBI LEOKA data returned")
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "states_fetched": len(states) if states else 0,
            "rows_inserted": rows_inserted
        }

    except Exception as e:
        logger.error(f"FBI LEOKA ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_all_fbi_crime_data(
    db: Session,
    api_key: Optional[str] = None,
    include_states: bool = False
) -> Dict[str, Any]:
    """
    Ingest all available FBI crime data.
    
    This is a convenience function that runs all FBI crime ingestion jobs.
    
    Args:
        db: Database session
        api_key: FBI Crime API key
        include_states: Whether to include state-level data (more API calls)
        
    Returns:
        Dictionary with results for each dataset
    """
    results = {}
    
    datasets = [
        ("estimates_national", ingest_fbi_crime_estimates, {"scope": "national"}),
        ("hate_crime", ingest_fbi_hate_crime, {}),
        ("leoka", ingest_fbi_leoka, {}),
    ]
    
    if include_states:
        datasets.extend([
            ("estimates_state", ingest_fbi_crime_estimates, {"scope": "state"}),
            ("summarized", ingest_fbi_crime_summarized, {}),
            ("nibrs", ingest_fbi_crime_nibrs, {}),
        ])
    
    for dataset_name, ingest_func, kwargs in datasets:
        logger.info(f"Starting ingestion for FBI Crime {dataset_name}")
        
        # Create job
        job_config = {
            "source": "fbi_crime",
            "dataset": dataset_name,
            **kwargs
        }
        
        job = IngestionJob(
            source="fbi_crime",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        try:
            result = await ingest_func(
                db=db,
                job_id=job.id,
                api_key=api_key,
                **kwargs
            )
            
            results[dataset_name] = {
                "status": "success",
                "job_id": job.id,
                **result
            }
        
        except Exception as e:
            logger.error(f"Failed to ingest {dataset_name}: {e}")
            results[dataset_name] = {
                "status": "failed",
                "job_id": job.id,
                "error": str(e)
            }
    
    return results


# ============================================
# Data Insertion Helper Functions
# ============================================

async def _insert_estimates_data(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]]
) -> int:
    """Insert estimates data with upsert logic."""
    if not rows:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            year, state_abbr, state_name, region_name, offense,
            population, violent_crime, homicide, rape_legacy, rape_revised,
            robbery, aggravated_assault, property_crime, burglary, larceny,
            motor_vehicle_theft, arson, violent_crime_rate, homicide_rate,
            rape_legacy_rate, rape_revised_rate, robbery_rate, aggravated_assault_rate,
            property_crime_rate, burglary_rate, larceny_rate, motor_vehicle_theft_rate
        )
        VALUES (
            :year, :state_abbr, :state_name, :region_name, :offense,
            :population, :violent_crime, :homicide, :rape_legacy, :rape_revised,
            :robbery, :aggravated_assault, :property_crime, :burglary, :larceny,
            :motor_vehicle_theft, :arson, :violent_crime_rate, :homicide_rate,
            :rape_legacy_rate, :rape_revised_rate, :robbery_rate, :aggravated_assault_rate,
            :property_crime_rate, :burglary_rate, :larceny_rate, :motor_vehicle_theft_rate
        )
        ON CONFLICT (year, state_abbr, offense)
        DO UPDATE SET
            population = EXCLUDED.population,
            violent_crime = EXCLUDED.violent_crime,
            homicide = EXCLUDED.homicide,
            property_crime = EXCLUDED.property_crime,
            violent_crime_rate = EXCLUDED.violent_crime_rate,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if rows_inserted % 2000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(rows)} rows")
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted


async def _insert_summarized_data(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]]
) -> int:
    """Insert summarized data with upsert logic."""
    if not rows:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            year, month, state_abbr, ori, agency_name, offense,
            actual, cleared, cleared_18_under, data_year
        )
        VALUES (
            :year, :month, :state_abbr, :ori, :agency_name, :offense,
            :actual, :cleared, :cleared_18_under, :data_year
        )
        ON CONFLICT (year, month, state_abbr, ori, offense)
        DO UPDATE SET
            actual = EXCLUDED.actual,
            cleared = EXCLUDED.cleared,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted


async def _insert_nibrs_data(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]]
) -> int:
    """Insert NIBRS data with upsert logic."""
    if not rows:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            data_year, state_abbr, offense_code, offense_name, offense_category,
            variable_name, variable_value, count, victim_count, offender_count
        )
        VALUES (
            :data_year, :state_abbr, :offense_code, :offense_name, :offense_category,
            :variable_name, :variable_value, :count, :victim_count, :offender_count
        )
        ON CONFLICT (data_year, state_abbr, offense_code, variable_name, variable_value)
        DO UPDATE SET
            count = EXCLUDED.count,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted


async def _insert_hate_crime_data(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]]
) -> int:
    """Insert hate crime data with upsert logic."""
    if not rows:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            data_year, state_abbr, state_name, bias_motivation, offense_name,
            victim_type, incident_count, offense_count, victim_count
        )
        VALUES (
            :data_year, :state_abbr, :state_name, :bias_motivation, :offense_name,
            :victim_type, :incident_count, :offense_count, :victim_count
        )
        ON CONFLICT (data_year, state_abbr, bias_motivation, offense_name)
        DO UPDATE SET
            incident_count = EXCLUDED.incident_count,
            offense_count = EXCLUDED.offense_count,
            victim_count = EXCLUDED.victim_count,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted


async def _insert_leoka_data(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]]
) -> int:
    """Insert LEOKA data with upsert logic."""
    if not rows:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            data_year, state_abbr, state_name, feloniously_killed, accidentally_killed,
            assaulted, assaulted_weapon_firearm, assaulted_weapon_knife,
            assaulted_weapon_other, assaulted_weapon_hands, activity_type, activity_count
        )
        VALUES (
            :data_year, :state_abbr, :state_name, :feloniously_killed, :accidentally_killed,
            :assaulted, :assaulted_weapon_firearm, :assaulted_weapon_knife,
            :assaulted_weapon_other, :assaulted_weapon_hands, :activity_type, :activity_count
        )
        ON CONFLICT (data_year, state_abbr, activity_type)
        DO UPDATE SET
            feloniously_killed = EXCLUDED.feloniously_killed,
            accidentally_killed = EXCLUDED.accidentally_killed,
            assaulted = EXCLUDED.assaulted,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted


# ============================================
# Dataset Registration Helper
# ============================================

async def _register_dataset(
    db: Session,
    source: str,
    dataset_id: str,
    table_name: str,
    display_name: str,
    description: str,
    metadata_extra: Dict[str, Any]
) -> None:
    """Register or update a dataset in the registry."""
    existing = db.query(DatasetRegistry).filter(
        DatasetRegistry.table_name == table_name
    ).first()
    
    if existing:
        logger.info(f"Dataset {dataset_id} already registered")
        existing.last_updated_at = datetime.utcnow()
        existing.source_metadata = metadata_extra
        db.commit()
    else:
        dataset_entry = DatasetRegistry(
            source=source,
            dataset_id=dataset_id,
            table_name=table_name,
            display_name=display_name,
            description=description,
            source_metadata=metadata_extra
        )
        db.add(dataset_entry)
        db.commit()
        logger.info(f"Registered dataset {dataset_id}")
