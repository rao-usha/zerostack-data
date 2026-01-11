"""
BLS ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
All ingestion operations are tracked via the ingestion_jobs table.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.bls.client import BLSClient, get_series_for_dataset, COMMON_SERIES
from app.sources.bls import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_dataset(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for BLS data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: BLS dataset name (ces, cps, jolts, cpi, ppi, oes)
        
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
        logger.info(f"Creating/ensuring table {table_name} for BLS {dataset.upper()}")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"bls_{dataset.lower()}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "dataset": dataset,
                "display_name": metadata.get_dataset_display_name(dataset),
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="bls",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_dataset_display_name(dataset),
                description=metadata.get_dataset_description(dataset),
                source_metadata={
                    "dataset": dataset,
                }
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {"table_name": table_name}
    
    except Exception as e:
        logger.error(f"Failed to prepare table for BLS {dataset}: {e}")
        raise


async def ingest_bls_series(
    db: Session,
    job_id: int,
    series_ids: List[str],
    start_year: int,
    end_year: int,
    dataset: str
) -> Dict[str, Any]:
    """
    Ingest BLS series data into Postgres.
    
    Steps:
    1. Validate parameters
    2. Prepare table
    3. Fetch data from BLS API
    4. Parse and insert data
    5. Update job status
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        series_ids: List of BLS series IDs
        start_year: Start year
        end_year: End year
        dataset: Dataset name (ces, cps, jolts, cpi, ppi, oes)
        
    Returns:
        Dictionary with ingestion results
        
    Raises:
        Exception: On ingestion errors
    """
    settings = get_settings()
    api_key = settings.get_bls_api_key()
    
    # Initialize BLS client
    client = BLSClient(
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
        
        # 1. Validate parameters
        metadata.validate_year_range(
            start_year, end_year, api_key_present=(api_key is not None)
        )
        
        logger.info(
            f"Ingesting BLS {dataset.upper()}: "
            f"{len(series_ids)} series, {start_year} to {end_year}"
        )
        
        # 2. Prepare table
        table_info = await prepare_table_for_dataset(db, dataset)
        table_name = table_info["table_name"]
        
        # 3. Fetch data from BLS API
        logger.info(f"Fetching {len(series_ids)} series from BLS API")
        
        all_parsed_data = await client.fetch_multiple_batches(
            series_ids=series_ids,
            start_year=start_year,
            end_year=end_year
        )
        
        # Parse the raw data
        parsed_data: Dict[str, List[Dict[str, Any]]] = {}
        for series_id, observations in all_parsed_data.items():
            parsed_obs = []
            for obs in observations:
                parsed = metadata.parse_bls_observation(obs, series_id)
                if parsed:
                    parsed_obs.append(parsed)
            parsed_data[series_id] = parsed_obs
            logger.debug(f"Parsed {len(parsed_obs)} observations for {series_id}")
        
        # 4. Build insert values
        rows = metadata.build_insert_values(parsed_data)
        
        if not rows:
            logger.warning("No data to insert")
            rows_inserted = 0
        else:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")
            
            # Build parameterized INSERT with ON CONFLICT (upsert)
            insert_sql = f"""
                INSERT INTO {table_name} 
                (series_id, series_title, year, period, period_name, value, footnote_codes)
                VALUES 
                (:series_id, :series_title, :year, :period, :period_name, :value, :footnote_codes)
                ON CONFLICT (series_id, year, period) 
                DO UPDATE SET
                    series_title = EXCLUDED.series_title,
                    period_name = EXCLUDED.period_name,
                    value = EXCLUDED.value,
                    footnote_codes = EXCLUDED.footnote_codes,
                    ingested_at = NOW()
            """
            
            # Execute in batches for efficiency
            batch_size = 1000
            rows_inserted = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                
                # Commit each batch
                db.commit()
                
                if rows_inserted % 5000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(rows)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # 5. Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        # Prepare series summary
        series_summary = {
            series_id: len(obs) for series_id, obs in parsed_data.items()
        }
        
        return {
            "table_name": table_name,
            "dataset": dataset,
            "series_count": len(series_ids),
            "rows_inserted": rows_inserted,
            "year_range": f"{start_year} to {end_year}",
            "series_summary": series_summary,
        }
    
    except Exception as e:
        logger.error(f"BLS ingestion failed: {e}", exc_info=True)
        
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


async def ingest_bls_dataset(
    db: Session,
    job_id: int,
    dataset: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    series_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Ingest all common series for a BLS dataset.
    
    Convenience function that fetches all standard series for a dataset.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        dataset: Dataset name (ces, cps, jolts, cpi, ppi)
        start_year: Optional start year (defaults based on API key)
        end_year: Optional end year (defaults to current year)
        series_ids: Optional specific series IDs (uses defaults if not provided)
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    api_key = settings.get_bls_api_key()
    
    # Set default year range
    if not start_year or not end_year:
        default_start, default_end = metadata.get_default_date_range(
            api_key_present=(api_key is not None)
        )
        start_year = start_year or default_start
        end_year = end_year or default_end
    
    # Get default series for dataset if not provided
    if not series_ids:
        series_ids = get_series_for_dataset(dataset)
        logger.info(f"Using {len(series_ids)} default series for {dataset.upper()}")
    
    return await ingest_bls_series(
        db=db,
        job_id=job_id,
        series_ids=series_ids,
        start_year=start_year,
        end_year=end_year,
        dataset=dataset
    )


async def ingest_all_bls_datasets(
    db: Session,
    datasets: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest multiple BLS datasets.
    
    Convenience function for ingesting multiple datasets at once.
    Creates a separate job for each dataset.
    
    Args:
        db: Database session
        datasets: List of dataset names (defaults to all)
        start_year: Optional start year
        end_year: Optional end year
        
    Returns:
        Dictionary with results for each dataset
    """
    if datasets is None:
        datasets = list(COMMON_SERIES.keys())
    
    results = {}
    
    for dataset in datasets:
        logger.info(f"Starting ingestion for BLS dataset: {dataset.upper()}")
        
        # Create job for this dataset
        job_config = {
            "source": "bls",
            "dataset": dataset,
            "start_year": start_year,
            "end_year": end_year,
        }
        
        job = IngestionJob(
            source="bls",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        try:
            result = await ingest_bls_dataset(
                db=db,
                job_id=job.id,
                dataset=dataset,
                start_year=start_year,
                end_year=end_year
            )
            
            results[dataset] = {
                "status": "success",
                "job_id": job.id,
                **result
            }
        
        except Exception as e:
            logger.error(f"Failed to ingest {dataset}: {e}")
            results[dataset] = {
                "status": "failed",
                "job_id": job.id,
                "error": str(e)
            }
    
    return results


# =============================================================================
# CONVENIENCE FUNCTIONS FOR SPECIFIC DATASETS
# =============================================================================

async def ingest_unemployment_data(
    db: Session,
    job_id: int,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest CPS unemployment and labor force data.
    
    Includes: unemployment rate, labor force participation, employment level, etc.
    """
    return await ingest_bls_dataset(
        db=db,
        job_id=job_id,
        dataset="cps",
        start_year=start_year,
        end_year=end_year
    )


async def ingest_employment_data(
    db: Session,
    job_id: int,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest CES employment data.
    
    Includes: total nonfarm, by industry, average hourly earnings, etc.
    """
    return await ingest_bls_dataset(
        db=db,
        job_id=job_id,
        dataset="ces",
        start_year=start_year,
        end_year=end_year
    )


async def ingest_cpi_data(
    db: Session,
    job_id: int,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest CPI inflation data.
    
    Includes: all items CPI, core CPI, category-specific indexes.
    """
    return await ingest_bls_dataset(
        db=db,
        job_id=job_id,
        dataset="cpi",
        start_year=start_year,
        end_year=end_year
    )


async def ingest_ppi_data(
    db: Session,
    job_id: int,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest PPI producer price data.
    
    Includes: final demand, intermediate demand, industry-specific indexes.
    """
    return await ingest_bls_dataset(
        db=db,
        job_id=job_id,
        dataset="ppi",
        start_year=start_year,
        end_year=end_year
    )


async def ingest_jolts_data(
    db: Session,
    job_id: int,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest JOLTS job openings and labor turnover data.
    
    Includes: job openings, hires, quits, layoffs, separations.
    """
    return await ingest_bls_dataset(
        db=db,
        job_id=job_id,
        dataset="jolts",
        start_year=start_year,
        end_year=end_year
    )
