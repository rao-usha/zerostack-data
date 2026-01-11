"""
IRS Statistics of Income ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for IRS SOI datasets.

All ingestion operations:
- Create/update job records in ingestion_jobs table
- Register datasets in dataset_registry
- Use bounded concurrency
- Support batch inserts for large files
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.irs_soi.client import IRSSOIClient, AVAILABLE_YEARS, DEFAULT_YEAR
from app.sources.irs_soi import metadata

logger = logging.getLogger(__name__)

# Batch size for inserts (prevents memory issues with large files)
BATCH_SIZE = 5000


async def prepare_table_for_soi_data(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for IRS SOI data ingestion.
    
    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        dataset: Dataset identifier (zip_income, county_income, migration, business_income)
        
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
        logger.info(f"Creating table {table_name} for IRS SOI {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"irs_soi_{dataset}"
        
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
                source="irs_soi",
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
        logger.error(f"Failed to prepare table for IRS SOI data: {e}")
        db.rollback()
        raise


async def ingest_zip_income_data(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Ingest IRS SOI individual income by ZIP code data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Tax year (defaults to most recent available)
        use_cache: Whether to use cached downloads
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    year = year or DEFAULT_YEAR
    
    # Validate year
    if year not in AVAILABLE_YEARS["zip_income"]:
        raise ValueError(
            f"ZIP income data not available for year {year}. "
            f"Available years: {AVAILABLE_YEARS['zip_income']}"
        )
    
    client = IRSSOIClient(
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
        
        logger.info(f"Ingesting IRS SOI ZIP income data for year {year}")
        
        # Prepare table
        table_info = await prepare_table_for_soi_data(db, "zip_income")
        table_name = table_info["table_name"]
        
        # Download and parse data
        df = await client.get_zip_income_data(year=year, use_cache=use_cache)
        
        # Parse records
        parsed_records = metadata.parse_zip_income_data(df, year)
        
        if not parsed_records:
            logger.warning(f"No ZIP income data to insert for year {year}")
            rows_inserted = 0
        else:
            # Insert data in batches
            rows_inserted = await _insert_records_batch(
                db, table_name, parsed_records, "zip_income"
            )
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "zip_income",
            "year": year,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"IRS SOI ZIP income ingestion failed: {e}", exc_info=True)
        
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


async def ingest_county_income_data(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Ingest IRS SOI individual income by county data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Tax year (defaults to most recent available)
        use_cache: Whether to use cached downloads
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    year = year or DEFAULT_YEAR
    
    if year not in AVAILABLE_YEARS["county_income"]:
        raise ValueError(
            f"County income data not available for year {year}. "
            f"Available years: {AVAILABLE_YEARS['county_income']}"
        )
    
    client = IRSSOIClient(
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
        
        logger.info(f"Ingesting IRS SOI county income data for year {year}")
        
        table_info = await prepare_table_for_soi_data(db, "county_income")
        table_name = table_info["table_name"]
        
        df = await client.get_county_income_data(year=year, use_cache=use_cache)
        parsed_records = metadata.parse_county_income_data(df, year)
        
        if not parsed_records:
            logger.warning(f"No county income data to insert for year {year}")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_records_batch(
                db, table_name, parsed_records, "county_income"
            )
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "county_income",
            "year": year,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"IRS SOI county income ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_migration_data(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    flow_type: str = "both",
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Ingest IRS SOI county-to-county migration data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Tax year (defaults to most recent available)
        flow_type: "inflow", "outflow", or "both"
        use_cache: Whether to use cached downloads
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    year = year or DEFAULT_YEAR
    
    if year not in AVAILABLE_YEARS["migration"]:
        raise ValueError(
            f"Migration data not available for year {year}. "
            f"Available years: {AVAILABLE_YEARS['migration']}"
        )
    
    client = IRSSOIClient(
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
        
        logger.info(f"Ingesting IRS SOI migration data for year {year}, flow_type={flow_type}")
        
        table_info = await prepare_table_for_soi_data(db, "migration")
        table_name = table_info["table_name"]
        
        total_rows_inserted = 0
        
        # Determine which flow types to process
        flow_types = ["inflow", "outflow"] if flow_type == "both" else [flow_type]
        
        for ft in flow_types:
            df = await client.get_migration_data(year=year, flow_type=ft, use_cache=use_cache)
            parsed_records = metadata.parse_migration_data(df, year, ft)
            
            if parsed_records:
                rows = await _insert_records_batch(
                    db, table_name, parsed_records, "migration"
                )
                total_rows_inserted += rows
                logger.info(f"Inserted {rows} {ft} migration records")
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "migration",
            "year": year,
            "flow_type": flow_type,
            "rows_inserted": total_rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"IRS SOI migration ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_business_income_data(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Ingest IRS SOI business income by ZIP code data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Tax year (defaults to most recent available)
        use_cache: Whether to use cached downloads
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    year = year or DEFAULT_YEAR
    
    if year not in AVAILABLE_YEARS["business_income"]:
        raise ValueError(
            f"Business income data not available for year {year}. "
            f"Available years: {AVAILABLE_YEARS['business_income']}"
        )
    
    client = IRSSOIClient(
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
        
        logger.info(f"Ingesting IRS SOI business income data for year {year}")
        
        table_info = await prepare_table_for_soi_data(db, "business_income")
        table_name = table_info["table_name"]
        
        df = await client.get_business_income_data(year=year, use_cache=use_cache)
        parsed_records = metadata.parse_business_income_data(df, year)
        
        if not parsed_records:
            logger.warning(f"No business income data to insert for year {year}")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_records_batch(
                db, table_name, parsed_records, "business_income"
            )
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "business_income",
            "year": year,
            "rows_inserted": rows_inserted,
        }
    
    except Exception as e:
        logger.error(f"IRS SOI business income ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_all_soi_data(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    Ingest all IRS SOI datasets for a given year.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Tax year (defaults to most recent available)
        use_cache: Whether to use cached downloads
        
    Returns:
        Dictionary with ingestion results for all datasets
    """
    year = year or DEFAULT_YEAR
    
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"Ingesting all IRS SOI datasets for year {year}")
        
        results = {}
        total_rows = 0
        errors = []
        
        # Create sub-jobs for each dataset
        datasets = [
            ("zip_income", ingest_zip_income_data),
            ("county_income", ingest_county_income_data),
            ("migration", ingest_migration_data),
            ("business_income", ingest_business_income_data),
        ]
        
        for dataset_name, ingest_func in datasets:
            try:
                # Create sub-job
                sub_job = IngestionJob(
                    source="irs_soi",
                    status=JobStatus.PENDING,
                    config={"dataset": dataset_name, "year": year, "parent_job_id": job_id}
                )
                db.add(sub_job)
                db.commit()
                db.refresh(sub_job)
                
                # Run ingestion
                if dataset_name == "migration":
                    result = await ingest_func(
                        db=db,
                        job_id=sub_job.id,
                        year=year,
                        flow_type="both",
                        use_cache=use_cache
                    )
                else:
                    result = await ingest_func(
                        db=db,
                        job_id=sub_job.id,
                        year=year,
                        use_cache=use_cache
                    )
                
                results[dataset_name] = {
                    "status": "success",
                    "rows_inserted": result["rows_inserted"],
                    "table_name": result["table_name"],
                }
                total_rows += result["rows_inserted"]
                
            except Exception as e:
                logger.error(f"Error ingesting {dataset_name}: {e}")
                results[dataset_name] = {
                    "status": "failed",
                    "error": str(e),
                }
                errors.append(f"{dataset_name}: {str(e)}")
        
        # Update parent job
        if job:
            if errors:
                job.status = JobStatus.FAILED
                job.error_message = "; ".join(errors)
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()
        
        return {
            "year": year,
            "total_rows_inserted": total_rows,
            "datasets": results,
            "errors": errors if errors else None,
        }
    
    except Exception as e:
        logger.error(f"IRS SOI all datasets ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise


async def _insert_records_batch(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]],
    dataset: str
) -> int:
    """
    Insert records in batches with upsert logic.
    
    Args:
        db: Database session
        table_name: Target table name
        records: Records to insert
        dataset: Dataset type for column selection
        
    Returns:
        Number of rows inserted/updated
    """
    if not records:
        return 0
    
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    # Get columns based on dataset type
    if dataset == "zip_income":
        columns = [
            "tax_year", "state_code", "state_abbr", "zip_code", "agi_class",
            "agi_class_label", "num_returns", "num_single_returns", "num_joint_returns",
            "num_head_household", "num_exemptions", "num_dependents",
            "total_agi", "total_wages", "total_dividends", "total_interest",
            "total_capital_gains", "total_business_income", "total_ira_distributions",
            "total_pensions", "total_social_security", "total_unemployment",
            "total_tax_liability", "total_amt", "total_earned_income_credit",
            "total_child_tax_credit", "avg_agi"
        ]
        conflict_cols = ["tax_year", "zip_code", "agi_class"]
    elif dataset == "county_income":
        columns = [
            "tax_year", "state_code", "state_abbr", "county_code", "county_name",
            "agi_class", "agi_class_label", "num_returns", "num_single_returns",
            "num_joint_returns", "num_head_household", "num_exemptions", "num_dependents",
            "total_agi", "total_wages", "total_dividends", "total_interest",
            "total_capital_gains", "total_business_income", "total_ira_distributions",
            "total_pensions", "total_social_security", "total_unemployment",
            "total_tax_liability", "avg_agi"
        ]
        conflict_cols = ["tax_year", "county_code", "agi_class"]
    elif dataset == "migration":
        columns = [
            "tax_year", "flow_type", "dest_state_code", "dest_state_abbr",
            "dest_county_code", "dest_county_name", "orig_state_code", "orig_state_abbr",
            "orig_county_code", "orig_county_name", "num_returns", "num_exemptions",
            "total_agi", "avg_agi"
        ]
        conflict_cols = [
            "tax_year", "flow_type", "dest_state_code", "dest_county_code",
            "orig_state_code", "orig_county_code"
        ]
    elif dataset == "business_income":
        columns = [
            "tax_year", "state_code", "state_abbr", "zip_code",
            "num_returns", "total_agi", "num_with_business_income", "total_business_income",
            "num_with_farm_income", "total_farm_income", "num_schedule_c",
            "total_schedule_c_income", "total_schedule_c_receipts",
            "num_partnership_income", "total_partnership_income",
            "num_rental_income", "total_rental_income",
            "num_with_se_tax", "total_se_tax"
        ]
        conflict_cols = ["tax_year", "zip_code"]
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
    
    # Build parameterized insert SQL
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    conflict_list = ", ".join([f'"{col}"' for col in conflict_cols])
    
    # Build UPDATE SET clause (exclude conflict columns)
    update_cols = [col for col in columns if col not in conflict_cols]
    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ({conflict_list})
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """
    
    # Execute in batches
    rows_inserted = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        # Prepare batch with only relevant columns
        batch_params = []
        for record in batch:
            params = {col: record.get(col) for col in columns}
            batch_params.append(params)
        
        # Execute batch
        for params in batch_params:
            db.execute(text(insert_sql), params)
        
        rows_inserted += len(batch)
        db.commit()
        
        if (i + BATCH_SIZE) % 10000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} rows")
    
    logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
    return rows_inserted
