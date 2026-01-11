"""
OpenFEMA ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for FEMA disaster and emergency management datasets.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.fema.client import FEMAClient
from app.sources.fema import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_fema_data(
    db: Session,
    dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for FEMA data ingestion.
    
    Args:
        db: Database session
        dataset: Dataset identifier
        
    Returns:
        Dictionary with table_name
    """
    try:
        table_name = metadata.generate_table_name(dataset)
        
        logger.info(f"Creating table {table_name} for FEMA {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)
        
        db.execute(text(create_sql))
        db.commit()
        
        # Register in dataset_registry
        dataset_id = f"fema_{dataset}"
        
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
                source="fema",
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
        logger.error(f"Failed to prepare table for FEMA data: {e}")
        raise


async def ingest_disaster_declarations(
    db: Session,
    job_id: int,
    state: Optional[str] = None,
    year: Optional[int] = None,
    disaster_type: Optional[str] = None,
    max_records: int = 50000
) -> Dict[str, Any]:
    """
    Ingest FEMA disaster declarations into Postgres.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        state: Filter by state code (e.g., "TX")
        year: Filter by fiscal year
        disaster_type: Filter by type (DR, EM, FM)
        max_records: Maximum records to fetch
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FEMAClient(
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
        
        logger.info(
            f"Ingesting FEMA disaster declarations: "
            f"state={state}, year={year}, type={disaster_type}"
        )
        
        # Prepare table
        table_info = await prepare_table_for_fema_data(db, "disaster_declarations")
        table_name = table_info["table_name"]
        
        # Fetch all disaster declarations with pagination
        records = await client.get_all_disaster_declarations(
            state=state,
            year=year,
            disaster_type=disaster_type,
            max_records=max_records
        )
        
        # Parse records
        parsed_records = metadata.parse_disaster_declarations(records)
        
        if not parsed_records:
            logger.warning("No disaster declaration data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_disaster_declarations(db, table_name, parsed_records)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "disaster_declarations",
            "rows_inserted": rows_inserted,
            "filters": {
                "state": state,
                "year": year,
                "disaster_type": disaster_type
            }
        }
    
    except Exception as e:
        logger.error(f"FEMA disaster declarations ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_disaster_declarations(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert disaster declaration data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "disaster_number", "declaration_type", "declaration_date", "fy_declared",
        "incident_type", "declaration_title", "state", "state_name", "county",
        "fips_state_code", "fips_county_code", "designated_area",
        "incident_begin_date", "incident_end_date",
        "ih_program_declared", "ia_program_declared", "pa_program_declared",
        "hm_program_declared", "place_code", "region"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("disaster_number", "state", "designated_area")
    ])
    
    # Use ON CONFLICT on the unique index expression
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (disaster_number, state, COALESCE(designated_area, ''))
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
        
        if (i + batch_size) % 5000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} rows")
    
    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_public_assistance_projects(
    db: Session,
    job_id: int,
    state: Optional[str] = None,
    disaster_number: Optional[int] = None,
    max_records: int = 50000
) -> Dict[str, Any]:
    """
    Ingest FEMA Public Assistance funded projects.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        state: Filter by state code
        disaster_number: Filter by specific disaster
        max_records: Maximum records to fetch
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FEMAClient(
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
        
        logger.info(
            f"Ingesting FEMA PA projects: state={state}, disaster={disaster_number}"
        )
        
        table_info = await prepare_table_for_fema_data(db, "pa_projects")
        table_name = table_info["table_name"]
        
        # Fetch with pagination
        all_records = []
        skip = 0
        top = 1000
        
        while len(all_records) < max_records:
            response = await client.get_public_assistance_projects(
                skip=skip,
                top=top,
                state=state,
                disaster_number=disaster_number
            )
            
            records = response.get("PublicAssistanceFundedProjectsDetails", [])
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} PA records (total: {len(all_records)})")
            
            if len(records) < top:
                break
            skip += top
        
        parsed_records = metadata.parse_pa_projects(all_records)
        
        if not parsed_records:
            logger.warning("No PA project data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_pa_projects(db, table_name, parsed_records)
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "pa_projects",
            "rows_inserted": rows_inserted,
            "filters": {"state": state, "disaster_number": disaster_number}
        }
    
    except Exception as e:
        logger.error(f"FEMA PA projects ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_pa_projects(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert PA project data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "disaster_number", "project_number", "state", "county", "applicant_id",
        "damage_category", "project_size", "project_title",
        "total_obligated", "federal_share_obligated", "project_amount", "obligation_date"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col not in ("disaster_number", "project_number", "state")
    ])
    
    # Use ON CONFLICT on the unique index expression
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (disaster_number, COALESCE(project_number, ''), state)
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


async def ingest_hazard_mitigation_projects(
    db: Session,
    job_id: int,
    state: Optional[str] = None,
    program_area: Optional[str] = None,
    max_records: int = 50000
) -> Dict[str, Any]:
    """
    Ingest FEMA Hazard Mitigation Assistance projects.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        state: Filter by state code
        program_area: Filter by program (HMGP, PDM, FMA, RFC)
        max_records: Maximum records to fetch
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FEMAClient(
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
        
        logger.info(
            f"Ingesting FEMA HMA projects: state={state}, program={program_area}"
        )
        
        table_info = await prepare_table_for_fema_data(db, "hma_projects")
        table_name = table_info["table_name"]
        
        # Fetch with pagination
        all_records = []
        skip = 0
        top = 1000
        
        while len(all_records) < max_records:
            response = await client.get_hazard_mitigation_projects(
                skip=skip,
                top=top,
                state=state,
                program_area=program_area
            )
            
            records = response.get("HazardMitigationAssistanceProjects", [])
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} HMA records (total: {len(all_records)})")
            
            if len(records) < top:
                break
            skip += top
        
        parsed_records = metadata.parse_hma_projects(all_records)
        
        if not parsed_records:
            logger.warning("No HMA project data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_hma_projects(db, table_name, parsed_records)
        
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "dataset": "hma_projects",
            "rows_inserted": rows_inserted,
            "filters": {"state": state, "program_area": program_area}
        }
    
    except Exception as e:
        logger.error(f"FEMA HMA projects ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def _insert_hma_projects(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]]
) -> int:
    """Insert HMA project data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")
    
    columns = [
        "project_identifier", "disaster_number", "state", "state_code", "county",
        "county_code", "region", "program_area", "program_fy", "project_type",
        "status", "recipient", "subrecipient", "project_amount",
        "federal_share_obligated", "cost_share_percentage", "benefit_cost_ratio",
        "date_approved", "date_closed"
    ]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join([
        f"{col} = EXCLUDED.{col}" for col in columns
        if col != "project_identifier"
    ])
    
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (project_identifier)
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
