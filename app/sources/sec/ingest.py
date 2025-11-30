"""
SEC EDGAR ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.sec.client import SECClient
from app.sources.sec import metadata
from app.sources.sec import xbrl_parser
from app.sources.sec.models import SECFinancialFact, SECIncomeStatement, SECBalanceSheet, SECCashFlowStatement

logger = logging.getLogger(__name__)


async def prepare_table_for_filing_type(
    db: Session,
    filing_type: str
) -> Dict[str, Any]:
    """
    Prepare database table for SEC filings.
    
    Steps:
    1. Generate table name based on filing type
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        filing_type: Filing type (e.g., "10-K", "10-Q", "8-K")
        
    Returns:
        Dictionary with table_name
        
    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(filing_type)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for {filing_type} filings")
        create_sql = metadata.generate_create_table_sql(table_name)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"sec_{filing_type.lower().replace('-', '').replace('/', '_')}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "filing_type": filing_type,
                "description": metadata.get_filing_type_description(filing_type)
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="sec",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=f"SEC {filing_type} Filings",
                description=metadata.get_filing_type_description(filing_type),
                source_metadata={
                    "filing_type": filing_type
                }
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {
            "table_name": table_name
        }
    
    except Exception as e:
        logger.error(f"Failed to prepare table for SEC filing type: {e}")
        raise


async def ingest_company_filings(
    db: Session,
    job_id: int,
    cik: str,
    filing_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Ingest SEC filings for a specific company into Postgres.
    
    Steps:
    1. Validate parameters
    2. Prepare tables for filing types
    3. Fetch data from SEC EDGAR API
    4. Parse and insert data
    5. Update job status
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        cik: Company CIK (Central Index Key)
        filing_types: Optional list of filing types (defaults to 10-K and 10-Q)
        start_date: Start date filter (optional)
        end_date: End date filter (optional)
        
    Returns:
        Dictionary with ingestion results
        
    Raises:
        Exception: On ingestion errors
    """
    settings = get_settings()
    
    # Initialize SEC client
    client = SECClient(
        max_concurrency=settings.max_concurrency,
        max_requests_per_second=8,  # SEC limit is 10/sec, we use 8 to be safe
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
        if not metadata.validate_cik(cik):
            raise ValueError(f"Invalid CIK: {cik}")
        
        cik = metadata.normalize_cik(cik)
        
        if not filing_types:
            filing_types = ["10-K", "10-Q"]  # Default to annual and quarterly reports
            logger.info(f"Using default filing types: {filing_types}")
        
        if not start_date or not end_date:
            default_start, default_end = metadata.get_default_date_range()
            start_date = start_date or default_start
            end_date = end_date or default_end
        
        logger.info(
            f"Ingesting SEC filings for CIK {cik}: "
            f"types={filing_types}, {start_date} to {end_date}"
        )
        
        # 2. Prepare tables for each filing type
        tables_prepared = {}
        for filing_type in filing_types:
            table_info = await prepare_table_for_filing_type(db, filing_type)
            tables_prepared[filing_type] = table_info["table_name"]
        
        # 3. Fetch data from SEC EDGAR API
        logger.info(f"Fetching submissions for CIK {cik}")
        
        api_response = await client.get_company_submissions(cik)
        
        # Parse company info
        company_info = metadata.parse_company_info(api_response)
        logger.info(
            f"Found company: {company_info['company_name']} "
            f"(ticker: {company_info.get('ticker', 'N/A')})"
        )
        
        # 4. Parse filings
        all_filings = metadata.parse_filings(
            api_response,
            filing_types=filing_types,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"Found {len(all_filings)} filings matching criteria")
        
        # Group filings by type
        filings_by_type: Dict[str, List[Dict[str, Any]]] = {}
        for filing in all_filings:
            ftype = filing["filing_type"]
            if ftype not in filings_by_type:
                filings_by_type[ftype] = []
            filings_by_type[ftype].append(filing)
        
        # 5. Insert data for each filing type
        total_rows_inserted = 0
        
        for filing_type, filings in filings_by_type.items():
            if not filings:
                continue
            
            table_name = tables_prepared.get(filing_type)
            if not table_name:
                logger.warning(f"No table prepared for {filing_type}, skipping")
                continue
            
            logger.info(f"Inserting {len(filings)} {filing_type} filings into {table_name}")
            
            rows = metadata.build_insert_values(filings)
            
            # Build parameterized INSERT with ON CONFLICT
            insert_sql = f"""
                INSERT INTO {table_name} 
                (cik, ticker, company_name, accession_number, filing_type, filing_date, 
                 report_date, primary_document, filing_url, interactive_data_url,
                 file_number, film_number, items)
                VALUES 
                (:cik, :ticker, :company_name, :accession_number, :filing_type, :filing_date,
                 :report_date, :primary_document, :filing_url, :interactive_data_url,
                 :file_number, :film_number, :items)
                ON CONFLICT (accession_number) 
                DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    company_name = EXCLUDED.company_name,
                    filing_date = EXCLUDED.filing_date,
                    report_date = EXCLUDED.report_date,
                    primary_document = EXCLUDED.primary_document,
                    filing_url = EXCLUDED.filing_url,
                    interactive_data_url = EXCLUDED.interactive_data_url,
                    file_number = EXCLUDED.file_number,
                    film_number = EXCLUDED.film_number,
                    items = EXCLUDED.items,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 100
            rows_inserted = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                
                # Commit each batch
                db.commit()
            
            logger.info(f"Inserted {rows_inserted} {filing_type} filings")
            total_rows_inserted += rows_inserted
        
        # 6. Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows_inserted
            db.commit()
        
        return {
            "cik": cik,
            "company_name": company_info["company_name"],
            "ticker": company_info.get("ticker"),
            "filing_types": list(filings_by_type.keys()),
            "rows_inserted": total_rows_inserted,
            "date_range": f"{start_date} to {end_date}"
        }
    
    except Exception as e:
        logger.error(f"SEC ingestion failed: {e}", exc_info=True)
        
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


async def ingest_multiple_companies(
    db: Session,
    ciks: List[str],
    filing_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Ingest SEC filings for multiple companies.
    
    This is a convenience function for ingesting multiple companies
    at once. Each company gets its own job.
    
    Args:
        db: Database session
        ciks: List of CIK numbers
        filing_types: Optional list of filing types
        start_date: Start date filter (optional)
        end_date: End date filter (optional)
        
    Returns:
        Dictionary with results for each CIK
    """
    results = {}
    
    for cik in ciks:
        logger.info(f"Starting ingestion for CIK: {cik}")
        
        # Normalize CIK
        cik_normalized = metadata.normalize_cik(cik)
        
        # Create job
        job_config = {
            "source": "sec",
            "cik": cik_normalized,
            "filing_types": filing_types or ["10-K", "10-Q"],
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }
        
        job = IngestionJob(
            source="sec",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        try:
            result = await ingest_company_filings(
                db=db,
                job_id=job.id,
                cik=cik_normalized,
                filing_types=filing_types,
                start_date=start_date,
                end_date=end_date
            )
            
            results[cik] = {
                "status": "success",
                "job_id": job.id,
                **result
            }
            
        except Exception as e:
            logger.error(f"Failed to ingest CIK {cik}: {e}")
            results[cik] = {
                "status": "failed",
                "job_id": job.id,
                "error": str(e)
            }
    
    return results

