"""
FDIC BankFind ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
Implements all 4 FDIC datasets:
- Bank Financials
- Institutions
- Failed Banks
- Summary of Deposits
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.fdic.client import FDICClient
from app.sources.fdic import metadata

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE PREPARATION
# =============================================================================

async def prepare_financials_table(db: Session) -> str:
    """
    Prepare the fdic_bank_financials table.
    
    Returns:
        Table name
    """
    table_name = metadata.generate_table_name("financials")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_financials_table_sql(table_name)
    db.execute(text(create_sql))
    db.commit()
    
    # Register dataset
    _register_dataset(db, "financials", table_name)
    
    return table_name


async def prepare_institutions_table(db: Session) -> str:
    """
    Prepare the fdic_institutions table.
    
    Returns:
        Table name
    """
    table_name = metadata.generate_table_name("institutions")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_institutions_table_sql(table_name)
    db.execute(text(create_sql))
    db.commit()
    
    # Register dataset
    _register_dataset(db, "institutions", table_name)
    
    return table_name


async def prepare_failed_banks_table(db: Session) -> str:
    """
    Prepare the fdic_failed_banks table.
    
    Returns:
        Table name
    """
    table_name = metadata.generate_table_name("failed_banks")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_failed_banks_table_sql(table_name)
    db.execute(text(create_sql))
    db.commit()
    
    # Register dataset
    _register_dataset(db, "failed_banks", table_name)
    
    return table_name


async def prepare_deposits_table(db: Session) -> str:
    """
    Prepare the fdic_summary_deposits table.
    
    Returns:
        Table name
    """
    table_name = metadata.generate_table_name("summary_deposits")
    
    logger.info(f"Creating table {table_name}")
    create_sql = metadata.generate_deposits_table_sql(table_name)
    db.execute(text(create_sql))
    db.commit()
    
    # Register dataset
    _register_dataset(db, "summary_deposits", table_name)
    
    return table_name


def _register_dataset(db: Session, dataset: str, table_name: str):
    """Register dataset in dataset_registry."""
    dataset_id = f"fdic_{dataset}"
    
    existing = db.query(DatasetRegistry).filter(
        DatasetRegistry.table_name == table_name
    ).first()
    
    if existing:
        existing.last_updated_at = datetime.utcnow()
        db.commit()
        logger.info(f"Updated dataset {dataset_id} registration")
    else:
        entry = DatasetRegistry(
            source="fdic",
            dataset_id=dataset_id,
            table_name=table_name,
            display_name=metadata.get_display_name(dataset),
            description=metadata.get_description(dataset),
            source_metadata={"api": "https://banks.data.fdic.gov/docs/"}
        )
        db.add(entry)
        db.commit()
        logger.info(f"Registered dataset {dataset_id}")


# =============================================================================
# INGESTION FUNCTIONS
# =============================================================================

async def ingest_bank_financials(
    db: Session,
    job_id: int,
    cert: Optional[int] = None,
    report_date: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest bank financial data from FDIC API.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        cert: Optional FDIC certificate number (for specific bank)
        report_date: Optional report date filter (YYYYMMDD)
        year: Optional year filter
        limit: Optional limit on records to fetch
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FDICClient(
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
        
        # Prepare table
        table_name = await prepare_financials_table(db)
        
        # Build filter
        filters = None
        if report_date:
            filters = f"REPDTE:{report_date}"
        elif year:
            # Get quarters for the year
            filters = f"REPDTE:[{year}0101 TO {year}1231]"
        
        # Fetch data
        logger.info(f"Fetching bank financials (cert={cert}, filters={filters})")
        
        if limit and limit < client.MAX_LIMIT:
            response = await client.get_bank_financials(
                cert=cert,
                filters=filters,
                limit=limit
            )
            raw_data = response.get("data", [])
        else:
            raw_data = await client.get_all_bank_financials(
                cert=cert,
                filters=filters
            )
        
        if not raw_data:
            logger.warning("No financial data returned from FDIC API")
            if job:
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = 0
                db.commit()
            return {
                "table_name": table_name,
                "rows_inserted": 0,
                "message": "No data returned"
            }
        
        # Parse and insert data
        logger.info(f"Parsing {len(raw_data)} financial records")
        parsed_data = [metadata.parse_financials_record(r) for r in raw_data]
        
        # Filter out records with missing required fields
        parsed_data = [r for r in parsed_data if r.get("cert") and r.get("repdte")]
        
        rows_inserted = await _insert_financials(db, table_name, parsed_data)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "records_fetched": len(raw_data)
        }
    
    except Exception as e:
        logger.error(f"Bank financials ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_institutions(
    db: Session,
    job_id: int,
    active_only: bool = True,
    state: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest bank institution demographics from FDIC API.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        active_only: Only fetch active institutions
        state: Optional state filter (2-letter code)
        limit: Optional limit on records
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FDICClient(
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
        
        # Prepare table
        table_name = await prepare_institutions_table(db)
        
        # Build filter
        filters = None
        if state:
            filters = f"STALP:{state}"
        
        # Fetch data
        logger.info(f"Fetching institutions (active_only={active_only}, state={state})")
        
        if limit and limit < client.MAX_LIMIT:
            response = await client.get_institutions(
                filters=filters,
                limit=limit
            )
            raw_data = response.get("data", [])
        else:
            raw_data = await client.get_all_institutions(
                filters=filters,
                active_only=active_only
            )
        
        if not raw_data:
            logger.warning("No institutions returned from FDIC API")
            if job:
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = 0
                db.commit()
            return {
                "table_name": table_name,
                "rows_inserted": 0,
                "message": "No data returned"
            }
        
        # Parse and insert data
        logger.info(f"Parsing {len(raw_data)} institution records")
        parsed_data = [metadata.parse_institution_record(r) for r in raw_data]
        
        # Filter out records with missing required fields
        parsed_data = [r for r in parsed_data if r.get("cert")]
        
        rows_inserted = await _insert_institutions(db, table_name, parsed_data)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "records_fetched": len(raw_data)
        }
    
    except Exception as e:
        logger.error(f"Institutions ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_failed_banks(
    db: Session,
    job_id: int,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest failed banks list from FDIC API.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year_start: Optional start year filter
        year_end: Optional end year filter
        limit: Optional limit on records
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FDICClient(
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
        
        # Prepare table
        table_name = await prepare_failed_banks_table(db)
        
        # Build filter for date range
        filters = None
        if year_start and year_end:
            filters = f"FAILDATE:[{year_start}0101 TO {year_end}1231]"
        elif year_start:
            filters = f"FAILDATE:[{year_start}0101 TO *]"
        elif year_end:
            filters = f"FAILDATE:[* TO {year_end}1231]"
        
        # Fetch data
        logger.info(f"Fetching failed banks (filters={filters})")
        
        if limit and limit < client.MAX_LIMIT:
            response = await client.get_failed_banks(
                filters=filters,
                limit=limit
            )
            raw_data = response.get("data", [])
        else:
            raw_data = await client.get_all_failed_banks(filters=filters)
        
        if not raw_data:
            logger.warning("No failed banks returned from FDIC API")
            if job:
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = 0
                db.commit()
            return {
                "table_name": table_name,
                "rows_inserted": 0,
                "message": "No data returned"
            }
        
        # Parse and insert data
        logger.info(f"Parsing {len(raw_data)} failed bank records")
        parsed_data = [metadata.parse_failed_bank_record(r) for r in raw_data]
        
        # Filter out records with missing required fields
        parsed_data = [r for r in parsed_data if r.get("cert") and r.get("faildate")]
        
        rows_inserted = await _insert_failed_banks(db, table_name, parsed_data)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "records_fetched": len(raw_data)
        }
    
    except Exception as e:
        logger.error(f"Failed banks ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_summary_of_deposits(
    db: Session,
    job_id: int,
    year: Optional[int] = None,
    cert: Optional[int] = None,
    state: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest Summary of Deposits (SOD) data from FDIC API.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Optional year filter
        cert: Optional FDIC certificate number
        state: Optional state filter
        limit: Optional limit on records
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    
    client = FDICClient(
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
        
        # Prepare table
        table_name = await prepare_deposits_table(db)
        
        # Build filters
        filters = None
        filter_parts = []
        if year:
            filter_parts.append(f"YEAR:{year}")
        if state:
            filter_parts.append(f"STALP:{state}")
        if filter_parts:
            filters = " AND ".join(filter_parts)
        
        # Fetch data
        logger.info(f"Fetching SOD data (year={year}, cert={cert}, state={state})")
        
        if limit and limit < client.MAX_LIMIT:
            response = await client.get_summary_of_deposits(
                cert=cert,
                filters=filters,
                limit=limit
            )
            raw_data = response.get("data", [])
        else:
            raw_data = await client.get_all_summary_of_deposits(
                cert=cert,
                filters=filters,
                year=year
            )
        
        if not raw_data:
            logger.warning("No SOD data returned from FDIC API")
            if job:
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = 0
                db.commit()
            return {
                "table_name": table_name,
                "rows_inserted": 0,
                "message": "No data returned"
            }
        
        # Parse and insert data
        logger.info(f"Parsing {len(raw_data)} SOD records")
        parsed_data = [metadata.parse_sod_record(r) for r in raw_data]
        
        # Filter out records with missing required fields
        parsed_data = [r for r in parsed_data if r.get("cert") and r.get("year")]
        
        rows_inserted = await _insert_deposits(db, table_name, parsed_data)
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "records_fetched": len(raw_data)
        }
    
    except Exception as e:
        logger.error(f"SOD ingestion failed: {e}", exc_info=True)
        
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_all_fdic_datasets(
    db: Session,
    include_financials: bool = True,
    include_institutions: bool = True,
    include_failed_banks: bool = True,
    include_deposits: bool = False,  # Large dataset, disabled by default
    year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Ingest all FDIC datasets.
    
    Args:
        db: Database session
        include_financials: Include bank financials
        include_institutions: Include institutions
        include_failed_banks: Include failed banks
        include_deposits: Include summary of deposits (large!)
        year: Optional year filter for financials and deposits
        
    Returns:
        Dictionary with results for each dataset
    """
    results = {}
    
    datasets_to_ingest = []
    if include_financials:
        datasets_to_ingest.append(("financials", ingest_bank_financials))
    if include_institutions:
        datasets_to_ingest.append(("institutions", ingest_institutions))
    if include_failed_banks:
        datasets_to_ingest.append(("failed_banks", ingest_failed_banks))
    if include_deposits:
        datasets_to_ingest.append(("deposits", ingest_summary_of_deposits))
    
    for dataset_name, ingest_func in datasets_to_ingest:
        logger.info(f"Starting ingestion for FDIC {dataset_name}")
        
        # Create job
        job_config = {
            "source": "fdic",
            "dataset": dataset_name,
            "year": year,
        }
        
        job = IngestionJob(
            source="fdic",
            status=JobStatus.PENDING,
            config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        try:
            if dataset_name == "financials":
                result = await ingest_func(db, job.id, year=year)
            elif dataset_name == "deposits":
                result = await ingest_func(db, job.id, year=year)
            else:
                result = await ingest_func(db, job.id)
            
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


# =============================================================================
# INSERT HELPERS
# =============================================================================

async def _insert_financials(db: Session, table_name: str, data: List[Dict]) -> int:
    """Insert financial records with upsert."""
    if not data:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            cert, name, repdte,
            asset, lnlsnet, sc, scus, scmuni, chbal, frepo, intang, oreo,
            liab, dep, depi, depuna, depdom, depfor, frepp, othbrf, subnd,
            eq, eqtot, rbct1j, rbct2, rbc1aaj,
            netinc, nim, nimy, intinc, intexp, netii, nonii, nonix, eeffr, elnatr, epreem,
            roa, roe, roaptx, roaq, roeq,
            rbc1rwaj, rbcrwaj, idt1cer, lnlsdepr,
            p3asset, p9asset, nclnlsr, ntlnlsr, lnlsntv, lnatres, lnresncr,
            lnre, lnrecons, lnrenres, lnremult, lnreres, lnci, lncon, lncrcd, lnag, lnoth,
            depnidom, depti, depsmamt, deplgamt, cotefn, bkdep,
            numemp, offdom, offfor, rssdhcr
        ) VALUES (
            :cert, :name, :repdte,
            :asset, :lnlsnet, :sc, :scus, :scmuni, :chbal, :frepo, :intang, :oreo,
            :liab, :dep, :depi, :depuna, :depdom, :depfor, :frepp, :othbrf, :subnd,
            :eq, :eqtot, :rbct1j, :rbct2, :rbc1aaj,
            :netinc, :nim, :nimy, :intinc, :intexp, :netii, :nonii, :nonix, :eeffr, :elnatr, :epreem,
            :roa, :roe, :roaptx, :roaq, :roeq,
            :rbc1rwaj, :rbcrwaj, :idt1cer, :lnlsdepr,
            :p3asset, :p9asset, :nclnlsr, :ntlnlsr, :lnlsntv, :lnatres, :lnresncr,
            :lnre, :lnrecons, :lnrenres, :lnremult, :lnreres, :lnci, :lncon, :lncrcd, :lnag, :lnoth,
            :depnidom, :depti, :depsmamt, :deplgamt, :cotefn, :bkdep,
            :numemp, :offdom, :offfor, :rssdhcr
        )
        ON CONFLICT (cert, repdte) DO UPDATE SET
            name = EXCLUDED.name,
            asset = EXCLUDED.asset,
            lnlsnet = EXCLUDED.lnlsnet,
            dep = EXCLUDED.dep,
            eq = EXCLUDED.eq,
            netinc = EXCLUDED.netinc,
            roa = EXCLUDED.roa,
            roe = EXCLUDED.roe,
            ingested_at = NOW()
    """
    
    batch_size = 1000
    rows_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if (i + batch_size) % 5000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(data)} financial records")
    
    logger.info(f"Successfully inserted {rows_inserted} financial records")
    return rows_inserted


async def _insert_institutions(db: Session, table_name: str, data: List[Dict]) -> int:
    """Insert institution records with upsert."""
    if not data:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            cert, name, active,
            address, city, stalp, stname, zip, county, stcnty,
            cbsa, cbsa_div, cbsa_metro, cbsa_metro_name,
            fdicregn, fdicsupv, fed, fedchrtr, insfdic, occdist, regagnt,
            bkclass, charter, chrtagnt, stchrtr, instcrcd, mutual,
            estymd, insdate, dateupdt, rundate,
            parcert, newcert,
            asset, dep, depdom, eq, netinc, roa, roe,
            offdom, offfor,
            specgrp, specgrpn, webaddr
        ) VALUES (
            :cert, :name, :active,
            :address, :city, :stalp, :stname, :zip, :county, :stcnty,
            :cbsa, :cbsa_div, :cbsa_metro, :cbsa_metro_name,
            :fdicregn, :fdicsupv, :fed, :fedchrtr, :insfdic, :occdist, :regagnt,
            :bkclass, :charter, :chrtagnt, :stchrtr, :instcrcd, :mutual,
            :estymd, :insdate, :dateupdt, :rundate,
            :parcert, :newcert,
            :asset, :dep, :depdom, :eq, :netinc, :roa, :roe,
            :offdom, :offfor,
            :specgrp, :specgrpn, :webaddr
        )
        ON CONFLICT (cert) DO UPDATE SET
            name = EXCLUDED.name,
            active = EXCLUDED.active,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            stalp = EXCLUDED.stalp,
            asset = EXCLUDED.asset,
            dep = EXCLUDED.dep,
            eq = EXCLUDED.eq,
            netinc = EXCLUDED.netinc,
            roa = EXCLUDED.roa,
            roe = EXCLUDED.roe,
            dateupdt = EXCLUDED.dateupdt,
            ingested_at = NOW()
    """
    
    batch_size = 1000
    rows_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if (i + batch_size) % 5000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(data)} institution records")
    
    logger.info(f"Successfully inserted {rows_inserted} institution records")
    return rows_inserted


async def _insert_failed_banks(db: Session, table_name: str, data: List[Dict]) -> int:
    """Insert failed bank records with upsert."""
    if not data:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            cert, name, city, state, cityst,
            faildate, savession, restype, restype1, resession, chession,
            qbfasset, qbfdep, cost,
            fund, psession, fession
        ) VALUES (
            :cert, :name, :city, :state, :cityst,
            :faildate, :savession, :restype, :restype1, :resession, :chession,
            :qbfasset, :qbfdep, :cost,
            :fund, :psession, :fession
        )
        ON CONFLICT (cert, faildate) DO UPDATE SET
            name = EXCLUDED.name,
            savession = EXCLUDED.savession,
            qbfasset = EXCLUDED.qbfasset,
            qbfdep = EXCLUDED.qbfdep,
            cost = EXCLUDED.cost,
            ingested_at = NOW()
    """
    
    batch_size = 500
    rows_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
    
    logger.info(f"Successfully inserted {rows_inserted} failed bank records")
    return rows_inserted


async def _insert_deposits(db: Session, table_name: str, data: List[Dict]) -> int:
    """Insert SOD records with upsert."""
    if not data:
        return 0
    
    insert_sql = f"""
        INSERT INTO {table_name} (
            cert, name, year,
            uninession, brnum, brsertyp, mainoff,
            address, city, stname, stalp, zipbr, county, stcnty,
            cbsa, cbsa_div, cbsa_metro, csa,
            latitude, longitude,
            asset, dession, depsum, depdom,
            bkclass, charter, specgrp,
            esession, rundate
        ) VALUES (
            :cert, :name, :year,
            :uninession, :brnum, :brsertyp, :mainoff,
            :address, :city, :stname, :stalp, :zipbr, :county, :stcnty,
            :cbsa, :cbsa_div, :cbsa_metro, :csa,
            :latitude, :longitude,
            :asset, :dession, :depsum, :depdom,
            :bkclass, :charter, :specgrp,
            :esession, :rundate
        )
        ON CONFLICT (cert, year, brnum) DO UPDATE SET
            name = EXCLUDED.name,
            depsum = EXCLUDED.depsum,
            asset = EXCLUDED.asset,
            ingested_at = NOW()
    """
    
    batch_size = 1000
    rows_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()
        
        if (i + batch_size) % 10000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(data)} SOD records")
    
    logger.info(f"Successfully inserted {rows_inserted} SOD records")
    return rows_inserted
