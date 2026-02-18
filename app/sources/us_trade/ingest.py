"""
US Trade ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for US Census Bureau International Trade datasets.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.us_trade.client import USTradeClient
from app.sources.us_trade import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_trade_data(db: Session, dataset: str) -> Dict[str, Any]:
    """
    Prepare database table for US Trade data ingestion.

    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry

    Args:
        db: Database session
        dataset: Dataset identifier (exports_hs, imports_hs, exports_state, etc.)

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
        logger.info(f"Creating table {table_name} for US Trade {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)

        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()

        # 4. Register in dataset_registry
        dataset_id = f"us_trade_{dataset}"

        # Check if already registered
        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {"dataset": dataset}
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="us_trade",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_dataset_display_name(dataset),
                description=metadata.get_dataset_description(dataset),
                source_metadata={"dataset": dataset},
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")

        return {"table_name": table_name}

    except Exception as e:
        logger.error(f"Failed to prepare table for US Trade data: {e}")
        raise


async def ingest_exports_by_hs(
    db: Session,
    job_id: int,
    year: int,
    month: Optional[int] = None,
    hs_code: Optional[str] = None,
    country: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest US export data by HS code into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Data year (2013+)
        month: Optional month (1-12)
        hs_code: Optional HS code filter
        country: Optional country code filter
        api_key: Optional Census API key

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    # Initialize client
    client = USTradeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(
            f"Ingesting US exports by HS: year={year}, month={month}, "
            f"hs_code={hs_code}, country={country}"
        )

        # Prepare table
        table_info = await prepare_table_for_trade_data(db, "exports_hs")
        table_name = table_info["table_name"]

        # Fetch data from Census API
        records = await client.get_exports_by_hs(
            year=year, month=month, hs_code=hs_code, country=country
        )

        logger.info(f"Fetched {len(records)} export records from Census API")

        # Parse records
        parsed_records = metadata.parse_exports_hs_response(records)

        if not parsed_records:
            logger.warning("No export data to insert")
            rows_inserted = 0
        else:
            # Insert data
            rows_inserted = await _insert_exports_hs_data(
                db, table_name, parsed_records
            )

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": "exports_hs",
            "rows_inserted": rows_inserted,
            "year": year,
            "month": month,
            "filters": {"hs_code": hs_code, "country": country},
        }

    except Exception as e:
        logger.error(f"US exports ingestion failed: {e}", exc_info=True)

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


async def _insert_exports_hs_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert export data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")

    columns = [
        "year",
        "month",
        "country_code",
        "country_name",
        "hs_code",
        "commodity_desc",
        "value_monthly",
        "value_ytd",
        "quantity_monthly",
        "quantity_ytd",
        "quantity_unit",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    update_set = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns
            if col not in ("year", "month", "country_code", "hs_code")
        ]
    )

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ("year", "month", "country_code", "hs_code")
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    # Execute in batches
    batch_size = 1000
    rows_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()

        if (i + batch_size) % 10000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} rows")

    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_imports_by_hs(
    db: Session,
    job_id: int,
    year: int,
    month: Optional[int] = None,
    hs_code: Optional[str] = None,
    country: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest US import data by HS code into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Data year (2013+)
        month: Optional month (1-12)
        hs_code: Optional HS code filter
        country: Optional country code filter
        api_key: Optional Census API key

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = USTradeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(
            f"Ingesting US imports by HS: year={year}, month={month}, "
            f"hs_code={hs_code}, country={country}"
        )

        # Prepare table
        table_info = await prepare_table_for_trade_data(db, "imports_hs")
        table_name = table_info["table_name"]

        # Fetch data
        records = await client.get_imports_by_hs(
            year=year, month=month, hs_code=hs_code, country=country
        )

        logger.info(f"Fetched {len(records)} import records from Census API")

        # Parse records
        parsed_records = metadata.parse_imports_hs_response(records)

        if not parsed_records:
            logger.warning("No import data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_imports_hs_data(
                db, table_name, parsed_records
            )

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": "imports_hs",
            "rows_inserted": rows_inserted,
            "year": year,
            "month": month,
            "filters": {"hs_code": hs_code, "country": country},
        }

    except Exception as e:
        logger.error(f"US imports ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def _insert_imports_hs_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert import data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")

    columns = [
        "year",
        "month",
        "country_code",
        "country_name",
        "hs_code",
        "commodity_desc",
        "general_value_monthly",
        "general_value_ytd",
        "consumption_value_monthly",
        "consumption_value_ytd",
        "dutyfree_value_monthly",
        "dutiable_value_monthly",
        "quantity_monthly",
        "quantity_ytd",
        "quantity_unit",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    update_set = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns
            if col not in ("year", "month", "country_code", "hs_code")
        ]
    )

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ("year", "month", "country_code", "hs_code")
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    batch_size = 1000
    rows_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()

    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_exports_by_state(
    db: Session,
    job_id: int,
    year: int,
    month: Optional[int] = None,
    state: Optional[str] = None,
    hs_code: Optional[str] = None,
    country: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest US state-level export data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Data year (2013+)
        month: Optional month (1-12)
        state: Optional state filter
        hs_code: Optional HS code filter
        country: Optional country code filter
        api_key: Optional Census API key

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = USTradeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(
            f"Ingesting US state exports: year={year}, month={month}, "
            f"state={state}, hs_code={hs_code}, country={country}"
        )

        # Prepare table
        table_info = await prepare_table_for_trade_data(db, "exports_state")
        table_name = table_info["table_name"]

        # Fetch data
        records = await client.get_exports_by_state(
            year=year, month=month, state=state, hs_code=hs_code, country=country
        )

        logger.info(f"Fetched {len(records)} state export records")

        # Parse records
        parsed_records = metadata.parse_exports_state_response(records)

        if not parsed_records:
            logger.warning("No state export data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_state_exports_data(
                db, table_name, parsed_records
            )

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": "exports_state",
            "rows_inserted": rows_inserted,
            "year": year,
            "month": month,
            "filters": {"state": state, "hs_code": hs_code, "country": country},
        }

    except Exception as e:
        logger.error(f"US state exports ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def _insert_state_exports_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert state export data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")

    columns = [
        "year",
        "month",
        "state_code",
        "state_name",
        "country_code",
        "country_name",
        "hs_code",
        "commodity_desc",
        "value_monthly",
        "value_ytd",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    update_set = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns
            if col not in ("year", "month", "state_code", "country_code", "hs_code")
        ]
    )

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ("year", "month", "state_code", "country_code", "hs_code")
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    batch_size = 1000
    rows_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()

    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_port_trade(
    db: Session,
    job_id: int,
    year: int,
    trade_type: str = "export",
    month: Optional[int] = None,
    district: Optional[str] = None,
    hs_code: Optional[str] = None,
    country: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest US port-level trade data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Data year (2013+)
        trade_type: "export" or "import"
        month: Optional month (1-12)
        district: Optional customs district filter
        hs_code: Optional HS code filter
        country: Optional country code filter
        api_key: Optional Census API key

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = USTradeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(
            f"Ingesting US port {trade_type}s: year={year}, month={month}, "
            f"district={district}, hs_code={hs_code}"
        )

        # Prepare table
        dataset = f"{trade_type}s_port"
        table_info = await prepare_table_for_trade_data(db, "port_trade")
        table_name = table_info["table_name"]

        # Fetch data
        if trade_type == "export":
            records = await client.get_exports_by_port(
                year=year,
                month=month,
                district=district,
                hs_code=hs_code,
                country=country,
            )
        else:
            records = await client.get_imports_by_port(
                year=year,
                month=month,
                district=district,
                hs_code=hs_code,
                country=country,
            )

        logger.info(f"Fetched {len(records)} port {trade_type} records")

        # Parse records
        parsed_records = metadata.parse_port_trade_response(records, trade_type)

        if not parsed_records:
            logger.warning(f"No port {trade_type} data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_port_trade_data(
                db, table_name, parsed_records
            )

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": dataset,
            "rows_inserted": rows_inserted,
            "year": year,
            "month": month,
            "trade_type": trade_type,
            "filters": {"district": district, "hs_code": hs_code, "country": country},
        }

    except Exception as e:
        logger.error(f"US port trade ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def _insert_port_trade_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert port trade data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")

    columns = [
        "year",
        "month",
        "district_code",
        "district_name",
        "country_code",
        "country_name",
        "hs_code",
        "commodity_desc",
        "trade_type",
        "value_monthly",
        "value_ytd",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    update_set = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns
            if col
            not in (
                "year",
                "month",
                "district_code",
                "country_code",
                "hs_code",
                "trade_type",
            )
        ]
    )

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ("year", "month", "district_code", "country_code", "hs_code", "trade_type")
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    batch_size = 1000
    rows_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()

    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted


async def ingest_trade_summary(
    db: Session,
    job_id: int,
    year: int,
    month: Optional[int] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest aggregated trade summary by country into Postgres.

    Fetches exports and imports, then aggregates by country to show
    total trade, trade balance, etc.

    Args:
        db: Database session
        job_id: Ingestion job ID
        year: Data year (2013+)
        month: Optional month (1-12)
        api_key: Optional Census API key

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = USTradeClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        logger.info(f"Ingesting US trade summary: year={year}, month={month}")

        # Prepare table
        table_info = await prepare_table_for_trade_data(db, "trade_summary")
        table_name = table_info["table_name"]

        # Fetch trade summary
        summary = await client.get_trade_summary_by_country(year=year, month=month)

        # Parse into summary records
        parsed_records = metadata.parse_trade_summary(
            exports=summary["exports"],
            imports=summary["imports"],
            year=year,
            month=month,
        )

        if not parsed_records:
            logger.warning("No trade summary data to insert")
            rows_inserted = 0
        else:
            rows_inserted = await _insert_trade_summary_data(
                db, table_name, parsed_records
            )

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": "trade_summary",
            "rows_inserted": rows_inserted,
            "year": year,
            "month": month,
        }

    except Exception as e:
        logger.error(f"US trade summary ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def _insert_trade_summary_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert trade summary data with upsert logic."""
    logger.info(f"Inserting {len(records)} records into {table_name}")

    columns = [
        "year",
        "month",
        "country_code",
        "country_name",
        "exports_value",
        "imports_value",
        "total_trade",
        "trade_balance",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])
    update_set = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns
            if col not in ("year", "month", "country_code")
        ]
    )

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT ("year", COALESCE("month", 0), "country_code")
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    batch_size = 1000
    rows_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(insert_sql), batch)
        rows_inserted += len(batch)
        db.commit()

    logger.info(f"Successfully inserted {rows_inserted} rows")
    return rows_inserted
