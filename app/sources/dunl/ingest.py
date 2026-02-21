"""
DUNL ingestion orchestration.

Fetches JSON-LD data from dunl.org, parses @graph arrays, and upserts
into PostgreSQL tables for currencies, ports, UOM, UOM conversions,
and holiday calendars.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.dunl.client import DunlClient
from app.sources.dunl import metadata

logger = logging.getLogger(__name__)


def _prepare_table(
    db: Session, dataset: str, table_name: str, create_sql: str
) -> None:
    """Create table and register in dataset_registry."""
    db.execute(text(create_sql))
    db.commit()

    info = metadata.DATASET_INFO.get(dataset, {})
    existing = (
        db.query(DatasetRegistry)
        .filter(DatasetRegistry.table_name == table_name)
        .first()
    )
    if existing:
        existing.last_updated_at = datetime.utcnow()
        db.commit()
    else:
        entry = DatasetRegistry(
            source="dunl",
            dataset_id=f"dunl_{dataset}",
            table_name=table_name,
            display_name=info.get("display_name", f"DUNL {dataset}"),
            description=info.get("description", ""),
            source_metadata={"dataset": dataset},
        )
        db.add(entry)
        db.commit()


def _upsert_batch(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]],
    columns: List[str],
    conflict_cols: str,
) -> int:
    """Batch upsert records with ON CONFLICT DO UPDATE."""
    if not records:
        return 0

    # Ensure all records have the same keys
    for rec in records:
        for col in columns:
            rec.setdefault(col, None)

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}"
        for col in columns
        if col not in conflict_cols.split(", ")
    )

    sql = f"""
        INSERT INTO {table_name} ({column_list})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_set}, ingested_at = NOW()
    """

    batch_size = 500
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        db.execute(text(sql), batch)
        total += len(batch)
        db.commit()

    return total


def _mark_job(db: Session, job_id: int, status: JobStatus, rows: int = 0, error: str = None):
    """Update job status."""
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        return
    job.status = status
    if status == JobStatus.RUNNING:
        job.started_at = datetime.utcnow()
    elif status in (JobStatus.SUCCESS, JobStatus.FAILED):
        job.completed_at = datetime.utcnow()
    if rows:
        job.rows_inserted = rows
    if error:
        job.error_message = error
    db.commit()


# ========== Ingestion Functions ==========


async def ingest_dunl_currencies(db: Session, job_id: int, **config) -> Dict[str, Any]:
    """Ingest all DUNL currency definitions."""
    client = DunlClient()
    try:
        _mark_job(db, job_id, JobStatus.RUNNING)
        _prepare_table(db, "currencies", "dunl_currencies", metadata.generate_create_currencies_sql())

        data = await client.fetch_currencies()
        graph = metadata.parse_graph(data)
        records = metadata.parse_currencies(graph)

        cols = ["currency_code", "currency_name", "dunl_uri", "country_region_ref"]
        rows = _upsert_batch(db, "dunl_currencies", records, cols, "currency_code")

        _mark_job(db, job_id, JobStatus.SUCCESS, rows=rows)
        logger.info(f"DUNL currencies: upserted {rows} rows")
        return {"table_name": "dunl_currencies", "rows_inserted": rows}

    except Exception as e:
        logger.error(f"DUNL currencies ingestion failed: {e}", exc_info=True)
        _mark_job(db, job_id, JobStatus.FAILED, error=str(e))
        raise
    finally:
        await client.close()


async def ingest_dunl_ports(db: Session, job_id: int, **config) -> Dict[str, Any]:
    """Ingest all DUNL port definitions."""
    client = DunlClient()
    try:
        _mark_job(db, job_id, JobStatus.RUNNING)
        _prepare_table(db, "ports", "dunl_ports", metadata.generate_create_ports_sql())

        data = await client.fetch_ports()
        graph = metadata.parse_graph(data)
        records = metadata.parse_ports(graph)

        cols = ["symbol", "port_name", "location", "dunl_uri"]
        rows = _upsert_batch(db, "dunl_ports", records, cols, "symbol")

        _mark_job(db, job_id, JobStatus.SUCCESS, rows=rows)
        logger.info(f"DUNL ports: upserted {rows} rows")
        return {"table_name": "dunl_ports", "rows_inserted": rows}

    except Exception as e:
        logger.error(f"DUNL ports ingestion failed: {e}", exc_info=True)
        _mark_job(db, job_id, JobStatus.FAILED, error=str(e))
        raise
    finally:
        await client.close()


async def ingest_dunl_uom(db: Session, job_id: int, **config) -> Dict[str, Any]:
    """Ingest all DUNL unit-of-measure definitions."""
    client = DunlClient()
    try:
        _mark_job(db, job_id, JobStatus.RUNNING)
        _prepare_table(db, "uom", "dunl_uom", metadata.generate_create_uom_sql())

        data = await client.fetch_uom()
        graph = metadata.parse_graph(data)
        records = metadata.parse_uom(graph)

        cols = ["uom_code", "uom_name", "description", "uom_type", "dunl_uri"]
        rows = _upsert_batch(db, "dunl_uom", records, cols, "uom_code")

        _mark_job(db, job_id, JobStatus.SUCCESS, rows=rows)
        logger.info(f"DUNL UOM: upserted {rows} rows")
        return {"table_name": "dunl_uom", "rows_inserted": rows}

    except Exception as e:
        logger.error(f"DUNL UOM ingestion failed: {e}", exc_info=True)
        _mark_job(db, job_id, JobStatus.FAILED, error=str(e))
        raise
    finally:
        await client.close()


async def ingest_dunl_uom_conversions(db: Session, job_id: int, **config) -> Dict[str, Any]:
    """Ingest all DUNL UOM conversion factors.

    Note: The DUNL UOM conversions listing endpoint may not support
    JSON-LD content negotiation. This will succeed with 0 rows if
    the endpoint is unavailable.
    """
    client = DunlClient()
    try:
        _mark_job(db, job_id, JobStatus.RUNNING)
        _prepare_table(db, "uom_conversions", "dunl_uom_conversions", metadata.generate_create_uom_conversions_sql())

        try:
            data = await client.fetch_uom_conversions()
            graph = metadata.parse_graph(data)
            records = metadata.parse_uom_conversions(graph)
        except Exception as e:
            logger.warning(f"DUNL UOM conversions endpoint unavailable: {e}")
            records = []

        cols = ["conversion_code", "from_uom", "to_uom", "factor", "description", "dunl_uri"]
        rows = _upsert_batch(db, "dunl_uom_conversions", records, cols, "conversion_code")

        _mark_job(db, job_id, JobStatus.SUCCESS, rows=rows)
        logger.info(f"DUNL UOM conversions: upserted {rows} rows")
        return {"table_name": "dunl_uom_conversions", "rows_inserted": rows}

    except Exception as e:
        logger.error(f"DUNL UOM conversions ingestion failed: {e}", exc_info=True)
        _mark_job(db, job_id, JobStatus.FAILED, error=str(e))
        raise
    finally:
        await client.close()


async def ingest_dunl_calendars(db: Session, job_id: int, **config) -> Dict[str, Any]:
    """Ingest DUNL holiday calendars for specified years."""
    years = config.get("years", [2024, 2025, 2026])
    client = DunlClient()
    try:
        _mark_job(db, job_id, JobStatus.RUNNING)
        _prepare_table(db, "calendars", "dunl_calendars", metadata.generate_create_calendars_sql())

        total_rows = 0
        for year in years:
            try:
                data = await client.fetch_calendar(year)
                graph = metadata.parse_graph(data)
                records = metadata.parse_calendars(graph, year)

                cols = [
                    "year", "commodity", "event_date",
                    "publication_affected", "publication_comments",
                    "service_affected", "service_comments", "dunl_uri",
                ]
                rows = _upsert_batch(db, "dunl_calendars", records, cols, "year, commodity, event_date")
                total_rows += rows
                logger.info(f"DUNL calendar {year}: upserted {rows} rows")
            except Exception as e:
                logger.warning(f"DUNL calendar {year} failed: {e}")

        _mark_job(db, job_id, JobStatus.SUCCESS, rows=total_rows)
        return {"table_name": "dunl_calendars", "years": years, "rows_inserted": total_rows}

    except Exception as e:
        logger.error(f"DUNL calendars ingestion failed: {e}", exc_info=True)
        _mark_job(db, job_id, JobStatus.FAILED, error=str(e))
        raise
    finally:
        await client.close()
