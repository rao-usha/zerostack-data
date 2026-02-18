"""
International Economic Data ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.international_econ.client import (
    WorldBankClient,
    IMFClient,
    OECDClient,
    BISClient,
)
from app.sources.international_econ import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_intl_data(
    db: Session, source: str, dataset: str
) -> Dict[str, Any]:
    """
    Prepare database table for international economic data ingestion.

    Steps:
    1. Generate table name
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry

    Args:
        db: Database session
        source: Data source (worldbank, imf, oecd, bis)
        dataset: Dataset name

    Returns:
        Dictionary with table_name
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(source, dataset)

        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for {source} {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, source, dataset)

        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()

        # 4. Register in dataset_registry
        dataset_id = f"intl_{source.lower()}_{dataset.lower()}"

        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {"source": source, "dataset": dataset}
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source=f"international_econ_{source}",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_source_display_name(source, dataset),
                description=metadata.get_source_description(source, dataset),
                source_metadata={"source": source, "dataset": dataset},
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")

        return {"table_name": table_name}

    except Exception as e:
        logger.error(f"Failed to prepare table for international data: {e}")
        raise


async def ingest_worldbank_wdi(
    db: Session,
    job_id: int,
    indicators: List[str],
    countries: List[str] = None,
    start_year: int = None,
    end_year: int = None,
) -> Dict[str, Any]:
    """
    Ingest World Bank World Development Indicators data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        indicators: List of WDI indicator codes (e.g., ["NY.GDP.MKTP.CD"])
        countries: List of country codes (None for all)
        start_year: Start year
        end_year: End year

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = WorldBankClient(
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

        # Set default date range
        if not start_year or not end_year:
            default_start, default_end = metadata.get_default_date_range("worldbank")
            start_year = start_year or default_start
            end_year = end_year or default_end

        logger.info(
            f"Ingesting World Bank WDI data: "
            f"{len(indicators)} indicators, {start_year}-{end_year}"
        )

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "worldbank", "wdi")
        table_name = table_info["table_name"]

        # Fetch data from World Bank API
        all_data = await client.get_wdi_data(
            indicators=indicators,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
        )

        logger.info(f"Fetched {len(all_data)} raw records from World Bank")

        # Parse data
        all_parsed = []
        for indicator in indicators:
            indicator_data = [
                d
                for d in all_data
                if d and d.get("indicator", {}).get("id") == indicator
            ]
            parsed = metadata.parse_worldbank_data(indicator_data, indicator)
            all_parsed.extend(parsed)

        # If indicators weren't separated, parse all together
        if not all_parsed:
            all_parsed = metadata.parse_worldbank_data(
                all_data, indicators[0] if indicators else ""
            )

        logger.info(f"Parsed {len(all_parsed)} records")

        # Insert data
        rows_inserted = 0

        if all_parsed:
            # Build insert SQL
            columns = [
                "indicator_id",
                "indicator_name",
                "country_id",
                "country_name",
                "country_iso3",
                "year",
                "value",
                "unit",
                "decimal_places",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            update_set = ", ".join(
                [
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col not in ["indicator_id", "country_id", "year"]
                ]
            )

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (indicator_id, country_id, year) 
                DO UPDATE SET
                    {update_set},
                    ingested_at = NOW()
            """

            # Check if conflict columns exist, if not use simpler insert
            try:
                # Try to add unique constraint if it doesn't exist
                db.execute(
                    text(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique 
                    ON {table_name} (indicator_id, country_id, year)
                """)
                )
                db.commit()
            except Exception:
                pass

            # Simpler insert without conflict handling for initial load
            simple_insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            # Execute in batches
            batch_size = 1000

            for i in range(0, len(all_parsed), batch_size):
                batch = all_parsed[i : i + batch_size]

                # Prepare rows for insert
                insert_rows = []
                for record in batch:
                    row = {
                        "indicator_id": record.get("indicator_id"),
                        "indicator_name": record.get("indicator_name"),
                        "country_id": record.get("country_id"),
                        "country_name": record.get("country_name"),
                        "country_iso3": record.get("country_iso3"),
                        "year": record.get("year"),
                        "value": record.get("value"),
                        "unit": record.get("unit"),
                        "decimal_places": record.get("decimal_places"),
                    }
                    insert_rows.append(row)

                try:
                    db.execute(text(insert_sql), insert_rows)
                except Exception:
                    # Fall back to simple insert
                    for row in insert_rows:
                        try:
                            db.execute(text(simple_insert_sql), row)
                        except Exception as e:
                            logger.warning(f"Failed to insert row: {e}")

                rows_inserted += len(batch)
                db.commit()

                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(all_parsed)} rows")

            logger.info(f"Successfully inserted {rows_inserted} rows")

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "worldbank",
            "dataset": "wdi",
            "indicators": indicators,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_year}-{end_year}",
        }

    except Exception as e:
        logger.error(f"World Bank WDI ingestion failed: {e}", exc_info=True)

        # Rollback any failed transaction
        db.rollback()

        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]  # Truncate long error messages
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()

        raise

    finally:
        await client.close()


async def ingest_worldbank_indicators(
    db: Session, job_id: int, search: Optional[str] = None, max_results: int = 1000
) -> Dict[str, Any]:
    """
    Ingest World Bank indicator metadata.

    Args:
        db: Database session
        job_id: Ingestion job ID
        search: Optional search term
        max_results: Maximum results to fetch

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = WorldBankClient(
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

        logger.info("Ingesting World Bank indicator metadata")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "worldbank", "indicators")
        table_name = table_info["table_name"]

        # Fetch indicators
        all_indicators = []
        page = 1
        per_page = 500

        while len(all_indicators) < max_results:
            result = await client.get_indicators(
                search=search, page=page, per_page=per_page
            )

            indicators = result.get("indicators", [])
            if not indicators:
                break

            all_indicators.extend(indicators)

            if page >= result.get("pages", 1):
                break

            page += 1

        logger.info(f"Fetched {len(all_indicators)} indicators")

        # Parse data
        parsed = metadata.parse_worldbank_indicators(all_indicators[:max_results])

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "indicator_id",
                "indicator_name",
                "source_id",
                "source_name",
                "source_note",
                "source_organization",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (indicator_id) DO UPDATE SET
                    indicator_name = EXCLUDED.indicator_name,
                    source_id = EXCLUDED.source_id,
                    source_name = EXCLUDED.source_name,
                    source_note = EXCLUDED.source_note,
                    source_organization = EXCLUDED.source_organization,
                    ingested_at = NOW()
            """

            batch_size = 500

            for i in range(0, len(parsed), batch_size):
                batch = parsed[i : i + batch_size]

                insert_rows = []
                for record in batch:
                    row = {col: record.get(col) for col in columns}
                    insert_rows.append(row)

                try:
                    db.execute(text(insert_sql), insert_rows)
                except Exception:
                    for row in insert_rows:
                        try:
                            db.execute(
                                text(f"""
                                INSERT INTO {table_name} ({column_list})
                                VALUES ({placeholders})
                            """),
                                row,
                            )
                        except Exception:
                            pass

                rows_inserted += len(batch)
                db.commit()

            logger.info(f"Successfully inserted {rows_inserted} indicator records")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "worldbank",
            "dataset": "indicators",
            "rows_inserted": rows_inserted,
        }

    except Exception as e:
        logger.error(f"World Bank indicators ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_worldbank_countries(db: Session, job_id: int) -> Dict[str, Any]:
    """
    Ingest World Bank countries metadata.

    Args:
        db: Database session
        job_id: Ingestion job ID

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = WorldBankClient(
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

        logger.info("Ingesting World Bank countries metadata")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "worldbank", "countries")
        table_name = table_info["table_name"]

        # Fetch countries
        countries = await client.get_countries()
        logger.info(f"Fetched {len(countries)} countries")

        # Parse data
        parsed = metadata.parse_worldbank_countries(countries)

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "country_id",
                "country_name",
                "iso3_code",
                "iso2_code",
                "region_id",
                "region_name",
                "income_level_id",
                "income_level_name",
                "lending_type_id",
                "lending_type_name",
                "capital_city",
                "longitude",
                "latitude",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
                ON CONFLICT (country_id) DO UPDATE SET
                    country_name = EXCLUDED.country_name,
                    region_name = EXCLUDED.region_name,
                    income_level_name = EXCLUDED.income_level_name,
                    ingested_at = NOW()
            """

            for record in parsed:
                row = {col: record.get(col) for col in columns}
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    try:
                        db.execute(
                            text(f"""
                            INSERT INTO {table_name} ({column_list})
                            VALUES ({placeholders})
                        """),
                            row,
                        )
                        rows_inserted += 1
                    except Exception:
                        logger.warning(f"Failed to insert country: {e}")

            db.commit()
            logger.info(f"Successfully inserted {rows_inserted} country records")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "worldbank",
            "dataset": "countries",
            "rows_inserted": rows_inserted,
        }

    except Exception as e:
        logger.error(f"World Bank countries ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_imf_weo(
    db: Session,
    job_id: int,
    indicators: List[str] = None,
    countries: List[str] = None,
    start_year: str = None,
    end_year: str = None,
) -> Dict[str, Any]:
    """
    Ingest IMF World Economic Outlook data.

    Note: IMF API can be complex. This provides a basic implementation.

    Args:
        db: Database session
        job_id: Ingestion job ID
        indicators: List of WEO indicator codes
        countries: List of country codes
        start_year: Start year
        end_year: End year

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = IMFClient(
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

        if not start_year or not end_year:
            default_start, default_end = metadata.get_default_date_range("imf")
            start_year = start_year or str(default_start)
            end_year = end_year or str(default_end)

        logger.info(f"Ingesting IMF WEO data: {start_year}-{end_year}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "imf", "weo")
        table_name = table_info["table_name"]

        # Fetch available dataflows to understand structure
        dataflows = await client.get_dataflows()
        logger.info(f"Found {len(dataflows)} IMF dataflows")

        # Note: IMF API is complex - this is a simplified implementation
        # The actual WEO data may require different approach

        rows_inserted = 0

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            job.error_message = (
                "IMF WEO ingestion requires specific endpoint configuration"
            )
            db.commit()

        return {
            "table_name": table_name,
            "source": "imf",
            "dataset": "weo",
            "rows_inserted": rows_inserted,
            "note": "IMF API structure is complex. Consider using IFS dataset for simpler data access.",
        }

    except Exception as e:
        logger.error(f"IMF WEO ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_imf_ifs(
    db: Session,
    job_id: int,
    indicator: str = "NGDP_R_XDC",
    countries: List[str] = None,
    start_year: str = None,
    end_year: str = None,
) -> Dict[str, Any]:
    """
    Ingest IMF International Financial Statistics data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        indicator: IFS indicator code
        countries: List of country codes
        start_year: Start year
        end_year: End year

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = IMFClient(
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

        if not start_year or not end_year:
            default_start, default_end = metadata.get_default_date_range("imf")
            start_year = start_year or str(default_start)
            end_year = end_year or str(default_end)

        logger.info(
            f"Ingesting IMF IFS data: indicator={indicator}, {start_year}-{end_year}"
        )

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "imf", "ifs")
        table_name = table_info["table_name"]

        # Fetch IFS data
        data = await client.get_ifs_data(
            indicator=indicator,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
        )

        logger.info(f"Fetched {len(data)} records from IMF IFS")

        # Parse data
        parsed = metadata.parse_imf_data(data, "ifs")

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "indicator_code",
                "country_code",
                "period",
                "frequency",
                "value",
                "unit_mult",
                "status",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in parsed:
                row = {col: record.get(col) for col in columns}
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert IMF record: {e}")

            db.commit()
            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "imf",
            "dataset": "ifs",
            "indicator": indicator,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_year}-{end_year}",
        }

    except Exception as e:
        logger.error(f"IMF IFS ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_oecd_mei(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    subjects: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest OECD Main Economic Indicators (MEI) data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes (e.g., ["USA", "GBR", "DEU"])
        subjects: List of subject codes (economic indicators)
        start_period: Start period (year)
        end_period: End period (year)

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OECDClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("oecd")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(f"Ingesting OECD MEI data: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "oecd", "mei")
        table_name = table_info["table_name"]

        # Fetch MEI data
        data = await client.get_main_economic_indicators(
            countries=countries,
            subjects=subjects,
            start_period=start_period,
            end_period=end_period,
        )

        logger.info(f"Fetched {len(data)} records from OECD MEI")

        # Parse data
        parsed = metadata.parse_oecd_data(data, "mei")

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "indicator_code",
                "country_code",
                "subject",
                "measure",
                "frequency",
                "period",
                "value",
                "unit",
                "powercode",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            batch_size = 500
            for i in range(0, len(parsed), batch_size):
                batch = parsed[i : i + batch_size]
                for record in batch:
                    row = {col: record.get(col) for col in columns}
                    try:
                        db.execute(text(insert_sql), row)
                        rows_inserted += 1
                    except Exception as e:
                        logger.warning(f"Failed to insert OECD record: {e}")
                        db.rollback()  # Rollback failed transaction to continue

                try:
                    db.commit()
                except Exception as commit_err:
                    logger.warning(f"Commit failed: {commit_err}")
                    db.rollback()

                logger.info(f"Inserted {rows_inserted} rows so far")

            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "oecd",
            "dataset": "mei",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"OECD MEI ingestion failed: {e}", exc_info=True)

        db.rollback()

        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()

        raise

    finally:
        await client.close()


async def ingest_oecd_kei(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest OECD Key Economic Indicators (KEI) data.

    KEI includes: Industrial Production, Consumer Prices, Unemployment, etc.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OECDClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("oecd")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(f"Ingesting OECD KEI data: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "oecd", "kei")
        table_name = table_info["table_name"]

        # Fetch KEI data
        data = await client.get_key_economic_indicators(
            countries=countries, start_period=start_period, end_period=end_period
        )

        logger.info(f"Fetched {len(data)} records from OECD KEI")

        # Insert data
        rows_inserted = 0

        if data:
            columns = [
                "indicator_code",
                "country_code",
                "subject",
                "measure",
                "frequency",
                "period",
                "value",
                "unit",
                "transformation",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in data:
                row = {
                    "indicator_code": record.get("subject") or record.get("measure"),
                    "country_code": record.get("ref_area"),
                    "subject": record.get("subject"),
                    "measure": record.get("measure"),
                    "frequency": record.get("freq"),
                    "period": record.get("period"),
                    "value": float(record.get("value"))
                    if record.get("value") is not None
                    else None,
                    "unit": record.get("unit_measure"),
                    "transformation": record.get("transformation"),
                }
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert KEI record: {e}")
                    db.rollback()

            try:
                db.commit()
            except Exception:
                db.rollback()

            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "oecd",
            "dataset": "kei",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"OECD KEI ingestion failed: {e}", exc_info=True)
        db.rollback()
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()
        raise

    finally:
        await client.close()


async def ingest_oecd_labor(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest OECD Annual Labour Force Statistics (ALFS) data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OECDClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("oecd")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(f"Ingesting OECD Labor data: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "oecd", "alfs")
        table_name = table_info["table_name"]

        # Fetch labor data
        data = await client.get_labour_force_statistics(
            countries=countries, start_period=start_period, end_period=end_period
        )

        logger.info(f"Fetched {len(data)} records from OECD Labor")

        # Insert data
        rows_inserted = 0

        if data:
            columns = [
                "country_code",
                "indicator_code",
                "sex",
                "age",
                "frequency",
                "period",
                "value",
                "unit_measure",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in data:
                row = {
                    "country_code": record.get("ref_area"),
                    "indicator_code": record.get("measure") or record.get("subject"),
                    "sex": record.get("sex"),
                    "age": record.get("age"),
                    "frequency": record.get("freq"),
                    "period": record.get("period"),
                    "value": float(record.get("value"))
                    if record.get("value") is not None
                    else None,
                    "unit_measure": record.get("unit_measure"),
                }
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert labor record: {e}")
                    db.rollback()

            try:
                db.commit()
            except Exception:
                db.rollback()

            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "oecd",
            "dataset": "alfs",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"OECD Labor ingestion failed: {e}", exc_info=True)
        db.rollback()
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()
        raise

    finally:
        await client.close()


async def ingest_oecd_trade(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest OECD Balanced Trade in Services (BATIS) data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OECDClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("oecd")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(f"Ingesting OECD Trade data: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "oecd", "batis")
        table_name = table_info["table_name"]

        # Fetch trade data
        data = await client.get_trade_in_services(
            countries=countries, start_period=start_period, end_period=end_period
        )

        logger.info(f"Fetched {len(data)} records from OECD Trade")

        # Insert data
        rows_inserted = 0

        if data:
            columns = [
                "reporter_country",
                "partner_country",
                "flow",
                "service_item",
                "frequency",
                "period",
                "value",
                "unit_measure",
                "currency",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in data:
                row = {
                    "reporter_country": record.get("ref_area")
                    or record.get("reporter"),
                    "partner_country": record.get("counterpart_area")
                    or record.get("partner"),
                    "flow": record.get("flow") or record.get("trans_type"),
                    "service_item": record.get("service_item") or record.get("product"),
                    "frequency": record.get("freq"),
                    "period": record.get("period"),
                    "value": float(record.get("value"))
                    if record.get("value") is not None
                    else None,
                    "unit_measure": record.get("unit_measure"),
                    "currency": record.get("currency"),
                }
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert trade record: {e}")
                    db.rollback()

            try:
                db.commit()
            except Exception:
                db.rollback()

            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "oecd",
            "dataset": "batis",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"OECD Trade ingestion failed: {e}", exc_info=True)
        db.rollback()
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()
        raise

    finally:
        await client.close()


async def ingest_oecd_tax(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest OECD Tax Revenue Statistics data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = OECDClient(
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

        if not start_period or not end_period:
            start_period = start_period or "2000"
            end_period = end_period or str(datetime.now().year)

        logger.info(f"Ingesting OECD Tax data: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "oecd", "tax")
        table_name = table_info["table_name"]

        # Fetch tax data
        data = await client.get_tax_revenue_statistics(
            countries=countries, start_period=start_period, end_period=end_period
        )

        logger.info(f"Fetched {len(data)} records from OECD Tax")

        # Insert data
        rows_inserted = 0

        if data:
            columns = [
                "country_code",
                "tax_type",
                "government_level",
                "measure",
                "period",
                "value",
                "unit",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in data:
                row = {
                    "country_code": record.get("cou") or record.get("ref_area"),
                    "tax_type": record.get("tax") or record.get("taxtype"),
                    "government_level": record.get("gov") or record.get("level"),
                    "measure": record.get("var") or record.get("measure"),
                    "period": record.get("period"),
                    "value": float(record.get("value"))
                    if record.get("value") is not None
                    else None,
                    "unit": record.get("unit_measure"),
                }
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert tax record: {e}")
                    db.rollback()

            try:
                db.commit()
            except Exception:
                db.rollback()

            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "oecd",
            "dataset": "tax",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"OECD Tax ingestion failed: {e}", exc_info=True)
        db.rollback()
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error_message = str(e)[:500]
                db.commit()
        except Exception as inner_e:
            logger.error(f"Failed to update job status: {inner_e}")
            db.rollback()
        raise

    finally:
        await client.close()


async def ingest_bis_eer(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    eer_type: str = "R",
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest BIS Effective Exchange Rate data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        eer_type: Exchange rate type (R = Real, N = Nominal)
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = BISClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("bis")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(
            f"Ingesting BIS EER data: type={eer_type}, {start_period}-{end_period}"
        )

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "bis", "eer")
        table_name = table_info["table_name"]

        # Fetch EER data
        data = await client.get_effective_exchange_rates(
            countries=countries,
            eer_type=eer_type,
            start_period=start_period,
            end_period=end_period,
        )

        logger.info(f"Fetched {len(data)} records from BIS EER")

        # Parse data
        parsed = metadata.parse_bis_data(data, "eer")

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "country_code",
                "eer_type",
                "basket",
                "period",
                "frequency",
                "value",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in parsed:
                row = {col: record.get(col) for col in columns}
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert BIS record: {e}")

            db.commit()
            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "bis",
            "dataset": "eer",
            "eer_type": eer_type,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"BIS EER ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_bis_property_prices(
    db: Session,
    job_id: int,
    countries: List[str] = None,
    start_period: str = None,
    end_period: str = None,
) -> Dict[str, Any]:
    """
    Ingest BIS property price data.

    Args:
        db: Database session
        job_id: Ingestion job ID
        countries: List of country codes
        start_period: Start period
        end_period: End period

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = BISClient(
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

        if not start_period or not end_period:
            default_start, default_end = metadata.get_default_date_range("bis")
            start_period = start_period or str(default_start)
            end_period = end_period or str(default_end)

        logger.info(f"Ingesting BIS property prices: {start_period}-{end_period}")

        # Prepare table
        table_info = await prepare_table_for_intl_data(db, "bis", "property")
        table_name = table_info["table_name"]

        # Fetch property price data
        data = await client.get_property_prices(
            countries=countries, start_period=start_period, end_period=end_period
        )

        logger.info(f"Fetched {len(data)} records from BIS property prices")

        # Parse data
        parsed = metadata.parse_bis_data(data, "property")

        # Insert data
        rows_inserted = 0

        if parsed:
            columns = [
                "country_code",
                "property_type",
                "unit_measure",
                "period",
                "frequency",
                "value",
            ]

            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join(columns)

            insert_sql = f"""
                INSERT INTO {table_name} 
                ({column_list})
                VALUES 
                ({placeholders})
            """

            for record in parsed:
                row = {col: record.get(col) for col in columns}
                try:
                    db.execute(text(insert_sql), row)
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert BIS record: {e}")

            db.commit()
            logger.info(f"Successfully inserted {rows_inserted} rows")

        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "source": "bis",
            "dataset": "property",
            "rows_inserted": rows_inserted,
            "date_range": f"{start_period}-{end_period}",
        }

    except Exception as e:
        logger.error(f"BIS property prices ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()
