"""
FCC Broadband ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for FCC broadband coverage and provider datasets.

Follows project rules:
- Job tracking via ingestion_jobs table
- Bounded concurrency via semaphores
- Parameterized SQL queries
- Typed database columns
- Exponential backoff with jitter
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.fcc_broadband.client import FCCBroadbandClient, STATE_FIPS, US_STATES
from app.sources.fcc_broadband import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_fcc_data(db: Session, dataset: str) -> Dict[str, Any]:
    """
    Prepare database table for FCC broadband data ingestion.

    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry

    Args:
        db: Database session
        dataset: Dataset identifier (broadband_coverage, broadband_summary, providers)

    Returns:
        Dictionary with table_name

    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)

        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for FCC {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)

        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()

        # 4. Register in dataset_registry
        dataset_id = f"fcc_{dataset}"

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
                source="fcc_broadband",
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
        logger.error(f"Failed to prepare table for FCC data: {e}")
        raise


async def ingest_state_coverage(
    db: Session, job_id: int, state_code: str, include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage data for a single state.

    Uses streaming batch insertion to avoid memory issues with large states.

    Args:
        db: Database session
        job_id: Ingestion job ID (MANDATORY per rules)
        state_code: 2-letter state code (e.g., "CA", "NY")
        include_summary: Also generate summary statistics

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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

        # Convert state code to FIPS
        state_code_upper = state_code.upper()
        state_fips = STATE_FIPS.get(state_code_upper)
        if not state_fips:
            raise ValueError(f"Invalid state code: {state_code}")

        state_name = metadata.STATE_NAMES.get(state_fips, state_code)

        logger.info(f"Ingesting FCC broadband coverage for {state_name} ({state_code})")

        # Prepare tables
        coverage_table_info = await prepare_table_for_fcc_data(db, "broadband_coverage")
        coverage_table = coverage_table_info["table_name"]

        summary_table = None
        if include_summary:
            summary_table_info = await prepare_table_for_fcc_data(
                db, "broadband_summary"
            )
            summary_table = summary_table_info["table_name"]

        # Streaming batch insertion - process and insert each batch immediately
        # This avoids memory issues with large states like California (5M+ records)
        offset = 0
        batch_size = 50000
        rows_inserted = 0
        total_fetched = 0

        # For summary statistics, we'll track aggregates across batches
        summary_stats = {
            "providers": set(),
            "technologies": set(),
            "max_down": 0,
            "max_up": 0,
            "speed_sum": 0,
            "record_count": 0,
        }

        while True:
            logger.info(f"Fetching FCC data for {state_code} (offset={offset})")

            records = await client.fetch_fixed_broadband_data(
                state_abbr=state_code_upper, limit=batch_size, offset=offset
            )

            if not records:
                break

            records_in_batch = len(records)
            total_fetched += records_in_batch
            logger.info(f"Fetched {records_in_batch} records (total: {total_fetched})")

            # Parse this batch
            parsed_batch = metadata.parse_broadband_coverage_response(
                records,
                geography_type="state",
                geography_id=state_fips,
                geography_name=state_name,
            )

            # Clear raw records from memory ASAP
            del records

            # Insert this batch immediately
            if parsed_batch:
                batch_inserted = await _insert_coverage_data(
                    db, coverage_table, parsed_batch
                )
                rows_inserted += batch_inserted

                # Update summary statistics for this batch
                for rec in parsed_batch:
                    if rec.get("provider_id"):
                        summary_stats["providers"].add(rec["provider_id"])
                    if rec.get("technology_code"):
                        summary_stats["technologies"].add(rec["technology_code"])
                    down_speed = rec.get("max_advertised_down_mbps") or 0
                    up_speed = rec.get("max_advertised_up_mbps") or 0
                    if down_speed > summary_stats["max_down"]:
                        summary_stats["max_down"] = down_speed
                    if up_speed > summary_stats["max_up"]:
                        summary_stats["max_up"] = up_speed
                    summary_stats["speed_sum"] += down_speed
                    summary_stats["record_count"] += 1

                # Clear batch from memory
                del parsed_batch

            # Check if we got a partial batch (means we're done)
            if records_in_batch < batch_size:
                break

            offset += batch_size

        # Generate summary if requested
        summary_inserted = 0
        if include_summary and summary_stats["record_count"] > 0:
            # Build summary record from aggregated stats
            tech_codes = summary_stats["technologies"]
            summary_record = {
                "geography_type": "state",
                "geography_id": state_fips,
                "geography_name": state_name,
                "total_providers": len(summary_stats["providers"]),
                "total_technologies": len(tech_codes),
                "fiber_available": "50" in tech_codes,
                "cable_available": "40" in tech_codes or "41" in tech_codes,
                "dsl_available": "10" in tech_codes or "20" in tech_codes,
                "fixed_wireless_available": "70" in tech_codes,
                "satellite_available": "60" in tech_codes,
                "mobile_5g_available": "71" in tech_codes,
                "max_speed_down_mbps": summary_stats["max_down"],
                "max_speed_up_mbps": summary_stats["max_up"],
                "avg_speed_down_mbps": summary_stats["speed_sum"]
                / summary_stats["record_count"]
                if summary_stats["record_count"] > 0
                else 0,
                "broadband_coverage_pct": None,  # Would need population data
                "gigabit_coverage_pct": None,  # Would need more analysis
                "provider_competition": "high"
                if len(summary_stats["providers"]) > 10
                else ("medium" if len(summary_stats["providers"]) > 3 else "low"),
                "data_date": None,
            }
            summary_inserted = await _insert_summary_data(
                db, summary_table, [summary_record]
            )

        # Update job status
        total_rows = rows_inserted + summary_inserted
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(
                    f"Job {job_id}: No FCC broadband data returned for {state_code}"
                )
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()

        return {
            "state_code": state_code,
            "state_name": state_name,
            "coverage_table": coverage_table,
            "summary_table": summary_table,
            "coverage_rows_inserted": rows_inserted,
            "summary_rows_inserted": summary_inserted,
            "total_records_fetched": total_fetched,
        }

    except Exception as e:
        logger.error(
            f"FCC state coverage ingestion failed for {state_code}: {e}", exc_info=True
        )

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


async def ingest_multiple_states(
    db: Session, job_id: int, state_codes: List[str], include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage for multiple states.

    Uses bounded concurrency to respect rate limits.

    Args:
        db: Database session
        job_id: Ingestion job ID
        state_codes: List of state codes to ingest
        include_summary: Generate summary statistics

    Returns:
        Aggregated ingestion results
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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
            f"Ingesting FCC broadband for {len(state_codes)} states: {state_codes}"
        )

        # Prepare tables once
        coverage_table_info = await prepare_table_for_fcc_data(db, "broadband_coverage")
        coverage_table = coverage_table_info["table_name"]

        summary_table = None
        if include_summary:
            summary_table_info = await prepare_table_for_fcc_data(
                db, "broadband_summary"
            )
            summary_table = summary_table_info["table_name"]

        total_coverage_rows = 0
        total_summary_rows = 0
        states_processed = 0
        failed_states = []

        # Process states with bounded concurrency
        semaphore = asyncio.Semaphore(settings.max_concurrency)

        async def process_state(state_code: str) -> Dict[str, Any]:
            async with semaphore:
                state_code_upper = state_code.upper()
                state_fips = STATE_FIPS.get(state_code_upper)
                if not state_fips:
                    logger.warning(f"Invalid state code: {state_code}")
                    return {"state": state_code, "error": "Invalid state code"}

                state_name = metadata.STATE_NAMES.get(state_fips, state_code)

                try:
                    logger.info(f"Processing {state_name} ({state_code})")

                    # Streaming batch approach - process and insert each batch immediately
                    offset = 0
                    batch_size = 50000
                    coverage_rows = 0
                    total_fetched = 0

                    # Track summary stats across batches
                    summary_stats = {
                        "providers": set(),
                        "technologies": set(),
                        "max_down": 0,
                        "max_up": 0,
                        "speed_sum": 0,
                        "record_count": 0,
                    }

                    while True:
                        records = await client.fetch_fixed_broadband_data(
                            state_abbr=state_code_upper, limit=batch_size, offset=offset
                        )

                        if not records:
                            break

                        records_in_batch = len(records)
                        total_fetched += records_in_batch

                        # Parse this batch
                        parsed_batch = metadata.parse_broadband_coverage_response(
                            records,
                            geography_type="state",
                            geography_id=state_fips,
                            geography_name=state_name,
                        )

                        # Clear raw records immediately
                        del records

                        # Insert batch immediately
                        if parsed_batch:
                            batch_inserted = await _insert_coverage_data(
                                db, coverage_table, parsed_batch
                            )
                            coverage_rows += batch_inserted

                            # Update summary stats
                            for rec in parsed_batch:
                                if rec.get("provider_id"):
                                    summary_stats["providers"].add(rec["provider_id"])
                                if rec.get("technology_code"):
                                    summary_stats["technologies"].add(
                                        rec["technology_code"]
                                    )
                                down_speed = rec.get("max_advertised_down_mbps") or 0
                                up_speed = rec.get("max_advertised_up_mbps") or 0
                                if down_speed > summary_stats["max_down"]:
                                    summary_stats["max_down"] = down_speed
                                if up_speed > summary_stats["max_up"]:
                                    summary_stats["max_up"] = up_speed
                                summary_stats["speed_sum"] += down_speed
                                summary_stats["record_count"] += 1

                            del parsed_batch

                        if records_in_batch < batch_size:
                            break

                        offset += batch_size

                    summary_rows = 0
                    if include_summary and summary_stats["record_count"] > 0:
                        tech_codes = summary_stats["technologies"]
                        summary_record = {
                            "geography_type": "state",
                            "geography_id": state_fips,
                            "geography_name": state_name,
                            "total_providers": len(summary_stats["providers"]),
                            "total_technologies": len(tech_codes),
                            "fiber_available": "50" in tech_codes,
                            "cable_available": "40" in tech_codes or "41" in tech_codes,
                            "dsl_available": "10" in tech_codes or "20" in tech_codes,
                            "fixed_wireless_available": "70" in tech_codes,
                            "satellite_available": "60" in tech_codes,
                            "mobile_5g_available": "71" in tech_codes,
                            "max_speed_down_mbps": summary_stats["max_down"],
                            "max_speed_up_mbps": summary_stats["max_up"],
                            "avg_speed_down_mbps": summary_stats["speed_sum"]
                            / summary_stats["record_count"],
                            "broadband_coverage_pct": None,
                            "gigabit_coverage_pct": None,
                            "provider_competition": "high"
                            if len(summary_stats["providers"]) > 10
                            else (
                                "medium"
                                if len(summary_stats["providers"]) > 3
                                else "low"
                            ),
                            "data_date": None,
                        }
                        summary_rows = await _insert_summary_data(
                            db, summary_table, [summary_record]
                        )

                    logger.info(
                        f"Completed {state_name}: {coverage_rows} coverage rows, {summary_rows} summary rows"
                    )

                    return {
                        "state": state_code,
                        "coverage_rows": coverage_rows,
                        "summary_rows": summary_rows,
                        "records_fetched": total_fetched,
                    }

                except Exception as e:
                    logger.error(f"Failed to process {state_code}: {e}")
                    return {"state": state_code, "error": str(e)}

        # Process all states
        results = []
        for state_code in state_codes:
            result = await process_state(state_code)
            results.append(result)

            if "error" in result:
                failed_states.append(state_code)
            else:
                states_processed += 1
                total_coverage_rows += result.get("coverage_rows", 0)
                total_summary_rows += result.get("summary_rows", 0)

        # Update job status
        total_rows = total_coverage_rows + total_summary_rows
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(
                    f"Job {job_id}: No FCC broadband data returned for any state"
                )
            elif failed_states:
                job.status = JobStatus.SUCCESS  # Partial success
                job.error_message = f"Failed states: {failed_states}"
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()

        return {
            "states_requested": len(state_codes),
            "states_processed": states_processed,
            "states_failed": failed_states,
            "coverage_table": coverage_table,
            "summary_table": summary_table,
            "total_coverage_rows": total_coverage_rows,
            "total_summary_rows": total_summary_rows,
            "details": results,
        }

    except Exception as e:
        logger.error(f"FCC multi-state ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


async def ingest_all_states(
    db: Session, job_id: int, include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage for all 50 states + DC.

    This is a large operation that may take 30-60 minutes.

    Args:
        db: Database session
        job_id: Ingestion job ID
        include_summary: Generate summary statistics

    Returns:
        Ingestion results for all states
    """
    return await ingest_multiple_states(
        db=db, job_id=job_id, state_codes=US_STATES, include_summary=include_summary
    )


async def ingest_county_coverage(
    db: Session, job_id: int, county_fips: str, include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage for a specific county.

    Args:
        db: Database session
        job_id: Ingestion job ID
        county_fips: 5-digit county FIPS code (e.g., "06001" for Alameda, CA)
        include_summary: Generate summary statistics

    Returns:
        Ingestion results
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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

        logger.info(f"Ingesting FCC broadband coverage for county {county_fips}")

        # Prepare tables
        coverage_table_info = await prepare_table_for_fcc_data(db, "broadband_coverage")
        coverage_table = coverage_table_info["table_name"]

        summary_table = None
        if include_summary:
            summary_table_info = await prepare_table_for_fcc_data(
                db, "broadband_summary"
            )
            summary_table = summary_table_info["table_name"]

        # Fetch county data
        county_data = await client.fetch_county_summary(county_fips)

        # Also fetch detailed provider data if available
        provider_data = await client.fetch_providers_by_county(county_fips)

        # Combine and parse
        all_records = []
        if isinstance(county_data, dict) and county_data.get("data"):
            all_records.extend(county_data["data"])
        if isinstance(provider_data, dict) and provider_data.get("data"):
            all_records.extend(provider_data["data"])

        parsed_records = metadata.parse_broadband_coverage_response(
            all_records,
            geography_type="county",
            geography_id=county_fips,
            geography_name=None,  # Would need lookup
        )

        rows_inserted = 0
        if parsed_records:
            rows_inserted = await _insert_coverage_data(
                db, coverage_table, parsed_records
            )

        summary_inserted = 0
        if include_summary and parsed_records:
            summary_record = metadata.parse_broadband_summary(
                parsed_records, geography_type="county", geography_id=county_fips
            )
            if summary_record:
                summary_inserted = await _insert_summary_data(
                    db, summary_table, [summary_record]
                )

        # Update job status
        total_rows = rows_inserted + summary_inserted
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
                logger.warning(
                    f"Job {job_id}: No FCC broadband data returned for county {county_fips}"
                )
            else:
                job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = total_rows
            db.commit()

        return {
            "county_fips": county_fips,
            "coverage_table": coverage_table,
            "summary_table": summary_table,
            "coverage_rows_inserted": rows_inserted,
            "summary_rows_inserted": summary_inserted,
        }

    except Exception as e:
        logger.error(f"FCC county coverage ingestion failed: {e}", exc_info=True)

        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


# ========== Data Insertion Functions ==========


async def _insert_coverage_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """
    Insert broadband coverage data with upsert logic.

    Uses parameterized queries per project rules.
    """
    if not records:
        return 0

    logger.info(f"Inserting {len(records)} coverage records into {table_name}")

    columns = [
        "geography_type",
        "geography_id",
        "geography_name",
        "provider_id",
        "provider_name",
        "brand_name",
        "technology_code",
        "technology_name",
        "max_advertised_down_mbps",
        "max_advertised_up_mbps",
        "speed_tier",
        "business_service",
        "consumer_service",
        "data_date",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])

    # Columns to update on conflict
    update_cols = [
        "geography_name",
        "provider_name",
        "brand_name",
        "technology_name",
        "max_advertised_down_mbps",
        "max_advertised_up_mbps",
        "speed_tier",
        "business_service",
        "consumer_service",
        "data_date",
    ]
    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # ON CONFLICT uses the unique index
    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (geography_type, geography_id, provider_id, technology_code)
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

        if (i + batch_size) % 5000 == 0:
            logger.info(f"Inserted {rows_inserted}/{len(records)} coverage rows")

    logger.info(f"Successfully inserted {rows_inserted} coverage rows")
    return rows_inserted


async def _insert_summary_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """
    Insert broadband summary data with upsert logic.
    """
    if not records:
        return 0

    logger.info(f"Inserting {len(records)} summary records into {table_name}")

    columns = [
        "geography_type",
        "geography_id",
        "geography_name",
        "total_providers",
        "total_technologies",
        "fiber_available",
        "cable_available",
        "dsl_available",
        "fixed_wireless_available",
        "satellite_available",
        "mobile_5g_available",
        "max_speed_down_mbps",
        "max_speed_up_mbps",
        "avg_speed_down_mbps",
        "broadband_coverage_pct",
        "gigabit_coverage_pct",
        "provider_competition",
        "data_date",
    ]

    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join([f'"{col}"' for col in columns])

    update_cols = [
        col for col in columns if col not in ("geography_type", "geography_id")
    ]
    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    insert_sql = f"""
        INSERT INTO {table_name} 
        ({column_list})
        VALUES 
        ({placeholders})
        ON CONFLICT (geography_type, geography_id)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
    """

    for record in records:
        db.execute(text(insert_sql), record)

    db.commit()

    logger.info(f"Successfully inserted {len(records)} summary rows")
    return len(records)
