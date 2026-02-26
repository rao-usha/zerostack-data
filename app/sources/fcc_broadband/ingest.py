"""
FCC Broadband ingestion orchestration.

Uses Socrata $select/$group aggregation to fetch pre-aggregated provider-level
data per state (~200-600 rows) instead of millions of raw census-block records.

Follows project rules:
- Job tracking via ingestion_jobs table
- Bounded concurrency via semaphores
- Parameterized SQL queries
- Typed database columns
- Exponential backoff with jitter
"""

import logging
from typing import Dict, Any, List
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
    """
    try:
        table_name = metadata.generate_table_name(dataset)

        logger.info(f"Creating table {table_name} for FCC {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)

        db.execute(text(create_sql))
        db.commit()

        dataset_id = f"fcc_{dataset}"

        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
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


async def _fetch_state_aggregated(
    client: FCCBroadbandClient, state_code: str, state_fips: str, state_name: str
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated broadband data for a state via Socrata GROUP BY.

    Returns parsed coverage records ready for insertion.
    One API call returns ~200-600 rows per state instead of millions.
    """
    all_parsed = []
    offset = 0
    page_size = 5000

    while True:
        raw_records = await client.fetch_state_broadband_aggregated(
            state_abbr=state_code, limit=page_size, offset=offset
        )

        if not raw_records:
            break

        # Parse aggregated records into coverage format
        for rec in raw_records:
            provider_id = rec.get("frn") or ""
            provider_name = rec.get("providername") or ""
            brand_name = rec.get("dbaname") or ""
            tech_code = str(rec.get("techcode") or "90")

            max_down = metadata._safe_float(rec.get("max_download"))
            max_up = metadata._safe_float(rec.get("max_upload"))

            if not provider_id or not provider_name:
                continue

            all_parsed.append({
                "geography_type": "state",
                "geography_id": state_fips,
                "geography_name": state_name,
                "provider_id": provider_id,
                "provider_name": provider_name,
                "brand_name": brand_name,
                "technology_code": tech_code,
                "technology_name": metadata.get_technology_name(tech_code),
                "max_advertised_down_mbps": max_down,
                "max_advertised_up_mbps": max_up,
                "speed_tier": metadata.classify_speed_tier(max_down),
                "business_service": None,
                "consumer_service": None,
                "data_date": None,
            })

        if len(raw_records) < page_size:
            break
        offset += page_size

    return all_parsed


def _build_summary_from_coverage(
    records: List[Dict[str, Any]], state_fips: str, state_name: str
) -> Dict[str, Any]:
    """Build a summary record from parsed coverage records."""
    providers = set()
    tech_codes = set()
    max_down = 0
    max_up = 0
    speed_sum = 0

    for r in records:
        if r.get("provider_id"):
            providers.add(r["provider_id"])
        tc = r.get("technology_code")
        if tc:
            tech_codes.add(tc)
        down = r.get("max_advertised_down_mbps") or 0
        up = r.get("max_advertised_up_mbps") or 0
        if down > max_down:
            max_down = down
        if up > max_up:
            max_up = up
        speed_sum += down

    total_providers = len(providers)
    avg_down = speed_sum / len(records) if records else 0

    # Estimate broadband/gigabit percentages from provider offerings
    broadband_count = sum(
        1 for r in records if (r.get("max_advertised_down_mbps") or 0) >= 25
    )
    gigabit_count = sum(
        1 for r in records if (r.get("max_advertised_down_mbps") or 0) >= 1000
    )
    broadband_pct = (broadband_count / len(records) * 100) if records else None
    gigabit_pct = (gigabit_count / len(records) * 100) if records else None

    return {
        "geography_type": "state",
        "geography_id": state_fips,
        "geography_name": state_name,
        "total_providers": total_providers,
        "total_technologies": len(tech_codes),
        "fiber_available": "50" in tech_codes,
        "cable_available": "40" in tech_codes or "41" in tech_codes,
        "dsl_available": "10" in tech_codes or "20" in tech_codes,
        "fixed_wireless_available": "70" in tech_codes,
        "satellite_available": "60" in tech_codes,
        "mobile_5g_available": "71" in tech_codes,
        "max_speed_down_mbps": max_down,
        "max_speed_up_mbps": max_up,
        "avg_speed_down_mbps": round(avg_down, 2),
        "broadband_coverage_pct": round(broadband_pct, 2) if broadband_pct else None,
        "gigabit_coverage_pct": round(gigabit_pct, 2) if gigabit_pct else None,
        "provider_competition": metadata.classify_competition(total_providers),
        "data_date": None,
    }


async def ingest_state_coverage(
    db: Session, job_id: int, state_code: str, include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage data for a single state.

    Uses Socrata aggregation to get ~200-600 rows per state in one API call.
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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
            summary_table_info = await prepare_table_for_fcc_data(db, "broadband_summary")
            summary_table = summary_table_info["table_name"]

        # Fetch aggregated data (one API call, ~200-600 rows)
        parsed_records = await _fetch_state_aggregated(
            client, state_code_upper, state_fips, state_name
        )

        logger.info(
            f"Fetched {len(parsed_records)} aggregated records for {state_name}"
        )

        # Insert coverage data
        rows_inserted = 0
        if parsed_records:
            rows_inserted = await _insert_coverage_data(db, coverage_table, parsed_records)

        # Generate summary
        summary_inserted = 0
        if include_summary and parsed_records:
            summary_record = _build_summary_from_coverage(
                parsed_records, state_fips, state_name
            )
            summary_inserted = await _insert_summary_data(
                db, summary_table, [summary_record]
            )

        # Update job status
        total_rows = rows_inserted + summary_inserted
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
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
        }

    except Exception as e:
        logger.error(
            f"FCC state coverage ingestion failed for {state_code}: {e}", exc_info=True
        )
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

    Uses Socrata aggregation — one API call per state, ~200-600 rows each.
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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
            f"Ingesting FCC broadband for {len(state_codes)} states: {state_codes}"
        )

        # Prepare tables once
        coverage_table_info = await prepare_table_for_fcc_data(db, "broadband_coverage")
        coverage_table = coverage_table_info["table_name"]

        summary_table = None
        if include_summary:
            summary_table_info = await prepare_table_for_fcc_data(db, "broadband_summary")
            summary_table = summary_table_info["table_name"]

        total_coverage_rows = 0
        total_summary_rows = 0
        states_processed = 0
        failed_states = []
        results = []

        for state_code in state_codes:
            state_code_upper = state_code.upper()
            state_fips = STATE_FIPS.get(state_code_upper)
            if not state_fips:
                logger.warning(f"Invalid state code: {state_code}")
                failed_states.append(state_code)
                results.append({"state": state_code, "error": "Invalid state code"})
                continue

            state_name = metadata.STATE_NAMES.get(state_fips, state_code)

            try:
                logger.info(f"Processing {state_name} ({state_code})")

                parsed_records = await _fetch_state_aggregated(
                    client, state_code_upper, state_fips, state_name
                )

                coverage_rows = 0
                if parsed_records:
                    coverage_rows = await _insert_coverage_data(
                        db, coverage_table, parsed_records
                    )

                summary_rows = 0
                if include_summary and parsed_records:
                    summary_record = _build_summary_from_coverage(
                        parsed_records, state_fips, state_name
                    )
                    summary_rows = await _insert_summary_data(
                        db, summary_table, [summary_record]
                    )

                logger.info(
                    f"Completed {state_name}: {coverage_rows} coverage, "
                    f"{summary_rows} summary rows"
                )

                states_processed += 1
                total_coverage_rows += coverage_rows
                total_summary_rows += summary_rows
                results.append({
                    "state": state_code,
                    "coverage_rows": coverage_rows,
                    "summary_rows": summary_rows,
                })

            except Exception as e:
                logger.error(f"Failed to process {state_code}: {e}")
                failed_states.append(state_code)
                results.append({"state": state_code, "error": str(e)})

        # Update job status
        total_rows = total_coverage_rows + total_summary_rows
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
            elif failed_states:
                job.status = JobStatus.SUCCESS
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

    With aggregation, this takes ~5-10 minutes instead of hours.
    """
    return await ingest_multiple_states(
        db=db, job_id=job_id, state_codes=US_STATES, include_summary=include_summary
    )


async def ingest_county_coverage(
    db: Session, job_id: int, county_fips: str, include_summary: bool = True
) -> Dict[str, Any]:
    """
    Ingest FCC broadband coverage for a specific county.
    """
    settings = get_settings()

    client = FCCBroadbandClient(
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

        logger.info(f"Ingesting FCC broadband coverage for county {county_fips}")

        # Prepare tables
        coverage_table_info = await prepare_table_for_fcc_data(db, "broadband_coverage")
        coverage_table = coverage_table_info["table_name"]

        summary_table = None
        if include_summary:
            summary_table_info = await prepare_table_for_fcc_data(db, "broadband_summary")
            summary_table = summary_table_info["table_name"]

        # Fetch county data
        county_data = await client.fetch_county_summary(county_fips)
        provider_data = await client.fetch_providers_by_county(county_fips)

        all_records = []
        if isinstance(county_data, dict) and county_data.get("data"):
            all_records.extend(county_data["data"])
        if isinstance(provider_data, dict) and provider_data.get("data"):
            all_records.extend(provider_data["data"])

        parsed_records = metadata.parse_broadband_coverage_response(
            all_records,
            geography_type="county",
            geography_id=county_fips,
            geography_name=None,
        )

        rows_inserted = 0
        if parsed_records:
            rows_inserted = await _insert_coverage_data(db, coverage_table, parsed_records)

        summary_inserted = 0
        if include_summary and parsed_records:
            summary_record = metadata.parse_broadband_summary(
                parsed_records, geography_type="county", geography_id=county_fips
            )
            if summary_record:
                summary_inserted = await _insert_summary_data(
                    db, summary_table, [summary_record]
                )

        total_rows = rows_inserted + summary_inserted
        if job:
            if total_rows == 0:
                job.status = JobStatus.FAILED
                job.error_message = "Ingestion completed but no rows were inserted"
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
    """Insert broadband coverage data with upsert logic."""
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

    logger.info(f"Successfully inserted {rows_inserted} coverage rows")
    return rows_inserted


async def _insert_summary_data(
    db: Session, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """Insert broadband summary data with upsert logic."""
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
