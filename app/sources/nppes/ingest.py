"""
NPPES NPI Registry ingestion orchestration.

High-level functions that coordinate data fetching from the NPPES API,
table creation, parsing, and data loading into PostgreSQL.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.nppes.client import NPPESClient
from app.sources.nppes import metadata

logger = logging.getLogger(__name__)


async def ingest_nppes_providers(
    db: Session,
    job_id: int,
    states: Optional[List[str]] = None,
    taxonomy_codes: Optional[List[str]] = None,
    taxonomy_description: Optional[str] = None,
    enumeration_type: Optional[str] = None,
    city: Optional[str] = None,
    postal_code: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Ingest provider data from the NPPES NPI Registry.

    Fetches providers matching the given filters, parses the nested
    API responses, and upserts into the nppes_providers table.

    Supports filtering by state list and taxonomy codes. When multiple
    states or taxonomy codes are provided, iterates through each
    combination and aggregates results.

    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        states: List of state abbreviations (e.g., ["CA", "NY"])
        taxonomy_codes: List of taxonomy codes to filter by
        taxonomy_description: Taxonomy description search string
        enumeration_type: NPI-1 (individual) or NPI-2 (organization)
        city: City name filter
        postal_code: ZIP code filter
        limit: Maximum total records to ingest

    Returns:
        Dictionary with ingestion results
    """
    start_time = datetime.utcnow()
    table_name = metadata.TABLE_NAME

    # Update job to RUNNING
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job:
        job.status = JobStatus.RUNNING
        job.started_at = start_time
        db.commit()

    try:
        # 1. Create table if not exists
        logger.info(f"Preparing table {table_name}")
        db.execute(text(metadata.CREATE_TABLE_SQL))
        db.commit()

        # 2. Register dataset
        _register_dataset(db)

        # 3. Initialize NPPES client
        client = NPPESClient(max_concurrency=2, max_retries=3)

        try:
            # 4. Build search combinations
            all_records = []
            search_combos = _build_search_combinations(
                states=states,
                taxonomy_codes=taxonomy_codes,
                taxonomy_description=taxonomy_description,
                enumeration_type=enumeration_type,
                city=city,
                postal_code=postal_code,
            )

            # 5. Execute searches
            for combo in search_combos:
                # Calculate remaining limit
                remaining_limit = None
                if limit:
                    remaining_limit = limit - len(all_records)
                    if remaining_limit <= 0:
                        break

                logger.info(f"[nppes] Searching with params: {combo}")

                results = await client.search_all_pages(
                    state=combo.get("state"),
                    city=combo.get("city"),
                    postal_code=combo.get("postal_code"),
                    taxonomy_description=combo.get("taxonomy_description"),
                    enumeration_type=combo.get("enumeration_type"),
                    max_records=remaining_limit,
                )

                # Parse results
                for result in results:
                    row = metadata.parse_provider_record(result)
                    all_records.append(row)

                logger.info(
                    f"[nppes] Got {len(results)} results for combo, "
                    f"total so far: {len(all_records)}"
                )

            # 6. Deduplicate by NPI (in case overlapping taxonomy queries)
            seen_npis = set()
            unique_records = []
            for rec in all_records:
                npi = rec.get("npi")
                if npi and npi not in seen_npis:
                    seen_npis.add(npi)
                    unique_records.append(rec)

            logger.info(
                f"[nppes] {len(all_records)} total records, "
                f"{len(unique_records)} unique NPIs"
            )

            # 7. Insert/upsert data
            rows_inserted = 0
            if unique_records:
                rows_inserted = _batch_upsert_providers(
                    db, table_name, unique_records
                )

            # 8. Calculate results
            duration = (datetime.utcnow() - start_time).total_seconds()

            # 9. Update job status
            if job:
                if rows_inserted == 0:
                    job.status = JobStatus.FAILED
                    job.error_message = (
                        "Ingestion completed but no rows were inserted"
                    )
                    logger.warning(
                        f"Job {job_id}: No NPPES providers returned for given filters"
                    )
                else:
                    job.status = JobStatus.SUCCESS
                job.completed_at = datetime.utcnow()
                job.rows_inserted = rows_inserted
                db.commit()

            logger.info(
                f"Successfully ingested {rows_inserted} NPPES providers "
                f"into {table_name} in {duration:.2f}s"
            )

            return {
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "duration_seconds": duration,
                "filters": {
                    "states": states,
                    "taxonomy_codes": taxonomy_codes,
                    "taxonomy_description": taxonomy_description,
                    "enumeration_type": enumeration_type,
                },
            }

        finally:
            await client.close()

    except Exception as e:
        logger.error(f"NPPES ingestion failed: {e}", exc_info=True)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        raise


def _build_search_combinations(
    states: Optional[List[str]] = None,
    taxonomy_codes: Optional[List[str]] = None,
    taxonomy_description: Optional[str] = None,
    enumeration_type: Optional[str] = None,
    city: Optional[str] = None,
    postal_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build list of search parameter combinations.

    The NPPES API does not support multiple taxonomy codes in a single
    request, so we need to iterate. Similarly, state-level queries
    are more manageable than national queries.

    If both taxonomy_codes and taxonomy_description are provided,
    taxonomy_codes takes precedence (each code is looked up by its
    description from the metadata mapping).

    Args:
        states: List of state abbreviations
        taxonomy_codes: List of taxonomy codes
        taxonomy_description: Taxonomy description string
        enumeration_type: NPI-1 or NPI-2
        city: City name
        postal_code: ZIP code

    Returns:
        List of param dicts, one per search to execute
    """
    base_params = {}
    if enumeration_type:
        base_params["enumeration_type"] = enumeration_type
    if city:
        base_params["city"] = city
    if postal_code:
        base_params["postal_code"] = postal_code

    combos = []

    # Resolve taxonomy codes to descriptions (NPPES API searches by description)
    # The API does partial matching on taxonomy_description, so we use
    # short, distinctive terms rather than the full classification string.
    tax_descriptions = []
    if taxonomy_codes:
        for code in taxonomy_codes:
            desc = metadata.TAXONOMY_SEARCH_TERMS.get(code)
            if desc:
                tax_descriptions.append(desc)
            else:
                # Fallback to the full description from MEDSPA_TAXONOMY_CODES
                full_desc = metadata.MEDSPA_TAXONOMY_CODES.get(code)
                if full_desc:
                    tax_descriptions.append(full_desc)
                else:
                    logger.warning(f"[nppes] Unknown taxonomy code: {code}, skipping")
                    continue
    elif taxonomy_description:
        tax_descriptions = [taxonomy_description]

    # Build combinations
    state_list = states if states else [None]
    tax_list = tax_descriptions if tax_descriptions else [None]

    for state in state_list:
        for tax_desc in tax_list:
            combo = dict(base_params)
            if state:
                combo["state"] = state.upper()
            if tax_desc:
                combo["taxonomy_description"] = tax_desc
            combos.append(combo)

    # If no combos were built, add one empty combo
    if not combos:
        combos.append(base_params)

    logger.info(f"[nppes] Built {len(combos)} search combinations")
    return combos


def _batch_upsert_providers(
    db: Session,
    table_name: str,
    records: List[Dict[str, Any]],
    batch_size: int = 500,
) -> int:
    """
    Batch upsert provider records into the database.

    Uses ON CONFLICT (npi) DO UPDATE to handle re-ingestion gracefully.

    Args:
        db: Database session
        table_name: Target table name
        records: List of parsed provider dicts
        batch_size: Rows per batch

    Returns:
        Number of rows upserted
    """
    if not records:
        return 0

    columns = metadata.COLUMN_NAMES
    update_cols = metadata.UPDATE_COLUMNS

    # Build upsert SQL
    col_list = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    update_set = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in update_cols]
    )

    upsert_sql = (
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (npi) DO UPDATE SET {update_set}, "
        f"ingestion_timestamp = CURRENT_TIMESTAMP"
    )

    total_upserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Ensure all records have all keys
        normalized = []
        for rec in batch:
            row = {}
            for col in columns:
                row[col] = rec.get(col)
            normalized.append(row)

        db.execute(text(upsert_sql), normalized)
        db.commit()
        total_upserted += len(batch)

        logger.debug(
            f"Upserted batch of {len(batch)} providers "
            f"({total_upserted}/{len(records)} total)"
        )

    return total_upserted


def _register_dataset(db: Session) -> None:
    """
    Register NPPES dataset in dataset_registry if not already registered.

    Args:
        db: Database session
    """
    existing = (
        db.query(DatasetRegistry)
        .filter(DatasetRegistry.table_name == metadata.TABLE_NAME)
        .first()
    )

    if existing:
        logger.info(f"Dataset {metadata.TABLE_NAME} already registered")
        existing.last_updated_at = datetime.utcnow()
        db.commit()
    else:
        dataset = DatasetRegistry(
            source="nppes",
            dataset_id=metadata.DATASET_ID,
            table_name=metadata.TABLE_NAME,
            display_name=metadata.DISPLAY_NAME,
            description=metadata.DESCRIPTION,
            source_metadata={
                "source_url": metadata.SOURCE_URL,
                "api_version": "2.1",
                "no_api_key_required": True,
            },
        )
        db.add(dataset)
        db.commit()
        logger.info(f"Registered dataset {metadata.TABLE_NAME}")
