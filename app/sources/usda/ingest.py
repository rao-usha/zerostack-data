"""
USDA data ingestion functions.

Handles fetching, parsing, and inserting agricultural data into PostgreSQL.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from .client import USDAClient
from .metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_usda_record,
    CROP_PRODUCTION_COLUMNS,
)

logger = logging.getLogger(__name__)


async def prepare_usda_table(
    conn, data_type: str, commodity: Optional[str] = None
) -> str:
    """
    Prepare database table for USDA data.

    Args:
        conn: Database connection
        data_type: Type of data (production, prices, livestock)
        commodity: Optional commodity name

    Returns:
        Table name
    """
    table_name = generate_table_name(data_type, commodity)
    create_sql = generate_create_table_sql(data_type, commodity)

    try:
        cursor = conn.cursor()
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Prepared table {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        raise

    return table_name


async def ingest_crop_production(
    conn,
    commodity: str,
    year: int = None,
    state: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    Ingest crop production data.

    Args:
        conn: Database connection
        commodity: Commodity name (CORN, SOYBEANS, etc.)
        year: Year (defaults to current)
        state: State name (optional)
        api_key: USDA API key (or uses env var)

    Returns:
        Number of rows inserted
    """
    if year is None:
        year = datetime.now().year

    client = USDAClient(api_key=api_key)

    try:
        # Fetch production data
        records = await client.get_crop_production(
            commodity=commodity, year=year, state=state, data_item="PRODUCTION"
        )

        if not records:
            logger.warning(f"No production data returned for {commodity} {year}")
            return 0

        # Prepare table
        table_name = await prepare_usda_table(conn, "crop_production")

        # Insert records
        rows_inserted = await _insert_usda_records(conn, table_name, records)

        logger.info(
            f"Ingested {rows_inserted} production records for {commodity} {year}"
        )
        return rows_inserted

    finally:
        await client.close()


async def ingest_crop_all_stats(
    conn,
    commodity: str,
    year: int = None,
    state: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    Ingest all statistics for a crop (production, yield, area, prices).

    Args:
        conn: Database connection
        commodity: Commodity name
        year: Year
        state: State name
        api_key: USDA API key

    Returns:
        Total rows inserted
    """
    if year is None:
        year = datetime.now().year

    client = USDAClient(api_key=api_key)
    total_rows = 0

    try:
        # Prepare table
        table_name = await prepare_usda_table(conn, "crop_production")

        # Fetch each statistic type
        stat_types = ["PRODUCTION", "YIELD", "AREA PLANTED", "AREA HARVESTED"]

        for stat_type in stat_types:
            try:
                records = await client.get_crop_production(
                    commodity=commodity, year=year, state=state, data_item=stat_type
                )

                if records:
                    rows = await _insert_usda_records(conn, table_name, records)
                    total_rows += rows
                    logger.debug(f"Inserted {rows} {stat_type} records for {commodity}")

            except Exception as e:
                logger.warning(f"Error fetching {stat_type} for {commodity}: {e}")
                continue

        # Also try to get prices
        try:
            price_records = await client.get_prices_received(
                commodity=commodity, year=year, state=state
            )
            if price_records:
                rows = await _insert_usda_records(conn, table_name, price_records)
                total_rows += rows
        except Exception as e:
            logger.debug(f"No price data for {commodity}: {e}")

        logger.info(f"Ingested {total_rows} total records for {commodity} {year}")
        return total_rows

    finally:
        await client.close()


async def ingest_livestock_inventory(
    conn,
    commodity: str,
    year: int = None,
    state: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    Ingest livestock inventory data.

    Args:
        conn: Database connection
        commodity: Livestock type (CATTLE, HOGS, etc.)
        year: Year
        state: State name
        api_key: USDA API key

    Returns:
        Number of rows inserted
    """
    if year is None:
        year = datetime.now().year

    client = USDAClient(api_key=api_key)

    try:
        records = await client.get_livestock_inventory(
            commodity=commodity, year=year, state=state
        )

        if not records:
            logger.warning(f"No livestock data returned for {commodity} {year}")
            return 0

        table_name = await prepare_usda_table(conn, "livestock")
        rows_inserted = await _insert_usda_records(conn, table_name, records)

        logger.info(
            f"Ingested {rows_inserted} livestock records for {commodity} {year}"
        )
        return rows_inserted

    finally:
        await client.close()


async def ingest_annual_crops(
    conn, year: int = None, api_key: Optional[str] = None
) -> int:
    """
    Ingest annual production data for all major crops.

    Args:
        conn: Database connection
        year: Year
        api_key: USDA API key

    Returns:
        Total rows inserted
    """
    if year is None:
        year = datetime.now().year

    client = USDAClient(api_key=api_key)
    total_rows = 0

    try:
        table_name = await prepare_usda_table(conn, "crop_production")

        # Fetch annual summary
        records = await client.get_annual_summary(year=year)

        if records:
            rows = await _insert_usda_records(conn, table_name, records)
            total_rows += rows

        logger.info(f"Ingested {total_rows} annual crop records for {year}")
        return total_rows

    finally:
        await client.close()


async def ingest_all_major_crops(
    conn, year: int = None, api_key: Optional[str] = None
) -> Dict[str, int]:
    """
    Ingest data for all major commodities.

    Args:
        conn: Database connection
        year: Year
        api_key: USDA API key

    Returns:
        Dictionary of commodity -> rows inserted
    """
    if year is None:
        year = datetime.now().year

    results = {}
    major_crops = ["CORN", "SOYBEANS", "WHEAT", "COTTON", "RICE"]

    for commodity in major_crops:
        try:
            rows = await ingest_crop_all_stats(
                conn, commodity=commodity, year=year, api_key=api_key
            )
            results[commodity] = rows
        except Exception as e:
            logger.error(f"Error ingesting {commodity}: {e}")
            results[commodity] = 0

    total = sum(results.values())
    logger.info(f"Total USDA crop records ingested for {year}: {total}")

    return results


async def _insert_usda_records(
    conn, table_name: str, records: List[Dict[str, Any]]
) -> int:
    """
    Insert USDA records into database.

    Args:
        conn: Database connection
        table_name: Target table name
        records: Records to insert

    Returns:
        Number of rows inserted
    """
    # Filter to insertable columns
    insert_columns = [
        col
        for col in CROP_PRODUCTION_COLUMNS.keys()
        if col not in ("id", "ingested_at")
    ]

    cursor = conn.cursor()
    rows_inserted = 0

    for record in records:
        parsed = parse_usda_record(record)

        # Skip records without required fields
        if not parsed.get("commodity_desc") or not parsed.get("year"):
            continue

        # Build values
        values = []
        for col in insert_columns:
            values.append(parsed.get(col))

        placeholders = ", ".join(["%s"] * len(insert_columns))
        columns_str = ", ".join(insert_columns)

        # Use ON CONFLICT for upsert
        update_cols = [
            f"{col} = EXCLUDED.{col}"
            for col in insert_columns
            if col
            not in (
                "commodity_desc",
                "year",
                "state_name",
                "statisticcat_desc",
                "reference_period_desc",
                "class_desc",
                "domain_desc",
            )
        ]
        update_str = ", ".join(update_cols) if update_cols else "value = EXCLUDED.value"

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (commodity_desc, year, state_name, statisticcat_desc, 
                        reference_period_desc, COALESCE(class_desc, ''), COALESCE(domain_desc, ''))
            DO UPDATE SET {update_str}
        """

        try:
            cursor.execute(sql, values)
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"Error inserting USDA record: {e}")
            continue

    conn.commit()
    return rows_inserted


# =============================================================================
# Dispatch-compatible wrappers
# =============================================================================
# These wrappers accept (db, job_id, **kwargs) to match the SOURCE_DISPATCH
# interface used by run_ingestion_job(). They extract the raw psycopg2
# connection from the SQLAlchemy session and delegate to the original
# conn-based ingest functions.


async def dispatch_usda_crop(db, job_id: int, commodity: str = "CORN", year: int = None, state: Optional[str] = None, all_stats: bool = True, api_key: Optional[str] = None):
    """Dispatch wrapper for USDA crop ingestion via SOURCE_DISPATCH."""
    conn = db.connection().connection
    if all_stats:
        rows = await ingest_crop_all_stats(conn, commodity, year, state, api_key)
    else:
        rows = await ingest_crop_production(conn, commodity, year, state, api_key)
    return {"rows_inserted": rows}


async def dispatch_usda_livestock(db, job_id: int, commodity: str = "CATTLE", year: int = None, state: Optional[str] = None, api_key: Optional[str] = None):
    """Dispatch wrapper for USDA livestock ingestion via SOURCE_DISPATCH."""
    conn = db.connection().connection
    rows = await ingest_livestock_inventory(conn, commodity, year, state, api_key)
    return {"rows_inserted": rows}


async def dispatch_usda_annual_summary(db, job_id: int, year: int = None, api_key: Optional[str] = None):
    """Dispatch wrapper for USDA annual summary ingestion via SOURCE_DISPATCH."""
    conn = db.connection().connection
    rows = await ingest_annual_crops(conn, year, api_key)
    return {"rows_inserted": rows}


async def dispatch_usda_all_major_crops(db, job_id: int, year: int = None, api_key: Optional[str] = None):
    """Dispatch wrapper for USDA all major crops ingestion via SOURCE_DISPATCH."""
    conn = db.connection().connection
    results = await ingest_all_major_crops(conn, year, api_key)
    return {"rows_inserted": sum(results.values())}
