"""
CFTC COT data ingestion functions.

Handles downloading, parsing, and inserting COT data into PostgreSQL.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from .client import CFTCCOTClient
from .metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_cot_record,
    COT_LEGACY_COLUMNS,
    COT_DISAGGREGATED_COLUMNS,
    COT_TFF_COLUMNS,
)

logger = logging.getLogger(__name__)


async def prepare_cot_table(conn, report_type: str) -> str:
    """
    Prepare database table for COT data.

    Args:
        conn: Database connection
        report_type: Type of COT report

    Returns:
        Table name
    """
    table_name = generate_table_name(report_type)
    create_sql = generate_create_table_sql(report_type)

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


async def ingest_cot_legacy(conn, year: int = None, combined: bool = False) -> int:
    """
    Ingest Legacy COT report data.

    Args:
        conn: Database connection
        year: Year to ingest (defaults to current)
        combined: If True, use futures+options combined report

    Returns:
        Number of rows inserted
    """
    if year is None:
        year = datetime.now().year

    report_type = "legacy_combined" if combined else "legacy_futures"

    client = CFTCCOTClient()

    try:
        # Fetch data
        if combined:
            records = await client.get_legacy_combined(year)
        else:
            records = await client.get_legacy_futures(year)

        if not records:
            logger.warning(f"No COT legacy data returned for {year}")
            return 0

        # Prepare table
        table_name = await prepare_cot_table(conn, report_type)

        # Insert records
        rows_inserted = await _insert_cot_records(
            conn, table_name, records, report_type
        )

        logger.info(f"Ingested {rows_inserted} COT legacy records for {year}")
        return rows_inserted

    finally:
        await client.close()


async def ingest_cot_disaggregated(
    conn, year: int = None, combined: bool = False
) -> int:
    """
    Ingest Disaggregated COT report data.

    Shows positions by trader category:
    - Producer/Merchant/Processor/User
    - Swap Dealers
    - Managed Money (hedge funds, CTAs)
    - Other Reportables

    Args:
        conn: Database connection
        year: Year to ingest
        combined: If True, use futures+options combined report

    Returns:
        Number of rows inserted
    """
    if year is None:
        year = datetime.now().year

    report_type = "disaggregated_combined" if combined else "disaggregated_futures"

    client = CFTCCOTClient()

    try:
        if combined:
            records = await client.get_disaggregated_combined(year)
        else:
            records = await client.get_disaggregated_futures(year)

        if not records:
            logger.warning(f"No COT disaggregated data returned for {year}")
            return 0

        table_name = await prepare_cot_table(conn, report_type)
        rows_inserted = await _insert_cot_records(
            conn, table_name, records, report_type
        )

        logger.info(f"Ingested {rows_inserted} COT disaggregated records for {year}")
        return rows_inserted

    finally:
        await client.close()


async def ingest_cot_tff(conn, year: int = None, combined: bool = False) -> int:
    """
    Ingest Traders in Financial Futures (TFF) report data.

    Shows positions for financial contracts by:
    - Dealer/Intermediary
    - Asset Manager/Institutional
    - Leveraged Funds
    - Other Reportables

    Args:
        conn: Database connection
        year: Year to ingest
        combined: If True, use futures+options combined report

    Returns:
        Number of rows inserted
    """
    if year is None:
        year = datetime.now().year

    report_type = "tff_combined" if combined else "tff_futures"

    client = CFTCCOTClient()

    try:
        if combined:
            records = await client.get_tff_combined(year)
        else:
            records = await client.get_tff_futures(year)

        if not records:
            logger.warning(f"No COT TFF data returned for {year}")
            return 0

        table_name = await prepare_cot_table(conn, report_type)
        rows_inserted = await _insert_cot_records(
            conn, table_name, records, report_type
        )

        logger.info(f"Ingested {rows_inserted} COT TFF records for {year}")
        return rows_inserted

    finally:
        await client.close()


async def _insert_cot_records(
    conn, table_name: str, records: List[Dict[str, Any]], report_type: str
) -> int:
    """
    Insert COT records into database.

    Args:
        conn: Database connection
        table_name: Target table name
        records: Records to insert
        report_type: Type of report

    Returns:
        Number of rows inserted
    """
    # Select appropriate columns
    if "disaggregated" in report_type:
        columns = COT_DISAGGREGATED_COLUMNS
    elif "tff" in report_type:
        columns = COT_TFF_COLUMNS
    else:
        columns = COT_LEGACY_COLUMNS

    # Filter to insertable columns (exclude auto-generated)
    insert_columns = [col for col in columns.keys() if col not in ("id", "ingested_at")]

    cursor = conn.cursor()
    rows_inserted = 0

    for record in records:
        # Parse and enhance record
        parsed = parse_cot_record(record)

        # Skip records without required fields
        if not parsed.get("report_date") or not parsed.get("market_name"):
            continue

        # Build insert
        values = []
        for col in insert_columns:
            values.append(parsed.get(col))

        placeholders = ", ".join(["%s"] * len(insert_columns))
        columns_str = ", ".join(insert_columns)

        # Use ON CONFLICT for upsert
        update_cols = [
            f"{col} = EXCLUDED.{col}"
            for col in insert_columns
            if col not in ("report_date", "market_name", "report_type")
        ]
        update_str = ", ".join(update_cols)

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (report_date, market_name, report_type)
            DO UPDATE SET {update_str}
        """

        try:
            cursor.execute(sql, values)
            rows_inserted += 1
        except Exception as e:
            logger.warning(f"Error inserting COT record: {e}")
            continue

    conn.commit()
    return rows_inserted


async def ingest_cot_all_reports(
    conn, year: int = None, combined: bool = True
) -> Dict[str, int]:
    """
    Ingest all COT report types for a given year.

    Args:
        conn: Database connection
        year: Year to ingest
        combined: If True, use futures+options combined reports

    Returns:
        Dictionary of report type -> rows inserted
    """
    if year is None:
        year = datetime.now().year

    results = {}

    # Legacy report
    try:
        results["legacy"] = await ingest_cot_legacy(conn, year, combined)
    except Exception as e:
        logger.error(f"Error ingesting COT legacy: {e}")
        results["legacy"] = 0

    # Disaggregated report
    try:
        results["disaggregated"] = await ingest_cot_disaggregated(conn, year, combined)
    except Exception as e:
        logger.error(f"Error ingesting COT disaggregated: {e}")
        results["disaggregated"] = 0

    # TFF report
    try:
        results["tff"] = await ingest_cot_tff(conn, year, combined)
    except Exception as e:
        logger.error(f"Error ingesting COT TFF: {e}")
        results["tff"] = 0

    total = sum(results.values())
    logger.info(f"Total COT records ingested for {year}: {total}")

    return results
