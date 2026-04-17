"""
FFIEC Bank Call Reports metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from FDIC API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "ffiec_bank_calls"

# Dataset identifier
DATASET_ID = "ffiec_bank_calls"

# Display name
DISPLAY_NAME = "FFIEC Bank Call Reports"

# Description
DESCRIPTION = (
    "Bank financial data from FDIC BankFind Suite API. "
    "Includes total assets, deposits, loans, equity, net income, "
    "return on assets, and office counts for FDIC-insured institutions."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "cert_id",
    "institution_name",
    "state",
    "city",
    "report_date",
    "total_assets",
    "total_deposits",
    "total_loans",
    "total_equity",
    "net_income",
    "return_on_assets",
    "num_offices",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "institution_name",
    "state",
    "city",
    "total_assets",
    "total_deposits",
    "total_loans",
    "total_equity",
    "net_income",
    "return_on_assets",
    "num_offices",
]

# Conflict columns for upsert
CONFLICT_COLUMNS = ["cert_id", "report_date"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for FFIEC bank call report data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        cert_id TEXT NOT NULL,
        institution_name TEXT,
        state TEXT,
        city TEXT,
        report_date TEXT NOT NULL,
        total_assets NUMERIC(18,2),
        total_deposits NUMERIC(18,2),
        total_loans NUMERIC(18,2),
        total_equity NUMERIC(18,2),
        net_income NUMERIC(18,2),
        return_on_assets NUMERIC(8,4),
        num_offices INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(cert_id, report_date)
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on report_date for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_report_date
        ON {TABLE_NAME} (report_date);

    -- Index on total_assets for ranking queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_total_assets
        ON {TABLE_NAME} (total_assets DESC);

    -- Index on cert_id for lookups
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_cert_id
        ON {TABLE_NAME} (cert_id);

    COMMENT ON TABLE {TABLE_NAME} IS 'FFIEC bank call report financial data via FDIC BankFind Suite';
    """


def _safe_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert a value to float for NUMERIC columns."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_bank_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single bank financial record from FDIC API response.

    FDIC API returns records in format: {"data": {"CERT": "...", "INSTNAME": "...", ...}}

    Args:
        raw: Raw record from API response

    Returns:
        Dict suitable for database insertion
    """
    # FDIC wraps each record's fields in a nested "data" key
    data = raw.get("data", raw) if isinstance(raw, dict) else raw

    cert_id = data.get("CERT")
    if not cert_id:
        return {}

    report_date = data.get("REPDTE", "")

    return {
        "cert_id": str(cert_id).strip(),
        "institution_name": (data.get("INSTNAME") or "").strip(),
        "state": (data.get("STNAME") or "").strip(),
        "city": (data.get("CITY") or "").strip(),
        "report_date": str(report_date).strip(),
        "total_assets": _safe_numeric(data.get("ASSET")),
        "total_deposits": _safe_numeric(data.get("DEP")),
        "total_loans": _safe_numeric(data.get("LNLSNET")),
        "total_equity": _safe_numeric(data.get("EQTOT")),
        "net_income": _safe_numeric(data.get("NITEFDSM")),
        "return_on_assets": _safe_numeric(data.get("ROAPTX")),
        "num_offices": _safe_int(data.get("OFFDOM")),
    }


def parse_bank_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of bank financial records from FDIC API response.

    Args:
        raw_records: List of raw records from API

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []
    skipped = 0

    for raw in raw_records:
        record = parse_bank_record(raw)
        if record and record.get("cert_id"):
            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)
            parsed.append(record)
        else:
            skipped += 1

    if skipped:
        logger.warning(f"Skipped {skipped} bank records with missing cert_id")

    logger.info(f"Parsed {len(parsed)} bank financial records")
    return parsed
