"""
OSHA metadata utilities.

Handles:
- Table definitions for inspections and violations
- CREATE TABLE SQL generation
- CSV parsing and data transformation
- Field mapping from DOL CSV columns to database columns
"""

import csv
import io
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# =========================================================================
# Inspections table
# =========================================================================

INSPECTIONS_TABLE_NAME = "osha_inspections"
INSPECTIONS_DATASET_ID = "osha_inspections"
INSPECTIONS_DISPLAY_NAME = "OSHA Workplace Inspections"
INSPECTIONS_DESCRIPTION = (
    "OSHA workplace inspection records from the Department of Labor enforcement "
    "data catalog. Includes inspection type, violations, penalties, and "
    "establishment information."
)

INSPECTIONS_COLUMNS = [
    "activity_nr",
    "establishment_name",
    "site_address",
    "site_city",
    "site_state",
    "site_zip",
    "naics_code",
    "sic_code",
    "inspection_type",
    "open_date",
    "close_case_date",
    "violation_type_s",
    "violation_type_o",
    "total_initial_penalty",
    "total_current_penalty",
    "total_violations",
]

INSPECTIONS_CONFLICT_COLUMNS = ["activity_nr"]

INSPECTIONS_UPDATE_COLUMNS = [
    "establishment_name",
    "site_address",
    "site_city",
    "site_state",
    "site_zip",
    "naics_code",
    "sic_code",
    "inspection_type",
    "open_date",
    "close_case_date",
    "violation_type_s",
    "violation_type_o",
    "total_initial_penalty",
    "total_current_penalty",
    "total_violations",
]

CREATE_INSPECTIONS_SQL = f"""
CREATE TABLE IF NOT EXISTS {INSPECTIONS_TABLE_NAME} (
    activity_nr TEXT PRIMARY KEY,
    establishment_name TEXT,
    site_address TEXT,
    site_city TEXT,
    site_state TEXT,
    site_zip TEXT,
    naics_code TEXT,
    sic_code TEXT,
    inspection_type TEXT,
    open_date DATE,
    close_case_date DATE,
    violation_type_s INTEGER DEFAULT 0,
    violation_type_o INTEGER DEFAULT 0,
    total_initial_penalty NUMERIC DEFAULT 0,
    total_current_penalty NUMERIC DEFAULT 0,
    total_violations INTEGER DEFAULT 0,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_{INSPECTIONS_TABLE_NAME}_state
    ON {INSPECTIONS_TABLE_NAME} (site_state);

CREATE INDEX IF NOT EXISTS idx_{INSPECTIONS_TABLE_NAME}_naics
    ON {INSPECTIONS_TABLE_NAME} (naics_code);

CREATE INDEX IF NOT EXISTS idx_{INSPECTIONS_TABLE_NAME}_name
    ON {INSPECTIONS_TABLE_NAME} (establishment_name);

CREATE INDEX IF NOT EXISTS idx_{INSPECTIONS_TABLE_NAME}_open_date
    ON {INSPECTIONS_TABLE_NAME} (open_date);

COMMENT ON TABLE {INSPECTIONS_TABLE_NAME} IS 'OSHA workplace inspection records from DOL enforcement data';
"""

# =========================================================================
# Violations table
# =========================================================================

VIOLATIONS_TABLE_NAME = "osha_violations"
VIOLATIONS_DATASET_ID = "osha_violations"
VIOLATIONS_DISPLAY_NAME = "OSHA Violations"
VIOLATIONS_DESCRIPTION = (
    "OSHA violation records linked to inspections. Includes citation details, "
    "penalty amounts, standards violated, and abatement dates."
)

VIOLATIONS_COLUMNS = [
    "activity_nr",
    "citation_id",
    "violation_type",
    "current_penalty",
    "initial_penalty",
    "issuance_date",
    "abate_date",
    "contest_date",
    "final_order_date",
    "standard",
    "description",
]

VIOLATIONS_CONFLICT_COLUMNS = ["activity_nr", "citation_id"]

VIOLATIONS_UPDATE_COLUMNS = [
    "violation_type",
    "current_penalty",
    "initial_penalty",
    "issuance_date",
    "abate_date",
    "contest_date",
    "final_order_date",
    "standard",
    "description",
]

CREATE_VIOLATIONS_SQL = f"""
CREATE TABLE IF NOT EXISTS {VIOLATIONS_TABLE_NAME} (
    activity_nr TEXT NOT NULL,
    citation_id TEXT NOT NULL,
    violation_type TEXT,
    current_penalty NUMERIC DEFAULT 0,
    initial_penalty NUMERIC DEFAULT 0,
    issuance_date DATE,
    abate_date DATE,
    contest_date DATE,
    final_order_date DATE,
    standard TEXT,
    description TEXT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (activity_nr, citation_id)
);

CREATE INDEX IF NOT EXISTS idx_{VIOLATIONS_TABLE_NAME}_activity
    ON {VIOLATIONS_TABLE_NAME} (activity_nr);

CREATE INDEX IF NOT EXISTS idx_{VIOLATIONS_TABLE_NAME}_type
    ON {VIOLATIONS_TABLE_NAME} (violation_type);

CREATE INDEX IF NOT EXISTS idx_{VIOLATIONS_TABLE_NAME}_standard
    ON {VIOLATIONS_TABLE_NAME} (standard);

COMMENT ON TABLE {VIOLATIONS_TABLE_NAME} IS 'OSHA violation records linked to inspections';
"""

# =========================================================================
# CSV column mappings
# =========================================================================

# DOL CSV column names -> our column names
INSPECTION_CSV_MAP = {
    "activity_nr": "activity_nr",
    "estab_name": "establishment_name",
    "site_address": "site_address",
    "site_city": "site_city",
    "site_state": "site_state",
    "site_zip": "site_zip",
    "naics_code": "naics_code",
    "sic_code": "sic_code",
    "insp_type": "inspection_type",
    "open_date": "open_date",
    "close_case_date": "close_case_date",
    "vio_type_s": "violation_type_s",
    "vio_type_o": "violation_type_o",
    "total_initial_penalty": "total_initial_penalty",
    "total_current_penalty": "total_current_penalty",
    "total_violations": "total_violations",
}

VIOLATION_CSV_MAP = {
    "activity_nr": "activity_nr",
    "citation_id": "citation_id",
    "vio_type": "violation_type",
    "current_penalty": "current_penalty",
    "initial_penalty": "initial_penalty",
    "issuance_date": "issuance_date",
    "abate_date": "abate_date",
    "contest_date": "contest_date",
    "final_order_date": "final_order_date",
    "standard": "standard",
    "vio_desc": "description",
}


def _safe_int(value: str) -> Optional[int]:
    """Convert string to int, returning None on failure."""
    if not value or not value.strip():
        return None
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None


def _safe_float(value: str) -> Optional[float]:
    """Convert string to float, returning None on failure."""
    if not value or not value.strip():
        return None
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return None


def _safe_date(value: str) -> Optional[str]:
    """
    Clean date string for database insertion.
    DOL CSVs use various date formats. Return None for empty/invalid.
    """
    if not value or not value.strip():
        return None
    cleaned = value.strip()
    # Handle common empty date indicators
    if cleaned in ("", "0", "00000000", "None"):
        return None
    return cleaned


def parse_inspections_csv(csv_content: str) -> List[Dict[str, Any]]:
    """
    Parse OSHA inspections CSV content into database rows.

    Args:
        csv_content: Raw CSV string from the DOL ZIP download

    Returns:
        List of row dicts suitable for database insertion
    """
    rows = []
    reader = csv.DictReader(io.StringIO(csv_content))

    for line_num, raw_row in enumerate(reader, start=2):
        try:
            activity_nr = raw_row.get("activity_nr", "").strip()
            if not activity_nr:
                continue

            row = {
                "activity_nr": activity_nr,
                "establishment_name": raw_row.get("estab_name", "").strip() or None,
                "site_address": raw_row.get("site_address", "").strip() or None,
                "site_city": raw_row.get("site_city", "").strip() or None,
                "site_state": raw_row.get("site_state", "").strip() or None,
                "site_zip": raw_row.get("site_zip", "").strip() or None,
                "naics_code": raw_row.get("naics_code", "").strip() or None,
                "sic_code": raw_row.get("sic_code", "").strip() or None,
                "inspection_type": raw_row.get("insp_type", "").strip() or None,
                "open_date": _safe_date(raw_row.get("open_date", "")),
                "close_case_date": _safe_date(raw_row.get("close_case_date", "")),
                "violation_type_s": _safe_int(raw_row.get("vio_type_s", "")) or 0,
                "violation_type_o": _safe_int(raw_row.get("vio_type_o", "")) or 0,
                "total_initial_penalty": _safe_float(
                    raw_row.get("total_initial_penalty", "")
                ) or 0,
                "total_current_penalty": _safe_float(
                    raw_row.get("total_current_penalty", "")
                ) or 0,
                "total_violations": _safe_int(
                    raw_row.get("total_violations", "")
                ) or 0,
            }
            rows.append(row)

        except Exception as e:
            logger.warning(f"Skipping inspection CSV line {line_num}: {e}")
            continue

    logger.info(f"Parsed {len(rows)} OSHA inspection records from CSV")
    return rows


def parse_violations_csv(csv_content: str) -> List[Dict[str, Any]]:
    """
    Parse OSHA violations CSV content into database rows.

    Args:
        csv_content: Raw CSV string from the DOL ZIP download

    Returns:
        List of row dicts suitable for database insertion
    """
    rows = []
    reader = csv.DictReader(io.StringIO(csv_content))

    for line_num, raw_row in enumerate(reader, start=2):
        try:
            activity_nr = raw_row.get("activity_nr", "").strip()
            citation_id = raw_row.get("citation_id", "").strip()
            if not activity_nr or not citation_id:
                continue

            row = {
                "activity_nr": activity_nr,
                "citation_id": citation_id,
                "violation_type": raw_row.get("vio_type", "").strip() or None,
                "current_penalty": _safe_float(
                    raw_row.get("current_penalty", "")
                ) or 0,
                "initial_penalty": _safe_float(
                    raw_row.get("initial_penalty", "")
                ) or 0,
                "issuance_date": _safe_date(raw_row.get("issuance_date", "")),
                "abate_date": _safe_date(raw_row.get("abate_date", "")),
                "contest_date": _safe_date(raw_row.get("contest_date", "")),
                "final_order_date": _safe_date(raw_row.get("final_order_date", "")),
                "standard": raw_row.get("standard", "").strip() or None,
                "description": raw_row.get("vio_desc", "").strip() or None,
            }
            rows.append(row)

        except Exception as e:
            logger.warning(f"Skipping violation CSV line {line_num}: {e}")
            continue

    logger.info(f"Parsed {len(rows)} OSHA violation records from CSV")
    return rows
