"""
IRS Statistics of Income metadata and schema definitions.

Defines:
- AGI (Adjusted Gross Income) brackets
- Database schemas for each dataset
- Parsing and transformation utilities
- Column mappings for IRS data files
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


# ========== AGI Bracket Definitions ==========

# AGI (Adjusted Gross Income) brackets used in IRS SOI data
# These are the standard income brackets used for aggregation
AGI_BRACKETS = {
    "1": "$1 under $25,000",
    "2": "$25,000 under $50,000",
    "3": "$50,000 under $75,000",
    "4": "$75,000 under $100,000",
    "5": "$100,000 under $200,000",
    "6": "$200,000 or more",
    "0": "Total (all income levels)",
}

# AGI bracket ranges for numeric filtering
AGI_BRACKET_RANGES = {
    "1": (1, 24999),
    "2": (25000, 49999),
    "3": (50000, 74999),
    "4": (75000, 99999),
    "5": (100000, 199999),
    "6": (200000, None),  # No upper bound
    "0": (None, None),  # All income levels
}


# ========== Database Schema Definitions ==========

# ZIP Code Income Data schema
ZIP_INCOME_COLUMNS = {
    "id": ("SERIAL PRIMARY KEY", "Unique identifier"),
    "tax_year": ("INTEGER NOT NULL", "Tax year"),
    "state_code": ("TEXT", "State FIPS code"),
    "state_abbr": ("TEXT", "State abbreviation"),
    "zip_code": ("TEXT NOT NULL", "ZIP code (5-digit)"),
    "agi_class": ("TEXT NOT NULL", "AGI bracket class (1-6, 0=total)"),
    "agi_class_label": ("TEXT", "AGI bracket description"),
    "num_returns": ("INTEGER", "Number of returns"),
    "num_single_returns": ("INTEGER", "Number of single returns"),
    "num_joint_returns": ("INTEGER", "Number of joint returns"),
    "num_head_household": ("INTEGER", "Number of head of household returns"),
    "num_exemptions": ("INTEGER", "Number of exemptions"),
    "num_dependents": ("INTEGER", "Number of dependents"),
    "total_agi": ("BIGINT", "Total adjusted gross income (thousands)"),
    "total_wages": ("BIGINT", "Total wages and salaries (thousands)"),
    "total_dividends": ("BIGINT", "Total ordinary dividends (thousands)"),
    "total_interest": ("BIGINT", "Total taxable interest (thousands)"),
    "total_capital_gains": ("BIGINT", "Total net capital gains (thousands)"),
    "total_business_income": (
        "BIGINT",
        "Total business/professional net income (thousands)",
    ),
    "total_ira_distributions": ("BIGINT", "Total IRA distributions (thousands)"),
    "total_pensions": ("BIGINT", "Total pensions and annuities (thousands)"),
    "total_social_security": ("BIGINT", "Total social security benefits (thousands)"),
    "total_unemployment": ("BIGINT", "Total unemployment compensation (thousands)"),
    "total_tax_liability": ("BIGINT", "Total income tax after credits (thousands)"),
    "total_amt": ("BIGINT", "Total alternative minimum tax (thousands)"),
    "total_earned_income_credit": ("BIGINT", "Total earned income credit (thousands)"),
    "total_child_tax_credit": ("BIGINT", "Total child tax credit (thousands)"),
    "avg_agi": ("NUMERIC(18,2)", "Average AGI per return"),
    "ingested_at": ("TIMESTAMP DEFAULT NOW()", "Ingestion timestamp"),
}

# County Income Data schema
COUNTY_INCOME_COLUMNS = {
    "id": ("SERIAL PRIMARY KEY", "Unique identifier"),
    "tax_year": ("INTEGER NOT NULL", "Tax year"),
    "state_code": ("TEXT", "State FIPS code"),
    "state_abbr": ("TEXT", "State abbreviation"),
    "county_code": ("TEXT NOT NULL", "County FIPS code"),
    "county_name": ("TEXT", "County name"),
    "agi_class": ("TEXT NOT NULL", "AGI bracket class (1-6, 0=total)"),
    "agi_class_label": ("TEXT", "AGI bracket description"),
    "num_returns": ("INTEGER", "Number of returns"),
    "num_single_returns": ("INTEGER", "Number of single returns"),
    "num_joint_returns": ("INTEGER", "Number of joint returns"),
    "num_head_household": ("INTEGER", "Number of head of household returns"),
    "num_exemptions": ("INTEGER", "Number of exemptions"),
    "num_dependents": ("INTEGER", "Number of dependents"),
    "total_agi": ("BIGINT", "Total adjusted gross income (thousands)"),
    "total_wages": ("BIGINT", "Total wages and salaries (thousands)"),
    "total_dividends": ("BIGINT", "Total ordinary dividends (thousands)"),
    "total_interest": ("BIGINT", "Total taxable interest (thousands)"),
    "total_capital_gains": ("BIGINT", "Total net capital gains (thousands)"),
    "total_business_income": (
        "BIGINT",
        "Total business/professional net income (thousands)",
    ),
    "total_ira_distributions": ("BIGINT", "Total IRA distributions (thousands)"),
    "total_pensions": ("BIGINT", "Total pensions and annuities (thousands)"),
    "total_social_security": ("BIGINT", "Total social security benefits (thousands)"),
    "total_unemployment": ("BIGINT", "Total unemployment compensation (thousands)"),
    "total_tax_liability": ("BIGINT", "Total income tax after credits (thousands)"),
    "avg_agi": ("NUMERIC(18,2)", "Average AGI per return"),
    "ingested_at": ("TIMESTAMP DEFAULT NOW()", "Ingestion timestamp"),
}

# Migration Data schema (county-to-county flows)
MIGRATION_COLUMNS = {
    "id": ("SERIAL PRIMARY KEY", "Unique identifier"),
    "tax_year": ("INTEGER NOT NULL", "Tax year (filing year)"),
    "flow_type": ("TEXT NOT NULL", "Flow type: inflow or outflow"),
    # Origin/Destination (depending on flow_type)
    "dest_state_code": ("TEXT", "Destination state FIPS code"),
    "dest_state_abbr": ("TEXT", "Destination state abbreviation"),
    "dest_county_code": ("TEXT", "Destination county FIPS code"),
    "dest_county_name": ("TEXT", "Destination county name"),
    "orig_state_code": ("TEXT", "Origin state FIPS code"),
    "orig_state_abbr": ("TEXT", "Origin state abbreviation"),
    "orig_county_code": ("TEXT", "Origin county FIPS code"),
    "orig_county_name": ("TEXT", "Origin county name"),
    # Migration statistics
    "num_returns": ("INTEGER", "Number of returns migrating"),
    "num_exemptions": ("INTEGER", "Number of exemptions migrating"),
    "total_agi": ("BIGINT", "Total AGI of migrants (thousands)"),
    "avg_agi": ("NUMERIC(18,2)", "Average AGI per migrating return"),
    "ingested_at": ("TIMESTAMP DEFAULT NOW()", "Ingestion timestamp"),
}

# Business Income by ZIP schema
BUSINESS_INCOME_COLUMNS = {
    "id": ("SERIAL PRIMARY KEY", "Unique identifier"),
    "tax_year": ("INTEGER NOT NULL", "Tax year"),
    "state_code": ("TEXT", "State FIPS code"),
    "state_abbr": ("TEXT", "State abbreviation"),
    "zip_code": ("TEXT NOT NULL", "ZIP code (5-digit)"),
    # Overall counts
    "num_returns": ("INTEGER", "Number of returns"),
    "total_agi": ("BIGINT", "Total adjusted gross income (thousands)"),
    # Business income types
    "num_with_business_income": (
        "INTEGER",
        "Returns with business/professional income",
    ),
    "total_business_income": (
        "BIGINT",
        "Total business/professional net income (thousands)",
    ),
    "num_with_farm_income": ("INTEGER", "Returns with farm income"),
    "total_farm_income": ("BIGINT", "Total farm net income (thousands)"),
    # Schedule C (Sole Proprietorships)
    "num_schedule_c": ("INTEGER", "Returns with Schedule C"),
    "total_schedule_c_income": ("BIGINT", "Total Schedule C net income (thousands)"),
    "total_schedule_c_receipts": (
        "BIGINT",
        "Total Schedule C gross receipts (thousands)",
    ),
    # Partnerships (Schedule E)
    "num_partnership_income": ("INTEGER", "Returns with partnership/S-corp income"),
    "total_partnership_income": (
        "BIGINT",
        "Total partnership/S-corp net income (thousands)",
    ),
    # Real Estate (Schedule E)
    "num_rental_income": ("INTEGER", "Returns with rental real estate income"),
    "total_rental_income": (
        "BIGINT",
        "Total rental real estate net income (thousands)",
    ),
    # Self-employment tax
    "num_with_se_tax": ("INTEGER", "Returns with self-employment tax"),
    "total_se_tax": ("BIGINT", "Total self-employment tax (thousands)"),
    "ingested_at": ("TIMESTAMP DEFAULT NOW()", "Ingestion timestamp"),
}


# ========== Table Name Generation ==========


def generate_table_name(dataset: str) -> str:
    """
    Generate PostgreSQL table name for IRS SOI data.

    Args:
        dataset: Dataset identifier (zip_income, county_income, migration, business_income)

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("zip_income")
        'irs_soi_zip_income'
        >>> generate_table_name("migration")
        'irs_soi_migration'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    return f"irs_soi_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for IRS SOI data.

    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema

    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "zip_income":
        columns = ZIP_INCOME_COLUMNS
        unique_constraint = '"tax_year", "zip_code", "agi_class"'
        indexes = [
            ("tax_year", '"tax_year"'),
            ("state_code", '"state_code"'),
            ("zip_code", '"zip_code"'),
            ("agi_class", '"agi_class"'),
        ]
    elif dataset == "county_income":
        columns = COUNTY_INCOME_COLUMNS
        unique_constraint = '"tax_year", "county_code", "agi_class"'
        indexes = [
            ("tax_year", '"tax_year"'),
            ("state_code", '"state_code"'),
            ("county_code", '"county_code"'),
            ("agi_class", '"agi_class"'),
        ]
    elif dataset == "migration":
        columns = MIGRATION_COLUMNS
        unique_constraint = '"tax_year", "flow_type", "dest_state_code", "dest_county_code", "orig_state_code", "orig_county_code"'
        indexes = [
            ("tax_year", '"tax_year"'),
            ("flow_type", '"flow_type"'),
            ("dest_county", '"dest_state_code", "dest_county_code"'),
            ("orig_county", '"orig_state_code", "orig_county_code"'),
        ]
    elif dataset == "business_income":
        columns = BUSINESS_INCOME_COLUMNS
        unique_constraint = '"tax_year", "zip_code"'
        indexes = [
            ("tax_year", '"tax_year"'),
            ("state_code", '"state_code"'),
            ("zip_code", '"zip_code"'),
        ]
    else:
        raise ValueError(f"Unknown IRS SOI dataset: {dataset}")

    # Build column definitions
    column_defs = []
    for col_name, (col_type, col_desc) in columns.items():
        if col_name == "id":
            column_defs.append(f"{col_name} {col_type}")
        else:
            column_defs.append(f'"{col_name}" {col_type}')

    columns_sql = ",\n        ".join(column_defs)

    # Build indexes
    index_sql_parts = []
    for idx_name, idx_cols in indexes:
        index_sql_parts.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx_name} "
            f"ON {table_name} ({idx_cols});"
        )

    indexes_sql = "\n    ".join(index_sql_parts)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql},
        CONSTRAINT {table_name}_unique UNIQUE ({unique_constraint})
    );
    
    {indexes_sql}
    """

    return sql


# ========== Data Parsing Functions ==========


def parse_zip_income_data(df: pd.DataFrame, tax_year: int) -> List[Dict[str, Any]]:
    """
    Parse ZIP code income data from IRS CSV.

    Args:
        df: DataFrame from IRS CSV
        tax_year: Tax year

    Returns:
        List of parsed records ready for insertion
    """
    records = []

    # Column mapping (IRS column names vary by year)
    # Standard columns (may need adjustment based on actual file structure)
    column_map = {
        "STATEFIPS": "state_code",
        "STATE": "state_abbr",
        "ZIPCODE": "zip_code",
        "zipcode": "zip_code",
        "AGI_STUB": "agi_class",
        "agi_stub": "agi_class",
        "N1": "num_returns",
        "N2": "num_exemptions",
        "MARS1": "num_single_returns",
        "MARS2": "num_joint_returns",
        "MARS4": "num_head_household",
        "NUMDEP": "num_dependents",
        "A00100": "total_agi",
        "A00200": "total_wages",
        "A00300": "total_interest",
        "A00600": "total_dividends",
        "A00650": "total_capital_gains",
        "A00900": "total_business_income",
        "A01400": "total_ira_distributions",
        "A01700": "total_pensions",
        "A02500": "total_social_security",
        "A02300": "total_unemployment",
        "A06500": "total_tax_liability",
        "A09400": "total_amt",
        "A59660": "total_earned_income_credit",
        "A11070": "total_child_tax_credit",
    }

    # Normalize column names (lowercase)
    df.columns = df.columns.str.upper()

    for _, row in df.iterrows():
        try:
            record = {
                "tax_year": tax_year,
            }

            # Map columns
            for src_col, dest_col in column_map.items():
                if src_col in df.columns:
                    value = row.get(src_col)
                    record[dest_col] = _safe_value(value, dest_col)

            # Handle lowercase column variations
            for src_col in ["zipcode", "agi_stub"]:
                if src_col.upper() not in df.columns and src_col in df.columns:
                    record[column_map[src_col]] = _safe_value(
                        row.get(src_col), column_map[src_col]
                    )

            # Add AGI class label
            agi_class = str(record.get("agi_class", "")).strip()
            record["agi_class_label"] = AGI_BRACKETS.get(agi_class, "Unknown")

            # Calculate average AGI if possible
            num_returns = record.get("num_returns")
            total_agi = record.get("total_agi")
            if num_returns and num_returns > 0 and total_agi:
                record["avg_agi"] = round((total_agi * 1000) / num_returns, 2)

            # Skip records without required fields
            if record.get("zip_code") and record.get("agi_class"):
                records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing ZIP income record: {e}")

    logger.info(f"Parsed {len(records)} ZIP income records for year {tax_year}")
    return records


def parse_county_income_data(df: pd.DataFrame, tax_year: int) -> List[Dict[str, Any]]:
    """
    Parse county income data from IRS CSV.

    Args:
        df: DataFrame from IRS CSV
        tax_year: Tax year

    Returns:
        List of parsed records ready for insertion
    """
    records = []

    column_map = {
        "STATEFIPS": "state_code",
        "STATE": "state_abbr",
        "COUNTYFIPS": "county_code",
        "COUNTYNAME": "county_name",
        "AGI_STUB": "agi_class",
        "N1": "num_returns",
        "N2": "num_exemptions",
        "MARS1": "num_single_returns",
        "MARS2": "num_joint_returns",
        "MARS4": "num_head_household",
        "NUMDEP": "num_dependents",
        "A00100": "total_agi",
        "A00200": "total_wages",
        "A00300": "total_interest",
        "A00600": "total_dividends",
        "A00650": "total_capital_gains",
        "A00900": "total_business_income",
        "A01400": "total_ira_distributions",
        "A01700": "total_pensions",
        "A02500": "total_social_security",
        "A02300": "total_unemployment",
        "A06500": "total_tax_liability",
    }

    df.columns = df.columns.str.upper()

    for _, row in df.iterrows():
        try:
            record = {
                "tax_year": tax_year,
            }

            for src_col, dest_col in column_map.items():
                if src_col in df.columns:
                    value = row.get(src_col)
                    record[dest_col] = _safe_value(value, dest_col)

            # Construct full county FIPS (state + county)
            state_code = record.get("state_code", "")
            county_code = record.get("county_code", "")
            if state_code and county_code:
                record["county_code"] = (
                    f"{str(state_code).zfill(2)}{str(county_code).zfill(3)}"
                )

            # Add AGI class label
            agi_class = str(record.get("agi_class", "")).strip()
            record["agi_class_label"] = AGI_BRACKETS.get(agi_class, "Unknown")

            # Calculate average AGI
            num_returns = record.get("num_returns")
            total_agi = record.get("total_agi")
            if num_returns and num_returns > 0 and total_agi:
                record["avg_agi"] = round((total_agi * 1000) / num_returns, 2)

            if record.get("county_code") and record.get("agi_class"):
                records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing county income record: {e}")

    logger.info(f"Parsed {len(records)} county income records for year {tax_year}")
    return records


def parse_migration_data(
    df: pd.DataFrame, tax_year: int, flow_type: str
) -> List[Dict[str, Any]]:
    """
    Parse county-to-county migration data from IRS CSV.

    Args:
        df: DataFrame from IRS CSV
        tax_year: Tax year
        flow_type: "inflow" or "outflow"

    Returns:
        List of parsed records ready for insertion
    """
    records = []

    # Column names vary - common patterns
    column_map_inflow = {
        "Y2_STATEFIPS": "dest_state_code",
        "Y2_STATE": "dest_state_abbr",
        "Y2_COUNTYFIPS": "dest_county_code",
        "Y2_COUNTYNAME": "dest_county_name",
        "Y1_STATEFIPS": "orig_state_code",
        "Y1_STATE": "orig_state_abbr",
        "Y1_COUNTYFIPS": "orig_county_code",
        "Y1_COUNTYNAME": "orig_county_name",
        "N1": "num_returns",
        "N2": "num_exemptions",
        "AGI": "total_agi",
    }

    column_map_outflow = {
        "Y1_STATEFIPS": "orig_state_code",
        "Y1_STATE": "orig_state_abbr",
        "Y1_COUNTYFIPS": "orig_county_code",
        "Y1_COUNTYNAME": "orig_county_name",
        "Y2_STATEFIPS": "dest_state_code",
        "Y2_STATE": "dest_state_abbr",
        "Y2_COUNTYFIPS": "dest_county_code",
        "Y2_COUNTYNAME": "dest_county_name",
        "N1": "num_returns",
        "N2": "num_exemptions",
        "AGI": "total_agi",
    }

    column_map = column_map_inflow if flow_type == "inflow" else column_map_outflow

    df.columns = df.columns.str.upper()

    for _, row in df.iterrows():
        try:
            record = {
                "tax_year": tax_year,
                "flow_type": flow_type,
            }

            for src_col, dest_col in column_map.items():
                if src_col in df.columns:
                    value = row.get(src_col)
                    record[dest_col] = _safe_value(value, dest_col)

            # Build full FIPS codes
            for prefix in ["dest", "orig"]:
                state_code = record.get(f"{prefix}_state_code", "")
                county_code = record.get(f"{prefix}_county_code", "")
                if state_code and county_code:
                    record[f"{prefix}_county_code"] = (
                        f"{str(state_code).zfill(2)}{str(county_code).zfill(3)}"
                    )

            # Calculate average AGI
            num_returns = record.get("num_returns")
            total_agi = record.get("total_agi")
            if num_returns and num_returns > 0 and total_agi:
                record["avg_agi"] = round((total_agi * 1000) / num_returns, 2)

            # Skip records without required fields
            if record.get("dest_county_code") and record.get("orig_county_code"):
                records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing migration record: {e}")

    logger.info(
        f"Parsed {len(records)} {flow_type} migration records for year {tax_year}"
    )
    return records


def parse_business_income_data(df: pd.DataFrame, tax_year: int) -> List[Dict[str, Any]]:
    """
    Parse business income by ZIP data from IRS CSV.

    Args:
        df: DataFrame from IRS CSV
        tax_year: Tax year

    Returns:
        List of parsed records ready for insertion
    """
    records = []

    column_map = {
        "STATEFIPS": "state_code",
        "STATE": "state_abbr",
        "ZIPCODE": "zip_code",
        "N1": "num_returns",
        "A00100": "total_agi",
        "N00900": "num_with_business_income",
        "A00900": "total_business_income",
        "N01400": "num_with_farm_income",
        "A01400": "total_farm_income",
        # Schedule C
        "SCHF": "num_schedule_c",
        # Partnership/S-corp
        "N26270": "num_partnership_income",
        "A26270": "total_partnership_income",
        # Rental
        "N26340": "num_rental_income",
        "A26340": "total_rental_income",
        # Self-employment tax
        "N09400": "num_with_se_tax",
        "A09400": "total_se_tax",
    }

    df.columns = df.columns.str.upper()

    for _, row in df.iterrows():
        try:
            record = {
                "tax_year": tax_year,
            }

            for src_col, dest_col in column_map.items():
                if src_col in df.columns:
                    value = row.get(src_col)
                    record[dest_col] = _safe_value(value, dest_col)

            if record.get("zip_code"):
                records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing business income record: {e}")

    logger.info(f"Parsed {len(records)} business income records for year {tax_year}")
    return records


def _safe_value(value: Any, column_name: str) -> Any:
    """
    Safely convert a value to the appropriate type.

    Args:
        value: Raw value
        column_name: Column name for type inference

    Returns:
        Converted value or None
    """
    if pd.isna(value) or value == "" or value == "d" or value == "D":
        return None

    # Numeric columns
    numeric_columns = [
        "num_returns",
        "num_exemptions",
        "num_single_returns",
        "num_joint_returns",
        "num_head_household",
        "num_dependents",
        "total_agi",
        "total_wages",
        "total_dividends",
        "total_interest",
        "total_capital_gains",
        "total_business_income",
        "total_ira_distributions",
        "total_pensions",
        "total_social_security",
        "total_unemployment",
        "total_tax_liability",
        "total_amt",
        "total_earned_income_credit",
        "total_child_tax_credit",
        "num_with_business_income",
        "total_farm_income",
        "num_with_farm_income",
        "num_schedule_c",
        "total_schedule_c_income",
        "total_schedule_c_receipts",
        "num_partnership_income",
        "total_partnership_income",
        "num_rental_income",
        "total_rental_income",
        "num_with_se_tax",
        "total_se_tax",
    ]

    if column_name in numeric_columns:
        try:
            # Remove commas and convert
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            return int(float(value))
        except (ValueError, TypeError):
            return None

    # String columns - strip whitespace
    if isinstance(value, str):
        return value.strip()

    return value


# ========== Dataset Metadata ==========


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "zip_income": "IRS SOI Individual Income by ZIP Code",
        "county_income": "IRS SOI Individual Income by County",
        "migration": "IRS SOI County-to-County Migration Data",
        "business_income": "IRS SOI Business Income by ZIP Code",
    }
    return display_names.get(dataset, f"IRS SOI {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "zip_income": (
            "Individual income tax statistics by ZIP code from IRS Statistics of Income. "
            "Includes number of returns, AGI, wages, dividends, capital gains, and other "
            "income sources broken down by AGI bracket."
        ),
        "county_income": (
            "Individual income tax statistics by county from IRS Statistics of Income. "
            "Similar to ZIP data but aggregated at the county level with FIPS codes."
        ),
        "migration": (
            "County-to-county migration data derived from tax return address changes. "
            "Shows migration flows including number of migrants and their aggregate income."
        ),
        "business_income": (
            "Business and self-employment income statistics by ZIP code. Includes Schedule C "
            "(sole proprietorships), partnership income, rental income, and self-employment tax."
        ),
    }
    return descriptions.get(
        dataset,
        "Income and wealth distribution statistics from IRS Statistics of Income.",
    )


# State FIPS codes reference
STATE_FIPS = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "72": "PR",
}
