"""
BLS metadata utilities.

Handles:
- Table name generation for each BLS dataset
- CREATE TABLE SQL generation with proper typed columns
- Data parsing and transformation
- Dataset descriptions and categories
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE NAME MAPPING
# =============================================================================

DATASET_TABLES = {
    "ces": "bls_ces_employment",
    "cps": "bls_cps_labor_force",
    "jolts": "bls_jolts",
    "cpi": "bls_cpi",
    "ppi": "bls_ppi",
    "oes": "bls_oes",
}


def generate_table_name(dataset: str) -> str:
    """
    Generate table name for BLS dataset.

    Convention: bls_{dataset}_description

    Args:
        dataset: Dataset name (ces, cps, jolts, cpi, ppi, oes)

    Returns:
        Table name (e.g., "bls_ces_employment")

    Raises:
        ValueError: If dataset is not recognized
    """
    dataset_lower = dataset.lower()

    if dataset_lower not in DATASET_TABLES:
        available = ", ".join(DATASET_TABLES.keys())
        raise ValueError(
            f"Unknown BLS dataset: {dataset}. " f"Available datasets: {available}"
        )

    return DATASET_TABLES[dataset_lower]


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for BLS data.

    Table schema (common for all BLS datasets):
    - id SERIAL PRIMARY KEY
    - series_id TEXT: BLS series ID
    - series_title TEXT: Human-readable title (if available)
    - year INTEGER: Observation year
    - period TEXT: Period code (e.g., "M01" for January, "Q1" for Q1)
    - period_name TEXT: Human-readable period (e.g., "January")
    - value NUMERIC: Observation value
    - footnote_codes TEXT: Footnote codes from BLS
    - ingested_at TIMESTAMP: When the data was ingested

    Unique constraint: (series_id, year, period)

    Args:
        table_name: Name of the table to create
        dataset: Dataset type for comments

    Returns:
        CREATE TABLE SQL statement (idempotent)
    """
    dataset_description = get_dataset_description(dataset)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        series_id TEXT NOT NULL,
        series_title TEXT,
        year INTEGER NOT NULL,
        period TEXT NOT NULL,
        period_name TEXT,
        value NUMERIC(20, 6),
        footnote_codes TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        CONSTRAINT {table_name}_unique UNIQUE (series_id, year, period)
    );
    
    -- Create indexes for common query patterns
    CREATE INDEX IF NOT EXISTS idx_{table_name}_series_id ON {table_name} (series_id);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name} (period);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_series_year ON {table_name} (series_id, year);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'BLS {dataset.upper()} - {dataset_description}';
    """

    return sql


# =============================================================================
# DATA PARSING
# =============================================================================


def parse_bls_observation(
    obs: Dict[str, Any], series_id: str, series_title: Optional[str] = None
) -> Dict[str, Any]:
    """
    Parse a single BLS API observation into a database row.

    BLS API observation format:
    {
        "year": "2024",
        "period": "M01",
        "periodName": "January",
        "value": "3.7",
        "footnotes": [{"code": "P", "text": "Preliminary"}]
    }

    Args:
        obs: Raw observation from BLS API
        series_id: BLS series ID
        series_title: Human-readable title (optional)

    Returns:
        Dictionary suitable for database insertion
    """
    year_str = obs.get("year")
    period = obs.get("period")
    value_str = obs.get("value")

    # Skip if missing required fields
    if not year_str or not period:
        logger.warning(f"Skipping observation with missing year/period: {obs}")
        return None

    # Parse year
    try:
        year = int(year_str)
    except (ValueError, TypeError):
        logger.warning(f"Invalid year '{year_str}' for {series_id}")
        return None

    # Parse value (may be missing or "-" for unavailable)
    value = None
    if value_str and value_str not in ("-", "", "N/A"):
        try:
            value = float(value_str)
        except ValueError:
            logger.warning(
                f"Invalid value '{value_str}' for {series_id} on {year} {period}"
            )
            # Continue with None value

    # Extract footnote codes
    footnotes = obs.get("footnotes", [])
    footnote_codes = None
    if footnotes:
        codes = [f.get("code", "") for f in footnotes if f.get("code")]
        if codes:
            footnote_codes = ",".join(codes)

    return {
        "series_id": series_id,
        "series_title": series_title,
        "year": year,
        "period": period,
        "period_name": obs.get("periodName"),
        "value": value,
        "footnote_codes": footnote_codes,
    }


def parse_bls_series_response(
    api_response: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse BLS API response containing multiple series.

    BLS API response format:
    {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 123,
        "Results": {
            "series": [
                {
                    "seriesID": "LNS14000000",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M01",
                            "periodName": "January",
                            "value": "3.7",
                            "footnotes": [{}]
                        },
                        ...
                    ]
                }
            ]
        }
    }

    Args:
        api_response: Raw API response

    Returns:
        Dict mapping series_id to list of parsed observations
    """
    results: Dict[str, List[Dict[str, Any]]] = {}

    if api_response.get("status") != "REQUEST_SUCCEEDED":
        logger.warning(f"BLS API response status: {api_response.get('status')}")
        return results

    series_list = api_response.get("Results", {}).get("series", [])

    for series in series_list:
        series_id = series.get("seriesID")
        if not series_id:
            continue

        observations = series.get("data", [])
        parsed_obs = []

        for obs in observations:
            parsed = parse_bls_observation(obs, series_id)
            if parsed:
                parsed_obs.append(parsed)

        results[series_id] = parsed_obs
        logger.debug(f"Parsed {len(parsed_obs)} observations for {series_id}")

    return results


def build_insert_values(
    parsed_data: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """
    Build list of dictionaries for parameterized INSERT.

    Args:
        parsed_data: Dict mapping series_id to list of parsed observations

    Returns:
        Flat list of dictionaries for parameterized INSERT
    """
    all_rows = []

    for series_id, observations in parsed_data.items():
        all_rows.extend(observations)

    logger.info(f"Built {len(all_rows)} rows for insertion")
    return all_rows


# =============================================================================
# DATASET METADATA
# =============================================================================


def get_dataset_display_name(dataset: str) -> str:
    """
    Get display name for a BLS dataset.

    Args:
        dataset: Dataset code (ces, cps, jolts, cpi, ppi, oes)

    Returns:
        Human-readable display name
    """
    display_names = {
        "ces": "Current Employment Statistics (CES)",
        "cps": "Current Population Survey (CPS)",
        "jolts": "Job Openings and Labor Turnover Survey (JOLTS)",
        "cpi": "Consumer Price Index (CPI)",
        "ppi": "Producer Price Index (PPI)",
        "oes": "Occupational Employment Statistics (OES)",
    }

    return display_names.get(dataset.lower(), dataset.upper())


def get_dataset_description(dataset: str) -> str:
    """
    Get description for a BLS dataset.

    Args:
        dataset: Dataset code

    Returns:
        Description text
    """
    descriptions = {
        "ces": (
            "Monthly employment, hours, and earnings data by industry. "
            "Includes total nonfarm employment, average hourly earnings, "
            "and average weekly hours worked."
        ),
        "cps": (
            "Labor force status including unemployment rate, labor force "
            "participation rate, employment-population ratio, and demographic "
            "breakdowns of the labor force."
        ),
        "jolts": (
            "Job openings, hires, quits, layoffs and separations by industry. "
            "Key indicators of labor market dynamics and worker confidence."
        ),
        "cpi": (
            "Consumer Price Index measuring changes in prices paid by urban "
            "consumers for goods and services. Includes headline CPI, core CPI "
            "(excluding food and energy), and category-specific indexes."
        ),
        "ppi": (
            "Producer Price Index measuring price changes from the perspective "
            "of sellers. Tracks wholesale prices and inflation pressures in the "
            "production pipeline."
        ),
        "oes": (
            "Employment and wage estimates by occupation and industry. "
            "Annual survey covering approximately 1.2 million establishments."
        ),
    }

    return descriptions.get(
        dataset.lower(), f"Bureau of Labor Statistics {dataset.upper()} data series"
    )


def get_default_date_range(api_key_present: bool = False) -> tuple[int, int]:
    """
    Get default year range for BLS data ingestion.

    Args:
        api_key_present: Whether an API key is configured

    Returns:
        Tuple of (start_year, end_year)

    Default: Last 10 years without key, 20 years with key
    """
    end_year = datetime.now().year
    max_years = 20 if api_key_present else 10
    start_year = end_year - max_years + 1

    return (start_year, end_year)


def validate_year_range(
    start_year: int, end_year: int, api_key_present: bool = False
) -> bool:
    """
    Validate year range against BLS API limits.

    Args:
        start_year: Start year
        end_year: End year
        api_key_present: Whether an API key is configured

    Returns:
        True if valid

    Raises:
        ValueError: If range is invalid
    """
    max_years = 20 if api_key_present else 10

    if start_year > end_year:
        raise ValueError(f"Start year ({start_year}) must be <= end year ({end_year})")

    if end_year - start_year > max_years:
        raise ValueError(
            f"Year range ({end_year - start_year} years) exceeds maximum "
            f"({max_years} years {'with' if api_key_present else 'without'} API key)"
        )

    current_year = datetime.now().year
    if end_year > current_year + 1:
        raise ValueError(f"End year ({end_year}) cannot be in the future")

    if start_year < 1900:
        raise ValueError(f"Start year ({start_year}) is too early")

    return True


# =============================================================================
# SERIES ID REFERENCE
# =============================================================================

# Complete reference of all common BLS series for documentation
ALL_SERIES_REFERENCE = {
    "cps": {
        "description": "Current Population Survey - Labor Force Statistics",
        "series": {
            "LNS14000000": "Unemployment Rate (seasonally adjusted)",
            "LNS11300000": "Labor Force Participation Rate",
            "LNS12000000": "Employment Level",
            "LNS13000000": "Unemployment Level",
            "LNS11000000": "Civilian Labor Force Level",
            "LNS12300000": "Employment-Population Ratio",
            "LNS13327709": "U-6 Total unemployed + marginally attached + part-time for economic reasons",
        },
    },
    "ces": {
        "description": "Current Employment Statistics - Establishment Survey",
        "series": {
            "CES0000000001": "Total Nonfarm Employment",
            "CES0500000001": "Total Private Employment",
            "CES3000000001": "Manufacturing Employment",
            "CES2000000001": "Construction Employment",
            "CES4200000001": "Retail Trade Employment",
            "CES7000000001": "Leisure and Hospitality Employment",
            "CES6000000001": "Professional and Business Services",
            "CES6500000001": "Education and Health Services",
            "CES5500000001": "Financial Activities",
            "CES0500000003": "Average Hourly Earnings, All Private",
            "CES0500000002": "Average Weekly Hours, All Private",
        },
    },
    "jolts": {
        "description": "Job Openings and Labor Turnover Survey",
        "series": {
            "JTS000000000000000JOL": "Total Job Openings Level",
            "JTS000000000000000JOR": "Job Openings Rate",
            "JTS000000000000000HIL": "Total Hires Level",
            "JTS000000000000000HIR": "Hires Rate",
            "JTS000000000000000QUL": "Total Quits Level",
            "JTS000000000000000QUR": "Quits Rate",
            "JTS000000000000000LDL": "Total Layoffs and Discharges Level",
            "JTS000000000000000LDR": "Layoffs and Discharges Rate",
            "JTS000000000000000TSL": "Total Separations Level",
            "JTS000000000000000TSR": "Total Separations Rate",
        },
    },
    "cpi": {
        "description": "Consumer Price Index - Measures of Inflation",
        "series": {
            "CUUR0000SA0": "CPI-U All Items (urban consumers)",
            "CUUR0000SA0L1E": "Core CPI (less food and energy)",
            "CUUR0000SAF1": "CPI Food",
            "CUUR0000SAF11": "CPI Food at Home",
            "CUUR0000SA0E": "CPI Energy",
            "CUUR0000SETB01": "CPI Gasoline",
            "CUUR0000SEHE": "CPI Electricity",
            "CUUR0000SAH1": "CPI Shelter",
            "CUUR0000SAM": "CPI Medical Care",
            "CUUR0000SAT": "CPI Transportation",
            "CUUR0000SAA": "CPI Apparel",
            "CUSR0000SA0": "CPI-U All Items (seasonally adjusted)",
            "CUSR0000SA0L1E": "Core CPI (seasonally adjusted)",
        },
    },
    "ppi": {
        "description": "Producer Price Index - Wholesale/Producer Prices",
        "series": {
            "WPSFD4": "PPI Final Demand",
            "WPSFD41": "PPI Final Demand Goods",
            "WPSFD42": "PPI Final Demand Services",
            "WPSID61": "PPI Intermediate Demand",
            "WPUIP1000000": "PPI Crude Materials for Further Processing",
            "WPUFD49104": "PPI Finished Goods",
            "PCU31-33--31-33--": "PPI Manufacturing Industries",
            "PCU23----23----": "PPI Construction",
        },
    },
    "oes": {
        "description": "Occupational Employment and Wage Statistics - Aesthetics-Adjacent Occupations",
        "series": {
            "OEUN000000000000029122901": "Employment - Physicians, All Other (incl. Dermatologists)",
            "OEUN000000000000029122904": "Annual Mean Wage - Physicians, All Other",
            "OEUN000000000000029114101": "Employment - Registered Nurses",
            "OEUN000000000000029114104": "Annual Mean Wage - Registered Nurses",
            "OEUN000000000000029117101": "Employment - Nurse Practitioners",
            "OEUN000000000000029117104": "Annual Mean Wage - Nurse Practitioners",
            "OEUN000000000000029107101": "Employment - Physician Assistants",
            "OEUN000000000000029107104": "Annual Mean Wage - Physician Assistants",
            "OEUN000000000000031901101": "Employment - Massage Therapists",
            "OEUN000000000000031901104": "Annual Mean Wage - Massage Therapists",
            "OEUN000000000000039501201": "Employment - Hairdressers/Hairstylists/Cosmetologists",
            "OEUN000000000000039501204": "Annual Mean Wage - Hairdressers/Hairstylists/Cosmetologists",
            "OEUN000000000000031909901": "Employment - Healthcare Support Workers, All Other",
            "OEUN000000000000031909904": "Annual Mean Wage - Healthcare Support Workers, All Other",
            "OEUN000000000000029209901": "Employment - Health Technologists & Technicians, All Other",
            "OEUN000000000000029209904": "Annual Mean Wage - Health Technologists & Technicians, All Other",
        },
    },
}


def get_series_reference(dataset: Optional[str] = None) -> Dict[str, Any]:
    """
    Get reference information about BLS series.

    Args:
        dataset: Optional dataset to filter by

    Returns:
        Dictionary with series reference information
    """
    if dataset:
        dataset_lower = dataset.lower()
        if dataset_lower in ALL_SERIES_REFERENCE:
            return {dataset_lower: ALL_SERIES_REFERENCE[dataset_lower]}
        else:
            raise ValueError(f"Unknown dataset: {dataset}")

    return ALL_SERIES_REFERENCE
