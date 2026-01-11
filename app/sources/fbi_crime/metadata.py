"""
FBI Crime Data Explorer metadata utilities.

Handles:
- Dataset definitions and categories
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================
# Table Name Generation
# ============================================

def generate_table_name(dataset_type: str, scope: str = "national") -> str:
    """
    Generate table name for FBI crime dataset.
    
    Convention: fbi_crime_{dataset_type}_{scope}
    
    Args:
        dataset_type: Type of data (estimates, summarized, nibrs, hate_crime, leoka)
        scope: Scope of data (national, state, regional)
        
    Returns:
        Table name (e.g., "fbi_crime_estimates_national")
    """
    sanitized_type = dataset_type.lower().replace("-", "_").replace(" ", "_")
    sanitized_scope = scope.lower().replace("-", "_").replace(" ", "_")
    return f"fbi_crime_{sanitized_type}_{sanitized_scope}"


# ============================================
# CREATE TABLE SQL Generation
# ============================================

def generate_estimates_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for FBI crime estimates data.
    
    Table schema designed for national and state estimates data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        state_abbr VARCHAR(2),
        state_name VARCHAR(50),
        region_name VARCHAR(50),
        offense VARCHAR(50) NOT NULL,
        
        -- Crime counts
        population BIGINT,
        violent_crime INTEGER,
        homicide INTEGER,
        rape_legacy INTEGER,
        rape_revised INTEGER,
        robbery INTEGER,
        aggravated_assault INTEGER,
        property_crime INTEGER,
        burglary INTEGER,
        larceny INTEGER,
        motor_vehicle_theft INTEGER,
        arson INTEGER,
        
        -- Rates per 100,000 population
        violent_crime_rate NUMERIC(10, 2),
        homicide_rate NUMERIC(10, 2),
        rape_legacy_rate NUMERIC(10, 2),
        rape_revised_rate NUMERIC(10, 2),
        robbery_rate NUMERIC(10, 2),
        aggravated_assault_rate NUMERIC(10, 2),
        property_crime_rate NUMERIC(10, 2),
        burglary_rate NUMERIC(10, 2),
        larceny_rate NUMERIC(10, 2),
        motor_vehicle_theft_rate NUMERIC(10, 2),
        
        -- Metadata
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (year, state_abbr, offense)
    );
    
    -- Create indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_offense ON {table_name} (offense);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year_state ON {table_name} (year, state_abbr);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - Crime estimates (UCR data)';
    """
    return sql


def generate_summarized_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for FBI summarized crime data.
    
    Summarized data is aggregated by state/agency over time.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        month INTEGER,
        state_abbr VARCHAR(2) NOT NULL,
        ori VARCHAR(20),
        agency_name TEXT,
        offense VARCHAR(50) NOT NULL,
        
        -- Crime counts
        actual INTEGER,
        cleared INTEGER,
        cleared_18_under INTEGER,
        
        -- Metadata
        data_year INTEGER,
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (year, month, state_abbr, ori, offense)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_offense ON {table_name} (offense);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_ori ON {table_name} (ori);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - Summarized crime data by agency';
    """
    return sql


def generate_nibrs_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for NIBRS data.
    
    NIBRS provides detailed incident-based reporting.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        data_year INTEGER NOT NULL,
        state_abbr VARCHAR(2) NOT NULL,
        offense_code VARCHAR(20),
        offense_name TEXT,
        offense_category VARCHAR(50),
        
        -- Counts by variable type
        variable_name VARCHAR(100),
        variable_value VARCHAR(100),
        count INTEGER,
        
        -- Demographics (when applicable)
        victim_count INTEGER,
        offender_count INTEGER,
        
        -- Metadata
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (data_year, state_abbr, offense_code, variable_name, variable_value)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (data_year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_offense ON {table_name} (offense_code);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_variable ON {table_name} (variable_name);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - NIBRS incident-based data';
    """
    return sql


def generate_hate_crime_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for hate crime statistics.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        data_year INTEGER NOT NULL,
        state_abbr VARCHAR(2),
        state_name VARCHAR(50),
        
        -- Hate crime categories
        bias_motivation VARCHAR(100),
        offense_name VARCHAR(100),
        victim_type VARCHAR(50),
        
        -- Counts
        incident_count INTEGER,
        offense_count INTEGER,
        victim_count INTEGER,
        
        -- Metadata
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (data_year, state_abbr, bias_motivation, offense_name)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (data_year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_bias ON {table_name} (bias_motivation);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - Hate crime statistics';
    """
    return sql


def generate_leoka_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for LEOKA (Law Enforcement Officers Killed and Assaulted) data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        data_year INTEGER NOT NULL,
        state_abbr VARCHAR(2),
        state_name VARCHAR(50),
        
        -- LEOKA statistics
        feloniously_killed INTEGER,
        accidentally_killed INTEGER,
        assaulted INTEGER,
        assaulted_weapon_firearm INTEGER,
        assaulted_weapon_knife INTEGER,
        assaulted_weapon_other INTEGER,
        assaulted_weapon_hands INTEGER,
        
        -- Activity type when killed/assaulted
        activity_type VARCHAR(100),
        activity_count INTEGER,
        
        -- Metadata
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (data_year, state_abbr, activity_type)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (data_year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - Law Enforcement Officers Killed and Assaulted';
    """
    return sql


def generate_participation_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for agency participation data.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        data_year INTEGER NOT NULL,
        state_abbr VARCHAR(2),
        state_name VARCHAR(50),
        
        -- Participation statistics
        total_agencies INTEGER,
        participating_agencies INTEGER,
        participation_rate NUMERIC(5, 2),
        population_covered BIGINT,
        nibrs_participating INTEGER,
        
        -- Metadata
        source_url TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE (data_year, state_abbr)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name} (data_year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name} (state_abbr);
    
    COMMENT ON TABLE {table_name} IS 'FBI Crime Data Explorer - Agency participation rates';
    """
    return sql


# ============================================
# Data Parsing Functions
# ============================================

def parse_national_estimates(
    api_response: Any,
    offense: str
) -> List[Dict[str, Any]]:
    """
    Parse national estimates API response into database rows.
    
    Args:
        api_response: Raw API response (can be list or dict)
        offense: Offense type being queried
        
    Returns:
        List of dictionaries suitable for database insertion
    """
    # Handle different response formats
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
        if not results and not isinstance(api_response.get("year"), type(None)):
            # Single result in root
            results = [api_response]
    else:
        logger.warning(f"Unexpected response format for {offense}: {type(api_response)}")
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "year": item.get("year") or item.get("data_year"),
            "state_abbr": None,  # National data
            "state_name": "United States",
            "region_name": None,
            "offense": offense,
            "population": item.get("population"),
            "violent_crime": item.get("violent_crime"),
            "homicide": item.get("homicide"),
            "rape_legacy": item.get("rape_legacy"),
            "rape_revised": item.get("rape_revised") or item.get("rape"),
            "robbery": item.get("robbery"),
            "aggravated_assault": item.get("aggravated_assault"),
            "property_crime": item.get("property_crime"),
            "burglary": item.get("burglary"),
            "larceny": item.get("larceny"),
            "motor_vehicle_theft": item.get("motor_vehicle_theft"),
            "arson": item.get("arson"),
            "violent_crime_rate": _safe_float(item.get("violent_crime_rate")),
            "homicide_rate": _safe_float(item.get("homicide_rate")),
            "rape_legacy_rate": _safe_float(item.get("rape_legacy_rate")),
            "rape_revised_rate": _safe_float(item.get("rape_revised_rate")),
            "robbery_rate": _safe_float(item.get("robbery_rate")),
            "aggravated_assault_rate": _safe_float(item.get("aggravated_assault_rate")),
            "property_crime_rate": _safe_float(item.get("property_crime_rate")),
            "burglary_rate": _safe_float(item.get("burglary_rate")),
            "larceny_rate": _safe_float(item.get("larceny_rate")),
            "motor_vehicle_theft_rate": _safe_float(item.get("motor_vehicle_theft_rate")),
        }
        
        if row.get("year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_state_estimates(
    api_response: Any,
    state_abbr: str,
    offense: str
) -> List[Dict[str, Any]]:
    """
    Parse state estimates API response into database rows.
    
    Args:
        api_response: Raw API response
        state_abbr: State abbreviation
        offense: Offense type being queried
        
    Returns:
        List of dictionaries suitable for database insertion
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
        if not results and api_response.get("year"):
            results = [api_response]
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "year": item.get("year") or item.get("data_year"),
            "state_abbr": state_abbr.upper(),
            "state_name": item.get("state_name"),
            "region_name": item.get("region_name"),
            "offense": offense,
            "population": item.get("population"),
            "violent_crime": item.get("violent_crime"),
            "homicide": item.get("homicide"),
            "rape_legacy": item.get("rape_legacy"),
            "rape_revised": item.get("rape_revised") or item.get("rape"),
            "robbery": item.get("robbery"),
            "aggravated_assault": item.get("aggravated_assault"),
            "property_crime": item.get("property_crime"),
            "burglary": item.get("burglary"),
            "larceny": item.get("larceny"),
            "motor_vehicle_theft": item.get("motor_vehicle_theft"),
            "arson": item.get("arson"),
            "violent_crime_rate": _safe_float(item.get("violent_crime_rate")),
            "homicide_rate": _safe_float(item.get("homicide_rate")),
            "rape_legacy_rate": _safe_float(item.get("rape_legacy_rate")),
            "rape_revised_rate": _safe_float(item.get("rape_revised_rate")),
            "robbery_rate": _safe_float(item.get("robbery_rate")),
            "aggravated_assault_rate": _safe_float(item.get("aggravated_assault_rate")),
            "property_crime_rate": _safe_float(item.get("property_crime_rate")),
            "burglary_rate": _safe_float(item.get("burglary_rate")),
            "larceny_rate": _safe_float(item.get("larceny_rate")),
            "motor_vehicle_theft_rate": _safe_float(item.get("motor_vehicle_theft_rate")),
        }
        
        if row.get("year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_summarized_data(
    api_response: Any,
    state_abbr: str,
    offense: str
) -> List[Dict[str, Any]]:
    """
    Parse summarized crime data API response into database rows.
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "year": item.get("year") or item.get("data_year"),
            "month": item.get("month"),
            "state_abbr": state_abbr.upper(),
            "ori": item.get("ori"),
            "agency_name": item.get("agency_name") or item.get("pub_agency_name"),
            "offense": offense,
            "actual": item.get("actual"),
            "cleared": item.get("cleared"),
            "cleared_18_under": item.get("cleared_18_under"),
            "data_year": item.get("data_year"),
        }
        
        if row.get("year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_nibrs_data(
    api_response: Any,
    state_abbr: str,
    variable: str
) -> List[Dict[str, Any]]:
    """
    Parse NIBRS data API response into database rows.
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
        
        # NIBRS data can have nested structure
        data_year = item.get("data_year") or item.get("year")
        
        row = {
            "data_year": data_year,
            "state_abbr": state_abbr.upper(),
            "offense_code": item.get("offense_code") or item.get("key"),
            "offense_name": item.get("offense_name") or item.get("offense"),
            "offense_category": item.get("offense_category"),
            "variable_name": variable,
            "variable_value": item.get("range") or item.get("key") or str(item.get("value", "")),
            "count": item.get("count") or item.get("value"),
            "victim_count": item.get("victim_count"),
            "offender_count": item.get("offender_count"),
        }
        
        if row.get("data_year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_hate_crime_data(
    api_response: Any,
    state_abbr: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Parse hate crime data API response into database rows.
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "data_year": item.get("data_year") or item.get("year"),
            "state_abbr": state_abbr,
            "state_name": item.get("state_name"),
            "bias_motivation": item.get("bias_motivation") or item.get("bias_desc"),
            "offense_name": item.get("offense_name"),
            "victim_type": item.get("victim_type"),
            "incident_count": item.get("incident_count") or item.get("total_incidents"),
            "offense_count": item.get("offense_count") or item.get("total_offenses"),
            "victim_count": item.get("victim_count") or item.get("total_victims"),
        }
        
        if row.get("data_year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_leoka_data(
    api_response: Any,
    state_abbr: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Parse LEOKA data API response into database rows.
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "data_year": item.get("data_year") or item.get("year"),
            "state_abbr": state_abbr,
            "state_name": item.get("state_name"),
            "feloniously_killed": item.get("feloniously_killed"),
            "accidentally_killed": item.get("accidentally_killed"),
            "assaulted": item.get("assaulted") or item.get("assault_count"),
            "assaulted_weapon_firearm": item.get("assaulted_weapon_firearm"),
            "assaulted_weapon_knife": item.get("assaulted_weapon_knife"),
            "assaulted_weapon_other": item.get("assaulted_weapon_other"),
            "assaulted_weapon_hands": item.get("assaulted_weapon_hands"),
            "activity_type": item.get("activity_type") or item.get("circumstance"),
            "activity_count": item.get("activity_count"),
        }
        
        if row.get("data_year"):
            parsed_rows.append(row)
    
    return parsed_rows


def parse_participation_data(
    api_response: Any,
    state_abbr: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Parse participation data API response into database rows.
    """
    if isinstance(api_response, list):
        results = api_response
    elif isinstance(api_response, dict):
        results = api_response.get("results", api_response.get("data", []))
        if not results:
            # Check if response itself is the data
            if api_response.get("data_year") or api_response.get("year"):
                results = [api_response]
    else:
        return []
    
    parsed_rows = []
    for item in results:
        if not isinstance(item, dict):
            continue
            
        row = {
            "data_year": item.get("data_year") or item.get("year"),
            "state_abbr": state_abbr,
            "state_name": item.get("state_name"),
            "total_agencies": item.get("total_agencies"),
            "participating_agencies": item.get("participating_agencies") or item.get("participating_agency_count"),
            "participation_rate": _safe_float(item.get("participation_rate")),
            "population_covered": item.get("population_covered") or item.get("population"),
            "nibrs_participating": item.get("nibrs_participating") or item.get("nibrs_agency_count"),
        }
        
        if row.get("data_year"):
            parsed_rows.append(row)
    
    return parsed_rows


# ============================================
# Helper Functions
# ============================================

def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_default_year_range() -> Tuple[int, int]:
    """
    Get default year range for FBI crime data ingestion.
    
    Returns:
        Tuple of (start_year, end_year)
        
    Default: 1985 to current year (full available range)
    """
    current_year = datetime.now().year
    # FBI crime data typically goes back to 1985
    return (1985, current_year)


def get_dataset_display_name(dataset_type: str) -> str:
    """
    Get display name for a dataset type.
    """
    display_names = {
        "estimates": "Crime Estimates (UCR)",
        "summarized": "Summarized Agency Data",
        "nibrs": "NIBRS Incident-Based Data",
        "hate_crime": "Hate Crime Statistics",
        "leoka": "Law Enforcement Officers Killed and Assaulted",
        "participation": "Agency Participation Rates",
    }
    return display_names.get(dataset_type.lower(), dataset_type.replace("_", " ").title())


def get_dataset_description(dataset_type: str) -> str:
    """
    Get description for a dataset type.
    """
    descriptions = {
        "estimates": (
            "FBI Uniform Crime Reports (UCR) crime estimates including "
            "violent crime, property crime, homicide, robbery, burglary, and more. "
            "National and state-level data."
        ),
        "summarized": (
            "Summarized crime data by law enforcement agency including "
            "reported offenses, clearances, and agency information."
        ),
        "nibrs": (
            "National Incident-Based Reporting System (NIBRS) data with "
            "detailed incident-level information including victim and offender demographics."
        ),
        "hate_crime": (
            "FBI hate crime statistics including bias motivation, "
            "offense types, and victim counts."
        ),
        "leoka": (
            "Law Enforcement Officers Killed and Assaulted (LEOKA) data "
            "including circumstances and weapon types."
        ),
        "participation": (
            "Agency participation rates in UCR and NIBRS programs, "
            "showing coverage and data completeness."
        ),
    }
    return descriptions.get(
        dataset_type.lower(),
        f"FBI Crime Data - {dataset_type.replace('_', ' ')}"
    )


# Available dataset types for the FBI Crime source
AVAILABLE_DATASETS = [
    "estimates_national",
    "estimates_state",
    "summarized",
    "nibrs_offense",
    "nibrs_victim",
    "nibrs_offender",
    "hate_crime",
    "leoka",
    "participation",
]
