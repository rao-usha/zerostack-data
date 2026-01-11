"""
Data Commons metadata and schema generation utilities.

Handles:
- Table name generation for Data Commons datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for various data types
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Statistical observations schema
STAT_OBSERVATION_COLUMNS = {
    "variable_dcid": ("TEXT", "Statistical variable DCID"),
    "variable_name": ("TEXT", "Human-readable variable name"),
    "entity_dcid": ("TEXT", "Entity (place) DCID"),
    "entity_name": ("TEXT", "Entity name"),
    "observation_date": ("TEXT", "Observation date (YYYY or YYYY-MM-DD)"),
    "observation_value": ("NUMERIC(20,6)", "Observation value"),
    "unit": ("TEXT", "Unit of measurement"),
    "measurement_method": ("TEXT", "How data was measured/collected"),
    "provenance_url": ("TEXT", "Data source URL"),
}

# Place statistics schema (aggregated by place)
PLACE_STATS_COLUMNS = {
    "place_dcid": ("TEXT", "Place DCID"),
    "place_name": ("TEXT", "Place name"),
    "place_type": ("TEXT", "Place type (State, County, City, etc.)"),
    "variable_dcid": ("TEXT", "Statistical variable DCID"),
    "variable_name": ("TEXT", "Human-readable variable name"),
    "latest_date": ("TEXT", "Most recent observation date"),
    "latest_value": ("NUMERIC(20,6)", "Most recent observation value"),
    "earliest_date": ("TEXT", "Earliest observation date"),
    "earliest_value": ("NUMERIC(20,6)", "Earliest observation value"),
    "observation_count": ("INTEGER", "Number of observations"),
}

# Time series schema
TIME_SERIES_COLUMNS = {
    "place_dcid": ("TEXT", "Place DCID"),
    "place_name": ("TEXT", "Place name"),
    "variable_dcid": ("TEXT", "Statistical variable DCID"),
    "variable_name": ("TEXT", "Human-readable variable name"),
    "observation_date": ("DATE", "Observation date"),
    "observation_value": ("NUMERIC(20,6)", "Observation value"),
    "year": ("INTEGER", "Year extracted from date"),
    "month": ("INTEGER", "Month extracted from date (if applicable)"),
}


def generate_table_name(dataset: str, variable_id: Optional[str] = None) -> str:
    """
    Generate PostgreSQL table name for Data Commons data.
    
    Args:
        dataset: Dataset identifier (observations, place_stats, time_series)
        variable_id: Optional specific variable identifier
        
    Returns:
        PostgreSQL table name
        
    Examples:
        >>> generate_table_name("observations")
        'data_commons_observations'
        >>> generate_table_name("place_stats", "population")
        'data_commons_place_stats_population'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    
    if variable_id:
        var_clean = variable_id.lower().replace("-", "_").replace(" ", "_")[:50]
        return f"data_commons_{dataset_clean}_{var_clean}"
    else:
        return f"data_commons_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for Data Commons data.
    
    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema
        
    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "observations":
        columns = STAT_OBSERVATION_COLUMNS
        unique_constraint = "variable_dcid, entity_dcid, observation_date"
        indexes = [
            ("variable_dcid", "variable_dcid"),
            ("entity_dcid", "entity_dcid"),
            ("observation_date", "observation_date"),
        ]
    elif dataset == "place_stats":
        columns = PLACE_STATS_COLUMNS
        unique_constraint = "place_dcid, variable_dcid"
        indexes = [
            ("place_dcid", "place_dcid"),
            ("place_type", "place_type"),
            ("variable_dcid", "variable_dcid"),
        ]
    elif dataset == "time_series":
        columns = TIME_SERIES_COLUMNS
        unique_constraint = "place_dcid, variable_dcid, observation_date"
        indexes = [
            ("place_dcid", "place_dcid"),
            ("variable_dcid", "variable_dcid"),
            ("observation_date", "observation_date"),
            ("year", "year"),
        ]
    else:
        raise ValueError(f"Unknown Data Commons dataset: {dataset}")
    
    # Build column definitions
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")
    
    for col_name, (col_type, _) in columns.items():
        column_defs.append(f"{col_name} {col_type}")
    
    column_defs.append("ingested_at TIMESTAMP DEFAULT NOW()")
    
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


def parse_observation_response(
    response: Dict[str, Any],
    variable_dcid: str,
    variable_name: str
) -> List[Dict[str, Any]]:
    """
    Parse observation data from Data Commons API response.
    
    Args:
        response: Raw API response
        variable_dcid: Statistical variable DCID
        variable_name: Human-readable variable name
        
    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []
    
    try:
        # Handle V2 API response format
        data = response.get("byVariable", {}).get(variable_dcid, {})
        
        if not data:
            # Try alternative format
            data = response.get("data", {}).get(variable_dcid, {})
        
        if not data:
            logger.warning("No observation data in response")
            return []
        
        by_entity = data.get("byEntity", {})
        
        for entity_dcid, entity_data in by_entity.items():
            ordered_facets = entity_data.get("orderedFacets", [])
            
            for facet in ordered_facets:
                facet_id = facet.get("facetId", "")
                observations = facet.get("observations", [])
                
                for obs in observations:
                    try:
                        parsed = {
                            "variable_dcid": variable_dcid,
                            "variable_name": variable_name,
                            "entity_dcid": entity_dcid,
                            "entity_name": "",  # Will be enriched later
                            "observation_date": obs.get("date"),
                            "observation_value": _safe_float(obs.get("value")),
                            "unit": "",
                            "measurement_method": facet_id,
                            "provenance_url": facet.get("provenanceUrl", ""),
                        }
                        
                        if parsed["observation_date"] and parsed["observation_value"] is not None:
                            parsed_records.append(parsed)
                    
                    except Exception as e:
                        logger.warning(f"Failed to parse observation: {e}")
        
        logger.info(f"Parsed {len(parsed_records)} observation records")
        return parsed_records
    
    except Exception as e:
        logger.error(f"Failed to parse observation response: {e}")
        return []


def parse_places_response(
    response: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Parse places data from Data Commons API response.
    
    Args:
        response: Raw API response
        
    Returns:
        List of place records with dcid and name
    """
    places = []
    
    try:
        data = response.get("data", {})
        
        for node_id, node_data in data.items():
            arcs = node_data.get("arcs", {})
            
            for arc_name, arc_data in arcs.items():
                nodes = arc_data.get("nodes", [])
                
                for node in nodes:
                    place = {
                        "dcid": node.get("dcid", ""),
                        "name": node.get("name", ""),
                        "types": node.get("types", []),
                    }
                    if place["dcid"]:
                        places.append(place)
        
        return places
    
    except Exception as e:
        logger.error(f"Failed to parse places response: {e}")
        return []


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "observations": "Data Commons Statistical Observations",
        "place_stats": "Data Commons Place Statistics Summary",
        "time_series": "Data Commons Time Series Data",
    }
    return display_names.get(dataset, f"Data Commons {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "observations": (
            "Statistical observations from Data Commons knowledge graph, "
            "aggregating data from 200+ public data sources including "
            "Census, BLS, CDC, FBI, World Bank, and more."
        ),
        "place_stats": (
            "Aggregated statistics by place (country, state, county, city) "
            "with latest and earliest values for each statistical variable."
        ),
        "time_series": (
            "Time series data for statistical variables, "
            "enabling trend analysis and forecasting."
        ),
    }
    return descriptions.get(
        dataset,
        "Public data from Google Data Commons knowledge graph"
    )


# Statistical Variable Categories
STAT_VAR_CATEGORIES = {
    "demographics": [
        "Count_Person",
        "Count_Person_Male",
        "Count_Person_Female",
        "Median_Age_Person",
        "Count_Household",
        "Count_HousingUnit",
    ],
    "income": [
        "Median_Income_Person",
        "Median_Income_Household",
        "Mean_Income_Person",
        "Count_Person_BelowPovertyLine",
    ],
    "employment": [
        "Count_Person_Employed",
        "Count_Person_Unemployed",
        "UnemploymentRate_Person",
    ],
    "education": [
        "Count_Person_EducationalAttainmentBachelorsDegree",
        "Count_Person_EducationalAttainmentGraduateOrProfessionalDegree",
    ],
    "health": [
        "Count_MedicalConditionIncident_COVID_19_ConfirmedCase",
        "Count_Death",
        "LifeExpectancy_Person",
    ],
    "crime": [
        "Count_CriminalActivities_CombinedCrime",
        "Count_CriminalActivities_ViolentCrime",
        "Count_CriminalActivities_PropertyCrime",
    ],
    "economy": [
        "Amount_EconomicActivity_GrossDomesticProduction_RealValue",
        "GrowthRate_Amount_EconomicActivity_GrossDomesticProduction",
    ],
    "environment": [
        "Mean_Temperature",
        "Mean_Precipitation",
        "Concentration_AirPollutant_PM2.5",
    ],
}

# Place Type Hierarchy
PLACE_TYPES = {
    "Country": "Top-level country (e.g., USA)",
    "State": "US State or equivalent administrative division",
    "County": "US County or equivalent",
    "City": "City or town",
    "CensusZipCodeTabulationArea": "ZIP code area",
    "CongressionalDistrict": "US Congressional district",
    "SchoolDistrict": "School district boundary",
}
