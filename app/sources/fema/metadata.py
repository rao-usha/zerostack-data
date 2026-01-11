"""
OpenFEMA metadata and schema generation utilities.

Handles:
- Table name generation for FEMA datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for disasters, grants, flood insurance
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Disaster Declarations schema
DISASTER_DECLARATIONS_COLUMNS = {
    "disaster_number": ("INTEGER", "Unique disaster number"),
    "declaration_type": ("TEXT", "DR (Major), EM (Emergency), FM (Fire)"),
    "declaration_date": ("DATE", "Date of declaration"),
    "fy_declared": ("INTEGER", "Fiscal year declared"),
    "incident_type": ("TEXT", "Type of incident (Hurricane, Flood, etc.)"),
    "declaration_title": ("TEXT", "Title of the disaster"),
    "state": ("TEXT", "State code"),
    "state_name": ("TEXT", "Full state name"),
    "county": ("TEXT", "County name"),
    "fips_state_code": ("TEXT", "FIPS state code"),
    "fips_county_code": ("TEXT", "FIPS county code"),
    "designated_area": ("TEXT", "Designated area description"),
    "incident_begin_date": ("DATE", "Start date of incident"),
    "incident_end_date": ("DATE", "End date of incident"),
    "ih_program_declared": ("BOOLEAN", "Individual & Households program"),
    "ia_program_declared": ("BOOLEAN", "Individual Assistance program"),
    "pa_program_declared": ("BOOLEAN", "Public Assistance program"),
    "hm_program_declared": ("BOOLEAN", "Hazard Mitigation program"),
    "place_code": ("TEXT", "Place code"),
    "region": ("INTEGER", "FEMA region number"),
}

# Public Assistance Projects schema
PA_PROJECTS_COLUMNS = {
    "disaster_number": ("INTEGER", "Disaster number"),
    "project_number": ("TEXT", "Project identifier"),
    "state": ("TEXT", "State code"),
    "county": ("TEXT", "County name"),
    "applicant_id": ("TEXT", "Applicant identifier"),
    "damage_category": ("TEXT", "Category of damage"),
    "project_size": ("TEXT", "Small or Large project"),
    "project_title": ("TEXT", "Project title"),
    "total_obligated": ("NUMERIC(18,2)", "Total obligated amount"),
    "federal_share_obligated": ("NUMERIC(18,2)", "Federal share"),
    "project_amount": ("NUMERIC(18,2)", "Total project amount"),
    "obligation_date": ("DATE", "Date of obligation"),
}

# Hazard Mitigation Projects schema (v4 API)
HMA_PROJECTS_COLUMNS = {
    "project_identifier": ("TEXT", "Unique project ID"),
    "disaster_number": ("INTEGER", "Associated disaster number"),
    "state": ("TEXT", "State name"),
    "state_code": ("TEXT", "State number code"),
    "county": ("TEXT", "County name"),
    "county_code": ("TEXT", "County code"),
    "region": ("INTEGER", "FEMA region number"),
    "program_area": ("TEXT", "HMGP, PDM, FMA, etc."),
    "program_fy": ("INTEGER", "Program fiscal year"),
    "project_type": ("TEXT", "Type of mitigation project"),
    "status": ("TEXT", "Project status"),
    "recipient": ("TEXT", "Recipient name"),
    "subrecipient": ("TEXT", "Subrecipient name"),
    "project_amount": ("NUMERIC(18,2)", "Total project amount"),
    "federal_share_obligated": ("NUMERIC(18,2)", "Federal share obligated"),
    "cost_share_percentage": ("NUMERIC(5,2)", "Cost share percentage"),
    "benefit_cost_ratio": ("NUMERIC(10,2)", "Benefit cost ratio"),
    "date_approved": ("DATE", "Approval date"),
    "date_closed": ("DATE", "Close date"),
}

# NFIP Policies schema (summarized - full data is very large)
NFIP_POLICIES_COLUMNS = {
    "policy_count": ("INTEGER", "Number of policies"),
    "state": ("TEXT", "State code"),
    "county": ("TEXT", "County name"),
    "census_tract": ("TEXT", "Census tract"),
    "flood_zone": ("TEXT", "Flood zone designation"),
    "total_coverage": ("NUMERIC(18,2)", "Total coverage amount"),
    "total_premium": ("NUMERIC(18,2)", "Total premium amount"),
    "policy_effective_date": ("DATE", "Policy effective date"),
    "crs_class_code": ("TEXT", "Community Rating System class"),
}

# NFIP Claims schema
NFIP_CLAIMS_COLUMNS = {
    "year_of_loss": ("INTEGER", "Year loss occurred"),
    "state": ("TEXT", "State code"),
    "county": ("TEXT", "County name"),
    "census_tract": ("TEXT", "Census tract"),
    "flood_zone": ("TEXT", "Flood zone designation"),
    "amount_paid_on_building_claim": ("NUMERIC(18,2)", "Building claim paid"),
    "amount_paid_on_contents_claim": ("NUMERIC(18,2)", "Contents claim paid"),
    "total_amount_paid": ("NUMERIC(18,2)", "Total amount paid"),
    "date_of_loss": ("DATE", "Date of loss"),
    "reported_zip_code": ("TEXT", "ZIP code"),
}


def generate_table_name(dataset: str) -> str:
    """
    Generate PostgreSQL table name for FEMA data.
    
    Args:
        dataset: Dataset identifier
        
    Returns:
        PostgreSQL table name
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    return f"fema_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for FEMA data.
    
    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema
        
    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "disaster_declarations":
        columns = DISASTER_DECLARATIONS_COLUMNS
        # Use simple columns for constraint - county can be NULL
        unique_index_expr = "disaster_number, state, COALESCE(designated_area, '')"
        indexes = [
            ("disaster_number", "disaster_number"),
            ("state", "state"),
            ("declaration_date", "declaration_date"),
            ("fy_declared", "fy_declared"),
            ("incident_type", "incident_type"),
        ]
    elif dataset == "pa_projects":
        columns = PA_PROJECTS_COLUMNS
        unique_index_expr = "disaster_number, COALESCE(project_number, ''), state"
        indexes = [
            ("disaster_number", "disaster_number"),
            ("state", "state"),
            ("obligation_date", "obligation_date"),
        ]
    elif dataset == "hma_projects":
        columns = HMA_PROJECTS_COLUMNS
        unique_index_expr = "project_identifier"
        indexes = [
            ("disaster_number", "disaster_number"),
            ("state", "state"),
            ("program_area", "program_area"),
        ]
    elif dataset == "nfip_policies":
        columns = NFIP_POLICIES_COLUMNS
        unique_index_expr = "state, COALESCE(county, ''), COALESCE(census_tract, ''), COALESCE(policy_effective_date, '1900-01-01')"
        indexes = [
            ("state", "state"),
            ("flood_zone", "flood_zone"),
        ]
    elif dataset == "nfip_claims":
        columns = NFIP_CLAIMS_COLUMNS
        unique_index_expr = "year_of_loss, state, COALESCE(county, ''), COALESCE(date_of_loss, '1900-01-01')"
        indexes = [
            ("year_of_loss", "year_of_loss"),
            ("state", "state"),
            ("flood_zone", "flood_zone"),
        ]
    else:
        raise ValueError(f"Unknown FEMA dataset: {dataset}")
    
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
    
    # Add unique index (supports expressions, unlike UNIQUE constraint)
    index_sql_parts.append(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique "
        f"ON {table_name} ({unique_index_expr});"
    )
    
    indexes_sql = "\n    ".join(index_sql_parts)
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    
    {indexes_sql}
    """
    
    return sql


def parse_disaster_declarations(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse disaster declaration records from OpenFEMA API.
    
    Args:
        records: Raw API response records
        
    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []
    
    for record in records:
        try:
            parsed = {
                "disaster_number": record.get("disasterNumber"),
                "declaration_type": record.get("declarationType"),
                "declaration_date": _parse_date(record.get("declarationDate")),
                "fy_declared": record.get("fyDeclared"),
                "incident_type": record.get("incidentType"),
                "declaration_title": record.get("declarationTitle"),
                "state": record.get("state"),
                "state_name": record.get("stateName") or _get_state_name(record.get("state")),
                "county": record.get("designatedArea"),
                "fips_state_code": record.get("fipsStateCode"),
                "fips_county_code": record.get("fipsCountyCode"),
                "designated_area": record.get("designatedArea"),
                "incident_begin_date": _parse_date(record.get("incidentBeginDate")),
                "incident_end_date": _parse_date(record.get("incidentEndDate")),
                "ih_program_declared": record.get("ihProgramDeclared"),
                "ia_program_declared": record.get("iaProgramDeclared"),
                "pa_program_declared": record.get("paProgramDeclared"),
                "hm_program_declared": record.get("hmProgramDeclared"),
                "place_code": record.get("placeCode"),
                "region": record.get("region"),
            }
            
            if parsed["disaster_number"]:
                parsed_records.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse disaster record: {e}")
    
    logger.info(f"Parsed {len(parsed_records)} disaster declaration records")
    return parsed_records


def parse_pa_projects(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse Public Assistance project records."""
    parsed_records = []
    
    for record in records:
        try:
            parsed = {
                "disaster_number": record.get("disasterNumber"),
                "project_number": record.get("projectNumber") or record.get("pwNumber"),
                "state": record.get("state"),
                "county": record.get("county"),
                "applicant_id": record.get("applicantId"),
                "damage_category": record.get("damageCategory"),
                "project_size": record.get("projectSize"),
                "project_title": record.get("projectTitle") or record.get("damageDescription"),
                "total_obligated": _safe_float(record.get("totalObligated")),
                "federal_share_obligated": _safe_float(record.get("federalShareObligated")),
                "project_amount": _safe_float(record.get("projectAmount")),
                "obligation_date": _parse_date(record.get("obligatedDate")),
            }
            
            if parsed["disaster_number"] and parsed["project_number"]:
                parsed_records.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse PA project record: {e}")
    
    logger.info(f"Parsed {len(parsed_records)} PA project records")
    return parsed_records


def parse_hma_projects(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse Hazard Mitigation Assistance project records (v4 API)."""
    parsed_records = []
    
    for record in records:
        try:
            parsed = {
                "project_identifier": record.get("projectIdentifier") or record.get("id"),
                "disaster_number": record.get("disasterNumber"),
                "state": record.get("state"),
                "state_code": record.get("stateNumberCode"),
                "county": record.get("county"),
                "county_code": record.get("countyCode"),
                "region": record.get("region"),
                "program_area": record.get("programArea"),
                "program_fy": record.get("programFy"),
                "project_type": record.get("projectType"),
                "status": record.get("status"),
                "recipient": record.get("recipient"),
                "subrecipient": record.get("subrecipient"),
                "project_amount": _safe_float(record.get("projectAmount")),
                "federal_share_obligated": _safe_float(record.get("federalShareObligated")),
                "cost_share_percentage": _safe_float(record.get("costSharePercentage")),
                "benefit_cost_ratio": _safe_float(record.get("benefitCostRatio")),
                "date_approved": _parse_date(record.get("dateApproved")),
                "date_closed": _parse_date(record.get("dateClosed")),
            }
            
            if parsed["project_identifier"]:
                parsed_records.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse HMA project record: {e}")
    
    logger.info(f"Parsed {len(parsed_records)} HMA project records")
    return parsed_records


def parse_nfip_claims(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse NFIP claims records."""
    parsed_records = []
    
    for record in records:
        try:
            parsed = {
                "year_of_loss": record.get("yearOfLoss"),
                "state": record.get("state"),
                "county": record.get("countyCode") or record.get("county"),
                "census_tract": record.get("censusTract"),
                "flood_zone": record.get("floodZone"),
                "amount_paid_on_building_claim": _safe_float(record.get("amountPaidOnBuildingClaim")),
                "amount_paid_on_contents_claim": _safe_float(record.get("amountPaidOnContentsClaim")),
                "total_amount_paid": _safe_float(record.get("totalBuildingInsuranceCoverage")),
                "date_of_loss": _parse_date(record.get("dateOfLoss")),
                "reported_zip_code": record.get("reportedZipCode"),
            }
            
            if parsed["year_of_loss"] and parsed["state"]:
                parsed_records.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse NFIP claim record: {e}")
    
    logger.info(f"Parsed {len(parsed_records)} NFIP claim records")
    return parsed_records


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse date string to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        # Handle ISO format
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        # Validate it's a valid date
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _get_state_name(state_code: Optional[str]) -> Optional[str]:
    """Get full state name from code."""
    state_names = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
        "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
        "AS": "American Samoa", "MP": "Northern Mariana Islands"
    }
    return state_names.get(state_code)


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "disaster_declarations": "FEMA Disaster Declarations",
        "pa_projects": "FEMA Public Assistance Projects",
        "hma_projects": "FEMA Hazard Mitigation Projects",
        "nfip_policies": "NFIP Flood Insurance Policies",
        "nfip_claims": "NFIP Flood Insurance Claims",
    }
    return display_names.get(dataset, f"FEMA {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "disaster_declarations": (
            "All federally declared disasters since 1953, including major disasters, "
            "emergencies, and fire management assistance declarations. "
            "Includes program eligibility (IA, PA, HM) and geographic designations."
        ),
        "pa_projects": (
            "Public Assistance funded project details including project amounts, "
            "federal share, damage categories, and obligation dates."
        ),
        "hma_projects": (
            "Hazard Mitigation Assistance projects including HMGP, PDM, FMA, "
            "and other mitigation programs. Tracks project status and funding."
        ),
        "nfip_policies": (
            "National Flood Insurance Program policy data including coverage amounts, "
            "premiums, and flood zone designations."
        ),
        "nfip_claims": (
            "NFIP flood insurance claims data including loss amounts, "
            "claim payments, and geographic information."
        ),
    }
    return descriptions.get(dataset, "FEMA emergency management data")
