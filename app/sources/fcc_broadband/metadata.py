"""
FCC Broadband metadata and schema generation utilities.

Handles:
- Table name generation for FCC broadband datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for coverage, provider, and summary data
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Broadband coverage by geography
BROADBAND_COVERAGE_COLUMNS = {
    "geography_type": ("TEXT NOT NULL", "Geography level: state, county, census_block"),
    "geography_id": (
        "TEXT NOT NULL",
        "Geography identifier (state code, FIPS, block ID)",
    ),
    "geography_name": ("TEXT", "Human-readable geography name"),
    "provider_id": ("TEXT NOT NULL", "FCC provider ID (FRN)"),
    "provider_name": ("TEXT NOT NULL", "Provider name"),
    "brand_name": ("TEXT", "Consumer-facing brand name"),
    "technology_code": (
        "TEXT NOT NULL",
        "FCC technology code (10=DSL, 40=Cable, 50=Fiber)",
    ),
    "technology_name": ("TEXT NOT NULL", "Technology type description"),
    "max_advertised_down_mbps": (
        "NUMERIC(10, 2)",
        "Max advertised download speed (Mbps)",
    ),
    "max_advertised_up_mbps": ("NUMERIC(10, 2)", "Max advertised upload speed (Mbps)"),
    "speed_tier": (
        "TEXT",
        "Speed classification: sub_broadband, basic, high_speed, gigabit",
    ),
    "business_service": ("BOOLEAN", "Offers business service"),
    "consumer_service": ("BOOLEAN", "Offers consumer service"),
    "data_date": ("DATE", "Date of data collection"),
}

# Broadband summary by geography (aggregated stats)
BROADBAND_SUMMARY_COLUMNS = {
    "geography_type": ("TEXT NOT NULL", "Geography level: state, county"),
    "geography_id": ("TEXT NOT NULL", "Geography identifier"),
    "geography_name": ("TEXT", "Human-readable name"),
    "total_providers": ("INTEGER", "Number of providers serving area"),
    "total_technologies": ("INTEGER", "Number of technology types available"),
    "fiber_available": ("BOOLEAN", "Fiber (FTTP) is available"),
    "cable_available": ("BOOLEAN", "Cable modem is available"),
    "dsl_available": ("BOOLEAN", "DSL is available"),
    "fixed_wireless_available": ("BOOLEAN", "Fixed wireless is available"),
    "satellite_available": ("BOOLEAN", "Satellite is available"),
    "mobile_5g_available": ("BOOLEAN", "5G mobile is available"),
    "max_speed_down_mbps": ("NUMERIC(10, 2)", "Highest available download speed"),
    "max_speed_up_mbps": ("NUMERIC(10, 2)", "Highest available upload speed"),
    "avg_speed_down_mbps": (
        "NUMERIC(10, 2)",
        "Average download speed across providers",
    ),
    "broadband_coverage_pct": ("NUMERIC(5, 2)", "% with 25/3 Mbps broadband"),
    "gigabit_coverage_pct": ("NUMERIC(5, 2)", "% with 1000+ Mbps available"),
    "provider_competition": (
        "TEXT",
        "Competition level: monopoly, duopoly, competitive",
    ),
    "data_date": ("DATE", "Date of data collection"),
}

# Provider detail
PROVIDER_COLUMNS = {
    "provider_id": ("TEXT NOT NULL", "FCC Filer Registration Number (FRN)"),
    "provider_name": ("TEXT NOT NULL", "Legal entity name"),
    "brand_name": ("TEXT", "Consumer brand name"),
    "holding_company": ("TEXT", "Parent company name"),
    "state_code": ("TEXT", "Primary state of operation"),
    "technology_types": ("TEXT[]", "Array of technology codes offered"),
    "states_served": ("TEXT[]", "Array of states where provider operates"),
    "residential_service": ("BOOLEAN", "Offers residential service"),
    "business_service": ("BOOLEAN", "Offers business service"),
    "max_down_speed": ("NUMERIC(10, 2)", "Max download speed offered (Mbps)"),
    "max_up_speed": ("NUMERIC(10, 2)", "Max upload speed offered (Mbps)"),
}


def generate_table_name(dataset: str) -> str:
    """
    Generate PostgreSQL table name for FCC broadband data.

    Args:
        dataset: Dataset identifier

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("broadband_coverage")
        'fcc_broadband_coverage'
        >>> generate_table_name("provider_summary")
        'fcc_provider_summary'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    return f"fcc_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for FCC broadband data.

    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema

    Returns:
        CREATE TABLE SQL statement (idempotent)
    """
    # Select appropriate schema
    if dataset == "broadband_coverage":
        columns = BROADBAND_COVERAGE_COLUMNS
        unique_expr = "geography_type, geography_id, provider_id, technology_code"
        indexes = [
            ("geography", "geography_type, geography_id"),
            ("provider", "provider_id"),
            ("technology", "technology_code"),
            ("speed_tier", "speed_tier"),
            ("data_date", "data_date"),
        ]
    elif dataset == "broadband_summary":
        columns = BROADBAND_SUMMARY_COLUMNS
        unique_expr = "geography_type, geography_id"
        indexes = [
            ("geography", "geography_type, geography_id"),
            ("coverage", "broadband_coverage_pct"),
            ("fiber", "fiber_available"),
            ("competition", "provider_competition"),
        ]
    elif dataset == "providers":
        columns = PROVIDER_COLUMNS
        unique_expr = "provider_id"
        indexes = [
            ("state", "state_code"),
            ("name", "provider_name"),
        ]
    else:
        raise ValueError(f"Unknown FCC dataset: {dataset}")

    # Build column definitions
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")

    for col_name, (col_type, _) in columns.items():
        column_defs.append(f'"{col_name}" {col_type}')

    column_defs.append("ingested_at TIMESTAMP DEFAULT NOW()")

    columns_sql = ",\n        ".join(column_defs)

    # Build indexes
    index_sql_parts = []
    for idx_name, idx_cols in indexes:
        index_sql_parts.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx_name} "
            f"ON {table_name} ({idx_cols});"
        )

    # Add unique index
    index_sql_parts.append(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique "
        f"ON {table_name} ({unique_expr});"
    )

    indexes_sql = "\n    ".join(index_sql_parts)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    
    {indexes_sql}
    """

    return sql


def classify_speed_tier(speed_mbps: Optional[float]) -> str:
    """
    Classify download speed into FCC-relevant tiers.

    Args:
        speed_mbps: Download speed in Mbps

    Returns:
        Speed tier classification

    FCC defines broadband as 25/3 Mbps (as of 2024).
    Proposed update to 100/20 Mbps.
    """
    if speed_mbps is None:
        return "unknown"
    elif speed_mbps < 25:
        return "sub_broadband"  # Below FCC broadband definition
    elif speed_mbps < 100:
        return "basic_broadband"  # 25-100 Mbps (meets current definition)
    elif speed_mbps < 1000:
        return "high_speed"  # 100-1000 Mbps
    else:
        return "gigabit"  # 1000+ Mbps


def classify_competition(provider_count: int) -> str:
    """
    Classify market competition level.

    Args:
        provider_count: Number of providers in area

    Returns:
        Competition classification
    """
    if provider_count <= 1:
        return "monopoly"
    elif provider_count == 2:
        return "duopoly"
    elif provider_count <= 4:
        return "limited"
    else:
        return "competitive"


def get_technology_name(code: str) -> str:
    """
    Get human-readable technology name from FCC code.

    Args:
        code: FCC technology code

    Returns:
        Technology name
    """
    tech_names = {
        "10": "Asymmetric xDSL",
        "11": "ADSL2, ADSL2+",
        "12": "VDSL",
        "20": "Symmetric xDSL",
        "30": "Other Copper Wireline",
        "40": "Cable Modem - DOCSIS 3.0",
        "41": "Cable Modem - DOCSIS 3.1",
        "42": "Cable Modem - Other",
        "50": "Fiber to the Premises (FTTP)",
        "60": "Satellite",
        "70": "Terrestrial Fixed Wireless",
        "71": "Licensed Fixed Wireless",
        "72": "Unlicensed Fixed Wireless",
        "80": "Electric Power Line",
        "90": "All Other",
    }
    return tech_names.get(str(code), f"Unknown ({code})")


def is_fiber(technology_code: str) -> bool:
    """Check if technology code is fiber (FTTP)."""
    return str(technology_code) == "50"


def is_cable(technology_code: str) -> bool:
    """Check if technology code is cable modem."""
    return str(technology_code) in ("40", "41", "42")


def is_dsl(technology_code: str) -> bool:
    """Check if technology code is DSL."""
    return str(technology_code) in ("10", "11", "12", "20")


def is_fixed_wireless(technology_code: str) -> bool:
    """Check if technology code is fixed wireless."""
    return str(technology_code) in ("70", "71", "72")


def is_satellite(technology_code: str) -> bool:
    """Check if technology code is satellite."""
    return str(technology_code) == "60"


def parse_broadband_coverage_response(
    records: List[Dict[str, Any]],
    geography_type: str,
    geography_id: str,
    geography_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Parse broadband coverage data from FCC API response.

    FCC Open Data field names (from 4kuc-phrr dataset):
    - provider_id: Provider ID
    - frn: FCC Registration Number
    - providername: Legal provider name
    - dbaname: DBA (doing business as) name
    - holdingcompanyname: Holding company
    - stateabbr: State abbreviation
    - blockcode: Census block code
    - techcode: Technology code
    - consumer: Consumer service flag (1/0)
    - business: Business service flag (1/0)
    - maxaddown: Max advertised download (Mbps)
    - maxadup: Max advertised upload (Mbps)

    Args:
        records: Raw API response records
        geography_type: Type of geography (state, county, etc.)
        geography_id: Geography identifier
        geography_name: Optional human-readable name

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            # FCC Open Data uses specific field names
            provider_id = (
                record.get("frn")
                or record.get("provider_id")
                or record.get("providerId")
            )

            provider_name = (
                record.get("providername")
                or record.get("dbaname")
                or record.get("provider_name")
                or record.get("holdingcompanyname")
            )

            brand_name = (
                record.get("dbaname")
                or record.get("brand_name")
                or record.get("brandName")
            )

            technology_code = str(
                record.get("techcode")
                or record.get("technology_code")
                or record.get("technology")
                or "90"  # Default to "Other"
            )

            max_down = _safe_float(
                record.get("maxaddown")
                or record.get("max_advertised_down_mbps")
                or record.get("max_down")
            )

            max_up = _safe_float(
                record.get("maxadup")
                or record.get("max_advertised_up_mbps")
                or record.get("max_up")
            )

            # Handle 1/0 flags from FCC data
            business_flag = record.get("business")
            consumer_flag = record.get("consumer")

            parsed = {
                "geography_type": geography_type,
                "geography_id": geography_id,
                "geography_name": geography_name,
                "provider_id": provider_id,
                "provider_name": provider_name,
                "brand_name": brand_name,
                "technology_code": technology_code,
                "technology_name": get_technology_name(technology_code),
                "max_advertised_down_mbps": max_down,
                "max_advertised_up_mbps": max_up,
                "speed_tier": classify_speed_tier(max_down),
                "business_service": _safe_bool(business_flag),
                "consumer_service": _safe_bool(consumer_flag),
                "data_date": _parse_date(
                    record.get("data_date") or record.get("as_of_date")
                ),
            }

            # Skip records without required fields
            if parsed["provider_id"] and parsed["provider_name"]:
                parsed_records.append(parsed)
            else:
                logger.debug(f"Skipping incomplete record: {record}")

        except Exception as e:
            logger.warning(f"Failed to parse coverage record: {e}")

    logger.info(f"Parsed {len(parsed_records)} coverage records")
    return parsed_records


def parse_broadband_summary(
    records: List[Dict[str, Any]],
    geography_type: str,
    geography_id: str,
    geography_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate summary statistics from coverage records.

    Args:
        records: Parsed coverage records for a geography
        geography_type: Type of geography
        geography_id: Geography identifier
        geography_name: Human-readable name

    Returns:
        Summary dict ready for insertion
    """
    if not records:
        return None

    # Count unique providers and technologies
    providers = set()
    technologies = set()
    speeds_down = []
    speeds_up = []

    fiber_available = False
    cable_available = False
    dsl_available = False
    fixed_wireless_available = False
    satellite_available = False

    for r in records:
        if r.get("provider_id"):
            providers.add(r["provider_id"])
        if r.get("technology_code"):
            technologies.add(r["technology_code"])
            tech = str(r["technology_code"])
            if is_fiber(tech):
                fiber_available = True
            if is_cable(tech):
                cable_available = True
            if is_dsl(tech):
                dsl_available = True
            if is_fixed_wireless(tech):
                fixed_wireless_available = True
            if is_satellite(tech):
                satellite_available = True

        if r.get("max_advertised_down_mbps"):
            speeds_down.append(r["max_advertised_down_mbps"])
        if r.get("max_advertised_up_mbps"):
            speeds_up.append(r["max_advertised_up_mbps"])

    total_providers = len(providers)
    max_down = max(speeds_down) if speeds_down else None
    max_up = max(speeds_up) if speeds_up else None
    avg_down = sum(speeds_down) / len(speeds_down) if speeds_down else None

    # Estimate coverage percentages (simplified)
    broadband_pct = None
    gigabit_pct = None
    if speeds_down:
        broadband_count = sum(1 for s in speeds_down if s >= 25)
        gigabit_count = sum(1 for s in speeds_down if s >= 1000)
        broadband_pct = (broadband_count / len(speeds_down)) * 100
        gigabit_pct = (gigabit_count / len(speeds_down)) * 100

    summary = {
        "geography_type": geography_type,
        "geography_id": geography_id,
        "geography_name": geography_name,
        "total_providers": total_providers,
        "total_technologies": len(technologies),
        "fiber_available": fiber_available,
        "cable_available": cable_available,
        "dsl_available": dsl_available,
        "fixed_wireless_available": fixed_wireless_available,
        "satellite_available": satellite_available,
        "mobile_5g_available": False,  # Would need separate mobile data
        "max_speed_down_mbps": max_down,
        "max_speed_up_mbps": max_up,
        "avg_speed_down_mbps": round(avg_down, 2) if avg_down else None,
        "broadband_coverage_pct": round(broadband_pct, 2) if broadband_pct else None,
        "gigabit_coverage_pct": round(gigabit_pct, 2) if gigabit_pct else None,
        "provider_competition": classify_competition(total_providers),
        "data_date": datetime.now().strftime("%Y-%m-%d"),
    }

    return summary


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse date string to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        if "T" in str(date_str):
            date_str = str(date_str).split("T")[0]
        datetime.strptime(str(date_str), "%Y-%m-%d")
        return str(date_str)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "" or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    """Safely convert value to boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "t", "y")
    return bool(value)


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "broadband_coverage": "FCC Broadband Coverage by Provider",
        "broadband_summary": "FCC Broadband Summary Statistics",
        "providers": "FCC Broadband Providers",
    }
    return display_names.get(dataset, f"FCC {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "broadband_coverage": (
            "Detailed broadband availability by geography showing provider, "
            "technology type (fiber, cable, DSL, etc.), and advertised speeds. "
            "Source: FCC National Broadband Map."
        ),
        "broadband_summary": (
            "Aggregated broadband statistics by geography including provider count, "
            "technology availability, max speeds, and coverage percentages. "
            "Useful for digital divide analysis."
        ),
        "providers": (
            "Master list of broadband service providers with their technology "
            "offerings and service areas. Source: FCC Form 477 data."
        ),
    }
    return descriptions.get(dataset, "FCC broadband infrastructure data")


# ========== Reference Data ==========

# State name to FIPS mapping
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "PR": "72",
    "VI": "78",
}

# State name lookup
STATE_NAMES = {
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
    "72": "Puerto Rico",
    "78": "Virgin Islands",
}

# All 50 states + DC
US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
]
