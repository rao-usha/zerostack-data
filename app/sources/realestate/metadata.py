"""
Real Estate / Housing metadata and schema generation.

Defines table schemas for each real estate data source.
"""
import re
from typing import List, Dict, Any
from datetime import datetime, timedelta


def generate_table_name(source: str) -> str:
    """
    Generate PostgreSQL table name for a real estate data source.
    
    Args:
        source: Source identifier (fhfa_hpi, hud_permits, redfin, osm_buildings)
        
    Returns:
        PostgreSQL-safe table name
        
    Examples:
        generate_table_name("fhfa_hpi") -> "realestate_fhfa_hpi"
        generate_table_name("hud_permits") -> "realestate_hud_permits"
    """
    # Sanitize source name
    safe_source = re.sub(r'[^a-z0-9_]', '_', source.lower())
    return f"realestate_{safe_source}"


def generate_create_table_sql(source: str) -> str:
    """
    Generate CREATE TABLE SQL for a real estate data source.
    
    Tables are created with IF NOT EXISTS (idempotent).
    All tables use typed columns (not JSON blobs).
    
    Args:
        source: Source identifier
        
    Returns:
        CREATE TABLE SQL statement
        
    Raises:
        ValueError: If source is not recognized
    """
    table_name = generate_table_name(source)
    
    if source == "fhfa_hpi":
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            geography_type TEXT NOT NULL,  -- 'National', 'State', 'MSA', 'ZIP3'
            geography_id TEXT,  -- State code, MSA code, or ZIP3 prefix
            geography_name TEXT,
            index_nsa NUMERIC,  -- Not seasonally adjusted
            index_sa NUMERIC,  -- Seasonally adjusted
            yoy_pct_change NUMERIC,  -- Year-over-year percent change
            qoq_pct_change NUMERIC,  -- Quarter-over-quarter percent change
            ingested_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (date, geography_type, geography_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geography ON {table_name}(geography_type, geography_id);
        """
    
    elif source == "hud_permits":
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            geography_type TEXT NOT NULL,  -- 'National', 'State', 'MSA', 'County'
            geography_id TEXT,  -- FIPS code or MSA code
            geography_name TEXT,
            permits_total INTEGER,  -- Total building permits
            permits_1unit INTEGER,  -- Single-family permits
            permits_2to4units INTEGER,  -- 2-4 unit permits
            permits_5plus INTEGER,  -- 5+ unit permits
            starts_total INTEGER,  -- Total housing starts
            starts_1unit INTEGER,
            starts_2to4units INTEGER,
            starts_5plus INTEGER,
            completions_total INTEGER,  -- Total completions
            completions_1unit INTEGER,
            completions_2to4units INTEGER,
            completions_5plus INTEGER,
            ingested_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (date, geography_type, geography_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geography ON {table_name}(geography_type, geography_id);
        """
    
    elif source == "redfin":
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            period_end DATE NOT NULL,
            region_type TEXT NOT NULL,  -- 'zip', 'city', 'neighborhood', 'metro'
            region_type_id INTEGER,
            region TEXT,
            state_code TEXT,
            property_type TEXT,  -- 'All Residential', 'Single Family', 'Condo/Co-op', etc.
            median_sale_price NUMERIC,
            median_list_price NUMERIC,
            median_ppsf NUMERIC,  -- Price per square foot
            homes_sold INTEGER,
            pending_sales INTEGER,
            new_listings INTEGER,
            inventory INTEGER,
            months_of_supply NUMERIC,
            median_dom INTEGER,  -- Days on market
            avg_sale_to_list NUMERIC,  -- Sale-to-list price ratio
            sold_above_list INTEGER,
            price_drops INTEGER,
            off_market_in_two_weeks INTEGER,
            ingested_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (period_end, region_type, region_type_id, property_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name}(period_end);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_region ON {table_name}(region_type, region);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(state_code);
        """
    
    elif source == "osm_buildings":
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            osm_id BIGINT NOT NULL,  -- OpenStreetMap feature ID
            osm_type TEXT NOT NULL,  -- 'way' or 'relation'
            latitude NUMERIC(10, 7) NOT NULL,
            longitude NUMERIC(10, 7) NOT NULL,
            building_type TEXT,  -- 'residential', 'commercial', 'industrial', etc.
            levels INTEGER,  -- Number of floors
            height NUMERIC,  -- Height in meters
            area_sqm NUMERIC,  -- Building footprint area
            address TEXT,
            city TEXT,
            state TEXT,
            postcode TEXT,
            country TEXT,
            name TEXT,  -- Building name if available
            tags JSONB,  -- Additional OSM tags
            geometry_geojson JSONB,  -- GeoJSON representation
            ingested_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (osm_id, osm_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_location ON {table_name}(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_building_type ON {table_name}(building_type);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_city ON {table_name}(city);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_postcode ON {table_name}(postcode);
        """
    
    else:
        raise ValueError(f"Unknown real estate source: {source}")


def get_source_display_name(source: str) -> str:
    """Get human-readable display name for a real estate source."""
    display_names = {
        "fhfa_hpi": "FHFA House Price Index",
        "hud_permits": "HUD Building Permits and Housing Starts",
        "redfin": "Redfin Housing Market Data",
        "osm_buildings": "OpenStreetMap Building Footprints"
    }
    return display_names.get(source, source)


def get_source_description(source: str) -> str:
    """Get description for a real estate source."""
    descriptions = {
        "fhfa_hpi": (
            "Federal Housing Finance Agency House Price Index. Tracks changes in "
            "single-family home values across the United States. Available at national, "
            "state, MSA, and ZIP code levels."
        ),
        "hud_permits": (
            "U.S. Department of Housing and Urban Development data on building permits, "
            "housing starts, and completions. Broken down by unit type (single-family, "
            "2-4 units, 5+ units) and geography."
        ),
        "redfin": (
            "Redfin public housing market data. Includes median sale prices, inventory, "
            "days on market, and other key housing metrics at various geographic levels."
        ),
        "osm_buildings": (
            "OpenStreetMap building footprints and metadata. Provides geographic data "
            "on building locations, types, sizes, and characteristics from the "
            "OpenStreetMap project."
        )
    }
    return descriptions.get(source, f"Real estate data from {source}")


def get_default_date_range(source: str) -> tuple[str, str]:
    """
    Get default date range for a real estate source.
    
    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    # Default to last 5 years for time series data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 5)
    
    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )


def validate_date_format(date_str: str) -> bool:
    """Validate date string is in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# FHFA-specific metadata
FHFA_GEOGRAPHY_TYPES = ["National", "State", "MSA", "ZIP3"]

# HUD-specific metadata
HUD_GEOGRAPHY_TYPES = ["National", "State", "MSA", "County"]

# Redfin-specific metadata
REDFIN_REGION_TYPES = ["zip", "city", "neighborhood", "metro"]
REDFIN_PROPERTY_TYPES = [
    "All Residential",
    "Single Family Residential",
    "Condo/Co-op",
    "Townhouse",
    "Multi-Family (2-4 Unit)"
]

# OSM-specific metadata
OSM_BUILDING_TYPES = [
    "residential",
    "commercial",
    "industrial",
    "retail",
    "office",
    "apartments",
    "house",
    "warehouse",
    "school",
    "hospital"
]


def parse_fhfa_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse FHFA API response into database rows.
    
    Args:
        raw_data: List of records from FHFA API
        
    Returns:
        List of dicts ready for database insertion
    """
    parsed = []
    
    for record in raw_data:
        parsed.append({
            "date": record.get("date"),
            "geography_type": record.get("geography_type"),
            "geography_id": record.get("geography_id"),
            "geography_name": record.get("geography_name"),
            "index_nsa": _safe_float(record.get("index_nsa")),
            "index_sa": _safe_float(record.get("index_sa")),
            "yoy_pct_change": _safe_float(record.get("yoy_pct_change")),
            "qoq_pct_change": _safe_float(record.get("qoq_pct_change"))
        })
    
    return parsed


def parse_hud_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse HUD API response into database rows."""
    parsed = []
    
    for record in raw_data:
        parsed.append({
            "date": record.get("date"),
            "geography_type": record.get("geography_type"),
            "geography_id": record.get("geography_id"),
            "geography_name": record.get("geography_name"),
            "permits_total": _safe_int(record.get("permits_total")),
            "permits_1unit": _safe_int(record.get("permits_1unit")),
            "permits_2to4units": _safe_int(record.get("permits_2to4units")),
            "permits_5plus": _safe_int(record.get("permits_5plus")),
            "starts_total": _safe_int(record.get("starts_total")),
            "starts_1unit": _safe_int(record.get("starts_1unit")),
            "starts_2to4units": _safe_int(record.get("starts_2to4units")),
            "starts_5plus": _safe_int(record.get("starts_5plus")),
            "completions_total": _safe_int(record.get("completions_total")),
            "completions_1unit": _safe_int(record.get("completions_1unit")),
            "completions_2to4units": _safe_int(record.get("completions_2to4units")),
            "completions_5plus": _safe_int(record.get("completions_5plus"))
        })
    
    return parsed


def parse_redfin_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse Redfin CSV data into database rows."""
    parsed = []
    
    for record in raw_data:
        parsed.append({
            "period_end": record.get("period_end"),
            "region_type": record.get("region_type"),
            "region_type_id": _safe_int(record.get("region_type_id")),
            "region": record.get("region"),
            "state_code": record.get("state_code"),
            "property_type": record.get("property_type"),
            "median_sale_price": _safe_float(record.get("median_sale_price")),
            "median_list_price": _safe_float(record.get("median_list_price")),
            "median_ppsf": _safe_float(record.get("median_ppsf")),
            "homes_sold": _safe_int(record.get("homes_sold")),
            "pending_sales": _safe_int(record.get("pending_sales")),
            "new_listings": _safe_int(record.get("new_listings")),
            "inventory": _safe_int(record.get("inventory")),
            "months_of_supply": _safe_float(record.get("months_of_supply")),
            "median_dom": _safe_int(record.get("median_dom")),
            "avg_sale_to_list": _safe_float(record.get("avg_sale_to_list")),
            "sold_above_list": _safe_int(record.get("sold_above_list")),
            "price_drops": _safe_int(record.get("price_drops")),
            "off_market_in_two_weeks": _safe_int(record.get("off_market_in_two_weeks"))
        })
    
    return parsed


def parse_osm_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse OpenStreetMap Overpass API response into database rows."""
    parsed = []
    
    for element in raw_data:
        # Extract coordinates (centroid for ways/relations)
        lat = element.get("lat") or element.get("center", {}).get("lat")
        lon = element.get("lon") or element.get("center", {}).get("lon")
        
        if not lat or not lon:
            continue
        
        tags = element.get("tags", {})
        
        parsed.append({
            "osm_id": element.get("id"),
            "osm_type": element.get("type"),
            "latitude": lat,
            "longitude": lon,
            "building_type": tags.get("building"),
            "levels": _safe_int(tags.get("building:levels")),
            "height": _safe_float(tags.get("height")),
            "area_sqm": _calculate_area(element),  # Would need implementation
            "address": _format_address(tags),
            "city": tags.get("addr:city"),
            "state": tags.get("addr:state"),
            "postcode": tags.get("addr:postcode"),
            "country": tags.get("addr:country"),
            "name": tags.get("name"),
            "tags": tags,
            "geometry_geojson": element.get("geometry")
        })
    
    return parsed


def _safe_float(value: Any) -> float | None:
    """Safely convert value to float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> int | None:
    """Safely convert value to int."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))  # Handle "1.0" -> 1
    except (ValueError, TypeError):
        return None


def _format_address(tags: Dict[str, Any]) -> str | None:
    """Format OSM address tags into single address string."""
    parts = []
    
    if tags.get("addr:housenumber"):
        parts.append(tags["addr:housenumber"])
    if tags.get("addr:street"):
        parts.append(tags["addr:street"])
    
    return " ".join(parts) if parts else None


def _calculate_area(element: Dict[str, Any]) -> float | None:
    """Calculate building footprint area from OSM geometry."""
    # Simplified placeholder - would need proper geometry calculation
    # Using tags if available
    tags = element.get("tags", {})
    area = tags.get("area")
    if area:
        return _safe_float(area)
    return None

