"""
DUNL metadata, table schemas, and JSON-LD parsing utilities.

Handles:
- CREATE TABLE SQL generation for all DUNL tables
- JSON-LD @graph parsing (localized strings, typed literals, URI refs)
- Record transformation from JSON-LD to flat DB rows
"""

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


# ========== JSON-LD Parsing Helpers ==========


def extract_value(val: Any) -> Any:
    """
    Extract a plain Python value from a JSON-LD value object.

    Handles:
    - Localized strings: {"@language": "en", "@value": "Russian Ruble"} -> "Russian Ruble"
    - Typed literals: {"@type": "xsd:float", "@value": "0.000051"} -> 0.000051
    - Booleans: {"@type": "xsd:boolean", "@value": "true"} -> True
    - Plain strings: "RUB" -> "RUB"
    - Lists: picks first English or first element
    - URI refs: {"@id": "http://..."} -> "http://..."
    - None -> None
    """
    if val is None:
        return None

    if isinstance(val, (str, int, float, bool)):
        return val

    if isinstance(val, list):
        if not val:
            return None
        # Prefer English for localized strings
        for item in val:
            if isinstance(item, dict) and item.get("@language") == "en":
                return extract_value(item)
        return extract_value(val[0])

    if isinstance(val, dict):
        # URI reference
        if "@id" in val and "@value" not in val:
            return val["@id"]

        raw = val.get("@value")
        if raw is None:
            return None

        vtype = val.get("@type", "")

        if "boolean" in vtype.lower():
            return str(raw).lower() in ("true", "1", "yes")

        if any(t in vtype.lower() for t in ("float", "double", "decimal", "integer", "int")):
            try:
                return float(raw)
            except (ValueError, TypeError):
                return raw

        return raw

    return val


def extract_code_from_uri(uri: str) -> str:
    """Extract the trailing code/identifier from a DUNL URI."""
    if not uri:
        return ""
    return uri.rstrip("/").rsplit("/", 1)[-1]


def parse_graph(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the @graph array from a JSON-LD response."""
    if isinstance(data, dict):
        graph = data.get("@graph", [])
        if graph:
            return graph
        # Single object (no @graph wrapper)
        if "@id" in data:
            return [data]
    return []


# ========== Table Schemas ==========


def generate_create_currencies_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS dunl_currencies (
        id SERIAL PRIMARY KEY,
        currency_code TEXT UNIQUE,
        currency_name TEXT,
        dunl_uri TEXT,
        country_region_ref TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_dunl_currencies_code ON dunl_currencies (currency_code);
    """


def generate_create_ports_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS dunl_ports (
        id SERIAL PRIMARY KEY,
        symbol TEXT UNIQUE,
        port_name TEXT,
        location TEXT,
        dunl_uri TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_dunl_ports_symbol ON dunl_ports (symbol);
    CREATE INDEX IF NOT EXISTS idx_dunl_ports_location ON dunl_ports (location);
    """


def generate_create_uom_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS dunl_uom (
        id SERIAL PRIMARY KEY,
        uom_code TEXT UNIQUE,
        uom_name TEXT,
        description TEXT,
        uom_type TEXT,
        dunl_uri TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_dunl_uom_code ON dunl_uom (uom_code);
    CREATE INDEX IF NOT EXISTS idx_dunl_uom_type ON dunl_uom (uom_type);
    """


def generate_create_uom_conversions_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS dunl_uom_conversions (
        id SERIAL PRIMARY KEY,
        conversion_code TEXT UNIQUE,
        from_uom TEXT,
        to_uom TEXT,
        factor NUMERIC(20,10),
        description TEXT,
        dunl_uri TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_dunl_uom_conv_code ON dunl_uom_conversions (conversion_code);
    CREATE INDEX IF NOT EXISTS idx_dunl_uom_conv_from ON dunl_uom_conversions (from_uom);
    CREATE INDEX IF NOT EXISTS idx_dunl_uom_conv_to ON dunl_uom_conversions (to_uom);
    """


def generate_create_calendars_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS dunl_calendars (
        id SERIAL PRIMARY KEY,
        year INTEGER,
        commodity TEXT,
        event_date TEXT,
        publication_affected BOOLEAN,
        publication_comments TEXT,
        service_affected BOOLEAN,
        service_comments TEXT,
        dunl_uri TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        CONSTRAINT dunl_calendars_unique UNIQUE (year, commodity, event_date)
    );
    CREATE INDEX IF NOT EXISTS idx_dunl_calendars_year ON dunl_calendars (year);
    CREATE INDEX IF NOT EXISTS idx_dunl_calendars_commodity ON dunl_calendars (commodity);
    """


# ========== Record Parsers ==========


def parse_currencies(graph: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse currency entries from JSON-LD @graph."""
    records = []
    for item in graph:
        uri = item.get("@id", "")
        if not uri or "/currency/" not in uri:
            continue

        code = extract_value(item.get("dunl:hasCurrencyCode")) or extract_code_from_uri(uri)
        name = extract_value(item.get("dunl:hasCurrencyLabel"))
        country_ref = None
        country_val = item.get("dunl:hasCountryRegion")
        if country_val:
            country_ref = extract_value(country_val)

        if code:
            records.append({
                "currency_code": str(code),
                "currency_name": str(name) if name else None,
                "dunl_uri": uri,
                "country_region_ref": str(country_ref) if country_ref else None,
            })

    logger.info(f"Parsed {len(records)} currency records from JSON-LD")
    return records


def parse_ports(graph: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse port entries from JSON-LD @graph."""
    records = []
    for item in graph:
        uri = item.get("@id", "")
        if not uri or "/port/" not in uri:
            continue

        symbol = extract_value(item.get("dunl:hasSymbol")) or extract_code_from_uri(uri)
        name = extract_value(item.get("dunl:hasName"))
        location = extract_value(item.get("dunl:hasLocation"))

        if symbol:
            records.append({
                "symbol": str(symbol),
                "port_name": str(name) if name else None,
                "location": str(location) if location else None,
                "dunl_uri": uri,
            })

    logger.info(f"Parsed {len(records)} port records from JSON-LD")
    return records


def parse_uom(graph: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse unit-of-measure entries from JSON-LD @graph."""
    records = []
    for item in graph:
        uri = item.get("@id", "")
        if not uri or "/uom/" not in uri:
            continue
        # Skip conversion entries that might be in the graph
        if "uom-conversion" in uri:
            continue

        code = extract_code_from_uri(uri)
        name = extract_value(item.get("dunl:hasName"))
        description = extract_value(item.get("dunl:hasDescription"))
        uom_type = extract_value(item.get("dunl:hasUOMType"))

        if code:
            records.append({
                "uom_code": str(code),
                "uom_name": str(name) if name else None,
                "description": str(description) if description else None,
                "uom_type": str(uom_type) if uom_type else None,
                "dunl_uri": uri,
            })

    logger.info(f"Parsed {len(records)} UOM records from JSON-LD")
    return records


def parse_uom_conversions(graph: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse UOM conversion entries from JSON-LD @graph."""
    records = []
    for item in graph:
        uri = item.get("@id", "")
        if not uri or "/uom-conversion/" not in uri:
            continue

        code = extract_code_from_uri(uri)
        from_uom = extract_value(item.get("dunl:hasFromUOM"))
        to_uom = extract_value(item.get("dunl:hasToUOM"))
        factor_raw = extract_value(item.get("dunl:hasFactor")) or extract_value(item.get("dunl:hasConversionFactor"))
        description = extract_value(item.get("rdfs:label")) or extract_value(item.get("skos:prefLabel"))

        factor = None
        if factor_raw is not None:
            try:
                factor = float(factor_raw)
            except (ValueError, TypeError):
                pass

        if code:
            records.append({
                "conversion_code": str(code),
                "from_uom": str(from_uom) if from_uom else None,
                "to_uom": str(to_uom) if to_uom else None,
                "factor": factor,
                "description": str(description) if description else None,
                "dunl_uri": uri,
            })

    logger.info(f"Parsed {len(records)} UOM conversion records from JSON-LD")
    return records


def parse_calendars(graph: List[Dict[str, Any]], year: int) -> List[Dict[str, Any]]:
    """Parse calendar entries from JSON-LD @graph for a given year.

    Two entry types exist:
    - Commodity events: @id like /c/commodity/Metals_20250130, date in URI
    - Calendar dates: @id like /c/calendar/2025/01/25, date in dunl:hasDate
    """
    records = []
    for item in graph:
        uri = item.get("@id", "")
        if not uri:
            continue
        # Accept both /calendar/ and /commodity/ entries
        if "/calendar/" not in uri and "/commodity/" not in uri:
            continue

        # Determine commodity name
        commodity = extract_value(item.get("dunl:hasName"))
        if not commodity and "/commodity/" in uri:
            # Extract commodity from URI: /c/commodity/Metals_20250130 -> "Metals"
            code = extract_code_from_uri(uri)
            parts = code.rsplit("_", 1)
            if len(parts) == 2 and parts[1].isdigit():
                commodity = parts[0]
            else:
                commodity = code

        # Extract date
        event_date = extract_value(item.get("dunl:hasDate"))
        if not event_date:
            if "/commodity/" in uri:
                # Extract YYYYMMDD from URI like "Metals_20250130"
                code = extract_code_from_uri(uri)
                date_match = re.search(r"(\d{8})$", code)
                if date_match:
                    raw = date_match.group(1)
                    event_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            elif "/calendar/" in uri:
                # Extract from URI like /c/calendar/2025/01/25
                date_match = re.search(r"/calendar/(\d{4})/(\d{2})/(\d{2})", uri)
                if date_match:
                    event_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

        if not event_date:
            continue

        pub_affected = extract_value(item.get("dunl:hasPublicationAffected"))
        pub_comments = extract_value(item.get("dunl:hasPublicationAffectedComments"))
        svc_affected = extract_value(item.get("dunl:hasServiceAffected"))
        svc_comments = extract_value(item.get("dunl:hasServiceAffectedComments"))

        records.append({
            "year": year,
            "commodity": str(commodity) if commodity else None,
            "event_date": str(event_date),
            "publication_affected": bool(pub_affected) if pub_affected is not None else None,
            "publication_comments": str(pub_comments) if pub_comments else None,
            "service_affected": bool(svc_affected) if svc_affected is not None else None,
            "service_comments": str(svc_comments) if svc_comments else None,
            "dunl_uri": uri,
        })

    logger.info(f"Parsed {len(records)} calendar records for year {year}")
    return records


# ========== Display Names & Descriptions ==========


DATASET_INFO = {
    "currencies": {
        "display_name": "DUNL Currencies",
        "description": "ISO currency codes and names from S&P Global Data Unlocked (DUNL.org). CC licensed.",
    },
    "ports": {
        "display_name": "DUNL Ports",
        "description": "Commodity trading port symbols and locations from DUNL.org. CC licensed.",
    },
    "uom": {
        "display_name": "DUNL Units of Measure",
        "description": "Units of measure for commodity trading from DUNL.org. CC licensed.",
    },
    "uom_conversions": {
        "display_name": "DUNL UOM Conversions",
        "description": "Unit-of-measure conversion factors from DUNL.org. CC licensed.",
    },
    "calendars": {
        "display_name": "DUNL Holiday Calendars",
        "description": "Commodity market holiday calendars from DUNL.org. CC licensed.",
    },
}
