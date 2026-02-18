"""
International Economic Data metadata and schema generation utilities.

Handles:
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
- Indicator metadata
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_table_name(source: str, dataset: str, year: Optional[int] = None) -> str:
    """
    Generate PostgreSQL table name for international economic data.

    Format: intl_{source}_{dataset}

    Args:
        source: Data source (worldbank, imf, oecd, bis)
        dataset: Dataset name (wdi, ifs, mei, eer)
        year: Optional year suffix

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("worldbank", "wdi")
        'intl_worldbank_wdi'
        >>> generate_table_name("imf", "ifs")
        'intl_imf_ifs'
    """
    source_clean = source.lower().replace("-", "_").replace(" ", "_")
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")

    table_name = f"intl_{source_clean}_{dataset_clean}"

    if year:
        table_name += f"_{year}"

    return table_name


def generate_create_table_sql(table_name: str, source: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for international economic data.

    Args:
        table_name: PostgreSQL table name
        source: Data source (worldbank, imf, oecd, bis)
        dataset: Dataset name

    Returns:
        CREATE TABLE SQL statement
    """
    # Source-specific schemas
    schemas = {
        "worldbank": {
            "wdi": """
                id SERIAL PRIMARY KEY,
                indicator_id TEXT NOT NULL,
                indicator_name TEXT,
                country_id TEXT,
                country_name TEXT,
                country_iso3 TEXT,
                region TEXT,
                income_level TEXT,
                year INTEGER,
                value NUMERIC,
                unit TEXT,
                decimal_places INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "countries": """
                id SERIAL PRIMARY KEY,
                country_id TEXT NOT NULL UNIQUE,
                country_name TEXT,
                iso3_code TEXT,
                iso2_code TEXT,
                region_id TEXT,
                region_name TEXT,
                income_level_id TEXT,
                income_level_name TEXT,
                lending_type_id TEXT,
                lending_type_name TEXT,
                capital_city TEXT,
                longitude NUMERIC,
                latitude NUMERIC,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "indicators": """
                id SERIAL PRIMARY KEY,
                indicator_id TEXT NOT NULL UNIQUE,
                indicator_name TEXT,
                source_id TEXT,
                source_name TEXT,
                source_note TEXT,
                source_organization TEXT,
                topics TEXT[],
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
        },
        "imf": {
            "ifs": """
                id SERIAL PRIMARY KEY,
                indicator_code TEXT NOT NULL,
                indicator_name TEXT,
                country_code TEXT,
                country_name TEXT,
                period TEXT,
                frequency TEXT,
                value NUMERIC,
                unit_mult TEXT,
                status TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "weo": """
                id SERIAL PRIMARY KEY,
                indicator_code TEXT NOT NULL,
                indicator_name TEXT,
                country_code TEXT,
                country_name TEXT,
                year INTEGER,
                value NUMERIC,
                units TEXT,
                scale TEXT,
                estimates_start INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
        },
        "oecd": {
            "mei": """
                id SERIAL PRIMARY KEY,
                indicator_code TEXT NOT NULL,
                indicator_name TEXT,
                country_code TEXT,
                country_name TEXT,
                subject TEXT,
                measure TEXT,
                frequency TEXT,
                period TEXT,
                value NUMERIC,
                unit TEXT,
                powercode TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "cli": """
                id SERIAL PRIMARY KEY,
                indicator_code TEXT,
                country_code TEXT,
                country_name TEXT,
                period TEXT,
                value NUMERIC,
                frequency TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "kei": """
                id SERIAL PRIMARY KEY,
                indicator_code TEXT,
                indicator_name TEXT,
                country_code TEXT,
                subject TEXT,
                measure TEXT,
                frequency TEXT,
                period TEXT,
                value NUMERIC,
                unit TEXT,
                transformation TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "alfs": """
                id SERIAL PRIMARY KEY,
                country_code TEXT,
                indicator_code TEXT,
                sex TEXT,
                age TEXT,
                frequency TEXT,
                period TEXT,
                value NUMERIC,
                unit_measure TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "batis": """
                id SERIAL PRIMARY KEY,
                reporter_country TEXT,
                partner_country TEXT,
                flow TEXT,
                service_item TEXT,
                frequency TEXT,
                period TEXT,
                value NUMERIC,
                unit_measure TEXT,
                currency TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "tax": """
                id SERIAL PRIMARY KEY,
                country_code TEXT,
                tax_type TEXT,
                government_level TEXT,
                measure TEXT,
                period TEXT,
                value NUMERIC,
                unit TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
        },
        "bis": {
            "eer": """
                id SERIAL PRIMARY KEY,
                country_code TEXT,
                country_name TEXT,
                eer_type TEXT,
                basket TEXT,
                period TEXT,
                frequency TEXT,
                value NUMERIC,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "property": """
                id SERIAL PRIMARY KEY,
                country_code TEXT,
                country_name TEXT,
                property_type TEXT,
                unit_measure TEXT,
                period TEXT,
                frequency TEXT,
                value NUMERIC,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
            "credit": """
                id SERIAL PRIMARY KEY,
                country_code TEXT,
                borrower_sector TEXT,
                lending_sector TEXT,
                valuation TEXT,
                period TEXT,
                frequency TEXT,
                value NUMERIC,
                unit TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            """,
        },
    }

    # Get schema for source/dataset
    source_schemas = schemas.get(source.lower(), {})
    columns = source_schemas.get(dataset.lower())

    if not columns:
        # Default schema for unknown datasets
        columns = """
            id SERIAL PRIMARY KEY,
            indicator_code TEXT,
            indicator_name TEXT,
            country_code TEXT,
            country_name TEXT,
            period TEXT,
            year INTEGER,
            value NUMERIC,
            unit TEXT,
            source_dataset TEXT,
            raw_data JSONB,
            ingested_at TIMESTAMP DEFAULT NOW()
        """

    # Build appropriate indexes based on the column names in the schema
    index_sql = ""

    # Determine which columns exist for indexing
    if "indicator_id" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_indicator ON {table_name} (indicator_id);"
    elif "indicator_code" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_indicator ON {table_name} (indicator_code);"

    if "country_id" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_country ON {table_name} (country_id);"
    elif "country_code" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_country ON {table_name} (country_code);"

    if "year" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name} (year);"
    elif "period" in columns:
        index_sql += f"\n    CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name} (period);"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    {index_sql}
    """

    return sql


def parse_worldbank_data(
    data: List[Dict[str, Any]], indicator_id: str
) -> List[Dict[str, Any]]:
    """
    Parse World Bank API response data.

    Args:
        data: Raw data from World Bank API
        indicator_id: Indicator being fetched

    Returns:
        List of parsed records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            # Extract country info
            country = item.get("country", {})
            indicator = item.get("indicator", {})

            record = {
                "indicator_id": indicator.get("id", indicator_id),
                "indicator_name": indicator.get("value"),
                "country_id": item.get("countryiso3code") or country.get("id"),
                "country_name": country.get("value"),
                "country_iso3": item.get("countryiso3code"),
                "year": int(item.get("date")) if item.get("date") else None,
                "value": float(item.get("value"))
                if item.get("value") is not None
                else None,
                "unit": item.get("unit"),
                "decimal_places": item.get("decimal"),
            }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse World Bank record: {e}")
            continue

    return records


def parse_worldbank_countries(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse World Bank countries API response.

    Args:
        data: Raw countries data from World Bank API

    Returns:
        List of parsed country records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            region = item.get("region", {})
            income = item.get("incomeLevel", {})
            lending = item.get("lendingType", {})

            record = {
                "country_id": item.get("id"),
                "country_name": item.get("name"),
                "iso3_code": item.get("iso2Code"),
                "iso2_code": item.get("id"),
                "region_id": region.get("id"),
                "region_name": region.get("value"),
                "income_level_id": income.get("id"),
                "income_level_name": income.get("value"),
                "lending_type_id": lending.get("id"),
                "lending_type_name": lending.get("value"),
                "capital_city": item.get("capitalCity"),
                "longitude": float(item.get("longitude"))
                if item.get("longitude")
                else None,
                "latitude": float(item.get("latitude"))
                if item.get("latitude")
                else None,
            }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse World Bank country: {e}")
            continue

    return records


def parse_worldbank_indicators(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse World Bank indicators API response.

    Args:
        data: Raw indicators data from World Bank API

    Returns:
        List of parsed indicator records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            source = item.get("source", {})
            topics = item.get("topics", [])

            record = {
                "indicator_id": item.get("id"),
                "indicator_name": item.get("name"),
                "source_id": source.get("id"),
                "source_name": source.get("value"),
                "source_note": item.get("sourceNote"),
                "source_organization": item.get("sourceOrganization"),
                "topics": [t.get("value") for t in topics if t.get("value")],
            }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse World Bank indicator: {e}")
            continue

    return records


def parse_imf_data(data: List[Dict[str, Any]], dataset: str) -> List[Dict[str, Any]]:
    """
    Parse IMF API response data.

    Args:
        data: Raw data from IMF API
        dataset: Dataset name (ifs, weo, etc.)

    Returns:
        List of parsed records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            record = {
                "indicator_code": item.get("indicator"),
                "indicator_name": item.get("indicator_name"),
                "country_code": item.get("ref_area"),
                "country_name": item.get("ref_area_name"),
                "period": item.get("period"),
                "frequency": item.get("freq"),
                "value": float(item.get("value"))
                if item.get("value") is not None
                else None,
                "unit_mult": item.get("unit_mult"),
                "status": item.get("status"),
            }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse IMF record: {e}")
            continue

    return records


def parse_oecd_data(data: List[Dict[str, Any]], dataset: str) -> List[Dict[str, Any]]:
    """
    Parse OECD API response data.

    Handles SDMX dimension names from the new OECD API:
    - REF_AREA -> country_code
    - MEASURE -> indicator_code
    - FREQ -> frequency
    - TIME_PERIOD -> period
    - ADJUSTMENT, ACTIVITY, etc.

    Args:
        data: Raw data from OECD API
        dataset: Dataset name (mei, cli, etc.)

    Returns:
        List of parsed records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            # Map SDMX dimension names to our schema
            # OECD CLI dimensions: REF_AREA, FREQ, MEASURE, UNIT_MEASURE, ACTIVITY, ADJUSTMENT, TRANSFORMATION, TIME_HORIZ, METHODOLOGY
            record = {
                # Country from ref_area (OECD SDMX name)
                "country_code": item.get("ref_area")
                or item.get("location")
                or item.get("country"),
                # Indicator code from measure or subject
                "indicator_code": item.get("measure")
                or item.get("subject")
                or item.get("indicator"),
                # Subject might be separate from indicator
                "subject": item.get("measure") or item.get("subject"),
                # Measure type (different from indicator in some OECD datasets)
                "measure": item.get("measure") or item.get("transformation"),
                # Frequency (M=Monthly, Q=Quarterly, A=Annual)
                "frequency": item.get("freq") or item.get("frequency"),
                # Time period
                "period": item.get("period")
                or item.get("time_period")
                or item.get("time"),
                # Value (numeric)
                "value": float(item.get("value"))
                if item.get("value") is not None
                else None,
                # Unit from unit_measure
                "unit": item.get("unit_measure") or item.get("unit"),
                # Power code / multiplier
                "powercode": item.get("powercode") or item.get("unit_mult"),
            }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse OECD record: {e}")
            continue

    return records


def parse_bis_data(data: List[Dict[str, Any]], dataset: str) -> List[Dict[str, Any]]:
    """
    Parse BIS API response data.

    Args:
        data: Raw data from BIS API
        dataset: Dataset name

    Returns:
        List of parsed records
    """
    records = []

    for item in data:
        if not item:
            continue

        try:
            # Map BIS fields to common schema
            if dataset.lower() in ["eer", "ws_eer"]:
                record = {
                    "country_code": item.get("ref_area"),
                    "country_name": item.get("ref_area_name"),
                    "eer_type": item.get("eer_type"),
                    "basket": item.get("eer_basket"),
                    "period": item.get("period"),
                    "frequency": item.get("freq"),
                    "value": float(item.get("value"))
                    if item.get("value") is not None
                    else None,
                }
            elif dataset.lower() in ["property", "ws_spp"]:
                record = {
                    "country_code": item.get("ref_area"),
                    "country_name": item.get("ref_area_name"),
                    "property_type": item.get("unit_type"),
                    "unit_measure": item.get("unit_measure"),
                    "period": item.get("period"),
                    "frequency": item.get("freq"),
                    "value": float(item.get("value"))
                    if item.get("value") is not None
                    else None,
                }
            else:
                record = {
                    "country_code": item.get("ref_area"),
                    "period": item.get("period"),
                    "frequency": item.get("freq"),
                    "value": float(item.get("value"))
                    if item.get("value") is not None
                    else None,
                }

            records.append(record)

        except Exception as e:
            logger.warning(f"Failed to parse BIS record: {e}")
            continue

    return records


def get_default_date_range(source: str = "worldbank") -> Tuple[int, int]:
    """
    Get default date range for international data.

    Args:
        source: Data source

    Returns:
        Tuple of (start_year, end_year)
    """
    current_year = datetime.now().year

    date_ranges = {
        "worldbank": (current_year - 10, current_year),
        "imf": (current_year - 10, current_year),
        "oecd": (current_year - 10, current_year),
        "bis": (current_year - 10, current_year),
    }

    return date_ranges.get(source.lower(), (current_year - 10, current_year))


def get_source_display_name(source: str, dataset: str) -> str:
    """
    Get human-readable display name for source/dataset.

    Args:
        source: Data source
        dataset: Dataset name

    Returns:
        Display name
    """
    display_names = {
        "worldbank_wdi": "World Bank World Development Indicators",
        "worldbank_countries": "World Bank Countries Metadata",
        "worldbank_indicators": "World Bank Indicators Metadata",
        "imf_ifs": "IMF International Financial Statistics",
        "imf_weo": "IMF World Economic Outlook",
        "oecd_mei": "OECD Main Economic Indicators",
        "oecd_cli": "OECD Composite Leading Indicators",
        "bis_eer": "BIS Effective Exchange Rates",
        "bis_property": "BIS Property Prices",
        "bis_credit": "BIS Credit Statistics",
    }

    key = f"{source.lower()}_{dataset.lower()}"
    return display_names.get(key, f"{source.upper()} {dataset.upper()} Data")


def get_source_description(source: str, dataset: str) -> str:
    """
    Get description for source/dataset.

    Args:
        source: Data source
        dataset: Dataset name

    Returns:
        Description
    """
    descriptions = {
        "worldbank_wdi": "World Development Indicators from World Bank - 1,600+ indicators for 200+ countries covering GDP, population, health, education, and more",
        "worldbank_countries": "Country metadata from World Bank including regions, income levels, and geographic coordinates",
        "worldbank_indicators": "Indicator metadata from World Bank including descriptions and source information",
        "imf_ifs": "International Financial Statistics from IMF - comprehensive macroeconomic data including exchange rates, prices, and monetary statistics",
        "imf_weo": "World Economic Outlook from IMF - economic projections and analysis for IMF member countries",
        "oecd_mei": "Main Economic Indicators from OECD - key economic indicators for OECD member countries",
        "oecd_cli": "Composite Leading Indicators from OECD - designed to provide early signals of turning points in business cycles",
        "bis_eer": "Effective Exchange Rates from BIS - nominal and real effective exchange rates",
        "bis_property": "Property Prices from BIS - residential property price statistics",
        "bis_credit": "Credit Statistics from BIS - credit to non-financial sector statistics",
    }

    key = f"{source.lower()}_{dataset.lower()}"
    return descriptions.get(key, f"International economic data from {source.upper()}")


def build_insert_values(
    records: List[Dict[str, Any]], source: str, dataset: str
) -> List[Dict[str, Any]]:
    """
    Build parameterized insert values from parsed records.

    Args:
        records: List of parsed data records
        source: Data source
        dataset: Dataset name

    Returns:
        List of dicts ready for parameterized insert
    """
    # Define expected columns per source/dataset
    column_sets = {
        "worldbank_wdi": [
            "indicator_id",
            "indicator_name",
            "country_id",
            "country_name",
            "country_iso3",
            "region",
            "income_level",
            "year",
            "value",
            "unit",
            "decimal_places",
        ],
        "worldbank_countries": [
            "country_id",
            "country_name",
            "iso3_code",
            "iso2_code",
            "region_id",
            "region_name",
            "income_level_id",
            "income_level_name",
            "lending_type_id",
            "lending_type_name",
            "capital_city",
            "longitude",
            "latitude",
        ],
        "worldbank_indicators": [
            "indicator_id",
            "indicator_name",
            "source_id",
            "source_name",
            "source_note",
            "source_organization",
            "topics",
        ],
        "imf_ifs": [
            "indicator_code",
            "indicator_name",
            "country_code",
            "country_name",
            "period",
            "frequency",
            "value",
            "unit_mult",
            "status",
        ],
        "oecd_mei": [
            "indicator_code",
            "indicator_name",
            "country_code",
            "country_name",
            "subject",
            "measure",
            "frequency",
            "period",
            "value",
            "unit",
            "powercode",
        ],
        "bis_eer": [
            "country_code",
            "country_name",
            "eer_type",
            "basket",
            "period",
            "frequency",
            "value",
        ],
        "bis_property": [
            "country_code",
            "country_name",
            "property_type",
            "unit_measure",
            "period",
            "frequency",
            "value",
        ],
    }

    key = f"{source.lower()}_{dataset.lower()}"
    columns = column_sets.get(key, list(records[0].keys()) if records else [])

    insert_rows = []
    for record in records:
        row = {col: record.get(col) for col in columns}
        insert_rows.append(row)

    return insert_rows


# Common World Bank Indicator categories
WDI_INDICATOR_CATEGORIES = {
    "economic": {
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
        "NY.GDP.PCAP.PP.CD": "GDP per capita, PPP (current international $)",
        "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
        "BN.CAB.XOKA.CD": "Current account balance (BoP, current US$)",
    },
    "population": {
        "SP.POP.TOTL": "Population, total",
        "SP.POP.GROW": "Population growth (annual %)",
        "SP.URB.TOTL.IN.ZS": "Urban population (% of total population)",
        "SP.DYN.CDRT.IN": "Death rate, crude (per 1,000 people)",
        "SP.DYN.CBRT.IN": "Birth rate, crude (per 1,000 people)",
    },
    "labor": {
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
        "SL.TLF.CACT.ZS": "Labor force participation rate, total (% of pop ages 15+)",
        "SL.GDP.PCAP.EM.KD": "GDP per person employed (constant 2017 PPP $)",
    },
    "prices": {
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "NY.GDP.DEFL.KD.ZG": "Inflation, GDP deflator (annual %)",
    },
    "poverty": {
        "SI.POV.DDAY": "Poverty headcount ratio at $2.15 a day (2017 PPP)",
        "SI.POV.GINI": "Gini index",
        "SI.DST.10TH.10": "Income share held by highest 10%",
    },
    "health": {
        "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
        "SH.XPD.CHEX.GD.ZS": "Current health expenditure (% of GDP)",
        "SH.DYN.MORT": "Mortality rate, under-5 (per 1,000 live births)",
    },
    "education": {
        "SE.XPD.TOTL.GD.ZS": "Government expenditure on education (% of GDP)",
        "SE.ADT.LITR.ZS": "Literacy rate, adult total (% ages 15+)",
        "SE.TER.ENRR": "School enrollment, tertiary (% gross)",
    },
    "environment": {
        "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
        "EG.USE.ELEC.KH.PC": "Electric power consumption (kWh per capita)",
        "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    },
}
