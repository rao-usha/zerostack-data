"""
openFDA Device Registration metadata and schema definitions.

Handles:
- Table schema for fda_device_registrations
- Record parsing from openFDA API response format
- Product code constants for aesthetic/MedSpa devices
- Display names and descriptions

openFDA API: https://open.fda.gov/apis/device/registrationlisting/
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE CONFIGURATION
# =============================================================================

TABLE_NAME = "fda_device_registrations"

COLUMNS = [
    "registration_number",
    "fei_number",
    "firm_name",
    "address_line1",
    "address_line2",
    "city",
    "state_code",
    "zip_code",
    "country_code",
    "establishment_type",
    "product_codes",
    "device_names",
    "proprietary_names",
    "k_numbers",
    "registration_status",
    "initial_importer_flag",
    "reg_expiry_date_year",
    "ingested_at",
]

CONFLICT_COLUMNS = ["registration_number"]

UPDATE_COLUMNS = [
    "fei_number",
    "firm_name",
    "address_line1",
    "address_line2",
    "city",
    "state_code",
    "zip_code",
    "country_code",
    "establishment_type",
    "product_codes",
    "device_names",
    "proprietary_names",
    "k_numbers",
    "registration_status",
    "initial_importer_flag",
    "reg_expiry_date_year",
    "ingested_at",
]


# =============================================================================
# AESTHETIC / MEDSPA PRODUCT CODES
# =============================================================================

AESTHETIC_PRODUCT_CODES = {
    "GEX": "Laser, Nd:YAG (neodymium-doped yttrium aluminum garnet)",
    "ILY": "Light-based device (Intense Pulsed Light / phototherapy)",
    "QMT": "Radiofrequency device (skin tightening / body contouring)",
    "OOF": "Microdermabrasion device",
    "GEY": "Laser, CO2 (carbon dioxide surgical laser)",
    "GEW": "Laser, Alexandrite",
    "IYE": "Laser, Diode (hair removal / skin treatment)",
    "IYN": "Laser, Erbium (skin resurfacing)",
    "KQH": "Cryolipolysis device (fat reduction)",
    "QKQ": "Powered suction lipoplasty device",
    "FRN": "Ultrasonic surgical instrument (HIFU / body contouring)",
    "OZP": "Skin micro-needling device (collagen induction)",
    "MQR": "Sclerotherapy device (vein treatment)",
    "LYZ": "Photodynamic therapy device",
    "BYJ": "Electrosurgical unit (cautery / tissue ablation)",
    "MYA": "LED light therapy device",
}

# All US state codes for pagination
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS",
]


# =============================================================================
# SQL GENERATION
# =============================================================================


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for fda_device_registrations.

    Uses registration_number as PRIMARY KEY.
    product_codes, device_names, proprietary_names, and k_numbers
    are stored as JSONB arrays for flexible querying.

    Returns:
        CREATE TABLE IF NOT EXISTS SQL string
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        registration_number TEXT PRIMARY KEY,
        fei_number TEXT,
        firm_name TEXT,
        address_line1 TEXT,
        address_line2 TEXT,
        city TEXT,
        state_code TEXT,
        zip_code TEXT,
        country_code TEXT,
        establishment_type TEXT,
        product_codes JSONB DEFAULT '[]'::jsonb,
        device_names JSONB DEFAULT '[]'::jsonb,
        proprietary_names JSONB DEFAULT '[]'::jsonb,
        k_numbers JSONB DEFAULT '[]'::jsonb,
        registration_status TEXT,
        initial_importer_flag TEXT,
        reg_expiry_date_year TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state_code);

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_firm
        ON {TABLE_NAME} (firm_name);

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_city_state
        ON {TABLE_NAME} (state_code, city);

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_product_codes
        ON {TABLE_NAME} USING GIN (product_codes);
    """


# =============================================================================
# RECORD PARSING
# =============================================================================


def _safe_get(data: dict, *keys, default=None):
    """Safely traverse nested dict keys."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current


def _extract_products(result: dict) -> Dict[str, List[str]]:
    """
    Extract product codes, device names, proprietary names, and k_numbers
    from the nested openFDA products array.

    The openFDA response nests products under each registration result:
    result.products[].openfda.{device_name, registration_number, ...}

    Args:
        result: A single registration result dict from openFDA

    Returns:
        Dict with product_codes, device_names, proprietary_names, k_numbers
    """
    product_codes = set()
    device_names = set()
    proprietary_names = set()
    k_numbers = set()

    products = result.get("products", [])
    if not isinstance(products, list):
        products = []

    for product in products:
        # Product code is a direct field
        code = product.get("product_code")
        if code:
            product_codes.add(code)

        # openfda sub-object has enriched data
        openfda = product.get("openfda", {})
        if isinstance(openfda, dict):
            for name in openfda.get("device_name", []):
                if name:
                    device_names.add(name)

            for k in openfda.get("k_number", []):
                if k:
                    k_numbers.add(k)

        # Proprietary name can also be on the product directly
        pname = product.get("proprietary_name")
        if pname:
            proprietary_names.add(pname)

    return {
        "product_codes": sorted(product_codes),
        "device_names": sorted(device_names),
        "proprietary_names": sorted(proprietary_names),
        "k_numbers": sorted(k_numbers),
    }


def parse_registration_record(result: dict) -> Optional[Dict[str, Any]]:
    """
    Parse a single openFDA device registration result into a flat row dict.

    The openFDA response structure is:
    {
        "registration": {
            "registration_number": "...",
            "fei_number": "...",
            "status_code": "...",
            "initial_importer_flag": "...",
            "reg_expiry_date_year": "...",
            "owner_operator": {
                "firm_name": "...",
                "official_correspondent": {...}
            },
            "address_line_1": "...",
            "city": "...",
            "state_code": "...",
            "zip_code": "...",
            "iso_country_code": "..."
        },
        "establishment_type": [...],
        "products": [...]
    }

    Args:
        result: Single result dict from openFDA API response

    Returns:
        Parsed row dict or None if registration_number is missing
    """
    reg = result.get("registration", {})
    if not isinstance(reg, dict):
        reg = {}

    registration_number = reg.get("registration_number")
    if not registration_number:
        return None

    # Extract owner/operator firm name
    owner_operator = reg.get("owner_operator", {})
    if not isinstance(owner_operator, dict):
        owner_operator = {}
    firm_name = owner_operator.get("firm_name", "")

    # Establishment type is a list at the top level
    establishment_types = result.get("establishment_type", [])
    if isinstance(establishment_types, list):
        establishment_type = ", ".join(establishment_types)
    elif isinstance(establishment_types, str):
        establishment_type = establishment_types
    else:
        establishment_type = None

    # Extract product data
    product_data = _extract_products(result)

    return {
        "registration_number": registration_number,
        "fei_number": reg.get("fei_number"),
        "firm_name": firm_name,
        "address_line1": reg.get("address_line_1"),
        "address_line2": reg.get("address_line_2"),
        "city": reg.get("city"),
        "state_code": reg.get("state_code"),
        "zip_code": reg.get("zip_code"),
        "country_code": reg.get("iso_country_code"),
        "establishment_type": establishment_type,
        "product_codes": product_data["product_codes"],
        "device_names": product_data["device_names"],
        "proprietary_names": product_data["proprietary_names"],
        "k_numbers": product_data["k_numbers"],
        "registration_status": reg.get("status_code"),
        "initial_importer_flag": reg.get("initial_importer_flag"),
        "reg_expiry_date_year": reg.get("reg_expiry_date_year"),
        "ingested_at": None,  # Will use DB default NOW()
    }


# =============================================================================
# DISPLAY METADATA
# =============================================================================


def get_display_name() -> str:
    """Human-readable name for dataset registry."""
    return "FDA Device Registrations & Listings"


def get_description() -> str:
    """Description for dataset registry."""
    return (
        "openFDA device registration and listing data including manufacturer "
        "establishments, product codes, device names, and 510(k) clearances. "
        "Covers all FDA-registered device establishments in the United States."
    )
