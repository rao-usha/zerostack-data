"""
SAM.gov metadata utilities.

Handles:
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation from API responses
- Field definitions and schema documentation
"""

import logging
from typing import Dict, List, Any, Optional
import json

logger = logging.getLogger(__name__)

TABLE_NAME = "sam_gov_entities"
DATASET_ID = "sam_gov_entities"
DISPLAY_NAME = "SAM.gov Entity Registrations"
DESCRIPTION = (
    "Federal contractor entity registrations from the System for Award "
    "Management (SAM.gov). Includes active registrations with UEI, CAGE codes, "
    "NAICS classifications, business types, and physical addresses."
)

COLUMNS = [
    "uei",
    "cage_code",
    "legal_business_name",
    "dba_name",
    "physical_address_line1",
    "physical_address_city",
    "physical_address_state",
    "physical_address_zip",
    "naics_code_primary",
    "naics_codes_all",
    "business_types",
    "entity_structure",
    "registration_status",
    "registration_date",
    "expiration_date",
    "entity_url",
]

CONFLICT_COLUMNS = ["uei"]

UPDATE_COLUMNS = [
    "cage_code",
    "legal_business_name",
    "dba_name",
    "physical_address_line1",
    "physical_address_city",
    "physical_address_state",
    "physical_address_zip",
    "naics_code_primary",
    "naics_codes_all",
    "business_types",
    "entity_structure",
    "registration_status",
    "registration_date",
    "expiration_date",
    "entity_url",
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    uei TEXT PRIMARY KEY,
    cage_code TEXT,
    legal_business_name TEXT,
    dba_name TEXT,
    physical_address_line1 TEXT,
    physical_address_city TEXT,
    physical_address_state TEXT,
    physical_address_zip TEXT,
    naics_code_primary TEXT,
    naics_codes_all JSONB,
    business_types JSONB,
    entity_structure TEXT,
    registration_status TEXT,
    registration_date DATE,
    expiration_date DATE,
    entity_url TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
    ON {TABLE_NAME} (physical_address_state);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_naics
    ON {TABLE_NAME} (naics_code_primary);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_name
    ON {TABLE_NAME} (legal_business_name);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_status
    ON {TABLE_NAME} (registration_status);

COMMENT ON TABLE {TABLE_NAME} IS 'SAM.gov entity registrations - federal contractor database';
"""


def parse_entity(entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single SAM.gov entity API response into a database row.

    The SAM.gov API returns deeply nested JSON. This function extracts
    the relevant fields into a flat dictionary.

    Args:
        entity: Raw entity dict from API response

    Returns:
        Parsed row dict or None if entity is missing required fields
    """
    try:
        registration = entity.get("entityRegistration", {})
        core_data = entity.get("coreData", {})
        entity_info = core_data.get("entityInformation", {})
        physical_addr = core_data.get("physicalAddress", {})
        general_info = core_data.get("generalInformation", {})

        uei = registration.get("ueiSAM")
        if not uei:
            logger.debug("Skipping entity with no UEI")
            return None

        # Extract NAICS codes
        naics_list = entity.get("assertions", {}).get("goodsAndServices", {}).get(
            "naicsList", []
        )
        primary_naics = None
        all_naics = []
        for naics in naics_list:
            code = naics.get("naicsCode")
            if code:
                all_naics.append(code)
                if naics.get("isPrimary", False):
                    primary_naics = code

        if not primary_naics and all_naics:
            primary_naics = all_naics[0]

        # Extract business types
        business_types_data = entity.get("assertions", {}).get(
            "businessTypes", {}
        ).get("businessTypeList", [])
        business_types = [
            bt.get("businessType", "") for bt in business_types_data if bt.get("businessType")
        ]

        return {
            "uei": uei,
            "cage_code": registration.get("cageCode"),
            "legal_business_name": registration.get("legalBusinessName"),
            "dba_name": registration.get("dbaName"),
            "physical_address_line1": physical_addr.get("addressLine1"),
            "physical_address_city": physical_addr.get("city"),
            "physical_address_state": physical_addr.get("stateOrProvinceCode"),
            "physical_address_zip": physical_addr.get("zipCode"),
            "naics_code_primary": primary_naics,
            "naics_codes_all": json.dumps(all_naics) if all_naics else None,
            "business_types": json.dumps(business_types) if business_types else None,
            "entity_structure": entity_info.get("entityStructureDesc"),
            "registration_status": registration.get("registrationStatus"),
            "registration_date": registration.get("registrationDate"),
            "expiration_date": registration.get("registrationExpirationDate"),
            "entity_url": general_info.get("entityUrl"),
        }

    except Exception as e:
        logger.warning(f"Failed to parse SAM.gov entity: {e}")
        return None


def parse_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of SAM.gov entity API responses into database rows.

    Args:
        entities: List of raw entity dicts from API

    Returns:
        List of parsed row dicts (skips entities with missing required fields)
    """
    rows = []
    for entity in entities:
        row = parse_entity(entity)
        if row:
            rows.append(row)

    logger.info(f"Parsed {len(rows)}/{len(entities)} SAM.gov entities")
    return rows
