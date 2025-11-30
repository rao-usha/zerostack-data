"""
SEC Form ADV metadata parsing and schema generation.

Handles:
- Parsing Form ADV data from SEC IAPD
- Extracting adviser information (business contact, personnel, AUM, etc.)
- Generating table schemas for Form ADV data
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, date

logger = logging.getLogger(__name__)


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for SEC Form ADV data.
    
    Form ADV contains:
    - Part 1: Registration information (business details, contact info, etc.)
    - Part 2: Narrative brochure (services, fees, practices)
    
    Returns:
        SQL CREATE TABLE statement
    """
    return """
        CREATE TABLE IF NOT EXISTS sec_form_adv (
            id SERIAL PRIMARY KEY,
            
            -- Adviser identifiers
            crd_number TEXT NOT NULL UNIQUE,
            sec_number TEXT,
            firm_name TEXT NOT NULL,
            
            -- Business contact information
            business_address_street1 TEXT,
            business_address_street2 TEXT,
            business_address_city TEXT,
            business_address_state TEXT,
            business_address_zip TEXT,
            business_address_country TEXT,
            business_phone TEXT,
            business_fax TEXT,
            business_email TEXT,
            website TEXT,
            
            -- Mailing address (if different)
            mailing_address_street1 TEXT,
            mailing_address_street2 TEXT,
            mailing_address_city TEXT,
            mailing_address_state TEXT,
            mailing_address_zip TEXT,
            mailing_address_country TEXT,
            
            -- Firm details
            legal_name TEXT,
            doing_business_as TEXT,
            registration_status TEXT,
            registration_date DATE,
            state_registrations TEXT[],  -- Array of states where registered
            
            -- Assets under management
            assets_under_management NUMERIC,
            aum_date DATE,
            aum_currency TEXT DEFAULT 'USD',
            
            -- Client information
            total_client_count INT,
            individual_client_count INT,
            high_net_worth_client_count INT,
            pooled_investment_vehicle_count INT,
            
            -- Business type
            is_family_office BOOLEAN DEFAULT FALSE,
            is_registered_with_sec BOOLEAN DEFAULT TRUE,
            is_registered_with_state BOOLEAN DEFAULT FALSE,
            
            -- Key personnel (stored as JSONB for flexibility)
            key_personnel JSONB,
            
            -- Form ADV metadata
            form_adv_url TEXT,
            filing_date DATE,
            last_amended_date DATE,
            
            -- Processing metadata
            ingested_at TIMESTAMP DEFAULT NOW(),
            last_updated_at TIMESTAMP DEFAULT NOW(),
            
            -- Indexes for efficient queries
            INDEX idx_formadv_crd (crd_number),
            INDEX idx_formadv_sec_number (sec_number),
            INDEX idx_formadv_firm_name (firm_name),
            INDEX idx_formadv_state (business_address_state),
            INDEX idx_formadv_family_office (is_family_office)
        );
    """


def generate_personnel_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for Form ADV key personnel.
    
    Separate table to store individual personnel records.
    
    Returns:
        SQL CREATE TABLE statement
    """
    return """
        CREATE TABLE IF NOT EXISTS sec_form_adv_personnel (
            id SERIAL PRIMARY KEY,
            
            -- Link to firm
            crd_number TEXT NOT NULL,
            
            -- Individual identifiers
            individual_crd_number TEXT,
            
            -- Name
            first_name TEXT,
            middle_name TEXT,
            last_name TEXT,
            full_name TEXT,
            
            -- Position/Title
            title TEXT,
            position_type TEXT,  -- e.g., "Executive Officer", "Control Person", etc.
            
            -- Contact information
            email TEXT,
            phone TEXT,
            
            -- Processing metadata
            ingested_at TIMESTAMP DEFAULT NOW(),
            
            -- Foreign key constraint
            FOREIGN KEY (crd_number) REFERENCES sec_form_adv(crd_number),
            
            -- Indexes
            INDEX idx_formadv_personnel_crd (crd_number),
            INDEX idx_formadv_personnel_individual_crd (individual_crd_number),
            INDEX idx_formadv_personnel_name (last_name, first_name)
        );
    """


def parse_adviser_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse adviser information from Form ADV data.
    
    Args:
        data: Raw Form ADV data (structure depends on source)
        
    Returns:
        Parsed adviser info ready for database insertion
    """
    try:
        info = data.get("info", data)  # Handle different response structures
        
        # Extract basic identifiers
        crd_number = str(info.get("crd_number") or info.get("crdNumber") or "")
        sec_number = info.get("sec_number") or info.get("secNumber")
        
        # Extract firm name
        firm_name = info.get("firm_name") or info.get("firmName") or info.get("name")
        legal_name = info.get("legal_name") or info.get("legalName") or firm_name
        
        # Extract business address
        business_addr = info.get("business_address") or info.get("businessAddress") or {}
        mailing_addr = info.get("mailing_address") or info.get("mailingAddress") or {}
        
        # Extract contact info
        contact = info.get("contact", {})
        
        # Extract registration info
        registration = info.get("registration", {})
        
        # Extract AUM info
        aum_info = info.get("assets_under_management") or info.get("assetsUnderManagement") or {}
        
        # Extract client counts
        clients = info.get("clients", {})
        
        parsed = {
            "crd_number": crd_number,
            "sec_number": sec_number,
            "firm_name": firm_name,
            "legal_name": legal_name,
            "doing_business_as": info.get("doing_business_as") or info.get("doingBusinessAs"),
            
            # Business address
            "business_address_street1": business_addr.get("street1") or business_addr.get("street_1"),
            "business_address_street2": business_addr.get("street2") or business_addr.get("street_2"),
            "business_address_city": business_addr.get("city"),
            "business_address_state": business_addr.get("state"),
            "business_address_zip": business_addr.get("zip") or business_addr.get("postal_code"),
            "business_address_country": business_addr.get("country") or "US",
            
            # Contact info
            "business_phone": contact.get("phone") or business_addr.get("phone"),
            "business_fax": contact.get("fax"),
            "business_email": contact.get("email"),
            "website": info.get("website") or info.get("web_address"),
            
            # Mailing address (if different)
            "mailing_address_street1": mailing_addr.get("street1") or mailing_addr.get("street_1"),
            "mailing_address_street2": mailing_addr.get("street2") or mailing_addr.get("street_2"),
            "mailing_address_city": mailing_addr.get("city"),
            "mailing_address_state": mailing_addr.get("state"),
            "mailing_address_zip": mailing_addr.get("zip") or mailing_addr.get("postal_code"),
            "mailing_address_country": mailing_addr.get("country"),
            
            # Registration info
            "registration_status": registration.get("status"),
            "registration_date": _parse_date(registration.get("date")),
            "state_registrations": registration.get("states") or [],
            "is_registered_with_sec": registration.get("with_sec", True),
            "is_registered_with_state": registration.get("with_state", False),
            
            # AUM
            "assets_under_management": _parse_numeric(aum_info.get("amount")),
            "aum_date": _parse_date(aum_info.get("date")),
            "aum_currency": aum_info.get("currency") or "USD",
            
            # Clients
            "total_client_count": _parse_int(clients.get("total")),
            "individual_client_count": _parse_int(clients.get("individuals")),
            "high_net_worth_client_count": _parse_int(clients.get("high_net_worth")),
            "pooled_investment_vehicle_count": _parse_int(clients.get("pooled_vehicles")),
            
            # Business type
            "is_family_office": _is_family_office(info),
            
            # Key personnel (as JSONB)
            "key_personnel": info.get("key_personnel") or info.get("keyPersonnel"),
            
            # Form ADV metadata
            "form_adv_url": info.get("form_adv_url") or info.get("url"),
            "filing_date": _parse_date(info.get("filing_date")),
            "last_amended_date": _parse_date(info.get("last_amended_date")),
        }
        
        return parsed
    
    except Exception as e:
        logger.error(f"Error parsing adviser info: {e}")
        raise


def parse_key_personnel(
    crd_number: str,
    personnel_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Parse key personnel from Form ADV data.
    
    Args:
        crd_number: Firm CRD number
        personnel_data: List of personnel records
        
    Returns:
        List of parsed personnel records ready for database insertion
    """
    parsed_personnel = []
    
    for person in personnel_data:
        try:
            parsed = {
                "crd_number": crd_number,
                "individual_crd_number": person.get("crd_number") or person.get("crdNumber"),
                "first_name": person.get("first_name") or person.get("firstName"),
                "middle_name": person.get("middle_name") or person.get("middleName"),
                "last_name": person.get("last_name") or person.get("lastName"),
                "full_name": person.get("full_name") or person.get("fullName"),
                "title": person.get("title"),
                "position_type": person.get("position_type") or person.get("positionType"),
                "email": person.get("email"),
                "phone": person.get("phone"),
            }
            
            # Construct full name if not provided
            if not parsed["full_name"]:
                name_parts = [
                    parsed["first_name"],
                    parsed["middle_name"],
                    parsed["last_name"]
                ]
                parsed["full_name"] = " ".join([p for p in name_parts if p])
            
            parsed_personnel.append(parsed)
            
        except Exception as e:
            logger.warning(f"Error parsing personnel record: {e}")
            continue
    
    return parsed_personnel


def _parse_date(date_value: Any) -> Optional[date]:
    """Parse date from various formats."""
    if not date_value:
        return None
    
    if isinstance(date_value, date):
        return date_value
    
    if isinstance(date_value, datetime):
        return date_value.date()
    
    if isinstance(date_value, str):
        try:
            # Try ISO format first
            return datetime.fromisoformat(date_value.replace('Z', '+00:00')).date()
        except:
            try:
                # Try common US format
                return datetime.strptime(date_value, "%m/%d/%Y").date()
            except:
                logger.warning(f"Unable to parse date: {date_value}")
                return None
    
    return None


def _parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value."""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        try:
            # Remove commas and dollar signs
            clean = value.replace(",", "").replace("$", "").strip()
            return float(clean)
        except:
            return None
    
    return None


def _parse_int(value: Any) -> Optional[int]:
    """Parse integer value."""
    if value is None:
        return None
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, (float, str)):
        try:
            return int(float(value))
        except:
            return None
    
    return None


def _is_family_office(info: Dict[str, Any]) -> bool:
    """
    Determine if an adviser is a family office based on available data.
    
    Heuristics:
    - "family office" in firm name
    - Business type indicates family office
    - Registration exemption indicates family office
    """
    firm_name = (info.get("firm_name") or info.get("firmName") or "").lower()
    legal_name = (info.get("legal_name") or info.get("legalName") or "").lower()
    
    # Check name
    if "family office" in firm_name or "family office" in legal_name:
        return True
    
    # Check business type
    business_type = (info.get("business_type") or info.get("businessType") or "").lower()
    if "family office" in business_type:
        return True
    
    # Check exemption
    exemptions = info.get("exemptions", [])
    if isinstance(exemptions, list):
        for exemption in exemptions:
            if isinstance(exemption, str) and "family office" in exemption.lower():
                return True
    
    return False


# Known family offices (from user's list) - CRD numbers
KNOWN_FAMILY_OFFICES = {
    "soros_fund_management": None,  # May not be registered
    "cohen_private_ventures": None,  # May not be registered
    "msd_capital": None,  # May not be registered
    "cascade_investment": None,  # May not be registered
    "pritzker_group": "158626",  # Example - actual CRD would need lookup
}

