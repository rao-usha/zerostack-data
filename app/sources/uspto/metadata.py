"""
USPTO PatentsView metadata and field definitions.

Contains field mappings, CPC code descriptions, and validation utilities.
"""
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

# Default fields to request from PatentsView API
DEFAULT_PATENT_FIELDS = [
    "patent_id",
    "patent_title",
    "patent_abstract",
    "patent_date",
    "patent_type",
    "patent_num_claims",
    "patent_num_cited_by_us_patents",
    "inventors.inventor_id",
    "inventors.inventor_name_first",
    "inventors.inventor_name_last",
    "inventors.inventor_city",
    "inventors.inventor_state",
    "inventors.inventor_country",
    "assignees.assignee_id",
    "assignees.assignee_organization",
    "assignees.assignee_type",
    "assignees.assignee_city",
    "assignees.assignee_state",
    "assignees.assignee_country",
    "cpc_current.cpc_section_id",
    "cpc_current.cpc_class_id",
    "cpc_current.cpc_subclass_id",
    "cpc_current.cpc_group_id",
]

DEFAULT_INVENTOR_FIELDS = [
    "inventor_id",
    "inventor_name_first",
    "inventor_name_last",
    "inventor_city",
    "inventor_state",
    "inventor_country",
    "inventor_total_num_patents",
    "inventor_first_seen_date",
    "inventor_last_seen_date",
]

DEFAULT_ASSIGNEE_FIELDS = [
    "assignee_id",
    "assignee_organization",
    "assignee_type",
    "assignee_city",
    "assignee_state",
    "assignee_country",
    "assignee_total_num_patents",
    "assignee_first_seen_date",
    "assignee_last_seen_date",
]

# Assignee type mappings from PatentsView
ASSIGNEE_TYPES = {
    "1": "Unassigned",
    "2": "US Company",
    "3": "Foreign Company",
    "4": "US Individual",
    "5": "Foreign Individual",
    "6": "US Federal Government",
    "7": "Foreign Government",
    "8": "US County Government",
    "9": "US State Government",
}

# Patent type mappings
PATENT_TYPES = {
    "utility": "Utility Patent",
    "design": "Design Patent",
    "plant": "Plant Patent",
    "reissue": "Reissue Patent",
    "defensive publication": "Defensive Publication",
    "statutory invention registration": "Statutory Invention Registration",
}

# CPC Section descriptions
CPC_SECTIONS = {
    "A": "Human Necessities",
    "B": "Performing Operations; Transporting",
    "C": "Chemistry; Metallurgy",
    "D": "Textiles; Paper",
    "E": "Fixed Constructions",
    "F": "Mechanical Engineering; Lighting; Heating; Weapons; Blasting",
    "G": "Physics",
    "H": "Electricity",
    "Y": "General Tagging of New Technological Developments",
}

# Common CPC classes with descriptions
CPC_CLASSES = {
    "G06": "Computing; Calculating; Counting",
    "G06F": "Electric Digital Data Processing",
    "G06N": "Computing Arrangements Based on Specific Computational Models",
    "G06Q": "Data Processing Systems for Administrative, Commercial, Financial Purposes",
    "G06V": "Image or Video Recognition or Understanding",
    "H01": "Electric Elements",
    "H01L": "Semiconductor Devices",
    "H04": "Electric Communication Technique",
    "H04L": "Transmission of Digital Information",
    "H04W": "Wireless Communication Networks",
    "A61": "Medical or Veterinary Science; Hygiene",
    "A61K": "Preparations for Medical, Dental or Toilet Purposes",
    "A61B": "Diagnosis; Surgery; Identification",
    "C12": "Biochemistry; Microbiology; Enzymology",
    "C12N": "Microorganisms or Enzymes",
    "B60": "Vehicles in General",
    "B60W": "Conjoint Control of Vehicle Sub-Units",
    "Y02": "Climate Change Mitigation Technologies",
    "Y02E": "Reduction of Greenhouse Gas Emissions Related to Energy",
}


def validate_date_format(date_str: str) -> bool:
    """
    Validate date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    if not date_str:
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_patent_id(patent_id: str) -> bool:
    """
    Validate patent ID format.

    Patent IDs are typically 7-8 digit numbers.

    Args:
        patent_id: Patent ID to validate

    Returns:
        True if valid format
    """
    if not patent_id:
        return False
    # Patent IDs are numeric, 5-10 digits
    return bool(re.match(r"^\d{5,10}$", patent_id))


def normalize_patent_id(patent_id: str) -> str:
    """
    Normalize patent ID (remove leading zeros, etc).

    Args:
        patent_id: Patent ID to normalize

    Returns:
        Normalized patent ID
    """
    # Remove any non-numeric characters and leading zeros
    return str(int(re.sub(r"[^\d]", "", patent_id)))


def get_cpc_section_description(cpc_code: str) -> Optional[str]:
    """
    Get description for a CPC section.

    Args:
        cpc_code: CPC code (e.g., "G06N")

    Returns:
        Section description or None
    """
    if not cpc_code:
        return None
    section = cpc_code[0].upper()
    return CPC_SECTIONS.get(section)


def get_cpc_class_description(cpc_code: str) -> Optional[str]:
    """
    Get description for a CPC class.

    Args:
        cpc_code: CPC code (e.g., "G06N")

    Returns:
        Class description or None
    """
    if not cpc_code or len(cpc_code) < 3:
        return None

    # Try exact match first (e.g., "G06N")
    if cpc_code in CPC_CLASSES:
        return CPC_CLASSES[cpc_code]

    # Try class level (e.g., "G06" from "G06N3")
    class_code = cpc_code[:3]
    if class_code in CPC_CLASSES:
        return CPC_CLASSES[class_code]

    return None


def get_assignee_type_description(type_code: str) -> str:
    """
    Get human-readable assignee type.

    Args:
        type_code: Assignee type code

    Returns:
        Type description
    """
    return ASSIGNEE_TYPES.get(str(type_code), "Unknown")


def get_patent_type_description(patent_type: str) -> str:
    """
    Get human-readable patent type.

    Args:
        patent_type: Patent type string

    Returns:
        Type description
    """
    if not patent_type:
        return "Unknown"
    return PATENT_TYPES.get(patent_type.lower(), patent_type.title())


def extract_patent_summary(patent_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a summary from full patent data.

    Args:
        patent_data: Full patent record from API

    Returns:
        Simplified patent summary
    """
    # Extract first inventor and assignee
    inventors = patent_data.get("inventors", [])
    first_inventor = None
    if inventors:
        inv = inventors[0]
        first_inventor = f"{inv.get('inventor_name_first', '')} {inv.get('inventor_name_last', '')}".strip()

    assignees = patent_data.get("assignees", [])
    first_assignee = None
    if assignees:
        first_assignee = assignees[0].get("assignee_organization")

    # Extract primary CPC
    cpc_codes = patent_data.get("cpc_current", [])
    primary_cpc = None
    if cpc_codes:
        primary_cpc = cpc_codes[0].get("cpc_group_id")

    return {
        "patent_id": patent_data.get("patent_id"),
        "title": patent_data.get("patent_title"),
        "date": patent_data.get("patent_date"),
        "type": get_patent_type_description(patent_data.get("patent_type")),
        "claims": patent_data.get("patent_num_claims"),
        "citations": patent_data.get("patent_num_cited_by_us_patents", 0),
        "inventor": first_inventor,
        "assignee": first_assignee,
        "cpc_code": primary_cpc,
        "cpc_description": get_cpc_class_description(primary_cpc) if primary_cpc else None,
    }


def build_patent_search_summary(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a summary of search results.

    Args:
        response: API response from patent search

    Returns:
        Search summary with stats
    """
    patents = response.get("patents", [])
    total_hits = response.get("total_hits", len(patents))

    # Count assignee types
    assignee_counts: Dict[str, int] = {}
    cpc_counts: Dict[str, int] = {}

    for patent in patents:
        for assignee in patent.get("assignees", []):
            org = assignee.get("assignee_organization", "Unknown")
            assignee_counts[org] = assignee_counts.get(org, 0) + 1

        for cpc in patent.get("cpc_current", []):
            section = cpc.get("cpc_section_id")
            if section:
                desc = CPC_SECTIONS.get(section, section)
                cpc_counts[desc] = cpc_counts.get(desc, 0) + 1

    # Sort and get top items
    top_assignees = sorted(assignee_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    top_cpc = sorted(cpc_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "total_results": total_hits,
        "returned_count": len(patents),
        "top_assignees": [{"name": k, "count": v} for k, v in top_assignees],
        "cpc_distribution": [{"category": k, "count": v} for k, v in top_cpc],
    }
