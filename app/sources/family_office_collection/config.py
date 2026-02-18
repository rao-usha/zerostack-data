"""
Family Office registry configuration and loader.

Loads the expanded family office registry from JSON and provides
access to FO metadata for collection.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from app.sources.family_office_collection.types import FoRegistryEntry

logger = logging.getLogger(__name__)

# Path to expanded FO registry
FO_REGISTRY_PATH = (
    Path(__file__).parent.parent.parent
    / "data"
    / "expanded_family_office_registry.json"
)

# Cached registry
_fo_registry: Optional[Dict[str, Any]] = None


def load_fo_registry() -> Dict[str, Any]:
    """
    Load the family office registry from JSON.

    Returns:
        Dictionary with registry metadata and family_offices list
    """
    global _fo_registry

    if _fo_registry is not None:
        return _fo_registry

    if not FO_REGISTRY_PATH.exists():
        logger.warning(f"FO registry not found at {FO_REGISTRY_PATH}")
        return {"family_offices": [], "fo_count": 0}

    try:
        with open(FO_REGISTRY_PATH, "r", encoding="utf-8") as f:
            _fo_registry = json.load(f)
        logger.info(
            f"Loaded FO registry with {_fo_registry.get('fo_count', 0)} family offices"
        )
        return _fo_registry
    except Exception as e:
        logger.error(f"Error loading FO registry: {e}")
        return {"family_offices": [], "fo_count": 0}


def get_fo_registry() -> List[FoRegistryEntry]:
    """
    Get list of family office entries from the registry.

    Returns:
        List of FoRegistryEntry objects
    """
    registry = load_fo_registry()
    entries = []

    for fo_data in registry.get("family_offices", []):
        try:
            entries.append(FoRegistryEntry.from_dict(fo_data))
        except Exception as e:
            logger.warning(f"Error parsing FO entry: {e}")
            continue

    return entries


def get_fo_by_name(name: str) -> Optional[FoRegistryEntry]:
    """
    Find a family office by name.

    Args:
        name: Family office name to search for

    Returns:
        FoRegistryEntry if found, None otherwise
    """
    entries = get_fo_registry()
    name_lower = name.lower()

    for entry in entries:
        if entry.name.lower() == name_lower:
            return entry

    return None


def get_fos_by_region(region: str) -> List[FoRegistryEntry]:
    """
    Get family offices by region.

    Args:
        region: Region code (us, europe, asia, middle_east, etc.)

    Returns:
        List of FoRegistryEntry objects in that region
    """
    entries = get_fo_registry()
    return [e for e in entries if e.region == region]


def get_fos_by_type(fo_type: str) -> List[FoRegistryEntry]:
    """
    Get family offices by type.

    Args:
        fo_type: FO type (single_family, multi_family)

    Returns:
        List of FoRegistryEntry objects of that type
    """
    entries = get_fo_registry()
    return [e for e in entries if e.fo_type == fo_type]


def get_fos_by_priority(max_priority: int = 3) -> List[FoRegistryEntry]:
    """
    Get high-priority family offices.

    Args:
        max_priority: Maximum priority level (1 = highest)

    Returns:
        List of FoRegistryEntry objects with priority <= max_priority
    """
    entries = get_fo_registry()
    return [e for e in entries if e.collection_priority <= max_priority]


def get_registry_stats() -> Dict[str, Any]:
    """
    Get statistics about the FO registry.

    Returns:
        Dictionary with counts by type, region, etc.
    """
    registry = load_fo_registry()
    entries = get_fo_registry()

    # Count by type
    by_type = {}
    for entry in entries:
        by_type[entry.fo_type] = by_type.get(entry.fo_type, 0) + 1

    # Count by region
    by_region = {}
    for entry in entries:
        by_region[entry.region] = by_region.get(entry.region, 0) + 1

    # Count by priority
    by_priority = {}
    for entry in entries:
        p = entry.collection_priority
        by_priority[p] = by_priority.get(p, 0) + 1

    return {
        "total_count": len(entries),
        "by_type": by_type,
        "by_region": by_region,
        "by_priority": by_priority,
        "version": registry.get("version"),
        "last_updated": registry.get("last_updated"),
    }


# Known investment vehicle names mapped to family offices
KNOWN_FO_VEHICLES = {
    "Cascade Investment": "Gates",
    "Bezos Expeditions": "Bezos",
    "Walton Enterprises": "Walton",
    "Soros Fund Management": "Soros",
    "Emerson Collective": "Powell Jobs",
    "MSD Capital": "Dell",
    "Thiel Capital": "Thiel",
    "Point72": "Cohen",
    "Bridgewater Associates": "Dalio",
    "Citadel": "Griffin",
    "Renaissance Technologies": "Simons",
    "Duquesne Family Office": "Druckenmiller",
    "Tiger Global": "Coleman",
    "Coatue Management": "Laffont",
    "Maverick Capital": "Ainslie",
    "Pershing Square": "Ackman",
    "Greenlight Capital": "Einhorn",
    "Third Point": "Loeb",
    "Elliott Management": "Singer",
    "Baupost Group": "Klarman",
}
