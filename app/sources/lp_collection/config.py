"""
Configuration and LP registry loader for the collection system.

Loads the expanded LP registry and provides filtering utilities.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from app.sources.lp_collection.types import LpRegistryEntry

logger = logging.getLogger(__name__)

# Path to the expanded LP registry
LP_REGISTRY_PATH = (
    Path(__file__).parent.parent.parent / "data" / "expanded_lp_registry.json"
)

# Valid LP types
VALID_LP_TYPES = [
    "public_pension",
    "sovereign_wealth",
    "endowment",
    "corporate_pension",
    "insurance",
]

# Valid regions
VALID_REGIONS = [
    "us",
    "europe",
    "asia",
    "middle_east",
    "oceania",
]

# Default collection schedules
DEFAULT_SCHEDULES = {
    "website_weekly": {
        "name": "Website Weekly Crawl",
        "source_type": "website",
        "frequency": "weekly",
        "day_of_week": 1,  # Tuesday
        "hour": 2,
        "description": "Weekly website crawl for all LPs",
    },
    "sec_adv_monthly": {
        "name": "SEC Form ADV Monthly",
        "source_type": "sec_adv",
        "frequency": "monthly",
        "day_of_month": 15,
        "hour": 3,
        "description": "Monthly SEC Form ADV collection for US pensions/endowments",
    },
    "cafr_quarterly": {
        "name": "CAFR Quarterly Collection",
        "source_type": "cafr",
        "frequency": "quarterly",
        "day_of_month": 1,
        "hour": 4,
        "description": "Quarterly CAFR collection for public pensions",
    },
    "news_daily": {
        "name": "News Daily Monitor",
        "source_type": "news",
        "frequency": "daily",
        "hour": 6,
        "description": "Daily news monitoring for high-priority LPs",
    },
}


class LpRegistry:
    """
    Manages the expanded LP registry.

    Provides methods to load, filter, and query LP data.
    """

    _instance: Optional["LpRegistry"] = None
    _registry: List[LpRegistryEntry] = []
    _registry_by_name: Dict[str, LpRegistryEntry] = {}

    def __new__(cls) -> "LpRegistry":
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_registry()
        return cls._instance

    def _load_registry(self) -> None:
        """Load the LP registry from JSON file."""
        try:
            with open(LP_REGISTRY_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            self._registry = [
                LpRegistryEntry.from_dict(lp) for lp in data.get("lps", [])
            ]
            self._registry_by_name = {lp.name: lp for lp in self._registry}

            logger.info(f"Loaded {len(self._registry)} LPs from registry")

        except FileNotFoundError:
            logger.warning(f"LP registry not found at {LP_REGISTRY_PATH}")
            self._registry = []
            self._registry_by_name = {}
        except Exception as e:
            logger.error(f"Error loading LP registry: {e}")
            self._registry = []
            self._registry_by_name = {}

    def reload(self) -> None:
        """Reload the registry from disk."""
        self._load_registry()

    @property
    def all_lps(self) -> List[LpRegistryEntry]:
        """Get all LPs in the registry."""
        return self._registry.copy()

    @property
    def lp_count(self) -> int:
        """Get total number of LPs."""
        return len(self._registry)

    def get_lp(self, name: str) -> Optional[LpRegistryEntry]:
        """Get an LP by name."""
        return self._registry_by_name.get(name)

    def filter_lps(
        self,
        lp_types: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        country_codes: Optional[List[str]] = None,
        has_cafr: Optional[bool] = None,
        min_priority: Optional[int] = None,
        max_priority: Optional[int] = None,
    ) -> List[LpRegistryEntry]:
        """
        Filter LPs by various criteria.

        Args:
            lp_types: Filter by LP types
            regions: Filter by regions
            country_codes: Filter by country codes
            has_cafr: Filter by CAFR availability
            min_priority: Minimum collection priority (1=highest)
            max_priority: Maximum collection priority (10=lowest)

        Returns:
            List of matching LPs
        """
        result = self._registry.copy()

        if lp_types:
            result = [lp for lp in result if lp.lp_type in lp_types]

        if regions:
            result = [lp for lp in result if lp.region in regions]

        if country_codes:
            result = [lp for lp in result if lp.country_code in country_codes]

        if has_cafr is not None:
            result = [lp for lp in result if lp.has_cafr == has_cafr]

        if min_priority is not None:
            result = [lp for lp in result if lp.collection_priority >= min_priority]

        if max_priority is not None:
            result = [lp for lp in result if lp.collection_priority <= max_priority]

        return result

    def get_high_priority_lps(self, max_priority: int = 2) -> List[LpRegistryEntry]:
        """Get high-priority LPs (for news monitoring, etc.)."""
        return self.filter_lps(max_priority=max_priority)

    def get_us_public_pensions(self) -> List[LpRegistryEntry]:
        """Get all US public pensions."""
        return self.filter_lps(lp_types=["public_pension"], regions=["us"])

    def get_sovereign_wealth_funds(self) -> List[LpRegistryEntry]:
        """Get all sovereign wealth funds."""
        return self.filter_lps(lp_types=["sovereign_wealth"])

    def get_endowments(self) -> List[LpRegistryEntry]:
        """Get all university endowments."""
        return self.filter_lps(lp_types=["endowment"])

    def get_lps_with_cafr(self) -> List[LpRegistryEntry]:
        """Get LPs that publish CAFRs."""
        return self.filter_lps(has_cafr=True)

    def get_by_region(self, region: str) -> List[LpRegistryEntry]:
        """Get all LPs in a region."""
        return self.filter_lps(regions=[region])

    def get_coverage_stats(self) -> Dict[str, Any]:
        """Get coverage statistics for the registry."""
        by_type = {}
        by_region = {}
        by_priority = {}

        for lp in self._registry:
            # By type
            by_type[lp.lp_type] = by_type.get(lp.lp_type, 0) + 1

            # By region
            by_region[lp.region] = by_region.get(lp.region, 0) + 1

            # By priority
            priority_key = f"priority_{lp.collection_priority}"
            by_priority[priority_key] = by_priority.get(priority_key, 0) + 1

        return {
            "total_lps": self.lp_count,
            "by_type": by_type,
            "by_region": by_region,
            "by_priority": by_priority,
            "with_cafr": len(self.get_lps_with_cafr()),
        }


# Global registry instance
_registry: Optional[LpRegistry] = None


def get_lp_registry() -> LpRegistry:
    """Get the global LP registry instance."""
    global _registry
    if _registry is None:
        _registry = LpRegistry()
    return _registry


def get_lp_by_name(name: str) -> Optional[LpRegistryEntry]:
    """Convenience function to get LP by name."""
    return get_lp_registry().get_lp(name)


def filter_lps(**kwargs) -> List[LpRegistryEntry]:
    """Convenience function to filter LPs."""
    return get_lp_registry().filter_lps(**kwargs)
