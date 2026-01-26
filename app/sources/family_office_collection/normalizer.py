"""
Family Office data normalizer and deduplicator.

Provides:
- Name normalization
- Duplicate detection
- Data merging from multiple sources
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set

from app.sources.family_office_collection.types import FoCollectedItem

logger = logging.getLogger(__name__)


class FoDataNormalizer:
    """
    Normalizes and deduplicates family office data.

    Handles:
    - Name variations
    - Duplicate detection across sources
    - Confidence-based merging
    """

    # Common suffixes to normalize
    COMPANY_SUFFIXES = [
        "LLC", "L.L.C.", "Inc.", "Inc", "Corporation", "Corp.",
        "LP", "L.P.", "LLP", "Holdings", "Group", "Partners",
        "Family Office", "Family Investments", "Capital",
    ]

    # Confidence ranking
    CONFIDENCE_ORDER = {"high": 3, "medium": 2, "low": 1}

    def normalize_name(self, name: str) -> str:
        """
        Normalize a company/person name for matching.

        Args:
            name: Raw name string

        Returns:
            Normalized name (lowercase, no punctuation)
        """
        if not name:
            return ""

        normalized = name.strip().lower()

        # Remove common suffixes
        for suffix in self.COMPANY_SUFFIXES:
            suffix_lower = suffix.lower()
            if normalized.endswith(suffix_lower):
                normalized = normalized[:-len(suffix_lower)].strip()
            if normalized.endswith(f", {suffix_lower}"):
                normalized = normalized[:-len(f", {suffix_lower}")].strip()

        # Remove punctuation except spaces
        normalized = re.sub(r"[^\w\s]", "", normalized)

        # Collapse multiple spaces
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def normalize_email(self, email: str) -> str:
        """Normalize an email address."""
        if not email:
            return ""
        return email.strip().lower()

    def normalize_phone(self, phone: str) -> str:
        """Normalize a phone number (digits only)."""
        if not phone:
            return ""
        return re.sub(r"[^\d]", "", phone)

    def deduplicate_items(
        self,
        items: List[FoCollectedItem],
    ) -> List[FoCollectedItem]:
        """
        Deduplicate collected items, keeping highest confidence.

        Args:
            items: List of collected items

        Returns:
            Deduplicated list of items
        """
        # Group by item type and key
        grouped: Dict[str, List[FoCollectedItem]] = {}

        for item in items:
            key = self._get_dedup_key(item)
            if key:
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(item)

        # Select best item from each group
        deduplicated = []

        for key, group in grouped.items():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                # Sort by confidence (highest first)
                group.sort(
                    key=lambda x: self.CONFIDENCE_ORDER.get(x.confidence, 0),
                    reverse=True
                )
                # Merge data from all items into best one
                best = group[0]
                for other in group[1:]:
                    best = self._merge_items(best, other)
                deduplicated.append(best)

        return deduplicated

    def _get_dedup_key(self, item: FoCollectedItem) -> Optional[str]:
        """
        Get deduplication key for an item.

        Returns:
            String key or None if item can't be deduped
        """
        item_type = item.item_type
        data = item.data

        if item_type == "team_member":
            name = self.normalize_name(data.get("full_name", ""))
            fo_id = data.get("fo_id", "")
            return f"team:{fo_id}:{name}" if name else None

        elif item_type == "portfolio_company":
            company = self.normalize_name(data.get("company_name", ""))
            fo_id = data.get("fo_id", "")
            return f"portfolio:{fo_id}:{company}" if company else None

        elif item_type == "contact_info":
            email = self.normalize_email(data.get("email", ""))
            if email:
                return f"contact:email:{email}"
            phone = self.normalize_phone(data.get("phone", ""))
            if phone:
                return f"contact:phone:{phone}"
            return None

        elif item_type == "news_item":
            # Dedupe by URL
            url = item.source_url
            return f"news:{url}" if url else None

        # Default: no deduplication
        return None

    def _merge_items(
        self,
        primary: FoCollectedItem,
        secondary: FoCollectedItem,
    ) -> FoCollectedItem:
        """
        Merge two items, preferring primary's values.

        Args:
            primary: Higher confidence item
            secondary: Lower confidence item

        Returns:
            Merged item
        """
        # Add non-null fields from secondary
        for key, value in secondary.data.items():
            if key not in primary.data or primary.data[key] is None:
                primary.data[key] = value

        # Keep track of multiple sources
        if secondary.source_url and secondary.source_url != primary.source_url:
            if "additional_sources" not in primary.data:
                primary.data["additional_sources"] = []
            primary.data["additional_sources"].append(secondary.source_url)

        return primary

    def filter_low_confidence(
        self,
        items: List[FoCollectedItem],
        min_confidence: str = "medium",
    ) -> List[FoCollectedItem]:
        """
        Filter out low-confidence items.

        Args:
            items: List of items
            min_confidence: Minimum confidence level ("low", "medium", "high")

        Returns:
            Filtered list
        """
        min_level = self.CONFIDENCE_ORDER.get(min_confidence, 0)

        return [
            item for item in items
            if self.CONFIDENCE_ORDER.get(item.confidence, 0) >= min_level
        ]

    def get_unique_companies(
        self,
        items: List[FoCollectedItem],
    ) -> Set[str]:
        """
        Get unique company names from portfolio items.

        Args:
            items: List of collected items

        Returns:
            Set of normalized company names
        """
        companies = set()

        for item in items:
            if item.item_type == "portfolio_company":
                company = item.data.get("company_name")
                if company:
                    companies.add(self.normalize_name(company))

        return companies

    def get_unique_contacts(
        self,
        items: List[FoCollectedItem],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get unique contacts organized by FO.

        Args:
            items: List of collected items

        Returns:
            Dict mapping FO names to list of contacts
        """
        contacts: Dict[str, List[Dict[str, Any]]] = {}

        for item in items:
            if item.item_type in ("team_member", "contact_info"):
                fo_name = item.data.get("fo_name", "unknown")

                if fo_name not in contacts:
                    contacts[fo_name] = []

                contacts[fo_name].append({
                    "name": item.data.get("full_name"),
                    "title": item.data.get("title"),
                    "email": item.data.get("email"),
                    "phone": item.data.get("phone"),
                    "role": item.data.get("role_category"),
                    "source": item.source_url,
                    "confidence": item.confidence,
                })

        return contacts
