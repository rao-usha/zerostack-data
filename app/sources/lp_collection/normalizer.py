"""
Data normalizer for LP collection.

Handles:
- Deduplication of collected items
- Data merging across sources
- Confidence scoring
- Data validation
"""

import logging
from typing import List, Dict, Any, Optional, Set, Tuple

from app.sources.lp_collection.types import CollectedItem, CollectionResult

logger = logging.getLogger(__name__)


# Confidence level ordering (higher is better)
CONFIDENCE_ORDER = {"high": 3, "medium": 2, "low": 1}


class DataNormalizer:
    """
    Normalizes and deduplicates collected LP data.

    Handles merging data from multiple sources with different
    confidence levels and data quality.
    """

    def __init__(self):
        """Initialize the normalizer."""
        self._seen_contacts: Dict[str, CollectedItem] = {}
        self._seen_documents: Set[str] = set()
        self._seen_news: Set[str] = set()

    def reset(self) -> None:
        """Reset state for a new normalization session."""
        self._seen_contacts = {}
        self._seen_documents = set()
        self._seen_news = set()

    def normalize_results(
        self,
        results: List[CollectionResult],
    ) -> List[CollectedItem]:
        """
        Normalize and deduplicate items from multiple collection results.

        Args:
            results: List of CollectionResult from different sources

        Returns:
            List of deduplicated and normalized CollectedItem
        """
        self.reset()
        all_items: List[CollectedItem] = []

        # Sort results by source confidence (SEC > CAFR > Website > News)
        source_priority = {
            "sec_adv": 4,
            "cafr": 3,
            "website": 2,
            "news": 1,
        }

        sorted_results = sorted(
            results, key=lambda r: source_priority.get(r.source.value, 0), reverse=True
        )

        for result in sorted_results:
            if not result.success:
                continue

            for item in result.items:
                normalized_item = self._normalize_item(item)
                if normalized_item:
                    all_items.append(normalized_item)

        return all_items

    def _normalize_item(self, item: CollectedItem) -> Optional[CollectedItem]:
        """
        Normalize a single item, handling deduplication.

        Returns:
            Normalized item, or None if duplicate
        """
        if item.item_type == "contact":
            return self._normalize_contact(item)
        elif item.item_type == "document_link":
            return self._normalize_document(item)
        elif item.item_type == "news":
            return self._normalize_news(item)
        else:
            # Pass through other types
            return item

    def _normalize_contact(self, item: CollectedItem) -> Optional[CollectedItem]:
        """Normalize and deduplicate contact items."""
        data = item.data
        name = data.get("full_name", "").strip().lower()
        lp_id = data.get("lp_id")

        if not name or not lp_id:
            return None

        # Create dedup key
        key = f"{lp_id}:{name}"

        existing = self._seen_contacts.get(key)
        if existing:
            # Merge data from higher confidence source
            merged = self._merge_contact_data(existing, item)
            self._seen_contacts[key] = merged
            return None  # Don't add duplicate

        # Clean the data
        cleaned_data = self._clean_contact_data(data)
        item.data = cleaned_data

        self._seen_contacts[key] = item
        return item

    def _merge_contact_data(
        self,
        existing: CollectedItem,
        new: CollectedItem,
    ) -> CollectedItem:
        """Merge contact data, preferring higher confidence values."""
        existing_conf = CONFIDENCE_ORDER.get(existing.confidence, 1)
        new_conf = CONFIDENCE_ORDER.get(new.confidence, 1)

        merged_data = existing.data.copy()

        # Merge fields from new if higher confidence or missing
        for field in ["email", "phone", "title", "role_category", "linkedin_url"]:
            new_value = new.data.get(field)
            existing_value = merged_data.get(field)

            if new_value and (not existing_value or new_conf > existing_conf):
                merged_data[field] = new_value

        # Update confidence to highest
        if new_conf > existing_conf:
            existing.confidence = new.confidence

        existing.data = merged_data
        return existing

    def _clean_contact_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize contact data."""
        cleaned = data.copy()

        # Normalize name
        if "full_name" in cleaned:
            name = cleaned["full_name"]
            # Title case
            cleaned["full_name"] = " ".join(word.capitalize() for word in name.split())

        # Normalize email
        if "email" in cleaned and cleaned["email"]:
            cleaned["email"] = cleaned["email"].lower().strip()

        # Normalize phone
        if "phone" in cleaned and cleaned["phone"]:
            phone = cleaned["phone"]
            # Remove non-digit characters for storage, keep formatted for display
            digits = "".join(c for c in phone if c.isdigit())
            if len(digits) == 10:
                cleaned["phone"] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

        # Normalize role category
        if "role_category" in cleaned and cleaned["role_category"]:
            cleaned["role_category"] = cleaned["role_category"].strip()

        return cleaned

    def _normalize_document(self, item: CollectedItem) -> Optional[CollectedItem]:
        """Normalize and deduplicate document items."""
        url = item.data.get("url", "").strip().lower()

        if not url or url in self._seen_documents:
            return None

        self._seen_documents.add(url)

        # Clean URL
        item.data["url"] = url.strip()

        return item

    def _normalize_news(self, item: CollectedItem) -> Optional[CollectedItem]:
        """Normalize and deduplicate news items."""
        title = item.data.get("title", "").strip().lower()
        url = item.data.get("url", "").strip().lower()

        # Dedupe by URL or title
        key = url or title
        if not key or key in self._seen_news:
            return None

        self._seen_news.add(key)

        return item

    def calculate_confidence_score(self, item: CollectedItem) -> float:
        """
        Calculate a numeric confidence score for an item.

        Args:
            item: The collected item

        Returns:
            Confidence score from 0.0 to 1.0
        """
        base_score = {
            "high": 0.9,
            "medium": 0.6,
            "low": 0.3,
        }.get(item.confidence, 0.5)

        # Source adjustments
        source_type = item.data.get("source_type", "")
        source_bonus = {
            "sec_adv": 0.1,
            "cafr": 0.05,
            "website": 0.0,
            "news": -0.05,
        }.get(source_type, 0.0)

        # Data completeness adjustments
        completeness_bonus = 0.0
        if item.item_type == "contact":
            required_fields = ["full_name", "title", "email"]
            filled = sum(1 for f in required_fields if item.data.get(f))
            completeness_bonus = (filled / len(required_fields)) * 0.1

        return min(1.0, max(0.0, base_score + source_bonus + completeness_bonus))

    def get_deduplication_stats(self) -> Dict[str, int]:
        """Get statistics on deduplication."""
        return {
            "unique_contacts": len(self._seen_contacts),
            "unique_documents": len(self._seen_documents),
            "unique_news": len(self._seen_news),
        }


def normalize_collection_results(
    results: List[CollectionResult],
) -> Tuple[List[CollectedItem], Dict[str, int]]:
    """
    Convenience function to normalize collection results.

    Args:
        results: List of CollectionResult

    Returns:
        Tuple of (normalized items, dedup stats)
    """
    normalizer = DataNormalizer()
    items = normalizer.normalize_results(results)
    stats = normalizer.get_deduplication_stats()
    return items, stats
