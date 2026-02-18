"""
Fuzzy string matching utilities for company name deduplication.

Uses Levenshtein distance to find similar company names that may
refer to the same entity despite minor differences in spelling,
punctuation, or suffixes.

Example matches:
- "Apple Inc" vs "Apple, Inc."
- "Microsoft Corporation" vs "Microsoft Corp"
- "Berkshire Hathaway" vs "Berkshire Hathaway Inc"
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of a fuzzy match operation."""

    matched: bool
    similarity: float
    normalized_name1: str
    normalized_name2: str


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate the Levenshtein (edit) distance between two strings.

    The Levenshtein distance is the minimum number of single-character
    edits (insertions, deletions, substitutions) needed to transform
    one string into another.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Edit distance (0 = identical)
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)

    for i, c1 in enumerate(s1):
        current_row = [i + 1]

        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)

            current_row.append(min(insertions, deletions, substitutions))

        previous_row = current_row

    return previous_row[-1]


def similarity_ratio(s1: str, s2: str) -> float:
    """
    Calculate similarity ratio between two strings.

    Returns a value between 0.0 and 1.0, where 1.0 means identical
    and 0.0 means completely different.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Similarity ratio (0.0 to 1.0)
    """
    if not s1 and not s2:
        return 1.0

    if not s1 or not s2:
        return 0.0

    distance = levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))

    return 1.0 - (distance / max_len)


class CompanyNameMatcher:
    """
    Fuzzy matcher specialized for company names.

    Handles common variations in company names:
    - Suffixes: Inc, LLC, Corp, Ltd, etc.
    - Punctuation differences
    - Case differences
    - Common abbreviations
    """

    # Company suffixes to remove during normalization
    COMPANY_SUFFIXES = [
        r",?\s*(inc\.?|incorporated)$",
        r",?\s*(llc|l\.l\.c\.)$",
        r",?\s*(ltd\.?|limited)$",
        r",?\s*(corp\.?|corporation)$",
        r",?\s*(co\.?|company)$",
        r",?\s*(plc|p\.l\.c\.)$",
        r",?\s*(s\.a\.?|sa)$",
        r",?\s*(n\.v\.?|nv)$",
        r",?\s*(ag)$",
        r",?\s*(gmbh)$",
        r",?\s*(lp|l\.p\.)$",
        r",?\s*(llp|l\.l\.p\.)$",
        r",?\s*(pllc)$",
        r",?\s*(the)$",
        r"^(the)\s+",
    ]

    # Common abbreviations to expand
    ABBREVIATIONS = {
        "intl": "international",
        "int'l": "international",
        "corp": "corporation",
        "assoc": "associates",
        "mgmt": "management",
        "svcs": "services",
        "tech": "technology",
        "sys": "systems",
        "grp": "group",
        "hldgs": "holdings",
        "invt": "investment",
        "invts": "investments",
        "ptnrs": "partners",
        "ptr": "partners",
    }

    def __init__(
        self,
        similarity_threshold: float = 0.85,
        use_abbreviation_expansion: bool = True,
    ):
        """
        Initialize the matcher.

        Args:
            similarity_threshold: Minimum similarity for a match (0.0-1.0)
            use_abbreviation_expansion: Whether to expand common abbreviations
        """
        self.similarity_threshold = similarity_threshold
        self.use_abbreviation_expansion = use_abbreviation_expansion
        self._normalization_cache: Dict[str, str] = {}

    def normalize(self, name: str) -> str:
        """
        Normalize a company name for comparison.

        Steps:
        1. Convert to lowercase
        2. Remove company suffixes
        3. Expand abbreviations (optional)
        4. Remove punctuation
        5. Normalize whitespace

        Args:
            name: Company name to normalize

        Returns:
            Normalized company name
        """
        if not name:
            return ""

        # Check cache
        if name in self._normalization_cache:
            return self._normalization_cache[name]

        normalized = name.lower().strip()

        # Remove company suffixes
        for suffix_pattern in self.COMPANY_SUFFIXES:
            normalized = re.sub(suffix_pattern, "", normalized, flags=re.IGNORECASE)

        # Expand abbreviations
        if self.use_abbreviation_expansion:
            words = normalized.split()
            expanded_words = []
            for word in words:
                word_clean = re.sub(r"[^\w]", "", word)
                if word_clean in self.ABBREVIATIONS:
                    expanded_words.append(self.ABBREVIATIONS[word_clean])
                else:
                    expanded_words.append(word)
            normalized = " ".join(expanded_words)

        # Remove punctuation except spaces
        normalized = re.sub(r"[^\w\s]", "", normalized)

        # Normalize whitespace
        normalized = " ".join(normalized.split())

        # Cache result
        self._normalization_cache[name] = normalized

        return normalized

    def match(self, name1: str, name2: str) -> MatchResult:
        """
        Check if two company names match.

        Args:
            name1: First company name
            name2: Second company name

        Returns:
            MatchResult with match status and similarity score
        """
        norm1 = self.normalize(name1)
        norm2 = self.normalize(name2)

        # Exact match after normalization
        if norm1 == norm2:
            return MatchResult(
                matched=True,
                similarity=1.0,
                normalized_name1=norm1,
                normalized_name2=norm2,
            )

        # Calculate similarity
        similarity = similarity_ratio(norm1, norm2)

        return MatchResult(
            matched=similarity >= self.similarity_threshold,
            similarity=similarity,
            normalized_name1=norm1,
            normalized_name2=norm2,
        )

    def is_match(self, name1: str, name2: str) -> bool:
        """
        Quick check if two company names match.

        Args:
            name1: First company name
            name2: Second company name

        Returns:
            True if names match above threshold
        """
        return self.match(name1, name2).matched

    def find_matches(
        self, name: str, candidates: List[str], top_n: Optional[int] = None
    ) -> List[Tuple[str, float]]:
        """
        Find matching company names from a list of candidates.

        Args:
            name: Company name to match
            candidates: List of candidate names
            top_n: Return only top N matches (None = all matches)

        Returns:
            List of (candidate_name, similarity_score) tuples, sorted by similarity
        """
        norm_name = self.normalize(name)
        matches = []

        for candidate in candidates:
            norm_candidate = self.normalize(candidate)

            if not norm_candidate:
                continue

            similarity = similarity_ratio(norm_name, norm_candidate)

            if similarity >= self.similarity_threshold:
                matches.append((candidate, similarity))

        # Sort by similarity (descending)
        matches.sort(key=lambda x: x[1], reverse=True)

        if top_n:
            matches = matches[:top_n]

        return matches

    def deduplicate_batch(
        self,
        records: List[Dict[str, Any]],
        name_field: str = "company_name",
        merge_func: Optional[callable] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate a batch of records using fuzzy matching.

        Args:
            records: List of records with company names
            name_field: Field containing the company name
            merge_func: Optional function to merge two records
                       Signature: merge_func(record1, record2) -> merged_record
                       If None, keeps the first record

        Returns:
            Deduplicated list of records
        """
        if not records:
            return []

        # Group records by normalized name
        groups: Dict[str, List[Dict[str, Any]]] = {}
        group_keys: List[str] = []  # Track order

        for record in records:
            name = record.get(name_field, "")
            if not name:
                continue

            norm_name = self.normalize(name)

            # Check if this matches any existing group
            matched_group = None
            for existing_key in group_keys:
                if (
                    similarity_ratio(norm_name, existing_key)
                    >= self.similarity_threshold
                ):
                    matched_group = existing_key
                    break

            if matched_group:
                groups[matched_group].append(record)
            else:
                groups[norm_name] = [record]
                group_keys.append(norm_name)

        # Merge records in each group
        result = []
        for key in group_keys:
            group_records = groups[key]

            if len(group_records) == 1:
                result.append(group_records[0])
            else:
                # Merge multiple records
                if merge_func:
                    merged = group_records[0]
                    for other in group_records[1:]:
                        merged = merge_func(merged, other)
                    # Track that this was fuzzy-matched
                    merged["_fuzzy_matched"] = True
                    merged["_matched_count"] = len(group_records)
                    result.append(merged)
                else:
                    # Just keep first record
                    group_records[0]["_fuzzy_matched"] = True
                    group_records[0]["_matched_count"] = len(group_records)
                    result.append(group_records[0])

        logger.info(
            f"Fuzzy deduplication: {len(records)} records -> {len(result)} unique "
            f"(merged {len(records) - len(result)} duplicates)"
        )

        return result


# Default matcher instance
_default_matcher = CompanyNameMatcher()


def get_default_matcher() -> CompanyNameMatcher:
    """Get the default company name matcher."""
    return _default_matcher


def fuzzy_match(name1: str, name2: str, threshold: float = 0.85) -> bool:
    """
    Convenience function to check if two company names match.

    Args:
        name1: First company name
        name2: Second company name
        threshold: Similarity threshold (0.0-1.0)

    Returns:
        True if names match
    """
    matcher = CompanyNameMatcher(similarity_threshold=threshold)
    return matcher.is_match(name1, name2)
