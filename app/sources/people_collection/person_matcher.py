"""
Person Name Fuzzy Matcher for Deduplication.

Compares person names using Levenshtein similarity with smart normalization:
- Handles "Last, First" format
- Strips suffixes (Jr, Sr, III, PhD)
- Expands common nicknames (Bob→Robert, Bill→William)
- Compares first+last only (drops middle names)

Uses similarity_ratio from app.agentic.fuzzy_matcher (Levenshtein-based).
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional

from app.agentic.fuzzy_matcher import similarity_ratio

logger = logging.getLogger(__name__)

# Common nickname → canonical name mappings
NICKNAME_MAP = {
    "bob": "robert",
    "bobby": "robert",
    "rob": "robert",
    "robbie": "robert",
    "bill": "william",
    "billy": "william",
    "will": "william",
    "willy": "william",
    "jim": "james",
    "jimmy": "james",
    "jamie": "james",
    "mike": "michael",
    "mikey": "michael",
    "dick": "richard",
    "rick": "richard",
    "rich": "richard",
    "ricky": "richard",
    "tom": "thomas",
    "tommy": "thomas",
    "dan": "daniel",
    "danny": "daniel",
    "dave": "david",
    "davy": "david",
    "joe": "joseph",
    "joey": "joseph",
    "steve": "steven",
    "stevie": "steven",
    "stephen": "steven",
    "chris": "christopher",
    "pat": "patrick",
    "paddy": "patrick",
    "ed": "edward",
    "eddie": "edward",
    "ted": "edward",
    "teddy": "edward",
    "tony": "anthony",
    "matt": "matthew",
    "matty": "matthew",
    "al": "albert",
    "alex": "alexander",
    "andy": "andrew",
    "drew": "andrew",
    "ben": "benjamin",
    "benny": "benjamin",
    "chuck": "charles",
    "charlie": "charles",
    "charley": "charles",
    "fred": "frederick",
    "freddy": "frederick",
    "greg": "gregory",
    "harry": "harold",
    "hank": "henry",
    "jack": "john",
    "johnny": "john",
    "jon": "john",
    "jerry": "gerald",
    "larry": "lawrence",
    "liz": "elizabeth",
    "beth": "elizabeth",
    "betty": "elizabeth",
    "lizzy": "elizabeth",
    "kate": "katherine",
    "kathy": "katherine",
    "cathy": "katherine",
    "katie": "katherine",
    "peg": "margaret",
    "peggy": "margaret",
    "maggie": "margaret",
    "meg": "margaret",
    "sue": "susan",
    "susie": "susan",
    "jen": "jennifer",
    "jenny": "jennifer",
    "deb": "deborah",
    "debbie": "deborah",
    "barb": "barbara",
    "barbie": "barbara",
    "sam": "samuel",
    "sammy": "samuel",
    "nick": "nicholas",
    "nicky": "nicholas",
    "phil": "philip",
    "pete": "peter",
    "ray": "raymond",
    "ron": "ronald",
    "ronnie": "ronald",
    "walt": "walter",
    "wally": "walter",
    "ken": "kenneth",
    "kenny": "kenneth",
    "doug": "douglas",
    "don": "donald",
    "donnie": "donald",
}

# Suffixes to strip
SUFFIX_PATTERN = re.compile(
    r",?\s+(jr|sr|ii|iii|iv|v|phd|md|esq|cpa|cfa|jd|mba|dds|dvm|pe|rn)\.?$",
    re.IGNORECASE,
)


@dataclass
class PersonMatchResult:
    """Result of comparing two person names."""

    matched: bool
    similarity: float  # 0.0 to 1.0
    match_type: str  # "name_exact", "name_fuzzy", "nickname_match", "no_match"
    notes: Optional[str] = None


class PersonNameMatcher:
    """
    Fuzzy person name matcher for deduplication.

    Classifies pairs as auto-merge (≥0.95 + shared company),
    review (≥0.80), or no-match (<0.80).
    """

    def __init__(
        self,
        auto_merge_threshold: float = 0.95,
        review_threshold: float = 0.80,
    ):
        self.auto_merge_threshold = auto_merge_threshold
        self.review_threshold = review_threshold

    def normalize_name(self, name: str) -> str:
        """
        Normalize a person name for comparison.

        - Lowercase
        - Handle "Last, First" format
        - Strip suffixes (Jr/Sr/III/PhD)
        - Remove punctuation
        - Collapse whitespace
        """
        if not name:
            return ""

        name = name.strip().lower()

        # Strip suffixes first
        name = SUFFIX_PATTERN.sub("", name)

        # Handle "Last, First" or "Last, First Middle" format
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2 and parts[1]:
                name = f"{parts[1]} {parts[0]}"

        # Remove remaining punctuation (periods, hyphens kept for now)
        name = re.sub(r"[^\w\s\-]", "", name)

        # Collapse whitespace
        name = re.sub(r"\s+", " ", name).strip()

        return name

    def _extract_first_last(self, normalized_name: str) -> tuple:
        """Extract first and last name, dropping middle names."""
        parts = normalized_name.split()
        if not parts:
            return ("", "")
        if len(parts) == 1:
            return (parts[0], "")
        return (parts[0], parts[-1])

    def _expand_nickname(self, first_name: str) -> str:
        """Expand a nickname to its canonical form."""
        return NICKNAME_MAP.get(first_name, first_name)

    def compare(self, name1: str, name2: str) -> PersonMatchResult:
        """
        Compare two person names and return match result.

        Performs multi-level comparison:
        1. Exact normalized match
        2. First+last only match (drop middle names)
        3. Nickname expansion match
        4. Fuzzy similarity on first+last
        """
        norm1 = self.normalize_name(name1)
        norm2 = self.normalize_name(name2)

        if not norm1 or not norm2:
            return PersonMatchResult(
                matched=False,
                similarity=0.0,
                match_type="no_match",
                notes="Empty name",
            )

        # Exact match after normalization
        if norm1 == norm2:
            return PersonMatchResult(
                matched=True,
                similarity=1.0,
                match_type="name_exact",
            )

        # Extract first + last (drop middle names)
        first1, last1 = self._extract_first_last(norm1)
        first2, last2 = self._extract_first_last(norm2)

        # First+last exact match
        if first1 == first2 and last1 == last2:
            return PersonMatchResult(
                matched=True,
                similarity=1.0,
                match_type="name_exact",
                notes="Exact match after dropping middle names",
            )

        # Nickname expansion
        canonical1 = self._expand_nickname(first1)
        canonical2 = self._expand_nickname(first2)

        if canonical1 == canonical2 and last1 == last2:
            return PersonMatchResult(
                matched=True,
                similarity=0.95,
                match_type="nickname_match",
                notes=f"Nickname match: {first1}={canonical1}, {first2}={canonical2}",
            )

        # Fuzzy comparison on first+last
        fl1 = f"{first1} {last1}".strip()
        fl2 = f"{first2} {last2}".strip()
        similarity = similarity_ratio(fl1, fl2)

        # Also try with nickname expansion
        cfl1 = f"{canonical1} {last1}".strip()
        cfl2 = f"{canonical2} {last2}".strip()
        nickname_similarity = similarity_ratio(cfl1, cfl2)

        best_similarity = max(similarity, nickname_similarity)
        is_nickname = nickname_similarity > similarity

        matched = best_similarity >= self.review_threshold

        if is_nickname and matched:
            match_type = (
                "nickname_match"
                if best_similarity >= self.auto_merge_threshold
                else "name_fuzzy"
            )
        else:
            match_type = "name_fuzzy" if matched else "no_match"

        return PersonMatchResult(
            matched=matched,
            similarity=round(best_similarity, 3),
            match_type=match_type if matched else "no_match",
            notes=f"Fuzzy: {best_similarity:.3f}"
            + (" (nickname-expanded)" if is_nickname else ""),
        )

    def classify_match(
        self,
        similarity: float,
        shared_company: bool,
    ) -> str:
        """
        Classify a match as auto_merge, review, or no_match.

        Auto-merge requires ≥0.95 similarity AND a shared company.
        Review requires ≥0.80 similarity.
        """
        if similarity >= self.auto_merge_threshold and shared_company:
            return "auto_merge"
        elif similarity >= self.review_threshold:
            return "review"
        else:
            return "no_match"
