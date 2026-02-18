"""
Leadership change detection by comparing snapshots.

Detects changes by comparing:
- Current extracted leadership vs database records
- Leadership across time (snapshot comparison)
- Missing/new people in leadership pages
"""

import logging
from typing import Optional, List, Dict, Set, Tuple
from datetime import date, datetime
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    ExtractionConfidence,
    ChangeType,
    TitleLevel,
)

logger = logging.getLogger(__name__)


@dataclass
class PersonSnapshot:
    """Snapshot of a person at a point in time."""

    full_name: str
    title: str
    title_level: TitleLevel = TitleLevel.UNKNOWN
    is_board_member: bool = False
    is_executive: bool = True
    snapshot_date: date = field(default_factory=date.today)


@dataclass
class ChangeDetectionResult:
    """Result of comparing leadership snapshots."""

    new_people: List[ExtractedPerson] = field(default_factory=list)
    departed_people: List[PersonSnapshot] = field(default_factory=list)
    title_changes: List[Tuple[PersonSnapshot, ExtractedPerson]] = field(
        default_factory=list
    )
    detected_changes: List[LeadershipChange] = field(default_factory=list)


class ChangeDetector:
    """
    Detects leadership changes by comparing data.

    Strategies:
    1. Compare extracted people vs existing database records
    2. Compare two snapshots in time
    3. Infer change types from differences
    """

    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize change detector.

        Args:
            similarity_threshold: Minimum name similarity for matching (0-1)
        """
        self.similarity_threshold = similarity_threshold

    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        if not name:
            return ""
        # Lowercase, remove punctuation, collapse spaces
        name = name.lower()
        name = "".join(c for c in name if c.isalnum() or c.isspace())
        return " ".join(name.split())

    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two names."""
        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)

        if n1 == n2:
            return 1.0

        # Use sequence matcher for fuzzy matching
        return SequenceMatcher(None, n1, n2).ratio()

    def _find_match(
        self,
        person: ExtractedPerson,
        candidates: List[PersonSnapshot],
    ) -> Optional[PersonSnapshot]:
        """Find matching person in candidates list."""
        best_match = None
        best_score = 0

        for candidate in candidates:
            score = self._name_similarity(person.full_name, candidate.full_name)
            if score > best_score and score >= self.similarity_threshold:
                best_score = score
                best_match = candidate

        return best_match

    def _find_match_in_extracted(
        self,
        snapshot: PersonSnapshot,
        people: List[ExtractedPerson],
    ) -> Optional[ExtractedPerson]:
        """Find matching person in extracted list."""
        best_match = None
        best_score = 0

        for person in people:
            score = self._name_similarity(snapshot.full_name, person.full_name)
            if score > best_score and score >= self.similarity_threshold:
                best_score = score
                best_match = person

        return best_match

    def _normalize_title(self, title: str) -> str:
        """Normalize title for comparison."""
        if not title:
            return ""
        # Lowercase and normalize common variations
        title = title.lower().strip()
        # Normalize common abbreviations
        replacements = {
            "chief executive officer": "ceo",
            "chief financial officer": "cfo",
            "chief operating officer": "coo",
            "chief technology officer": "cto",
            "vice president": "vp",
            "senior vice president": "svp",
            "executive vice president": "evp",
        }
        for full, abbrev in replacements.items():
            title = title.replace(full, abbrev)
        return title

    def _titles_different(self, title1: str, title2: str) -> bool:
        """Check if two titles are meaningfully different."""
        t1 = self._normalize_title(title1)
        t2 = self._normalize_title(title2)

        if t1 == t2:
            return False

        # Check if one contains the other (e.g., "CEO" vs "CEO and President")
        if t1 in t2 or t2 in t1:
            return False

        return True

    def _infer_change_type(
        self,
        old: PersonSnapshot,
        new: ExtractedPerson,
    ) -> ChangeType:
        """Infer the type of change from old/new comparison."""
        old_title = self._normalize_title(old.title)
        new_title = self._normalize_title(new.title)

        # Check for seniority changes
        seniority_order = [
            "ceo",
            "president",
            "evp",
            "svp",
            "vp",
            "director",
            "manager",
        ]

        old_level = 99
        new_level = 99
        for i, level in enumerate(seniority_order):
            if level in old_title:
                old_level = min(old_level, i)
            if level in new_title:
                new_level = min(new_level, i)

        if new_level < old_level:
            return ChangeType.PROMOTION
        elif new_level > old_level:
            return ChangeType.DEMOTION
        else:
            return ChangeType.LATERAL

    def compare_with_existing(
        self,
        extracted_people: List[ExtractedPerson],
        existing_snapshots: List[PersonSnapshot],
        company_name: str,
    ) -> ChangeDetectionResult:
        """
        Compare extracted people with existing database records.

        Args:
            extracted_people: Newly extracted people
            existing_snapshots: Current leadership from database
            company_name: Company name for change records

        Returns:
            ChangeDetectionResult with detected changes
        """
        result = ChangeDetectionResult()

        matched_existing: Set[int] = set()
        matched_extracted: Set[int] = set()

        # Find matches and title changes
        for i, extracted in enumerate(extracted_people):
            match = self._find_match(extracted, existing_snapshots)

            if match:
                match_idx = existing_snapshots.index(match)
                matched_existing.add(match_idx)
                matched_extracted.add(i)

                # Check for title change
                if self._titles_different(match.title, extracted.title):
                    result.title_changes.append((match, extracted))

                    # Create change record
                    change_type = self._infer_change_type(match, extracted)
                    change = LeadershipChange(
                        person_name=extracted.full_name,
                        change_type=change_type,
                        old_title=match.title,
                        new_title=extracted.title,
                        announced_date=date.today(),
                        source_type="website_change",
                        confidence=ExtractionConfidence.MEDIUM,
                        is_c_suite=extracted.title_level == TitleLevel.C_SUITE,
                        is_board=extracted.is_board_member,
                    )
                    result.detected_changes.append(change)

        # Find new people (in extracted but not in existing)
        for i, extracted in enumerate(extracted_people):
            if i not in matched_extracted:
                result.new_people.append(extracted)

                # Create hire change record
                change = LeadershipChange(
                    person_name=extracted.full_name,
                    change_type=ChangeType.HIRE,
                    new_title=extracted.title,
                    announced_date=date.today(),
                    source_type="website_change",
                    confidence=ExtractionConfidence.MEDIUM,
                    is_c_suite=extracted.title_level == TitleLevel.C_SUITE,
                    is_board=extracted.is_board_member,
                )
                result.detected_changes.append(change)

        # Find departed people (in existing but not in extracted)
        for i, existing in enumerate(existing_snapshots):
            if i not in matched_existing:
                result.departed_people.append(existing)

                # Create departure change record
                change = LeadershipChange(
                    person_name=existing.full_name,
                    change_type=ChangeType.DEPARTURE,
                    old_title=existing.title,
                    announced_date=date.today(),
                    source_type="website_change",
                    confidence=ExtractionConfidence.LOW,  # Less confident about departures
                    is_c_suite=existing.title_level == TitleLevel.C_SUITE,
                    is_board=existing.is_board_member,
                )
                result.detected_changes.append(change)

        logger.info(
            f"Change detection for {company_name}: "
            f"{len(result.new_people)} new, "
            f"{len(result.departed_people)} departed, "
            f"{len(result.title_changes)} title changes"
        )

        return result

    def compare_snapshots(
        self,
        old_snapshot: List[PersonSnapshot],
        new_snapshot: List[PersonSnapshot],
        company_name: str,
    ) -> ChangeDetectionResult:
        """
        Compare two leadership snapshots in time.

        Args:
            old_snapshot: Previous leadership snapshot
            new_snapshot: Current leadership snapshot
            company_name: Company name

        Returns:
            ChangeDetectionResult with changes between snapshots
        """
        # Convert new snapshot to ExtractedPerson format for reuse
        extracted = [
            ExtractedPerson(
                full_name=p.full_name,
                title=p.title,
                title_level=p.title_level,
                is_board_member=p.is_board_member,
                is_executive=p.is_executive,
            )
            for p in new_snapshot
        ]

        return self.compare_with_existing(extracted, old_snapshot, company_name)

    def filter_significant_changes(
        self,
        changes: List[LeadershipChange],
        min_significance: int = 5,
    ) -> List[LeadershipChange]:
        """
        Filter to only significant leadership changes.

        Args:
            changes: List of detected changes
            min_significance: Minimum significance score (1-10)

        Returns:
            Filtered list of significant changes
        """
        significant = []

        for change in changes:
            score = self._calculate_significance(change)
            change.significance_score = score

            if score >= min_significance:
                significant.append(change)

        return significant

    def _calculate_significance(self, change: LeadershipChange) -> int:
        """Calculate significance score for a change (1-10)."""
        score = 5  # Base score

        # C-suite changes are most significant
        if change.is_c_suite:
            score += 3

        # Board changes are significant
        if change.is_board:
            score += 2

        # CEO changes are highest
        if change.new_title and "ceo" in change.new_title.lower():
            score += 2
        if change.old_title and "ceo" in change.old_title.lower():
            score += 2

        # Departures slightly less significant (might be false positive)
        if change.change_type == ChangeType.DEPARTURE:
            score -= 1

        # High confidence increases score
        if change.confidence == ExtractionConfidence.HIGH:
            score += 1

        return min(max(score, 1), 10)


def detect_leadership_changes(
    extracted_people: List[ExtractedPerson],
    existing_people: List[Dict],
    company_name: str,
) -> List[LeadershipChange]:
    """
    Convenience function to detect changes.

    Args:
        extracted_people: Newly extracted people
        existing_people: Existing records as dicts with name, title, etc.
        company_name: Company name

    Returns:
        List of detected LeadershipChange objects
    """
    detector = ChangeDetector()

    # Convert existing to snapshots
    snapshots = [
        PersonSnapshot(
            full_name=p.get("full_name", p.get("name", "")),
            title=p.get("title", ""),
            title_level=TitleLevel(p.get("title_level", "unknown")),
            is_board_member=p.get("is_board_member", False),
            is_executive=p.get("is_executive", True),
        )
        for p in existing_people
    ]

    result = detector.compare_with_existing(extracted_people, snapshots, company_name)
    return detector.filter_significant_changes(result.detected_changes)
