"""
Press release parser for leadership announcements.

Parses press releases from various sources to extract leadership changes:
- Company newsrooms
- PR Newswire / Business Wire
- SEC 8-K filings (cross-referenced)
"""

import re
import logging
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass, field

from bs4 import BeautifulSoup

from app.sources.people_collection.types import (
    LeadershipChange,
    ExtractionConfidence,
    ChangeType,
)
from app.sources.people_collection.llm_extractor import LLMExtractor

logger = logging.getLogger(__name__)


@dataclass
class PressRelease:
    """Represents a press release."""
    title: str
    content: str
    publish_date: Optional[date]
    source_url: str
    company_name: str
    source_type: str = "press_release"  # press_release, newsroom, pr_wire


@dataclass
class PressReleaseParseResult:
    """Result of parsing a press release."""
    changes: List[LeadershipChange] = field(default_factory=list)
    is_leadership_related: bool = False
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    key_phrases: List[str] = field(default_factory=list)


class PressReleaseParser:
    """
    Parses press releases to extract leadership changes.

    Uses a combination of:
    1. Keyword detection to identify leadership-related releases
    2. Pattern matching for common announcement formats
    3. LLM extraction for complex cases
    """

    # Keywords indicating leadership announcement
    LEADERSHIP_KEYWORDS = [
        # Appointments
        "appoints", "appointed", "names", "named", "promotes", "promoted",
        "elevates", "elevated", "announces appointment", "new ceo",
        "new chief", "new president", "joins as", "hire", "hired",

        # Departures
        "resigns", "resigned", "resignation", "retires", "retired",
        "retirement", "steps down", "stepping down", "departs",
        "departure", "leaves", "leaving", "transition",

        # Board changes
        "board of directors", "elected to board", "joins board",
        "board appointment", "director", "chairman",

        # Succession
        "succession", "successor", "succeeds", "effective immediately",
        "effective date",
    ]

    # Title keywords for classification
    EXECUTIVE_TITLES = [
        "ceo", "cfo", "coo", "cto", "cio", "cmo", "cro", "chro",
        "chief executive", "chief financial", "chief operating",
        "chief technology", "chief information", "chief marketing",
        "president", "vice president", "vp", "evp", "svp",
        "general counsel", "general manager", "managing director",
    ]

    BOARD_TITLES = [
        "board", "director", "chairman", "chairwoman", "chair",
        "trustee", "independent director", "non-executive",
    ]

    def __init__(self):
        self.llm_extractor = LLMExtractor()

    def _clean_html(self, html: str) -> str:
        """Remove HTML tags and clean text."""
        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style
        for element in soup.find_all(['script', 'style', 'noscript']):
            element.decompose()

        text = soup.get_text(separator='\n')
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        return text.strip()

    def is_leadership_related(self, title: str, content: str = "") -> bool:
        """
        Quick check if a press release is likely about leadership.

        Args:
            title: Press release title/headline
            content: Optional content text

        Returns:
            True if likely leadership-related
        """
        text = f"{title} {content[:1000]}".lower()

        # Count keyword matches
        matches = sum(1 for kw in self.LEADERSHIP_KEYWORDS if kw in text)

        # Also check for executive titles
        title_matches = sum(1 for t in self.EXECUTIVE_TITLES if t in text)
        board_matches = sum(1 for t in self.BOARD_TITLES if t in text)

        # Need at least 2 leadership keywords OR 1 keyword + title mention
        return matches >= 2 or (matches >= 1 and (title_matches >= 1 or board_matches >= 1))

    def _extract_key_phrases(self, text: str) -> List[str]:
        """Extract key phrases that indicate the type of announcement."""
        phrases = []
        text_lower = text.lower()

        phrase_patterns = [
            (r'appoint\w*\s+\w+\s+\w+\s+as', 'appointment'),
            (r'nam\w*\s+\w+\s+\w+\s+as', 'appointment'),
            (r'promot\w*\s+to', 'promotion'),
            (r'resign\w*', 'resignation'),
            (r'retir\w*', 'retirement'),
            (r'step\w*\s+down', 'departure'),
            (r'succession\s+plan', 'succession'),
            (r'effective\s+immediately', 'immediate'),
            (r'effective\s+\w+\s+\d+', 'dated'),
        ]

        for pattern, label in phrase_patterns:
            if re.search(pattern, text_lower):
                phrases.append(label)

        return list(set(phrases))

    async def parse(
        self,
        press_release: PressRelease,
    ) -> PressReleaseParseResult:
        """
        Parse a press release to extract leadership changes.

        Args:
            press_release: PressRelease object to parse

        Returns:
            PressReleaseParseResult with extracted changes
        """
        result = PressReleaseParseResult()

        # Clean content if HTML
        content = press_release.content
        if '<html' in content.lower() or '<body' in content.lower():
            content = self._clean_html(content)

        # Quick relevance check
        full_text = f"{press_release.title}\n{content}"
        result.is_leadership_related = self.is_leadership_related(
            press_release.title, content
        )

        if not result.is_leadership_related:
            result.extraction_confidence = ExtractionConfidence.LOW
            return result

        # Extract key phrases
        result.key_phrases = self._extract_key_phrases(full_text)

        # Try pattern-based extraction first
        pattern_changes = self._extract_with_patterns(
            full_text,
            press_release.company_name,
            press_release.publish_date,
            press_release.source_url,
        )

        # Use LLM for more complete extraction
        llm_changes = await self._extract_with_llm(
            content,
            press_release.company_name,
            press_release.publish_date,
        )

        # Merge results (prefer LLM but include unique pattern matches)
        result.changes = self._merge_changes(llm_changes, pattern_changes)

        # Add source info
        for change in result.changes:
            change.source_url = press_release.source_url
            change.source_type = press_release.source_type
            change.source_headline = press_release.title

        # Set confidence based on extraction quality
        if result.changes:
            result.extraction_confidence = ExtractionConfidence.HIGH
        elif result.is_leadership_related:
            result.extraction_confidence = ExtractionConfidence.MEDIUM
        else:
            result.extraction_confidence = ExtractionConfidence.LOW

        return result

    def _extract_with_patterns(
        self,
        text: str,
        company_name: str,
        publish_date: Optional[date],
        source_url: str,
    ) -> List[LeadershipChange]:
        """Extract changes using regex patterns."""
        changes = []

        # Pattern: "Company appoints/names NAME as TITLE"
        appointment_patterns = [
            r'(?:appoints?|names?|hires?)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:as\s+)?(?:its\s+)?(?:new\s+)?([A-Z][^.]{5,60})',
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)\s+(?:has been|is)\s+(?:appointed|named|promoted)\s+(?:as\s+)?(?:the\s+)?([A-Z][^.]{5,60})',
        ]

        for pattern in appointment_patterns:
            matches = re.findall(pattern, text)
            for match in matches[:5]:
                name, title = match[0].strip(), match[1].strip()
                if self._is_valid_name(name) and len(title) > 3:
                    change = LeadershipChange(
                        person_name=name,
                        change_type=ChangeType.HIRE,
                        new_title=self._clean_title(title),
                        announced_date=publish_date,
                        source_type="press_release",
                        confidence=ExtractionConfidence.MEDIUM,
                        is_c_suite=self._is_c_suite(title),
                        is_board=self._is_board_role(title),
                    )
                    changes.append(change)

        # Pattern: "NAME resigns/retires from TITLE"
        departure_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)\s+(?:has\s+)?(?:resigned?|retires?|step(?:s|ped)\s+down)\s+(?:as\s+|from\s+)?(?:the\s+)?([A-Z][^.]{5,60})?',
            r'(?:resignation|retirement|departure)\s+of\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)',
        ]

        for pattern in departure_patterns:
            matches = re.findall(pattern, text)
            for match in matches[:5]:
                if isinstance(match, tuple):
                    name = match[0].strip()
                    title = match[1].strip() if len(match) > 1 and match[1] else ""
                else:
                    name = match.strip()
                    title = ""

                if self._is_valid_name(name):
                    # Determine if retirement or resignation
                    change_type = ChangeType.RETIREMENT if 'retire' in text.lower() else ChangeType.DEPARTURE

                    change = LeadershipChange(
                        person_name=name,
                        change_type=change_type,
                        old_title=self._clean_title(title) if title else None,
                        announced_date=publish_date,
                        source_type="press_release",
                        confidence=ExtractionConfidence.MEDIUM,
                    )
                    changes.append(change)

        # Pattern: "NAME promoted to TITLE"
        promotion_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)\s+(?:has been\s+)?promoted\s+to\s+([A-Z][^.]{5,60})',
        ]

        for pattern in promotion_patterns:
            matches = re.findall(pattern, text)
            for match in matches[:5]:
                name, title = match[0].strip(), match[1].strip()
                if self._is_valid_name(name):
                    change = LeadershipChange(
                        person_name=name,
                        change_type=ChangeType.PROMOTION,
                        new_title=self._clean_title(title),
                        announced_date=publish_date,
                        source_type="press_release",
                        confidence=ExtractionConfidence.MEDIUM,
                        is_c_suite=self._is_c_suite(title),
                    )
                    changes.append(change)

        return changes

    async def _extract_with_llm(
        self,
        content: str,
        company_name: str,
        publish_date: Optional[date],
    ) -> List[LeadershipChange]:
        """Extract changes using LLM."""
        try:
            date_str = publish_date.isoformat() if publish_date else None
            return await self.llm_extractor.extract_changes_from_press_release(
                content[:50000],
                company_name,
                date_str,
            )
        except Exception as e:
            logger.warning(f"LLM extraction failed: {e}")
            return []

    def _merge_changes(
        self,
        llm_changes: List[LeadershipChange],
        pattern_changes: List[LeadershipChange],
    ) -> List[LeadershipChange]:
        """Merge LLM and pattern-extracted changes."""
        # Use LLM as base (usually more accurate)
        merged = list(llm_changes)

        # Add pattern matches not found by LLM
        llm_names = {self._normalize_name(c.person_name) for c in llm_changes}

        for change in pattern_changes:
            if self._normalize_name(change.person_name) not in llm_names:
                merged.append(change)

        return merged

    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        return ' '.join(name.lower().split())

    def _is_valid_name(self, name: str) -> bool:
        """Validate potential person name."""
        if not name or len(name) < 4:
            return False

        words = name.split()
        if len(words) < 2 or len(words) > 5:
            return False

        # Should be mostly letters
        letters = sum(1 for c in name if c.isalpha())
        if letters < len(name) * 0.7:
            return False

        # Avoid common false positives
        false_positives = ['the company', 'board of', 'effective']
        if any(fp in name.lower() for fp in false_positives):
            return False

        return True

    def _clean_title(self, title: str) -> str:
        """Clean and normalize title."""
        # Remove trailing noise
        noise = [' and will', ' effective', ' of the company', ' at the company']
        for n in noise:
            if n in title.lower():
                idx = title.lower().find(n)
                title = title[:idx]

        return title.strip().rstrip(',').strip()

    def _is_c_suite(self, title: str) -> bool:
        """Check if title is C-suite level."""
        title_lower = title.lower()
        c_suite = ['chief', 'ceo', 'cfo', 'coo', 'cto', 'cio', 'cmo', 'cro', 'chro', 'president']
        return any(t in title_lower for t in c_suite)

    def _is_board_role(self, title: str) -> bool:
        """Check if title is a board role."""
        title_lower = title.lower()
        return any(t in title_lower for t in self.BOARD_TITLES)


async def parse_press_release(
    title: str,
    content: str,
    company_name: str,
    source_url: str,
    publish_date: Optional[date] = None,
) -> PressReleaseParseResult:
    """
    Convenience function to parse a press release.

    Args:
        title: Press release headline
        content: Full text content
        company_name: Company name
        source_url: URL of the press release
        publish_date: Publication date

    Returns:
        PressReleaseParseResult with extracted changes
    """
    parser = PressReleaseParser()
    pr = PressRelease(
        title=title,
        content=content,
        publish_date=publish_date,
        source_url=source_url,
        company_name=company_name,
    )
    return await parser.parse(pr)
