"""
Website collection agent for people/leadership data.

Crawls company websites to extract leadership team information:
1. Discovers leadership pages
2. Extracts people data using LLM
3. Deduplicates and validates results
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.page_finder import PageFinder
from app.sources.people_collection.html_cleaner import HTMLCleaner, extract_people_cards
from app.sources.people_collection.llm_extractor import LLMExtractor
from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipPageResult,
    CollectionResult,
    ExtractionConfidence,
    TitleLevel,
)
from app.sources.people_collection.config import COLLECTION_SETTINGS

logger = logging.getLogger(__name__)


class WebsiteAgent(BaseCollector):
    """
    Agent for collecting leadership data from company websites.

    Process:
    1. Find leadership/team pages on the website
    2. Fetch and clean HTML content
    3. Extract people using LLM
    4. Validate and deduplicate results
    """

    def __init__(self):
        super().__init__(source_type="website")
        self.page_finder = PageFinder()
        self.html_cleaner = HTMLCleaner(max_length=COLLECTION_SETTINGS.html_max_length)
        self.llm_extractor = LLMExtractor()

    async def close(self) -> None:
        """Close all resources."""
        await super().close()
        await self.page_finder.close()

    async def collect(
        self,
        company_id: int,
        company_name: str,
        website_url: str,
        max_pages: int = None,
    ) -> CollectionResult:
        """
        Collect leadership data from a company website.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            website_url: Company website URL
            max_pages: Max pages to crawl (default from config)

        Returns:
            CollectionResult with extracted people
        """
        max_pages = max_pages or COLLECTION_SETTINGS.max_pages_per_company
        started_at = datetime.utcnow()

        result = CollectionResult(
            company_id=company_id,
            company_name=company_name,
            source="website",
            started_at=started_at,
        )

        # Track page URLs for diagnostics
        result.page_urls = []

        try:
            # Step 1: Find leadership pages
            logger.info(f"[WebsiteAgent] Finding leadership pages for {company_name} at {website_url}")
            pages = await self.page_finder.find_leadership_pages(website_url, max_pages)

            if not pages:
                result.warnings.append(f"No leadership pages found at {website_url}")
                logger.warning(
                    f"[WebsiteAgent] No leadership pages found for {company_name}. "
                    f"Tried URL patterns but none returned 200 OK. "
                    f"Website may have non-standard structure or be JavaScript-rendered."
                )
                return self._finalize_result(result)

            logger.info(f"[WebsiteAgent] Found {len(pages)} potential leadership pages for {company_name}")
            for page in pages:
                logger.debug(f"[WebsiteAgent] Page: {page['url']} (type={page.get('page_type')}, score={page.get('score')})")
                result.page_urls.append(page['url'])

            # Step 2: Extract people from each page
            all_people: List[ExtractedPerson] = []
            page_results: List[LeadershipPageResult] = []

            for page_info in pages:
                page_url = page_info['url']
                page_type = page_info.get('page_type', 'unknown')

                logger.info(f"[WebsiteAgent] Extracting from page: {page_url}")

                try:
                    page_result = await self._extract_from_page(
                        page_url, company_name, page_type
                    )
                    page_results.append(page_result)
                    all_people.extend(page_result.people)

                    logger.info(
                        f"[WebsiteAgent] Extracted {len(page_result.people)} people from {page_url} "
                        f"(confidence={page_result.extraction_confidence})"
                    )
                    if page_result.extraction_notes:
                        logger.debug(f"[WebsiteAgent] Notes: {page_result.extraction_notes}")

                except Exception as e:
                    logger.warning(f"[WebsiteAgent] Error extracting from {page_url}: {e}")
                    result.warnings.append(f"Failed to extract from {page_url}: {str(e)}")

            # Step 3: Deduplicate people
            logger.info(f"[WebsiteAgent] Deduplicating {len(all_people)} raw extractions")
            unique_people = self._deduplicate_people(all_people)
            logger.info(f"[WebsiteAgent] After dedup: {len(unique_people)} unique people")

            # Step 4: Validate and filter
            valid_people = self._validate_people(unique_people, company_name)
            filtered_count = len(unique_people) - len(valid_people)
            if filtered_count > 0:
                logger.info(f"[WebsiteAgent] Validation filtered out {filtered_count} people")

            # Update result
            result.extracted_people = valid_people
            result.people_found = len(valid_people)
            result.pages_checked = len(pages)
            result.success = len(valid_people) > 0

            if not valid_people:
                if len(all_people) == 0:
                    result.warnings.append(
                        "LLM extraction returned 0 people - page may be JS-rendered or format not recognized"
                    )
                elif len(unique_people) == 0:
                    result.warnings.append(
                        f"All {len(all_people)} extracted people were duplicates"
                    )
                else:
                    result.warnings.append(
                        f"All {len(unique_people)} people failed validation (name/title checks)"
                    )

            logger.info(
                f"[WebsiteAgent] Final result for {company_name}: "
                f"{len(valid_people)} valid people "
                f"(raw={len(all_people)}, deduped={len(unique_people)}, pages={len(pages)})"
            )

        except Exception as e:
            logger.error(f"Error collecting from {company_name}: {e}")
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _extract_from_page(
        self,
        page_url: str,
        company_name: str,
        page_type: str,
    ) -> LeadershipPageResult:
        """Extract leadership data from a single page."""
        logger.debug(f"[WebsiteAgent] Extracting from {page_url}")

        # Fetch page
        html = await self.fetch_url(page_url)
        if not html:
            logger.warning(f"[WebsiteAgent] Failed to fetch page: {page_url}")
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type=page_type,
                people=[],
                extraction_confidence=ExtractionConfidence.LOW,
                extraction_notes="Failed to fetch page - may be blocked, timeout, or 404",
            )

        logger.debug(f"[WebsiteAgent] Fetched {len(html)} bytes from {page_url}")

        # Clean HTML
        cleaned = self.html_cleaner.clean(html)
        logger.debug(
            f"[WebsiteAgent] Cleaned HTML: {len(cleaned.text)} chars, "
            f"has_leadership_content={cleaned.has_leadership_content}"
        )

        if not cleaned.has_leadership_content:
            logger.debug(f"[WebsiteAgent] No leadership keywords detected at {page_url}")
            # Still try extraction in case detection missed something

        # Try structured extraction first (faster, no LLM cost)
        structured_people = self._extract_structured(html, page_url, company_name)
        logger.debug(f"[WebsiteAgent] Structured extraction found {len(structured_people)} people")

        # If structured extraction found people, use those
        if len(structured_people) >= 3:
            logger.info(
                f"[WebsiteAgent] Using structured extraction for {page_url}: "
                f"{len(structured_people)} people (skipping LLM)"
            )
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type=page_type,
                people=structured_people,
                extraction_confidence=ExtractionConfidence.MEDIUM,
                extraction_notes="Structured extraction from HTML cards/patterns",
            )

        # Fall back to LLM extraction - but only if we have meaningful content
        min_text_length = 200  # Skip LLM if page content is too short (likely JS-rendered)
        if len(cleaned.text.strip()) < min_text_length:
            logger.warning(
                f"[WebsiteAgent] Skipping LLM for {page_url} - content too short "
                f"({len(cleaned.text.strip())} chars < {min_text_length}). "
                f"Page likely requires JavaScript rendering."
            )
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type=page_type,
                people=structured_people,
                extraction_confidence=ExtractionConfidence.LOW,
                extraction_notes=f"Content too short for LLM ({len(cleaned.text.strip())} chars) - likely JS-rendered",
            )

        logger.info(f"[WebsiteAgent] Using LLM extraction for {page_url}")
        result = await self.llm_extractor.extract_leadership_from_html(
            cleaned.text, company_name, page_url
        )

        logger.info(
            f"[WebsiteAgent] LLM extraction result: {len(result.people)} people, "
            f"confidence={result.extraction_confidence}"
        )

        if len(result.people) == 0:
            logger.warning(
                f"[WebsiteAgent] LLM returned 0 people for {page_url}. "
                f"This could indicate: JS-rendered content, unusual HTML structure, "
                f"or page doesn't actually contain leadership info. "
                f"Cleaned text length: {len(cleaned.text)} chars"
            )

        # Merge with structured results (LLM might find more)
        if structured_people:
            result.people = self._merge_people_lists(result.people, structured_people)
            logger.debug(f"[WebsiteAgent] After merge: {len(result.people)} people")

        return result

    def _extract_structured(
        self,
        html: str,
        page_url: str,
        company_name: str,
    ) -> List[ExtractedPerson]:
        """Extract people using HTML structure (no LLM)."""
        people = []

        try:
            cards = extract_people_cards(html)

            for card in cards:
                name = card.get('name', '')
                title = card.get('title', '')

                if not name or not title:
                    continue

                # Validate it looks like a real person
                if not self._is_valid_name(name):
                    continue

                person = ExtractedPerson(
                    full_name=name,
                    title=title,
                    bio=card.get('bio'),
                    linkedin_url=card.get('linkedin_url'),
                    photo_url=card.get('image_url'),
                    source_url=page_url,
                    confidence=ExtractionConfidence.MEDIUM,
                    is_executive=True,
                )

                # Try to parse name
                name_parts = self._parse_name(name)
                person.first_name = name_parts.get('first_name')
                person.last_name = name_parts.get('last_name')
                person.suffix = name_parts.get('suffix')

                # Infer title level
                person.title_level = self._infer_title_level(title)

                people.append(person)

        except Exception as e:
            logger.debug(f"Structured extraction failed: {e}")

        return people

    def _merge_people_lists(
        self,
        llm_people: List[ExtractedPerson],
        structured_people: List[ExtractedPerson],
    ) -> List[ExtractedPerson]:
        """Merge LLM and structured extraction results."""
        # Use LLM results as base
        merged = list(llm_people)

        # Add structured results not in LLM results
        llm_names = {self._normalize_name(p.full_name) for p in llm_people}

        for person in structured_people:
            if self._normalize_name(person.full_name) not in llm_names:
                merged.append(person)

        return merged

    def _deduplicate_people(
        self,
        people: List[ExtractedPerson],
    ) -> List[ExtractedPerson]:
        """Deduplicate people by name similarity."""
        if not people:
            return []

        unique = []
        seen_names = {}  # normalized_name -> ExtractedPerson

        for person in people:
            name_key = self._normalize_name(person.full_name)

            if name_key in seen_names:
                # Merge info from duplicate
                existing = seen_names[name_key]
                self._merge_person_info(existing, person)
            else:
                # Check for similar names (fuzzy match)
                match_found = False
                for existing_key, existing in seen_names.items():
                    if self._names_similar(name_key, existing_key):
                        self._merge_person_info(existing, person)
                        match_found = True
                        break

                if not match_found:
                    seen_names[name_key] = person
                    unique.append(person)

        return unique

    def _merge_person_info(
        self,
        target: ExtractedPerson,
        source: ExtractedPerson,
    ) -> None:
        """Merge info from source into target, filling gaps."""
        # Fill in missing fields
        if not target.bio and source.bio:
            target.bio = source.bio
        if not target.linkedin_url and source.linkedin_url:
            target.linkedin_url = source.linkedin_url
        if not target.email and source.email:
            target.email = source.email
        if not target.photo_url and source.photo_url:
            target.photo_url = source.photo_url
        if not target.reports_to and source.reports_to:
            target.reports_to = source.reports_to
        if target.title_level == TitleLevel.UNKNOWN and source.title_level != TitleLevel.UNKNOWN:
            target.title_level = source.title_level
        if not target.department and source.department:
            target.department = source.department

        # Use higher confidence
        if source.confidence == ExtractionConfidence.HIGH and target.confidence != ExtractionConfidence.HIGH:
            target.confidence = source.confidence

    def _validate_people(
        self,
        people: List[ExtractedPerson],
        company_name: str,
    ) -> List[ExtractedPerson]:
        """Validate and filter extracted people."""
        valid = []

        for person in people:
            # Must have name and title
            if not person.full_name or not person.title:
                continue

            # Name must look valid
            if not self._is_valid_name(person.full_name):
                continue

            # Filter out company name as person name
            if self._normalize_name(person.full_name) == self._normalize_name(company_name):
                continue

            # Filter obvious non-people
            lower_name = person.full_name.lower()
            invalid_names = ['contact us', 'our team', 'leadership', 'management', 'learn more']
            if any(inv in lower_name for inv in invalid_names):
                continue

            # Filter LLM-hallucinated placeholder names
            placeholder_names = {
                'jane doe', 'john doe', 'john smith', 'jane smith',
                'bob smith', 'alice smith', 'joe smith', 'mary smith',
                'test user', 'sample person', 'example name',
            }
            if self._normalize_name(person.full_name) in placeholder_names:
                logger.warning(f"[WebsiteAgent] Filtering placeholder name: {person.full_name}")
                continue

            valid.append(person)

        return valid

    def _is_valid_name(self, name: str) -> bool:
        """Check if a string looks like a valid person name."""
        if not name:
            return False

        # Must have at least 2 characters
        if len(name) < 2:
            return False

        # Should have 2-5 words
        words = name.split()
        if len(words) < 2 or len(words) > 6:
            return False

        # Shouldn't be too long
        if len(name) > 60:
            return False

        # Should contain letters
        if not any(c.isalpha() for c in name):
            return False

        # Shouldn't contain URLs
        if 'http' in name.lower() or 'www.' in name.lower():
            return False

        # Shouldn't contain @
        if '@' in name:
            return False

        return True

    def _normalize_name(self, name: str) -> str:
        """Normalize a name for comparison."""
        if not name:
            return ""

        # Lowercase
        name = name.lower()

        # Remove common suffixes
        suffixes = [' jr', ' sr', ' iii', ' ii', ' iv', ' phd', ' md', ' esq', ' cpa']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]

        # Remove punctuation
        name = ''.join(c for c in name if c.isalnum() or c.isspace())

        # Collapse whitespace
        name = ' '.join(name.split())

        return name

    def _names_similar(self, name1: str, name2: str) -> bool:
        """Check if two names are similar enough to be the same person."""
        # Exact match
        if name1 == name2:
            return True

        # One is substring of other
        if name1 in name2 or name2 in name1:
            return True

        # Check word overlap
        words1 = set(name1.split())
        words2 = set(name2.split())
        overlap = len(words1 & words2)

        # If most words match, consider similar
        if overlap >= 2 and overlap >= min(len(words1), len(words2)) - 1:
            return True

        return False

    def _parse_name(self, full_name: str) -> Dict[str, Optional[str]]:
        """Parse a full name into components."""
        parts = full_name.split()

        if len(parts) == 1:
            return {'first_name': parts[0], 'last_name': None, 'suffix': None}

        # Check for suffix
        suffix = None
        suffixes = ['Jr.', 'Jr', 'Sr.', 'Sr', 'III', 'II', 'IV', 'PhD', 'MD', 'Esq', 'CPA']
        if parts[-1] in suffixes:
            suffix = parts[-1]
            parts = parts[:-1]

        if len(parts) == 0:
            return {'first_name': None, 'last_name': None, 'suffix': suffix}
        elif len(parts) == 1:
            return {'first_name': parts[0], 'last_name': None, 'suffix': suffix}
        else:
            return {
                'first_name': parts[0],
                'last_name': ' '.join(parts[1:]),
                'suffix': suffix,
            }

    def _infer_title_level(self, title: str) -> TitleLevel:
        """Infer title level from title string."""
        if not title:
            return TitleLevel.UNKNOWN

        title_lower = title.lower()
        # Add spaces for word boundary matching
        title_spaced = f' {title_lower} '

        # C-Suite (check first, highest priority)
        # Use word boundaries to avoid matching "cto" in "director"
        c_suite_patterns = [' chief ', ' ceo ', ' cfo ', ' coo ', ' cto ', ' cio ', ' cmo ', ' cro ', ' chro ']
        if any(pat in title_spaced for pat in c_suite_patterns):
            return TitleLevel.C_SUITE

        # EVP (check before president since "Executive Vice President" contains "president")
        if 'executive vice president' in title_lower or 'evp' in title_lower:
            return TitleLevel.EVP

        # SVP (check before president and VP)
        if 'senior vice president' in title_lower or 'svp' in title_lower:
            return TitleLevel.SVP

        # VP (check before president since "Vice President" contains "president")
        if 'vice president' in title_lower or ' vp ' in f' {title_lower} ':
            return TitleLevel.VP

        # President (standalone, after VP variants)
        if 'president' in title_lower:
            return TitleLevel.PRESIDENT

        # Board (check for "director" only if it's board-style, not "Director of X")
        if 'board' in title_lower or 'chairman' in title_lower:
            return TitleLevel.BOARD
        if 'director' in title_lower and 'director of' not in title_lower and 'director,' not in title_lower:
            return TitleLevel.BOARD

        # Director (functional role like "Director of Sales")
        if 'director' in title_lower:
            return TitleLevel.DIRECTOR

        # Manager
        if 'manager' in title_lower:
            return TitleLevel.MANAGER

        return TitleLevel.UNKNOWN

    def _finalize_result(self, result: CollectionResult) -> CollectionResult:
        """Finalize collection result with timing."""
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
        return result


async def collect_company_website(
    company_id: int,
    company_name: str,
    website_url: str,
) -> CollectionResult:
    """
    Convenience function to collect from a company website.

    Args:
        company_id: Database ID
        company_name: Company name
        website_url: Company website URL

    Returns:
        CollectionResult with extracted people
    """
    async with WebsiteAgent() as agent:
        return await agent.collect(company_id, company_name, website_url)
