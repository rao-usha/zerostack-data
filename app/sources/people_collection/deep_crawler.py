"""
Multi-site BFS website crawler for deep people collection.

Instead of finding 3-5 leadership pages (existing PageFinder), the DeepCrawler:
1. Starts from multiple seed URLs across company web properties
2. BFS crawl up to configurable depth from each seed
3. At each page, scores links for people/team relevance, follows high-scoring ones
4. Extracts people from every qualifying page via LLMExtractor

Designed for large companies with multiple domains (e.g., Prudential Financial
with prudential.com, pgim.com, news.prudential.com).
"""

import asyncio
import logging
import re
from collections import deque
from typing import List, Optional, Set, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime

from bs4 import BeautifulSoup

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.html_cleaner import HTMLCleaner
from app.sources.people_collection.llm_extractor import LLMExtractor
from app.sources.people_collection.types import (
    ExtractedPerson,
    CollectionResult,
    ExtractionConfidence,
    TitleLevel,
)

logger = logging.getLogger(__name__)


class DeepCrawler(BaseCollector):
    """
    BFS-based multi-site crawler for deep people extraction.

    Crawls multiple domains belonging to a company, discovers leadership-related
    pages, and extracts people from each qualifying page.
    """

    # Keywords that boost a link's relevance score
    HIGH_VALUE_KEYWORDS = [
        "leadership",
        "leaders",
        "executive",
        "executives",
        "management",
        "management-team",
        "our-team",
        "team",
        "board",
        "directors",
        "board-of-directors",
        "officers",
        "who-we-are",
    ]
    MEDIUM_VALUE_KEYWORDS = [
        "about",
        "about-us",
        "people",
        "staff",
        "bios",
        "profiles",
        "governance",
        "corporate-governance",
        "senior-management",
    ]
    # Keywords that penalize a link (not relevant to people)
    PENALTY_KEYWORDS = [
        "careers",
        "jobs",
        "job",
        "apply",
        "recruiting",
        "recruitment",
        "login",
        "sign-in",
        "signup",
        "cart",
        "checkout",
        "shop",
        "products",
        "services",
        "solutions",
        "pricing",
        "blog",
        "news",
        "press",
        "media",
        "events",
        "webinar",
        "faq",
        "help",
        "support",
        "contact-us",
        "privacy",
        "terms",
        "cookie",
        "sitemap",
        "search",
        "subscribe",
        "unsubscribe",
        "esg",
        "sustainability",
        "diversity",
        "inclusion",
        "annual-report",
        "investor",
        "sec-filing",
        "financial",
    ]

    # Text content on links that suggests people content
    LINK_TEXT_KEYWORDS = [
        "leadership",
        "team",
        "executive",
        "management",
        "board",
        "directors",
        "officers",
        "who we are",
        "our people",
        "senior leadership",
        "leadership team",
    ]

    def __init__(self):
        super().__init__(source_type="website")
        self.html_cleaner = HTMLCleaner()
        self.llm_extractor = LLMExtractor()

    async def crawl(
        self,
        company_id: int,
        company_name: str,
        seed_urls: List[str],
        allowed_domains: List[str],
        max_pages: int = 50,
        max_depth: int = 3,
    ) -> CollectionResult:
        """
        Deep crawl multiple sites to find and extract people.

        Args:
            company_id: Database company ID
            company_name: Company name for extraction context
            seed_urls: List of starting URLs to crawl from
            allowed_domains: List of domains we're allowed to crawl
            max_pages: Maximum total pages to visit
            max_depth: Maximum BFS depth from any seed URL
        """
        started_at = datetime.utcnow()

        result = CollectionResult(
            company_id=company_id,
            company_name=company_name,
            source="deep_crawl",
            started_at=started_at,
        )

        all_people: List[ExtractedPerson] = []
        visited: Set[str] = set()
        pages_with_people = 0

        # Normalize allowed domains
        allowed = set(d.lower().lstrip("www.") for d in allowed_domains)

        # BFS queue: (url, depth)
        queue: deque = deque()
        for url in seed_urls:
            normalized = self._normalize_url(url)
            if normalized not in visited:
                queue.append((normalized, 0))
                visited.add(normalized)

        logger.info(
            f"[DeepCrawler] Starting deep crawl for {company_name}: "
            f"{len(seed_urls)} seeds, {len(allowed)} domains, "
            f"max_pages={max_pages}, max_depth={max_depth}"
        )

        pages_visited = 0

        while queue and pages_visited < max_pages:
            url, depth = queue.popleft()

            # Check domain is allowed
            domain = urlparse(url).netloc.lower().lstrip("www.")
            if domain not in allowed and not any(
                domain.endswith("." + d) for d in allowed
            ):
                continue

            logger.debug(f"[DeepCrawler] Visiting (depth={depth}): {url}")

            try:
                html = await self.fetch_url(url, use_cache=True)
                if not html:
                    logger.debug(f"[DeepCrawler] Failed to fetch: {url}")
                    continue

                pages_visited += 1
                result.pages_checked += 1
                result.page_urls.append(url)

                # Check if page has people content
                cleaned = self.html_cleaner.clean(html, url)

                if cleaned.has_leadership_content or self._page_likely_has_people(
                    html, url
                ):
                    # Extract people from this page
                    people = await self._extract_people_from_page(
                        html, cleaned, url, company_name
                    )

                    if people:
                        pages_with_people += 1
                        all_people.extend(people)
                        logger.info(
                            f"[DeepCrawler] Extracted {len(people)} people from {url}"
                        )

                # Discover and score links for further crawling
                if depth < max_depth:
                    new_links = self._discover_links(html, url, visited, allowed)

                    # Sort by score descending, take top links
                    new_links.sort(key=lambda x: x[1], reverse=True)
                    for link_url, score in new_links[:20]:  # Top 20 per page
                        if score > 0 and link_url not in visited:
                            visited.add(link_url)
                            queue.append((link_url, depth + 1))

            except Exception as e:
                logger.warning(f"[DeepCrawler] Error processing {url}: {e}")
                result.warnings.append(f"Error on {url}: {str(e)[:100]}")

        # Deduplicate people
        unique_people = self._deduplicate_people(all_people)

        result.extracted_people = unique_people
        result.people_found = len(unique_people)
        result.success = True
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (result.completed_at - started_at).total_seconds()

        logger.info(
            f"[DeepCrawler] Deep crawl complete for {company_name}: "
            f"{pages_visited} pages visited, {pages_with_people} had people, "
            f"{len(all_people)} extracted, {len(unique_people)} unique"
        )

        return result

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication."""
        if not url.startswith("http"):
            url = "https://" + url
        # Remove trailing slash, fragment, common tracking params
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    def _page_likely_has_people(self, html: str, url: str) -> bool:
        """Quick check if a page likely contains people information."""
        url_lower = url.lower()

        # URL-based signals
        url_signals = any(
            kw in url_lower
            for kw in self.HIGH_VALUE_KEYWORDS + self.MEDIUM_VALUE_KEYWORDS
        )

        # Content-based signals (check for multiple name-title patterns)
        name_title_pattern = r"[A-Z][a-z]+ [A-Z][a-z]+.*(?:Chief|President|Vice|Director|Officer|Manager|SVP|EVP|VP|CEO|CFO|COO|CTO)"
        content_signals = len(re.findall(name_title_pattern, html[:20000])) >= 2

        return url_signals or content_signals

    async def _extract_people_from_page(
        self,
        html: str,
        cleaned,
        url: str,
        company_name: str,
    ) -> List[ExtractedPerson]:
        """Extract people from a single page."""
        people = []

        # Try structured extraction first (CSS selectors for people cards)
        if cleaned.people_sections:
            for section in cleaned.people_sections:
                name = section.get("name", "")
                title = section.get("title", "")
                if name and title:
                    person = ExtractedPerson(
                        full_name=name,
                        title=title,
                        bio=section.get("bio"),
                        photo_url=section.get("image_url"),
                        linkedin_url=section.get("linkedin_url"),
                        source_url=url,
                        confidence=ExtractionConfidence.HIGH,
                        extraction_notes="Deep crawl - structured extraction",
                    )
                    people.append(person)

        # If structured extraction found enough people, skip LLM
        if len(people) >= 3:
            return people

        # LLM fallback for unstructured pages
        text = cleaned.text
        if len(text) < 200:
            return people  # Not enough content

        try:
            llm_result = await self.llm_extractor.extract_leadership_from_html(
                text[:50000], company_name, url
            )

            if llm_result and llm_result.people:
                for person in llm_result.people:
                    person.source_url = url
                    person.extraction_notes = "Deep crawl - LLM extraction"
                    # Don't add duplicates from structured extraction
                    if person.full_name not in [p.full_name for p in people]:
                        people.append(person)

        except Exception as e:
            logger.warning(f"[DeepCrawler] LLM extraction failed for {url}: {e}")

        return people

    def _discover_links(
        self,
        html: str,
        base_url: str,
        visited: Set[str],
        allowed_domains: Set[str],
    ) -> List[Tuple[str, int]]:
        """
        Discover and score links on a page for crawling priority.

        Returns list of (url, score) tuples.
        """
        soup = BeautifulSoup(html, "html.parser")
        scored_links: List[Tuple[str, int]] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()

            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            normalized = self._normalize_url(full_url)

            # Skip already visited
            if normalized in visited:
                continue

            # Check domain is allowed
            domain = urlparse(normalized).netloc.lower().lstrip("www.")
            if domain not in allowed_domains and not any(
                domain.endswith("." + d) for d in allowed_domains
            ):
                continue

            # Skip non-page URLs
            path_lower = urlparse(normalized).path.lower()
            if any(
                path_lower.endswith(ext)
                for ext in [
                    ".pdf",
                    ".jpg",
                    ".png",
                    ".gif",
                    ".css",
                    ".js",
                    ".zip",
                    ".doc",
                    ".xlsx",
                ]
            ):
                continue

            # Score the link
            score = self._score_link(normalized, text)

            if score != 0:  # Only include non-zero-scored links
                scored_links.append((normalized, score))

        return scored_links

    def _score_link(self, url: str, link_text: str) -> int:
        """
        Score a link for people-relevance.

        Positive scores = likely contains people info
        Negative scores = unlikely to contain people info
        Zero = neutral
        """
        score = 0
        url_lower = url.lower()
        path = urlparse(url_lower).path

        # High value URL keywords (+10 each)
        for kw in self.HIGH_VALUE_KEYWORDS:
            if kw in path:
                score += 10

        # Medium value URL keywords (+5 each)
        for kw in self.MEDIUM_VALUE_KEYWORDS:
            if kw in path:
                score += 5

        # Penalty keywords (-10 each)
        for kw in self.PENALTY_KEYWORDS:
            if kw in path:
                score -= 10

        # Link text bonuses (+8 each)
        for kw in self.LINK_TEXT_KEYWORDS:
            if kw in link_text:
                score += 8

        # Penalize very deep paths (>4 segments)
        segments = [s for s in path.split("/") if s]
        if len(segments) > 4:
            score -= 5

        return score

    def _deduplicate_people(
        self,
        people: List[ExtractedPerson],
    ) -> List[ExtractedPerson]:
        """Deduplicate people by normalized name."""
        seen: Dict[str, ExtractedPerson] = {}

        for person in people:
            key = self._normalize_name(person.full_name)
            if not key:
                continue

            if key in seen:
                # Merge: keep richer data
                existing = seen[key]
                if not existing.bio and person.bio:
                    existing.bio = person.bio
                if not existing.linkedin_url and person.linkedin_url:
                    existing.linkedin_url = person.linkedin_url
                if not existing.photo_url and person.photo_url:
                    existing.photo_url = person.photo_url
                if not existing.email and person.email:
                    existing.email = person.email
                if person.is_board_member:
                    existing.is_board_member = True
                if person.is_executive:
                    existing.is_executive = True
                # Prefer higher confidence
                if person.confidence == "high":
                    existing.confidence = ExtractionConfidence.HIGH
            else:
                seen[key] = person

        return list(seen.values())

    def _normalize_name(self, name: str) -> str:
        """Normalize name for deduplication."""
        if not name:
            return ""
        name = name.lower()
        name = "".join(c for c in name if c.isalnum() or c.isspace())
        return " ".join(name.split())
