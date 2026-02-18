"""
LinkedIn People Discovery via Google Search.

Discovers unknown people at a company by searching Google for
site:linkedin.com/in results. Extends the existing LinkedInValidator
from validation-only into a full discovery tool.

Key capabilities:
- Company-wide people search across divisions
- Role-targeted search for specific departments/levels
- Tech org chain discovery (CTO -> VPs -> Directors)
- Google snippet parsing for name + title extraction

Rate limited: 1 req / 30 seconds via linkedin_google config.
"""

import asyncio
import logging
import re
from typing import List, Optional, Dict, Any
from urllib.parse import quote_plus

import httpx

from app.sources.people_collection.base_collector import BaseCollector, RateLimiter
from app.sources.people_collection.config import RATE_LIMITS
from app.sources.people_collection.types import (
    ExtractedPerson,
    ExtractionConfidence,
    TitleLevel,
)
from app.sources.people_collection.llm_extractor import LLMExtractor

logger = logging.getLogger(__name__)


# Role x Department search matrix for targeted discovery
TECH_ROLES = [
    "CTO",
    "CIO",
    "CISO",
    "CDO",
    "VP Engineering",
    "VP Technology",
    "VP Infrastructure",
    "VP Data",
    "VP Information Technology",
    "VP Platform",
    "VP Security",
    "Director Engineering",
    "Director Technology",
    "Director IT",
    "Director DevOps",
    "Director Infrastructure",
    "Director Data",
    "Head of Engineering",
    "Head of Technology",
    "Chief Architect",
    "Chief Technology Officer",
    "Chief Information Officer",
    "Chief Information Security Officer",
    "Chief Data Officer",
]

TECH_DEPARTMENTS = [
    "Technology",
    "Engineering",
    "IT",
    "Infrastructure",
    "Data",
    "Platform",
    "Security",
    "DevOps",
    "Software",
]

FUNCTION_ROLE_CONFIGS = {
    "technology": {
        "title_patterns": TECH_ROLES,
        "department_keywords": TECH_DEPARTMENTS,
    },
    "finance": {
        "title_patterns": [
            "CFO",
            "Controller",
            "Treasurer",
            "VP Finance",
            "Chief Financial Officer",
            "Chief Accounting Officer",
            "Director Finance",
            "Head of Finance",
        ],
        "department_keywords": ["Finance", "Accounting", "Treasury"],
    },
    "legal": {
        "title_patterns": [
            "General Counsel",
            "CLO",
            "Chief Legal Officer",
            "VP Legal",
            "Deputy General Counsel",
            "Chief Compliance Officer",
            "Head of Legal",
        ],
        "department_keywords": ["Legal", "Compliance"],
    },
    "operations": {
        "title_patterns": [
            "COO",
            "Chief Operating Officer",
            "VP Operations",
            "Head of Operations",
            "Director Operations",
        ],
        "department_keywords": ["Operations"],
    },
}

# Patterns for extracting name + title from Google search snippets
SNIPPET_PATTERNS = [
    # "Jane Smith - VP of Engineering - PGIM | LinkedIn"
    re.compile(
        r"^([A-Z][a-zA-Z\-\'\. ]{2,30})\s*[-–—]\s*(.{5,80}?)\s*[-–—]\s*(.+?)(?:\s*\|\s*LinkedIn)?$"
    ),
    # "Jane Smith. VP of Engineering at PGIM."
    re.compile(r"^([A-Z][a-zA-Z\-\'\. ]{2,30})\.\s*(.{5,80}?)\s+at\s+(.+?)\.?$"),
    # "Jane Smith VP of Engineering | PGIM"
    re.compile(r"^([A-Z][a-zA-Z\-\'\. ]{2,30})\s+(.{5,80}?)\s*\|\s*(.+?)$"),
]

# Google search result link pattern
LINKEDIN_URL_RE = re.compile(
    r"https?://(?:www\.)?linkedin\.com/in/([a-zA-Z0-9\-_]+)",
    re.IGNORECASE,
)


class LinkedInDiscovery(BaseCollector):
    """
    Discovers people at a company via Google LinkedIn search.

    Uses Google site:linkedin.com/in searches to find people, then
    parses names and titles from the search result snippets.
    """

    def __init__(self):
        super().__init__(source_type="linkedin_google")
        self._llm = LLMExtractor()
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create httpx client (better for Google searches)."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close HTTP clients."""
        await super().close()
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def discover_people(
        self,
        company_name: str,
        division_names: Optional[List[str]] = None,
        target_roles: Optional[List[str]] = None,
        target_department: Optional[str] = None,
        max_searches: int = 50,
    ) -> List[ExtractedPerson]:
        """
        Discover people at a company via LinkedIn Google search.

        Args:
            company_name: Main company name
            division_names: Optional list of subsidiary/division names
            target_roles: Specific roles to search for
            target_department: Target department (e.g., "technology")
            max_searches: Maximum number of Google searches to perform

        Returns:
            List of discovered people
        """
        logger.info(
            f"[LinkedInDiscovery] Starting discovery for {company_name} "
            f"(divisions={len(division_names or [])}, max_searches={max_searches})"
        )

        # Build search queries
        queries = self._build_search_queries(
            company_name=company_name,
            division_names=division_names or [],
            department=target_department,
            roles=target_roles,
        )

        # Limit queries
        queries = queries[:max_searches]

        logger.info(f"[LinkedInDiscovery] Running {len(queries)} Google searches")

        # Execute searches with rate limiting
        all_people: List[ExtractedPerson] = []
        seen_names: set = set()

        for i, query in enumerate(queries):
            try:
                # Rate limit: 1 req per 30 seconds
                await self.rate_limiter.acquire(
                    "https://www.google.com", self.source_type
                )

                results = await self._search_google(query)
                people = self._parse_google_results(results, company_name)

                for person in people:
                    name_key = person.full_name.lower().strip()
                    if name_key not in seen_names:
                        seen_names.add(name_key)
                        all_people.append(person)

                logger.debug(
                    f"[LinkedInDiscovery] Query {i+1}/{len(queries)}: "
                    f"found {len(people)} people ({len(all_people)} total unique)"
                )

            except Exception as e:
                logger.warning(f"[LinkedInDiscovery] Search {i+1} failed: {e}")
                continue

        logger.info(
            f"[LinkedInDiscovery] Discovery complete: {len(all_people)} unique people "
            f"from {len(queries)} searches"
        )

        return all_people

    async def discover_tech_org(
        self,
        company_name: str,
        division_names: Optional[List[str]] = None,
        known_cto: Optional[ExtractedPerson] = None,
        depth: int = 3,
        max_searches: int = 30,
    ) -> List[ExtractedPerson]:
        """
        Specifically discover technology leadership chain.

        Searches for tech leaders at each level:
        Level 1: CTO/CIO
        Level 2: VPs of Engineering/Technology/etc.
        Level 3: Directors of Engineering/Technology/etc.

        Args:
            company_name: Company name
            division_names: Division names to search within
            known_cto: Already known CTO/CIO (skip level 1 search)
            depth: How many levels deep (default 3)
            max_searches: Max Google searches

        Returns:
            List of tech org people
        """
        logger.info(
            f"[LinkedInDiscovery] Discovering tech org for {company_name} "
            f"(depth={depth})"
        )

        all_people: List[ExtractedPerson] = []
        seen_names: set = set()
        search_count = 0

        companies = [company_name] + (division_names or [])

        # Level 1: CTO/CIO
        if not known_cto and depth >= 1:
            level1_roles = [
                "CTO",
                "CIO",
                "Chief Technology Officer",
                "Chief Information Officer",
            ]
            for co in companies[:5]:  # Top 5 companies only for L1
                for role in level1_roles:
                    if search_count >= max_searches:
                        break
                    query = f'site:linkedin.com/in "{co}" "{role}"'
                    await self.rate_limiter.acquire(
                        "https://www.google.com", self.source_type
                    )
                    search_count += 1

                    try:
                        results = await self._search_google(query)
                        for person in self._parse_google_results(results, co):
                            key = person.full_name.lower().strip()
                            if key not in seen_names:
                                seen_names.add(key)
                                person.department = "Technology"
                                all_people.append(person)
                    except Exception as e:
                        logger.debug(f"[LinkedInDiscovery] L1 search failed: {e}")

        # Level 2: VPs
        if depth >= 2:
            level2_roles = [
                "VP Engineering",
                "VP Technology",
                "VP Infrastructure",
                "VP Data",
                "VP Platform",
                "VP Security",
            ]
            for co in companies[:8]:
                for role in level2_roles:
                    if search_count >= max_searches:
                        break
                    query = f'site:linkedin.com/in "{co}" "{role}"'
                    await self.rate_limiter.acquire(
                        "https://www.google.com", self.source_type
                    )
                    search_count += 1

                    try:
                        results = await self._search_google(query)
                        for person in self._parse_google_results(results, co):
                            key = person.full_name.lower().strip()
                            if key not in seen_names:
                                seen_names.add(key)
                                person.department = "Technology"
                                all_people.append(person)
                    except Exception as e:
                        logger.debug(f"[LinkedInDiscovery] L2 search failed: {e}")

        # Level 3: Directors
        if depth >= 3:
            level3_roles = [
                "Director Engineering",
                "Director Technology",
                "Director IT",
                "Director DevOps",
                "Director Infrastructure",
            ]
            for co in companies[:5]:
                for role in level3_roles:
                    if search_count >= max_searches:
                        break
                    query = f'site:linkedin.com/in "{co}" "{role}"'
                    await self.rate_limiter.acquire(
                        "https://www.google.com", self.source_type
                    )
                    search_count += 1

                    try:
                        results = await self._search_google(query)
                        for person in self._parse_google_results(results, co):
                            key = person.full_name.lower().strip()
                            if key not in seen_names:
                                seen_names.add(key)
                                person.department = "Technology"
                                all_people.append(person)
                    except Exception as e:
                        logger.debug(f"[LinkedInDiscovery] L3 search failed: {e}")

        logger.info(
            f"[LinkedInDiscovery] Tech org discovery complete: "
            f"{len(all_people)} people from {search_count} searches"
        )

        return all_people

    def _build_search_queries(
        self,
        company_name: str,
        division_names: List[str],
        department: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Build Google search queries for LinkedIn discovery.

        Generates a prioritized list of queries combining:
        - Company/division names
        - Role titles (if specified) or generic leadership queries
        - Department keywords (if specified)
        """
        queries = []
        all_companies = [company_name] + division_names

        if roles:
            # Role-targeted search
            for co in all_companies:
                for role in roles:
                    queries.append(f'site:linkedin.com/in "{co}" "{role}"')
        elif department and department in FUNCTION_ROLE_CONFIGS:
            # Department-targeted search
            config = FUNCTION_ROLE_CONFIGS[department]
            for co in all_companies:
                for role in config["title_patterns"]:
                    queries.append(f'site:linkedin.com/in "{co}" "{role}"')
        else:
            # General leadership search
            general_roles = [
                "CEO",
                "President",
                "Chief",
                "Senior Vice President",
                "Executive Vice President",
                "Vice President",
                "SVP",
                "EVP",
                "Managing Director",
                "Director",
                "Head of",
            ]
            for co in all_companies:
                # Broad company search first
                queries.append(f'site:linkedin.com/in "{co}" leadership')

                for role in general_roles:
                    queries.append(f'site:linkedin.com/in "{co}" "{role}"')

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for q in queries:
            if q not in seen:
                seen.add(q)
                unique.append(q)

        return unique

    async def _search_google(self, query: str) -> str:
        """
        Execute a Google search and return the results HTML.

        Returns raw HTML of the search results page.
        """
        search_url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"

        client = await self._get_http_client()
        response = await client.get(search_url)
        response.raise_for_status()

        return response.text

    def _parse_google_results(
        self,
        html: str,
        company_name: str,
    ) -> List[ExtractedPerson]:
        """
        Extract people from Google search result snippets.

        Google results for site:linkedin.com/in typically show:
        - Title: "Jane Smith - VP of Engineering - PGIM | LinkedIn"
        - Snippet: "Jane Smith. VP of Engineering at PGIM. Greater Newark Area."
        """
        if not html:
            return []

        people = []
        seen_urls = set()

        # Extract all LinkedIn URLs from the page
        urls = LINKEDIN_URL_RE.findall(html)
        unique_usernames = list(dict.fromkeys(urls))

        # Try to extract structured data from result titles and snippets
        # Google wraps results in <h3> tags for titles and <span> for snippets
        results = self._extract_search_result_blocks(html)

        for result in results:
            title_text = result.get("title", "")
            snippet_text = result.get("snippet", "")
            url = result.get("url", "")

            # Skip if we already processed this URL
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Try to parse person from title
            person = self._parse_result_text(title_text, snippet_text, company_name)
            if person:
                # Add LinkedIn URL
                url_match = LINKEDIN_URL_RE.search(url) if url else None
                if url_match:
                    person.linkedin_url = (
                        f"https://www.linkedin.com/in/{url_match.group(1)}"
                    )
                elif url:
                    person.linkedin_url = url

                person.source_url = "google_linkedin_search"
                people.append(person)

        # If structured parsing found nothing, try regex on the full HTML
        if not people and unique_usernames:
            for username in unique_usernames[:5]:
                person = self._person_from_username(username, html, company_name)
                if person:
                    people.append(person)

        return people

    def _extract_search_result_blocks(self, html: str) -> List[Dict[str, str]]:
        """
        Extract search result blocks (title + snippet + URL) from Google HTML.
        """
        results = []

        # Pattern for Google result links containing linkedin.com/in
        # Google formats results with <a> tags containing the URL and <h3> containing title
        link_pattern = re.compile(
            r'<a[^>]+href="(/url\?q=|)(https?://[^"]*linkedin\.com/in/[^"&]+)[^"]*"[^>]*>'
            r".*?<h3[^>]*>(.*?)</h3>",
            re.DOTALL | re.IGNORECASE,
        )

        for match in link_pattern.finditer(html):
            url = match.group(2)
            title = re.sub(r"<[^>]+>", "", match.group(3)).strip()

            # Try to find the snippet near this result
            snippet = ""
            pos = match.end()
            snippet_match = re.search(
                r'<(?:span|div)[^>]*class="[^"]*(?:st|VwiC3b|IsZvec)[^"]*"[^>]*>(.*?)</(?:span|div)>',
                html[pos : pos + 2000],
                re.DOTALL | re.IGNORECASE,
            )
            if snippet_match:
                snippet = re.sub(r"<[^>]+>", "", snippet_match.group(1)).strip()

            if title:
                results.append(
                    {
                        "title": title,
                        "snippet": snippet,
                        "url": url,
                    }
                )

        # Fallback: simpler pattern matching
        if not results:
            # Look for linkedin.com/in URLs near text that looks like names
            simple_pattern = re.compile(
                r"(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_]+)[^<]*"
                r"(?:<[^>]+>)*\s*([A-Z][a-zA-Z\-\'\. ]+[-–—].+?)(?:<|$)",
                re.IGNORECASE,
            )
            for match in simple_pattern.finditer(html):
                results.append(
                    {
                        "title": re.sub(r"<[^>]+>", "", match.group(2)).strip(),
                        "snippet": "",
                        "url": match.group(1),
                    }
                )

        return results

    def _parse_result_text(
        self,
        title: str,
        snippet: str,
        company_name: str,
    ) -> Optional[ExtractedPerson]:
        """
        Parse a person's name and title from Google result title/snippet text.
        """
        # Clean up text
        title = re.sub(r"\s*\|\s*LinkedIn\s*$", "", title).strip()
        title = re.sub(r"\s*-\s*LinkedIn\s*$", "", title).strip()

        # Try snippet patterns
        for pattern in SNIPPET_PATTERNS:
            match = pattern.match(title)
            if match:
                name = match.group(1).strip()
                role = match.group(2).strip()

                if self._is_valid_person_name(name) and len(role) > 3:
                    return ExtractedPerson(
                        full_name=name,
                        title=role,
                        title_level=self._infer_title_level(role),
                        confidence=ExtractionConfidence.LOW,
                        extraction_notes="Extracted from LinkedIn Google search snippet",
                    )

        # Try snippet text
        if snippet:
            for pattern in SNIPPET_PATTERNS:
                match = pattern.match(snippet)
                if match:
                    name = match.group(1).strip()
                    role = match.group(2).strip()

                    if self._is_valid_person_name(name) and len(role) > 3:
                        return ExtractedPerson(
                            full_name=name,
                            title=role,
                            title_level=self._infer_title_level(role),
                            confidence=ExtractionConfidence.LOW,
                            extraction_notes="Extracted from LinkedIn Google search snippet",
                        )

        # Try "Name at Company" pattern from snippet
        at_pattern = re.compile(
            r"([A-Z][a-zA-Z\-\'\. ]{2,30})\.\s*(.{5,80}?)\s+at\s+",
        )
        for text in [title, snippet]:
            match = at_pattern.search(text)
            if match:
                name = match.group(1).strip()
                role = match.group(2).strip()
                if self._is_valid_person_name(name) and len(role) > 3:
                    return ExtractedPerson(
                        full_name=name,
                        title=role,
                        title_level=self._infer_title_level(role),
                        confidence=ExtractionConfidence.LOW,
                        extraction_notes="Extracted from LinkedIn Google search snippet",
                    )

        # Last resort: if title has "Name - Title" format
        parts = re.split(r"\s*[-–—]\s*", title)
        if len(parts) >= 2:
            name = parts[0].strip()
            role = parts[1].strip()
            if self._is_valid_person_name(name) and len(role) > 3:
                return ExtractedPerson(
                    full_name=name,
                    title=role,
                    title_level=self._infer_title_level(role),
                    confidence=ExtractionConfidence.LOW,
                    extraction_notes="Extracted from LinkedIn Google search title",
                )

        return None

    def _person_from_username(
        self,
        username: str,
        html: str,
        company_name: str,
    ) -> Optional[ExtractedPerson]:
        """
        Create a person from a LinkedIn username, inferring name from the slug.
        """
        # Convert slug to name: "jane-smith-12345" -> "Jane Smith"
        # Remove trailing numbers
        clean = re.sub(r"-\d+[a-z]?$", "", username)
        clean = re.sub(r"-\d{4,}", "", clean)  # Remove long number sequences
        parts = clean.split("-")

        # Filter out very short parts and numbers
        name_parts = [p.capitalize() for p in parts if len(p) > 1 and p.isalpha()]

        if len(name_parts) < 2:
            return None

        name = " ".join(name_parts)

        # Try to find their title near this username in the HTML
        username_pos = html.find(username)
        if username_pos > 0:
            context = html[max(0, username_pos - 500) : username_pos + 500]
            context_clean = re.sub(r"<[^>]+>", " ", context)

            # Look for title patterns near the name
            title_match = re.search(
                r"(?:" + re.escape(name_parts[0]) + r".*?)"
                r"[-–—]\s*(.{5,80}?)(?:\s*[-–—]|\s*\||\s*at\s)",
                context_clean,
                re.IGNORECASE,
            )
            title = title_match.group(1).strip() if title_match else ""
        else:
            title = ""

        if not title:
            return None

        return ExtractedPerson(
            full_name=name,
            title=title,
            linkedin_url=f"https://www.linkedin.com/in/{username}",
            title_level=self._infer_title_level(title),
            confidence=ExtractionConfidence.LOW,
            source_url="google_linkedin_search",
            extraction_notes="Inferred from LinkedIn URL slug and context",
        )

    def _is_valid_person_name(self, name: str) -> bool:
        """Check if a string looks like a valid person name."""
        if not name or len(name) < 3:
            return False

        parts = name.split()
        if len(parts) < 2 or len(parts) > 5:
            return False

        # Must be mostly alphabetic
        alpha_ratio = sum(1 for c in name if c.isalpha() or c in " '-.")
        if alpha_ratio / len(name) < 0.7:
            return False

        # First character should be uppercase
        if not name[0].isupper():
            return False

        # Filter common false positives
        lower = name.lower()
        false_positives = [
            "view all",
            "see more",
            "show more",
            "people also",
            "related searches",
            "sign in",
            "log in",
        ]
        if any(fp in lower for fp in false_positives):
            return False

        return True

    def _infer_title_level(self, title: str) -> str:
        """Infer title level from title string."""
        if not title:
            return TitleLevel.UNKNOWN

        title_lower = title.lower()

        if any(k in title_lower for k in ["ceo", "chief executive"]):
            return TitleLevel.C_SUITE
        if any(
            k in title_lower
            for k in [
                "cto",
                "cfo",
                "coo",
                "cio",
                "ciso",
                "cdo",
                "cmo",
                "cro",
                "chief",
                "general counsel",
            ]
        ):
            return TitleLevel.C_SUITE
        if "president" in title_lower and "vice" not in title_lower:
            return TitleLevel.PRESIDENT
        if "executive vice president" in title_lower or "evp" in title_lower:
            return TitleLevel.EVP
        if "senior vice president" in title_lower or "svp" in title_lower:
            return TitleLevel.SVP
        if "vice president" in title_lower or title_lower.startswith("vp"):
            return TitleLevel.VP
        if any(
            k in title_lower
            for k in [
                "managing director",
                "head of",
                "director",
            ]
        ):
            return TitleLevel.DIRECTOR
        if "manager" in title_lower:
            return TitleLevel.MANAGER

        return TitleLevel.UNKNOWN
