"""
Bio Extractor for PE firm team members.

Scrapes PE firm team/about/professionals pages, then uses GPT-4o-mini
to extract structured biographical data for each person.
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)

logger = logging.getLogger(__name__)

# URL path patterns for team/people pages (reuses FirmWebsiteCollector concept)
TEAM_PATTERNS = [
    "/team",
    "/people",
    "/leadership",
    "/our-team",
    "/about/team",
    "/professionals",
    "/about/leadership",
    "/about/professionals",
    "/about-us/team",
    "/who-we-are/team",
    "/about/people",
    "/our-people",
]

# Maximum people to extract per firm
MAX_PEOPLE_PER_FIRM = 50

# LLM extraction prompt
BIO_EXTRACTION_PROMPT = """Extract biographical information for each person mentioned on this PE firm team page.
Firm name: {firm_name}

Return ONLY valid JSON — an array of person objects:
[
  {{
    "full_name": "First Last",
    "title": "Managing Director",
    "bio": "2-3 sentence professional summary",
    "education": [
      {{"institution": "Harvard Business School", "degree": "MBA", "field": "Finance"}}
    ],
    "experience": [
      {{"company": "Goldman Sachs", "title": "Vice President"}}
    ],
    "focus_areas": ["Technology", "Healthcare"]
  }}
]

Rules:
- Only include people who are clearly current members of {firm_name}
- If education or experience details are not mentioned, use empty arrays
- Keep bios factual; do not fabricate details not present in the text
- focus_areas should reflect investment or industry focus mentioned in their bio

Page text:
{text}"""


class BioExtractor(BasePECollector):
    """
    Extracts structured biographical data from PE firm team pages.

    Discovers team/people pages on the firm website, fetches HTML,
    and uses GPT-4o-mini to parse structured bios including education,
    experience, and focus areas.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.BIO_EXTRACTOR

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._llm_client = None

    def _get_llm_client(self):
        """Lazily initialize LLM client."""
        if self._llm_client is None:
            from app.agentic.llm_client import get_llm_client

            self._llm_client = get_llm_client(model="gpt-4o-mini")
        return self._llm_client

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Extract bios for team members at a PE firm.

        Args:
            entity_id: PE firm ID
            entity_name: Firm name
            website_url: Firm website URL (required)
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        if not website_url:
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message="No website URL provided — cannot discover team page",
                started_at=started_at,
            )

        # Normalize URL
        if not website_url.startswith(("http://", "https://")):
            website_url = f"https://{website_url}"

        try:
            # Step 1: Find the team page
            team_url = await self._find_team_page(website_url)
            if not team_url:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=["Could not find team/people page on website"],
                    started_at=started_at,
                )

            # Step 2: Fetch team page content
            page_text = await self._fetch_page_text(team_url)
            if not page_text:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=[f"Could not fetch team page at {team_url}"],
                    started_at=started_at,
                )

            # Step 3: Check for individual profile links
            profile_texts = await self._fetch_profile_pages(team_url, page_text)
            if profile_texts:
                # Combine team listing + individual profiles for richer bios
                page_text = (
                    page_text
                    + "\n\n--- Individual Profiles ---\n\n"
                    + "\n\n".join(profile_texts)
                )

            # Step 4: Extract bios with LLM
            llm_client = self._get_llm_client()
            if not llm_client:
                warnings.append("LLM not available — cannot extract structured bios")
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=warnings,
                    started_at=started_at,
                )

            people = await self._extract_bios_with_llm(
                llm_client, page_text, entity_name
            )
            if not people:
                warnings.append("LLM extraction returned no people")
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=warnings,
                    started_at=started_at,
                )

            # Step 5: Build items
            for person in people[:MAX_PEOPLE_PER_FIRM]:
                full_name = person.get("full_name", "").strip()
                if not full_name:
                    continue

                items.append(
                    self._create_item(
                        item_type="person",
                        data={
                            "firm_id": entity_id,
                            "firm_name": entity_name,
                            "full_name": full_name,
                            "title": person.get("title"),
                            "bio": person.get("bio"),
                            "education": person.get("education", []),
                            "experience": person.get("experience", []),
                            "focus_areas": person.get("focus_areas", []),
                        },
                        source_url=team_url,
                        confidence="llm_extracted",
                    )
                )

            logger.info(f"Extracted {len(items)} bios from {entity_name} team page")

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error extracting bios for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _find_team_page(self, website_url: str) -> Optional[str]:
        """
        Find the team/people page on a PE firm website.

        Tries common URL patterns first, then falls back to scanning
        the homepage for team-related links.
        """
        # Try common patterns directly
        for pattern in TEAM_PATTERNS:
            url = urljoin(website_url.rstrip("/") + "/", pattern.lstrip("/"))
            response = await self._fetch_url(url)
            if response and response.status_code == 200:
                # Verify it's actually a team page (not a redirect to homepage)
                text = response.text.lower()
                if any(
                    kw in text
                    for kw in [
                        "team",
                        "people",
                        "professional",
                        "leadership",
                        "managing",
                    ]
                ):
                    return url

        # Fall back: fetch homepage and look for team links
        response = await self._fetch_url(website_url)
        if not response or response.status_code != 200:
            return None

        return self._find_team_link_in_html(response.text, website_url)

    def _find_team_link_in_html(self, html: str, base_url: str) -> Optional[str]:
        """Find a team/people link in HTML content."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return None

        soup = BeautifulSoup(html, "html.parser")
        team_keywords = [
            "team",
            "people",
            "leadership",
            "professionals",
            "our team",
            "who we are",
        ]

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            href_lower = href.lower()

            if any(kw in text or kw in href_lower for kw in team_keywords):
                return urljoin(base_url, href)

        return None

    async def _fetch_page_text(self, url: str) -> Optional[str]:
        """Fetch a page and extract clean text content."""
        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return None

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Remove noise elements
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Try to find main content area
        main = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", class_=re.compile(r"team|people|content|main", re.I))
            or soup.find("div", id=re.compile(r"team|people|content|main", re.I))
        )
        if main:
            return main.get_text(separator="\n", strip=True)

        return soup.get_text(separator="\n", strip=True)

    async def _fetch_profile_pages(
        self, team_url: str, team_html_text: str
    ) -> List[str]:
        """
        Check for individual profile page links and fetch a subset.

        Many PE firms have individual /team/person-name pages with detailed bios.
        Fetch up to 10 to enrich the extraction.
        """
        # Re-fetch the raw HTML to find links
        response = await self._fetch_url(team_url)
        if not response or response.status_code != 200:
            return []

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        profile_urls = []

        # Look for links that seem to be individual profiles
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            href_lower = href.lower()
            # Profile links often contain team member paths
            if any(
                pat in href_lower
                for pat in ["/team/", "/people/", "/professionals/", "/bio/", "/staff/"]
            ):
                full_url = urljoin(team_url, href)
                if full_url not in profile_urls and full_url != team_url:
                    profile_urls.append(full_url)

        # Fetch up to 10 profile pages
        profile_texts = []
        for url in profile_urls[:10]:
            text = await self._fetch_page_text(url)
            if text and len(text) > 100:
                profile_texts.append(text[:3000])

        return profile_texts

    async def _extract_bios_with_llm(
        self, llm_client, text: str, firm_name: str
    ) -> List[Dict[str, Any]]:
        """Use LLM to extract structured bio data from team page text."""

        # Truncate to keep input manageable — 12k chars is well within
        # GPT-4o-mini's 128k context while covering most team pages
        text = text[:12000]
        prompt = BIO_EXTRACTION_PROMPT.format(firm_name=firm_name, text=text)

        try:
            response = await llm_client.complete(
                prompt=prompt,
                system_prompt=(
                    "You are a financial data analyst extracting PE firm team bios. "
                    "Return only valid JSON — an array of person objects."
                ),
                json_mode=True,
            )

            # Try standard parse first; falls back to JSON repair on None
            result = response.parse_json()
            if result is None:
                raw = (
                    response.content if hasattr(response, "content") else str(response)
                )
                result = self._repair_json_array(raw)

            # Handle both list and dict with a "people" key
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("people", result.get("persons", []))
            return []

        except Exception as e:
            logger.warning(f"LLM bio extraction failed: {e}")
            return []

    @staticmethod
    def _repair_json_array(raw: str) -> Any:
        """Attempt to repair and parse a JSON array from raw LLM output."""
        import json as json_mod

        # Find the outermost [ ... ] bracket pair
        start = raw.find("[")
        if start == -1:
            return []

        # Find matching closing bracket
        depth = 0
        end = -1
        for i in range(start, len(raw)):
            if raw[i] == "[":
                depth += 1
            elif raw[i] == "]":
                depth -= 1
                if depth == 0:
                    end = i
                    break

        if end == -1:
            # Truncated output — find last complete object and close array
            candidate = raw[start:]
            # Find the last complete "}" that closes a top-level object
            last_complete = -1
            obj_depth = 0
            in_string = False
            escape_next = False
            for i, ch in enumerate(candidate):
                if escape_next:
                    escape_next = False
                    continue
                if ch == "\\":
                    escape_next = True
                    continue
                if ch == '"' and not escape_next:
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if ch == "{":
                    obj_depth += 1
                elif ch == "}":
                    obj_depth -= 1
                    if obj_depth == 0:
                        last_complete = i

            if last_complete > 0:
                candidate = candidate[: last_complete + 1] + "]"
            else:
                candidate = "[]"
        else:
            candidate = raw[start : end + 1]

        # Fix common JSON issues: trailing commas before ] or }
        candidate = re.sub(r",\s*([}\]])", r"\1", candidate)

        try:
            return json_mod.loads(candidate)
        except json_mod.JSONDecodeError:
            logger.debug("JSON repair failed, returning empty list")
            return []
