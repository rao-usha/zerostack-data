"""
LinkedIn Validator for People Intelligence Platform.

Validates and discovers LinkedIn profiles using public search methods.
Note: Uses Google site: search only - no LinkedIn API or scraping.
"""

import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from urllib.parse import quote_plus
import logging

import httpx

logger = logging.getLogger(__name__)


@dataclass
class LinkedInMatch:
    """A potential LinkedIn profile match."""

    url: str
    name_from_url: Optional[str]
    confidence: str  # high, medium, low
    match_reason: str


class LinkedInValidator:
    """
    Validates and discovers LinkedIn profiles.

    Uses Google site: search to find profiles without scraping LinkedIn directly.
    """

    LINKEDIN_URL_PATTERN = re.compile(
        r"https?://(?:www\.)?linkedin\.com/in/([a-zA-Z0-9\-_]+)", re.IGNORECASE
    )

    def __init__(self):
        self.http_client = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self.http_client is None:
            self.http_client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                },
            )
        return self.http_client

    async def close(self):
        """Close HTTP client."""
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None

    def validate_url_format(self, url: str) -> bool:
        """
        Validate that a URL is a valid LinkedIn profile URL.
        """
        if not url:
            return False
        return bool(self.LINKEDIN_URL_PATTERN.match(url))

    def normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize a LinkedIn URL to standard format.
        """
        if not url:
            return None

        match = self.LINKEDIN_URL_PATTERN.match(url)
        if match:
            username = match.group(1)
            return f"https://www.linkedin.com/in/{username}"
        return None

    def extract_username(self, url: str) -> Optional[str]:
        """
        Extract username/slug from LinkedIn URL.
        """
        if not url:
            return None

        match = self.LINKEDIN_URL_PATTERN.match(url)
        if match:
            return match.group(1)
        return None

    def build_search_query(
        self,
        full_name: str,
        company_name: Optional[str] = None,
        title: Optional[str] = None,
    ) -> str:
        """
        Build a Google search query to find LinkedIn profile.
        """
        parts = [f'site:linkedin.com/in "{full_name}"']

        if company_name:
            # Clean company name
            clean_company = re.sub(
                r"\b(inc|llc|corp|corporation|company|co)\b",
                "",
                company_name,
                flags=re.IGNORECASE,
            ).strip()
            parts.append(f'"{clean_company}"')

        if title:
            # Simplify title for search
            simple_title = title.split(",")[0].strip()
            if len(simple_title) < 50:
                parts.append(simple_title)

        return " ".join(parts)

    async def search_linkedin_profile(
        self,
        full_name: str,
        company_name: Optional[str] = None,
        title: Optional[str] = None,
    ) -> List[LinkedInMatch]:
        """
        Search for LinkedIn profile using Google.

        Returns list of potential matches with confidence scores.

        Note: This method uses public Google search. Rate limit accordingly.
        """
        # Build search URL
        query = self.build_search_query(full_name, company_name, title)
        search_url = f"https://www.google.com/search?q={quote_plus(query)}"

        try:
            client = await self._get_client()
            response = await client.get(search_url)
            response.raise_for_status()

            # Extract LinkedIn URLs from results
            urls = self.LINKEDIN_URL_PATTERN.findall(response.text)
            unique_urls = list(dict.fromkeys(urls))  # Remove duplicates, keep order

            matches = []
            for i, username in enumerate(unique_urls[:5]):  # Top 5 results
                url = f"https://www.linkedin.com/in/{username}"

                # Determine confidence based on position and name match
                confidence = "low"
                match_reason = "Found in search results"

                # Check if username contains parts of the name
                name_parts = full_name.lower().split()
                username_lower = username.lower().replace("-", " ")

                name_match_count = sum(
                    1 for part in name_parts if len(part) > 2 and part in username_lower
                )

                if name_match_count >= 2:
                    confidence = "high"
                    match_reason = "Name matches URL"
                elif name_match_count >= 1:
                    confidence = "medium"
                    match_reason = "Partial name match"
                elif i == 0:
                    confidence = "medium"
                    match_reason = "Top search result"

                matches.append(
                    LinkedInMatch(
                        url=url,
                        name_from_url=username.replace("-", " ").title(),
                        confidence=confidence,
                        match_reason=match_reason,
                    )
                )

            return matches

        except httpx.HTTPError as e:
            logger.warning(f"Error searching for LinkedIn profile: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in LinkedIn search: {e}")
            return []

    def match_name_to_url(
        self,
        full_name: str,
        linkedin_url: str,
    ) -> float:
        """
        Calculate how well a name matches a LinkedIn URL.

        Returns confidence score 0.0 to 1.0.
        """
        if not full_name or not linkedin_url:
            return 0.0

        username = self.extract_username(linkedin_url)
        if not username:
            return 0.0

        # Normalize both
        name_parts = set(full_name.lower().split())
        url_parts = set(username.lower().replace("-", " ").split())

        # Remove very short parts (initials, etc.)
        name_parts = {p for p in name_parts if len(p) > 2}
        url_parts = {p for p in url_parts if len(p) > 2}

        if not name_parts:
            return 0.0

        # Calculate overlap
        overlap = len(name_parts & url_parts)
        score = overlap / len(name_parts)

        return min(score, 1.0)

    async def validate_profile_exists(self, linkedin_url: str) -> bool:
        """
        Check if a LinkedIn profile URL is valid (returns 200).

        Note: LinkedIn may block automated requests. Use sparingly.
        """
        if not self.validate_url_format(linkedin_url):
            return False

        normalized_url = self.normalize_url(linkedin_url)
        if not normalized_url:
            return False

        try:
            client = await self._get_client()
            response = await client.head(
                normalized_url,
                follow_redirects=True,
            )
            # LinkedIn returns 200 for valid profiles
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Error validating LinkedIn URL: {e}")
            return False


class LinkedInEnricher:
    """
    Enriches person records with LinkedIn data.
    """

    def __init__(self):
        self.validator = LinkedInValidator()

    async def find_linkedin_for_person(
        self,
        full_name: str,
        company_name: Optional[str] = None,
        title: Optional[str] = None,
        existing_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Find or validate LinkedIn profile for a person.
        """
        result = {
            "full_name": full_name,
            "company_name": company_name,
            "existing_url": existing_url,
            "validated_url": None,
            "confidence": None,
            "alternatives": [],
        }

        # If existing URL, validate it
        if existing_url:
            normalized = self.validator.normalize_url(existing_url)
            if normalized:
                match_score = self.validator.match_name_to_url(full_name, normalized)
                result["validated_url"] = normalized
                result["confidence"] = (
                    "high"
                    if match_score > 0.5
                    else "medium"
                    if match_score > 0.25
                    else "low"
                )
                return result

        # Search for profile
        matches = await self.validator.search_linkedin_profile(
            full_name, company_name, title
        )

        if matches:
            # Best match
            best = matches[0]
            result["validated_url"] = best.url
            result["confidence"] = best.confidence

            # Alternatives
            result["alternatives"] = [
                {
                    "url": m.url,
                    "confidence": m.confidence,
                    "reason": m.match_reason,
                }
                for m in matches[1:4]  # Up to 3 alternatives
            ]

        return result

    async def close(self):
        """Close resources."""
        await self.validator.close()
