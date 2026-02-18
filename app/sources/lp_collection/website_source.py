"""
Website collector for LP data.

Crawls LP websites to extract:
- Contact information (leadership, investment team)
- Investment strategy updates
- Document links (board materials, reports)
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# Common patterns for finding contact/team pages
TEAM_PAGE_PATTERNS = [
    r"/about/?",
    r"/leadership/?",
    r"/team/?",
    r"/staff/?",
    r"/board/?",
    r"/our-team/?",
    r"/management/?",
    r"/executives/?",
    r"/people/?",
    r"/investment-team/?",
    r"/investment-staff/?",
]

# Common patterns for finding investment pages
INVESTMENT_PAGE_PATTERNS = [
    r"/investments/?",
    r"/portfolio/?",
    r"/asset-allocation/?",
    r"/investment-strategy/?",
    r"/investment-performance/?",
    r"/returns/?",
    r"/performance/?",
]

# Role keywords for contact extraction
ROLE_KEYWORDS = {
    "CIO": ["chief investment officer", "cio", "investment officer"],
    "CEO": ["chief executive officer", "ceo", "executive director"],
    "CFO": ["chief financial officer", "cfo", "treasurer"],
    "Investment Director": [
        "investment director",
        "director of investments",
        "portfolio director",
    ],
    "Board Member": ["board member", "trustee", "board chair", "chairman"],
    "Managing Director": ["managing director", "md"],
    "IR Contact": ["investor relations", "ir contact"],
}

# Email pattern
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# Phone pattern (US)
PHONE_PATTERN = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")


class WebsiteCollector(BaseCollector):
    """
    Collects LP data from their official websites.

    Extracts:
    - Leadership/team contacts
    - Investment strategy information
    - Document links (PDFs, board materials)
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.WEBSITE

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect data from LP website.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL

        Returns:
            CollectionResult with contacts, documents, and strategy info
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        if not website_url:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No website URL provided",
                started_at=started_at,
            )

        logger.info(f"Collecting website data for {lp_name} from {website_url}")

        try:
            # Fetch homepage
            homepage_response = await self._fetch_url(website_url)
            if not homepage_response or homepage_response.status_code != 200:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message=f"Failed to fetch homepage: {website_url}",
                    started_at=started_at,
                )

            homepage_html = homepage_response.text

            # Find team/contact pages
            team_links = self._find_matching_links(
                homepage_html, website_url, TEAM_PAGE_PATTERNS
            )
            logger.debug(f"Found {len(team_links)} potential team page links")

            # Collect contacts from team pages
            for team_url in team_links[:3]:  # Limit to 3 pages
                contact_items = await self._extract_contacts_from_page(
                    team_url, lp_id, lp_name
                )
                items.extend(contact_items)

            # Find investment/strategy pages
            investment_links = self._find_matching_links(
                homepage_html, website_url, INVESTMENT_PAGE_PATTERNS
            )
            logger.debug(
                f"Found {len(investment_links)} potential investment page links"
            )

            # Extract document links
            doc_items = self._extract_document_links(homepage_html, website_url, lp_id)
            items.extend(doc_items)

            # Basic strategy extraction from homepage
            strategy_item = self._extract_basic_strategy(
                homepage_html, lp_id, website_url
            )
            if strategy_item:
                items.append(strategy_item)

            success = len(items) > 0
            if not items:
                warnings.append("No data extracted from website")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting from {lp_name} website: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    def _find_matching_links(
        self,
        html: str,
        base_url: str,
        patterns: List[str],
    ) -> List[str]:
        """Find links matching the given patterns."""
        links = []
        base_domain = urlparse(base_url).netloc

        # Simple href extraction
        href_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
        for match in href_pattern.finditer(html):
            href = match.group(1)

            # Make absolute URL
            if href.startswith("/"):
                full_url = urljoin(base_url, href)
            elif href.startswith("http"):
                full_url = href
            else:
                continue

            # Check if same domain
            if urlparse(full_url).netloc != base_domain:
                continue

            # Check against patterns
            for pattern in patterns:
                if re.search(pattern, href, re.IGNORECASE):
                    if full_url not in links:
                        links.append(full_url)
                    break

        return links

    async def _extract_contacts_from_page(
        self,
        page_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Extract contact information from a page."""
        items = []

        response = await self._fetch_url(page_url)
        if not response or response.status_code != 200:
            return items

        html = response.text

        # Extract emails
        emails = EMAIL_PATTERN.findall(html)
        emails = list(set(emails))  # Dedupe

        # Extract phones
        phones = PHONE_PATTERN.findall(html)
        phones = list(set(phones))  # Dedupe

        # Try to extract structured contact info
        # This is a simplified extraction - in production, would use
        # more sophisticated HTML parsing (BeautifulSoup, etc.)

        contacts = self._parse_contact_blocks(html, emails, phones)

        for contact in contacts:
            item = CollectedItem(
                item_type="contact",
                data={
                    "lp_id": lp_id,
                    "full_name": contact.get("name", "Unknown"),
                    "title": contact.get("title"),
                    "role_category": contact.get("role_category"),
                    "email": contact.get("email"),
                    "phone": contact.get("phone"),
                    "source_type": "website",
                },
                source_url=page_url,
                confidence="medium" if contact.get("name") else "low",
            )
            items.append(item)

        return items

    def _parse_contact_blocks(
        self,
        html: str,
        emails: List[str],
        phones: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Parse contact blocks from HTML.

        This is a simplified implementation. In production, would use
        proper HTML parsing and potentially ML-based extraction.
        """
        contacts = []

        # Look for name patterns near titles
        # Pattern: Name followed by title keywords
        name_title_pattern = re.compile(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*[,\-]?\s*"
            r"((?:Chief|Director|Managing|Vice|Senior|Executive|Board|Trustee)[^<\n]{5,60})",
            re.IGNORECASE,
        )

        for match in name_title_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()

            # Determine role category
            role_category = self._categorize_role(title)

            contact = {
                "name": name,
                "title": title,
                "role_category": role_category,
            }

            # Try to associate an email
            name_parts = name.lower().split()
            for email in emails:
                email_lower = email.lower()
                if any(part in email_lower for part in name_parts):
                    contact["email"] = email
                    break

            contacts.append(contact)

        return contacts[:20]  # Limit to avoid noise

    def _categorize_role(self, title: str) -> Optional[str]:
        """Categorize a job title into standard role categories."""
        title_lower = title.lower()

        for category, keywords in ROLE_KEYWORDS.items():
            if any(kw in title_lower for kw in keywords):
                return category

        return "Other"

    def _extract_document_links(
        self,
        html: str,
        base_url: str,
        lp_id: int,
    ) -> List[CollectedItem]:
        """Extract links to documents (PDFs, etc.)."""
        items = []

        # Find PDF links
        pdf_pattern = re.compile(r'href=["\']([^"\']+\.pdf)["\']', re.IGNORECASE)
        for match in pdf_pattern.finditer(html):
            href = match.group(1)

            # Make absolute URL
            if href.startswith("/"):
                full_url = urljoin(base_url, href)
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = urljoin(base_url, href)

            # Try to determine document type from filename/path
            doc_type = self._guess_document_type(full_url)

            item = CollectedItem(
                item_type="document_link",
                data={
                    "lp_id": lp_id,
                    "url": full_url,
                    "file_format": "pdf",
                    "document_type": doc_type,
                    "source_type": "website",
                },
                source_url=base_url,
                confidence="low",  # We haven't verified the document
            )
            items.append(item)

        return items[:50]  # Limit to avoid too many

    def _guess_document_type(self, url: str) -> str:
        """Guess document type from URL."""
        url_lower = url.lower()

        if any(kw in url_lower for kw in ["annual", "cafr", "acfr"]):
            return "annual_report"
        if any(kw in url_lower for kw in ["quarterly", "q1", "q2", "q3", "q4"]):
            return "quarterly_report"
        if any(kw in url_lower for kw in ["board", "meeting", "minutes"]):
            return "board_materials"
        if any(kw in url_lower for kw in ["policy", "ips"]):
            return "policy_statement"
        if any(kw in url_lower for kw in ["pacing", "commitment"]):
            return "pacing_plan"

        return "other"

    def _extract_basic_strategy(
        self,
        html: str,
        lp_id: int,
        source_url: str,
    ) -> Optional[CollectedItem]:
        """Extract basic strategy information from page text."""
        # Look for AUM mentions
        aum_pattern = re.compile(
            r"\$?\s*(\d+(?:\.\d+)?)\s*(billion|million|B|M)\s*(?:in\s+)?(?:assets|AUM|under management)",
            re.IGNORECASE,
        )

        match = aum_pattern.search(html)
        if match:
            amount = float(match.group(1))
            unit = match.group(2).lower()

            if unit in ["billion", "b"]:
                aum_billions = amount
            else:
                aum_billions = amount / 1000

            return CollectedItem(
                item_type="strategy_info",
                data={
                    "lp_id": lp_id,
                    "aum_usd_billions": str(aum_billions),
                    "source_type": "website",
                },
                source_url=source_url,
                confidence="medium",
            )

        return None
