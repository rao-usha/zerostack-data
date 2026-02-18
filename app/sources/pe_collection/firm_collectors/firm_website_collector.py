"""
PE Firm Website Collector.

Scrapes PE firm websites to extract:
- Portfolio companies
- Team members
- Fund information
- News and press releases
"""

import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)

logger = logging.getLogger(__name__)


class FirmWebsiteCollector(BasePECollector):
    """
    Collects data by scraping PE firm websites.

    Extracts:
    - Portfolio company list
    - Team/people information
    - Investment focus/strategy
    - Contact information
    """

    # Common URL patterns for PE firm sections
    PORTFOLIO_PATTERNS = [
        "/portfolio",
        "/companies",
        "/investments",
        "/our-portfolio",
        "/portfolio-companies",
    ]

    TEAM_PATTERNS = [
        "/team",
        "/people",
        "/leadership",
        "/our-team",
        "/about/team",
        "/professionals",
    ]

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.FIRM_WEBSITE

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect data from a PE firm website.

        Args:
            entity_id: PE firm ID in our database
            entity_name: Firm name
            website_url: Firm website URL

        Returns:
            Collection result with website data items
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
                error_message="No website URL provided",
                started_at=started_at,
            )

        try:
            # Normalize URL
            if not website_url.startswith(("http://", "https://")):
                website_url = f"https://{website_url}"

            # Fetch main page
            main_page = await self._fetch_url(website_url)
            if not main_page or main_page.status_code != 200:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=False,
                    error_message=f"Failed to fetch website: {website_url}",
                    started_at=started_at,
                )

            main_html = main_page.text

            # Extract main page data
            main_data = self._extract_main_page_data(main_html, website_url)
            if main_data:
                items.append(
                    self._create_item(
                        item_type="firm_update",
                        data=main_data,
                        source_url=website_url,
                        confidence="medium",
                    )
                )

            # Find and scrape portfolio page
            portfolio_url = self._find_section_url(
                main_html, website_url, self.PORTFOLIO_PATTERNS
            )
            if portfolio_url:
                portfolio_data = await self._scrape_portfolio_page(portfolio_url)
                for company in portfolio_data:
                    items.append(
                        self._create_item(
                            item_type="portfolio_company",
                            data=company,
                            source_url=portfolio_url,
                            confidence="medium",
                        )
                    )

            # Find and scrape team page
            team_url = self._find_section_url(
                main_html, website_url, self.TEAM_PATTERNS
            )
            if team_url:
                team_data = await self._scrape_team_page(team_url)
                for person in team_data:
                    items.append(
                        self._create_item(
                            item_type="team_member",
                            data=person,
                            source_url=team_url,
                            confidence="medium",
                        )
                    )

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error scraping website for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    def _find_section_url(
        self, html: str, base_url: str, patterns: List[str]
    ) -> Optional[str]:
        """
        Find a section URL in the page HTML.

        Args:
            html: Page HTML content
            base_url: Base URL for resolving relative links
            patterns: URL patterns to look for

        Returns:
            Absolute URL if found, None otherwise
        """
        html_lower = html.lower()

        for pattern in patterns:
            # Look for href containing the pattern
            pattern_escaped = re.escape(pattern)
            match = re.search(
                rf'href=["\']([^"\']*{pattern_escaped}[^"\']*)["\']',
                html_lower,
            )
            if match:
                href = match.group(1)
                return urljoin(base_url, href)

        return None

    def _extract_main_page_data(self, html: str, url: str) -> Dict[str, Any]:
        """
        Extract key data from the main page.

        Args:
            html: Page HTML content
            url: Page URL

        Returns:
            Extracted data dictionary
        """
        data = {}

        # Extract title
        title_match = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
        if title_match:
            data["page_title"] = title_match.group(1).strip()

        # Extract meta description
        desc_match = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        if desc_match:
            data["meta_description"] = desc_match.group(1).strip()

        # Look for social links
        linkedin_match = re.search(
            r'href=["\']([^"\']*linkedin\.com/company/[^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        if linkedin_match:
            data["linkedin_url"] = linkedin_match.group(1)

        twitter_match = re.search(
            r'href=["\']([^"\']*(?:twitter|x)\.com/[^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        if twitter_match:
            data["twitter_url"] = twitter_match.group(1)

        # Look for contact email
        email_match = re.search(
            r"mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
            html,
        )
        if email_match:
            data["contact_email"] = email_match.group(1)

        return data

    async def _scrape_portfolio_page(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape portfolio companies from a portfolio page.

        Args:
            url: Portfolio page URL

        Returns:
            List of portfolio company data
        """
        companies = []

        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return companies

        html = response.text

        # This is a simplified extraction - real implementation would use
        # BeautifulSoup or an LLM for more accurate extraction

        # Look for company names in common patterns
        # Pattern: links to company websites or company names in specific elements
        company_patterns = [
            # Links to external company sites
            r'<a[^>]+href=["\']https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})["\'][^>]*>([^<]+)</a>',
            # Company name in header elements
            r'<h[2-4][^>]*class=["\'][^"\']*(?:company|portfolio)[^"\']*["\'][^>]*>([^<]+)</h[2-4]>',
        ]

        seen_companies = set()
        for pattern in company_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # Pattern with multiple groups
                    if len(match) >= 2:
                        website, name = match[0], match[1]
                        name = name.strip()
                        if name and name not in seen_companies:
                            seen_companies.add(name)
                            companies.append(
                                {
                                    "name": name,
                                    "website": f"https://{website}"
                                    if not website.startswith("http")
                                    else website,
                                    "source_type": "portfolio_page",
                                }
                            )
                else:
                    name = match.strip()
                    if name and name not in seen_companies:
                        seen_companies.add(name)
                        companies.append(
                            {
                                "name": name,
                                "source_type": "portfolio_page",
                            }
                        )

        return companies[:50]  # Limit to 50 companies

    async def _scrape_team_page(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape team members from a team page.

        Args:
            url: Team page URL

        Returns:
            List of team member data
        """
        people = []

        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return people

        html = response.text

        # This is a simplified extraction - real implementation would use
        # BeautifulSoup or an LLM for more accurate extraction

        # Look for people in common patterns
        # Pattern: Name followed by title
        person_patterns = [
            r"<h[2-4][^>]*>([A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?)</h[2-4]>\s*<(?:p|span|div)[^>]*>([^<]+)</(?:p|span|div)>",
        ]

        seen_people = set()
        for pattern in person_patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                if len(match) >= 2:
                    name = match[0].strip()
                    title = match[1].strip()
                    if name and name not in seen_people:
                        seen_people.add(name)
                        people.append(
                            {
                                "full_name": name,
                                "title": title,
                                "source_type": "team_page",
                            }
                        )

        return people[:100]  # Limit to 100 people
