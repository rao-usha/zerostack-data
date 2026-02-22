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

        Uses BeautifulSoup to extract names and titles from common HTML
        patterns (card grids, heading+subtitle pairs, figure/figcaption,
        list items). Falls back to regex if BS4 is unavailable.

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

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            # Fallback: regex extraction
            return self._scrape_team_page_regex(html)

        soup = BeautifulSoup(html, "html.parser")

        # Remove noisy elements
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        seen_people: set = set()

        # Strategy 1: Card grids — divs/articles/lis with a heading + subtitle
        card_selectors = [
            "div.team-member", "div.person", "div.member", "div.staff",
            "div.team-card", "div.people-card", "div.bio-card",
            "article.team-member", "article.person",
            "li.team-member", "li.person",
        ]
        for sel in card_selectors:
            for card in soup.select(sel):
                # Prefer headings (they contain the name); <a> may be empty links
                name_el = card.find(["h2", "h3", "h4", "h5"]) or card.find(["a", "strong"])
                if not name_el:
                    continue

                # Extract name: use only direct text, excluding <small>/<span> children
                # which often contain the title (e.g. <h2>Name<small>Title</small></h2>)
                small = name_el.find("small")
                if small:
                    title_from_small = small.get_text(strip=True)
                    small.extract()  # temporarily remove to get clean name
                    name = name_el.get_text(strip=True)
                else:
                    title_from_small = ""
                    name = name_el.get_text(strip=True)

                # Find title: <small> inside heading > class-matched p/span > first <p>
                # Exclude <div> from class match — divs with "title" in class are
                # usually containers (e.g. "person--title"), not the title itself.
                if title_from_small:
                    title = title_from_small
                else:
                    # Try p/span with title/role class first
                    title_el = card.find(["p", "span"], class_=re.compile(
                        r"title|role|position|designation", re.I
                    ))
                    if title_el:
                        title = title_el.get_text(strip=True)
                    else:
                        first_p = card.find("p")
                        title = first_p.get_text(strip=True) if first_p else ""

                if self._looks_like_name(name) and name not in seen_people:
                    seen_people.add(name)
                    people.append({"full_name": name, "title": title, "source_type": "team_page"})

        # Strategy 2: Heading (h2-h5) immediately followed by a p/span/div sibling
        if not people:
            for heading in soup.find_all(["h2", "h3", "h4", "h5"]):
                name = heading.get_text(strip=True)
                if not self._looks_like_name(name) or name in seen_people:
                    continue
                # Next sibling element may contain the title
                sibling = heading.find_next_sibling(["p", "span", "div"])
                title = sibling.get_text(strip=True) if sibling else ""
                # Skip if title looks like a long paragraph (>80 chars)
                if len(title) > 80:
                    title = ""
                seen_people.add(name)
                people.append({"full_name": name, "title": title, "source_type": "team_page"})

        # Strategy 3: figure/figcaption (image + caption pattern)
        if not people:
            for fig in soup.find_all("figure"):
                caption = fig.find("figcaption")
                if not caption:
                    continue
                lines = [l.strip() for l in caption.get_text(separator="\n").split("\n") if l.strip()]
                if lines and self._looks_like_name(lines[0]):
                    name = lines[0]
                    title = lines[1] if len(lines) > 1 else ""
                    if name not in seen_people:
                        seen_people.add(name)
                        people.append({"full_name": name, "title": title, "source_type": "team_page"})

        # Strategy 4: Any element with a class/id containing "name" paired with "title"
        if not people:
            name_els = soup.find_all(
                class_=re.compile(r"\bname\b", re.I)
            )
            for name_el in name_els:
                name = name_el.get_text(strip=True)
                if not self._looks_like_name(name) or name in seen_people:
                    continue
                # Look for a sibling or nearby element with title/role class
                parent = name_el.parent
                title_el = parent.find(
                    class_=re.compile(r"title|role|position", re.I)
                ) if parent else None
                title = title_el.get_text(strip=True) if title_el else ""
                seen_people.add(name)
                people.append({"full_name": name, "title": title, "source_type": "team_page"})

        # Final fallback: regex
        if not people:
            people = self._scrape_team_page_regex(html)

        return people[:100]

    def _scrape_team_page_regex(self, html: str) -> List[Dict[str, Any]]:
        """Regex fallback for team page extraction."""
        people = []
        seen_people: set = set()
        pattern = r"<h[2-4][^>]*>([A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?)</h[2-4]>\s*<(?:p|span|div)[^>]*>([^<]+)</(?:p|span|div)>"
        matches = re.findall(pattern, html)
        for match in matches:
            if len(match) >= 2:
                name = match[0].strip()
                title = match[1].strip()
                if name and name not in seen_people:
                    seen_people.add(name)
                    people.append({"full_name": name, "title": title, "source_type": "team_page"})
        return people[:100]

    @staticmethod
    def _looks_like_name(text: str) -> bool:
        """Check if text looks like a person's name (2-4 capitalized words, <60 chars)."""
        if not text or len(text) > 60 or len(text) < 3:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 5:
            return False
        # At least first and last word should start with uppercase
        if not words[0][0].isupper() or not words[-1][0].isupper():
            return False
        # Should not contain common non-name indicators
        lower = text.lower()
        non_name = ["about", "team", "our", "the", "view", "read", "more", "contact", "learn"]
        if any(w in lower for w in non_name):
            return False
        return True
