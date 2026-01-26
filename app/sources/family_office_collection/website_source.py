"""
Website collector for Family Office data.

Collects publicly available information from family office websites:
- Investment team / principals
- Investment focus and sectors
- Portfolio companies (if disclosed)
- Contact information
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.types import (
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
)

logger = logging.getLogger(__name__)


# Common team/about page patterns
TEAM_PAGE_PATTERNS = [
    "/team",
    "/about",
    "/about-us",
    "/our-team",
    "/leadership",
    "/people",
    "/principals",
    "/who-we-are",
]

# Common portfolio/investments page patterns
PORTFOLIO_PAGE_PATTERNS = [
    "/portfolio",
    "/investments",
    "/companies",
    "/portfolio-companies",
    "/our-investments",
    "/select-investments",
]


class FoWebsiteCollector(FoBaseCollector):
    """
    Collects family office data from websites.

    Extracts:
    - Team members and principals
    - Investment focus and strategy
    - Portfolio companies (if disclosed)
    - Contact information
    """

    @property
    def source_type(self) -> FoCollectionSource:
        return FoCollectionSource.WEBSITE

    async def collect(
        self,
        fo_id: int,
        fo_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> FoCollectionResult:
        """
        Collect data from family office website.

        Args:
            fo_id: Family office ID
            fo_name: Family office name
            website_url: FO website URL

        Returns:
            FoCollectionResult with website-extracted data
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[FoCollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting website data for {fo_name}")

        if not website_url:
            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=False,
                error_message="No website URL provided",
                started_at=started_at,
            )

        try:
            # Collect team members
            team_items = await self._collect_team_members(
                website_url, fo_id, fo_name
            )
            items.extend(team_items)

            # Collect portfolio companies
            portfolio_items = await self._collect_portfolio(
                website_url, fo_id, fo_name
            )
            items.extend(portfolio_items)

            # Collect contact info from main page
            contact_items = await self._collect_contact_info(
                website_url, fo_id, fo_name
            )
            items.extend(contact_items)

            success = len(items) > 0
            if not items:
                warnings.append("No data found on website")

            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting website data for {fo_name}: {e}")
            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _collect_team_members(
        self,
        website_url: str,
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Collect team member information."""
        items = []

        for pattern in TEAM_PAGE_PATTERNS:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                members = self._extract_team_members(
                    response.text, page_url, fo_id, fo_name
                )
                items.extend(members)

                if members:
                    logger.info(f"Found {len(members)} team members at {page_url}")
                    break  # Found team page

        return items

    def _extract_team_members(
        self,
        html: str,
        source_url: str,
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Extract team member information from HTML."""
        items = []
        seen_names = set()

        # Pattern 1: Name in heading followed by title
        name_title_pattern = re.compile(
            r'<(?:h[2-4]|strong|b)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*</(?:h[2-4]|strong|b)>'
            r'[^<]*<[^>]*>\s*([^<]{3,100})',
            re.IGNORECASE
        )

        # Pattern 2: Card/list item with name and role
        card_pattern = re.compile(
            r'class="[^"]*(?:team|member|person|principal)[^"]*"[^>]*>.*?'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'.*?(?:title|role|position)[^>]*>([^<]+)',
            re.IGNORECASE | re.DOTALL
        )

        for match in name_title_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()

            if self._is_valid_name(name) and name not in seen_names:
                seen_names.add(name)
                role = self._categorize_fo_role(title)

                if role:
                    items.append(FoCollectedItem(
                        item_type="team_member",
                        data={
                            "fo_id": fo_id,
                            "fo_name": fo_name,
                            "full_name": name,
                            "title": title[:200],
                            "role_category": role,
                            "source_type": "website",
                        },
                        source_url=source_url,
                        confidence="medium",
                    ))

        # Try card pattern if not enough found
        if len(items) < 3:
            for match in card_pattern.finditer(html):
                name = match.group(1).strip()
                title = match.group(2).strip()

                if self._is_valid_name(name) and name not in seen_names:
                    seen_names.add(name)
                    role = self._categorize_fo_role(title)

                    if role:
                        items.append(FoCollectedItem(
                            item_type="team_member",
                            data={
                                "fo_id": fo_id,
                                "fo_name": fo_name,
                                "full_name": name,
                                "title": title[:200],
                                "role_category": role,
                                "source_type": "website",
                            },
                            source_url=source_url,
                            confidence="medium",
                        ))

        return items

    def _is_valid_name(self, name: str) -> bool:
        """Check if extracted string looks like a person's name."""
        if not name or len(name) < 5:
            return False

        parts = name.split()
        if len(parts) < 2:
            return False

        # Check for common non-name patterns
        non_names = [
            "about", "contact", "home", "login", "menu", "search",
            "privacy", "terms", "copyright", "read more", "learn more",
            "portfolio", "investment", "company",
        ]
        name_lower = name.lower()
        if any(non in name_lower for non in non_names):
            return False

        return True

    def _categorize_fo_role(self, title: str) -> Optional[str]:
        """Categorize title into role category."""
        if not title:
            return None

        title_lower = title.lower()

        # Founder/Principal roles
        if any(kw in title_lower for kw in ["founder", "principal", "owner"]):
            return "Principal"

        # C-level
        if any(kw in title_lower for kw in ["ceo", "chief executive"]):
            return "CEO"
        if any(kw in title_lower for kw in ["cio", "chief investment"]):
            return "CIO"
        if any(kw in title_lower for kw in ["cfo", "chief financial"]):
            return "CFO"
        if any(kw in title_lower for kw in ["coo", "chief operating"]):
            return "COO"

        # Investment roles
        if any(kw in title_lower for kw in ["managing director", "managing partner"]):
            return "Managing Director"
        if any(kw in title_lower for kw in ["partner"]):
            return "Partner"
        if any(kw in title_lower for kw in ["director"]):
            return "Director"
        if any(kw in title_lower for kw in ["analyst", "associate"]):
            return "Investment Team"

        return None

    async def _collect_portfolio(
        self,
        website_url: str,
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Collect portfolio company information."""
        items = []

        for pattern in PORTFOLIO_PAGE_PATTERNS:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                companies = self._extract_portfolio_companies(
                    response.text, page_url, fo_id, fo_name
                )
                items.extend(companies)

                if companies:
                    logger.info(f"Found {len(companies)} portfolio companies at {page_url}")
                    break

        return items

    def _extract_portfolio_companies(
        self,
        html: str,
        source_url: str,
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Extract portfolio company information from HTML."""
        items = []
        seen_companies = set()

        # Pattern for company cards/logos with links
        company_pattern = re.compile(
            r'<(?:a|div)[^>]*(?:href="([^"]+)")?[^>]*class="[^"]*(?:portfolio|company|investment)[^"]*"[^>]*>'
            r'.*?(?:<img[^>]*alt="([^"]+)"[^>]*>|<h[2-5][^>]*>([^<]+)</h[2-5]>)',
            re.IGNORECASE | re.DOTALL
        )

        # Simpler pattern for company names in lists
        list_pattern = re.compile(
            r'<li[^>]*class="[^"]*(?:portfolio|company)[^"]*"[^>]*>.*?'
            r'(?:<a[^>]*>([^<]+)</a>|([^<]+))</li>',
            re.IGNORECASE | re.DOTALL
        )

        for match in company_pattern.finditer(html):
            company_url = match.group(1)
            company_name = match.group(2) or match.group(3)

            if company_name:
                company_name = company_name.strip()

                if len(company_name) > 2 and company_name not in seen_companies:
                    seen_companies.add(company_name)
                    items.append(FoCollectedItem(
                        item_type="portfolio_company",
                        data={
                            "fo_id": fo_id,
                            "fo_name": fo_name,
                            "company_name": company_name,
                            "company_url": company_url,
                            "source_type": "website",
                        },
                        source_url=source_url,
                        confidence="medium",
                    ))

        # Try list pattern
        for match in list_pattern.finditer(html):
            company_name = match.group(1) or match.group(2)

            if company_name:
                company_name = company_name.strip()

                if len(company_name) > 2 and company_name not in seen_companies:
                    seen_companies.add(company_name)
                    items.append(FoCollectedItem(
                        item_type="portfolio_company",
                        data={
                            "fo_id": fo_id,
                            "fo_name": fo_name,
                            "company_name": company_name,
                            "source_type": "website",
                        },
                        source_url=source_url,
                        confidence="low",
                    ))

        return items[:50]  # Limit

    async def _collect_contact_info(
        self,
        website_url: str,
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Collect contact information from website."""
        items = []

        response = await self._fetch_url(website_url)
        if not response or response.status_code != 200:
            return items

        html = response.text

        # Extract email addresses
        email_pattern = re.compile(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        )
        emails = set(email_pattern.findall(html))

        # Filter out common non-contact emails
        filtered_emails = [
            e for e in emails
            if not any(x in e.lower() for x in ['support', 'noreply', 'unsubscribe', 'example'])
        ]

        for email in filtered_emails[:3]:  # Limit
            items.append(FoCollectedItem(
                item_type="contact_info",
                data={
                    "fo_id": fo_id,
                    "fo_name": fo_name,
                    "email": email,
                    "source_type": "website",
                },
                source_url=website_url,
                confidence="medium",
            ))

        # Extract phone numbers
        phone_pattern = re.compile(
            r'(?:\+1|1)?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})'
        )
        for match in phone_pattern.finditer(html):
            phone = f"({match.group(1)}) {match.group(2)}-{match.group(3)}"
            items.append(FoCollectedItem(
                item_type="contact_info",
                data={
                    "fo_id": fo_id,
                    "fo_name": fo_name,
                    "phone": phone,
                    "source_type": "website",
                },
                source_url=website_url,
                confidence="low",
            ))
            break  # Only first phone

        return items
