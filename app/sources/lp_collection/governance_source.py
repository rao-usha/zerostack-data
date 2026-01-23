"""
Governance data collector for LP funds.

Collects board members, trustees, and committee information from:
- LP official websites (board/about pages)
- Annual reports
- SEC filings

IMPORTANT: Only collects publicly available information.
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# Patterns to identify governance pages
GOVERNANCE_PAGE_PATTERNS = [
    "/board",
    "/trustees",
    "/leadership",
    "/governance",
    "/about-us",
    "/about",
    "/our-team",
    "/who-we-are",
    "/board-of-trustees",
    "/board-of-directors",
    "/investment-committee",
]

# Role patterns for extraction
GOVERNANCE_ROLE_PATTERNS = {
    "board_chair": [
        r"chair(?:man|woman|person)?(?:\s+of\s+the\s+board)?",
        r"presiding\s+(?:member|officer)",
    ],
    "board_member": [
        r"board\s+member",
        r"director",
        r"member\s+of\s+the\s+board",
    ],
    "trustee": [
        r"trustee",
        r"board\s+of\s+trustees",
    ],
    "investment_committee_chair": [
        r"investment\s+committee\s+chair",
        r"chair.*investment\s+committee",
    ],
    "investment_committee_member": [
        r"investment\s+committee(?:\s+member)?",
    ],
    "executive_director": [
        r"executive\s+director",
        r"ceo",
        r"chief\s+executive",
    ],
}

# Representation patterns
REPRESENTATION_PATTERNS = [
    (r"state\s+treasurer", "State Treasurer"),
    (r"retiree\s+representative", "Retiree Representative"),
    (r"public\s+member", "Public Member"),
    (r"governor(?:'s)?\s+appoint", "Governor Appointee"),
    (r"legislature|legislative", "Legislative Appointee"),
    (r"employee\s+representative", "Employee Representative"),
    (r"beneficiary", "Beneficiary Representative"),
    (r"ex\s*-?\s*officio", "Ex Officio"),
]


class GovernanceCollector(BaseCollector):
    """
    Collects governance structure data from LP sources.

    Extracts:
    - Board/trustee members
    - Committee memberships
    - Tenure information
    - Meeting schedules and documents
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.WEBSITE  # Uses website crawling

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect governance data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL

        Returns:
            CollectionResult with governance items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting governance data for {lp_name}")

        if not website_url:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No website URL provided",
                started_at=started_at,
            )

        try:
            # Find and parse governance pages
            governance_items = await self._collect_governance_members(
                website_url, lp_id, lp_name
            )
            items.extend(governance_items)

            # Collect meeting information
            meeting_items = await self._collect_meeting_info(
                website_url, lp_id, lp_name
            )
            items.extend(meeting_items)

            success = len(items) > 0
            if not items:
                warnings.append("No governance information found")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting governance for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _collect_governance_members(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Find and parse governance pages for board/trustee members."""
        items = []

        # Try common governance page patterns
        for pattern in GOVERNANCE_PAGE_PATTERNS:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                # Extract members from this page
                members = self._extract_governance_members(
                    response.text, page_url, lp_id, lp_name
                )
                items.extend(members)

                if members:
                    logger.info(
                        f"Found {len(members)} governance members at {page_url}"
                    )
                    break  # Found governance page, stop searching

        return items

    def _extract_governance_members(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Extract governance member information from HTML."""
        items = []
        html_lower = html.lower()

        # Common patterns for member listings
        # Pattern 1: Name followed by title/role
        name_title_pattern = re.compile(
            r'<(?:h[2-4]|strong|b)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*</(?:h[2-4]|strong|b)>'
            r'[^<]*<[^>]*>\s*([^<]+)',
            re.IGNORECASE
        )

        # Pattern 2: Card/list item with name and role
        card_pattern = re.compile(
            r'class="[^"]*(?:member|trustee|director|team)[^"]*"[^>]*>.*?'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'.*?(?:title|role|position)[^>]*>([^<]+)',
            re.IGNORECASE | re.DOTALL
        )

        seen_names = set()

        # Try pattern 1
        for match in name_title_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()

            if self._is_valid_name(name) and name not in seen_names:
                seen_names.add(name)
                item = self._create_governance_item(
                    lp_id, lp_name, name, title, source_url
                )
                if item:
                    items.append(item)

        # Try pattern 2 if pattern 1 didn't find much
        if len(items) < 3:
            for match in card_pattern.finditer(html):
                name = match.group(1).strip()
                title = match.group(2).strip()

                if self._is_valid_name(name) and name not in seen_names:
                    seen_names.add(name)
                    item = self._create_governance_item(
                        lp_id, lp_name, name, title, source_url
                    )
                    if item:
                        items.append(item)

        return items

    def _is_valid_name(self, name: str) -> bool:
        """Check if extracted string looks like a person's name."""
        if not name or len(name) < 5:
            return False

        # Must have at least first and last name
        parts = name.split()
        if len(parts) < 2:
            return False

        # Check for common non-name patterns
        non_names = [
            "board", "committee", "member", "trustee", "director",
            "about", "contact", "home", "login", "menu", "search",
            "privacy", "terms", "copyright", "read more"
        ]
        name_lower = name.lower()
        if any(non in name_lower for non in non_names):
            return False

        return True

    def _create_governance_item(
        self,
        lp_id: int,
        lp_name: str,
        name: str,
        title: str,
        source_url: str,
    ) -> Optional[CollectedItem]:
        """Create a governance member CollectedItem."""
        # Determine governance role from title
        role = self._categorize_governance_role(title)
        if not role:
            return None

        # Determine who they represent
        representing = self._extract_representation(title)

        return CollectedItem(
            item_type="governance_member",
            data={
                "lp_id": lp_id,
                "lp_name": lp_name,
                "full_name": name,
                "title": title[:200] if title else None,
                "governance_role": role,
                "representing": representing,
                "is_current": 1,
                "source_type": "website",
            },
            source_url=source_url,
            confidence="medium",
        )

    def _categorize_governance_role(self, title: str) -> Optional[str]:
        """Categorize title into governance role."""
        if not title:
            return None

        title_lower = title.lower()

        for role, patterns in GOVERNANCE_ROLE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower):
                    return role

        # Default to board_member if title contains relevant keywords
        if any(kw in title_lower for kw in ["board", "trustee", "director"]):
            return "board_member"

        return None

    def _extract_representation(self, title: str) -> Optional[str]:
        """Extract who the member represents from their title."""
        if not title:
            return None

        title_lower = title.lower()

        for pattern, representation in REPRESENTATION_PATTERNS:
            if re.search(pattern, title_lower):
                return representation

        return None

    async def _collect_meeting_info(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Collect board meeting information."""
        items = []

        # Common meeting page patterns
        meeting_patterns = [
            "/meetings",
            "/board-meetings",
            "/calendar",
            "/agendas",
            "/minutes",
            "/board/meetings",
        ]

        for pattern in meeting_patterns:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                meetings = self._extract_meetings(
                    response.text, page_url, lp_id, lp_name
                )
                items.extend(meetings)

                if meetings:
                    logger.info(f"Found {len(meetings)} meetings at {page_url}")
                    break

        return items

    def _extract_meetings(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Extract meeting information from HTML."""
        items = []

        # Look for date patterns near meeting-related text
        date_pattern = re.compile(
            r'(\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})'
        )

        # Look for document links (PDFs)
        pdf_pattern = re.compile(
            r'href=["\']([^"\']+\.pdf)["\'][^>]*>([^<]*(?:agenda|minutes|meeting)[^<]*)',
            re.IGNORECASE
        )

        # Extract PDF meeting documents
        for match in pdf_pattern.finditer(html):
            pdf_url = match.group(1)
            link_text = match.group(2).strip()

            # Determine meeting type from link text
            meeting_type = "board_regular"
            link_lower = link_text.lower()
            if "special" in link_lower:
                meeting_type = "board_special"
            elif "investment" in link_lower:
                meeting_type = "investment_committee"
            elif "audit" in link_lower:
                meeting_type = "audit_committee"

            # Try to extract date
            meeting_date = None
            context_start = max(0, match.start() - 100)
            context = html[context_start:match.end() + 100]
            date_match = date_pattern.search(context)
            if date_match:
                meeting_date = self._parse_date(date_match.group(1))

            # Construct full URL if relative
            if not pdf_url.startswith("http"):
                base = source_url.rsplit("/", 1)[0]
                pdf_url = f"{base}/{pdf_url.lstrip('/')}"

            # Determine if agenda or minutes
            is_agenda = "agenda" in link_lower
            is_minutes = "minute" in link_lower

            item = CollectedItem(
                item_type="board_meeting",
                data={
                    "lp_id": lp_id,
                    "lp_name": lp_name,
                    "meeting_date": meeting_date.isoformat() if meeting_date else None,
                    "meeting_type": meeting_type,
                    "meeting_title": link_text[:200],
                    "agenda_url": pdf_url if is_agenda else None,
                    "minutes_url": pdf_url if is_minutes else None,
                    "materials_url": pdf_url if not is_agenda and not is_minutes else None,
                    "source_type": "website",
                },
                source_url=source_url,
                confidence="medium",
            )
            items.append(item)

        return items[:20]  # Limit to recent meetings

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        formats = [
            "%B %d, %Y",
            "%B %d %Y",
            "%b %d, %Y",
            "%m/%d/%Y",
            "%m/%d/%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        return None
