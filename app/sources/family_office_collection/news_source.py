"""
News collector for Family Office data.

Monitors news sources for:
- Deal announcements
- Investment activity
- Personnel changes
- Strategic updates

Uses free news APIs and web scraping.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional, List

from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.types import (
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
)

logger = logging.getLogger(__name__)


# News source patterns
DEAL_KEYWORDS = [
    "investment",
    "invested",
    "led round",
    "participated",
    "acquired",
    "acquisition",
    "backed",
    "funding",
    "series a",
    "series b",
    "series c",
    "series d",
    "seed round",
    "growth equity",
    "stake in",
]

PERSONNEL_KEYWORDS = [
    "hired",
    "appointed",
    "promoted",
    "joined",
    "new partner",
    "new director",
    "departing",
]


class FoNewsCollector(FoBaseCollector):
    """
    Collects family office news and deal activity.

    Sources:
    - Press releases
    - Tech/finance news sites
    - Deal databases (public APIs)
    """

    @property
    def source_type(self) -> FoCollectionSource:
        return FoCollectionSource.NEWS

    async def collect(
        self,
        fo_id: int,
        fo_name: str,
        website_url: Optional[str] = None,
        principal_name: Optional[str] = None,
        days_back: int = 30,
        **kwargs,
    ) -> FoCollectionResult:
        """
        Collect news about a family office.

        Args:
            fo_id: Family office ID
            fo_name: Family office name
            website_url: FO website (not directly used)
            principal_name: Principal's name for additional search
            days_back: How many days of news to search

        Returns:
            FoCollectionResult with news items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[FoCollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting news for {fo_name}")

        try:
            # Search Google News RSS
            news_items = await self._search_news(fo_name, days_back)
            items.extend(news_items)

            # Search for principal if provided
            if principal_name and principal_name != fo_name:
                principal_items = await self._search_news(principal_name, days_back)
                # Filter to avoid duplicates
                existing_urls = {i.source_url for i in items}
                for item in principal_items:
                    if item.source_url not in existing_urls:
                        items.append(item)

            # Categorize and enrich items
            items = self._categorize_news_items(items, fo_id, fo_name)

            success = len(items) > 0
            if not items:
                warnings.append("No recent news found")

            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting news for {fo_name}: {e}")
            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _search_news(
        self,
        query: str,
        days_back: int = 30,
    ) -> List[FoCollectedItem]:
        """Search for news articles about a family office."""
        items = []

        # Use Google News RSS (free, no API key needed)
        # Note: This is rate-limited and may not work for all queries
        encoded_query = query.replace(" ", "+")
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

        response = await self._fetch_url(rss_url)
        if not response or response.status_code != 200:
            return items

        # Parse RSS feed
        items = self._parse_rss_feed(response.text, query)

        # Filter by date
        cutoff = datetime.utcnow() - timedelta(days=days_back)
        items = [i for i in items if self._item_is_recent(i, cutoff)]

        return items[:20]  # Limit results

    def _parse_rss_feed(
        self, xml_content: str, search_query: str
    ) -> List[FoCollectedItem]:
        """Parse Google News RSS feed."""
        items = []

        # Simple regex parsing (avoid full XML parsing for speed)
        item_pattern = re.compile(
            r"<item>.*?<title>([^<]+)</title>.*?<link>([^<]+)</link>.*?<pubDate>([^<]+)</pubDate>.*?</item>",
            re.DOTALL,
        )

        for match in item_pattern.finditer(xml_content):
            title = match.group(1).strip()
            link = match.group(2).strip()
            pub_date = match.group(3).strip()

            # Clean up title (remove CDATA if present)
            title = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", title)

            items.append(
                FoCollectedItem(
                    item_type="news_item",
                    data={
                        "title": title,
                        "pub_date": pub_date,
                        "search_query": search_query,
                    },
                    source_url=link,
                    confidence="medium",
                )
            )

        return items

    def _item_is_recent(self, item: FoCollectedItem, cutoff: datetime) -> bool:
        """Check if a news item is recent enough."""
        pub_date = item.data.get("pub_date")
        if not pub_date:
            return True  # Include if no date

        try:
            # Parse RSS date format
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(pub_date)
            return dt.replace(tzinfo=None) >= cutoff
        except Exception:
            return True  # Include if can't parse

    def _categorize_news_items(
        self,
        items: List[FoCollectedItem],
        fo_id: int,
        fo_name: str,
    ) -> List[FoCollectedItem]:
        """Categorize news items by type (deal, personnel, general)."""
        categorized = []

        for item in items:
            title = item.data.get("title", "").lower()

            # Determine category
            category = "general"
            confidence = "low"

            if any(kw in title for kw in DEAL_KEYWORDS):
                category = "deal"
                confidence = "medium"

                # Try to extract company name from title
                company = self._extract_company_from_deal(item.data.get("title", ""))
                if company:
                    item.data["extracted_company"] = company
                    confidence = "high"

            elif any(kw in title for kw in PERSONNEL_KEYWORDS):
                category = "personnel"
                confidence = "medium"

            item.data["news_category"] = category
            item.data["fo_id"] = fo_id
            item.data["fo_name"] = fo_name
            item.confidence = confidence

            categorized.append(item)

        return categorized

    def _extract_company_from_deal(self, title: str) -> Optional[str]:
        """Try to extract company name from deal headline."""
        # Common patterns:
        # "Family Office leads Series A in [Company]"
        # "[Company] raises $X from Family Office"
        # "Family Office invests in [Company]"

        patterns = [
            r"(?:invests? in|backs?|leads? (?:series \w+ in)?|participates? in)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)",
            r"([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)\s+raises",
            r"([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)\s+announces?\s+funding",
        ]

        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                company = match.group(1).strip()
                # Filter out common false positives
                if company.lower() not in ["the", "a", "series", "family", "office"]:
                    return company

        return None
