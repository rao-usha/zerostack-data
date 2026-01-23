"""
News collector for LP data.

Monitors news and press releases for:
- Allocation changes
- Leadership changes
- Investment announcements
- Policy updates
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# News categories and keywords
NEWS_CATEGORIES = {
    "allocation_change": [
        "asset allocation",
        "portfolio rebalancing",
        "target allocation",
        "overweight",
        "underweight",
        "increased exposure",
        "reduced exposure",
    ],
    "leadership_change": [
        "appointed",
        "named",
        "hired",
        "resigned",
        "retiring",
        "new cio",
        "new ceo",
        "new board member",
    ],
    "investment_announcement": [
        "committed",
        "investment in",
        "allocated to",
        "partnership with",
        "co-investment",
        "direct investment",
    ],
    "performance_update": [
        "returns",
        "performance",
        "benchmark",
        "outperformed",
        "underperformed",
        "fiscal year results",
    ],
    "policy_update": [
        "policy change",
        "esg",
        "divestment",
        "investment policy",
        "proxy voting",
    ],
}


class NewsCollector(BaseCollector):
    """
    Collects LP news and press releases.

    Monitors various sources for LP-related news:
    - LP official news/press releases
    - Financial news (Reuters, Bloomberg, etc.)
    - Industry publications (Pensions & Investments, etc.)
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.NEWS

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        days_back: int = 30,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect news for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL (for press releases)
            days_back: How many days back to search

        Returns:
            CollectionResult with news items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting news for {lp_name}")

        try:
            # Collect from LP website press releases
            if website_url:
                press_items = await self._collect_press_releases(
                    website_url, lp_id, lp_name, days_back
                )
                items.extend(press_items)

            # Collect from external news sources
            external_items = await self._collect_external_news(
                lp_name, lp_id, days_back
            )
            items.extend(external_items)

            success = len(items) > 0
            if not items:
                warnings.append("No recent news found")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting news for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _collect_press_releases(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
        days_back: int,
    ) -> List[CollectedItem]:
        """Collect press releases from LP website."""
        items = []

        # Common press release page patterns
        press_patterns = [
            "/news",
            "/press",
            "/media",
            "/newsroom",
            "/press-releases",
            "/announcements",
        ]

        # Try to find press release page
        for pattern in press_patterns:
            press_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(press_url)

            if response and response.status_code == 200:
                # Found press releases page
                news_items = self._extract_press_releases(
                    response.text, press_url, lp_id, lp_name, days_back
                )
                items.extend(news_items)
                break

        return items

    def _extract_press_releases(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
        days_back: int,
    ) -> List[CollectedItem]:
        """Extract press release items from HTML."""
        items = []
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)

        # Look for news items with dates
        # Pattern: date followed by title/link
        date_pattern = re.compile(
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4})"
        )

        # Find linked items that look like news
        link_pattern = re.compile(
            r'href=["\']([^"\']+)["\'][^>]*>([^<]+)',
            re.IGNORECASE
        )

        for match in link_pattern.finditer(html):
            href = match.group(1)
            link_text = match.group(2).strip()

            # Skip navigation/menu links
            if len(link_text) < 20:
                continue

            # Look for date near the link
            context_start = max(0, match.start() - 200)
            context_end = min(len(html), match.end() + 200)
            context = html[context_start:context_end]

            date_match = date_pattern.search(context)
            pub_date = None
            if date_match:
                pub_date = self._parse_date(date_match.group(1))
                if pub_date and pub_date < cutoff_date:
                    continue  # Too old

            # Categorize the news
            category = self._categorize_news(link_text)

            item = CollectedItem(
                item_type="news",
                data={
                    "lp_id": lp_id,
                    "lp_name": lp_name,
                    "title": link_text[:500],
                    "url": href if href.startswith("http") else f"{source_url.rstrip('/')}/{href.lstrip('/')}",
                    "published_date": pub_date.isoformat() if pub_date else None,
                    "category": category,
                    "source_type": "news",
                },
                source_url=source_url,
                confidence="medium",
            )
            items.append(item)

        return items[:20]  # Limit results

    async def _collect_external_news(
        self,
        lp_name: str,
        lp_id: int,
        days_back: int,
    ) -> List[CollectedItem]:
        """
        Collect news from external sources.

        Note: This is a placeholder. In production, would integrate with
        news APIs (NewsAPI, Google News, etc.) or industry feeds.
        """
        items = []

        # Placeholder for external news API integration
        # In production, would query:
        # - Google News API
        # - Pensions & Investments RSS
        # - Reuters/Bloomberg APIs
        # - PR Newswire

        logger.debug(f"External news collection for {lp_name} (placeholder)")

        return items

    def _categorize_news(self, text: str) -> str:
        """Categorize news based on keywords."""
        text_lower = text.lower()

        for category, keywords in NEWS_CATEGORIES.items():
            if any(kw in text_lower for kw in keywords):
                return category

        return "general"

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        formats = [
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%B %d %Y",
            "%m/%d/%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        return None
