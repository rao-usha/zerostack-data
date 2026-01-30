"""
News and press release collection agent.

Monitors company news sources for leadership announcements:
- Company newsroom/press release pages
- Google News (via search)
- PR distribution services (PR Newswire, Business Wire)
"""

import asyncio
import logging
import re
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
from urllib.parse import urljoin, urlparse, quote_plus

from bs4 import BeautifulSoup

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.press_release_parser import (
    PressReleaseParser,
    PressRelease,
)
from app.sources.people_collection.change_detector import ChangeDetector
from app.sources.people_collection.types import (
    LeadershipChange,
    CollectionResult,
    ExtractionConfidence,
)

logger = logging.getLogger(__name__)


class NewsAgent(BaseCollector):
    """
    Agent for collecting leadership changes from news sources.

    Sources:
    1. Company newsroom pages
    2. Google News search
    3. SEC 8-K (cross-referenced via SEC agent)
    """

    # Common newsroom URL patterns
    NEWSROOM_PATTERNS = [
        "/news",
        "/newsroom",
        "/press",
        "/press-releases",
        "/media",
        "/media/press-releases",
        "/about/news",
        "/about/newsroom",
        "/company/news",
        "/company/press",
        "/investors/news",
        "/investor-relations/news",
    ]

    # Keywords to identify leadership press releases
    LEADERSHIP_KEYWORDS = [
        "appoints", "appointed", "names", "named",
        "announces", "announcement", "leadership",
        "executive", "officer", "board", "director",
        "ceo", "cfo", "president", "chairman",
        "resignation", "retires", "retirement",
        "succession", "transition",
    ]

    def __init__(self):
        super().__init__(source_type="news")
        self.parser = PressReleaseParser()
        self.detector = ChangeDetector()

    async def collect(
        self,
        company_id: int,
        company_name: str,
        website_url: Optional[str] = None,
        days_back: int = 90,
    ) -> CollectionResult:
        """
        Collect leadership changes from news sources.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            website_url: Company website URL (for newsroom)
            days_back: How far back to look for news

        Returns:
            CollectionResult with extracted changes
        """
        started_at = datetime.utcnow()

        result = CollectionResult(
            company_id=company_id,
            company_name=company_name,
            source="news",
            started_at=started_at,
        )

        all_changes: List[LeadershipChange] = []

        try:
            # 1. Check company newsroom
            if website_url:
                newsroom_changes = await self._collect_from_newsroom(
                    website_url, company_name, days_back
                )
                all_changes.extend(newsroom_changes)
                logger.info(f"Found {len(newsroom_changes)} changes from newsroom")

            # 2. Search Google News (optional, may be rate limited)
            # google_changes = await self._search_google_news(company_name, days_back)
            # all_changes.extend(google_changes)

            # Deduplicate changes
            unique_changes = self._deduplicate_changes(all_changes)

            result.extracted_changes = unique_changes
            result.changes_detected = len(unique_changes)
            result.success = True

            logger.info(f"News collection for {company_name}: {len(unique_changes)} changes")

        except Exception as e:
            logger.exception(f"Error collecting news for {company_name}: {e}")
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _collect_from_newsroom(
        self,
        website_url: str,
        company_name: str,
        days_back: int,
    ) -> List[LeadershipChange]:
        """Collect from company newsroom page."""
        changes = []

        # Normalize URL
        if not website_url.startswith('http'):
            website_url = 'https://' + website_url
        base_url = website_url.rstrip('/')

        # Try to find newsroom page
        newsroom_url = await self._find_newsroom_url(base_url)

        if not newsroom_url:
            logger.debug(f"No newsroom found for {base_url}")
            return changes

        logger.debug(f"Found newsroom at {newsroom_url}")

        # Fetch newsroom page
        html = await self.fetch_url(newsroom_url)
        if not html:
            return changes

        # Extract press release links
        press_releases = self._extract_press_release_links(
            html, newsroom_url, days_back
        )

        logger.debug(f"Found {len(press_releases)} potential press releases")

        # Filter to leadership-related releases
        leadership_releases = [
            pr for pr in press_releases
            if self._is_leadership_related(pr['title'])
        ]

        logger.debug(f"{len(leadership_releases)} are leadership-related")

        # Parse each leadership press release
        for pr_info in leadership_releases[:10]:  # Limit to prevent overload
            try:
                pr_html = await self.fetch_url(pr_info['url'])
                if not pr_html:
                    continue

                pr = PressRelease(
                    title=pr_info['title'],
                    content=pr_html,
                    publish_date=pr_info.get('date'),
                    source_url=pr_info['url'],
                    company_name=company_name,
                    source_type="newsroom",
                )

                result = await self.parser.parse(pr)
                changes.extend(result.changes)

            except Exception as e:
                logger.debug(f"Error parsing press release: {e}")

        return changes

    async def _find_newsroom_url(self, base_url: str) -> Optional[str]:
        """Find the newsroom URL for a company website."""
        # Try common patterns
        for pattern in self.NEWSROOM_PATTERNS:
            url = base_url + pattern
            exists = await self.check_url_exists(url)
            if exists:
                return url

        # Try to find from homepage
        homepage = await self.fetch_url(base_url)
        if homepage:
            soup = BeautifulSoup(homepage, 'html.parser')

            # Look for news/press links
            for a in soup.find_all('a', href=True):
                href = a['href'].lower()
                text = a.get_text().lower()

                if any(kw in href or kw in text for kw in ['news', 'press', 'media']):
                    full_url = urljoin(base_url, a['href'])
                    # Verify it's on the same domain
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        return full_url

        return None

    def _extract_press_release_links(
        self,
        html: str,
        base_url: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from newsroom page."""
        soup = BeautifulSoup(html, 'html.parser')
        releases = []

        cutoff_date = date.today() - timedelta(days=days_back)

        # Common press release container patterns
        containers = soup.find_all(['article', 'div', 'li'], class_=lambda x: x and any(
            kw in str(x).lower() for kw in ['press', 'news', 'release', 'article', 'post']
        ))

        if not containers:
            # Fall back to all links
            containers = [soup]

        for container in containers[:50]:  # Limit containers
            for a in container.find_all('a', href=True):
                href = a['href']
                title = a.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # Skip navigation links
                if any(skip in title.lower() for skip in ['read more', 'learn more', 'view all', 'see all']):
                    continue

                full_url = urljoin(base_url, href)

                # Try to extract date
                pr_date = self._extract_date_near_link(container, a)

                # Skip if too old
                if pr_date and pr_date < cutoff_date:
                    continue

                releases.append({
                    'url': full_url,
                    'title': title,
                    'date': pr_date,
                })

        # Dedupe by URL
        seen_urls = set()
        unique = []
        for pr in releases:
            if pr['url'] not in seen_urls:
                seen_urls.add(pr['url'])
                unique.append(pr)

        return unique

    def _extract_date_near_link(
        self,
        container,
        link,
    ) -> Optional[date]:
        """Try to extract a date near a press release link."""
        # Look for date patterns in the container
        text = container.get_text()

        # Common date patterns
        patterns = [
            r'(\w+\s+\d{1,2},?\s+\d{4})',  # January 15, 2024
            r'(\d{1,2}/\d{1,2}/\d{4})',      # 01/15/2024
            r'(\d{4}-\d{2}-\d{2})',           # 2024-01-15
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    from dateutil import parser as date_parser
                    return date_parser.parse(match.group(1)).date()
                except:
                    pass

        return None

    def _is_leadership_related(self, title: str) -> bool:
        """Check if a press release title is leadership-related."""
        title_lower = title.lower()
        return any(kw in title_lower for kw in self.LEADERSHIP_KEYWORDS)

    async def _search_google_news(
        self,
        company_name: str,
        days_back: int,
    ) -> List[LeadershipChange]:
        """
        Search Google News for leadership announcements.

        Note: This is rate-limited and may not work without API key.
        """
        changes = []

        # Build search queries
        queries = [
            f'"{company_name}" CEO appointed',
            f'"{company_name}" executive leadership',
            f'"{company_name}" names president',
        ]

        for query in queries[:2]:  # Limit queries
            try:
                # Use Google News RSS (no API required)
                encoded_query = quote_plus(query)
                rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

                content = await self.fetch_url(rss_url)
                if not content:
                    continue

                # Parse RSS
                soup = BeautifulSoup(content, 'xml')
                items = soup.find_all('item')

                for item in items[:5]:
                    title = item.find('title')
                    link = item.find('link')
                    pub_date = item.find('pubDate')

                    if not title or not link:
                        continue

                    title_text = title.get_text()

                    # Check relevance
                    if not self._is_leadership_related(title_text):
                        continue

                    # Note: We're not fetching full articles to avoid rate limits
                    # Just record that we found a potential change
                    logger.debug(f"Found news: {title_text}")

            except Exception as e:
                logger.debug(f"Google News search error: {e}")

        return changes

    def _deduplicate_changes(
        self,
        changes: List[LeadershipChange],
    ) -> List[LeadershipChange]:
        """Deduplicate leadership changes."""
        seen = set()
        unique = []

        for change in changes:
            # Create key for comparison
            key = (
                change.person_name.lower(),
                change.change_type.value if hasattr(change.change_type, 'value') else str(change.change_type),
            )

            if key not in seen:
                seen.add(key)
                unique.append(change)

        return unique

    def _finalize_result(self, result: CollectionResult) -> CollectionResult:
        """Finalize collection result with timing."""
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
        return result


async def collect_company_news(
    company_id: int,
    company_name: str,
    website_url: Optional[str] = None,
    days_back: int = 90,
) -> CollectionResult:
    """
    Convenience function to collect news for a company.

    Args:
        company_id: Database ID
        company_name: Company name
        website_url: Company website
        days_back: How far back to look

    Returns:
        CollectionResult with extracted changes
    """
    async with NewsAgent() as agent:
        return await agent.collect(company_id, company_name, website_url, days_back)
