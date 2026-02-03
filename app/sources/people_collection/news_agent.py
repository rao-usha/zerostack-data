"""
News and press release collection agent.

Monitors company news sources for leadership announcements:
- Company newsroom/press release pages
- Google News RSS feeds
- PR distribution services (PR Newswire, Business Wire)
- RSS/Atom feeds for company news

Enhanced with:
- 40+ newsroom URL patterns
- RSS/Atom feed auto-discovery
- Google News RSS integration
- Investor relations page parsing
- Comprehensive diagnostic logging
"""

import asyncio
import base64
import logging
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from urllib.parse import urljoin, urlparse, quote_plus, unquote

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

    # Expanded newsroom URL patterns (40+ patterns)
    NEWSROOM_PATTERNS = [
        # Primary newsroom paths
        "/news",
        "/newsroom",
        "/press",
        "/press-releases",
        "/press-room",
        "/pressroom",
        "/media",
        "/media-center",
        "/media-room",
        "/media/press-releases",
        "/media/news",

        # About section patterns
        "/about/news",
        "/about/newsroom",
        "/about/press",
        "/about/media",
        "/about-us/news",
        "/about-us/press",

        # Company section patterns
        "/company/news",
        "/company/press",
        "/company/newsroom",
        "/company/press-releases",
        "/corporate/news",
        "/corporate/press",

        # Investor relations patterns (often have leadership announcements)
        "/investors/news",
        "/investors/press-releases",
        "/investor-relations/news",
        "/investor-relations/press",
        "/ir/news",
        "/ir/press-releases",

        # Blog/stories patterns (some companies use these)
        "/blog",
        "/stories",
        "/insights",
        "/updates",

        # Resources section
        "/resources/news",
        "/resources/press",

        # International variants
        "/en/news",
        "/en/newsroom",
        "/en/press",
        "/en-us/news",
        "/en-us/newsroom",

        # Alternate naming
        "/announcements",
        "/latest-news",
        "/latest",
        "/whats-new",
        "/news-releases",
        "/news-and-events",
        "/news-events",
        "/press-and-news",
    ]

    # RSS feed discovery patterns
    RSS_FEED_PATTERNS = [
        "/feed",
        "/rss",
        "/rss.xml",
        "/feed.xml",
        "/atom.xml",
        "/news/feed",
        "/news/rss",
        "/newsroom/feed",
        "/press/feed",
        "/press-releases/feed",
        "/blog/feed",
        "/blog/rss",
    ]

    # Keywords to identify leadership press releases
    LEADERSHIP_KEYWORDS = [
        # Appointment keywords
        "appoints", "appointed", "names", "named",
        "hires", "hired", "promotes", "promoted",
        "elevates", "elevated", "joins", "joined",

        # Announcement keywords
        "announces", "announcement", "leadership",
        "executive", "officer", "board", "director",

        # Title keywords
        "ceo", "cfo", "coo", "cto", "cmo", "cio", "ciso",
        "president", "chairman", "chairwoman", "chairperson",
        "chief", "vice president", "vp", "svp", "evp",
        "general counsel", "treasurer", "controller",

        # Departure keywords
        "resignation", "resigns", "resigned",
        "retires", "retirement", "retiring",
        "departs", "departure", "departing",
        "steps down", "stepping down",

        # Transition keywords
        "succession", "transition", "interim",
        "effective immediately", "effective date",
    ]

    # PR distribution service domains to recognize
    PR_DISTRIBUTION_DOMAINS = [
        "prnewswire.com",
        "businesswire.com",
        "globenewswire.com",
        "accesswire.com",
        "marketwatch.com",
        "reuters.com",
        "bloomberg.com",
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
        include_google_news: bool = True,
    ) -> CollectionResult:
        """
        Collect leadership changes from news sources.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            website_url: Company website URL (for newsroom)
            days_back: How far back to look for news
            include_google_news: Whether to search Google News RSS

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

        # Track pages checked for diagnostics
        result.pages_checked = 0
        result.page_urls = []

        all_changes: List[LeadershipChange] = []

        logger.info(f"[NewsAgent] Starting news collection for {company_name}")
        logger.info(f"[NewsAgent] Website URL: {website_url or 'None provided'}")
        logger.info(f"[NewsAgent] Days back: {days_back}, Google News: {include_google_news}")

        try:
            # 1. Check company newsroom
            if website_url:
                logger.info(f"[NewsAgent] Step 1: Searching for company newsroom")
                newsroom_changes, newsroom_urls = await self._collect_from_newsroom(
                    website_url, company_name, days_back, result
                )
                all_changes.extend(newsroom_changes)
                result.page_urls.extend(newsroom_urls)
                logger.info(f"[NewsAgent] Newsroom: Found {len(newsroom_changes)} changes from {len(newsroom_urls)} pages")
            else:
                logger.info(f"[NewsAgent] Step 1: Skipping newsroom (no website URL)")

            # 2. Try RSS feed discovery
            if website_url:
                logger.info(f"[NewsAgent] Step 2: Discovering RSS/Atom feeds")
                rss_changes, rss_urls = await self._collect_from_rss_feeds(
                    website_url, company_name, days_back, result
                )
                all_changes.extend(rss_changes)
                result.page_urls.extend(rss_urls)
                logger.info(f"[NewsAgent] RSS feeds: Found {len(rss_changes)} changes from {len(rss_urls)} feeds")

            # 3. Search Google News RSS
            if include_google_news:
                logger.info(f"[NewsAgent] Step 3: Searching Google News RSS")
                google_changes = await self._search_google_news(company_name, days_back, result)
                all_changes.extend(google_changes)
                logger.info(f"[NewsAgent] Google News: Found {len(google_changes)} changes")

            # 4. Search PR distribution services directly (bypasses company website blocking)
            logger.info(f"[NewsAgent] Step 4: Searching PR distribution services")
            pr_changes = await self._search_pr_services(company_name, days_back, result)
            all_changes.extend(pr_changes)
            logger.info(f"[NewsAgent] PR Services: Found {len(pr_changes)} changes")

            # 5. Search financial news sites (Yahoo Finance, MarketWatch)
            logger.info(f"[NewsAgent] Step 5: Searching financial news sites")
            finance_changes = await self._search_finance_news(company_name, days_back, result)
            all_changes.extend(finance_changes)
            logger.info(f"[NewsAgent] Finance News: Found {len(finance_changes)} changes")

            # Deduplicate changes
            unique_changes = self._deduplicate_changes(all_changes)

            result.extracted_changes = unique_changes
            result.changes_detected = len(unique_changes)
            result.success = True

            logger.info(
                f"[NewsAgent] News collection complete for {company_name}: "
                f"{len(unique_changes)} unique changes, "
                f"{result.pages_checked} pages checked"
            )

        except Exception as e:
            logger.exception(f"[NewsAgent] Error collecting news for {company_name}: {e}")
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _collect_from_newsroom(
        self,
        website_url: str,
        company_name: str,
        days_back: int,
        result: CollectionResult,
    ) -> Tuple[List[LeadershipChange], List[str]]:
        """Collect from company newsroom page."""
        changes = []
        urls_checked = []

        # Normalize URL
        if not website_url.startswith('http'):
            website_url = 'https://' + website_url
        base_url = website_url.rstrip('/')

        logger.debug(f"[NewsAgent] Looking for newsroom on {base_url}")

        # Try to find newsroom page
        newsroom_url = await self._find_newsroom_url(base_url, result)

        if not newsroom_url:
            logger.info(f"[NewsAgent] No newsroom found for {base_url}")
            result.warnings.append(f"No newsroom page found for {base_url}")
            return changes, urls_checked

        logger.info(f"[NewsAgent] Found newsroom at {newsroom_url}")
        urls_checked.append(newsroom_url)
        result.pages_checked += 1

        # Fetch newsroom page
        html = await self.fetch_url(newsroom_url)
        if not html:
            logger.warning(f"[NewsAgent] Failed to fetch newsroom content from {newsroom_url}")
            result.warnings.append(f"Failed to fetch newsroom: {newsroom_url}")
            return changes, urls_checked

        logger.debug(f"[NewsAgent] Newsroom content length: {len(html)} chars")

        # Extract press release links
        press_releases = self._extract_press_release_links(
            html, newsroom_url, days_back
        )

        logger.info(f"[NewsAgent] Found {len(press_releases)} potential press releases")

        # Filter to leadership-related releases
        leadership_releases = [
            pr for pr in press_releases
            if self._is_leadership_related(pr['title'])
        ]

        logger.info(f"[NewsAgent] {len(leadership_releases)} are leadership-related")

        if leadership_releases:
            logger.debug(f"[NewsAgent] Leadership releases: {[pr['title'][:50] for pr in leadership_releases[:5]]}")

        # Parse each leadership press release
        for pr_info in leadership_releases[:10]:  # Limit to prevent overload
            try:
                logger.debug(f"[NewsAgent] Parsing press release: {pr_info['title'][:60]}")
                result.pages_checked += 1
                urls_checked.append(pr_info['url'])

                pr_html = await self.fetch_url(pr_info['url'])
                if not pr_html:
                    logger.debug(f"[NewsAgent] Failed to fetch press release: {pr_info['url']}")
                    continue

                pr = PressRelease(
                    title=pr_info['title'],
                    content=pr_html,
                    publish_date=pr_info.get('date'),
                    source_url=pr_info['url'],
                    company_name=company_name,
                    source_type="newsroom",
                )

                parse_result = await self.parser.parse(pr)
                if parse_result.changes:
                    logger.info(f"[NewsAgent] Extracted {len(parse_result.changes)} changes from: {pr_info['title'][:50]}")
                changes.extend(parse_result.changes)

            except Exception as e:
                logger.debug(f"[NewsAgent] Error parsing press release: {e}")

        return changes, urls_checked

    async def _find_newsroom_url(
        self,
        base_url: str,
        result: CollectionResult,
    ) -> Optional[str]:
        """Find the newsroom URL for a company website."""
        logger.debug(f"[NewsAgent] Trying {len(self.NEWSROOM_PATTERNS)} newsroom URL patterns")

        # Strategy 1: Try common patterns (check first 15 quickly)
        checked_count = 0
        for pattern in self.NEWSROOM_PATTERNS[:15]:
            url = base_url + pattern
            exists = await self.check_url_exists(url)
            checked_count += 1
            if exists:
                logger.debug(f"[NewsAgent] Found newsroom via pattern: {pattern}")
                return url

        logger.debug(f"[NewsAgent] No match in first 15 patterns, trying homepage link discovery")

        # Strategy 2: Try to find from homepage links
        homepage = await self.fetch_url(base_url)
        if homepage:
            result.pages_checked += 1
            soup = BeautifulSoup(homepage, 'html.parser')

            # Keywords to look for in links
            newsroom_keywords = [
                'news', 'newsroom', 'press', 'media', 'announcements',
                'press-releases', 'press releases', 'latest news',
            ]

            # Look for news/press links
            for a in soup.find_all('a', href=True):
                href = a['href'].lower()
                text = a.get_text().lower().strip()

                if any(kw in href or kw in text for kw in newsroom_keywords):
                    full_url = urljoin(base_url, a['href'])
                    # Verify it's on the same domain
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        logger.debug(f"[NewsAgent] Found newsroom via homepage link: {a.get_text()[:30]}")
                        return full_url

        # Strategy 3: Try remaining patterns (slower, more exhaustive)
        logger.debug(f"[NewsAgent] Trying remaining {len(self.NEWSROOM_PATTERNS) - 15} patterns")
        for pattern in self.NEWSROOM_PATTERNS[15:]:
            url = base_url + pattern
            exists = await self.check_url_exists(url)
            if exists:
                logger.debug(f"[NewsAgent] Found newsroom via extended pattern: {pattern}")
                return url

        # Strategy 4: Try investor relations pages (often have leadership news)
        ir_patterns = ["/investors", "/investor-relations", "/ir"]
        for pattern in ir_patterns:
            url = base_url + pattern
            exists = await self.check_url_exists(url)
            if exists:
                # Check if IR page has news section
                ir_html = await self.fetch_url(url)
                if ir_html and any(kw in ir_html.lower() for kw in ['leadership', 'executive', 'management changes']):
                    logger.debug(f"[NewsAgent] Found leadership content on investor relations page: {pattern}")
                    return url

        logger.debug(f"[NewsAgent] Exhausted all {checked_count + len(self.NEWSROOM_PATTERNS) - 15} patterns")
        return None

    async def _collect_from_rss_feeds(
        self,
        website_url: str,
        company_name: str,
        days_back: int,
        result: CollectionResult,
    ) -> Tuple[List[LeadershipChange], List[str]]:
        """Discover and parse RSS/Atom feeds for leadership news."""
        changes = []
        urls_checked = []

        # Normalize URL
        if not website_url.startswith('http'):
            website_url = 'https://' + website_url
        base_url = website_url.rstrip('/')

        # Strategy 1: Try common RSS feed patterns
        feed_url = None
        for pattern in self.RSS_FEED_PATTERNS:
            url = base_url + pattern
            exists = await self.check_url_exists(url)
            if exists:
                feed_url = url
                logger.debug(f"[NewsAgent] Found RSS feed via pattern: {pattern}")
                break

        # Strategy 2: Check HTML for feed autodiscovery links
        if not feed_url:
            homepage = await self.fetch_url(base_url)
            if homepage:
                soup = BeautifulSoup(homepage, 'html.parser')

                # Look for RSS/Atom link tags
                for link in soup.find_all('link', type=lambda t: t and ('rss' in t or 'atom' in t)):
                    href = link.get('href')
                    if href:
                        feed_url = urljoin(base_url, href)
                        logger.debug(f"[NewsAgent] Found RSS feed via autodiscovery: {feed_url}")
                        break

        if not feed_url:
            logger.debug(f"[NewsAgent] No RSS feed found for {base_url}")
            return changes, urls_checked

        # Parse the RSS feed
        try:
            result.pages_checked += 1
            urls_checked.append(feed_url)

            feed_content = await self.fetch_url(feed_url)
            if not feed_content:
                logger.warning(f"[NewsAgent] Failed to fetch RSS feed: {feed_url}")
                return changes, urls_checked

            # Parse as XML
            soup = BeautifulSoup(feed_content, 'xml')

            # Find items (RSS) or entries (Atom)
            items = soup.find_all('item') or soup.find_all('entry')
            logger.info(f"[NewsAgent] RSS feed has {len(items)} items")

            cutoff_date = date.today() - timedelta(days=days_back)

            for item in items[:20]:  # Check first 20 items
                try:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubDate') or item.find('published') or item.find('updated')

                    if not title_elem:
                        continue

                    title = title_elem.get_text(strip=True)

                    # Get link (RSS vs Atom format)
                    if link_elem:
                        link = link_elem.get('href') or link_elem.get_text(strip=True)
                    else:
                        continue

                    # Check if leadership-related
                    if not self._is_leadership_related(title):
                        continue

                    logger.debug(f"[NewsAgent] RSS leadership item: {title[:50]}")

                    # Parse date if available
                    pub_date = None
                    if pub_date_elem:
                        try:
                            from dateutil import parser as date_parser
                            pub_date = date_parser.parse(pub_date_elem.get_text()).date()
                            if pub_date < cutoff_date:
                                continue
                        except Exception:
                            pass

                    # Fetch and parse the full article
                    result.pages_checked += 1
                    urls_checked.append(link)

                    article_html = await self.fetch_url(link)
                    if not article_html:
                        continue

                    pr = PressRelease(
                        title=title,
                        content=article_html,
                        publish_date=pub_date,
                        source_url=link,
                        company_name=company_name,
                        source_type="rss_feed",
                    )

                    parse_result = await self.parser.parse(pr)
                    if parse_result.changes:
                        logger.info(f"[NewsAgent] RSS: Extracted {len(parse_result.changes)} changes from: {title[:50]}")
                    changes.extend(parse_result.changes)

                except Exception as e:
                    logger.debug(f"[NewsAgent] Error parsing RSS item: {e}")

        except Exception as e:
            logger.warning(f"[NewsAgent] Error parsing RSS feed {feed_url}: {e}")

        return changes, urls_checked

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
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """
        Search Google News RSS for leadership announcements.

        Uses Google News RSS feeds which don't require an API key.
        Parses articles when they're from known PR distribution services.
        """
        changes = []
        cutoff_date = date.today() - timedelta(days=days_back)

        # Build search queries targeting leadership announcements
        queries = [
            f'"{company_name}" CEO appointed OR named OR announces',
            f'"{company_name}" executive leadership change',
            f'"{company_name}" CFO COO president appointed',
            f'"{company_name}" board director names',
        ]

        articles_found = 0
        articles_parsed = 0

        for query in queries[:3]:  # Limit to 3 queries to avoid rate limits
            try:
                # Use Google News RSS (no API required)
                encoded_query = quote_plus(query)
                rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

                logger.debug(f"[NewsAgent] Google News query: {query[:50]}")
                result.pages_checked += 1

                content = await self.fetch_url(rss_url)
                if not content:
                    logger.debug(f"[NewsAgent] Failed to fetch Google News RSS")
                    continue

                # Parse RSS
                soup = BeautifulSoup(content, 'xml')
                items = soup.find_all('item')

                logger.debug(f"[NewsAgent] Google News returned {len(items)} items for query")

                for item in items[:7]:  # Check first 7 results per query
                    try:
                        title_elem = item.find('title')
                        link_elem = item.find('link')
                        pub_date_elem = item.find('pubDate')
                        source_elem = item.find('source')

                        if not title_elem or not link_elem:
                            continue

                        title_text = title_elem.get_text(strip=True)
                        raw_link = link_elem.get_text(strip=True)

                        # Get source info (domain of original article)
                        source_name = source_elem.get_text(strip=True) if source_elem else None
                        source_url = source_elem.get('url') if source_elem else None
                        logger.debug(f"[NewsAgent] Source: {source_name} - {source_url}")

                        # Check relevance first (before URL processing)
                        if not self._is_leadership_related(title_text):
                            continue

                        articles_found += 1

                        # Try to get the actual article URL
                        # Strategy 1: If source is a known PR service, search there directly
                        link_url = None
                        if source_url:
                            source_domain = source_url.rstrip('/')
                            # For Business Wire, PR Newswire, etc. - go directly to their site
                            if 'businesswire.com' in source_domain:
                                # Business Wire: search for exact title
                                search_title = title_text.split(' - ')[0].strip()[:60]
                                link_url = f"https://www.businesswire.com/news/home/?searchTerm={quote_plus(search_title)}"
                            elif 'prnewswire.com' in source_domain:
                                search_title = title_text.split(' - ')[0].strip()[:60]
                                link_url = f"https://www.prnewswire.com/search/news/?keyword={quote_plus(search_title)}"
                            elif 'globenewswire.com' in source_domain:
                                search_title = title_text.split(' - ')[0].strip()[:60]
                                link_url = f"https://www.globenewswire.com/en/search/tag/{quote_plus(search_title)}"

                        # Strategy 2: Try decoding the Google News URL
                        if not link_url or 'news.google.com' in (link_url or ''):
                            decoded_url = self._decode_google_news_url(raw_link)
                            if decoded_url and 'news.google.com' not in decoded_url:
                                link_url = decoded_url

                        # Strategy 3: Follow redirect (last resort)
                        if not link_url or 'news.google.com' in link_url:
                            real_url = await self._follow_google_redirect(raw_link)
                            if real_url and 'news.google.com' not in real_url:
                                link_url = real_url

                        # If still no URL, use source_url as base (won't have article but might help)
                        if not link_url or 'news.google.com' in link_url:
                            link_url = source_url if source_url else raw_link

                        logger.info(f"[NewsAgent] Google News article: {title_text[:50]}")
                        logger.info(f"[NewsAgent] Source: {source_name or 'unknown'}")
                        logger.info(f"[NewsAgent] Article URL: {link_url[:80] if link_url else 'none'}")

                        # Parse date if available
                        pub_date = None
                        if pub_date_elem:
                            try:
                                from dateutil import parser as date_parser
                                pub_date = date_parser.parse(pub_date_elem.get_text()).date()
                                if pub_date < cutoff_date:
                                    logger.debug(f"[NewsAgent] Skipping old article: {pub_date}")
                                    continue
                            except Exception:
                                pass

                        # Check if it's from a trusted PR distribution service
                        check_url = (link_url or '') + (source_url or '')
                        is_pr_service = any(
                            domain in check_url.lower()
                            for domain in self.PR_DISTRIBUTION_DOMAINS
                        )

                        # Fetch and parse ANY leadership article (not just PR services)
                        # Limit to first 5 non-PR articles to avoid overload
                        should_parse = is_pr_service or articles_parsed < 5

                        if should_parse and link_url:
                            source_type = "pr_service" if is_pr_service else "news_article"
                            logger.info(f"[NewsAgent] Fetching {source_type}: {link_url[:60]}")
                            result.pages_checked += 1
                            result.page_urls.append(link_url)

                            article_html = await self.fetch_url(link_url)
                            if article_html:
                                articles_parsed += 1

                                pr = PressRelease(
                                    title=title_text,
                                    content=article_html,
                                    publish_date=pub_date,
                                    source_url=link_url,
                                    company_name=company_name,
                                    source_type="google_news",
                                )

                                parse_result = await self.parser.parse(pr)
                                if parse_result.changes:
                                    logger.info(
                                        f"[NewsAgent] Google News: Extracted {len(parse_result.changes)} "
                                        f"changes from: {title_text[:50]}"
                                    )
                                changes.extend(parse_result.changes)
                        else:
                            logger.debug(f"[NewsAgent] Skipping (limit reached): {link_url[:50]}")

                    except Exception as e:
                        logger.debug(f"[NewsAgent] Error processing Google News item: {e}")

            except Exception as e:
                logger.warning(f"[NewsAgent] Google News search error: {e}")

        logger.info(
            f"[NewsAgent] Google News: Found {articles_found} leadership articles, "
            f"parsed {articles_parsed} from PR services"
        )

        return changes

    async def _search_pr_services(
        self,
        company_name: str,
        days_back: int,
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """
        Search PR distribution services directly for leadership announcements.

        These services host press releases and are more reliably accessible
        than company websites that may block scrapers.
        """
        changes = []

        # Normalize company name for search (remove Inc, Corp, etc)
        search_name = self._normalize_company_for_search(company_name)

        # PR Newswire search
        try:
            logger.debug(f"[NewsAgent] Searching PR Newswire for {search_name}")
            prnewswire_url = f"https://www.prnewswire.com/search/news/?keyword={quote_plus(search_name)}+executive+OR+CEO+OR+leadership&page=1&pagesize=10"

            result.pages_checked += 1
            html = await self.fetch_url(prnewswire_url)

            if html:
                pr_links = self._extract_pr_newswire_links(html, days_back)
                logger.info(f"[NewsAgent] PR Newswire: Found {len(pr_links)} potential releases")

                for pr_info in pr_links[:5]:  # Limit to 5
                    if self._is_leadership_related(pr_info['title']):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info['url'])

                        pr_html = await self.fetch_url(pr_info['url'])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info['title'],
                                content=pr_html,
                                publish_date=pr_info.get('date'),
                                source_url=pr_info['url'],
                                company_name=company_name,
                                source_type="prnewswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(f"[NewsAgent] PR Newswire: {len(parse_result.changes)} changes from {pr_info['title'][:40]}")
                            changes.extend(parse_result.changes)
        except Exception as e:
            logger.debug(f"[NewsAgent] PR Newswire search error: {e}")

        # Business Wire search
        try:
            logger.debug(f"[NewsAgent] Searching Business Wire for {search_name}")
            bizwire_url = f"https://www.businesswire.com/portal/site/home/news/?searchtype=news&searchterm={quote_plus(search_name)}+CEO+OR+executive"

            result.pages_checked += 1
            html = await self.fetch_url(bizwire_url)

            if html:
                bw_links = self._extract_business_wire_links(html, days_back)
                logger.info(f"[NewsAgent] Business Wire: Found {len(bw_links)} potential releases")

                for pr_info in bw_links[:5]:
                    if self._is_leadership_related(pr_info['title']):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info['url'])

                        pr_html = await self.fetch_url(pr_info['url'])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info['title'],
                                content=pr_html,
                                publish_date=pr_info.get('date'),
                                source_url=pr_info['url'],
                                company_name=company_name,
                                source_type="businesswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(f"[NewsAgent] Business Wire: {len(parse_result.changes)} changes from {pr_info['title'][:40]}")
                            changes.extend(parse_result.changes)
        except Exception as e:
            logger.debug(f"[NewsAgent] Business Wire search error: {e}")

        # GlobeNewswire search
        try:
            logger.debug(f"[NewsAgent] Searching GlobeNewswire for {search_name}")
            globe_url = f"https://www.globenewswire.com/search/tag/executive%20changes?query={quote_plus(search_name)}"

            result.pages_checked += 1
            html = await self.fetch_url(globe_url)

            if html:
                globe_links = self._extract_globenewswire_links(html, days_back)
                logger.info(f"[NewsAgent] GlobeNewswire: Found {len(globe_links)} potential releases")

                for pr_info in globe_links[:5]:
                    if self._is_leadership_related(pr_info['title']):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info['url'])

                        pr_html = await self.fetch_url(pr_info['url'])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info['title'],
                                content=pr_html,
                                publish_date=pr_info.get('date'),
                                source_url=pr_info['url'],
                                company_name=company_name,
                                source_type="globenewswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(f"[NewsAgent] GlobeNewswire: {len(parse_result.changes)} changes")
                            changes.extend(parse_result.changes)
        except Exception as e:
            logger.debug(f"[NewsAgent] GlobeNewswire search error: {e}")

        return changes

    async def _search_finance_news(
        self,
        company_name: str,
        days_back: int,
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """
        Search financial news sites for leadership announcements.

        Yahoo Finance, MarketWatch, and Reuters often carry executive
        appointment news and are more accessible than company sites.
        """
        changes = []
        search_name = self._normalize_company_for_search(company_name)

        # Yahoo Finance News RSS
        try:
            logger.debug(f"[NewsAgent] Searching Yahoo Finance for {search_name}")
            # Yahoo Finance RSS feed for company news
            yahoo_rss = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(search_name)}&region=US&lang=en-US"

            result.pages_checked += 1
            content = await self.fetch_url(yahoo_rss)

            if content:
                soup = BeautifulSoup(content, 'xml')
                items = soup.find_all('item')

                leadership_items = 0
                for item in items[:10]:
                    title_elem = item.find('title')
                    link_elem = item.find('link')

                    if not title_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    if self._is_leadership_related(title):
                        leadership_items += 1
                        if link_elem:
                            link = link_elem.get_text(strip=True)
                            result.page_urls.append(link)
                            logger.debug(f"[NewsAgent] Yahoo Finance match: {title[:50]}")

                logger.info(f"[NewsAgent] Yahoo Finance: {leadership_items} leadership items found")
        except Exception as e:
            logger.debug(f"[NewsAgent] Yahoo Finance search error: {e}")

        # MarketWatch search
        try:
            logger.debug(f"[NewsAgent] Searching MarketWatch for {search_name}")
            mw_url = f"https://www.marketwatch.com/search?q={quote_plus(search_name + ' CEO OR executive appointment')}&ts=0&ns=y"

            result.pages_checked += 1
            html = await self.fetch_url(mw_url)

            if html:
                mw_links = self._extract_marketwatch_links(html, days_back)
                logger.info(f"[NewsAgent] MarketWatch: Found {len(mw_links)} results")

                for article in mw_links[:3]:
                    if self._is_leadership_related(article['title']):
                        logger.debug(f"[NewsAgent] MarketWatch match: {article['title'][:50]}")
                        result.page_urls.append(article['url'])
        except Exception as e:
            logger.debug(f"[NewsAgent] MarketWatch search error: {e}")

        return changes

    def _normalize_company_for_search(self, company_name: str) -> str:
        """Normalize company name for search queries."""
        # Remove common suffixes
        suffixes = [
            ', Inc.', ', Inc', ' Inc.', ' Inc',
            ', Corp.', ', Corp', ' Corp.', ' Corp',
            ', LLC', ' LLC',
            ', Ltd.', ', Ltd', ' Ltd.', ' Ltd',
            ', Co.', ', Co', ' Co.', ' Co',
            ' Company', ' Corporation', ' International',
        ]

        name = company_name
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break

        return name.strip()

    def _extract_pr_newswire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from PR Newswire search results."""
        soup = BeautifulSoup(html, 'html.parser')
        releases = []

        # PR Newswire uses specific card structure
        for card in soup.find_all(['div', 'article'], class_=lambda x: x and 'card' in str(x).lower()):
            link = card.find('a', href=True)
            if link:
                href = link.get('href', '')
                title = link.get_text(strip=True)

                if href and title and '/news-releases/' in href:
                    full_url = urljoin('https://www.prnewswire.com', href)
                    releases.append({
                        'url': full_url,
                        'title': title,
                        'date': None,
                    })

        return releases[:10]

    def _extract_business_wire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from Business Wire search results."""
        soup = BeautifulSoup(html, 'html.parser')
        releases = []

        for item in soup.find_all(['div', 'li'], class_=lambda x: x and any(kw in str(x).lower() for kw in ['result', 'news', 'headline'])):
            link = item.find('a', href=True)
            if link:
                href = link.get('href', '')
                title = link.get_text(strip=True)

                if href and title:
                    full_url = urljoin('https://www.businesswire.com', href)
                    releases.append({
                        'url': full_url,
                        'title': title,
                        'date': None,
                    })

        return releases[:10]

    def _extract_globenewswire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from GlobeNewswire search results."""
        soup = BeautifulSoup(html, 'html.parser')
        releases = []

        for item in soup.find_all(['div', 'article'], class_=lambda x: x and 'article' in str(x).lower()):
            link = item.find('a', href=True)
            if link:
                href = link.get('href', '')
                title = link.get_text(strip=True)

                if href and title and '/news-release/' in href:
                    full_url = urljoin('https://www.globenewswire.com', href)
                    releases.append({
                        'url': full_url,
                        'title': title,
                        'date': None,
                    })

        return releases[:10]

    def _extract_marketwatch_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract article links from MarketWatch search results."""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        for item in soup.find_all(['div', 'article'], class_=lambda x: x and 'article' in str(x).lower()):
            link = item.find('a', href=True)
            if link:
                href = link.get('href', '')
                title = link.get_text(strip=True)

                if href and title:
                    articles.append({
                        'url': href,
                        'title': title,
                        'date': None,
                    })

        return articles[:10]

    def _decode_google_news_url(self, google_url: str) -> Optional[str]:
        """
        Decode a Google News RSS URL to get the actual article URL.

        Google News RSS wraps article URLs in their redirect system using protobuf.
        The format is: https://news.google.com/rss/articles/[protobuf-encoded-data]

        This method tries multiple decoding strategies.
        """
        if not google_url:
            return None

        # If it's not a Google News URL, return as-is
        if 'news.google.com' not in google_url:
            return google_url

        try:
            if '/articles/' in google_url:
                encoded_part = google_url.split('/articles/')[-1].split('?')[0]

                # Try multiple decoding strategies
                for attempt in range(4):
                    try:
                        # Strategy 1: Decode full encoded part
                        if attempt == 0:
                            to_decode = encoded_part
                        # Strategy 2: Skip CBMi prefix (4 chars)
                        elif attempt == 1 and encoded_part.startswith('CBMi'):
                            to_decode = encoded_part[4:]
                        # Strategy 3: Skip just CB prefix (2 chars)
                        elif attempt == 2 and encoded_part.startswith('CB'):
                            to_decode = encoded_part[2:]
                        # Strategy 4: Skip first 5 chars (protobuf header varies)
                        elif attempt == 3:
                            to_decode = encoded_part[5:]
                        else:
                            continue

                        # Try different padding
                        for extra_padding in range(4):
                            try:
                                padded = to_decode + '=' * extra_padding
                                decoded = base64.urlsafe_b64decode(padded)

                                # Try UTF-8 decode
                                try:
                                    decoded_str = decoded.decode('utf-8', errors='ignore')
                                except:
                                    decoded_str = decoded.decode('latin-1', errors='ignore')

                                # Look for URL pattern in decoded data
                                url_match = re.search(r'https?://[^\s<>"\'\x00-\x1f]+', decoded_str)
                                if url_match:
                                    url = url_match.group(0).rstrip('.')
                                    # Verify it's not a Google URL
                                    if 'google.com' not in url and 'goo.gl' not in url:
                                        logger.info(f"[NewsAgent] Decoded URL (strategy {attempt+1}): {url[:60]}")
                                        return url
                                break  # Padding worked, move to next strategy
                            except Exception:
                                continue

                    except Exception as e:
                        logger.debug(f"[NewsAgent] Decode attempt {attempt+1} failed: {e}")
                        continue

            # If decoding fails, return original URL - will use _follow_google_redirect
            logger.debug(f"[NewsAgent] Could not decode, will follow redirect: {google_url[:60]}")
            return google_url

        except Exception as e:
            logger.debug(f"[NewsAgent] Error decoding Google News URL: {e}")
            return google_url

    async def _follow_google_redirect(self, google_url: str) -> Optional[str]:
        """
        Follow a Google News redirect to get the actual article URL.

        Google News uses JavaScript redirects, so we need to fetch the page
        and extract the real URL from the HTML content.
        """
        logger.info(f"[NewsAgent] Following redirect for: {google_url[:60]}...")
        try:
            import aiohttp
            from aiohttp import ClientTimeout

            timeout = ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # First try HEAD to see if there's an HTTP redirect
                try:
                    async with session.head(
                        google_url,
                        headers=self.DEFAULT_HEADERS,
                        allow_redirects=True,
                        max_redirects=5,
                    ) as response:
                        final_url = str(response.url)
                        logger.info(f"[NewsAgent] HEAD redirect result: {final_url[:60]}")
                        if 'news.google.com' not in final_url:
                            return final_url
                except Exception as e:
                    logger.info(f"[NewsAgent] HEAD request failed: {e}")

                # HEAD didn't work, fetch the page and look for JS/meta redirect
                logger.info(f"[NewsAgent] Trying GET request...")
                async with session.get(
                    google_url,
                    headers=self.DEFAULT_HEADERS,
                    allow_redirects=True,
                ) as response:
                    html = await response.text()
                    logger.info(f"[NewsAgent] Got page content, length: {len(html)}")

                    # Method 1: Look for data-n-au attribute (article URL)
                    match = re.search(r'data-n-au="([^"]+)"', html)
                    if match:
                        url = match.group(1)
                        if url.startswith('http'):
                            logger.info(f"[NewsAgent] Extracted URL from data-n-au: {url[:60]}")
                            return url

                    # Method 2: Look for "url" in JSON data
                    match = re.search(r'"url"\s*:\s*"(https?://[^"]+)"', html)
                    if match:
                        url = match.group(1).replace('\\u002F', '/').replace('\\/', '/')
                        if 'news.google.com' not in url:
                            logger.info(f"[NewsAgent] Extracted URL from JSON: {url[:60]}")
                            return url

                    # Method 3: Look for canonical URL
                    match = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', html)
                    if match:
                        url = match.group(1)
                        if 'news.google.com' not in url:
                            logger.info(f"[NewsAgent] Extracted canonical URL: {url[:60]}")
                            return url

                    # Method 4: Look for og:url meta tag
                    match = re.search(r'<meta[^>]+property="og:url"[^>]+content="([^"]+)"', html)
                    if match:
                        url = match.group(1)
                        if 'news.google.com' not in url:
                            logger.info(f"[NewsAgent] Extracted og:url: {url[:60]}")
                            return url

                    # Method 5: Look for window.location redirect
                    match = re.search(r'window\.location\s*=\s*["\']([^"\']+)["\']', html)
                    if match:
                        url = match.group(1)
                        if url.startswith('http') and 'news.google.com' not in url:
                            logger.info(f"[NewsAgent] Extracted JS redirect: {url[:60]}")
                            return url

                    # Method 6: Look for meta refresh
                    match = re.search(r'<meta[^>]+http-equiv="refresh"[^>]+content="[^"]*url=([^"]+)"', html, re.I)
                    if match:
                        url = match.group(1)
                        if url.startswith('http') and 'news.google.com' not in url:
                            logger.info(f"[NewsAgent] Extracted meta refresh: {url[:60]}")
                            return url

                    # Method 7: Look for any href that's not Google
                    urls_found = re.findall(r'href="(https?://[^"]+)"', html)
                    for url in urls_found:
                        if 'google' not in url.lower() and 'gstatic' not in url.lower():
                            logger.info(f"[NewsAgent] Found href URL: {url[:60]}")
                            return url

                    # Log sample of content for debugging
                    logger.info(f"[NewsAgent] Page sample: {html[:500]}")
                    logger.info(f"[NewsAgent] Could not extract URL from Google News page")

        except Exception as e:
            logger.debug(f"[NewsAgent] Redirect follow failed: {e}")

        return None

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
