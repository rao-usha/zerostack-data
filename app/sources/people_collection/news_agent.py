"""
News and press release collection agent.

Monitors company news sources for leadership announcements:
- Company newsroom/press release pages
- Bing News RSS feeds (direct article URLs)
- PR distribution services (PR Newswire, Business Wire)
- RSS/Atom feeds for company news
- Financial news (Yahoo Finance, MarketWatch)

Enhanced with:
- 40+ newsroom URL patterns
- RSS/Atom feed auto-discovery
- Bing News RSS integration (replaced Google News — protobuf URLs broken)
- Investor relations page parsing
- Comprehensive diagnostic logging
"""

import asyncio
import logging
import re
from typing import Optional, List, Dict, Any, Tuple
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
    2. Bing News RSS search
    3. PR distribution services (PR Newswire, Business Wire, GlobeNewswire)
    4. Financial news (Yahoo Finance, MarketWatch)
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
        "appoints",
        "appointed",
        "names",
        "named",
        "hires",
        "hired",
        "promotes",
        "promoted",
        "elevates",
        "elevated",
        "joins",
        "joined",
        # Announcement keywords
        "announces",
        "announcement",
        "leadership",
        "executive",
        "officer",
        "board",
        "director",
        # Title keywords
        "ceo",
        "cfo",
        "coo",
        "cto",
        "cmo",
        "cio",
        "ciso",
        "president",
        "chairman",
        "chairwoman",
        "chairperson",
        "chief",
        "vice president",
        "vp",
        "svp",
        "evp",
        "general counsel",
        "treasurer",
        "controller",
        # Departure keywords
        "resignation",
        "resigns",
        "resigned",
        "retires",
        "retirement",
        "retiring",
        "departs",
        "departure",
        "departing",
        "steps down",
        "stepping down",
        # Transition keywords
        "succession",
        "transition",
        "interim",
        "effective immediately",
        "effective date",
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
        newsroom_url: Optional[str] = None,
    ) -> CollectionResult:
        """
        Collect leadership changes from news sources.

        Args:
            company_id: Database ID of the company
            company_name: Name of the company
            website_url: Company website URL (for newsroom)
            days_back: How far back to look for news
            include_google_news: Whether to search Google News RSS
            newsroom_url: Direct newsroom URL (skips pattern search if provided)

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
        logger.info(
            f"[NewsAgent] Days back: {days_back}, Google News: {include_google_news}"
        )

        try:
            # 1. Check company newsroom
            if website_url or newsroom_url:
                logger.info(f"[NewsAgent] Step 1: Searching for company newsroom")
                newsroom_changes, newsroom_urls = await self._collect_from_newsroom(
                    website_url or newsroom_url,
                    company_name,
                    days_back,
                    result,
                    newsroom_url=newsroom_url,
                )
                all_changes.extend(newsroom_changes)
                result.page_urls.extend(newsroom_urls)
                logger.info(
                    f"[NewsAgent] Newsroom: Found {len(newsroom_changes)} changes from {len(newsroom_urls)} pages"
                )
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
                logger.info(
                    f"[NewsAgent] RSS feeds: Found {len(rss_changes)} changes from {len(rss_urls)} feeds"
                )

            # 3. Search Bing News RSS
            if include_google_news:
                logger.info(f"[NewsAgent] Step 3: Searching Bing News RSS")
                bing_changes = await self._search_bing_news(
                    company_name, days_back, result
                )
                all_changes.extend(bing_changes)
                logger.info(f"[NewsAgent] Bing News: Found {len(bing_changes)} changes")

            # 4. Search PR distribution services directly (bypasses company website blocking)
            logger.info(f"[NewsAgent] Step 4: Searching PR distribution services")
            pr_changes = await self._search_pr_services(company_name, days_back, result)
            all_changes.extend(pr_changes)
            logger.info(f"[NewsAgent] PR Services: Found {len(pr_changes)} changes")

            # 5. Search financial news sites (Yahoo Finance, MarketWatch)
            logger.info(f"[NewsAgent] Step 5: Searching financial news sites")
            finance_changes = await self._search_finance_news(
                company_name, days_back, result
            )
            all_changes.extend(finance_changes)
            logger.info(
                f"[NewsAgent] Finance News: Found {len(finance_changes)} changes"
            )

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
            logger.exception(
                f"[NewsAgent] Error collecting news for {company_name}: {e}"
            )
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _collect_from_newsroom(
        self,
        website_url: str,
        company_name: str,
        days_back: int,
        result: CollectionResult,
        newsroom_url: Optional[str] = None,
    ) -> Tuple[List[LeadershipChange], List[str]]:
        """Collect from company newsroom page."""
        changes = []
        urls_checked = []

        # Normalize URL
        if not website_url.startswith("http"):
            website_url = "https://" + website_url
        base_url = website_url.rstrip("/")

        # Use provided newsroom_url directly, or discover it
        if newsroom_url:
            logger.info(f"[NewsAgent] Using provided newsroom URL: {newsroom_url}")
        else:
            logger.debug(f"[NewsAgent] Looking for newsroom on {base_url}")
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
            logger.warning(
                f"[NewsAgent] Failed to fetch newsroom content from {newsroom_url}"
            )
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
            pr for pr in press_releases if self._is_leadership_related(pr["title"])
        ]

        logger.info(f"[NewsAgent] {len(leadership_releases)} are leadership-related")

        if leadership_releases:
            logger.debug(
                f"[NewsAgent] Leadership releases: {[pr['title'][:50] for pr in leadership_releases[:5]]}"
            )

        # Parse each leadership press release
        for pr_info in leadership_releases[:10]:  # Limit to prevent overload
            try:
                logger.debug(
                    f"[NewsAgent] Parsing press release: {pr_info['title'][:60]}"
                )
                result.pages_checked += 1
                urls_checked.append(pr_info["url"])

                pr_html = await self.fetch_url(pr_info["url"])
                if not pr_html:
                    logger.debug(
                        f"[NewsAgent] Failed to fetch press release: {pr_info['url']}"
                    )
                    continue

                pr = PressRelease(
                    title=pr_info["title"],
                    content=pr_html,
                    publish_date=pr_info.get("date"),
                    source_url=pr_info["url"],
                    company_name=company_name,
                    source_type="newsroom",
                )

                parse_result = await self.parser.parse(pr)
                if parse_result.changes:
                    logger.info(
                        f"[NewsAgent] Extracted {len(parse_result.changes)} changes from: {pr_info['title'][:50]}"
                    )
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
        logger.debug(
            f"[NewsAgent] Trying {len(self.NEWSROOM_PATTERNS)} newsroom URL patterns"
        )

        # Strategy 0: Try common newsroom subdomains (news.example.com, etc.)
        parsed = urlparse(base_url)
        root_domain = parsed.netloc.lstrip("www.")
        for prefix in ["news", "newsroom", "press", "media"]:
            sub_url = f"{parsed.scheme}://{prefix}.{root_domain}"
            exists = await self.check_url_exists(sub_url, timeout=10)
            if exists:
                logger.debug(f"[NewsAgent] Found newsroom via subdomain: {sub_url}")
                return sub_url

        # Strategy 1: Try common patterns (check first 15 quickly)
        checked_count = 0
        for pattern in self.NEWSROOM_PATTERNS[:15]:
            url = base_url + pattern
            exists = await self.check_url_exists(url, timeout=10)
            checked_count += 1
            if exists:
                logger.debug(f"[NewsAgent] Found newsroom via pattern: {pattern}")
                return url

        logger.debug(
            f"[NewsAgent] No match in first 15 patterns, trying homepage link discovery"
        )

        # Strategy 2: Try to find from homepage links
        homepage = await self.fetch_url(base_url)
        if homepage:
            result.pages_checked += 1
            soup = BeautifulSoup(homepage, "html.parser")

            # Keywords to look for in links
            newsroom_keywords = [
                "news",
                "newsroom",
                "press",
                "media",
                "announcements",
                "press-releases",
                "press releases",
                "latest news",
            ]

            # Look for news/press links
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                text = a.get_text().lower().strip()

                if any(kw in href or kw in text for kw in newsroom_keywords):
                    full_url = urljoin(base_url, a["href"])
                    # Allow same domain or subdomains (e.g. news.prudential.com)
                    link_netloc = urlparse(full_url).netloc.lstrip("www.")
                    if link_netloc == root_domain or link_netloc.endswith(
                        "." + root_domain
                    ):
                        logger.debug(
                            f"[NewsAgent] Found newsroom via homepage link: {a.get_text()[:30]}"
                        )
                        return full_url

        # Strategy 3: Try remaining patterns (slower, more exhaustive)
        logger.debug(
            f"[NewsAgent] Trying remaining {len(self.NEWSROOM_PATTERNS) - 15} patterns"
        )
        for pattern in self.NEWSROOM_PATTERNS[15:]:
            url = base_url + pattern
            exists = await self.check_url_exists(url, timeout=10)
            if exists:
                logger.debug(
                    f"[NewsAgent] Found newsroom via extended pattern: {pattern}"
                )
                return url

        # Strategy 4: Try investor relations pages (often have leadership news)
        ir_patterns = ["/investors", "/investor-relations", "/ir"]
        for pattern in ir_patterns:
            url = base_url + pattern
            exists = await self.check_url_exists(url, timeout=10)
            if exists:
                # Check if IR page has news section
                ir_html = await self.fetch_url(url)
                if ir_html and any(
                    kw in ir_html.lower()
                    for kw in ["leadership", "executive", "management changes"]
                ):
                    logger.debug(
                        f"[NewsAgent] Found leadership content on investor relations page: {pattern}"
                    )
                    return url

        logger.debug(
            f"[NewsAgent] Exhausted all {checked_count + len(self.NEWSROOM_PATTERNS) - 15} patterns"
        )
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
        if not website_url.startswith("http"):
            website_url = "https://" + website_url
        base_url = website_url.rstrip("/")

        # Strategy 1: Try common RSS feed patterns (parallel probes)
        feed_url = None
        candidate_urls = [base_url + pattern for pattern in self.RSS_FEED_PATTERNS]
        results = await asyncio.gather(
            *(self.check_url_exists(url, timeout=10) for url in candidate_urls),
            return_exceptions=True,
        )
        for url, exists in zip(candidate_urls, results):
            if exists is True:
                feed_url = url
                logger.debug(f"[NewsAgent] Found RSS feed via pattern: {url}")
                break

        # Strategy 2: Check HTML for feed autodiscovery links
        if not feed_url:
            homepage = await self.fetch_url(base_url)
            if homepage:
                soup = BeautifulSoup(homepage, "html.parser")

                # Look for RSS/Atom link tags
                for link in soup.find_all(
                    "link", type=lambda t: t and ("rss" in t or "atom" in t)
                ):
                    href = link.get("href")
                    if href:
                        feed_url = urljoin(base_url, href)
                        logger.debug(
                            f"[NewsAgent] Found RSS feed via autodiscovery: {feed_url}"
                        )
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
            soup = BeautifulSoup(feed_content, "xml")

            # Find items (RSS) or entries (Atom)
            items = soup.find_all("item") or soup.find_all("entry")
            logger.info(f"[NewsAgent] RSS feed has {len(items)} items")

            cutoff_date = date.today() - timedelta(days=days_back)

            for item in items[:20]:  # Check first 20 items
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = (
                        item.find("pubDate")
                        or item.find("published")
                        or item.find("updated")
                    )

                    if not title_elem:
                        continue

                    title = title_elem.get_text(strip=True)

                    # Get link (RSS vs Atom format)
                    if link_elem:
                        link = link_elem.get("href") or link_elem.get_text(strip=True)
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

                            pub_date = date_parser.parse(
                                pub_date_elem.get_text()
                            ).date()
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
                        logger.info(
                            f"[NewsAgent] RSS: Extracted {len(parse_result.changes)} changes from: {title[:50]}"
                        )
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
        soup = BeautifulSoup(html, "html.parser")
        releases = []

        cutoff_date = date.today() - timedelta(days=days_back)

        # Common press release container patterns
        containers = soup.find_all(
            ["article", "div", "li"],
            class_=lambda x: x
            and any(
                kw in str(x).lower()
                for kw in ["press", "news", "release", "article", "post"]
            ),
        )

        if not containers:
            # Fall back to all links
            containers = [soup]

        for container in containers[:50]:  # Limit containers
            for a in container.find_all("a", href=True):
                href = a["href"]
                title = a.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # Skip navigation links
                if any(
                    skip in title.lower()
                    for skip in ["read more", "learn more", "view all", "see all"]
                ):
                    continue

                full_url = urljoin(base_url, href)

                # Try to extract date
                pr_date = self._extract_date_near_link(container, a)

                # Skip if too old
                if pr_date and pr_date < cutoff_date:
                    continue

                releases.append(
                    {
                        "url": full_url,
                        "title": title,
                        "date": pr_date,
                    }
                )

        # Dedupe by URL
        seen_urls = set()
        unique = []
        for pr in releases:
            if pr["url"] not in seen_urls:
                seen_urls.add(pr["url"])
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
            r"(\w+\s+\d{1,2},?\s+\d{4})",  # January 15, 2024
            r"(\d{1,2}/\d{1,2}/\d{4})",  # 01/15/2024
            r"(\d{4}-\d{2}-\d{2})",  # 2024-01-15
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    from dateutil import parser as date_parser

                    return date_parser.parse(match.group(1)).date()
                except (ValueError, TypeError):
                    pass

        return None

    def _is_leadership_related(self, title: str) -> bool:
        """Check if a press release title is leadership-related."""
        title_lower = title.lower()
        return any(kw in title_lower for kw in self.LEADERSHIP_KEYWORDS)

    async def _search_bing_news(
        self,
        company_name: str,
        days_back: int,
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """
        Search Bing News RSS for leadership announcements.

        Uses Bing News RSS feeds which return direct article URLs
        (no protobuf encoding or JS redirects like Google News).
        """
        changes = []
        cutoff_date = date.today() - timedelta(days=days_back)

        # Build search queries targeting leadership announcements
        queries = [
            f'"{company_name}" CEO appointed OR named OR announces',
            f'"{company_name}" executive leadership change',
            f'"{company_name}" CFO COO president appointed',
        ]

        articles_found = 0
        articles_parsed = 0

        for query in queries[:3]:  # Limit to 3 queries to avoid rate limits
            try:
                encoded_query = quote_plus(query)
                rss_url = (
                    f"https://www.bing.com/news/search?q={encoded_query}&format=rss"
                )

                logger.debug(f"[NewsAgent] Bing News query: {query[:50]}")
                result.pages_checked += 1

                content = await self.fetch_url(rss_url)
                if not content:
                    logger.debug(f"[NewsAgent] Failed to fetch Bing News RSS")
                    continue

                # Parse RSS
                soup = BeautifulSoup(content, "xml")
                items = soup.find_all("item")

                logger.debug(
                    f"[NewsAgent] Bing News returned {len(items)} items for query"
                )

                for item in items[:7]:  # Check first 7 results per query
                    try:
                        title_elem = item.find("title")
                        link_elem = item.find("link")
                        pub_date_elem = item.find("pubDate")

                        if not title_elem or not link_elem:
                            continue

                        title_text = title_elem.get_text(strip=True)
                        # Bing returns direct article URLs
                        link_url = link_elem.get_text(strip=True)

                        # Check relevance first
                        if not self._is_leadership_related(title_text):
                            continue

                        articles_found += 1

                        logger.info(f"[NewsAgent] Bing News article: {title_text[:50]}")
                        logger.info(
                            f"[NewsAgent] Article URL: {link_url[:80] if link_url else 'none'}"
                        )

                        # Parse date if available
                        pub_date = None
                        if pub_date_elem:
                            try:
                                from dateutil import parser as date_parser

                                pub_date = date_parser.parse(
                                    pub_date_elem.get_text()
                                ).date()
                                if pub_date < cutoff_date:
                                    logger.debug(
                                        f"[NewsAgent] Skipping old article: {pub_date}"
                                    )
                                    continue
                            except Exception:
                                pass

                        # Check if it's from a trusted PR distribution service
                        is_pr_service = any(
                            domain in link_url.lower()
                            for domain in self.PR_DISTRIBUTION_DOMAINS
                        )

                        # Fetch and parse ANY leadership article (not just PR services)
                        # Limit to first 5 non-PR articles to avoid overload
                        should_parse = is_pr_service or articles_parsed < 5

                        if should_parse and link_url:
                            source_type = (
                                "pr_service" if is_pr_service else "news_article"
                            )
                            logger.info(
                                f"[NewsAgent] Fetching {source_type}: {link_url[:60]}"
                            )
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
                                    source_type="bing_news",
                                )

                                parse_result = await self.parser.parse(pr)
                                if parse_result.changes:
                                    logger.info(
                                        f"[NewsAgent] Bing News: Extracted {len(parse_result.changes)} "
                                        f"changes from: {title_text[:50]}"
                                    )
                                changes.extend(parse_result.changes)
                        else:
                            logger.debug(
                                f"[NewsAgent] Skipping (limit reached): {link_url[:50]}"
                            )

                    except Exception as e:
                        logger.debug(
                            f"[NewsAgent] Error processing Bing News item: {e}"
                        )

            except Exception as e:
                logger.warning(f"[NewsAgent] Bing News search error: {e}")

        logger.info(
            f"[NewsAgent] Bing News: Found {articles_found} leadership articles, "
            f"parsed {articles_parsed}"
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
                logger.info(
                    f"[NewsAgent] PR Newswire: Found {len(pr_links)} potential releases"
                )

                for pr_info in pr_links[:5]:  # Limit to 5
                    if self._is_leadership_related(pr_info["title"]):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info["url"])

                        pr_html = await self.fetch_url(pr_info["url"])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info["title"],
                                content=pr_html,
                                publish_date=pr_info.get("date"),
                                source_url=pr_info["url"],
                                company_name=company_name,
                                source_type="prnewswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(
                                    f"[NewsAgent] PR Newswire: {len(parse_result.changes)} changes from {pr_info['title'][:40]}"
                                )
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
                logger.info(
                    f"[NewsAgent] Business Wire: Found {len(bw_links)} potential releases"
                )

                for pr_info in bw_links[:5]:
                    if self._is_leadership_related(pr_info["title"]):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info["url"])

                        pr_html = await self.fetch_url(pr_info["url"])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info["title"],
                                content=pr_html,
                                publish_date=pr_info.get("date"),
                                source_url=pr_info["url"],
                                company_name=company_name,
                                source_type="businesswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(
                                    f"[NewsAgent] Business Wire: {len(parse_result.changes)} changes from {pr_info['title'][:40]}"
                                )
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
                logger.info(
                    f"[NewsAgent] GlobeNewswire: Found {len(globe_links)} potential releases"
                )

                for pr_info in globe_links[:5]:
                    if self._is_leadership_related(pr_info["title"]):
                        result.pages_checked += 1
                        result.page_urls.append(pr_info["url"])

                        pr_html = await self.fetch_url(pr_info["url"])
                        if pr_html:
                            pr = PressRelease(
                                title=pr_info["title"],
                                content=pr_html,
                                publish_date=pr_info.get("date"),
                                source_url=pr_info["url"],
                                company_name=company_name,
                                source_type="globenewswire",
                            )
                            parse_result = await self.parser.parse(pr)
                            if parse_result.changes:
                                logger.info(
                                    f"[NewsAgent] GlobeNewswire: {len(parse_result.changes)} changes"
                                )
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
                soup = BeautifulSoup(content, "xml")
                items = soup.find_all("item")

                leadership_items = 0
                cutoff_date = date.today() - timedelta(days=days_back)

                for item in items[:10]:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")

                    if not title_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    if self._is_leadership_related(title):
                        leadership_items += 1
                        if leadership_items > 5:
                            break  # Limit to 5 parsed articles

                        if link_elem:
                            link = link_elem.get_text(strip=True)
                            result.page_urls.append(link)
                            logger.debug(
                                f"[NewsAgent] Yahoo Finance match: {title[:50]}"
                            )

                            # Parse date
                            pub_date = None
                            if pub_date_elem:
                                try:
                                    from dateutil import parser as date_parser

                                    pub_date = date_parser.parse(
                                        pub_date_elem.get_text()
                                    ).date()
                                    if pub_date < cutoff_date:
                                        continue
                                except Exception:
                                    pass

                            # Fetch and parse the article
                            result.pages_checked += 1
                            article_html = await self.fetch_url(link)
                            if article_html:
                                pr = PressRelease(
                                    title=title,
                                    content=article_html,
                                    publish_date=pub_date,
                                    source_url=link,
                                    company_name=company_name,
                                    source_type="yahoo_finance",
                                )
                                parse_result = await self.parser.parse(pr)
                                changes.extend(parse_result.changes)

                logger.info(
                    f"[NewsAgent] Yahoo Finance: {leadership_items} leadership items found"
                )
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
                    if self._is_leadership_related(article["title"]):
                        logger.debug(
                            f"[NewsAgent] MarketWatch match: {article['title'][:50]}"
                        )
                        result.page_urls.append(article["url"])

                        # Fetch and parse the article
                        result.pages_checked += 1
                        article_html = await self.fetch_url(article["url"])
                        if article_html:
                            pr = PressRelease(
                                title=article["title"],
                                content=article_html,
                                publish_date=article.get("date"),
                                source_url=article["url"],
                                company_name=company_name,
                                source_type="marketwatch",
                            )
                            parse_result = await self.parser.parse(pr)
                            changes.extend(parse_result.changes)
        except Exception as e:
            logger.debug(f"[NewsAgent] MarketWatch search error: {e}")

        return changes

    def _normalize_company_for_search(self, company_name: str) -> str:
        """Normalize company name for search queries."""
        # Remove common suffixes
        suffixes = [
            ", Inc.",
            ", Inc",
            " Inc.",
            " Inc",
            ", Corp.",
            ", Corp",
            " Corp.",
            " Corp",
            ", LLC",
            " LLC",
            ", Ltd.",
            ", Ltd",
            " Ltd.",
            " Ltd",
            ", Co.",
            ", Co",
            " Co.",
            " Co",
            " Company",
            " Corporation",
            " International",
        ]

        name = company_name
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break

        return name.strip()

    def _extract_pr_newswire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from PR Newswire search results."""
        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # PR Newswire uses specific card structure
        for card in soup.find_all(
            ["div", "article"], class_=lambda x: x and "card" in str(x).lower()
        ):
            link = card.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)

                if href and title and "/news-releases/" in href:
                    full_url = urljoin("https://www.prnewswire.com", href)
                    releases.append(
                        {
                            "url": full_url,
                            "title": title,
                            "date": None,
                        }
                    )

        return releases[:10]

    def _extract_business_wire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from Business Wire search results."""
        soup = BeautifulSoup(html, "html.parser")
        releases = []

        for item in soup.find_all(
            ["div", "li"],
            class_=lambda x: x
            and any(kw in str(x).lower() for kw in ["result", "news", "headline"]),
        ):
            link = item.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)

                if href and title:
                    full_url = urljoin("https://www.businesswire.com", href)
                    releases.append(
                        {
                            "url": full_url,
                            "title": title,
                            "date": None,
                        }
                    )

        return releases[:10]

    def _extract_globenewswire_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract press release links from GlobeNewswire search results."""
        soup = BeautifulSoup(html, "html.parser")
        releases = []

        for item in soup.find_all(
            ["div", "article"], class_=lambda x: x and "article" in str(x).lower()
        ):
            link = item.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)

                if href and title and "/news-release/" in href:
                    full_url = urljoin("https://www.globenewswire.com", href)
                    releases.append(
                        {
                            "url": full_url,
                            "title": title,
                            "date": None,
                        }
                    )

        return releases[:10]

    def _extract_marketwatch_links(
        self,
        html: str,
        days_back: int,
    ) -> List[Dict[str, Any]]:
        """Extract article links from MarketWatch search results."""
        soup = BeautifulSoup(html, "html.parser")
        articles = []

        for item in soup.find_all(
            ["div", "article"], class_=lambda x: x and "article" in str(x).lower()
        ):
            link = item.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)

                if href and title:
                    articles.append(
                        {
                            "url": href,
                            "title": title,
                            "date": None,
                        }
                    )

        return articles[:10]

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
                change.change_type.value
                if hasattr(change.change_type, "value")
                else str(change.change_type),
            )

            if key not in seen:
                seen.add(key)
                unique.append(change)

        return unique

    def _finalize_result(self, result: CollectionResult) -> CollectionResult:
        """Finalize collection result with timing."""
        result.completed_at = datetime.utcnow()
        result.duration_seconds = (
            result.completed_at - result.started_at
        ).total_seconds()
        return result

    async def deep_collect(
        self,
        company_id: int,
        company_name: str,
        subsidiary_names: Optional[List[str]] = None,
        website_url: Optional[str] = None,
        newsroom_url: Optional[str] = None,
        days_back: int = 1825,
        max_results_per_query: int = 20,
    ) -> CollectionResult:
        """
        Deep news collection with multi-query search and expanded time windows.

        Enhanced version of collect() for Fortune 500 companies with multiple
        subsidiaries and divisions. Runs separate search queries for each
        subsidiary name, crawls newsroom archives, and increases result limits.

        Args:
            company_id: Database company ID
            company_name: Primary company name
            subsidiary_names: List of subsidiary/division names to search for
            website_url: Company website URL
            newsroom_url: Direct newsroom URL (if known)
            days_back: How far back to search (default 5 years)
            max_results_per_query: Max results per Google News query
        """
        started_at = datetime.utcnow()

        result = CollectionResult(
            company_id=company_id,
            company_name=company_name,
            source="news_deep",
            started_at=started_at,
        )

        result.pages_checked = 0
        result.page_urls = []
        all_changes: List[LeadershipChange] = []

        logger.info(
            f"[NewsAgent] Deep collection for {company_name}: "
            f"days_back={days_back}, subsidiaries={subsidiary_names}"
        )

        try:
            # 1. Standard collection first
            standard_result = await self.collect(
                company_id,
                company_name,
                website_url,
                days_back=min(days_back, 365),
                include_google_news=True,
                newsroom_url=newsroom_url,
            )
            all_changes.extend(standard_result.extracted_changes)
            result.pages_checked += standard_result.pages_checked
            result.errors.extend(standard_result.errors)

            # 2. Multi-query search for each subsidiary name
            all_names = [company_name] + (subsidiary_names or [])

            for name in all_names:
                search_queries = [
                    f'"{name}" appoints OR names OR hires executive',
                    f'"{name}" "promoted to" vice president OR managing director',
                    f'"{name}" CEO OR CFO OR president appointed OR named',
                    f'"{name}" board director elected OR appointed',
                ]

                for query in search_queries:
                    try:
                        changes = await self._search_bing_news_query(
                            query,
                            company_name,
                            days_back,
                            max_results=max_results_per_query,
                            result=result,
                        )
                        all_changes.extend(changes)
                    except Exception as e:
                        logger.debug(
                            f"[NewsAgent] Query failed for '{query[:40]}': {e}"
                        )

            # 2b. Search PR services for each subsidiary name
            for name in all_names[
                1:
            ]:  # Skip main name (already searched in standard collect)
                try:
                    pr_changes = await self._search_pr_services(name, days_back, result)
                    all_changes.extend(pr_changes)
                except Exception as e:
                    logger.debug(f"[NewsAgent] PR services failed for '{name}': {e}")

            # 3. Crawl newsroom archive if URL provided
            if newsroom_url:
                logger.info(f"[NewsAgent] Crawling newsroom archive: {newsroom_url}")
                archive_changes = await self._crawl_newsroom_archive(
                    newsroom_url,
                    company_name,
                    days_back,
                    result,
                )
                all_changes.extend(archive_changes)

            # Deduplicate
            unique_changes = self._deduplicate_changes(all_changes)

            result.extracted_changes = unique_changes
            result.changes_detected = len(unique_changes)
            result.success = True

            logger.info(
                f"[NewsAgent] Deep collection complete for {company_name}: "
                f"{len(unique_changes)} unique changes from {result.pages_checked} pages"
            )

        except Exception as e:
            logger.exception(
                f"[NewsAgent] Deep collection error for {company_name}: {e}"
            )
            result.errors.append(str(e))
            result.success = False

        return self._finalize_result(result)

    async def _search_bing_news_query(
        self,
        query: str,
        company_name: str,
        days_back: int,
        max_results: int,
        result: CollectionResult,
    ) -> List[LeadershipChange]:
        """Run a single Bing News query and extract changes."""
        changes = []
        cutoff_date = date.today() - timedelta(days=days_back)

        encoded_query = quote_plus(query)
        rss_url = f"https://www.bing.com/news/search?q={encoded_query}&format=rss"

        result.pages_checked += 1
        content = await self.fetch_url(rss_url)
        if not content:
            return changes

        soup = BeautifulSoup(content, "xml")
        items = soup.find_all("item")

        for item in items[:max_results]:
            try:
                title_elem = item.find("title")
                link_elem = item.find("link")
                pub_date_elem = item.find("pubDate")

                if not title_elem or not link_elem:
                    continue

                title_text = title_elem.get_text(strip=True)
                # Bing returns direct article URLs
                link_url = link_elem.get_text(strip=True)

                if not self._is_leadership_related(title_text):
                    continue

                # Parse date
                pub_date = None
                if pub_date_elem:
                    try:
                        from dateutil import parser as date_parser

                        pub_date = date_parser.parse(pub_date_elem.get_text()).date()
                        if pub_date < cutoff_date:
                            continue
                    except Exception:
                        pass

                if not link_url:
                    continue

                result.pages_checked += 1
                article_html = await self.fetch_url(link_url)
                if not article_html:
                    continue

                pr = PressRelease(
                    title=title_text,
                    content=article_html,
                    publish_date=pub_date,
                    source_url=link_url,
                    company_name=company_name,
                    source_type="bing_news_deep",
                )

                parse_result = await self.parser.parse(pr)
                changes.extend(parse_result.changes)

            except Exception as e:
                logger.debug(f"[NewsAgent] Error processing deep news item: {e}")

        return changes

    async def _crawl_newsroom_archive(
        self,
        newsroom_url: str,
        company_name: str,
        days_back: int,
        result: CollectionResult,
        max_pages: int = 5,
    ) -> List[LeadershipChange]:
        """
        Crawl a company's newsroom archive for leadership press releases.

        Paginates through the newsroom and extracts leadership-related
        press releases.
        """
        changes = []
        cutoff_date = date.today() - timedelta(days=days_back)

        # Normalize URL
        if not newsroom_url.startswith("http"):
            newsroom_url = "https://" + newsroom_url
        base_url = newsroom_url.rstrip("/")

        # Try to find pagination pattern
        page_urls = [base_url]

        # Common pagination patterns
        pagination_patterns = [
            "/page/{page}",
            "?page={page}",
            "?p={page}",
            "&page={page}",
        ]

        # Add paginated URLs
        for page_num in range(2, max_pages + 1):
            for pattern in pagination_patterns:
                page_urls.append(base_url + pattern.format(page=page_num))

        for page_url in page_urls:
            try:
                result.pages_checked += 1
                html = await self.fetch_url(page_url)
                if not html:
                    continue

                press_releases = self._extract_press_release_links(
                    html, page_url, days_back
                )

                # Filter to leadership-related
                leadership_releases = [
                    pr
                    for pr in press_releases
                    if self._is_leadership_related(pr["title"])
                ]

                for pr_info in leadership_releases[:10]:
                    try:
                        result.pages_checked += 1
                        pr_html = await self.fetch_url(pr_info["url"])
                        if not pr_html:
                            continue

                        pr = PressRelease(
                            title=pr_info["title"],
                            content=pr_html,
                            publish_date=pr_info.get("date"),
                            source_url=pr_info["url"],
                            company_name=company_name,
                            source_type="newsroom_archive",
                        )

                        parse_result = await self.parser.parse(pr)
                        changes.extend(parse_result.changes)

                    except Exception as e:
                        logger.debug(f"[NewsAgent] Archive PR error: {e}")

                # If no press releases found on a page, stop paginating
                if not press_releases:
                    break

            except Exception as e:
                logger.debug(f"[NewsAgent] Archive page error: {e}")
                break

        logger.info(f"[NewsAgent] Newsroom archive: {len(changes)} changes extracted")
        return changes


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
