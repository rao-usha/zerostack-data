"""
Leadership page discovery for company websites.

Finds pages that contain leadership/team information by:
1. Trying common URL patterns
2. Crawling the homepage for relevant links
3. Checking sitemap.xml
4. Google search fallback (site:domain.com leadership team)
"""

import asyncio
import logging
import re
import os
from typing import Optional, List, Set, Tuple
from urllib.parse import urljoin, urlparse, quote_plus

import aiohttp
from bs4 import BeautifulSoup

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.config import (
    LEADERSHIP_URL_PATTERNS,
    LEADERSHIP_LINK_PATTERNS,
)

logger = logging.getLogger(__name__)

# Google Custom Search API (optional - for search fallback)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")


class PageFinder(BaseCollector):
    """
    Discovers leadership and team pages on company websites.

    Uses multiple strategies:
    1. Direct URL pattern matching (e.g., /about/leadership)
    2. Homepage link crawling
    3. Sitemap parsing
    """

    def __init__(self):
        super().__init__(source_type="website")
        self._visited_urls: Set[str] = set()

    def _normalize_url(self, base_url: str, path: str) -> str:
        """Normalize and join URLs."""
        # Remove trailing slash from base
        base_url = base_url.rstrip("/")

        # Ensure path starts with /
        if not path.startswith("/") and not path.startswith("http"):
            path = "/" + path

        # Join URLs
        if path.startswith("http"):
            return path
        return urljoin(base_url, path)

    def _is_same_domain(
        self, url1: str, url2: str, allow_subdomains: bool = True
    ) -> bool:
        """
        Check if two URLs are on the same domain.

        Args:
            url1: First URL
            url2: Second URL
            allow_subdomains: If True, ir.company.com matches www.company.com
        """
        domain1 = urlparse(url1).netloc.lower()
        domain2 = urlparse(url2).netloc.lower()

        # Handle www prefix
        domain1 = domain1.replace("www.", "")
        domain2 = domain2.replace("www.", "")

        # Exact match
        if domain1 == domain2:
            return True

        # Allow subdomain matching (e.g., ir.lincolnelectric.com matches lincolnelectric.com)
        if allow_subdomains:
            # Extract root domain (last 2 parts)
            parts1 = domain1.split(".")
            parts2 = domain2.split(".")

            # Get root domain (e.g., lincolnelectric.com)
            root1 = ".".join(parts1[-2:]) if len(parts1) >= 2 else domain1
            root2 = ".".join(parts2[-2:]) if len(parts2) >= 2 else domain2

            return root1 == root2

        return False

    def _get_ir_subdomain_urls(self, base_url: str) -> list:
        """
        Get investor relations subdomain variations.

        Many public companies host leadership info on ir.company.com or investors.company.com
        """
        parsed = urlparse(base_url)
        domain = parsed.netloc.lower().replace("www.", "")

        # Common IR subdomain patterns
        ir_subdomains = [
            f"ir.{domain}",
            f"investors.{domain}",
            f"investor.{domain}",
        ]

        return [f"https://{subdomain}" for subdomain in ir_subdomains]

    def _extract_links(self, html: str, base_url: str) -> List[Tuple[str, str]]:
        """
        Extract links from HTML with their anchor text.

        Returns list of (url, text) tuples.
        """
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()

            # Skip empty or javascript links
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            # Normalize URL
            full_url = self._normalize_url(base_url, href)

            # Only include same-domain links
            if self._is_same_domain(base_url, full_url):
                links.append((full_url, text))

        return links

    def _is_leadership_url(self, url: str, link_text: str = "") -> bool:
        """Check if a URL or link text suggests a leadership page."""
        url_lower = url.lower()

        # Check URL patterns
        for pattern in LEADERSHIP_URL_PATTERNS:
            if pattern in url_lower:
                return True

        # Check link text patterns
        for pattern in LEADERSHIP_LINK_PATTERNS:
            if re.search(pattern, link_text, re.IGNORECASE):
                return True
            if re.search(pattern, url_lower, re.IGNORECASE):
                return True

        return False

    def _score_leadership_url(self, url: str, link_text: str = "") -> int:
        """
        Score how likely a URL is to be a leadership page.

        Higher score = more likely to be relevant.
        """
        score = 0
        url_lower = url.lower()
        text_lower = link_text.lower()

        # High-value keywords in URL
        high_value = [
            "leadership",
            "executive",
            "management-team",
            "our-team",
            "leadership-team",
        ]
        for keyword in high_value:
            if keyword in url_lower:
                score += 10

        # Medium-value keywords
        medium_value = ["team", "management", "people", "who-we-are"]
        for keyword in medium_value:
            if keyword in url_lower:
                score += 5

        # Investor relations governance pages (often have leadership)
        ir_patterns = [
            "governance",
            "corporate-governance",
            "executive-officers",
            "corporate-officers",
        ]
        for pattern in ir_patterns:
            if pattern in url_lower:
                score += 8

        # Link text bonuses
        text_keywords = [
            "leadership",
            "team",
            "executive",
            "management",
            "meet the",
            "our people",
            "officers",
        ]
        for keyword in text_keywords:
            if keyword in text_lower:
                score += 8

        # Board-specific pages
        if "board" in url_lower or "directors" in url_lower:
            score += 7

        # Penalize clearly wrong pages
        wrong_patterns = [
            "career",
            "job",
            "news",
            "press",
            "blog",
            "contact",
            "login",
            "signup",
            "product",
            "service",
            "shop",
            "store",
            "support",
        ]
        for pattern in wrong_patterns:
            if pattern in url_lower:
                score -= 10

        # Heavily penalize ESG/sustainability pages (these rarely have leadership)
        esg_patterns = [
            "esg",
            "sustainability",
            "environmental",
            "social-responsibility",
            "csr",
            "diversity",
            "inclusion",
            "empowering",
        ]
        for pattern in esg_patterns:
            if pattern in url_lower:
                score -= 15

        # Penalize generic investor pages (but not governance)
        if (
            "investor" in url_lower
            and "governance" not in url_lower
            and "leadership" not in url_lower
        ):
            score -= 5

        # About page without specifics should score lower
        if url_lower.endswith("/about") or url_lower.endswith("/about/"):
            score += 2  # Still positive, but lower than specific pages

        return score

    async def find_leadership_pages(
        self,
        website_url: str,
        max_pages: int = 5,
    ) -> List[dict]:
        """
        Find leadership/team pages on a company website.

        Strategies (in order):
        1. Try common URL patterns directly
        2. Crawl homepage for relevant links
        3. Check sitemap.xml
        4. Google search fallback (if API configured)

        Args:
            website_url: Base website URL (e.g., https://example.com)
            max_pages: Maximum number of pages to return

        Returns:
            List of dicts with 'url', 'page_type', and 'score'
        """
        self._visited_urls.clear()
        found_pages = []

        # Normalize base URL
        parsed = urlparse(website_url)
        if not parsed.scheme:
            website_url = "https://" + website_url
        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc or website_url.replace('https://', '').replace('http://', '').split('/')[0]}"

        logger.info(f"[PageFinder] Finding leadership pages for {base_url}")

        # Strategy 1: Try common URL patterns directly
        logger.debug(
            f"[PageFinder] Strategy 1: Trying {len(LEADERSHIP_URL_PATTERNS)} URL patterns"
        )
        pattern_pages = await self._try_url_patterns(base_url)
        found_pages.extend(pattern_pages)
        logger.info(f"[PageFinder] URL patterns found {len(pattern_pages)} pages")

        # Strategy 1b: Try IR subdomain patterns (for public companies)
        if len(found_pages) < max_pages:
            ir_urls = self._get_ir_subdomain_urls(base_url)
            ir_patterns = [
                "/governance/leadership-team",
                "/governance/board-of-directors",
                "/corporate-governance/leadership-team",
                "/corporate-governance/board-of-directors",
                "/governance/executive-officers",
                "/governance/leadership-team/default.aspx",
                "/governance/board-of-directors/default.aspx",
            ]
            for ir_base in ir_urls:
                for pattern in ir_patterns:
                    url = ir_base + pattern
                    if url not in self._visited_urls:
                        self._visited_urls.add(url)
                        exists = await self._check_url_exists(url)
                        if exists:
                            page_type = self._infer_page_type(url)
                            score = self._score_leadership_url(url)
                            found_pages.append(
                                {
                                    "url": url,
                                    "page_type": page_type,
                                    "score": score + 5,  # Bonus for IR pages
                                    "source": "ir_subdomain",
                                }
                            )
                            logger.info(f"[PageFinder] IR subdomain found: {url}")
            logger.info(
                f"[PageFinder] IR subdomain strategy found {len([p for p in found_pages if p.get('source') == 'ir_subdomain'])} pages"
            )

        # Early exit: if we already have enough high-quality pages, skip fallback strategies
        high_quality_pages = [p for p in found_pages if p.get("score", 0) >= 8]
        if len(high_quality_pages) >= 3:
            logger.info(
                f"[PageFinder] Found {len(high_quality_pages)} high-quality pages (score>=8), "
                f"skipping fallback strategies (homepage crawl, sitemap, search)"
            )
            found_pages.sort(key=lambda x: x["score"], reverse=True)
            return found_pages[:max_pages]

        # Strategy 2: Crawl homepage for links
        if len(found_pages) < max_pages:
            logger.debug(f"[PageFinder] Strategy 2: Crawling homepage for links")
            homepage_pages = await self._crawl_homepage(base_url)
            new_count = 0
            for page in homepage_pages:
                if page["url"] not in [p["url"] for p in found_pages]:
                    found_pages.append(page)
                    new_count += 1
            logger.info(
                f"[PageFinder] Homepage crawl found {new_count} additional pages"
            )

        # Strategy 3: Check sitemap
        if len(found_pages) < max_pages:
            logger.debug(f"[PageFinder] Strategy 3: Checking sitemap.xml")
            sitemap_pages = await self._check_sitemap(base_url)
            new_count = 0
            for page in sitemap_pages:
                if page["url"] not in [p["url"] for p in found_pages]:
                    found_pages.append(page)
                    new_count += 1
            logger.info(f"[PageFinder] Sitemap found {new_count} additional pages")

        # Strategy 4: Google search fallback (only if Google API is configured)
        # DuckDuckGo is blocked from Docker containers — skip unless Google API available
        if len(found_pages) < 2 and GOOGLE_API_KEY and GOOGLE_CSE_ID:
            logger.info(f"[PageFinder] Strategy 4: Trying Google API search fallback")
            google_pages = await self._google_api_search(base_url)
            found_pages.extend(google_pages)
            logger.info(f"[PageFinder] Google search found {len(google_pages)} pages")
        elif len(found_pages) < 2:
            logger.info(
                f"[PageFinder] Strategy 4: Skipping search fallback (no Google API key, DuckDuckGo blocked in Docker)"
            )

        # Sort by score and limit
        found_pages.sort(key=lambda x: x["score"], reverse=True)
        result = found_pages[:max_pages]

        if result:
            logger.info(
                f"[PageFinder] Found {len(result)} leadership pages for {base_url}"
            )
            for page in result:
                logger.debug(
                    f"[PageFinder] - {page['url']} (score={page['score']}, type={page['page_type']})"
                )
        else:
            logger.warning(
                f"[PageFinder] No leadership pages found for {base_url}. "
                f"Website may have non-standard structure, require JavaScript, "
                f"or block automated access."
            )

        return result

    async def _google_search_fallback(self, base_url: str) -> List[dict]:
        """
        Use Google Custom Search API to find leadership pages.

        Requires GOOGLE_API_KEY and GOOGLE_CSE_ID environment variables.
        Falls back to DuckDuckGo if Google API not configured.
        """
        found = []

        # Try Google first if configured
        if GOOGLE_API_KEY and GOOGLE_CSE_ID:
            found = await self._google_api_search(base_url)
            if found:
                return found

        # Fall back to DuckDuckGo HTML scraping (no API needed)
        found = await self._duckduckgo_search(base_url)

        return found

    async def _google_api_search(self, base_url: str) -> List[dict]:
        """Search using Google Custom Search API."""
        found = []

        # Extract domain for site: search
        parsed = urlparse(base_url)
        domain = parsed.netloc.replace("www.", "")

        # Search queries to try - include root domain to catch IR subdomains
        queries = [
            f"site:{domain} leadership team",
            f"site:{domain} about team executives",
            f"site:{domain} management board directors",
            f"site:ir.{domain} leadership",
            f"site:ir.{domain} governance board",
        ]

        for query in queries:
            try:
                search_url = (
                    f"https://www.googleapis.com/customsearch/v1"
                    f"?key={GOOGLE_API_KEY}"
                    f"&cx={GOOGLE_CSE_ID}"
                    f"&q={quote_plus(query)}"
                    f"&num=5"
                )

                data = await self.fetch_json(search_url, use_cache=False)

                if data and "items" in data:
                    for item in data["items"]:
                        url = item.get("link", "")

                        # Only include if on the same domain
                        if not self._is_same_domain(base_url, url):
                            continue

                        if url in self._visited_urls:
                            continue

                        self._visited_urls.add(url)
                        score = self._score_leadership_url(url, item.get("title", ""))

                        if score > 0:
                            page_type = self._infer_page_type(url)
                            found.append(
                                {
                                    "url": url,
                                    "page_type": page_type,
                                    "score": score,
                                    "source": "google_search",
                                    "title": item.get("title"),
                                }
                            )

                # If we found pages, don't need more queries
                if found:
                    break

                # Rate limit between queries
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.warning(f"[PageFinder] Google API search error: {e}")

        return found

    async def _duckduckgo_search(self, base_url: str) -> List[dict]:
        """
        Search using DuckDuckGo HTML (no API key required).

        Note: This is a fallback and may be rate limited.
        """
        found = []

        parsed = urlparse(base_url)
        domain = parsed.netloc.replace("www.", "")

        # Search entire domain including subdomains like ir.company.com
        query = f"site:{domain} leadership team executives governance"
        search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"

        try:
            # DuckDuckGo needs different headers
            logger.info(f"[PageFinder] DuckDuckGo searching: {query}")
            html = await self.fetch_url(search_url)

            if not html:
                logger.warning("[PageFinder] DuckDuckGo returned empty response")
                return found

            soup = BeautifulSoup(html, "html.parser")
            all_links = soup.select(".result__a")
            logger.info(f"[PageFinder] DuckDuckGo found {len(all_links)} raw results")

            # DuckDuckGo HTML results are in .result__a links
            for link in all_links:
                href = link.get("href", "")

                # DuckDuckGo wraps URLs, need to extract
                if "uddg=" in href:
                    # URL is encoded in uddg parameter
                    import urllib.parse

                    parsed_href = urllib.parse.urlparse(href)
                    params = urllib.parse.parse_qs(parsed_href.query)
                    if "uddg" in params:
                        href = params["uddg"][0]

                # Only include same domain
                if not self._is_same_domain(base_url, href):
                    continue

                if href in self._visited_urls:
                    continue

                self._visited_urls.add(href)
                title = link.get_text(strip=True)
                score = self._score_leadership_url(href, title)

                if score > 0:
                    page_type = self._infer_page_type(href)
                    found.append(
                        {
                            "url": href,
                            "page_type": page_type,
                            "score": score,
                            "source": "duckduckgo_search",
                            "title": title,
                        }
                    )

                if len(found) >= 5:
                    break

        except Exception as e:
            logger.warning(f"[PageFinder] DuckDuckGo search error: {e}")

        return found

    async def _try_url_patterns(self, base_url: str, max_pages: int = 5) -> List[dict]:
        """Try common leadership URL patterns."""
        found = []

        # Try patterns in batches for efficiency
        batch_size = 10
        for i in range(0, len(LEADERSHIP_URL_PATTERNS), batch_size):
            # Stop early if we already have enough pages
            if len(found) >= max_pages:
                break
            batch = LEADERSHIP_URL_PATTERNS[i : i + batch_size]

            tasks = []
            for pattern in batch:
                url = base_url + pattern
                if url not in self._visited_urls:
                    self._visited_urls.add(url)
                    tasks.append(self._check_url_exists(url))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, result in enumerate(results):
                    if isinstance(result, bool) and result:
                        url = base_url + batch[j]
                        page_type = self._infer_page_type(url)
                        score = self._score_leadership_url(url)
                        if score <= 0:
                            logger.debug(
                                f"[PageFinder] Skipping low-score URL pattern: {url} (score={score})"
                            )
                            continue
                        found.append(
                            {
                                "url": url,
                                "page_type": page_type,
                                "score": score,
                                "source": "url_pattern",
                            }
                        )

        return found

    async def _check_url_exists(self, url: str) -> bool:
        """Check if a URL returns 200 with a fast timeout."""
        try:
            await self.rate_limiter.acquire(url, self.source_type)
            session = await self._get_session()
            headers = self._get_headers(url)
            fast_timeout = aiohttp.ClientTimeout(
                total=10
            )  # 10s instead of 30s for existence checks
            async with session.head(
                url, headers=headers, allow_redirects=True, timeout=fast_timeout
            ) as response:
                return response.status == 200
        except Exception:
            return False

    async def _crawl_homepage(self, base_url: str) -> List[dict]:
        """Crawl homepage to find leadership links."""
        found = []

        try:
            homepage_html = await self.fetch_url(base_url)
            if not homepage_html:
                return found

            links = self._extract_links(homepage_html, base_url)

            # Score and filter links
            scored_links = []
            for url, text in links:
                if url in self._visited_urls:
                    continue

                score = self._score_leadership_url(url, text)
                if score > 0:
                    scored_links.append((url, text, score))

            # Sort by score
            scored_links.sort(key=lambda x: x[2], reverse=True)

            # Verify top candidates exist
            for url, text, score in scored_links[:10]:
                if url in self._visited_urls:
                    continue

                self._visited_urls.add(url)
                exists = await self._check_url_exists(url)

                if exists:
                    page_type = self._infer_page_type(url)
                    found.append(
                        {
                            "url": url,
                            "page_type": page_type,
                            "score": score,
                            "source": "homepage_link",
                            "link_text": text,
                        }
                    )

        except Exception as e:
            logger.warning(f"Error crawling homepage {base_url}: {e}")

        return found

    async def _check_sitemap(self, base_url: str) -> List[dict]:
        """Check sitemap.xml for leadership pages."""
        found = []

        sitemap_urls = [
            base_url + "/sitemap.xml",
            base_url + "/sitemap_index.xml",
            base_url + "/sitemap-index.xml",
        ]

        for sitemap_url in sitemap_urls:
            try:
                content = await self.fetch_url(sitemap_url)
                if not content:
                    continue

                # Parse sitemap XML
                soup = BeautifulSoup(content, "xml")
                urls = soup.find_all("loc")

                for loc in urls:
                    url = loc.get_text(strip=True)

                    if url in self._visited_urls:
                        continue

                    score = self._score_leadership_url(url)
                    if score > 5:
                        self._visited_urls.add(url)
                        page_type = self._infer_page_type(url)
                        found.append(
                            {
                                "url": url,
                                "page_type": page_type,
                                "score": score,
                                "source": "sitemap",
                            }
                        )

                # Only need one working sitemap
                if found:
                    break

            except Exception as e:
                logger.debug(f"Error checking sitemap {sitemap_url}: {e}")

        return found

    def _infer_page_type(self, url: str) -> str:
        """Infer page type from URL."""
        url_lower = url.lower()

        if "board" in url_lower or "director" in url_lower:
            return "board"
        elif "leadership" in url_lower or "executive" in url_lower:
            return "leadership"
        elif "team" in url_lower or "people" in url_lower:
            return "team"
        elif "management" in url_lower:
            return "management"
        elif "about" in url_lower:
            return "about"
        else:
            return "unknown"


async def find_company_leadership_pages(
    company_name: str,
    website_url: str,
    max_pages: int = 5,
) -> List[dict]:
    """
    Convenience function to find leadership pages for a company.

    Args:
        company_name: Name of the company
        website_url: Company website URL
        max_pages: Maximum pages to return

    Returns:
        List of leadership page info dicts
    """
    async with PageFinder() as finder:
        pages = await finder.find_leadership_pages(website_url, max_pages)

        # Add company name to results
        for page in pages:
            page["company_name"] = company_name

        return pages
