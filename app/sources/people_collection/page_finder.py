"""
Leadership page discovery for company websites.

Finds pages that contain leadership/team information by:
1. Trying common URL patterns
2. Crawling the homepage for relevant links
3. Checking sitemap.xml
"""

import asyncio
import logging
import re
from typing import Optional, List, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.config import (
    LEADERSHIP_URL_PATTERNS,
    LEADERSHIP_LINK_PATTERNS,
)

logger = logging.getLogger(__name__)


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

    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are on the same domain."""
        domain1 = urlparse(url1).netloc.lower()
        domain2 = urlparse(url2).netloc.lower()

        # Handle www prefix
        domain1 = domain1.replace("www.", "")
        domain2 = domain2.replace("www.", "")

        return domain1 == domain2

    def _extract_links(self, html: str, base_url: str) -> List[Tuple[str, str]]:
        """
        Extract links from HTML with their anchor text.

        Returns list of (url, text) tuples.
        """
        soup = BeautifulSoup(html, 'html.parser')
        links = []

        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True).lower()

            # Skip empty or javascript links
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
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
        high_value = ["leadership", "executive", "management-team", "our-team", "leadership-team"]
        for keyword in high_value:
            if keyword in url_lower:
                score += 10

        # Medium-value keywords
        medium_value = ["team", "about", "management", "people", "who-we-are"]
        for keyword in medium_value:
            if keyword in url_lower:
                score += 5

        # Link text bonuses
        text_keywords = ["leadership", "team", "executive", "management", "meet the", "our people"]
        for keyword in text_keywords:
            if keyword in text_lower:
                score += 8

        # Board-specific pages
        if "board" in url_lower or "directors" in url_lower:
            score += 7

        # Penalize clearly wrong pages
        wrong_patterns = ["career", "job", "investor", "news", "press", "blog", "contact", "login", "signup"]
        for pattern in wrong_patterns:
            if pattern in url_lower:
                score -= 10

        return score

    async def find_leadership_pages(
        self,
        website_url: str,
        max_pages: int = 5,
    ) -> List[dict]:
        """
        Find leadership/team pages on a company website.

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

        logger.info(f"Finding leadership pages for {base_url}")

        # Strategy 1: Try common URL patterns directly
        pattern_pages = await self._try_url_patterns(base_url)
        found_pages.extend(pattern_pages)

        # Strategy 2: Crawl homepage for links
        if len(found_pages) < max_pages:
            homepage_pages = await self._crawl_homepage(base_url)
            for page in homepage_pages:
                if page['url'] not in [p['url'] for p in found_pages]:
                    found_pages.append(page)

        # Strategy 3: Check sitemap
        if len(found_pages) < max_pages:
            sitemap_pages = await self._check_sitemap(base_url)
            for page in sitemap_pages:
                if page['url'] not in [p['url'] for p in found_pages]:
                    found_pages.append(page)

        # Sort by score and limit
        found_pages.sort(key=lambda x: x['score'], reverse=True)
        result = found_pages[:max_pages]

        logger.info(f"Found {len(result)} leadership pages for {base_url}")
        return result

    async def _try_url_patterns(self, base_url: str) -> List[dict]:
        """Try common leadership URL patterns."""
        found = []

        # Try patterns in batches for efficiency
        batch_size = 5
        for i in range(0, len(LEADERSHIP_URL_PATTERNS), batch_size):
            batch = LEADERSHIP_URL_PATTERNS[i:i + batch_size]

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
                        found.append({
                            'url': url,
                            'page_type': page_type,
                            'score': score,
                            'source': 'url_pattern',
                        })

        return found

    async def _check_url_exists(self, url: str) -> bool:
        """Check if a URL returns 200."""
        try:
            return await self.check_url_exists(url)
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
                    found.append({
                        'url': url,
                        'page_type': page_type,
                        'score': score,
                        'source': 'homepage_link',
                        'link_text': text,
                    })

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
                soup = BeautifulSoup(content, 'xml')
                urls = soup.find_all('loc')

                for loc in urls:
                    url = loc.get_text(strip=True)

                    if url in self._visited_urls:
                        continue

                    score = self._score_leadership_url(url)
                    if score > 5:
                        self._visited_urls.add(url)
                        page_type = self._infer_page_type(url)
                        found.append({
                            'url': url,
                            'page_type': page_type,
                            'score': score,
                            'source': 'sitemap',
                        })

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
            page['company_name'] = company_name

        return pages
