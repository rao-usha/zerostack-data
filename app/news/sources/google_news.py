"""
Google News RSS Feed Parser.

Fetches news from Google News RSS feeds for companies and sectors.
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import quote_plus
import httpx

logger = logging.getLogger(__name__)

# Default search queries for investment news
DEFAULT_QUERIES = [
    "venture capital funding",
    "private equity acquisition",
    "startup series A",
    "IPO filing",
    "institutional investor",
]


class GoogleNewsSource:
    """Google News RSS feed source."""

    def __init__(self):
        self.name = "google_news"
        self.base_url = "https://news.google.com/rss/search"
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    def _build_url(self, query: str) -> str:
        """Build Google News RSS URL for a query."""
        encoded_query = quote_plus(query)
        return f"{self.base_url}?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    async def fetch(
        self,
        queries: Optional[List[str]] = None,
        company_names: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Fetch news from Google News.

        Args:
            queries: Custom search queries
            company_names: Company names to search for

        Returns:
            List of parsed news items.
        """
        items = []
        all_queries = []

        # Add default queries
        if queries:
            all_queries.extend(queries)
        else:
            all_queries.extend(DEFAULT_QUERIES[:3])  # Limit to avoid rate limits

        # Add company-specific queries
        if company_names:
            for company in company_names[:5]:  # Limit companies
                all_queries.append(f'"{company}" funding OR acquisition OR investment')

        # Fetch each query
        seen_urls = set()
        for query in all_queries:
            try:
                query_items = await self._fetch_query(query)
                for item in query_items:
                    # Deduplicate by URL
                    if item.get("url") and item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        items.append(item)
            except Exception as e:
                logger.error(f"Error fetching Google News for '{query}': {e}")

        logger.info(f"Fetched {len(items)} unique items from Google News")
        return items

    async def _fetch_query(self, query: str) -> List[Dict]:
        """Fetch and parse news for a single query."""
        items = []
        url = self._build_url(query)

        try:
            response = await self.client.get(
                url, headers={"User-Agent": "Mozilla/5.0 (compatible; Nexdata/1.0)"}
            )
            response.raise_for_status()

            # Parse RSS feed
            root = ET.fromstring(response.text)
            channel = root.find("channel")

            if channel is None:
                return items

            for item_elem in channel.findall("item"):
                item = self._parse_item(item_elem, query)
                if item:
                    items.append(item)

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching Google News: {e}")
        except ET.ParseError as e:
            logger.error(f"XML parse error for Google News: {e}")

        return items

    def _parse_item(self, item_elem: ET.Element, query: str) -> Optional[Dict]:
        """Parse a single RSS item into a news item."""
        try:
            title_elem = item_elem.find("title")
            link_elem = item_elem.find("link")
            pubdate_elem = item_elem.find("pubDate")
            description_elem = item_elem.find("description")
            guid_elem = item_elem.find("guid")

            if title_elem is None or title_elem.text is None:
                return None

            title = self._clean_html(title_elem.text.strip())

            # Parse published date (RFC 2822 format)
            published_at = None
            if pubdate_elem is not None and pubdate_elem.text:
                try:
                    from email.utils import parsedate_to_datetime

                    published_at = parsedate_to_datetime(pubdate_elem.text)
                except Exception:
                    published_at = datetime.utcnow()

            # Get URL (Google News uses redirect URLs)
            url = None
            if link_elem is not None and link_elem.text:
                url = link_elem.text.strip()

            # Get description/summary
            summary = None
            if description_elem is not None and description_elem.text:
                summary = self._clean_html(description_elem.text.strip())[:500]

            # Generate source ID
            source_id = None
            if guid_elem is not None and guid_elem.text:
                source_id = f"gn_{hash(guid_elem.text)}"
            else:
                source_id = f"gn_{hash(title + str(published_at))}"

            # Classify event type based on content
            event_type = self._classify_event(title, summary or "")

            # Extract company mentions
            company_name = self._extract_company(title, summary or "")

            return {
                "source": self.name,
                "source_id": source_id,
                "title": title,
                "summary": summary,
                "url": url,
                "published_at": published_at,
                "event_type": event_type,
                "filing_type": None,
                "company_name": company_name,
                "company_ticker": None,
                "investor_id": None,
                "investor_type": None,
                "relevance_score": self._calculate_relevance(
                    title, summary or "", query
                ),
            }

        except Exception as e:
            logger.error(f"Error parsing Google News item: {e}")
            return None

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags from text."""
        clean = re.sub(r"<[^>]+>", "", text)
        clean = re.sub(r"\s+", " ", clean)
        return clean.strip()

    def _classify_event(self, title: str, summary: str) -> str:
        """Classify the event type based on content."""
        text = f"{title} {summary}".lower()

        if any(
            kw in text
            for kw in [
                "raises",
                "funding",
                "series a",
                "series b",
                "series c",
                "seed round",
                "venture",
            ]
        ):
            return "funding"
        elif any(
            kw in text
            for kw in ["acquires", "acquisition", "merger", "bought", "deal closes"]
        ):
            return "acquisition"
        elif any(
            kw in text for kw in ["ipo", "goes public", "public offering", "listing"]
        ):
            return "ipo"
        elif any(
            kw in text
            for kw in ["ceo", "cfo", "appoints", "hires", "joins as", "executive"]
        ):
            return "leadership"
        elif any(
            kw in text for kw in ["layoff", "cuts", "downsizing", "restructuring"]
        ):
            return "restructuring"
        elif any(
            kw in text for kw in ["partnership", "partners with", "collaboration"]
        ):
            return "partnership"
        else:
            return "news"

    def _extract_company(self, title: str, summary: str) -> Optional[str]:
        """Extract the main company name mentioned."""
        # This is a simple heuristic - looks for capitalized words
        # A more robust solution would use NER
        text = title

        # Common patterns: "Company raises...", "Company acquires..."
        match = re.match(r"^([A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+)*)", text)
        if match:
            company = match.group(1)
            # Filter out common non-company words
            skip_words = {
                "The",
                "A",
                "An",
                "This",
                "How",
                "Why",
                "What",
                "When",
                "Where",
            }
            if company.split()[0] not in skip_words:
                return company

        return None

    def _calculate_relevance(self, title: str, summary: str, query: str) -> float:
        """Calculate relevance score based on query match."""
        text = f"{title} {summary}".lower()
        query_lower = query.lower()

        # Base score
        score = 0.5

        # Boost for query terms
        query_words = query_lower.split()
        matches = sum(1 for word in query_words if word in text)
        score += (matches / len(query_words)) * 0.3

        # Boost for investment-related keywords
        investment_keywords = [
            "funding",
            "investment",
            "acquire",
            "ipo",
            "venture",
            "private equity",
        ]
        if any(kw in text for kw in investment_keywords):
            score += 0.2

        return min(1.0, score)
