"""
PE News Collector.

Searches Bing News RSS and Yahoo Finance RSS for PE firm and portfolio
company news. Uses GPT-4o-mini to classify news type and sentiment.
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import quote_plus

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)
from app.sources.pe_collection.config import settings

logger = logging.getLogger(__name__)

# RSS feed URLs (free, no API key required)
BING_NEWS_RSS = "https://www.bing.com/news/search?q={query}&format=rss"
YAHOO_FINANCE_RSS = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Maximum articles to process per entity
MAX_ARTICLES_TO_FETCH = 15
MAX_ARTICLES_TO_CLASSIFY = 10

# LLM classification prompt
NEWS_CLASSIFICATION_PROMPT = """Classify these news articles about {entity_name} (a PE/VC firm or portfolio company).

For each article, return ONLY valid JSON — an array of objects:
[
  {{
    "index": 0,
    "news_type": "Fundraise|Deal|Hire|Strategy|Earnings|Exit|IPO|Restructuring|Other",
    "sentiment": "Positive|Negative|Neutral",
    "relevance_score": 0.85,
    "summary": "1-2 sentence summary of the article"
  }}
]

Rules:
- relevance_score: 0.0 to 1.0, how relevant the article is to {entity_name}'s PE/VC activities
- Skip articles with relevance_score < 0.3 (set to 0.0)
- news_type should reflect the primary topic
- sentiment should reflect the tone toward {entity_name}

Articles:
{articles}"""


class PENewsCollector(BasePECollector):
    """
    Collects news articles about PE firms and portfolio companies.

    Searches free RSS feeds (Bing News, Google News, Yahoo Finance)
    and uses LLM to classify news type, sentiment, and relevance.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.NEWS_API

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", settings.news_rate_limit_delay
        )
        self._llm_client = None

    def _get_llm_client(self):
        """Lazily initialize LLM client."""
        if self._llm_client is None:
            from app.agentic.llm_client import get_llm_client
            self._llm_client = get_llm_client(model="gpt-4o-mini")
        return self._llm_client

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        ticker: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect news articles for a PE firm or portfolio company.

        Args:
            entity_id: Entity ID
            entity_name: Entity name (firm or company)
            website_url: Not used
            ticker: Stock ticker for Yahoo Finance RSS (optional)
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        try:
            # Search multiple RSS sources
            articles = await self._search_all_feeds(entity_name, ticker)
            logger.info(
                f"Found {len(articles)} candidate news articles for {entity_name}"
            )

            if not articles:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=["No news articles found"],
                    started_at=started_at,
                )

            # Deduplicate by title similarity
            articles = self._deduplicate_articles(articles)

            # Classify with LLM
            llm_client = self._get_llm_client()
            if llm_client:
                classified = await self._classify_articles(
                    llm_client, articles[:MAX_ARTICLES_TO_CLASSIFY], entity_name
                )
            else:
                warnings.append("LLM not available — returning unclassified news metadata")
                classified = None

            # Build items
            for i, article in enumerate(articles[:MAX_ARTICLES_TO_CLASSIFY]):
                classification = None
                if classified and i < len(classified):
                    classification = classified[i]

                # Skip low-relevance articles if classified
                if classification and classification.get("relevance_score", 1.0) < 0.3:
                    continue

                item_data = {
                    "entity_id": entity_id,
                    "entity_name": entity_name,
                    "title": article.get("title"),
                    "url": article.get("url"),
                    "published_date": article.get("published_date"),
                    "source_name": article.get("source"),
                    "description": article.get("description"),
                }

                if classification:
                    item_data.update({
                        "news_type": classification.get("news_type", "Other"),
                        "sentiment": classification.get("sentiment", "Neutral"),
                        "relevance_score": classification.get("relevance_score"),
                        "summary": classification.get("summary"),
                    })

                confidence = "llm_extracted" if classification else "low"
                items.append(
                    self._create_item(
                        item_type="firm_news",
                        data=item_data,
                        source_url=article.get("url"),
                        confidence=confidence,
                    )
                )

            logger.info(
                f"Collected {len(items)} news items for {entity_name}"
            )

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting news for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _search_all_feeds(
        self, entity_name: str, ticker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search all RSS feeds and combine results."""
        all_articles = []

        # Bing News RSS
        try:
            query = f'"{entity_name}" private equity OR acquisition OR investment OR portfolio'
            bing_articles = await self._fetch_rss(
                BING_NEWS_RSS.format(query=quote_plus(query)),
                source_name="bing_news",
            )
            all_articles.extend(bing_articles)
        except Exception as e:
            logger.warning(f"Bing News RSS failed: {e}")

        # Google News RSS
        try:
            query = f'"{entity_name}" private equity'
            google_articles = await self._fetch_rss(
                GOOGLE_NEWS_RSS.format(query=quote_plus(query)),
                source_name="google_news",
            )
            all_articles.extend(google_articles)
        except Exception as e:
            logger.warning(f"Google News RSS failed: {e}")

        # Yahoo Finance RSS (if ticker available)
        if ticker:
            try:
                yahoo_articles = await self._fetch_rss(
                    YAHOO_FINANCE_RSS.format(ticker=ticker),
                    source_name="yahoo_finance",
                )
                all_articles.extend(yahoo_articles)
            except Exception as e:
                logger.warning(f"Yahoo Finance RSS failed: {e}")

        return all_articles

    async def _fetch_rss(
        self, url: str, source_name: str
    ) -> List[Dict[str, Any]]:
        """Fetch and parse an RSS feed."""
        response = await self._fetch_url(
            url,
            headers={
                "Accept": "application/rss+xml, application/xml, text/xml",
            },
        )
        if not response or response.status_code != 200:
            return []

        return self._parse_rss(response.text, source_name)

    def _parse_rss(self, xml_content: str, source_name: str) -> List[Dict[str, Any]]:
        """Parse RSS XML and extract article metadata."""
        articles = []

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            logger.warning(f"Failed to parse RSS XML from {source_name}")
            return []

        # Standard RSS 2.0 items
        for item in root.iter("item"):
            title_elem = item.find("title")
            link_elem = item.find("link")
            desc_elem = item.find("description")
            pub_date_elem = item.find("pubDate")

            title = title_elem.text.strip() if title_elem is not None and title_elem.text else None
            link = link_elem.text.strip() if link_elem is not None and link_elem.text else None

            if not title or not link:
                continue

            description = None
            if desc_elem is not None and desc_elem.text:
                # Strip HTML tags from description
                description = re.sub(r"<[^>]+>", "", desc_elem.text).strip()
                if len(description) > 500:
                    description = description[:500]

            pub_date = None
            if pub_date_elem is not None and pub_date_elem.text:
                pub_date = pub_date_elem.text.strip()

            articles.append({
                "title": title,
                "url": link,
                "description": description,
                "published_date": pub_date,
                "source": source_name,
            })

            if len(articles) >= MAX_ARTICLES_TO_FETCH:
                break

        return articles

    def _deduplicate_articles(
        self, articles: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Remove duplicate articles by URL and similar titles."""
        seen_urls = set()
        seen_titles = set()
        unique = []

        for article in articles:
            url = article.get("url", "")
            title = (article.get("title") or "").lower().strip()

            # Skip exact URL duplicates
            if url in seen_urls:
                continue

            # Skip very similar titles (first 60 chars match)
            title_key = title[:60]
            if title_key in seen_titles:
                continue

            seen_urls.add(url)
            seen_titles.add(title_key)
            unique.append(article)

        return unique

    async def _classify_articles(
        self,
        llm_client,
        articles: List[Dict[str, Any]],
        entity_name: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """Use LLM to classify articles by news type, sentiment, and relevance."""
        # Build article summaries for LLM
        article_texts = []
        for i, article in enumerate(articles):
            title = article.get("title", "No title")
            desc = article.get("description", "No description")
            article_texts.append(f"[{i}] {title}\n    {desc}")

        articles_str = "\n\n".join(article_texts)
        prompt = NEWS_CLASSIFICATION_PROMPT.format(
            entity_name=entity_name, articles=articles_str
        )

        try:
            response = await llm_client.complete(
                prompt=prompt,
                system_prompt=(
                    "You are a financial news analyst classifying PE/VC news articles. "
                    "Return only valid JSON."
                ),
                json_mode=True,
            )
            result = response.parse_json()

            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("articles", result.get("classifications", []))
            return None

        except Exception as e:
            logger.warning(f"LLM news classification failed: {e}")
            return None
