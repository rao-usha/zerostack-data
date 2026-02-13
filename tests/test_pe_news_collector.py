"""
Unit tests for PENewsCollector.

Tests RSS parsing, deduplication, LLM classification,
and error handling for PE news collection.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.sources.pe_collection.news_collectors.news_collector import (
    PENewsCollector,
    MAX_ARTICLES_TO_FETCH,
    MAX_ARTICLES_TO_CLASSIFY,
)
from app.sources.pe_collection.types import (
    PECollectionSource,
    EntityType,
)


@pytest.fixture
def collector():
    """Create a PENewsCollector with mocked rate limiting."""
    c = PENewsCollector()
    c._rate_limit = AsyncMock()
    return c


def _make_http_response(status_code=200, text="", content=b""):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = content or text.encode()
    return resp


VALID_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>Test Feed</title>
  <item>
    <title>Blackstone acquires TechCo</title>
    <link>https://example.com/article1</link>
    <description>Blackstone announced the acquisition of TechCo for $2B.</description>
    <pubDate>Mon, 10 Feb 2025 12:00:00 GMT</pubDate>
  </item>
  <item>
    <title>KKR raises new fund</title>
    <link>https://example.com/article2</link>
    <description>&lt;b&gt;KKR&lt;/b&gt; raised a $5B fund for technology investments.</description>
    <pubDate>Tue, 11 Feb 2025 10:00:00 GMT</pubDate>
  </item>
</channel>
</rss>"""


# ============================================================================
# Property Tests
# ============================================================================

class TestPENewsCollectorProperties:

    def test_source_type(self, collector):
        assert collector.source_type == PECollectionSource.NEWS_API

    def test_entity_type(self, collector):
        assert collector.entity_type == EntityType.FIRM


# ============================================================================
# RSS Parsing Tests
# ============================================================================

class TestParseRSS:

    def test_parse_rss_valid(self, collector):
        """Valid RSS XML -> list of article dicts with expected fields."""
        articles = collector._parse_rss(VALID_RSS, "test_source")
        assert len(articles) == 2
        assert articles[0]["title"] == "Blackstone acquires TechCo"
        assert articles[0]["url"] == "https://example.com/article1"
        assert articles[0]["description"] is not None
        assert articles[0]["published_date"] is not None
        assert articles[0]["source"] == "test_source"

    def test_parse_rss_invalid_xml(self, collector):
        """Malformed XML -> empty list, no crash."""
        articles = collector._parse_rss("<<<not valid xml>>>", "test_source")
        assert articles == []

    def test_parse_rss_missing_fields(self, collector):
        """Items without title or link are skipped."""
        xml = """<?xml version="1.0"?>
        <rss version="2.0"><channel>
          <item><title>Has title but no link</title></item>
          <item><link>https://example.com/no-title</link></item>
          <item>
            <title>Valid Article</title>
            <link>https://example.com/valid</link>
          </item>
        </channel></rss>"""
        articles = collector._parse_rss(xml, "test_source")
        assert len(articles) == 1
        assert articles[0]["title"] == "Valid Article"

    def test_parse_rss_html_description(self, collector):
        """HTML tags stripped from description."""
        articles = collector._parse_rss(VALID_RSS, "test_source")
        desc = articles[1]["description"]
        # The <b> tags should be stripped
        assert "<b>" not in desc
        assert "KKR" in desc

    def test_parse_rss_max_articles(self, collector):
        """More than MAX_ARTICLES_TO_FETCH items -> capped."""
        items_xml = ""
        for i in range(20):
            items_xml += f"""
            <item>
              <title>Article {i}</title>
              <link>https://example.com/article{i}</link>
              <description>Description {i}</description>
            </item>"""
        xml = f'<?xml version="1.0"?><rss version="2.0"><channel>{items_xml}</channel></rss>'
        articles = collector._parse_rss(xml, "test_source")
        assert len(articles) == MAX_ARTICLES_TO_FETCH


# ============================================================================
# Deduplication Tests
# ============================================================================

class TestDeduplication:

    def test_deduplicate_by_url(self, collector):
        """Same URL from different sources -> kept once."""
        articles = [
            {"title": "Article A", "url": "https://example.com/same", "source": "bing"},
            {"title": "Article A (copy)", "url": "https://example.com/same", "source": "google"},
            {"title": "Different", "url": "https://example.com/different", "source": "bing"},
        ]
        result = collector._deduplicate_articles(articles)
        assert len(result) == 2
        urls = [a["url"] for a in result]
        assert "https://example.com/same" in urls
        assert "https://example.com/different" in urls

    def test_deduplicate_by_title_prefix(self, collector):
        """Titles matching first 60 chars -> kept once."""
        long_prefix = "A" * 60
        articles = [
            {"title": f"{long_prefix} version 1", "url": "https://example.com/1"},
            {"title": f"{long_prefix} version 2", "url": "https://example.com/2"},
            {"title": "Completely different title", "url": "https://example.com/3"},
        ]
        result = collector._deduplicate_articles(articles)
        assert len(result) == 2


# ============================================================================
# Collect Method Tests
# ============================================================================

class TestPENewsCollectorCollect:

    @pytest.mark.asyncio
    async def test_no_articles_found(self, collector):
        """All RSS feeds return no items -> empty result with warning."""
        collector._fetch_url = AsyncMock(
            return_value=_make_http_response(200, '<?xml version="1.0"?><rss><channel></channel></rss>')
        )
        collector._get_llm_client = MagicMock(return_value=None)

        result = await collector.collect(entity_id=1, entity_name="TestFirm")
        assert result.success is True
        assert len(result.items) == 0
        assert any("No news articles" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_yahoo_finance_with_ticker(self, collector):
        """Ticker provided -> Yahoo RSS also searched."""
        call_urls = []

        async def mock_fetch(url, **kwargs):
            call_urls.append(url)
            return _make_http_response(200, '<?xml version="1.0"?><rss><channel></channel></rss>')

        collector._fetch_url = AsyncMock(side_effect=mock_fetch)
        collector._get_llm_client = MagicMock(return_value=None)

        await collector.collect(entity_id=1, entity_name="TestFirm", ticker="TF")
        yahoo_calls = [u for u in call_urls if "yahoo" in u.lower()]
        assert len(yahoo_calls) > 0

    @pytest.mark.asyncio
    async def test_yahoo_finance_without_ticker(self, collector):
        """No ticker -> Yahoo RSS skipped."""
        call_urls = []

        async def mock_fetch(url, **kwargs):
            call_urls.append(url)
            return _make_http_response(200, '<?xml version="1.0"?><rss><channel></channel></rss>')

        collector._fetch_url = AsyncMock(side_effect=mock_fetch)
        collector._get_llm_client = MagicMock(return_value=None)

        await collector.collect(entity_id=1, entity_name="TestFirm")
        yahoo_calls = [u for u in call_urls if "yahoo" in u.lower()]
        assert len(yahoo_calls) == 0

    @pytest.mark.asyncio
    async def test_llm_classification(self, collector):
        """LLM classifies -> items get news_type, sentiment, relevance_score, summary."""
        collector._fetch_url = AsyncMock(
            return_value=_make_http_response(200, VALID_RSS)
        )

        classifications = [
            {"index": 0, "news_type": "Deal", "sentiment": "Positive", "relevance_score": 0.9, "summary": "Acquisition deal"},
            {"index": 1, "news_type": "Fundraise", "sentiment": "Positive", "relevance_score": 0.8, "summary": "New fund"},
        ]

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = classifications
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        collector._get_llm_client = MagicMock(return_value=mock_llm)

        result = await collector.collect(entity_id=1, entity_name="Blackstone")
        assert result.success is True
        assert len(result.items) == 2
        assert result.items[0].data["news_type"] == "Deal"
        assert result.items[0].data["sentiment"] == "Positive"
        assert result.items[0].data["relevance_score"] == 0.9
        assert result.items[0].data["summary"] == "Acquisition deal"
        assert result.items[0].confidence == "llm_extracted"

    @pytest.mark.asyncio
    async def test_low_relevance_filtered(self, collector):
        """Article with relevance_score < 0.3 -> excluded from items."""
        collector._fetch_url = AsyncMock(
            return_value=_make_http_response(200, VALID_RSS)
        )

        classifications = [
            {"index": 0, "news_type": "Deal", "sentiment": "Positive", "relevance_score": 0.9, "summary": "Good"},
            {"index": 1, "news_type": "Other", "sentiment": "Neutral", "relevance_score": 0.1, "summary": "Irrelevant"},
        ]

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.parse_json.return_value = classifications
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        collector._get_llm_client = MagicMock(return_value=mock_llm)

        result = await collector.collect(entity_id=1, entity_name="Blackstone")
        assert result.success is True
        assert len(result.items) == 1
        assert result.items[0].data["news_type"] == "Deal"

    @pytest.mark.asyncio
    async def test_llm_unavailable(self, collector):
        """No LLM -> items still created with confidence='low'."""
        collector._fetch_url = AsyncMock(
            return_value=_make_http_response(200, VALID_RSS)
        )
        collector._get_llm_client = MagicMock(return_value=None)

        result = await collector.collect(entity_id=1, entity_name="Blackstone")
        assert result.success is True
        assert len(result.items) == 2
        assert all(item.confidence == "low" for item in result.items)
        assert any("LLM not available" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_exception_during_collect(self, collector):
        """Exception in outer collect -> success=False with error message."""
        # _search_all_feeds catches per-feed exceptions internally, so we
        # must raise from the outer flow to trigger the error path.
        collector._search_all_feeds = AsyncMock(side_effect=RuntimeError("Network error"))

        result = await collector.collect(entity_id=1, entity_name="TestFirm")
        assert result.success is False
        assert "Network error" in result.error_message
