"""
Unit tests for BioExtractor PE collector.

Tests team page discovery, HTML extraction, LLM bio parsing,
JSON repair, and error handling.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from app.sources.pe_collection.people_collectors.bio_extractor import (
    BioExtractor,
    TEAM_PATTERNS,
    MAX_PEOPLE_PER_FIRM,
)
from app.sources.pe_collection.types import (
    PECollectionSource,
    EntityType,
)


@pytest.fixture
def extractor():
    """Create a BioExtractor with mocked rate limiting."""
    ext = BioExtractor()
    ext._rate_limit = AsyncMock()
    return ext


@pytest.fixture
def mock_llm_response():
    """Create a factory for mock LLM responses."""
    def _make(content, parse_json_return=None):
        resp = MagicMock()
        resp.content = content
        if parse_json_return is not None:
            resp.parse_json.return_value = parse_json_return
        else:
            resp.parse_json.return_value = json.loads(content)
        return resp
    return _make


def _make_http_response(status_code=200, text="", content=b""):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = content or text.encode()
    return resp


# ============================================================================
# Property Tests
# ============================================================================

class TestBioExtractorProperties:

    def test_source_type(self, extractor):
        assert extractor.source_type == PECollectionSource.BIO_EXTRACTOR

    def test_entity_type(self, extractor):
        assert extractor.entity_type == EntityType.FIRM


# ============================================================================
# Collect Method Tests
# ============================================================================

class TestBioExtractorCollect:

    @pytest.mark.asyncio
    async def test_no_website_url(self, extractor):
        """Returns failure when no website URL provided."""
        result = await extractor.collect(entity_id=1, entity_name="TestFirm")
        assert result.success is False
        assert "No website URL" in result.error_message

    @pytest.mark.asyncio
    async def test_url_normalization(self, extractor):
        """URL without https:// gets prepended."""
        extractor._fetch_url = AsyncMock(return_value=None)
        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="example.com"
        )
        # Should have tried patterns with https:// prepended
        calls = extractor._fetch_url.call_args_list
        assert any("https://example.com" in str(c) for c in calls)

    @pytest.mark.asyncio
    async def test_team_page_not_found(self, extractor):
        """All URL patterns 404 + homepage has no links -> empty items with warning."""
        # All fetches return 404
        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(404, "<html>Not found</html>")
        )
        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 0
        assert any("Could not find team" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_team_page_found_via_pattern(self, extractor):
        """Mock /team returns 200 with team keywords -> proceeds to extraction."""
        team_html = """
        <html><body>
        <main>
            <h1>Our Team</h1>
            <p>John Doe - Managing Director. 10 years of experience.</p>
            <p>Jane Smith - Partner. Expert in technology investments.</p>
        </main>
        </body></html>
        """
        people_json = json.dumps([
            {"full_name": "John Doe", "title": "Managing Director", "bio": "10 years experience"},
            {"full_name": "Jane Smith", "title": "Partner", "bio": "Technology expert"},
        ])

        call_count = 0
        async def mock_fetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "/team" in url and call_count <= len(TEAM_PATTERNS):
                return _make_http_response(200, team_html)
            return _make_http_response(200, team_html)

        extractor._fetch_url = AsyncMock(side_effect=mock_fetch)

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = people_json
        mock_resp.parse_json.return_value = json.loads(people_json)
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 2
        assert result.items[0].data["full_name"] == "John Doe"

    @pytest.mark.asyncio
    async def test_team_page_found_via_homepage_link(self, extractor):
        """Homepage has team link -> team URL discovered."""
        homepage_html = """
        <html><body>
        <nav><a href="/our-team">Our Team</a></nav>
        <main><p>Welcome to TestFirm</p></main>
        </body></html>
        """
        team_html = """
        <html><body>
        <main><h1>Team</h1><p>Alice Wang - VP. Finance background.</p></main>
        </body></html>
        """
        people_json = json.dumps([
            {"full_name": "Alice Wang", "title": "VP", "bio": "Finance background"},
        ])

        async def mock_fetch(url, **kwargs):
            if "/our-team" in url:
                return _make_http_response(200, team_html)
            if any(p in url for p in TEAM_PATTERNS):
                return _make_http_response(404, "Not found")
            # Homepage
            return _make_http_response(200, homepage_html)

        extractor._fetch_url = AsyncMock(side_effect=mock_fetch)

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = people_json
        mock_resp.parse_json.return_value = json.loads(people_json)
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 1
        assert result.items[0].data["full_name"] == "Alice Wang"

    @pytest.mark.asyncio
    async def test_profile_pages_fetched(self, extractor):
        """Team page has /team/person links -> fetches profiles."""
        team_html = """
        <html><body>
        <main>
            <h1>Team</h1>
            <a href="/team/john-doe">John Doe</a>
            <a href="/team/jane-smith">Jane Smith</a>
        </main>
        </body></html>
        """
        profile_html = """
        <html><body>
        <main><p>John Doe is a Managing Director with extensive experience in
        private equity investing across technology sectors.</p></main>
        </body></html>
        """
        people_json = json.dumps([
            {"full_name": "John Doe", "title": "Managing Director", "bio": "PE investor"},
        ])

        async def mock_fetch(url, **kwargs):
            if "/team/john-doe" in url or "/team/jane-smith" in url:
                return _make_http_response(200, profile_html)
            # Team page and patterns
            if any(p in url for p in ["/team"] + TEAM_PATTERNS):
                return _make_http_response(200, team_html)
            return _make_http_response(200, team_html)

        extractor._fetch_url = AsyncMock(side_effect=mock_fetch)

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = people_json
        mock_resp.parse_json.return_value = json.loads(people_json)
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        # LLM was called with enriched text including profile content
        call_args = mock_llm.complete.call_args
        assert "Individual Profiles" in call_args.kwargs.get("prompt", call_args[1].get("prompt", str(call_args)))

    @pytest.mark.asyncio
    async def test_llm_unavailable(self, extractor):
        """_get_llm_client returns None -> empty items with warning."""
        team_html = "<html><body><main><h1>Team</h1><p>People here</p></main></body></html>"

        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, team_html)
        )
        extractor._get_llm_client = MagicMock(return_value=None)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 0
        assert any("LLM not available" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_llm_returns_people(self, extractor):
        """LLM returns JSON array of 3 people -> 3 items with correct data."""
        team_html = "<html><body><main><h1>Team</h1><p>Three professionals</p></main></body></html>"
        people = [
            {"full_name": "Alice", "title": "Partner", "bio": "Bio A", "education": [], "experience": [], "focus_areas": ["Tech"]},
            {"full_name": "Bob", "title": "VP", "bio": "Bio B", "education": [], "experience": [], "focus_areas": []},
            {"full_name": "Charlie", "title": "Analyst", "bio": "Bio C", "education": [], "experience": [], "focus_areas": []},
        ]
        people_json = json.dumps(people)

        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, team_html)
        )

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = people_json
        mock_resp.parse_json.return_value = people
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=42, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 3
        assert result.items[0].data["full_name"] == "Alice"
        assert result.items[0].data["firm_id"] == 42
        assert result.items[0].data["firm_name"] == "TestFirm"
        assert result.items[0].confidence == "llm_extracted"
        assert result.items[0].item_type == "person"
        assert result.items[2].data["full_name"] == "Charlie"

    @pytest.mark.asyncio
    async def test_llm_returns_dict_with_people_key(self, extractor):
        """LLM returns {"people": [...]} -> still extracts correctly."""
        team_html = "<html><body><main><h1>Team</h1><p>People here</p></main></body></html>"
        people = [{"full_name": "Dave", "title": "MD"}]
        wrapped = {"people": people}

        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, team_html)
        )

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = json.dumps(wrapped)
        mock_resp.parse_json.return_value = wrapped
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 1
        assert result.items[0].data["full_name"] == "Dave"

    @pytest.mark.asyncio
    async def test_llm_parse_json_none_triggers_repair(self, extractor):
        """parse_json() returns None -> falls back to _repair_json_array()."""
        team_html = "<html><body><main><h1>Team</h1><p>People</p></main></body></html>"
        raw_content = '[{"full_name": "Eve", "title": "Partner"}]'

        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, team_html)
        )

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = raw_content
        mock_resp.parse_json.return_value = None  # Triggers repair path
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == 1
        assert result.items[0].data["full_name"] == "Eve"

    @pytest.mark.asyncio
    async def test_max_people_limit(self, extractor):
        """60 people in LLM output -> capped at MAX_PEOPLE_PER_FIRM."""
        team_html = "<html><body><main><h1>Team</h1><p>Large team</p></main></body></html>"
        people = [{"full_name": f"Person {i}", "title": "Analyst"} for i in range(60)]

        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, team_html)
        )

        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = json.dumps(people)
        mock_resp.parse_json.return_value = people
        mock_llm.complete = AsyncMock(return_value=mock_resp)
        extractor._get_llm_client = MagicMock(return_value=mock_llm)

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is True
        assert len(result.items) == MAX_PEOPLE_PER_FIRM

    @pytest.mark.asyncio
    async def test_exception_during_collect(self, extractor):
        """Exception in fetch -> success=False with error message."""
        extractor._fetch_url = AsyncMock(side_effect=RuntimeError("Connection failed"))

        result = await extractor.collect(
            entity_id=1, entity_name="TestFirm", website_url="https://example.com"
        )
        assert result.success is False
        assert "Connection failed" in result.error_message


# ============================================================================
# Page Text Extraction Tests
# ============================================================================

class TestPageTextExtraction:

    @pytest.mark.asyncio
    async def test_page_text_extraction(self, extractor):
        """HTML -> clean text, noise tags removed."""
        html = """
        <html>
        <head><title>Team</title></head>
        <body>
        <nav>Navigation bar</nav>
        <script>var x = 1;</script>
        <style>.hidden { display: none; }</style>
        <main>
            <h1>Our Team</h1>
            <p>John Doe is a Managing Director.</p>
        </main>
        <footer>Copyright 2024</footer>
        </body>
        </html>
        """
        extractor._fetch_url = AsyncMock(
            return_value=_make_http_response(200, html)
        )
        # Mock Playwright fallback so it doesn't hit real URLs when
        # _has_people_content() fails (test HTML has < 3 names)
        extractor._fetch_with_playwright = AsyncMock(return_value=None)
        text = await extractor._fetch_page_text("https://example.com/team")
        assert text is not None
        assert "John Doe" in text
        assert "Managing Director" in text
        # Noise should be removed
        assert "Navigation bar" not in text
        assert "var x = 1" not in text
        assert "Copyright" not in text


# ============================================================================
# JSON Repair Tests
# ============================================================================

class TestRepairJsonArray:

    def test_repair_json_truncated(self):
        """Truncated JSON array -> repairs to last complete object."""
        raw = '[{"full_name": "Alice", "title": "MD"}, {"full_name": "Bob", "tit'
        result = BioExtractor._repair_json_array(raw)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["full_name"] == "Alice"

    def test_repair_json_trailing_commas(self):
        """Trailing commas removed before parse."""
        raw = '[{"full_name": "Alice", "title": "MD",}, {"full_name": "Bob", "title": "VP",},]'
        result = BioExtractor._repair_json_array(raw)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_repair_json_no_array(self):
        """No array bracket found -> empty list."""
        raw = "This is not JSON at all"
        result = BioExtractor._repair_json_array(raw)
        assert result == []

    def test_repair_json_valid(self):
        """Valid JSON array passes through."""
        raw = '[{"full_name": "Alice"}, {"full_name": "Bob"}]'
        result = BioExtractor._repair_json_array(raw)
        assert len(result) == 2
