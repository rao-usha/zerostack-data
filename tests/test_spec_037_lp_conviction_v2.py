"""
SPEC_037 — LP Conviction 2.0 skeleton tests.

Tests that can run offline (no Docker, no API keys).
"""

import pytest


# ---------------------------------------------------------------------------
# lp_tier_classifier tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_classify_lp_tier_sovereign():
    from app.sources.lp_collection.lp_tier_classifier import classify_lp_tier

    lp = type("LP", (), {
        "lp_type": "sovereign_wealth_fund",
        "aum_usd": 500_000_000_000,
        "name": "GIC",
    })()
    assert classify_lp_tier(lp) == 1


@pytest.mark.unit
def test_classify_lp_tier_endowment():
    from app.sources.lp_collection.lp_tier_classifier import classify_lp_tier

    lp = type("LP", (), {
        "lp_type": "endowment",
        "aum_usd": 40_000_000_000,
        "name": "Harvard",
    })()
    assert classify_lp_tier(lp) == 1


@pytest.mark.unit
def test_classify_lp_tier_large_public_pension():
    from app.sources.lp_collection.lp_tier_classifier import classify_lp_tier

    lp = type("LP", (), {
        "lp_type": "public_pension",
        "aum_usd": 60_000_000_000,
        "name": "CalPERS",
    })()
    assert classify_lp_tier(lp) == 2


@pytest.mark.unit
def test_classify_lp_tier_family_office():
    from app.sources.lp_collection.lp_tier_classifier import classify_lp_tier

    lp = type("LP", (), {
        "lp_type": "family_office",
        "aum_usd": 2_000_000_000,
        "name": "Some Family Office",
    })()
    assert classify_lp_tier(lp) == 5


# ---------------------------------------------------------------------------
# form_990_html_parser tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_parse_form_990_schedule_d_with_pe_table():
    from app.sources.lp_collection.form_990_html_parser import parse_form_990_schedule_d

    html = """
    <html><body>
    <h2>Schedule D</h2>
    <table>
      <tr><th>Name</th><th>Book Value</th></tr>
      <tr><td>KKR Americas Fund XII LP</td><td>50000000</td></tr>
      <tr><td>Blackstone Capital Partners VIII</td><td>75000000</td></tr>
    </table>
    </body></html>
    """
    records = parse_form_990_schedule_d(html, "Test Foundation")
    assert isinstance(records, list)
    assert len(records) >= 1


@pytest.mark.unit
def test_parse_form_990_schedule_d_empty():
    from app.sources.lp_collection.form_990_html_parser import parse_form_990_schedule_d

    records = parse_form_990_schedule_d("<html><body><p>No data</p></body></html>", "Test")
    assert records == []


# ---------------------------------------------------------------------------
# pension_cafr_collector tests (offline / mock HTTP)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_pension_cafr_collector_instantiates():
    from app.sources.lp_collection.pension_cafr_collector import PensionCafrCollector

    collector = PensionCafrCollector()
    assert collector is not None


@pytest.mark.unit
def test_extract_text_from_pdf_empty_bytes():
    """Empty bytes should return empty string without crashing."""
    from app.sources.lp_collection.pension_cafr_collector import extract_text_from_pdf

    result = extract_text_from_pdf(b"", label="test")
    assert isinstance(result, str)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_collect_all_all_404(monkeypatch):
    """When all URLs return 404, collect_all() returns []."""
    from app.sources.lp_collection.pension_cafr_collector import PensionCafrCollector

    async def mock_is_pdf_url(self, url: str) -> bool:
        return False

    async def mock_discover_cafr_url_via_llm(self, pension: dict):
        return None

    monkeypatch.setattr(PensionCafrCollector, "_is_pdf_url", mock_is_pdf_url)
    monkeypatch.setattr(
        PensionCafrCollector, "_discover_cafr_url_via_llm", mock_discover_cafr_url_via_llm
    )

    collector = PensionCafrCollector()
    records = await collector.collect_all()
    assert records == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_collect_all_mock_pdf_success(monkeypatch):
    """When HEAD succeeds and LLM extracts records, collect_all() returns them."""
    from app.sources.lp_collection.pension_cafr_collector import (
        PensionCafrCollector,
        PENSION_CAFR_TARGETS,
    )
    import app.sources.lp_collection.pension_cafr_collector as mod

    # Only test first pension to keep it fast
    monkeypatch.setattr(mod, "PENSION_CAFR_TARGETS", [PENSION_CAFR_TARGETS[0]])
    monkeypatch.setattr(mod, "DOWNLOAD_DELAY", 0)

    async def mock_is_pdf_url(self, url: str) -> bool:
        return True

    async def mock_download_pdf(self, url: str):
        return b"%PDF-1.4 fake"

    def mock_extract_text(pdf_bytes, label=""):
        return "private equity portfolio KKR Americas Fund XII commitment 50000000"

    async def mock_extract_pe(text, lp_name):
        return [{"manager_name": "KKR", "fund_name": "KKR Americas Fund XII", "commitment_amount_usd": 50_000_000}]

    monkeypatch.setattr(PensionCafrCollector, "_is_pdf_url", mock_is_pdf_url)
    monkeypatch.setattr(PensionCafrCollector, "_download_pdf", mock_download_pdf)
    monkeypatch.setattr(mod, "extract_text_from_pdf", mock_extract_text)
    monkeypatch.setattr(mod, "_extract_pe_from_text", mock_extract_pe)

    collector = PensionCafrCollector()
    records = await collector.collect_all()
    assert len(records) == 1
    assert records[0]["gp_name"] == "KKR"
    assert records[0]["data_source"] == "cafr"


# ---------------------------------------------------------------------------
# form_990_pe_extractor — ProPublica integration
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_search_990_filing_no_filings(monkeypatch):
    """Returns None when ProPublica returns no filings_with_data."""
    import httpx
    from app.sources.lp_collection.form_990_pe_extractor import Form990PEExtractor

    class MockResponse:
        status_code = 200
        def json(self): return {"filings_with_data": []}

    async def mock_get(self, url, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    extractor = Form990PEExtractor()
    result = await extractor.search_990_filing({"ein": "13-1684331", "short": "Ford"})
    assert result is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_search_990_filing_returns_pdf_url(monkeypatch):
    """Returns pdf_url from most recent filing."""
    import httpx
    from app.sources.lp_collection.form_990_pe_extractor import Form990PEExtractor

    expected_url = "https://pp-990-documents.s3.amazonaws.com/fake.pdf"

    class MockResponse:
        status_code = 200
        def json(self):
            return {"filings_with_data": [{"pdf_url": expected_url, "tax_prd_yr": 2023}]}

    async def mock_get(self, url, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    extractor = Form990PEExtractor()
    result = await extractor.search_990_filing({"ein": "13-1684331", "short": "Ford"})
    assert result == expected_url
