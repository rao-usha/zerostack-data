"""
SPEC_038 — LP Public Data: Seed + HTML Portal Scraper
Tests for Path B (public seed) and Path C (HTML portal scraper).
"""
import pytest


# ---------------------------------------------------------------------------
# Path B: Public Seed Data
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_seed_records_schema():
    """Every seed record must have lp_name, gp_name, fund_vintage, data_source."""
    from app.sources.lp_collection.lp_public_seed import get_seed_records
    records = get_seed_records()
    assert len(records) > 0
    for rec in records:
        assert "lp_name" in rec and rec["lp_name"], f"Missing lp_name: {rec}"
        assert "gp_name" in rec and rec["gp_name"], f"Missing gp_name: {rec}"
        assert "fund_vintage" in rec and rec["fund_vintage"], f"Missing fund_vintage: {rec}"
        assert rec.get("data_source") == "public_seed", f"Wrong data_source: {rec}"


@pytest.mark.unit
def test_seed_records_count():
    """Must have at least 200 records across all LPs."""
    from app.sources.lp_collection.lp_public_seed import get_seed_records
    records = get_seed_records()
    assert len(records) >= 200, f"Only {len(records)} seed records, expected >= 200"


@pytest.mark.unit
def test_seed_records_lp_coverage():
    """Must cover at least 5 distinct LPs."""
    from app.sources.lp_collection.lp_public_seed import get_seed_records
    records = get_seed_records()
    lps = {r["lp_name"] for r in records}
    assert len(lps) >= 5, f"Only {len(lps)} LPs covered: {lps}"


@pytest.mark.unit
def test_seed_records_gp_coverage():
    """Must cover at least 20 distinct GPs."""
    from app.sources.lp_collection.lp_public_seed import get_seed_records
    records = get_seed_records()
    gps = {r["gp_name"] for r in records}
    assert len(gps) >= 20, f"Only {len(gps)} GPs covered"


@pytest.mark.unit
def test_seed_records_commitment_amounts():
    """At least 80% of records should have a commitment_amount_usd."""
    from app.sources.lp_collection.lp_public_seed import get_seed_records
    records = get_seed_records()
    with_amount = sum(1 for r in records if r.get("commitment_amount_usd"))
    pct = with_amount / len(records)
    assert pct >= 0.8, f"Only {pct:.0%} of records have commitment_amount_usd"


# ---------------------------------------------------------------------------
# Path C: HTML Portal Scraper
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_html_scraper_graceful_404():
    """Scraper must return [] and not raise on HTTP errors."""
    import asyncio
    from unittest.mock import AsyncMock, patch
    from app.sources.lp_collection.pension_html_scraper import PensionHtmlScraper

    scraper = PensionHtmlScraper()

    async def run():
        with patch.object(scraper, "_fetch_page", new=AsyncMock(return_value=None)):
            return await scraper.collect_all()

    result = asyncio.get_event_loop().run_until_complete(run())
    assert result == []


@pytest.mark.unit
def test_html_scraper_table_parse():
    """Given HTML with a PE fund table, scraper should extract records."""
    from app.sources.lp_collection.pension_html_scraper import PensionHtmlScraper

    fixture_html = """
    <html><body>
    <h2>Private Equity Portfolio</h2>
    <table>
      <tr><th>Manager</th><th>Fund Name</th><th>Vintage</th><th>Commitment ($M)</th></tr>
      <tr><td>KKR</td><td>KKR Americas Fund XII</td><td>2019</td><td>500</td></tr>
      <tr><td>Blackstone</td><td>Blackstone Capital Partners VIII</td><td>2018</td><td>750</td></tr>
      <tr><td>Apollo</td><td>Apollo Investment Fund IX</td><td>2017</td><td>400</td></tr>
    </table>
    </body></html>
    """

    scraper = PensionHtmlScraper()
    records = scraper._parse_html_for_commitments(fixture_html, lp_name="TestLP", source_url="http://test.example")
    assert len(records) >= 2, f"Expected >= 2 records, got {len(records)}"
    gp_names = {r["gp_name"] for r in records}
    assert "KKR" in gp_names or any("KKR" in g for g in gp_names)


@pytest.mark.unit
def test_html_scraper_no_tables():
    """Scraper returns [] gracefully when page has no PE tables."""
    from app.sources.lp_collection.pension_html_scraper import PensionHtmlScraper

    html = "<html><body><p>No tables here.</p></body></html>"
    scraper = PensionHtmlScraper()
    result = scraper._parse_html_for_commitments(html, lp_name="TestLP", source_url="http://test.example")
    assert result == []
