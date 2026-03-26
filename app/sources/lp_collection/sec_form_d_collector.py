"""
SEC Form D Collector — collects PE fund capital raise data from SEC EDGAR.

Form D is filed by PE funds within 15 days of first sale of securities.
Provides: fund name, total offering amount, amount sold, date of first sale,
investor count — key inputs for oversubscription ratio and time-to-close.
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&dateRange=custom&startdt={start}&enddt={end}&forms=D"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
    "Accept": "application/json",
}


class SECFormDCollector:
    """Collects PE fund Form D filings from SEC EDGAR."""

    def __init__(self):
        self._semaphore = asyncio.Semaphore(2)  # max 2 concurrent requests to SEC

    async def _get(self, url: str) -> Optional[dict]:
        async with self._semaphore:
            await asyncio.sleep(0.15)  # SEC: max ~10 req/sec
            try:
                async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        return resp.json()
                    return None
            except Exception as e:
                logger.error(f"SEC EDGAR request failed: {e}")
                return None

    async def search_form_d_by_gp(self, gp_name: str, start_year: int = 2010) -> list:
        """
        Search EDGAR for Form D filings by a PE firm name.
        Returns list of fund filing records.
        """
        # Use EDGAR full-text search
        url = (
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q=%22{gp_name.replace(' ', '%20')}%22"
            f"&dateRange=custom&startdt={start_year}-01-01"
            f"&forms=D&hits.hits._source=period_of_report,display_date_filed,entity_name,file_num"
            f"&hits.hits.total.value=1&hits.hits.highlight=*"
        )
        # Fall back to simpler EDGAR company search
        company_search_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar"
            f"?company={gp_name.replace(' ', '+')}&CIK=&type=D&dateb=&owner=include"
            f"&count=40&search_text=&action=getcompany&output=atom"
        )

        results = []
        try:
            async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                resp = await client.get(company_search_url)
                if resp.status_code != 200:
                    return []

                # Parse ATOM feed for Form D filings
                text = resp.text
                entries = re.findall(r'<entry>(.*?)</entry>', text, re.DOTALL)

                for entry in entries[:20]:  # cap at 20 filings
                    def extract(tag, s):
                        m = re.search(rf'<{tag}[^>]*>(.*?)</{tag}>', s, re.DOTALL)
                        return m.group(1).strip() if m else None

                    record = {
                        "entity_name": extract("company-name", entry),
                        "file_number": extract("file-number", entry),
                        "date_filed": extract("date-filed", entry),
                        "filing_href": extract("filing-href", entry),
                        "gp_name_searched": gp_name,
                        "data_source": "form_d",
                    }
                    if record["entity_name"]:
                        results.append(record)
        except Exception as e:
            logger.error(f"Form D search error for {gp_name}: {e}")

        return results

    async def get_fund_details(self, filing_href: str) -> Optional[dict]:
        """
        Fetch Form D filing details to extract offering amount, investor count, etc.
        """
        if not filing_href:
            return None
        try:
            async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                # Get the filing index
                resp = await client.get(filing_href)
                if resp.status_code != 200:
                    return None

                text = resp.text

                # Parse key Form D fields from XML/HTML
                def find(pattern, s, default=None):
                    m = re.search(pattern, s, re.DOTALL | re.IGNORECASE)
                    return m.group(1).strip() if m else default

                total_offering = find(r'totalOfferingAmount[^>]*>([^<]+)', text)
                amount_sold = find(r'totalAmountSold[^>]*>([^<]+)', text)
                date_first_sale = find(r'dateOfFirstSale[^>]*>([^<]+)', text)
                investor_count = find(r'totalNumberAlreadySold[^>]*>([^<]+)', text)

                # Parse amounts
                def parse_amount(s):
                    if not s:
                        return None
                    clean = re.sub(r'[,$\s]', '', s)
                    try:
                        return float(clean)
                    except Exception:
                        return None

                total = parse_amount(total_offering)
                sold = parse_amount(amount_sold)

                return {
                    "total_offering_usd": total,
                    "amount_sold_usd": sold,
                    "oversubscription_ratio": (sold / total) if total and sold and total > 0 else None,
                    "date_of_first_sale": date_first_sale,
                    "investor_count": int(investor_count) if investor_count and investor_count.isdigit() else None,
                }
        except Exception as e:
            logger.error(f"Error fetching Form D details: {e}")
            return None

    async def collect_for_gp(self, gp_name: str) -> list:
        """Collect all Form D fund records for a PE firm."""
        filings = await self.search_form_d_by_gp(gp_name)

        enriched = []
        for filing in filings:
            if filing.get("filing_href"):
                details = await self.get_fund_details(filing["filing_href"])
                if details:
                    filing.update(details)
            enriched.append(filing)

        return enriched
