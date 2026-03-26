"""
Public Pension IR Scraper — collects PE fund commitment data from public pension websites.

These pensions publish their PE portfolio publicly as required by state law.
Rate limit: 1 request per 3 seconds per domain.
"""
import asyncio
import logging
import re
from typing import Optional
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

# Known public pension PE commitment pages
# Each entry: (lp_name, url, parser_hint)
PENSION_PE_PAGES = [
    {
        "lp_name": "California Public Employees' Retirement System",
        "short_name": "CalPERS",
        "url": "https://www.calpers.ca.gov/page/investments/alternative-investments/private-equity",
        "parser_hint": "html_table",
    },
    {
        "lp_name": "California State Teachers' Retirement System",
        "short_name": "CalSTRS",
        "url": "https://www.calstrs.com/private-equity-holdings",
        "parser_hint": "html_table",
    },
    {
        "lp_name": "New York State Common Retirement Fund",
        "short_name": "NY Common",
        "url": "https://www.osc.ny.gov/pension/investments/private-equity",
        "parser_hint": "html_table",
    },
    {
        "lp_name": "Oregon Public Employees Retirement Fund",
        "short_name": "Oregon PERS",
        "url": "https://www.oregon.gov/pers/investments/Pages/Private-Equity.aspx",
        "parser_hint": "html_table",
    },
    {
        "lp_name": "Washington State Investment Board",
        "short_name": "Washington State",
        "url": "https://www.sib.wa.gov/investments/private-equity/",
        "parser_hint": "html_table",
    },
]

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
    "Accept": "text/html,application/xhtml+xml",
}


class PensionIRScraper:
    """Scrapes public pension PE commitment pages."""

    def __init__(self):
        self._last_request: dict = {}
        self._rate_limit_seconds = 3.0

    async def _rate_limited_get(self, url: str) -> Optional[str]:
        """Fetch a URL with domain-level rate limiting."""
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        last = self._last_request.get(domain, 0)
        wait = self._rate_limit_seconds - (asyncio.get_event_loop().time() - last)
        if wait > 0:
            await asyncio.sleep(wait)

        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=HEADERS) as client:
                resp = await client.get(url)
                self._last_request[domain] = asyncio.get_event_loop().time()
                if resp.status_code == 200:
                    return resp.text
                else:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _parse_html_table(self, html: str, source_url: str) -> list:
        """Extract PE commitment rows from an HTML table using simple parsing."""
        results = []
        try:
            # Find tables
            table_pattern = re.compile(r'<table[^>]*>(.*?)</table>', re.DOTALL | re.IGNORECASE)
            row_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
            cell_pattern = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.DOTALL | re.IGNORECASE)
            tag_strip = re.compile(r'<[^>]+>')

            for table_match in table_pattern.finditer(html):
                table_html = table_match.group(1)
                rows = row_pattern.findall(table_html)
                if len(rows) < 3:
                    continue

                # Extract header row
                headers = []
                first_row_cells = cell_pattern.findall(rows[0])
                for cell in first_row_cells:
                    headers.append(tag_strip.sub('', cell).strip().lower())

                # Check if this looks like a PE table
                header_text = ' '.join(headers)
                if not any(kw in header_text for kw in ['fund', 'manager', 'commitment', 'vintage', 'private equity', 'investment']):
                    continue

                # Map columns
                col_map = {}
                for i, h in enumerate(headers):
                    if any(k in h for k in ['manager', 'general partner', 'gp', 'firm']):
                        col_map['gp_name'] = i
                    elif any(k in h for k in ['fund name', 'fund', 'vehicle']):
                        col_map['fund_name'] = i
                    elif 'vintage' in h or 'year' in h:
                        col_map['vintage_year'] = i
                    elif any(k in h for k in ['commitment', 'committed']):
                        col_map['commitment_amount_usd'] = i
                    elif any(k in h for k in ['status', 'stage']):
                        col_map['status'] = i

                if 'gp_name' not in col_map and 'fund_name' not in col_map:
                    continue

                # Parse data rows
                for row_html in rows[1:]:
                    cells = cell_pattern.findall(row_html)
                    cells = [tag_strip.sub('', c).strip() for c in cells]
                    if not cells or all(not c for c in cells):
                        continue

                    record = {"source_url": source_url, "data_source": "pension_ir"}
                    for field, idx in col_map.items():
                        if idx < len(cells):
                            val = cells[idx].strip()
                            if field == 'vintage_year':
                                m = re.search(r'\b(19|20)\d{2}\b', val)
                                record[field] = int(m.group()) if m else None
                            elif field == 'commitment_amount_usd':
                                # Parse "$125,000,000" or "125M" or "125.0"
                                clean = re.sub(r'[$,\s]', '', val)
                                m = re.match(r'([\d.]+)([MBK]?)', clean, re.IGNORECASE)
                                if m:
                                    num = float(m.group(1))
                                    suffix = m.group(2).upper()
                                    if suffix == 'B':
                                        num *= 1e9
                                    elif suffix == 'M':
                                        num *= 1e6
                                    elif suffix == 'K':
                                        num *= 1e3
                                    record[field] = num
                                else:
                                    record[field] = None
                            else:
                                record[field] = val if val else None

                    if record.get('gp_name') or record.get('fund_name'):
                        results.append(record)

        except Exception as e:
            logger.error(f"Error parsing HTML table: {e}")

        return results

    async def collect_pension(self, pension: dict) -> list:
        """Collect PE commitments from a single pension."""
        logger.info(f"Collecting PE commitments from {pension['short_name']}")
        html = await self._rate_limited_get(pension["url"])
        if not html:
            return []

        records = self._parse_html_table(html, pension["url"])

        # Add LP name to each record
        for r in records:
            r["lp_name"] = pension["short_name"]
            r["as_of_date"] = datetime.utcnow().isoformat()

        logger.info(f"  {pension['short_name']}: found {len(records)} commitment records")
        return records

    async def collect_all(self) -> list:
        """Collect PE commitments from all configured pensions."""
        all_records = []
        for pension in PENSION_PE_PAGES:
            try:
                records = await self.collect_pension(pension)
                all_records.extend(records)
            except Exception as e:
                logger.error(f"Failed to collect {pension['short_name']}: {e}")
        return all_records

    async def collect_by_name(self, short_name: str) -> list:
        """Collect from a specific pension by short name."""
        for pension in PENSION_PE_PAGES:
            if pension["short_name"].lower() == short_name.lower():
                return await self.collect_pension(pension)
        raise ValueError(
            f"Unknown pension: {short_name}. "
            f"Known: {[p['short_name'] for p in PENSION_PE_PAGES]}"
        )
