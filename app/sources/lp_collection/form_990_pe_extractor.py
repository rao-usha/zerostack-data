"""
Form 990 PE Extractor — extracts PE fund investments from IRS Form 990 filings.

Targets university endowments and large foundations that file 990s and hold PE funds.
Schedule D Part VIII covers investments in other entities (including PE funds).
"""
import asyncio
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Known endowments/foundations with PE portfolios and their EINs
ENDOWMENT_TARGETS = [
    {"name": "Harvard Management Company", "ein": "04-2103580", "short": "Harvard"},
    {"name": "Yale University", "ein": "06-0646973", "short": "Yale"},
    {"name": "Stanford University", "ein": "94-1156365", "short": "Stanford"},
    {"name": "MIT Investment Management Company", "ein": "04-2103594", "short": "MIT"},
    {"name": "Princeton University", "ein": "21-0634501", "short": "Princeton"},
]

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
}

EDGAR_990_SEARCH = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcompany&company={name}&type=990&dateb=&owner=include"
    "&count=5&search_text=&output=atom"
)


class Form990PEExtractor:
    """Extracts PE investments from IRS Form 990 filings via SEC EDGAR."""

    async def search_990_filing(self, org_name: str) -> Optional[str]:
        """Find the most recent Form 990 filing URL for an organization."""
        url = EDGAR_990_SEARCH.format(name=org_name.replace(' ', '+'))
        try:
            async with httpx.AsyncClient(timeout=20, headers=HEADERS) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    return None

                # Find first filing href in ATOM feed
                m = re.search(r'<filing-href>(.*?)</filing-href>', resp.text)
                return m.group(1).strip() if m else None
        except Exception as e:
            logger.error(f"990 search error for {org_name}: {e}")
            return None

    def _extract_pe_investments_from_text(self, text: str, org_name: str) -> list:
        """
        Extract PE fund investments from Form 990 text.
        Looks for Schedule D Part VIII: "Investments—Other Securities" and
        similar schedules listing fund investments.
        """
        results = []

        # Patterns that indicate PE fund entries
        # Form 990 Schedule D typically lists: Entity Name | Investment Type | Book Value
        lines = text.split('\n')

        in_pe_section = False
        for i, line in enumerate(lines):
            line_clean = line.strip()

            # Detect PE section headers
            if any(kw in line_clean.lower() for kw in [
                'private equity', 'venture capital', 'alternative investment',
                'schedule d', 'investments - other', 'other investments',
                'partnership interests'
            ]):
                in_pe_section = True
                continue

            # Exit section on blank or new section
            if in_pe_section and (not line_clean or line_clean.startswith('Part ')):
                in_pe_section = False
                continue

            if not in_pe_section:
                continue

            # Try to extract fund name + book value
            # Common pattern: "KKR Americas Fund XII LP    Partnership    125,000,000"
            m = re.match(
                r'^([A-Z][A-Za-z0-9\s,\.\-&]+(?:Fund|Partners|Capital|Ventures|LP|LLC|L\.P\.)[^$\d]*)'
                r'\s+([\d,]+(?:\.\d+)?)\s*$',
                line_clean
            )
            if m:
                fund_name = m.group(1).strip()
                book_value_str = m.group(2).replace(',', '')
                try:
                    book_value = float(book_value_str)
                    # Only include if looks like a dollar amount (> $1M)
                    if book_value > 1_000_000:
                        # Try to extract manager name (usually first 2-3 words before "Fund")
                        manager_match = re.match(
                            r'^((?:\w+\s+){1,3})(?:Fund|Partners|Capital)', fund_name
                        )
                        results.append({
                            "fund_name": fund_name,
                            "gp_name": manager_match.group(1).strip() if manager_match else fund_name.split()[0],
                            "fair_value_usd": book_value,
                            "lp_name": org_name,
                            "data_source": "form_990",
                        })
                except ValueError:
                    pass

        return results

    async def collect_for_endowment(self, target: dict) -> list:
        """Collect PE investments for a specific endowment."""
        logger.info(f"Collecting Form 990 PE data for {target['short']}")

        filing_url = await self.search_990_filing(target['name'])
        if not filing_url:
            logger.warning(f"No 990 filing found for {target['name']}")
            return []

        try:
            async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
                resp = await client.get(filing_url)
                if resp.status_code != 200:
                    return []

                records = self._extract_pe_investments_from_text(resp.text, target['short'])
                logger.info(f"  {target['short']}: found {len(records)} PE investment records")
                return records
        except Exception as e:
            logger.error(f"Error fetching 990 for {target['name']}: {e}")
            return []

    async def collect_all(self) -> list:
        """Collect PE investments from all configured endowments."""
        all_records = []
        for target in ENDOWMENT_TARGETS:
            await asyncio.sleep(1.0)  # rate limit
            try:
                records = await self.collect_for_endowment(target)
                all_records.extend(records)
            except Exception as e:
                logger.error(f"Failed {target['short']}: {e}")
        return all_records
