"""
Board Seat Collection Agent.

Collects board directorships from two sources:
1. SEC DEF 14A — "Other Public Company Directorships" table
2. Company website "Board of Directors" pages

Uses existing FilingFetcher + LLMClient infrastructure.
"""

import logging
import re
from typing import List, Optional
from datetime import datetime

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.agentic.llm_client import LLMClient

logger = logging.getLogger(__name__)

BOARD_EXTRACTION_PROMPT = """
Extract board of directors information from this text. Return a JSON array where each element has:
{
  "person_name": "Full Name",
  "role": "Independent Director",
  "committee": "Audit Committee",
  "is_chair": false,
  "other_directorships": ["Company A", "Company B"]
}
Only include actual board members. Exclude executives unless they also have a board seat.
"""

PROXY_OTHER_DIRECTORSHIPS_PROMPT = """
From this SEC proxy filing text, extract the "Other Directorships" or "Other Public Company Directorships"
table. Return JSON array:
[
  {
    "director_name": "Full Name",
    "other_companies": [
      {"company_name": "XYZ Corp", "role": "Director", "since_year": 2019}
    ]
  }
]
Return empty array if this section is not present.
"""


class BoardAgent(BaseCollector):
    """Collects board seat data from SEC proxies and company websites."""

    def __init__(self):
        super().__init__(source_type="sec_edgar")
        self.fetcher = FilingFetcher()
        self.llm     = LLMClient()

    async def close(self):
        await super().close()
        await self.fetcher.close()

    async def collect_from_proxy(self, cik: str, company_name: str) -> List[dict]:
        """
        Parse DEF 14A for board members + their other directorships.
        Returns list of dicts: {director_name, other_companies: [...]}
        """
        filings = await self.fetcher.get_company_filings(cik, filing_types=["DEF 14A"], limit=1)
        if not filings:
            logger.info(f"No DEF 14A found for {company_name} (CIK {cik})")
            return []

        text = await self.fetcher.get_filing_content(filings[0])
        if not text:
            return []

        board_section = self._extract_board_section(text)
        if not board_section:
            return []

        try:
            response = await self.llm.complete(
                prompt=f"{PROXY_OTHER_DIRECTORSHIPS_PROMPT}\n\n---\n\n{board_section[:8000]}"
            )
            data = response.parse_json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"LLM extraction failed for {company_name}: {e}")
            return []

    async def collect_from_website(self, company_id: int, board_url: str) -> List[dict]:
        """
        Scrape company board page for director listings.
        Returns list of dicts: {person_name, role, committee}
        """
        try:
            html = await self.fetch(board_url)
        except Exception as e:
            logger.warning(f"Failed to fetch board page {board_url}: {e}")
            return []

        from app.sources.people_collection.html_cleaner import HTMLCleaner
        text = HTMLCleaner().clean(html)

        try:
            response = await self.llm.complete(
                prompt=f"{BOARD_EXTRACTION_PROMPT}\n\n---\n\n{text[:6000]}"
            )
            data = response.parse_json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Board page extraction failed for {board_url}: {e}")
            return []

    def _extract_board_section(self, text: str) -> Optional[str]:
        """Find the director/board section in a proxy filing."""
        markers = [
            "other directorships", "other public company directorships",
            "director qualifications", "information about our directors",
            "nominees for director"
        ]
        text_lower = text.lower()
        for marker in markers:
            idx = text_lower.find(marker)
            if idx != -1:
                return text[max(0, idx - 200): idx + 12000]
        return None
