"""
SEC EDGAR filing fetcher.

Retrieves company filings from SEC EDGAR with proper rate limiting
and caching. Supports DEF 14A, 8-K, and 10-K filings.
"""

import asyncio
import logging
import re
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass

from app.sources.people_collection.base_collector import BaseCollector

logger = logging.getLogger(__name__)


@dataclass
class SECFiling:
    """Represents an SEC filing."""
    accession_number: str
    filing_type: str
    filing_date: date
    description: str
    primary_document: str
    filing_url: str
    document_url: str
    company_name: str
    cik: str


class FilingFetcher(BaseCollector):
    """
    Fetches SEC filings from EDGAR.

    Uses the SEC EDGAR API and follows SEC rate limiting guidelines
    (10 requests per second max, but we use conservative limits).
    """

    # SEC EDGAR API endpoints
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
    FULL_TEXT_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"

    # Filing types we care about for leadership data
    LEADERSHIP_FILING_TYPES = {
        "DEF 14A": "Proxy statement - executives, board, compensation",
        "DEFA14A": "Additional proxy materials",
        "8-K": "Current report - may contain leadership changes",
        "10-K": "Annual report - executive information",
        "SC 13D": "Beneficial ownership - board/investor info",
        "SC 13G": "Beneficial ownership - institutional",
    }

    def __init__(self):
        super().__init__(source_type="sec_edgar")

    def _normalize_cik(self, cik: str) -> str:
        """Normalize CIK to 10-digit zero-padded format."""
        # Remove any non-numeric characters
        cik_clean = re.sub(r'\D', '', str(cik))
        # Pad to 10 digits
        return cik_clean.zfill(10)

    def _format_accession(self, accession: str, with_dashes: bool = True) -> str:
        """Format accession number with or without dashes."""
        # Remove existing dashes
        clean = accession.replace("-", "")
        if with_dashes:
            # Format as NNNNNNNNNN-NN-NNNNNN
            return f"{clean[:10]}-{clean[10:12]}-{clean[12:]}"
        return clean

    async def get_company_filings(
        self,
        cik: str,
        filing_types: List[str] = None,
        limit: int = 50,
    ) -> List[SECFiling]:
        """
        Get recent filings for a company.

        Args:
            cik: SEC CIK number
            filing_types: List of filing types to include (default: leadership-related)
            limit: Maximum number of filings to return

        Returns:
            List of SECFiling objects
        """
        if filing_types is None:
            filing_types = list(self.LEADERSHIP_FILING_TYPES.keys())

        cik_padded = self._normalize_cik(cik)
        url = self.SUBMISSIONS_URL.format(cik=cik_padded)

        logger.debug(f"Fetching submissions for CIK {cik_padded}")

        data = await self.fetch_json(url)
        if not data:
            logger.warning(f"No submissions data for CIK {cik}")
            return []

        filings = []
        company_name = data.get("name", "Unknown")

        # Parse recent filings
        recent = data.get("filings", {}).get("recent", {})
        if not recent:
            return []

        accession_numbers = recent.get("accessionNumber", [])
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        primary_documents = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        for i in range(min(len(accession_numbers), 200)):  # Check up to 200 filings
            form = forms[i] if i < len(forms) else ""

            # Filter by filing type
            if form not in filing_types:
                continue

            accession = accession_numbers[i]
            accession_nodash = self._format_accession(accession, with_dashes=False)

            filing_date_str = filing_dates[i] if i < len(filing_dates) else ""
            try:
                filing_date = date.fromisoformat(filing_date_str)
            except ValueError:
                filing_date = date.today()

            primary_doc = primary_documents[i] if i < len(primary_documents) else ""
            description = descriptions[i] if i < len(descriptions) else ""

            filing = SECFiling(
                accession_number=accession,
                filing_type=form,
                filing_date=filing_date,
                description=description,
                primary_document=primary_doc,
                filing_url=self.FILING_URL.format(
                    cik=cik_padded.lstrip("0"),
                    accession=accession_nodash,
                ),
                document_url=self.FULL_TEXT_URL.format(
                    cik=cik_padded.lstrip("0"),
                    accession=accession_nodash,
                    document=primary_doc,
                ),
                company_name=company_name,
                cik=cik,
            )
            filings.append(filing)

            if len(filings) >= limit:
                break

        logger.info(f"Found {len(filings)} relevant filings for {company_name}")
        return filings

    async def get_filing_content(
        self,
        filing: SECFiling,
        max_length: int = 500000,
    ) -> Optional[str]:
        """
        Fetch the full text content of a filing.

        Args:
            filing: SECFiling object
            max_length: Maximum content length to return

        Returns:
            Filing text content or None if failed
        """
        content = await self.fetch_url(filing.document_url)

        if not content:
            logger.warning(f"Failed to fetch filing {filing.accession_number}")
            return None

        # Truncate if too long
        if len(content) > max_length:
            content = content[:max_length]

        return content

    async def get_latest_proxy(self, cik: str) -> Optional[SECFiling]:
        """Get the most recent DEF 14A proxy statement (full proxy, not amendments)."""
        filings = await self.get_company_filings(
            cik,
            filing_types=["DEF 14A", "DEFA14A"],
            limit=10,  # Get several to find the best one
        )

        # Prefer DEF 14A over DEFA14A (DEFA14A is additional materials)
        for filing in filings:
            if filing.filing_type == "DEF 14A":
                return filing

        # Fall back to DEFA14A if no DEF 14A found
        return filings[0] if filings else None

    async def get_recent_8ks(
        self,
        cik: str,
        since_date: Optional[date] = None,
        limit: int = 10,
    ) -> List[SECFiling]:
        """
        Get recent 8-K filings (current reports).

        These often contain leadership change announcements.
        """
        filings = await self.get_company_filings(
            cik,
            filing_types=["8-K"],
            limit=limit * 2,  # Fetch more in case we filter
        )

        if since_date:
            filings = [f for f in filings if f.filing_date >= since_date]

        return filings[:limit]

    async def search_8k_for_leadership(
        self,
        filing: SECFiling,
    ) -> bool:
        """
        Quick check if an 8-K likely contains leadership changes.

        Looks for Item 5.02 (Departure/Appointment of Officers/Directors).
        """
        content = await self.get_filing_content(filing, max_length=50000)

        if not content:
            return False

        # Item 5.02 is specifically about leadership changes
        leadership_indicators = [
            "item 5.02",
            "item 5.03",  # Amendments to articles
            "departure of directors",
            "appointment of",
            "resignation of",
            "principal executive officer",
            "principal financial officer",
            "named executive officer",
        ]

        content_lower = content.lower()
        return any(ind in content_lower for ind in leadership_indicators)


async def get_company_sec_filings(
    cik: str,
    filing_types: List[str] = None,
    limit: int = 20,
) -> List[SECFiling]:
    """
    Convenience function to get SEC filings for a company.

    Args:
        cik: SEC CIK number
        filing_types: Types to include (default: leadership-related)
        limit: Maximum filings to return

    Returns:
        List of SECFiling objects
    """
    async with FilingFetcher() as fetcher:
        return await fetcher.get_company_filings(cik, filing_types, limit)
