"""
SEC EDGAR filing fetcher.

Retrieves company filings from SEC EDGAR with proper rate limiting
and caching. Supports DEF 14A, 8-K, 10-K, and Form 4 filings.

Includes CIK auto-discovery to find CIK by company name.
"""

import asyncio
import logging
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from app.sources.people_collection.base_collector import BaseCollector

logger = logging.getLogger(__name__)


@dataclass
class CIKSearchResult:
    """Result of a CIK search."""

    cik: str
    company_name: str
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    sic_code: Optional[str] = None
    sic_description: Optional[str] = None
    match_score: float = 0.0  # 0-1 similarity score


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

    Includes CIK auto-discovery to find CIK by company name.
    """

    # SEC EDGAR API endpoints
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
    FULL_TEXT_URL = (
        "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
    )

    # Company search endpoint (tickers and company names)
    COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    COMPANY_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt=2020-01-01&enddt={end_date}&forms=10-K"

    # Filing types we care about for leadership data
    LEADERSHIP_FILING_TYPES = {
        "DEF 14A": "Proxy statement - executives, board, compensation",
        "DEFA14A": "Additional proxy materials",
        "8-K": "Current report - may contain leadership changes",
        "10-K": "Annual report - executive information",
        "4": "Form 4 - insider transactions (lists officers/directors)",
        "SC 13D": "Beneficial ownership - board/investor info",
        "SC 13G": "Beneficial ownership - institutional",
    }

    # Cache for company tickers data
    _company_tickers_cache: Optional[Dict] = None
    _company_tickers_cache_time: Optional[datetime] = None

    def __init__(self):
        super().__init__(source_type="sec_edgar")

    def _normalize_cik(self, cik: str) -> str:
        """Normalize CIK to 10-digit zero-padded format."""
        # Remove any non-numeric characters
        cik_clean = re.sub(r"\D", "", str(cik))
        # Pad to 10 digits
        return cik_clean.zfill(10)

    # =========================================================================
    # CIK Auto-Discovery Methods
    # =========================================================================

    async def search_cik_by_company_name(
        self,
        company_name: str,
        limit: int = 5,
    ) -> List[CIKSearchResult]:
        """
        Search for a company's CIK by name.

        Uses SEC's company tickers file which contains all registered companies.
        Returns matches sorted by similarity score.

        Args:
            company_name: Company name to search for
            limit: Maximum results to return

        Returns:
            List of CIKSearchResult with best matches
        """
        logger.info(f"[FilingFetcher] Searching CIK for company: {company_name}")

        # Load company tickers data
        tickers_data = await self._get_company_tickers()
        if not tickers_data:
            logger.warning("[FilingFetcher] Failed to load company tickers data")
            return []

        # Normalize search name
        search_name = self._normalize_company_name(company_name)
        search_words = set(search_name.split())

        results = []

        for entry in tickers_data.values():
            sec_name = entry.get("title", "")
            if not sec_name:
                continue

            # Calculate similarity score
            normalized_sec_name = self._normalize_company_name(sec_name)
            score = self._calculate_name_similarity(
                search_name, normalized_sec_name, search_words
            )

            if score > 0.4:  # Minimum threshold
                cik = str(entry.get("cik_str", ""))
                results.append(
                    CIKSearchResult(
                        cik=cik,
                        company_name=sec_name,
                        ticker=entry.get("ticker"),
                        match_score=score,
                    )
                )

        # Sort by score descending
        results.sort(key=lambda x: x.match_score, reverse=True)

        if results:
            logger.info(
                f"[FilingFetcher] Found {len(results)} CIK matches for '{company_name}'. "
                f"Best: {results[0].company_name} (CIK: {results[0].cik}, score: {results[0].match_score:.2f})"
            )
        else:
            logger.warning(f"[FilingFetcher] No CIK matches found for '{company_name}'")

        return results[:limit]

    async def get_cik_for_company(
        self,
        company_name: str,
        min_score: float = 0.7,
    ) -> Optional[str]:
        """
        Get the best matching CIK for a company name.

        Args:
            company_name: Company name to search for
            min_score: Minimum similarity score to accept (0-1)

        Returns:
            CIK string if found with sufficient confidence, None otherwise
        """
        results = await self.search_cik_by_company_name(company_name, limit=1)

        if results and results[0].match_score >= min_score:
            return results[0].cik

        return None

    async def search_cik_by_ticker(self, ticker: str) -> Optional[CIKSearchResult]:
        """
        Search for a company's CIK by stock ticker.

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL")

        Returns:
            CIKSearchResult if found, None otherwise
        """
        logger.info(f"[FilingFetcher] Searching CIK for ticker: {ticker}")

        tickers_data = await self._get_company_tickers()
        if not tickers_data:
            return None

        ticker_upper = ticker.upper().strip()

        for entry in tickers_data.values():
            if entry.get("ticker", "").upper() == ticker_upper:
                return CIKSearchResult(
                    cik=str(entry.get("cik_str", "")),
                    company_name=entry.get("title", ""),
                    ticker=entry.get("ticker"),
                    match_score=1.0,
                )

        logger.warning(f"[FilingFetcher] No CIK found for ticker: {ticker}")
        return None

    async def _get_company_tickers(self) -> Optional[Dict]:
        """
        Get the SEC company tickers data (cached).

        Returns dict mapping index to company info:
        {
            "0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc."},
            ...
        }
        """
        # Check cache (refresh every 24 hours)
        if (
            self._company_tickers_cache is not None
            and self._company_tickers_cache_time is not None
            and (datetime.now() - self._company_tickers_cache_time).total_seconds()
            < 86400
        ):
            return self._company_tickers_cache

        logger.debug("[FilingFetcher] Fetching company tickers from SEC")

        data = await self.fetch_json(self.COMPANY_TICKERS_URL, use_cache=True)

        if data:
            FilingFetcher._company_tickers_cache = data
            FilingFetcher._company_tickers_cache_time = datetime.now()
            logger.info(f"[FilingFetcher] Loaded {len(data)} company tickers from SEC")

        return data

    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for comparison."""
        if not name:
            return ""

        name = name.lower()

        # Remove common suffixes
        suffixes = [
            " inc.",
            " inc",
            " incorporated",
            " corp.",
            " corp",
            " corporation",
            " llc",
            " l.l.c.",
            " ltd",
            " ltd.",
            " limited",
            " plc",
            " plc.",
            " co.",
            " co",
            " company",
            " companies",
            " group",
            " holdings",
            " holding",
            " international",
            " intl",
            " intl.",
            " &",
            " and",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

        # Remove punctuation
        name = re.sub(r"[^\w\s]", " ", name)

        # Collapse whitespace
        name = " ".join(name.split())

        return name.strip()

    def _calculate_name_similarity(
        self,
        search_name: str,
        sec_name: str,
        search_words: set,
    ) -> float:
        """
        Calculate similarity between search name and SEC name.

        Uses multiple methods:
        1. Exact match
        2. SequenceMatcher ratio
        3. Word overlap
        """
        # Exact match
        if search_name == sec_name:
            return 1.0

        # One contains the other
        if search_name in sec_name or sec_name in search_name:
            return 0.9

        # SequenceMatcher
        ratio = SequenceMatcher(None, search_name, sec_name).ratio()

        # Word overlap bonus
        sec_words = set(sec_name.split())
        common_words = search_words & sec_words
        if common_words:
            # Bonus for matching words
            word_ratio = len(common_words) / max(len(search_words), len(sec_words))
            ratio = max(ratio, word_ratio * 0.85)

        # First word match bonus (company name usually starts the same)
        search_first = search_name.split()[0] if search_name else ""
        sec_first = sec_name.split()[0] if sec_name else ""
        if search_first and sec_first and search_first == sec_first:
            ratio = max(ratio, 0.75)

        return ratio

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

        for i in range(len(accession_numbers)):
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

    async def get_latest_filing(
        self,
        cik: str,
        filing_type: str = "10-K",
    ) -> Optional[SECFiling]:
        """
        Get the most recent filing of any type.

        Args:
            cik: SEC CIK number
            filing_type: Filing type (e.g. "10-K", "10-Q", "DEF 14A")

        Returns:
            Most recent filing of that type, or None
        """
        filings = await self.get_company_filings(
            cik,
            filing_types=[filing_type],
            limit=5,
        )

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

    async def get_recent_form4s(
        self,
        cik: str,
        since_date: Optional[date] = None,
        limit: int = 20,
    ) -> List[SECFiling]:
        """
        Get recent Form 4 filings (insider transactions).

        Form 4s are filed by officers and directors when they trade company stock.
        They list the person's name, title, and relationship to the company.

        Args:
            cik: Company CIK number
            since_date: Only include filings after this date
            limit: Maximum filings to return

        Returns:
            List of Form 4 SECFiling objects
        """
        logger.info(f"[FilingFetcher] Fetching Form 4s for CIK {cik}")

        filings = await self.get_company_filings(
            cik,
            filing_types=["4"],
            limit=limit * 2,  # Fetch more in case we filter
        )

        if since_date:
            filings = [f for f in filings if f.filing_date >= since_date]

        logger.info(f"[FilingFetcher] Found {len(filings[:limit])} Form 4 filings")
        return filings[:limit]

    async def get_form4_filers(
        self,
        cik: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get unique filers (officers/directors) from recent Form 4s.

        This is a quick way to get a list of current insiders without
        parsing proxy statements.

        Args:
            cik: Company CIK number
            limit: Maximum Form 4s to check

        Returns:
            List of dicts with name, title, is_officer, is_director
        """
        from datetime import timedelta

        logger.info(f"[FilingFetcher] Getting Form 4 filers for CIK {cik}")

        # Get recent Form 4s (last 2 years)
        since = date.today() - timedelta(days=730)
        form4s = await self.get_recent_form4s(cik, since_date=since, limit=limit)

        filers = {}  # name -> info

        for filing in form4s:
            # Fetch and parse the Form 4 XML
            filer_info = await self._parse_form4_filer(filing)
            if filer_info:
                name = filer_info.get("name", "")
                if name and name not in filers:
                    filers[name] = filer_info

        result = list(filers.values())
        logger.info(f"[FilingFetcher] Found {len(result)} unique Form 4 filers")
        return result

    async def _parse_form4_filer(self, filing: SECFiling) -> Optional[Dict[str, Any]]:
        """
        Parse a Form 4 filing to extract filer information.

        Form 4 XML contains:
        - reportingOwner/reportingOwnerId/rptOwnerName
        - reportingOwner/reportingOwnerRelationship (isDirector, isOfficer, officerTitle)
        """
        try:
            content = await self.get_filing_content(filing, max_length=100000)
            if not content:
                return None

            # Form 4s are XML, try to parse
            from bs4 import BeautifulSoup

            # Check if it's XML
            if (
                "<?xml" not in content[:100].lower()
                and "<ownershipDocument" not in content[:500]
            ):
                return None

            soup = BeautifulSoup(content, "xml")

            # Find reporting owner
            owner = soup.find("reportingOwner")
            if not owner:
                return None

            # Get name
            name_el = owner.find("rptOwnerName")
            if not name_el:
                return None
            name = name_el.get_text(strip=True)

            # Get relationship
            relationship = owner.find("reportingOwnerRelationship")
            is_director = False
            is_officer = False
            title = None

            if relationship:
                is_director_el = relationship.find("isDirector")
                is_officer_el = relationship.find("isOfficer")
                title_el = relationship.find("officerTitle")

                is_director = (
                    is_director_el and is_director_el.get_text(strip=True) == "1"
                )
                is_officer = is_officer_el and is_officer_el.get_text(strip=True) == "1"
                title = title_el.get_text(strip=True) if title_el else None

            return {
                "name": name,
                "title": title,
                "is_director": is_director,
                "is_officer": is_officer,
                "filing_date": filing.filing_date,
                "source": "form4",
            }

        except Exception as e:
            logger.debug(f"[FilingFetcher] Failed to parse Form 4: {e}")
            return None


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
