"""
SEC Form D Client.

Fetches Form D filings from SEC EDGAR.
"""

import logging
import httpx
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)


class FormDClient:
    """
    Client for accessing SEC Form D filings from EDGAR.

    Form D filings are submitted for private placements under Regulation D.
    """

    BASE_URL = "https://data.sec.gov"
    EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
    ARCHIVES_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

    # Required by SEC - must identify yourself
    USER_AGENT = "Nexdata Data Ingestion Service (contact: support@nexdata.io)"

    # Rate limit: 10 requests per second
    RATE_LIMIT_DELAY = 0.15  # 150ms between requests

    def __init__(self):
        self._last_request_time = 0

    async def _rate_limit(self):
        """Enforce SEC rate limits."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with required User-Agent."""
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json, application/xml, text/html",
        }

    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """
        Get all submissions for a company, including Form D filings.

        Args:
            cik: Company CIK (Central Index Key)

        Returns:
            Dict with company info and filing history
        """
        await self._rate_limit()

        cik_padded = str(cik).zfill(10)
        url = f"{self.BASE_URL}/submissions/CIK{cik_padded}.json"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()
            return response.json()

    async def get_form_d_filings_from_submissions(self, cik: str) -> List[Dict]:
        """
        Extract Form D filings from company submissions.

        Args:
            cik: Company CIK

        Returns:
            List of Form D filing metadata
        """
        submissions = await self.get_company_submissions(cik)

        filings = []
        recent = submissions.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form in ("D", "D/A"):
                filings.append(
                    {
                        "cik": cik,
                        "form": form,
                        "accession_number": accessions[i]
                        if i < len(accessions)
                        else None,
                        "filing_date": filing_dates[i]
                        if i < len(filing_dates)
                        else None,
                        "primary_document": primary_docs[i]
                        if i < len(primary_docs)
                        else None,
                        "company_name": submissions.get("name"),
                    }
                )

        return filings

    async def get_filing_xml(
        self, cik: str, accession_number: str, primary_document: str = None
    ) -> Optional[str]:
        """
        Download Form D XML content.

        Args:
            cik: Company CIK
            accession_number: Filing accession number (e.g., "0001234567-24-000001")
            primary_document: Primary document filename if known

        Returns:
            XML content as string, or None if not found
        """
        await self._rate_limit()

        cik_padded = str(cik).zfill(10)
        accession_clean = accession_number.replace("-", "")

        # Build list of URLs to try
        urls_to_try = []

        # If primary document is provided, try it first
        if primary_document:
            urls_to_try.append(
                f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/{primary_document}"
            )

        # Standard Form D document names
        urls_to_try.extend(
            [
                f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/primary_doc.xml",
                f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/form-d.xml",
            ]
        )

        async with httpx.AsyncClient(follow_redirects=True) as client:
            for url in urls_to_try:
                try:
                    response = await client.get(
                        url, headers=self._get_headers(), timeout=30
                    )
                    if response.status_code == 200:
                        content = response.text
                        # Verify it's XML
                        if (
                            content.strip().startswith("<?xml")
                            or "<edgarSubmission" in content
                        ):
                            return content
                except Exception as e:
                    logger.debug(f"Failed to fetch {url}: {e}")
                    continue

            # Last resort: try to get filing index and find XML file
            try:
                index = await self.get_filing_index(cik, accession_number)
                if index:
                    directory = index.get("directory", {})
                    items = directory.get("item", [])
                    for item in items:
                        name = item.get("name", "")
                        if name.endswith(".xml") and "form" in name.lower():
                            url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/{name}"
                            response = await client.get(
                                url, headers=self._get_headers(), timeout=30
                            )
                            if response.status_code == 200:
                                return response.text
            except Exception as e:
                logger.debug(f"Failed to fetch filing index: {e}")

        logger.warning(f"Could not find Form D XML for {cik}/{accession_number}")
        return None

    async def get_filing_index(self, cik: str, accession_number: str) -> Optional[Dict]:
        """
        Get filing index to find all documents in a filing.

        Args:
            cik: Company CIK
            accession_number: Filing accession number

        Returns:
            Filing index data
        """
        await self._rate_limit()

        cik_padded = str(cik).zfill(10)
        accession_clean = accession_number.replace("-", "")

        url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/index.json"

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                response = await client.get(
                    url, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                logger.warning(f"Failed to fetch filing index: {e}")

        return None

    async def search_recent_form_d(self, days: int = 7) -> List[Dict]:
        """
        Search for recent Form D filings using EDGAR full-text search.

        Note: This uses the SEC's full-text search which may have limitations.
        For comprehensive data, use the submissions API per-company.

        Args:
            days: Number of days to look back

        Returns:
            List of recent Form D filing metadata
        """
        await self._rate_limit()

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Use the SEC EDGAR full-text search endpoint
        params = {
            "forms": "D,D/A",
            "dateRange": "custom",
            "startdt": start_date.strftime("%Y-%m-%d"),
            "enddt": end_date.strftime("%Y-%m-%d"),
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self.EFTS_URL,
                    params=params,
                    headers=self._get_headers(),
                    timeout=30,
                )
                if response.status_code == 200:
                    data = response.json()
                    return data.get("hits", {}).get("hits", [])
            except Exception as e:
                logger.warning(f"Full-text search failed: {e}")

        return []

    async def search_recent_form_d_filings(
        self,
        start_date: str = None,
        end_date: str = None,
        limit: int = 200,
    ) -> List[Dict]:
        """
        Discover recent Form D filers via EDGAR EFTS full-text search.

        Returns list of {cik, accession_number, entity_name, file_date} dicts
        ready for XML ingestion.

        Args:
            start_date: ISO date string (default: 90 days ago)
            end_date: ISO date string (default: today)
            limit: max filings to return (default 200)
        """
        await self._rate_limit()

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        params = {
            "forms": "D,D/A",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "hits.hits._source": "period_of_report,entity_name,file_date,form_type",
            "hits.hits.total.value": "true",
            "_source": "period_of_report,entity_name,file_date,form_type",
            "from": 0,
            "size": min(limit, 200),
        }

        results = []
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self.EFTS_URL,
                    params=params,
                    headers=self._get_headers(),
                    timeout=30,
                )
                if response.status_code == 200:
                    data = response.json()
                    hits = data.get("hits", {}).get("hits", [])
                    seen_accessions = set()
                    for hit in hits:
                        source = hit.get("_source", {})
                        # _id format: "0002076112-26-000001:primary_doc.xml"
                        hit_id = hit.get("_id", "")
                        accession = source.get("adsh") or hit_id.split(":")[0]
                        if accession in seen_accessions:
                            continue
                        seen_accessions.add(accession)

                        # CIK is in source.ciks array
                        ciks_list = source.get("ciks", [])
                        cik = ciks_list[0] if ciks_list else accession.split("-")[0]

                        # Entity name from display_names
                        display_names = source.get("display_names", [])
                        if display_names:
                            entity_name = display_names[0].split("(CIK")[0].strip()
                        else:
                            entity_name = source.get("entity_name", "")

                        results.append({
                            "cik": cik,
                            "accession_number": accession,
                            "entity_name": entity_name,
                            "file_date": source.get("file_date", ""),
                        })
                else:
                    logger.warning(f"EFTS search returned {response.status_code}")
            except Exception as e:
                logger.warning(f"EFTS search_recent_form_d_filings failed: {e}")

        logger.info(f"EFTS search found {len(results)} Form D filings ({start_date} to {end_date})")
        return results

    async def get_recent_form_d_rss(self) -> List[Dict]:
        """
        Get recent Form D filings from SEC RSS feed.

        Returns:
            List of recent Form D filings
        """
        await self._rate_limit()

        # SEC provides RSS feeds for recent filings
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "action": "getcurrent",
            "type": "D",
            "company": "",
            "dateb": "",
            "owner": "include",
            "count": 100,
            "output": "atom",
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, params=params, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    # Parse Atom feed
                    return self._parse_atom_feed(response.text)
            except Exception as e:
                logger.warning(f"RSS feed fetch failed: {e}")

        return []

    def _parse_atom_feed(self, xml_content: str) -> List[Dict]:
        """Parse Atom feed XML to extract filing info."""
        import xml.etree.ElementTree as ET

        filings = []
        try:
            root = ET.fromstring(xml_content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            for entry in root.findall("atom:entry", ns):
                title = entry.find("atom:title", ns)
                link = entry.find("atom:link", ns)
                updated = entry.find("atom:updated", ns)
                summary = entry.find("atom:summary", ns)

                if title is not None:
                    filings.append(
                        {
                            "title": title.text,
                            "link": link.get("href") if link is not None else None,
                            "updated": updated.text if updated is not None else None,
                            "summary": summary.text if summary is not None else None,
                        }
                    )
        except Exception as e:
            logger.warning(f"Failed to parse Atom feed: {e}")

        return filings
