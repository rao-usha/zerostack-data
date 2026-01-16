"""
SEC EDGAR RSS Feed Parser.

Fetches recent SEC filings from EDGAR RSS feeds.
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional
import httpx

logger = logging.getLogger(__name__)

# SEC EDGAR RSS feed URLs
SEC_FEEDS = {
    "all_filings": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=&dateb=&owner=include&count=100&output=atom",
    "13f": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F&company=&dateb=&owner=include&count=40&output=atom",
    "13d": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13D&company=&dateb=&owner=include&count=40&output=atom",
    "8k": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=40&output=atom",
    "form_d": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=D&company=&dateb=&owner=include&count=40&output=atom",
}

# Atom namespace
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


class SECEdgarSource:
    """SEC EDGAR RSS feed source."""

    def __init__(self):
        self.name = "sec_edgar"
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    async def fetch(self, filing_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Fetch recent SEC filings.

        Args:
            filing_types: List of filing types to fetch (13f, 13d, 8k, form_d)
                         If None, fetches from all feeds.

        Returns:
            List of parsed news items.
        """
        items = []

        # Determine which feeds to fetch
        if filing_types:
            feeds_to_fetch = {k: v for k, v in SEC_FEEDS.items() if k in filing_types}
        else:
            # Default to key investment-related filings
            feeds_to_fetch = {
                "13f": SEC_FEEDS["13f"],
                "8k": SEC_FEEDS["8k"],
                "form_d": SEC_FEEDS["form_d"],
            }

        for feed_name, feed_url in feeds_to_fetch.items():
            try:
                feed_items = await self._fetch_feed(feed_url, feed_name)
                items.extend(feed_items)
            except Exception as e:
                logger.error(f"Error fetching SEC feed {feed_name}: {e}")

        return items

    async def _fetch_feed(self, url: str, feed_name: str) -> List[Dict]:
        """Fetch and parse a single RSS feed."""
        items = []

        try:
            response = await self.client.get(url, headers={
                "User-Agent": "Nexdata/1.0 (Investment Research Platform)"
            })
            response.raise_for_status()

            # Parse Atom feed
            root = ET.fromstring(response.text)

            for entry in root.findall("atom:entry", ATOM_NS):
                item = self._parse_entry(entry, feed_name)
                if item:
                    items.append(item)

            logger.info(f"Fetched {len(items)} items from SEC {feed_name} feed")

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching SEC feed: {e}")
        except ET.ParseError as e:
            logger.error(f"XML parse error for SEC feed: {e}")

        return items

    def _parse_entry(self, entry: ET.Element, feed_name: str) -> Optional[Dict]:
        """Parse a single Atom entry into a news item."""
        try:
            title_elem = entry.find("atom:title", ATOM_NS)
            link_elem = entry.find("atom:link", ATOM_NS)
            updated_elem = entry.find("atom:updated", ATOM_NS)
            summary_elem = entry.find("atom:summary", ATOM_NS)
            id_elem = entry.find("atom:id", ATOM_NS)

            if title_elem is None or title_elem.text is None:
                return None

            title = title_elem.text.strip()

            # Extract filing type from title
            filing_type = self._extract_filing_type(title)

            # Extract company/filer name
            company_name = self._extract_company_name(title)

            # Parse published date
            published_at = None
            if updated_elem is not None and updated_elem.text:
                try:
                    # SEC uses ISO format
                    date_str = updated_elem.text.strip()
                    if date_str.endswith("Z"):
                        date_str = date_str[:-1] + "+00:00"
                    published_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except ValueError:
                    published_at = datetime.utcnow()

            # Get URL
            url = None
            if link_elem is not None:
                url = link_elem.get("href")

            # Get summary
            summary = None
            if summary_elem is not None and summary_elem.text:
                summary = summary_elem.text.strip()[:500]

            # Generate unique source ID
            source_id = None
            if id_elem is not None and id_elem.text:
                source_id = id_elem.text.strip()
            else:
                source_id = f"sec_{feed_name}_{hash(title)}"

            return {
                "source": self.name,
                "source_id": source_id,
                "title": title,
                "summary": summary,
                "url": url,
                "published_at": published_at,
                "event_type": "filing",
                "filing_type": filing_type,
                "company_name": company_name,
                "company_ticker": self._extract_ticker(title),
                "investor_id": None,
                "investor_type": None,
                "relevance_score": 0.7 if filing_type in ["13F", "13D"] else 0.5,
            }

        except Exception as e:
            logger.error(f"Error parsing SEC entry: {e}")
            return None

    def _extract_filing_type(self, title: str) -> str:
        """Extract filing type from title."""
        title_upper = title.upper()

        if "13F" in title_upper:
            return "13F"
        elif "13D" in title_upper:
            return "13D"
        elif "13G" in title_upper:
            return "13G"
        elif "8-K" in title_upper or "8K" in title_upper:
            return "8-K"
        elif "10-K" in title_upper or "10K" in title_upper:
            return "10-K"
        elif "10-Q" in title_upper or "10Q" in title_upper:
            return "10-Q"
        elif "FORM D" in title_upper or "FORM-D" in title_upper:
            return "Form D"
        elif "S-1" in title_upper:
            return "S-1"
        else:
            return "Other"

    def _extract_company_name(self, title: str) -> Optional[str]:
        """Extract company/filer name from title."""
        # SEC titles often follow pattern: "Form Type - Company Name (CIK)"
        # or "Company Name (Form Type)"

        # Try to extract before parentheses
        match = re.search(r"^(.+?)\s*\(", title)
        if match:
            name = match.group(1).strip()
            # Remove form type prefix if present
            name = re.sub(r"^(13F-HR|13F|13D|13G|8-K|10-K|10-Q|Form D|S-1)\s*[-:]\s*", "", name, flags=re.I)
            if name and len(name) > 2:
                return name

        # Try splitting by dash
        parts = title.split(" - ")
        if len(parts) >= 2:
            return parts[1].strip()

        return None

    def _extract_ticker(self, title: str) -> Optional[str]:
        """Extract stock ticker from title if present."""
        # Look for ticker patterns like (AAPL) or [MSFT]
        match = re.search(r"[\(\[]([A-Z]{1,5})[\)\]]", title)
        if match:
            return match.group(1)
        return None
