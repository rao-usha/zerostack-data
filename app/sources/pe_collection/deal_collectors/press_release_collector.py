"""
Press Release Deal Collector for PE deal intelligence.

Searches PR distribution services (PR Newswire, Business Wire, GlobeNewswire)
for deal announcements, then uses GPT-4o-mini to extract structured deal data.
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, quote_plus

from app.sources.pe_collection.base_collector import BasePECollector
from app.sources.pe_collection.types import (
    PECollectionResult,
    PECollectedItem,
    PECollectionSource,
    EntityType,
)
from app.sources.pe_collection.config import settings

logger = logging.getLogger(__name__)

# PR search endpoints
PR_NEWSWIRE_SEARCH = "https://www.prnewswire.com/search/news/?keyword={query}&page=1&pagesize=25"
BUSINESS_WIRE_SEARCH = "https://www.businesswire.com/portal/site/home/search/?searchType=news&searchTerm={query}"
GLOBENEWSWIRE_SEARCH = "https://www.globenewswire.com/search?keyword={query}&pageSize=25"
SEC_8K_SEARCH = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt={start}&enddt={end}&forms=8-K"

# Deal-related keywords for filtering
DEAL_KEYWORDS = [
    "acquisition", "acquire", "acquired",
    "investment", "invest",
    "portfolio company",
    "completes", "completed",
    "merger", "merge",
    "recapitalization",
    "buyout", "buy-out",
    "majority stake", "minority stake",
    "strategic partnership",
    "capital investment",
    "growth equity",
    "add-on", "bolt-on",
    "platform investment",
    "exit", "divest", "divestiture",
    "ipo", "public offering",
]

# Maximum press releases to process with LLM per firm
MAX_PR_TO_PROCESS = 15

# LLM extraction prompt
DEAL_EXTRACTION_PROMPT = """Extract deal information from this press release about {firm_name}. Return ONLY valid JSON:
{{
  "is_deal": true or false,
  "deal_type": "LBO|Growth|Add-on|Exit|Recap|Merger|IPO|Other",
  "target_company": "company name or null",
  "target_description": "1-sentence description of the target company or null",
  "enterprise_value_usd": number or null,
  "announced_date": "YYYY-MM-DD or null",
  "closed_date": "YYYY-MM-DD or null",
  "co_investors": ["list of co-investors"] or [],
  "seller": "seller name or null",
  "description": "1-2 sentence deal summary"
}}

If this is not about a PE/VC deal by {firm_name}, set is_deal to false.

Press release text:
{text}"""


class PressReleaseCollector(BasePECollector):
    """
    Collects PE deal data from press release distribution services.

    Searches PR Newswire, Business Wire, and GlobeNewswire for deal
    announcements, then uses LLM to extract structured deal information.
    """

    @property
    def source_type(self) -> PECollectionSource:
        return PECollectionSource.PRESS_RELEASE

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FIRM

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", settings.news_rate_limit_delay
        )
        self._llm_client = None

    def _get_llm_client(self):
        """Lazily initialize LLM client."""
        if self._llm_client is None:
            from app.agentic.llm_client import get_llm_client
            self._llm_client = get_llm_client(model="gpt-4o-mini")
        return self._llm_client

    async def collect(
        self,
        entity_id: int,
        entity_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> PECollectionResult:
        """
        Collect deal announcements for a PE firm from press releases.

        Args:
            entity_id: PE firm ID
            entity_name: Firm name
            website_url: Firm website (not used)
        """
        started_at = datetime.utcnow()
        self.reset_tracking()
        items: List[PECollectedItem] = []
        warnings: List[str] = []

        try:
            # Build search query
            query = f'"{entity_name}" acquisition OR investment OR portfolio OR completes'

            # Search PR services
            press_releases = await self._search_all_sources(query, entity_name)
            logger.info(
                f"Found {len(press_releases)} candidate press releases for {entity_name}"
            )

            if not press_releases:
                return self._create_result(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    success=True,
                    items=[],
                    warnings=["No deal-related press releases found"],
                    started_at=started_at,
                )

            # Separate SEC 8-K metadata items from fetchable press releases
            sec_items = [pr for pr in press_releases if pr.get("source") == "sec_8k"]
            pr_items = [pr for pr in press_releases if pr.get("source") != "sec_8k"]

            # SEC 8-K filings: create deal items from metadata directly
            # (filing documents are blocked from Docker, but metadata is sufficient)
            for sec in sec_items[:15]:
                items.append(
                    self._create_item(
                        item_type="deal_8k_filing",
                        data={
                            "firm_id": entity_id,
                            "firm_name": entity_name,
                            "title": sec.get("title"),
                            "filing_date": sec.get("date"),
                            "company_name": sec.get("company_name"),
                            "filing_items": sec.get("items", []),
                        },
                        source_url=sec.get("url"),
                        confidence="high",
                    )
                )

            # Process fetchable press releases with LLM extraction
            llm_client = self._get_llm_client()
            processed = 0
            for pr in pr_items:
                if processed >= MAX_PR_TO_PROCESS:
                    break

                pr_url = pr.get("url")
                if not pr_url:
                    continue

                # Fetch full text
                text = await self._fetch_pr_text(pr_url)
                if not text or len(text) < 100:
                    continue

                # Truncate to avoid exceeding token limits
                text = text[:8000]
                processed += 1

                if not llm_client:
                    # No LLM — store as metadata-only item
                    items.append(
                        self._create_item(
                            item_type="deal_press_release",
                            data={
                                "firm_id": entity_id,
                                "firm_name": entity_name,
                                "title": pr.get("title"),
                                "url": pr_url,
                                "source": pr.get("source"),
                            },
                            source_url=pr_url,
                            confidence="low",
                        )
                    )
                    continue

                # Extract deal data via LLM
                deal_data = await self._extract_deal_with_llm(
                    llm_client, text, entity_name
                )
                if not deal_data or not deal_data.get("is_deal"):
                    continue

                # Build deal item
                deal_item = self._build_deal_item(
                    deal_data, entity_id, entity_name, pr_url, pr.get("title")
                )
                if deal_item:
                    items.append(deal_item)

                # Build portfolio company item if target identified
                target = deal_data.get("target_company")
                if target and deal_data.get("deal_type") != "Exit":
                    items.append(
                        self._create_item(
                            item_type="portfolio_company",
                            data={
                                "name": target,
                                "description": deal_data.get("target_description"),
                                "current_pe_owner": entity_name,
                                "ownership_status": "PE-Backed",
                            },
                            source_url=pr_url,
                            confidence="llm_extracted",
                        )
                    )

            logger.info(
                f"Extracted {len(items)} deal items from {processed} press releases "
                f"for {entity_name}"
            )

            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=True,
                items=items,
                warnings=warnings if warnings else None,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting press releases for {entity_name}: {e}")
            return self._create_result(
                entity_id=entity_id,
                entity_name=entity_name,
                success=False,
                error_message=str(e),
                items=items,
                started_at=started_at,
            )

    async def _search_all_sources(
        self, query: str, firm_name: str
    ) -> List[Dict[str, Any]]:
        """Search all PR sources and deduplicate results."""
        all_releases = []

        # Search SEC EDGAR full-text search (most reliable from Docker)
        try:
            sec_results = await self._search_sec_efts(firm_name)
            all_releases.extend(sec_results)
        except Exception as e:
            logger.warning(f"SEC EFTS search failed: {e}")

        # Search PR Newswire
        try:
            prs = await self._search_pr_newswire(query)
            all_releases.extend(prs)
        except Exception as e:
            logger.warning(f"PR Newswire search failed: {e}")

        # Search Business Wire
        try:
            bws = await self._search_business_wire(query)
            all_releases.extend(bws)
        except Exception as e:
            logger.warning(f"Business Wire search failed: {e}")

        # Search GlobeNewswire (firm-specific query for better results)
        try:
            gns = await self._search_globenewswire_firm(firm_name)
            all_releases.extend(gns)
        except Exception as e:
            logger.warning(f"GlobeNewswire search failed: {e}")

        # Filter to deal-related press releases
        filtered = self._filter_deal_related(all_releases, firm_name)

        # Deduplicate by URL
        seen_urls = set()
        unique = []
        for pr in filtered:
            url = pr.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique.append(pr)

        return unique

    async def _search_sec_efts(self, firm_name: str) -> List[Dict[str, Any]]:
        """Search SEC EDGAR full-text search for 8-K deal announcements."""
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{firm_name}" acquisition',
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": "2023-01-01",
            "enddt": datetime.utcnow().strftime("%Y-%m-%d"),
        }
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
        }
        data = await self._fetch_json(url, headers=headers, params=params)
        if not data:
            return []

        releases = []
        hits = data.get("hits", {}).get("hits", [])
        for hit in hits[:15]:
            source = hit.get("_source", {})
            # SEC EFTS fields: adsh (accession), ciks (list), file_num (list),
            # display_names (list), file_date (string)
            adsh = source.get("adsh", "")
            ciks = source.get("ciks", [])
            cik = ciks[0].lstrip("0") if ciks else ""
            display_names = source.get("display_names", [])
            title = display_names[0] if display_names else ""
            filing_date = source.get("file_date", "")

            if adsh and cik:
                accession_clean = adsh.replace("-", "")
                sec_url = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{cik}/{accession_clean}/"
                )
                # Extract 8-K item numbers (e.g., 1.01 = Material Agreement)
                filing_items = source.get("items", [])
                # Clean company name from display format
                company_name = title.split("(")[0].strip() if title else ""
                releases.append({
                    "url": sec_url,
                    "title": f"8-K: {company_name}" if company_name else f"8-K filing {filing_date}",
                    "source": "sec_8k",
                    "date": filing_date,
                    "company_name": company_name,
                    "items": filing_items,
                })

        return releases

    async def _search_pr_newswire(self, query: str) -> List[Dict[str, Any]]:
        """Search PR Newswire for press releases."""
        url = PR_NEWSWIRE_SEARCH.format(query=quote_plus(query))
        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return []

        return self._extract_pr_newswire_links(response.text)

    async def _search_business_wire(self, query: str) -> List[Dict[str, Any]]:
        """Search Business Wire for press releases."""
        url = BUSINESS_WIRE_SEARCH.format(query=quote_plus(query))
        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return []

        return self._extract_business_wire_links(response.text)

    async def _search_globenewswire(self, query: str) -> List[Dict[str, Any]]:
        """Search GlobeNewswire for press releases."""
        # GlobeNewswire redirects keyword= to /en/search/keyword/ path;
        # use a simpler query to get better results
        url = GLOBENEWSWIRE_SEARCH.format(query=quote_plus(query))
        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return []

        return self._extract_globenewswire_links(response.text)

    async def _search_globenewswire_firm(self, firm_name: str) -> List[Dict[str, Any]]:
        """Search GlobeNewswire specifically for the firm name + deal terms."""
        query = f"{firm_name} acquisition investment"
        url = GLOBENEWSWIRE_SEARCH.format(query=quote_plus(query))
        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return []

        return self._extract_globenewswire_links(response.text)

    def _extract_pr_newswire_links(self, html: str) -> List[Dict[str, Any]]:
        """Extract links from PR Newswire search results."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.warning("BeautifulSoup not available for PR parsing")
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # PR Newswire uses 'search-result-item' divs with h3 > a for titles
        for item in soup.find_all("div", class_="search-result-item"):
            heading = item.find("h3")
            if heading:
                link = heading.find("a", href=True)
            else:
                link = item.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if href and title and "/news-releases/" in href:
                    releases.append({
                        "url": urljoin("https://www.prnewswire.com", href),
                        "title": title,
                        "source": "pr_newswire",
                    })

        return releases[:15]

    def _extract_business_wire_links(self, html: str) -> List[Dict[str, Any]]:
        """Extract links from Business Wire search results."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        for item in soup.find_all(
            ["div", "li"],
            class_=lambda x: x
            and any(kw in str(x).lower() for kw in ["result", "news", "headline"]),
        ):
            link = item.find("a", href=True)
            if link:
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if href and title:
                    releases.append({
                        "url": urljoin("https://www.businesswire.com", href),
                        "title": title,
                        "source": "business_wire",
                    })

        return releases[:15]

    def _extract_globenewswire_links(self, html: str) -> List[Dict[str, Any]]:
        """Extract links from GlobeNewswire search results."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # GlobeNewswire uses links with /news-release/ in the href
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            title = link.get_text(strip=True)
            if href and title and len(title) > 15 and "/news-release/" in href:
                releases.append({
                    "url": urljoin("https://www.globenewswire.com", href),
                    "title": title,
                    "source": "globenewswire",
                })

        return releases[:15]

    def _filter_deal_related(
        self, releases: List[Dict[str, Any]], firm_name: str
    ) -> List[Dict[str, Any]]:
        """Filter press releases to those likely about deals."""
        scored = []
        for pr in releases:
            title = (pr.get("title") or "").lower()
            source = pr.get("source", "")

            # SEC 8-K filings are already filtered by search query — keep them
            if source == "sec_8k":
                scored.append((2, pr))
                continue

            firm_words = firm_name.lower().split()
            firm_match = any(w in title for w in firm_words if len(w) > 3)
            deal_match = any(kw in title for kw in DEAL_KEYWORDS)

            if firm_match and deal_match:
                scored.append((3, pr))  # Best: firm + deal keyword
            elif deal_match:
                scored.append((1, pr))  # Deal keyword only (search was firm-specific)
            elif firm_match:
                scored.append((0, pr))  # Firm name only

        # Sort by score descending, return all
        scored.sort(key=lambda x: x[0], reverse=True)
        return [pr for _, pr in scored]

    async def _fetch_pr_text(self, url: str) -> Optional[str]:
        """Fetch and extract text content from a press release or SEC filing URL."""
        # For SEC filing index pages, find and fetch the actual document
        if "sec.gov/Archives/edgar/data/" in url:
            return await self._fetch_sec_8k_text(url)

        response = await self._fetch_url(url)
        if not response or response.status_code != 200:
            return None

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Remove script/style elements
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        # Try to find the article body
        article = (
            soup.find("article")
            or soup.find("div", class_=re.compile(r"release|article|body|content", re.I))
            or soup.find("main")
        )
        if article:
            return article.get_text(separator="\n", strip=True)

        return soup.get_text(separator="\n", strip=True)

    async def _fetch_sec_8k_text(self, index_url: str) -> Optional[str]:
        """Fetch the primary document from a SEC 8-K filing index page."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return None

        response = await self._fetch_url(
            index_url, headers={"User-Agent": settings.sec_user_agent}
        )
        if not response or response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find the primary document link — typically the largest .htm file
        # SEC index pages list files in a table
        doc_links = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if href.endswith((".htm", ".html", ".txt")) and "primary_doc" not in href.lower():
                doc_links.append(href)

        if not doc_links:
            return None

        # Fetch the first/primary document
        doc_url = doc_links[0]
        if not doc_url.startswith("http"):
            doc_url = "https://www.sec.gov" + doc_url

        doc_response = await self._fetch_url(
            doc_url, headers={"User-Agent": settings.sec_user_agent}
        )
        if not doc_response or doc_response.status_code != 200:
            return None

        doc_soup = BeautifulSoup(doc_response.text, "html.parser")
        for tag in doc_soup(["script", "style"]):
            tag.decompose()

        return doc_soup.get_text(separator="\n", strip=True)

    async def _extract_deal_with_llm(
        self, llm_client, text: str, firm_name: str
    ) -> Optional[Dict[str, Any]]:
        """Use LLM to extract structured deal data from press release text."""
        prompt = DEAL_EXTRACTION_PROMPT.format(firm_name=firm_name, text=text)

        try:
            response = await llm_client.complete(
                prompt=prompt,
                system_prompt="You are a financial analyst extracting PE deal data. Return only valid JSON.",
                json_mode=True,
            )
            return response.parse_json()
        except Exception as e:
            logger.warning(f"LLM extraction failed: {e}")
            return None

    def _build_deal_item(
        self,
        deal_data: Dict[str, Any],
        entity_id: int,
        entity_name: str,
        source_url: str,
        pr_title: Optional[str],
    ) -> Optional[PECollectedItem]:
        """Build a PECollectedItem from LLM-extracted deal data."""
        target = deal_data.get("target_company")
        deal_type = deal_data.get("deal_type", "Other")

        deal_name = f"{entity_name} - {target}" if target else pr_title or "Unknown Deal"

        return self._create_item(
            item_type="deal",
            data={
                "firm_id": entity_id,
                "firm_name": entity_name,
                "deal_name": deal_name,
                "deal_type": deal_type,
                "target_company": target,
                "target_description": deal_data.get("target_description"),
                "enterprise_value_usd": deal_data.get("enterprise_value_usd"),
                "announced_date": deal_data.get("announced_date"),
                "closed_date": deal_data.get("closed_date"),
                "co_investors": deal_data.get("co_investors", []),
                "seller": deal_data.get("seller"),
                "description": deal_data.get("description"),
                "pr_title": pr_title,
            },
            source_url=source_url,
            confidence="llm_extracted",
        )
