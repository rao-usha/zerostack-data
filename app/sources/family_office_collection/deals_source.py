"""
Deals collector for Family Office investment tracking.

Collects investment/deal data from:
- News articles (Google News, press releases)
- Crunchbase-style deal databases (public)
- SEC filings (Form D for private placements)

Extracts:
- Company name and website
- Investment amount and stage
- Investment type (equity, debt, real estate, etc.)
- Lead investor status
- Deal date
"""

import logging
import re
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.types import (
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
)

logger = logging.getLogger(__name__)


# Investment stage patterns
STAGE_PATTERNS = {
    "seed": r"\b(seed|pre-seed|angel)\b",
    "series_a": r"\b(series[\s-]?a)\b",
    "series_b": r"\b(series[\s-]?b)\b",
    "series_c": r"\b(series[\s-]?c)\b",
    "series_d": r"\b(series[\s-]?d|series[\s-]?e|late[\s-]?stage)\b",
    "growth": r"\b(growth[\s-]?equity|growth[\s-]?round|expansion)\b",
    "buyout": r"\b(buyout|acquisition|acquired|lbo)\b",
    "real_estate": r"\b(real[\s-]?estate|property|reit)\b",
    "debt": r"\b(debt|loan|credit[\s-]?facility|mezzanine)\b",
    "secondary": r"\b(secondary|tender[\s-]?offer)\b",
}

# Investment type patterns
TYPE_PATTERNS = {
    "venture": r"\b(venture|vc|startup|series[\s-]?[a-e]|seed)\b",
    "private_equity": r"\b(private[\s-]?equity|pe[\s-]?fund|buyout|lbo)\b",
    "real_estate": r"\b(real[\s-]?estate|property|reit|commercial[\s-]?property)\b",
    "hedge_fund": r"\b(hedge[\s-]?fund|fund[\s-]?of[\s-]?funds)\b",
    "direct_investment": r"\b(direct[\s-]?investment|co[\s-]?invest|co[\s-]?investment)\b",
    "credit": r"\b(credit|debt|loan|fixed[\s-]?income|mezzanine)\b",
    "infrastructure": r"\b(infrastructure|infra|utilities)\b",
    "public_equity": r"\b(public[\s-]?equity|stock|shares|nasdaq|nyse)\b",
}

# Amount extraction patterns
AMOUNT_PATTERNS = [
    r"\$(\d+(?:\.\d+)?)\s*(billion|b)\b",  # $X billion
    r"\$(\d+(?:\.\d+)?)\s*(million|m|mn)\b",  # $X million
    r"(\d+(?:\.\d+)?)\s*(billion|b)\s*(?:dollars?|usd)",  # X billion dollars
    r"(\d+(?:\.\d+)?)\s*(million|m|mn)\s*(?:dollars?|usd)",  # X million dollars
]


class FoDealsCollector(FoBaseCollector):
    """
    Collects family office deal and investment activity.

    Uses multiple sources:
    1. News search (Google News RSS)
    2. Press release aggregators
    3. SEC EDGAR Form D filings

    Extracts structured deal data using pattern matching and LLM.
    """

    # OpenAI API for deal extraction (optional)
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

    @property
    def source_type(self) -> FoCollectionSource:
        return FoCollectionSource.DEALS

    async def collect(
        self,
        fo_id: int,
        fo_name: str,
        website_url: Optional[str] = None,
        principal_name: Optional[str] = None,
        principal_family: Optional[str] = None,
        days_back: int = 180,
        **kwargs,
    ) -> FoCollectionResult:
        """
        Collect deal/investment data for a family office.

        Args:
            fo_id: Family office ID
            fo_name: Family office name
            website_url: FO website (for context)
            principal_name: Principal's name for additional search
            principal_family: Family name for additional search
            days_back: How many days back to search (default 6 months)

        Returns:
            FoCollectionResult with deal items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[FoCollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting deals for {fo_name}")

        try:
            # 1. Search news for deal announcements
            news_deals = await self._search_news_deals(fo_name, days_back)
            items.extend(news_deals)

            # 2. Search for principal/family if provided
            if principal_name and principal_name.lower() != fo_name.lower():
                principal_deals = await self._search_news_deals(
                    f'"{principal_name}" investment',
                    days_back
                )
                items.extend(self._deduplicate_deals(principal_deals, items))

            if principal_family and principal_family.lower() != fo_name.lower():
                family_deals = await self._search_news_deals(
                    f'"{principal_family}" family office investment',
                    days_back
                )
                items.extend(self._deduplicate_deals(family_deals, items))

            # 3. Search SEC Form D for private placements
            sec_deals = await self._search_sec_form_d(fo_name)
            items.extend(sec_deals)

            # 4. Enrich items with FO context
            for item in items:
                item.data["fo_id"] = fo_id
                item.data["fo_name"] = fo_name

            success = len(items) > 0
            if not items:
                warnings.append("No deals found in search period")

            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting deals for {fo_name}: {e}")
            return self._create_result(
                fo_id=fo_id,
                fo_name=fo_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _search_news_deals(
        self,
        query: str,
        days_back: int = 180,
    ) -> List[FoCollectedItem]:
        """Search news for deal announcements."""
        items = []

        # Search terms that indicate deal activity
        deal_queries = [
            f'"{query}" investment',
            f'"{query}" invested',
            f'"{query}" backs',
            f'"{query}" leads round',
            f'"{query}" acquisition',
        ]

        seen_urls = set()

        for search_query in deal_queries:
            encoded_query = search_query.replace('"', "%22").replace(" ", "+")
            rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

            response = await self._fetch_url(rss_url)
            if not response or response.status_code != 200:
                continue

            # Parse RSS feed
            news_items = self._parse_deal_rss(response.text, query)

            # Filter by date and deduplicate
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            for item in news_items:
                if item.source_url not in seen_urls and self._item_is_recent(item, cutoff):
                    seen_urls.add(item.source_url)
                    items.append(item)

            # Rate limit between searches
            if len(items) >= 50:
                break

        return items[:50]  # Limit results

    def _parse_deal_rss(self, xml_content: str, search_query: str) -> List[FoCollectedItem]:
        """Parse Google News RSS feed for deal items."""
        items = []

        item_pattern = re.compile(
            r'<item>.*?<title>([^<]+)</title>.*?<link>([^<]+)</link>.*?<pubDate>([^<]+)</pubDate>.*?</item>',
            re.DOTALL
        )

        for match in item_pattern.finditer(xml_content):
            title = match.group(1).strip()
            link = match.group(2).strip()
            pub_date = match.group(3).strip()

            # Clean up title
            title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)
            title = title.replace("&amp;", "&").replace("&quot;", '"')

            # Extract deal details from title
            deal_data = self._extract_deal_from_title(title)

            if deal_data:
                deal_data["title"] = title
                deal_data["pub_date"] = pub_date
                deal_data["search_query"] = search_query

                items.append(FoCollectedItem(
                    item_type="deal",
                    data=deal_data,
                    source_url=link,
                    confidence=deal_data.get("confidence", "medium"),
                ))

        return items

    def _extract_deal_from_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Extract structured deal data from a news title."""
        title_lower = title.lower()

        # Must contain deal-related keywords
        deal_keywords = [
            "invest", "funded", "funding", "raise", "led",
            "acquired", "acquisition", "backs", "backed",
            "series", "round", "stake", "buys", "bought"
        ]

        if not any(kw in title_lower for kw in deal_keywords):
            return None

        deal_data: Dict[str, Any] = {
            "confidence": "medium",
        }

        # Extract investment amount
        amount_usd = self._extract_amount(title)
        if amount_usd:
            deal_data["investment_amount_usd"] = amount_usd
            deal_data["confidence"] = "high"

        # Extract investment stage
        stage = self._extract_stage(title_lower)
        if stage:
            deal_data["investment_stage"] = stage

        # Extract investment type
        inv_type = self._extract_type(title_lower)
        if inv_type:
            deal_data["investment_type"] = inv_type

        # Extract company name (challenging without full article)
        company = self._extract_company_name(title)
        if company:
            deal_data["company_name"] = company
            deal_data["confidence"] = "high"

        # Check if lead investor
        if "leads" in title_lower or "led" in title_lower:
            deal_data["lead_investor"] = True

        # Try to extract date from title
        deal_date = self._extract_deal_date(title)
        if deal_date:
            deal_data["investment_date"] = deal_date

        return deal_data

    def _extract_amount(self, text: str) -> Optional[float]:
        """Extract investment amount in USD."""
        text_lower = text.lower()

        for pattern in AMOUNT_PATTERNS:
            match = re.search(pattern, text_lower)
            if match:
                amount = float(match.group(1))
                unit = match.group(2).lower()

                if unit in ("billion", "b"):
                    return amount * 1_000_000_000
                elif unit in ("million", "m", "mn"):
                    return amount * 1_000_000

        return None

    def _extract_stage(self, text_lower: str) -> Optional[str]:
        """Extract investment stage."""
        for stage, pattern in STAGE_PATTERNS.items():
            if re.search(pattern, text_lower):
                return stage
        return None

    def _extract_type(self, text_lower: str) -> Optional[str]:
        """Extract investment type."""
        for inv_type, pattern in TYPE_PATTERNS.items():
            if re.search(pattern, text_lower):
                return inv_type
        return None

    def _extract_company_name(self, title: str) -> Optional[str]:
        """Extract company name from deal headline."""
        # Common patterns:
        # "[Company] raises $X in Series A"
        # "[Investor] invests in [Company]"
        # "[Investor] leads $X round in [Company]"
        # "[Company] secures $X from [Investor]"

        patterns = [
            # Company raises...
            r"^([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]*)*)\s+(?:raises?|secures?|closes?|announces?)",
            # invests in Company
            r"(?:invests?\s+(?:\$[\d.]+\s*(?:million|billion|m|b)\s+)?in|backs?|leads?\s+.*?\s+in)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]*)*)",
            # acquires Company
            r"(?:acquires?|buys?|purchases?)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]*)*)",
        ]

        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                company = match.group(1).strip()
                # Filter out common false positives
                if company.lower() not in [
                    "the", "a", "series", "family", "office", "new",
                    "this", "that", "its", "their", "our", "we"
                ]:
                    return company

        return None

    def _extract_deal_date(self, text: str) -> Optional[str]:
        """Extract deal date from text."""
        # Look for month/year patterns
        month_pattern = r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})"
        match = re.search(month_pattern, text.lower())
        if match:
            month_name = match.group(1)
            year = match.group(2)
            months = {
                "january": "01", "february": "02", "march": "03",
                "april": "04", "may": "05", "june": "06",
                "july": "07", "august": "08", "september": "09",
                "october": "10", "november": "11", "december": "12"
            }
            return f"{year}-{months[month_name]}-01"

        return None

    def _item_is_recent(self, item: FoCollectedItem, cutoff: datetime) -> bool:
        """Check if a news item is recent enough."""
        pub_date = item.data.get("pub_date")
        if not pub_date:
            return True

        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(pub_date)
            return dt.replace(tzinfo=None) >= cutoff
        except Exception:
            return True

    def _deduplicate_deals(
        self,
        new_items: List[FoCollectedItem],
        existing_items: List[FoCollectedItem],
    ) -> List[FoCollectedItem]:
        """Remove duplicate deals based on URL and company name."""
        existing_urls = {i.source_url for i in existing_items}
        existing_companies = {
            i.data.get("company_name", "").lower()
            for i in existing_items
            if i.data.get("company_name")
        }

        unique = []
        for item in new_items:
            if item.source_url in existing_urls:
                continue

            company = item.data.get("company_name", "").lower()
            if company and company in existing_companies:
                continue

            unique.append(item)

        return unique

    async def _search_sec_form_d(self, fo_name: str) -> List[FoCollectedItem]:
        """
        Search SEC EDGAR for Form D filings mentioning the family office.

        Form D is filed for private placements (Reg D offerings).
        This can reveal private investments the FO has participated in.
        """
        items = []

        # SEC EDGAR full-text search
        # Note: This is a simplified approach - a full implementation
        # would use the SEC EDGAR API more comprehensively
        encoded_name = fo_name.replace(" ", "+")
        search_url = f"https://efts.sec.gov/LATEST/search-index?q=%22{encoded_name}%22&dateRange=custom&startdt=2023-01-01&forms=D"

        try:
            response = await self._fetch_json(search_url)
            if not response:
                return items

            hits = response.get("hits", {}).get("hits", [])

            for hit in hits[:20]:  # Limit results
                source = hit.get("_source", {})

                # Extract filing details
                filing_data = {
                    "company_name": source.get("display_names", ["Unknown"])[0],
                    "investment_type": "venture",  # Form D typically indicates private placement
                    "investment_stage": self._infer_stage_from_form_d(source),
                    "source_type": "sec_form_d",
                    "filing_date": source.get("file_date"),
                    "form_type": source.get("form"),
                    "cik": source.get("ciks", [None])[0],
                }

                # Try to extract amount from filing
                amount = self._extract_form_d_amount(source)
                if amount:
                    filing_data["investment_amount_usd"] = amount

                items.append(FoCollectedItem(
                    item_type="deal",
                    data=filing_data,
                    source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={filing_data['cik']}&type=D",
                    confidence="high",  # SEC filings are authoritative
                ))

        except Exception as e:
            logger.warning(f"Error searching SEC EDGAR for {fo_name}: {e}")

        return items

    def _infer_stage_from_form_d(self, source: Dict[str, Any]) -> Optional[str]:
        """Infer investment stage from Form D filing details."""
        # Form D doesn't directly indicate stage, but we can infer
        # from the issuer type and other details
        return "venture"  # Default for Form D

    def _extract_form_d_amount(self, source: Dict[str, Any]) -> Optional[float]:
        """Extract offering amount from Form D filing."""
        # This would require parsing the actual Form D XML
        # Simplified implementation returns None
        return None


# Alias for backward compatibility
DealsCollector = FoDealsCollector
