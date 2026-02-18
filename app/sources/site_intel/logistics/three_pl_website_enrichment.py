"""
3PL Company Enrichment Collector - Website Scraping.

Extracts company metadata (HQ, employees, facilities, services) directly
from each 3PL company's About/Contact pages.

Pipeline:
1. Page Discovery - try common About page URL patterns
2. HTML Fetching & Cleaning - strip nav/footer/scripts
3. Structured Extraction - regex patterns for key fields
4. LLM Fallback - GPT-4o-mini when structured extraction fails
5. Storage - null_preserving_upsert to preserve authoritative data
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any

import httpx

from app.core.models_site_intel import ThreePLCompany
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# Common About page URL paths to try
ABOUT_PAGE_PATHS = [
    "/about",
    "/about-us",
    "/company",
    "/who-we-are",
    "/our-company",
    "/about/overview",
    "/company/about",
    "/about/company",
]

CONTACT_PAGE_PATHS = [
    "/contact",
    "/contact-us",
]

# Regex patterns for structured extraction
EMPLOYEE_PATTERNS = [
    re.compile(
        r"([\d,]+)\s*(?:\+\s*)?(?:employees|team members|associates|workers|people)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:more than|over|approximately|nearly)\s*([\d,]+)\s*(?:employees|team members|associates)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:workforce|staff|team)\s+(?:of\s+)?(?:more than\s+|over\s+)?([\d,]+)",
        re.IGNORECASE,
    ),
]

HQ_PATTERNS = [
    re.compile(
        r"(?:headquartered|based|located)\s+in\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+([A-Z]{2})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:headquarters|head office|corporate office|main office)\s*(?:in|:)?\s*([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+([A-Z]{2})",
        re.IGNORECASE,
    ),
]

FACILITY_PATTERNS = [
    re.compile(
        r"([\d,]+)\s*(?:\+\s*)?(?:facilities|locations|warehouses|offices|terminals|service centers|distribution centers)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:more than|over|approximately)\s*([\d,]+)\s*(?:facilities|locations|warehouses)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:network of|operates)\s*([\d,]+)\s*(?:facilities|locations|warehouses)",
        re.IGNORECASE,
    ),
]

FOUNDED_PATTERNS = [
    re.compile(r"(?:founded|established|since)\s+(?:in\s+)?(\d{4})", re.IGNORECASE),
]

# US state abbreviations for validation
US_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}


def _clean_html(html: str) -> str:
    """Strip navigation, footer, scripts, and tags from HTML. Return plain text."""
    # Remove script and style blocks
    html = re.sub(
        r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE
    )
    html = re.sub(
        r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE
    )
    # Remove nav and footer
    html = re.sub(r"<nav[^>]*>.*?</nav>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(
        r"<footer[^>]*>.*?</footer>", " ", html, flags=re.DOTALL | re.IGNORECASE
    )
    html = re.sub(
        r"<header[^>]*>.*?</header>", " ", html, flags=re.DOTALL | re.IGNORECASE
    )
    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Decode common entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&#39;", "'").replace("&quot;", '"')
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_number(s: str) -> Optional[int]:
    """Parse a number string like '14,695' to int."""
    try:
        return int(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


@register_collector(SiteIntelSource.THREE_PL_WEBSITE)
class ThreePLWebsiteEnrichmentCollector(BaseCollector):
    """
    Enrichment collector that scrapes company websites for metadata.

    For each 3PL company with a website URL in the DB:
    1. Discover About/Contact pages
    2. Fetch and clean HTML
    3. Extract fields via regex (structured first)
    4. Fall back to GPT-4o-mini if structured extraction fails
    5. Store via null_preserving_upsert
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.THREE_PL_WEBSITE

    rate_limit_delay = 2.0  # 0.5 req/sec per domain — respectful scraping

    def get_default_base_url(self) -> str:
        return ""  # Per-company URLs

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (compatible; NexdataBot/1.0; +https://nexdata.io/bot)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Scrape company websites to enrich 3PL company records."""
        try:
            # Load companies that have a website URL
            companies = (
                self.db.query(ThreePLCompany)
                .filter(ThreePLCompany.website.isnot(None))
                .all()
            )

            if not companies:
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    error_message="No 3PL companies with website URLs to scrape",
                )

            logger.info(f"Scraping websites for {len(companies)} 3PL companies...")

            records = []
            errors = []
            processed = 0

            # Create a dedicated client for website scraping
            async with httpx.AsyncClient(
                headers=self.get_default_headers(),
                timeout=10.0,
                follow_redirects=True,
            ) as client:
                for company in companies:
                    processed += 1
                    try:
                        record = await self._scrape_company(client, company)
                        if record:
                            records.append(record)
                            logger.debug(
                                f"Enriched {company.company_name} from website "
                                f"({len(record) - 3} fields)"  # minus name, source, collected_at
                            )

                        self.update_progress(
                            processed=processed,
                            total=len(companies),
                            current_step=f"Scraping {company.company_name}",
                        )

                    except Exception as e:
                        logger.debug(
                            f"Website scrape failed for {company.company_name}: {e}"
                        )
                        errors.append(
                            {
                                "company": company.company_name,
                                "website": company.website,
                                "error": str(e),
                            }
                        )

            logger.info(
                f"Scraped {len(records)}/{len(companies)} company websites, "
                f"{len(errors)} errors"
            )

            if records:
                # Ensure all records have consistent keys for batch INSERT
                all_keys = [
                    "company_name",
                    "headquarters_city",
                    "headquarters_state",
                    "employee_count",
                    "facility_count",
                    "source",
                    "collected_at",
                ]
                for rec in records:
                    for key in all_keys:
                        rec.setdefault(key, None)

                inserted, updated = self.null_preserving_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "headquarters_city",
                        "headquarters_state",
                        "employee_count",
                        "facility_count",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS
                    if not errors
                    else CollectionStatus.PARTIAL,
                    total=len(companies),
                    processed=processed,
                    inserted=inserted,
                    updated=updated,
                    failed=len(errors),
                    errors=errors if errors else None,
                    sample=records[:3],
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(companies),
                processed=processed,
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Website enrichment failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    def _get_base_domain(self, website: str) -> str:
        """Extract base domain URL (scheme + host) from a full website URL."""
        from urllib.parse import urlparse

        parsed = urlparse(website)
        return f"{parsed.scheme}://{parsed.netloc}"

    async def _scrape_company(
        self, client: httpx.AsyncClient, company: ThreePLCompany
    ) -> Optional[Dict[str, Any]]:
        """Scrape a single company's website for metadata."""
        base_url = self._get_base_domain(company.website)

        # 1. Discover valid About/Contact pages
        about_text = ""
        contact_text = ""

        # Try About pages
        for path in ABOUT_PAGE_PATHS:
            url = f"{base_url}{path}"
            text = await self._fetch_page_text(client, url)
            if text and len(text) > 200:
                about_text = text
                break
            await asyncio.sleep(self.rate_limit_delay)

        # Try Contact pages (for HQ address)
        if not about_text or not self._has_hq_info(about_text):
            for path in CONTACT_PAGE_PATHS:
                url = f"{base_url}{path}"
                text = await self._fetch_page_text(client, url)
                if text and len(text) > 100:
                    contact_text = text
                    break
                await asyncio.sleep(self.rate_limit_delay)

        # If no About page found, try the original website URL (may include path)
        if not about_text:
            homepage_url = company.website.rstrip("/")
            about_text = await self._fetch_page_text(client, homepage_url) or ""
            await asyncio.sleep(self.rate_limit_delay)

        combined_text = f"{about_text} {contact_text}".strip()
        if not combined_text or len(combined_text) < 100:
            return None

        # 2. Structured extraction via regex
        record = {"company_name": company.company_name}
        extracted = self._extract_structured(combined_text)
        record.update(extracted)

        # 3. LLM fallback if structured extraction got less than 2 fields
        if len(extracted) < 2:
            llm_data = await self._extract_with_llm(combined_text, company.company_name)
            if llm_data:
                # Only add LLM fields that weren't found by structured extraction
                for key, value in llm_data.items():
                    if key not in record or record[key] is None:
                        record[key] = value

        # Only return if we got meaningful data
        data_fields = {
            k: v for k, v in record.items() if k != "company_name" and v is not None
        }
        if not data_fields:
            return None

        record["source"] = "company_website"
        record["collected_at"] = datetime.utcnow()
        return record

    async def _fetch_page_text(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[str]:
        """Fetch a page and return cleaned text, or None if not found."""
        try:
            response = await client.get(url)
            if response.status_code != 200:
                return None
            content_type = response.headers.get("content-type", "")
            if (
                "text/html" not in content_type
                and "application/xhtml" not in content_type
            ):
                return None
            return _clean_html(response.text)
        except (httpx.RequestError, httpx.HTTPStatusError):
            return None

    def _has_hq_info(self, text: str) -> bool:
        """Check if text contains HQ location information."""
        for pattern in HQ_PATTERNS:
            if pattern.search(text):
                return True
        return False

    def _extract_structured(self, text: str) -> Dict[str, Any]:
        """Extract metadata from page text using regex patterns."""
        result = {}

        # Employee count
        for pattern in EMPLOYEE_PATTERNS:
            match = pattern.search(text)
            if match:
                count = _parse_number(match.group(1))
                if count and 10 <= count <= 500000:
                    result["employee_count"] = count
                    break

        # HQ location
        for pattern in HQ_PATTERNS:
            match = pattern.search(text)
            if match:
                city = match.group(1).strip()
                state = match.group(2).upper().strip()
                if state in US_STATES and len(city) > 1:
                    result["headquarters_city"] = city.title()
                    result["headquarters_state"] = state
                    break

        # Facility count
        for pattern in FACILITY_PATTERNS:
            match = pattern.search(text)
            if match:
                count = _parse_number(match.group(1))
                if count and 1 <= count <= 10000:
                    result["facility_count"] = count
                    break

        return result

    async def _extract_with_llm(
        self, text: str, company_name: str
    ) -> Optional[Dict[str, Any]]:
        """Use GPT-4o-mini to extract metadata from page text."""
        try:
            import openai
        except ImportError:
            logger.debug("openai package not available, skipping LLM extraction")
            return None

        try:
            import os

            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                logger.debug("No OPENAI_API_KEY set, skipping LLM extraction")
                return None

            client = openai.AsyncOpenAI(api_key=api_key)

            # Truncate text to keep costs low
            truncated = text[:4000]

            prompt = f"""Extract the following fields from this company's About page for "{company_name}".
Return ONLY a JSON object with these fields (use null if not found):

{{
  "employee_count": <integer or null>,
  "headquarters_city": "<city name or null>",
  "headquarters_state": "<2-letter US state code or null>",
  "facility_count": <integer or null>
}}

Page text:
{truncated}"""

            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=200,
            )

            # Track LLM cost
            try:
                from app.core.llm_cost_tracker import get_cost_tracker

                tracker = get_cost_tracker()
                in_tok = response.usage.prompt_tokens if response.usage else 0
                out_tok = response.usage.completion_tokens if response.usage else 0
                await tracker.record(
                    model="gpt-4o-mini",
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                    source="3pl_enrichment",
                    prompt_chars=len(prompt),
                )
            except Exception as track_err:
                logger.debug(f"[3PL Enrichment] Cost tracking failed: {track_err}")

            content = response.choices[0].message.content.strip()
            # Extract JSON from response (handle markdown code blocks)
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            data = json.loads(content)
            result = {}

            if data.get("employee_count") and isinstance(data["employee_count"], int):
                if 10 <= data["employee_count"] <= 500000:
                    result["employee_count"] = data["employee_count"]

            if data.get("headquarters_city") and data.get("headquarters_state"):
                state = str(data["headquarters_state"]).upper()
                if state in US_STATES:
                    result["headquarters_city"] = str(data["headquarters_city"]).title()
                    result["headquarters_state"] = state

            if data.get("facility_count") and isinstance(data["facility_count"], int):
                if 1 <= data["facility_count"] <= 10000:
                    result["facility_count"] = data["facility_count"]

            return result if result else None

        except Exception as e:
            logger.debug(f"LLM extraction failed for {company_name}: {e}")
            return None
