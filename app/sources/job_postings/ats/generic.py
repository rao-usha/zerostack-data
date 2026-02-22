"""
Generic job posting scraper for companies without a known ATS.

Tries structured data (JSON-LD), CSS selectors, then falls back to basic link extraction.
"""

import json
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"

# Common CSS-like patterns for job cards (used as regex on raw HTML)
JOB_LINK_PATTERNS = [
    # href containing job-related keywords + text
    r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
]

JOB_URL_KEYWORDS = [
    "job", "career", "position", "opening", "vacancy", "role",
    "apply", "requisition",
]


class GenericJobScraper:
    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0, connect=10.0),
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_jobs(self, careers_url: str) -> list[dict]:
        """
        Attempt to extract jobs from a generic careers page.

        1. Look for JSON-LD JobPosting structured data
        2. Extract job-like links from HTML
        """
        try:
            client = await self._get_client()
            resp = await client.get(careers_url)
            if resp.status_code >= 400:
                logger.warning(f"Generic scraper got {resp.status_code} from {careers_url}")
                return []
            html = resp.text
        except Exception as e:
            logger.warning(f"Generic scraper failed to fetch {careers_url}: {e}")
            return []

        # Try JSON-LD first
        jobs = self._extract_jsonld(html)
        if jobs:
            return jobs

        # Fall back to link extraction
        return self._extract_job_links(html, str(resp.url))

    def _extract_jsonld(self, html: str) -> list[dict]:
        """Extract JobPosting schema from JSON-LD script tags."""
        jobs = []
        for m in re.finditer(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            try:
                data = json.loads(m.group(1))
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict) and item.get("@type") == "JobPosting":
                        jobs.append(self._normalize_jsonld(item))
                    # Handle @graph wrapper
                    if isinstance(item, dict) and "@graph" in item:
                        for node in item["@graph"]:
                            if isinstance(node, dict) and node.get("@type") == "JobPosting":
                                jobs.append(self._normalize_jsonld(node))
            except (json.JSONDecodeError, TypeError):
                continue
        return jobs

    def _normalize_jsonld(self, item: dict) -> dict:
        """Normalize a JSON-LD JobPosting to our schema."""
        # Location
        location = ""
        loc_obj = item.get("jobLocation")
        if isinstance(loc_obj, dict):
            addr = loc_obj.get("address", {})
            if isinstance(addr, dict):
                parts = [addr.get("addressLocality", ""), addr.get("addressRegion", "")]
                location = ", ".join(p for p in parts if p)
        elif isinstance(loc_obj, list) and loc_obj:
            addr = loc_obj[0].get("address", {}) if isinstance(loc_obj[0], dict) else {}
            parts = [addr.get("addressLocality", ""), addr.get("addressRegion", "")]
            location = ", ".join(p for p in parts if p)

        # Salary
        salary = item.get("baseSalary", {}) or {}
        value = salary.get("value", {}) or {}
        salary_min = value.get("minValue") if isinstance(value, dict) else None
        salary_max = value.get("maxValue") if isinstance(value, dict) else None
        salary_currency = salary.get("currency")

        return {
            "external_job_id": item.get("identifier", {}).get("value", "") if isinstance(item.get("identifier"), dict) else str(hash(item.get("title", "")))[:12],
            "title": item.get("title", ""),
            "department": item.get("occupationalCategory", ""),
            "location": location,
            "employment_type": item.get("employmentType"),
            "description_text": item.get("description", ""),
            "source_url": item.get("url", ""),
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": salary_currency,
            "ats_type": "generic",
            "posted_date": item.get("datePosted"),
        }

    def _extract_job_links(self, html: str, base_url: str) -> list[dict]:
        """Extract job-like links from raw HTML as a last resort."""
        jobs = []
        seen_urls = set()

        for match in re.finditer(JOB_LINK_PATTERNS[0], html, re.DOTALL | re.IGNORECASE):
            href = match.group(1).strip()
            text = re.sub(r"<[^>]+>", "", match.group(2)).strip()

            if not text or len(text) < 3 or len(text) > 200:
                continue

            href_lower = href.lower()
            text_lower = text.lower()
            is_job_link = any(kw in href_lower or kw in text_lower for kw in JOB_URL_KEYWORDS)
            if not is_job_link:
                continue

            # Skip navigation/generic links
            if text_lower in ("apply", "apply now", "learn more", "view all", "see all jobs"):
                continue

            # Resolve relative URLs
            if href.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                href = f"{parsed.scheme}://{parsed.netloc}{href}"

            if href in seen_urls:
                continue
            seen_urls.add(href)

            jobs.append({
                "external_job_id": str(hash(href))[:12],
                "title": text,
                "source_url": href,
                "ats_type": "generic",
            })

        return jobs
