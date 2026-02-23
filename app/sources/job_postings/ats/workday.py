"""
Workday job scraper client.

Workday uses a JSON API at:
  POST https://{company}.wd{N}.myworkdayjobs.com/wday/cxs/{company}/{site}/jobs
"""

import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class WorkdayClient:
    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
                headers={
                    "User-Agent": USER_AGENT,
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_jobs(
        self, careers_url: str, board_token: Optional[str] = None
    ) -> list[dict]:
        """
        Fetch all jobs via Workday's internal JSON API.

        Parses company slug and site from the careers URL, then paginates
        through the /wday/cxs/ API.
        """
        slug, wd_instance, site = self._parse_workday_url(careers_url, board_token)
        if not slug:
            logger.warning(f"Could not parse Workday URL: {careers_url}")
            return []

        client = await self._get_client()
        base = f"https://{slug}.{wd_instance}.myworkdayjobs.com/wday/cxs/{slug}/{site}/jobs"

        all_jobs: list[dict] = []
        offset = 0
        limit = 20

        while True:
            payload = {
                "appliedFacets": {},
                "searchText": "",
                "limit": limit,
                "offset": offset,
            }
            try:
                resp = await client.post(base, json=payload)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"Workday fetch error at offset {offset}: {e}")
                break

            postings = data.get("jobPostings", [])
            if not postings:
                break
            all_jobs.extend(postings)
            total = data.get("total", 0)
            offset += limit
            if offset >= total:
                break

        return all_jobs

    def _parse_workday_url(
        self, url: str, token: Optional[str]
    ) -> tuple[Optional[str], str, str]:
        """Extract (company_slug, wd_instance, site_name) from a Workday URL."""
        # Pattern: {slug}.wd{N}.myworkdayjobs.com/{site} or .com/en/{site}
        m = re.search(
            r"([\w-]+)\.(wd[1-5])\.myworkdayjobs\.com(?:/(?:en/)?([\w-]+))?",
            url,
            re.IGNORECASE,
        )
        if m:
            slug = m.group(1)
            wd = m.group(2)
            site = m.group(3) or token or "External"
            return slug, wd, site
        # If token was passed from detection
        if token:
            m2 = re.search(r"([\w-]+)\.(wd[1-5])", url, re.IGNORECASE)
            if m2:
                return m2.group(1), m2.group(2), token
        return None, "wd5", "External"

    def normalize_job(self, raw: dict, company_slug: str) -> dict:
        """Map Workday fields to our unified schema."""
        title = raw.get("title", "")
        # Workday wraps title in a bulletFields list sometimes
        bullet_fields = raw.get("bulletFields", [])
        location = bullet_fields[0] if bullet_fields else None
        posted_on = bullet_fields[1] if len(bullet_fields) > 1 else None

        external_path = raw.get("externalPath", "")
        source_url = ""
        if external_path:
            source_url = f"https://{company_slug}.myworkdayjobs.com{external_path}"

        return {
            "external_job_id": raw.get("id", external_path),
            "title": title,
            "location": location,
            "source_url": source_url,
            "ats_type": "workday",
            "posted_date": posted_on,
        }
