"""
Ashby public job board API client.

API: GET https://api.ashbyhq.com/posting-api/job-board/{boardName}
No authentication required.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class AshbyClient:
    BASE_URL = "https://api.ashbyhq.com/posting-api/job-board"

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_jobs(self, board_name: str) -> list[dict]:
        """Fetch all jobs from an Ashby board."""
        client = await self._get_client()
        url = f"{self.BASE_URL}/{board_name}"
        resp = await client.get(url, params={"includeCompensation": "true"})
        resp.raise_for_status()
        data = resp.json()
        return data.get("jobs", [])

    def normalize_job(self, raw: dict, board_name: str) -> dict:
        """Map Ashby fields to our unified schema."""
        compensation = raw.get("compensation", {}) or {}
        salary_min = compensation.get("min")
        salary_max = compensation.get("max")
        salary_currency = compensation.get("currency", "USD") if compensation else None
        salary_interval = compensation.get("interval", "yearly") if compensation else None

        return {
            "external_job_id": raw.get("id", ""),
            "title": raw.get("title", ""),
            "department": raw.get("department"),
            "team": raw.get("team"),
            "location": raw.get("location"),
            "employment_type": raw.get("employmentType"),
            "workplace_type": raw.get("workplaceType"),
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": salary_currency,
            "salary_interval": salary_interval,
            "description_text": raw.get("descriptionPlain", ""),
            "source_url": raw.get("jobUrl", ""),
            "ats_type": "ashby",
            "posted_date": raw.get("publishedAt"),
        }
