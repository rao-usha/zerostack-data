"""
Greenhouse public job board API client.

API docs: https://developers.greenhouse.io/job-board.html
No authentication required for public boards.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class GreenhouseClient:
    BASE_URL = "https://boards-api.greenhouse.io/v1/boards"

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

    async def fetch_jobs(self, board_token: str) -> list[dict]:
        """Fetch all jobs from a Greenhouse board. Returns raw job dicts."""
        client = await self._get_client()
        url = f"{self.BASE_URL}/{board_token}/jobs"
        resp = await client.get(url, params={"content": "true"})
        resp.raise_for_status()
        data = resp.json()
        return data.get("jobs", [])

    async def fetch_departments(self, board_token: str) -> list[dict]:
        client = await self._get_client()
        url = f"{self.BASE_URL}/{board_token}/departments"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json().get("departments", [])

    async def fetch_offices(self, board_token: str) -> list[dict]:
        client = await self._get_client()
        url = f"{self.BASE_URL}/{board_token}/offices"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json().get("offices", [])

    def normalize_job(self, raw: dict, board_token: str) -> dict:
        """Map Greenhouse fields to our unified schema."""
        location = raw.get("location", {})
        location_name = location.get("name", "") if isinstance(location, dict) else str(location)

        departments = raw.get("departments", [])
        dept_name = departments[0].get("name") if departments else None

        # Salary from pay transparency fields (if available)
        salary_min = salary_max = salary_currency = salary_interval = None
        pay_ranges = raw.get("pay_input_ranges", [])
        if pay_ranges:
            pr = pay_ranges[0]
            salary_min = pr.get("min_cents", 0) / 100 if pr.get("min_cents") else None
            salary_max = pr.get("max_cents", 0) / 100 if pr.get("max_cents") else None
            salary_currency = pr.get("currency_type", "USD")
            salary_interval = pr.get("pay_period", "yearly")

        return {
            "external_job_id": str(raw.get("id", "")),
            "title": raw.get("title", ""),
            "department": dept_name,
            "location": location_name,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": salary_currency,
            "salary_interval": salary_interval,
            "description_text": raw.get("content", ""),
            "source_url": raw.get("absolute_url", ""),
            "ats_type": "greenhouse",
            "posted_date": raw.get("updated_at"),
        }
