"""
Lever public postings API client.

API: GET https://api.lever.co/v0/postings/{site}?mode=json
No authentication required.  10 req/sec limit.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class LeverClient:
    BASE_URL = "https://api.lever.co/v0/postings"

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

    async def fetch_jobs(self, site_name: str) -> list[dict]:
        """Fetch all postings from a Lever site. Handles pagination."""
        client = await self._get_client()
        all_jobs: list[dict] = []
        skip = 0
        limit = 100

        while True:
            url = f"{self.BASE_URL}/{site_name}"
            resp = await client.get(url, params={"mode": "json", "skip": skip, "limit": limit})
            if resp.status_code == 404:
                logger.warning(f"Lever site '{site_name}' not found (404)")
                return []
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            all_jobs.extend(batch)
            if len(batch) < limit:
                break
            skip += limit

        return all_jobs

    @staticmethod
    def _epoch_ms_to_iso(epoch_ms) -> str | None:
        """Convert epoch milliseconds to ISO 8601 string."""
        if not epoch_ms:
            return None
        try:
            from datetime import datetime, timezone
            ts = int(epoch_ms) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (ValueError, TypeError, OverflowError):
            return None

    def normalize_job(self, raw: dict, site_name: str) -> dict:
        """Map Lever fields to our unified schema."""
        categories = raw.get("categories", {})

        # Salary range (Lever exposes this in some postings)
        salary_range = raw.get("salaryRange", {}) or {}
        salary_min = salary_range.get("min")
        salary_max = salary_range.get("max")
        salary_currency = salary_range.get("currency", "USD") if salary_range else None
        salary_interval = salary_range.get("interval", "yearly") if salary_range else None

        # Workplace type
        workplace = raw.get("workplaceType")

        return {
            "external_job_id": raw.get("id", ""),
            "title": raw.get("text", ""),
            "department": categories.get("department"),
            "team": categories.get("team"),
            "location": categories.get("location"),
            "employment_type": categories.get("commitment"),
            "workplace_type": workplace,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": salary_currency,
            "salary_interval": salary_interval,
            "description_text": raw.get("descriptionPlain", ""),
            "source_url": raw.get("hostedUrl", ""),
            "ats_type": "lever",
            "posted_date": self._epoch_ms_to_iso(raw.get("createdAt")),
        }
