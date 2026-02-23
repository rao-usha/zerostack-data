"""
SmartRecruiters public posting API client.

API docs: https://developers.smartrecruiters.com/docs/posting-api
Public endpoint — no authentication required for listing postings.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class SmartRecruitersClient:
    BASE_URL = "https://api.smartrecruiters.com/v1/companies"

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

    async def fetch_jobs(self, company_identifier: str) -> list[dict]:
        """Fetch all postings from a SmartRecruiters company. Handles pagination."""
        client = await self._get_client()
        all_jobs: list[dict] = []
        offset = 0
        limit = 100  # max per page

        while True:
            url = f"{self.BASE_URL}/{company_identifier}/postings"
            resp = await client.get(url, params={"limit": limit, "offset": offset})
            resp.raise_for_status()
            data = resp.json()

            content = data.get("content", [])
            if not content:
                break
            all_jobs.extend(content)

            total = data.get("totalFound", 0)
            offset += limit
            if offset >= total:
                break

        return all_jobs

    def normalize_job(self, raw: dict, company_identifier: str) -> dict:
        """Map SmartRecruiters fields to our unified schema."""
        # Location
        loc = raw.get("location", {}) or {}
        location = loc.get("fullLocation", "")
        if not location:
            parts = [loc.get("city", ""), loc.get("region", "")]
            location = ", ".join(p for p in parts if p)

        # Workplace type from location flags
        workplace_type = None
        if loc.get("remote"):
            workplace_type = "remote"
        elif loc.get("hybrid"):
            workplace_type = "hybrid"
        elif location:
            workplace_type = "onsite"

        # Department
        dept = raw.get("department", {}) or {}
        department = dept.get("label")

        # Employment type
        toe = raw.get("typeOfEmployment", {}) or {}
        employment_type = toe.get("label")

        # Experience level -> seniority
        exp = raw.get("experienceLevel", {}) or {}
        experience_label = exp.get("label", "")

        # Function (broader category)
        func = raw.get("function", {}) or {}

        return {
            "external_job_id": str(raw.get("id", "")),
            "title": raw.get("name", ""),
            "department": department,
            "team": func.get("label"),
            "location": location,
            "employment_type": employment_type,
            "workplace_type": workplace_type,
            "description_text": "",  # Summary not in list response
            "source_url": f"https://jobs.smartrecruiters.com/{company_identifier}/{raw.get('id', '')}",
            "ats_type": "smartrecruiters",
            "posted_date": raw.get("releasedDate"),
        }
