"""
ATS (Applicant Tracking System) detector.

Discovers which ATS a company uses from their careers URL or website.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
REQUEST_TIMEOUT = 10.0


@dataclass
class ATSResult:
    ats_type: str  # greenhouse / lever / ashby / workday / smartrecruiters / generic / unknown
    board_token: Optional[str] = None
    careers_url: Optional[str] = None
    api_url: Optional[str] = None
    error: Optional[str] = None

    @property
    def is_known(self) -> bool:
        return self.ats_type not in ("unknown", "generic")


# URL regex patterns -> (ats_type, capture-group = board_token)
ATS_URL_PATTERNS: dict[str, list[str]] = {
    "greenhouse": [
        r"boards\.greenhouse\.io/([\w-]+)",
        r"job-boards\.greenhouse\.io/([\w-]+)",
    ],
    "lever": [
        r"jobs\.lever\.co/([\w-]+)",
        r"api\.lever\.co/v0/postings/([\w-]+)",
    ],
    "ashby": [
        r"jobs\.ashbyhq\.com/([\w-]+)",
    ],
    "workday": [
        r"([\w-]+)\.wd([1-5])\.myworkdayjobs\.com",
    ],
    "smartrecruiters": [
        r"jobs\.smartrecruiters\.com/([\w-]+)",
    ],
}

# HTML signatures: (substring in page, ats_type)
ATS_HTML_SIGNATURES: list[tuple[str, str]] = [
    ("greenhouse.io", "greenhouse"),
    ("boards-api.greenhouse.io", "greenhouse"),
    ("lever.co", "lever"),
    ("jobs.lever.co", "lever"),
    ("ashbyhq.com", "ashby"),
    ("myworkdayjobs.com", "workday"),
    ("smartrecruiters.com", "smartrecruiters"),
    ("Powered by Greenhouse", "greenhouse"),
    ("Powered by Lever", "lever"),
    ("Powered by Ashby", "ashby"),
]

CAREER_PATH_SUFFIXES = [
    "/careers", "/jobs", "/join-us", "/open-positions", "/work-with-us",
    "/about/careers", "/company/careers", "/en/careers",
]


class ATSDetector:
    """Detect ATS type from a company's career page URL or HTML."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=5.0),
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
                limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def detect(
        self,
        company_name: str,
        website: Optional[str],
        careers_url: Optional[str] = None,
    ) -> ATSResult:
        """
        Detect ATS for a company.

        Strategy:
        1. If careers_url is provided, check URL patterns first
        2. Try common career URL paths on the website domain
        3. Match HTML signatures on the discovered page
        """
        try:
            # Phase 1: try the provided careers URL
            if careers_url:
                result = self._match_url_patterns(careers_url)
                if result:
                    ats_type, token = result
                    return ATSResult(
                        ats_type=ats_type,
                        board_token=token,
                        careers_url=careers_url,
                        api_url=self._build_api_url(ats_type, token),
                    )
                # URL didn't match a pattern — inspect the HTML
                html_result = await self._inspect_page_html(careers_url)
                if html_result:
                    ats_type, token = html_result
                    return ATSResult(
                        ats_type=ats_type,
                        board_token=token,
                        careers_url=careers_url,
                        api_url=self._build_api_url(ats_type, token),
                    )

            # Phase 2: discover careers URL from the website
            if website:
                discovered = await self._discover_careers_url(website)
                if discovered:
                    result = self._match_url_patterns(discovered)
                    if result:
                        ats_type, token = result
                        return ATSResult(
                            ats_type=ats_type,
                            board_token=token,
                            careers_url=discovered,
                            api_url=self._build_api_url(ats_type, token),
                        )
                    html_result = await self._inspect_page_html(discovered)
                    if html_result:
                        ats_type, token = html_result
                        return ATSResult(
                            ats_type=ats_type,
                            board_token=token,
                            careers_url=discovered,
                            api_url=self._build_api_url(ats_type, token),
                        )
                    # Found a careers page but can't identify ATS
                    return ATSResult(ats_type="generic", careers_url=discovered)

            return ATSResult(ats_type="unknown", error="No careers page found")

        except Exception as e:
            logger.warning(f"ATS detection failed for {company_name}: {e}")
            return ATSResult(ats_type="unknown", error=str(e))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _match_url_patterns(self, url: str) -> Optional[tuple[str, str]]:
        """Match URL against known ATS patterns. Returns (ats_type, token)."""
        for ats_type, patterns in ATS_URL_PATTERNS.items():
            for pattern in patterns:
                m = re.search(pattern, url, re.IGNORECASE)
                if m:
                    token = m.group(1)
                    return (ats_type, token)
        return None

    async def _discover_careers_url(self, website: str) -> Optional[str]:
        """Try common career URL patterns on the company domain."""
        if not website:
            return None

        parsed = urlparse(website if "://" in website else f"https://{website}")
        base_domain = parsed.netloc or parsed.path.split("/")[0]
        base_domain = base_domain.replace("www.", "")

        # Build candidate URLs
        candidates = []
        for suffix in CAREER_PATH_SUFFIXES:
            candidates.append(f"https://www.{base_domain}{suffix}")
            candidates.append(f"https://{base_domain}{suffix}")
        # ATS subdomain guesses
        candidates.append(f"https://careers.{base_domain}")
        candidates.append(f"https://jobs.{base_domain}")

        client = await self._get_client()

        # Check candidates concurrently (bounded)
        sem = asyncio.Semaphore(3)

        async def _check(url: str) -> Optional[str]:
            async with sem:
                try:
                    resp = await client.head(url)
                    if resp.status_code < 400:
                        return str(resp.url)  # follow redirects
                except Exception:
                    pass
            return None

        tasks = [_check(u) for u in candidates]
        results = await asyncio.gather(*tasks)
        for r in results:
            if r:
                return r
        return None

    async def _inspect_page_html(self, url: str) -> Optional[tuple[str, str]]:
        """Fetch page and look for ATS signatures in HTML."""
        try:
            client = await self._get_client()
            resp = await client.get(url)
            if resp.status_code >= 400:
                return None
            html = resp.text[:100_000]  # only scan first 100KB
            final_url = str(resp.url)

            # Check the final (redirected) URL for ATS patterns
            match = self._match_url_patterns(final_url)
            if match:
                return match

            # Look for known signatures in the HTML body
            html_lower = html.lower()
            for signature, ats_type in ATS_HTML_SIGNATURES:
                if signature.lower() in html_lower:
                    # Try to extract board token from iframes/links
                    token = self._extract_token_from_html(html, ats_type)
                    if token:
                        return (ats_type, token)
                    return (ats_type, "")

        except Exception as e:
            logger.debug(f"Failed to inspect {url}: {e}")
        return None

    def _extract_token_from_html(self, html: str, ats_type: str) -> Optional[str]:
        """Try to extract the board token from embedded iframes/scripts."""
        patterns = ATS_URL_PATTERNS.get(ats_type, [])
        for pattern in patterns:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def _build_api_url(self, ats_type: str, token: str) -> Optional[str]:
        """Build the API URL for a known ATS type."""
        if not token:
            return None
        builders = {
            "greenhouse": lambda t: f"https://boards-api.greenhouse.io/v1/boards/{t}/jobs?content=true",
            "lever": lambda t: f"https://api.lever.co/v0/postings/{t}?mode=json",
            "ashby": lambda t: f"https://api.ashbyhq.com/posting-api/job-board/{t}",
        }
        builder = builders.get(ats_type)
        return builder(token) if builder else None
