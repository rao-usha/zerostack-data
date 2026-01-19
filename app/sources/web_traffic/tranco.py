"""
Tranco List client for domain rankings.

Tranco provides a research-oriented top sites ranking that combines
multiple sources (Alexa, Umbrella, Majestic, Quantcast).

Free to use, updated daily, no API key required.
"""

import io
import csv
import zipfile
import httpx
from typing import Optional
from datetime import datetime, timedelta


class TrancoClient:
    """Client for Tranco top 1M domain rankings."""

    LIST_URL = "https://tranco-list.eu/download/daily/top-1m.csv.zip"
    CACHE_TTL_HOURS = 24

    def __init__(self):
        self._rankings: dict[str, int] = {}
        self._domains: list[str] = []
        self._last_fetch: Optional[datetime] = None
        self._list_date: Optional[str] = None
        self.client = httpx.Client(timeout=60.0, follow_redirects=True)

    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid."""
        if not self._last_fetch:
            return False
        return datetime.now() - self._last_fetch < timedelta(hours=self.CACHE_TTL_HOURS)

    def _fetch_list(self) -> None:
        """Download and parse Tranco top 1M list."""
        if self._is_cache_valid():
            return

        try:
            response = self.client.get(self.LIST_URL)
            response.raise_for_status()

            # Extract CSV from ZIP
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f:
                    content = f.read().decode("utf-8")

            # Parse CSV: rank,domain
            self._rankings = {}
            self._domains = []
            reader = csv.reader(io.StringIO(content))
            for row in reader:
                if len(row) >= 2:
                    rank = int(row[0])
                    domain = row[1].lower().strip()
                    self._rankings[domain] = rank
                    self._domains.append(domain)

            self._last_fetch = datetime.now()
            self._list_date = datetime.now().strftime("%Y-%m-%d")

        except Exception as e:
            # If fetch fails but we have cached data, use it
            if self._rankings:
                return
            raise RuntimeError(f"Failed to fetch Tranco list: {e}")

    def get_rank(self, domain: str) -> Optional[int]:
        """
        Get rank for a domain.

        Args:
            domain: Domain name (e.g., 'google.com')

        Returns:
            Rank (1-based) or None if not in top 1M
        """
        self._fetch_list()
        domain = domain.lower().strip()

        # Try exact match
        if domain in self._rankings:
            return self._rankings[domain]

        # Try without www prefix
        if domain.startswith("www."):
            base_domain = domain[4:]
            if base_domain in self._rankings:
                return self._rankings[base_domain]
        else:
            # Try with www prefix
            www_domain = f"www.{domain}"
            if www_domain in self._rankings:
                return self._rankings[www_domain]

        return None

    def get_top_domains(self, limit: int = 100, offset: int = 0) -> list[dict]:
        """
        Get top ranked domains.

        Args:
            limit: Number of domains to return (max 1000)
            offset: Starting position (0-based)

        Returns:
            List of {rank, domain} dicts
        """
        self._fetch_list()
        limit = min(limit, 1000)
        end = min(offset + limit, len(self._domains))

        return [
            {"rank": offset + i + 1, "domain": self._domains[offset + i]}
            for i in range(end - offset)
        ]

    def search_domains(self, keyword: str, limit: int = 50) -> list[dict]:
        """
        Search domains containing keyword.

        Args:
            keyword: Keyword to search for in domain names
            limit: Maximum results to return

        Returns:
            List of {rank, domain} dicts matching the keyword
        """
        self._fetch_list()
        keyword = keyword.lower().strip()
        results = []

        for domain, rank in self._rankings.items():
            if keyword in domain:
                results.append({"rank": rank, "domain": domain})
                if len(results) >= limit:
                    break

        # Sort by rank
        results.sort(key=lambda x: x["rank"])
        return results[:limit]

    def get_domains_in_range(self, start_rank: int, end_rank: int) -> list[dict]:
        """
        Get domains within a rank range.

        Args:
            start_rank: Starting rank (1-based, inclusive)
            end_rank: Ending rank (1-based, inclusive)

        Returns:
            List of {rank, domain} dicts
        """
        self._fetch_list()
        start_idx = max(0, start_rank - 1)
        end_idx = min(end_rank, len(self._domains))

        return [
            {"rank": start_idx + i + 1, "domain": self._domains[start_idx + i]}
            for i in range(end_idx - start_idx)
        ]

    def get_stats(self) -> dict:
        """Get statistics about the Tranco list."""
        self._fetch_list()
        return {
            "total_domains": len(self._domains),
            "list_date": self._list_date,
            "last_updated": self._last_fetch.isoformat() if self._last_fetch else None,
            "cache_ttl_hours": self.CACHE_TTL_HOURS,
        }

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
