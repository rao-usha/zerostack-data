"""
Web Traffic client with multi-provider support.

Supports:
- Tranco: Free domain rankings (top 1M)
- SimilarWeb: Full traffic data (requires paid API key)
"""

import os
import httpx
from typing import Optional
from datetime import datetime

from .tranco import TrancoClient


class WebTrafficClient:
    """Client for web traffic data from multiple providers."""

    SIMILARWEB_BASE_URL = "https://api.similarweb.com/v1"

    def __init__(self, similarweb_api_key: str = None):
        """
        Initialize client with optional SimilarWeb API key.

        Args:
            similarweb_api_key: SimilarWeb API key (optional)
        """
        self.similarweb_key = similarweb_api_key or os.getenv("SIMILARWEB_API_KEY")
        self.tranco = TrancoClient()
        self.http_client = httpx.Client(timeout=30.0)

    def get_available_providers(self) -> list[dict]:
        """
        Get list of available data providers.

        Returns:
            List of provider info dicts
        """
        providers = [
            {
                "name": "tranco",
                "available": True,
                "features": ["rankings", "search"],
                "description": "Top 1M domain rankings (free, updated daily)",
            }
        ]

        if self.similarweb_key:
            providers.append({
                "name": "similarweb",
                "available": True,
                "features": ["traffic", "sources", "geography", "history"],
                "description": "Full traffic analytics (requires API key)",
            })
        else:
            providers.append({
                "name": "similarweb",
                "available": False,
                "features": ["traffic", "sources", "geography", "history"],
                "description": "Full traffic analytics (API key not configured)",
            })

        return providers

    def get_domain_traffic(self, domain: str) -> dict:
        """
        Get traffic overview for a domain.

        Uses SimilarWeb if available, otherwise returns Tranco rank only.

        Args:
            domain: Domain name (e.g., 'google.com')

        Returns:
            Traffic metrics dict
        """
        domain = self._normalize_domain(domain)
        result = {
            "domain": domain,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
            "providers_used": [],
        }

        # Always get Tranco rank (free)
        tranco_rank = self.tranco.get_rank(domain)
        if tranco_rank:
            result["tranco_rank"] = tranco_rank
            result["providers_used"].append("tranco")

        # If SimilarWeb available, get detailed traffic
        if self.similarweb_key:
            try:
                sw_data = self._get_similarweb_traffic(domain)
                if sw_data:
                    result.update(sw_data)
                    result["providers_used"].append("similarweb")
            except Exception:
                pass  # Fall back to Tranco only

        # If no data from any provider
        if not result["providers_used"]:
            result["error"] = "Domain not found in any data source"
            result["tranco_rank"] = None

        return result

    def get_traffic_history(self, domain: str, months: int = 12) -> dict:
        """
        Get historical traffic data for a domain.

        Requires SimilarWeb API key.

        Args:
            domain: Domain name
            months: Number of months of history

        Returns:
            Historical traffic data
        """
        domain = self._normalize_domain(domain)
        result = {
            "domain": domain,
            "period_months": months,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
        }

        if not self.similarweb_key:
            result["error"] = "Traffic history requires SimilarWeb API key"
            result["history"] = []
            return result

        try:
            history = self._get_similarweb_history(domain, months)
            result.update(history)
        except Exception as e:
            result["error"] = str(e)
            result["history"] = []

        return result

    def compare_domains(self, domains: list[str]) -> dict:
        """
        Compare traffic across multiple domains.

        Args:
            domains: List of domain names to compare

        Returns:
            Comparison data
        """
        domains = [self._normalize_domain(d) for d in domains]
        result = {
            "domains": domains,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
            "comparison": [],
        }

        for domain in domains:
            traffic = self.get_domain_traffic(domain)
            comparison_entry = {
                "domain": domain,
                "tranco_rank": traffic.get("tranco_rank"),
            }

            # Add SimilarWeb data if available
            if "metrics" in traffic:
                comparison_entry["monthly_visits"] = traffic["metrics"].get("monthly_visits")
                comparison_entry["global_rank"] = traffic["metrics"].get("global_rank")

            result["comparison"].append(comparison_entry)

        # Sort by rank (Tranco or SimilarWeb)
        result["comparison"].sort(
            key=lambda x: x.get("tranco_rank") or x.get("global_rank") or float("inf")
        )

        return result

    def get_rankings(self, limit: int = 100, offset: int = 0) -> dict:
        """
        Get top domain rankings from Tranco.

        Args:
            limit: Number of domains (max 1000)
            offset: Starting position

        Returns:
            Rankings data
        """
        rankings = self.tranco.get_top_domains(limit=limit, offset=offset)
        stats = self.tranco.get_stats()

        return {
            "provider": "tranco",
            "date": stats["list_date"],
            "rankings": rankings,
            "total_domains": stats["total_domains"],
            "offset": offset,
            "limit": limit,
        }

    def search_domains(self, keyword: str, limit: int = 50) -> dict:
        """
        Search domains by keyword.

        Args:
            keyword: Keyword to search for
            limit: Maximum results

        Returns:
            Search results
        """
        results = self.tranco.search_domains(keyword=keyword, limit=limit)

        return {
            "provider": "tranco",
            "keyword": keyword,
            "results": results,
            "count": len(results),
        }

    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain name."""
        domain = domain.lower().strip()
        # Remove protocol
        if domain.startswith("http://"):
            domain = domain[7:]
        elif domain.startswith("https://"):
            domain = domain[8:]
        # Remove path
        domain = domain.split("/")[0]
        # Remove www prefix for consistency
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    def _get_similarweb_traffic(self, domain: str) -> Optional[dict]:
        """Get traffic data from SimilarWeb API."""
        if not self.similarweb_key:
            return None

        try:
            # Get total traffic
            visits_url = f"{self.SIMILARWEB_BASE_URL}/website/{domain}/total-traffic-and-engagement/visits"
            params = {"api_key": self.similarweb_key, "granularity": "monthly", "main_domain_only": "false"}

            response = self.http_client.get(visits_url, params=params)
            response.raise_for_status()
            visits_data = response.json()

            # Get traffic sources
            sources_url = f"{self.SIMILARWEB_BASE_URL}/website/{domain}/traffic-sources/overview-share"
            sources_response = self.http_client.get(sources_url, params=params)
            sources_data = sources_response.json() if sources_response.status_code == 200 else {}

            # Get geography
            geo_url = f"{self.SIMILARWEB_BASE_URL}/website/{domain}/geo/traffic-by-country"
            geo_response = self.http_client.get(geo_url, params=params)
            geo_data = geo_response.json() if geo_response.status_code == 200 else {}

            # Parse and return
            latest_visits = visits_data.get("visits", [{}])[-1] if visits_data.get("visits") else {}

            return {
                "metrics": {
                    "monthly_visits": latest_visits.get("visits"),
                    "global_rank": visits_data.get("global_rank"),
                    "category_rank": visits_data.get("category_rank"),
                    "category": visits_data.get("category"),
                },
                "traffic_sources": sources_data.get("overview", {}),
                "geography": {
                    item.get("country"): item.get("share")
                    for item in geo_data.get("records", [])[:10]
                },
            }
        except Exception:
            return None

    def _get_similarweb_history(self, domain: str, months: int) -> dict:
        """Get historical traffic from SimilarWeb."""
        if not self.similarweb_key:
            return {"history": [], "error": "SimilarWeb API key required"}

        try:
            url = f"{self.SIMILARWEB_BASE_URL}/website/{domain}/total-traffic-and-engagement/visits"
            params = {
                "api_key": self.similarweb_key,
                "granularity": "monthly",
                "main_domain_only": "false",
            }

            response = self.http_client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            visits = data.get("visits", [])[-months:]

            history = [
                {
                    "month": v.get("date"),
                    "visits": v.get("visits"),
                }
                for v in visits
            ]

            # Calculate growth rate
            growth_rate = None
            if len(history) >= 2 and history[0]["visits"] and history[-1]["visits"]:
                growth_rate = (history[-1]["visits"] - history[0]["visits"]) / history[0]["visits"]

            return {
                "history": history,
                "growth_rate": growth_rate,
                "provider": "similarweb",
            }
        except Exception as e:
            return {"history": [], "error": str(e)}

    def close(self):
        """Close HTTP clients."""
        self.tranco.close()
        self.http_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
