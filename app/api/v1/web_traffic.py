"""
Web Traffic API endpoints.

Provides website traffic intelligence using multiple data providers.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List

from app.sources.web_traffic import WebTrafficClient

router = APIRouter(prefix="/web-traffic", tags=["web-traffic"])


def get_client() -> WebTrafficClient:
    """Get WebTrafficClient instance."""
    return WebTrafficClient()


@router.get("/domain/{domain}")
def get_domain_traffic(domain: str):
    """
    Get traffic overview for a domain.

    Returns Tranco ranking (free) and SimilarWeb traffic data (if API key configured).

    - **domain**: Domain name (e.g., 'google.com', 'stripe.com')

    Without SimilarWeb API key, only Tranco rank is returned.
    """
    try:
        with get_client() as client:
            result = client.get_domain_traffic(domain)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching traffic data: {str(e)}"
        )


@router.get("/domain/{domain}/history")
def get_traffic_history(
    domain: str,
    months: int = Query(12, ge=1, le=24, description="Number of months of history"),
):
    """
    Get historical traffic data for a domain.

    **Requires SimilarWeb API key** for traffic history.
    Without API key, returns empty history.

    - **domain**: Domain name
    - **months**: Number of months of history (1-24, default: 12)
    """
    try:
        with get_client() as client:
            result = client.get_traffic_history(domain, months=months)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching traffic history: {str(e)}"
        )


@router.get("/compare")
def compare_domains(
    domains: List[str] = Query(
        ..., min_length=2, max_length=10, description="Domains to compare"
    ),
):
    """
    Compare traffic across multiple domains.

    Returns side-by-side comparison with rankings and traffic metrics.

    - **domains**: List of domain names to compare (2-10 domains)

    Example: `/compare?domains=stripe.com&domains=square.com&domains=paypal.com`
    """
    if len(domains) < 2:
        raise HTTPException(
            status_code=400, detail="At least 2 domains required for comparison"
        )

    try:
        with get_client() as client:
            result = client.compare_domains(domains)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error comparing domains: {str(e)}"
        )


@router.get("/rankings")
def get_rankings(
    limit: int = Query(100, ge=1, le=1000, description="Number of domains to return"),
    offset: int = Query(0, ge=0, description="Starting position (0-based)"),
):
    """
    Get top domain rankings from Tranco list.

    Tranco combines data from multiple sources (Umbrella, Majestic, etc.)
    and is updated daily. Free to use, no API key required.

    - **limit**: Number of domains to return (max 1000)
    - **offset**: Starting position for pagination

    Example: `/rankings?limit=100&offset=0` for top 100 domains
    """
    try:
        with get_client() as client:
            result = client.get_rankings(limit=limit, offset=offset)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching rankings: {str(e)}"
        )


@router.get("/search")
def search_domains(
    keyword: str = Query(
        ..., min_length=2, description="Keyword to search for in domain names"
    ),
    limit: int = Query(50, ge=1, le=200, description="Maximum results to return"),
):
    """
    Search domains by keyword in Tranco list.

    Searches for domains containing the keyword.

    - **keyword**: Keyword to search for (min 2 characters)
    - **limit**: Maximum results (default: 50, max: 200)

    Example: `/search?keyword=shop&limit=20` for domains containing 'shop'
    """
    try:
        with get_client() as client:
            result = client.search_domains(keyword=keyword, limit=limit)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error searching domains: {str(e)}"
        )


@router.get("/providers")
def list_providers():
    """
    List available web traffic data providers.

    Shows which providers are configured and their capabilities.

    - **tranco**: Always available (free rankings)
    - **similarweb**: Requires SIMILARWEB_API_KEY environment variable
    """
    try:
        with get_client() as client:
            providers = client.get_available_providers()
            return {
                "providers": providers,
                "count": len(providers),
            }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error listing providers: {str(e)}"
        )
