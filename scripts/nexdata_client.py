"""
Nexdata API Client for Claude Code

A lightweight Python client for querying the Nexdata API.
Designed for use by Claude Code to answer natural language questions.

Usage:
    from scripts.nexdata_client import NexdataClient

    client = NexdataClient()
    investors = client.search_investors("Sequoia")
    portfolio = client.get_portfolio(investors[0]["id"])
"""

import httpx
from typing import Any, Dict, List, Optional
from dataclasses import dataclass


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_BASE_URL = "http://localhost:8001/api/v1"
DEFAULT_TIMEOUT = 30.0


# =============================================================================
# CLIENT CLASS
# =============================================================================

class NexdataClient:
    """
    Nexdata API client for natural language queries.

    Covers the most useful endpoints for:
    - Investor/LP lookups and portfolios
    - Company data and health scores
    - Deal pipeline and predictions
    - Analytics and trends
    - Data enrichment
    """

    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: float = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make GET request and return JSON."""
        url = f"{self.base_url}{path}"
        response = self._client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, json: Optional[Dict] = None) -> Dict[str, Any]:
        """Make POST request and return JSON."""
        url = f"{self.base_url}{path}"
        response = self._client.post(url, json=json)
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # SEARCH
    # -------------------------------------------------------------------------

    def search(self, query: str, type: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
        """
        Search across investors and companies.

        Args:
            query: Search term (name, keyword)
            type: Filter by type - "investor", "company", or None for all
            limit: Max results (default 20)

        Returns:
            Search results with investors and/or companies

        Example:
            results = client.search("Sequoia")
            results = client.search("fintech", type="company")
        """
        params = {"q": query, "limit": limit}
        if type:
            params["type"] = type
        return self._get("/search", params)

    def search_investors(self, query: str, limit: int = 20) -> List[Dict]:
        """Search for investors by name. Returns list of investor matches."""
        result = self.search(query, type="investor", limit=limit)
        return result.get("results", [])

    def search_companies(self, query: str, limit: int = 20) -> List[Dict]:
        """Search for companies by name. Returns list of company matches."""
        result = self.search(query, type="company", limit=limit)
        return result.get("results", [])

    # -------------------------------------------------------------------------
    # INVESTORS
    # -------------------------------------------------------------------------

    def get_investor(self, investor_id: int) -> Dict[str, Any]:
        """
        Get investor details by ID.

        Returns investor profile with name, type, AUM, location, etc.
        """
        return self._get(f"/investors/{investor_id}")

    def get_portfolio(self, investor_id: int) -> Dict[str, Any]:
        """
        Get investor's portfolio companies.

        Returns list of companies the investor has invested in,
        with sectors, investment dates, and amounts where available.
        """
        return self._get(f"/investors/{investor_id}/portfolio")

    def get_similar_investors(self, investor_id: int, limit: int = 10) -> Dict[str, Any]:
        """
        Find investors similar to this one based on portfolio overlap.

        Returns similar investors with similarity scores (Jaccard index).
        """
        return self._get(f"/discover/similar/{investor_id}", {"limit": limit})

    def compare_investors(self, investor_id_1: int, investor_id_2: int) -> Dict[str, Any]:
        """
        Compare two investor portfolios side-by-side.

        Returns:
            - Overlap count and percentage
            - Shared companies
            - Unique holdings for each
            - Sector allocation comparison
        """
        return self._post("/compare/portfolios", {
            "investor_ids": [investor_id_1, investor_id_2]
        })

    # -------------------------------------------------------------------------
    # COMPANIES
    # -------------------------------------------------------------------------

    def get_company(self, company_id: int) -> Dict[str, Any]:
        """Get company details by ID."""
        return self._get(f"/companies/{company_id}")

    def get_company_score(self, company_name: str) -> Dict[str, Any]:
        """
        Get company health score (0-100).

        Returns:
            - composite_score: Overall score 0-100
            - tier: A/B/C/D/F rating
            - category_scores: growth, stability, market, tech
            - confidence: Score reliability

        Example:
            score = client.get_company_score("Stripe")
            print(f"Stripe health: {score['composite_score']}/100 ({score['tier']} tier)")
        """
        return self._get(f"/scores/company/{company_name}")

    def get_company_enrichment(self, company_name: str) -> Dict[str, Any]:
        """
        Get enriched company data from multiple sources.

        Returns SEC financials, funding data, employee counts,
        industry classification, and company status.
        """
        return self._get(f"/enrichment/companies/{company_name}")

    def get_company_news(self, company_name: str, limit: int = 10) -> Dict[str, Any]:
        """
        Get recent news for a company.

        Returns news articles, SEC filings, and press releases.
        """
        return self._get(f"/news/company/{company_name}", {"limit": limit})

    # -------------------------------------------------------------------------
    # DEALS & PREDICTIONS
    # -------------------------------------------------------------------------

    def get_deals(
        self,
        stage: Optional[str] = None,
        sector: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get deals from the pipeline.

        Args:
            stage: Filter by stage - sourced, reviewing, due_diligence, negotiation
            sector: Filter by sector
            limit: Max results

        Returns list of deals with stage, priority, and company info.
        """
        params = {"limit": limit}
        if stage:
            params["pipeline_stage"] = stage
        if sector:
            params["sector"] = sector
        return self._get("/deals", params)

    def get_deal(self, deal_id: int) -> Dict[str, Any]:
        """Get single deal details."""
        return self._get(f"/deals/{deal_id}")

    def get_deal_prediction(self, deal_id: int) -> Dict[str, Any]:
        """
        Get win probability prediction for a deal.

        Returns:
            - win_probability: 0-1 probability of closing
            - confidence: high/medium/low
            - tier: A/B/C/D/F
            - scores: category breakdown (company, deal, pipeline, pattern)
            - strengths: Positive signals
            - risks: Warning signs
            - recommendations: Suggested actions

        Example:
            pred = client.get_deal_prediction(123)
            print(f"Win probability: {pred['win_probability']*100:.0f}%")
        """
        return self._get(f"/predictions/deal/{deal_id}")

    def get_pipeline_insights(self) -> Dict[str, Any]:
        """
        Get aggregate pipeline insights.

        Returns:
            - pipeline_health: Total deals, value, expected wins
            - stage_analysis: Deals per stage with avg probability
            - risk_alerts: Stalled or at-risk deals
            - opportunities: High-probability deals ready to close
            - sector_performance: Win rates by sector
        """
        return self._get("/predictions/insights")

    def get_scored_pipeline(self, min_probability: float = 0.0, limit: int = 50) -> Dict[str, Any]:
        """
        Get pipeline deals sorted by win probability.

        Args:
            min_probability: Filter deals below this probability (0-1)
            limit: Max results

        Returns deals ranked by likelihood to close.
        """
        return self._get("/predictions/pipeline", {
            "min_probability": min_probability,
            "limit": limit
        })

    # -------------------------------------------------------------------------
    # ANALYTICS & TRENDS
    # -------------------------------------------------------------------------

    def get_sector_trends(self) -> Dict[str, Any]:
        """
        Get sector allocation trends over time.

        Shows how investment allocation across sectors is changing.
        """
        return self._get("/trends/sectors")

    def get_emerging_sectors(self) -> Dict[str, Any]:
        """
        Get hot/emerging sectors with momentum.

        Identifies sectors seeing accelerating investment.
        """
        return self._get("/trends/emerging")

    def get_industry_breakdown(self) -> Dict[str, Any]:
        """
        Get aggregate industry distribution across all portfolios.
        """
        return self._get("/analytics/industry-breakdown")

    def get_analytics_overview(self) -> Dict[str, Any]:
        """
        Get system-wide analytics overview.

        Returns total investors, companies, recent activity, data quality.
        """
        return self._get("/analytics/overview")

    # -------------------------------------------------------------------------
    # DATA SOURCES
    # -------------------------------------------------------------------------

    def get_github_metrics(self, org: str) -> Dict[str, Any]:
        """
        Get GitHub activity metrics for an organization.

        Returns:
            - velocity_score: Developer velocity 0-100
            - repos: Repository count, stars, forks
            - activity: Commit frequency, contributor count

        Example:
            github = client.get_github_metrics("stripe")
            print(f"Stripe dev velocity: {github['velocity_score']}/100")
        """
        return self._get(f"/github/org/{org}")

    def get_glassdoor_data(self, company_name: str) -> Dict[str, Any]:
        """
        Get Glassdoor ratings and reviews for a company.

        Returns:
            - overall_rating: 1-5 stars
            - ceo_approval: CEO approval percentage
            - recommend_to_friend: Would recommend percentage
            - category_ratings: Work-life, compensation, culture, etc.
        """
        return self._get(f"/glassdoor/company/{company_name}")

    def get_web_traffic(self, domain: str) -> Dict[str, Any]:
        """
        Get web traffic data for a domain.

        Returns Tranco ranking and traffic estimates.
        """
        return self._get(f"/web-traffic/domain/{domain}")

    def get_sec_form_d(self, company_name: str) -> Dict[str, Any]:
        """
        Search SEC Form D filings (private placements).

        Returns private fundraising filings with amounts and investor types.
        """
        return self._get("/form-d/search", {"issuer_name": company_name})

    def get_form_adv_adviser(self, name: str) -> Dict[str, Any]:
        """
        Search SEC Form ADV investment advisers.

        Returns AUM, client types, fee structures for registered advisers.
        """
        return self._get("/form-adv/search", {"name": name})

    # -------------------------------------------------------------------------
    # RESEARCH (Agentic)
    # -------------------------------------------------------------------------

    def research_company(self, company_name: str) -> Dict[str, Any]:
        """
        Start autonomous research job for a company.

        The agent queries all data sources (SEC, GitHub, Glassdoor,
        App Store, web traffic, news) and synthesizes findings.

        Returns job_id to check status.
        """
        return self._post("/agents/research/company", {"company_name": company_name})

    def get_research_result(self, company_name: str) -> Dict[str, Any]:
        """
        Get cached research results for a company.

        Returns synthesized profile from all data sources.
        """
        return self._get(f"/agents/research/company/{company_name}")


# =============================================================================
# CONVENIENCE FUNCTIONS (for quick use without instantiating client)
# =============================================================================

_default_client: Optional[NexdataClient] = None

def _get_client() -> NexdataClient:
    global _default_client
    if _default_client is None:
        _default_client = NexdataClient()
    return _default_client


def search_investors(query: str, limit: int = 20) -> List[Dict]:
    """Search for investors by name."""
    return _get_client().search_investors(query, limit)


def search_companies(query: str, limit: int = 20) -> List[Dict]:
    """Search for companies by name."""
    return _get_client().search_companies(query, limit)


def get_portfolio(investor_id: int) -> Dict[str, Any]:
    """Get investor's portfolio companies."""
    return _get_client().get_portfolio(investor_id)


def get_company_score(company_name: str) -> Dict[str, Any]:
    """Get company health score (0-100)."""
    return _get_client().get_company_score(company_name)


def get_deal_prediction(deal_id: int) -> Dict[str, Any]:
    """Get win probability prediction for a deal."""
    return _get_client().get_deal_prediction(deal_id)


def get_pipeline_insights() -> Dict[str, Any]:
    """Get aggregate pipeline insights."""
    return _get_client().get_pipeline_insights()


def compare_investors(id1: int, id2: int) -> Dict[str, Any]:
    """Compare two investor portfolios."""
    return _get_client().compare_investors(id1, id2)


def get_github_metrics(org: str) -> Dict[str, Any]:
    """Get GitHub activity metrics for an organization."""
    return _get_client().get_github_metrics(org)


def get_glassdoor_data(company_name: str) -> Dict[str, Any]:
    """Get Glassdoor ratings for a company."""
    return _get_client().get_glassdoor_data(company_name)


def research_company(company_name: str) -> Dict[str, Any]:
    """Start autonomous research job for a company."""
    return _get_client().research_company(company_name)


# =============================================================================
# MAIN (for testing)
# =============================================================================

if __name__ == "__main__":
    # Quick test
    client = NexdataClient()

    print("Testing Nexdata client...")

    # Test search
    print("\n1. Searching for investors...")
    investors = client.search_investors("Capital", limit=3)
    print(f"   Found {len(investors)} investors")

    # Test analytics
    print("\n2. Getting analytics overview...")
    overview = client.get_analytics_overview()
    print(f"   Total investors: {overview.get('total_investors', 'N/A')}")

    # Test pipeline
    print("\n3. Getting pipeline insights...")
    insights = client.get_pipeline_insights()
    health = insights.get("pipeline_health", {})
    print(f"   Active deals: {health.get('total_active_deals', 'N/A')}")

    print("\nClient working!")
