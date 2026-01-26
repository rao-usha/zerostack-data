# Plan T35: Web Traffic Data

## Overview
**Task:** T35
**Tab:** 2
**Feature:** Website traffic intelligence for company performance analysis
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

Investment teams need web traffic data for:

1. **Growth Analysis**: Track website traffic trends as proxy for business growth
2. **Competitor Benchmarking**: Compare traffic across competing companies
3. **Market Position**: Understand domain rankings and market share
4. **Due Diligence**: Validate claimed user metrics during investment review
5. **Lead Generation**: Identify high-traffic companies in target sectors

### User Scenarios

#### Scenario 1: Company Research
**Analyst** researches a potential investment target.
- Query: "Get traffic data for stripe.com"
- Result: Monthly visits, traffic sources, geographic breakdown, rankings

#### Scenario 2: Competitive Analysis
**Sourcing Team** compares companies in a sector.
- Query: "Compare traffic for shopify.com, squarespace.com, wix.com"
- Result: Side-by-side traffic metrics, market share, growth rates

#### Scenario 3: Trend Monitoring
**Portfolio Manager** tracks portfolio company performance.
- Query: "Get 12-month traffic history for notion.so"
- Result: Monthly traffic trends, seasonality, growth trajectory

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | Domain traffic overview | Get visits, sources, geography for a domain |
| M2 | Traffic history | Historical traffic trends over time |
| M3 | Domain comparison | Compare multiple domains side-by-side |
| M4 | Domain rankings | Get global/category rankings |
| M5 | Multiple providers | Support SimilarWeb + free alternatives |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Technology detection | Identify tech stack (analytics, frameworks) |
| S2 | Caching | Cache responses to reduce API costs |
| S3 | Bulk lookup | Process multiple domains efficiently |

---

## Technical Design

### Data Sources

We'll support multiple web traffic data providers:

| Provider | Cost | Features | Rate Limits |
|----------|------|----------|-------------|
| **SimilarWeb** | Paid (Enterprise) | Full traffic data, sources, geography | Varies by plan |
| **Tranco List** | Free | Top 1M domain rankings | None |
| **BuiltWith** | Freemium | Technology detection | 50/day free |

### SimilarWeb API

If user has SimilarWeb API access, we'll use their comprehensive endpoints:

**Base URL:** `https://api.similarweb.com/v1`

**Authentication:** API key in header

**Endpoints:**
- `GET /website/{domain}/total-traffic-and-engagement/visits` - Monthly visits
- `GET /website/{domain}/traffic-sources/overview-share` - Traffic sources
- `GET /website/{domain}/geo/traffic-by-country` - Geographic breakdown
- `GET /website/{domain}/similar-sites/similarsites` - Similar sites

### Tranco List (Free Alternative)

Tranco is a research-oriented top sites ranking that combines multiple sources.

**URL:** `https://tranco-list.eu/`

**Features:**
- Top 1 million domains ranking
- Updated daily
- Free API access
- No rate limits

### Our API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/web-traffic/domain/{domain}` | Get traffic overview |
| GET | `/api/v1/web-traffic/domain/{domain}/history` | Get traffic history |
| GET | `/api/v1/web-traffic/compare` | Compare multiple domains |
| GET | `/api/v1/web-traffic/rankings` | Get top domains by category |
| GET | `/api/v1/web-traffic/search` | Search domains by keyword |
| GET | `/api/v1/web-traffic/providers` | List available data providers |

### Request/Response Models

**Domain Traffic Response:**
```json
{
  "domain": "stripe.com",
  "provider": "similarweb",
  "retrieved_at": "2026-01-18T12:00:00Z",
  "metrics": {
    "global_rank": 1234,
    "category_rank": 45,
    "category": "Finance > Payments",
    "monthly_visits": 45000000,
    "avg_visit_duration": 185.5,
    "pages_per_visit": 4.2,
    "bounce_rate": 0.35
  },
  "traffic_sources": {
    "direct": 0.45,
    "search": 0.30,
    "referral": 0.15,
    "social": 0.05,
    "paid": 0.03,
    "email": 0.02
  },
  "geography": {
    "US": 0.40,
    "GB": 0.12,
    "DE": 0.08,
    "CA": 0.06,
    "AU": 0.04
  }
}
```

**Traffic History Response:**
```json
{
  "domain": "stripe.com",
  "provider": "similarweb",
  "period": "2025-01 to 2026-01",
  "history": [
    {
      "month": "2025-01",
      "visits": 42000000,
      "global_rank": 1280
    },
    {
      "month": "2025-02",
      "visits": 43500000,
      "global_rank": 1265
    }
  ],
  "growth_rate": 0.07
}
```

**Domain Comparison Response:**
```json
{
  "domains": ["stripe.com", "square.com", "paypal.com"],
  "comparison": [
    {
      "domain": "stripe.com",
      "monthly_visits": 45000000,
      "global_rank": 1234,
      "market_share": 0.35
    },
    {
      "domain": "square.com",
      "monthly_visits": 28000000,
      "global_rank": 2100,
      "market_share": 0.22
    },
    {
      "domain": "paypal.com",
      "monthly_visits": 55000000,
      "global_rank": 890,
      "market_share": 0.43
    }
  ],
  "total_market_visits": 128000000
}
```

**Rankings Response (Tranco):**
```json
{
  "provider": "tranco",
  "date": "2026-01-18",
  "rankings": [
    {"rank": 1, "domain": "google.com"},
    {"rank": 2, "domain": "facebook.com"},
    {"rank": 3, "domain": "youtube.com"}
  ],
  "total_domains": 1000000
}
```

### Client Implementation

```python
class WebTrafficClient:
    """Client for web traffic data from multiple providers."""

    def __init__(self, similarweb_api_key: str = None):
        self.similarweb_key = similarweb_api_key or os.getenv("SIMILARWEB_API_KEY")
        self.tranco_cache = {}
        self.cache_ttl = 3600  # 1 hour

    def get_domain_traffic(self, domain: str, provider: str = "auto") -> dict:
        """Get traffic overview for a domain."""

    def get_traffic_history(
        self, domain: str, months: int = 12, provider: str = "auto"
    ) -> dict:
        """Get historical traffic data."""

    def compare_domains(self, domains: list[str]) -> dict:
        """Compare traffic across multiple domains."""

    def get_rankings(
        self, limit: int = 100, offset: int = 0, category: str = None
    ) -> dict:
        """Get top domain rankings from Tranco."""

    def search_domains(self, keyword: str, limit: int = 50) -> dict:
        """Search domains by keyword in Tranco list."""

    def _fetch_tranco_list(self) -> list:
        """Download and cache Tranco top 1M list."""

    def _query_similarweb(self, endpoint: str, params: dict) -> dict:
        """Query SimilarWeb API."""
```

### Tranco List Integration

The Tranco list provides free domain rankings:

```python
def _fetch_tranco_list(self) -> list:
    """Download Tranco top 1M domains list."""
    # Download latest list
    url = "https://tranco-list.eu/top-1m.csv.zip"
    # Parse CSV: rank,domain
    # Cache in memory with TTL
    return rankings

def get_domain_rank(self, domain: str) -> Optional[int]:
    """Get domain rank from Tranco list."""
    tranco = self._fetch_tranco_list()
    return tranco.get(domain)
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/sources/web_traffic/__init__.py` | Package init |
| `app/sources/web_traffic/client.py` | Multi-provider traffic client |
| `app/sources/web_traffic/tranco.py` | Tranco list integration |
| `app/api/v1/web_traffic.py` | 6 API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register web_traffic router |

---

## Implementation Steps

1. Create `app/sources/web_traffic/` directory structure
2. Implement `tranco.py` for free ranking data
3. Implement `client.py` with multi-provider support
4. Create `app/api/v1/web_traffic.py` with 6 endpoints
5. Register router in main.py
6. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| WT-001 | Get domain traffic | Returns traffic metrics |
| WT-002 | Get traffic history | Returns monthly history |
| WT-003 | Compare domains | Returns comparison data |
| WT-004 | Get rankings | Returns Tranco top domains |
| WT-005 | Search domains | Returns matching domains |
| WT-006 | List providers | Returns available providers |

### Test Commands

```bash
# Get domain traffic
curl -s "http://localhost:8001/api/v1/web-traffic/domain/google.com" \
  | python -m json.tool

# Get traffic history
curl -s "http://localhost:8001/api/v1/web-traffic/domain/stripe.com/history?months=6" \
  | python -m json.tool

# Compare domains
curl -s "http://localhost:8001/api/v1/web-traffic/compare?domains=stripe.com,square.com,paypal.com" \
  | python -m json.tool

# Get top rankings
curl -s "http://localhost:8001/api/v1/web-traffic/rankings?limit=100" \
  | python -m json.tool

# Search domains
curl -s "http://localhost:8001/api/v1/web-traffic/search?keyword=shop&limit=20" \
  | python -m json.tool

# List providers
curl -s "http://localhost:8001/api/v1/web-traffic/providers" \
  | python -m json.tool
```

---

## Environment Variables

```bash
SIMILARWEB_API_KEY=your_api_key_here  # Optional, enables full traffic data
```

---

## Notes

- **Tranco** provides free rankings for top 1M domains (updated daily)
- **SimilarWeb** requires paid API access for detailed traffic data
- Without SimilarWeb API key, only Tranco rankings are available
- Tranco list is cached for 24 hours to minimize downloads

---

## Approval

- [x] **Approved by user** (2026-01-18)

## Implementation Notes

- Created `app/sources/web_traffic/tranco.py` for free Tranco rankings (top 1M domains)
- Created `app/sources/web_traffic/client.py` with multi-provider support
- Created `app/api/v1/web_traffic.py` with 6 endpoints
- Tranco list is downloaded and cached for 24 hours
- SimilarWeb integration ready (requires API key)
- Fixed redirect handling for Tranco list download

---

*Plan created: 2026-01-18*
*Completed: 2026-01-18*
