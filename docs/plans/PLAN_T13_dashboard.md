# PLAN T13: Dashboard Analytics API

## Overview
**Task:** T13
**Tab:** 1
**Feature:** Pre-computed analytics for frontend dashboards
**Status:** COMPLETE

---

## Goal
Provide comprehensive analytics endpoints that power frontend dashboards with portfolio insights, trends, data quality metrics, and system health statistics. All endpoints are read-only aggregations over existing data.

---

## Files to Create

| File | Description |
|------|-------------|
| `app/analytics/__init__.py` | Package init, exports |
| `app/analytics/dashboard.py` | Analytics computation engine |
| `app/api/v1/analytics.py` | FastAPI router with endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Add `analytics` import and register router |

---

## Endpoint Specifications

### 1. GET `/api/v1/analytics/overview`
**Purpose:** System-wide statistics for main dashboard header/summary cards.

**Query Parameters:** None

**Response Model:**
```python
class SystemOverviewResponse(BaseModel):
    # Investor coverage
    total_lps: int
    total_family_offices: int
    lps_with_portfolio_data: int
    fos_with_portfolio_data: int
    coverage_percentage: float  # (investors with data / total) * 100

    # Portfolio data
    total_portfolio_companies: int
    unique_companies: int  # Deduplicated across investors
    total_market_value_usd: Optional[float]

    # Data sources breakdown
    companies_by_source: Dict[str, int]  # {"sec_13f": 500, "website": 200, ...}

    # Collection activity (last 24h, 7d, 30d)
    collection_stats: CollectionStats

    # Alert stats
    alert_stats: AlertStats

    # Data freshness
    last_collection_at: Optional[datetime]
    avg_data_age_days: float

class CollectionStats(BaseModel):
    jobs_last_24h: int
    jobs_last_7d: int
    jobs_last_30d: int
    success_rate_7d: float
    avg_companies_per_job: float
    total_companies_collected_7d: int

class AlertStats(BaseModel):
    pending_alerts: int
    alerts_triggered_today: int
    alerts_triggered_7d: int
    active_subscriptions: int
```

**SQL Queries:**
```sql
-- Investor counts
SELECT COUNT(*) FROM lp_fund;
SELECT COUNT(*) FROM family_offices;

-- Investors with portfolio data
SELECT COUNT(DISTINCT investor_id) FROM portfolio_companies WHERE investor_type = 'lp';
SELECT COUNT(DISTINCT investor_id) FROM portfolio_companies WHERE investor_type = 'family_office';

-- Portfolio totals
SELECT COUNT(*), COUNT(DISTINCT company_name), SUM(market_value_usd) FROM portfolio_companies;

-- Source breakdown
SELECT source_type, COUNT(*) FROM portfolio_companies GROUP BY source_type;

-- Collection stats (last 7 days)
SELECT
    COUNT(*) as total_jobs,
    COUNT(*) FILTER (WHERE status = 'success') as successful,
    AVG(companies_found) as avg_companies
FROM agentic_collection_jobs
WHERE created_at > NOW() - INTERVAL '7 days';

-- Alert stats
SELECT COUNT(*) FROM portfolio_alerts WHERE status = 'pending';
SELECT COUNT(*) FROM portfolio_alerts WHERE created_at > NOW() - INTERVAL '1 day';
SELECT COUNT(*) FROM alert_subscriptions WHERE is_active = TRUE;
```

---

### 2. GET `/api/v1/analytics/investor/{investor_id}`
**Purpose:** Detailed analytics for a single investor's portfolio.

**Path Parameters:**
- `investor_id`: int

**Query Parameters:**
- `investor_type`: str (required) - "lp" or "family_office"

**Response Model:**
```python
class InvestorAnalyticsResponse(BaseModel):
    investor_id: int
    investor_type: str
    investor_name: str

    # Portfolio summary
    portfolio_summary: PortfolioSummary

    # Industry distribution (for pie chart)
    industry_distribution: List[IndustryBreakdown]

    # Top holdings by value
    top_holdings: List[HoldingSummary]

    # Data quality
    data_quality: DataQualityScore

    # Collection history
    collection_history: List[CollectionEvent]

    # Growth trend (if historical data available)
    portfolio_trend: Optional[List[TrendPoint]]

class PortfolioSummary(BaseModel):
    total_companies: int
    total_market_value_usd: Optional[float]
    sources_used: List[str]
    last_updated: Optional[datetime]
    data_age_days: int

class IndustryBreakdown(BaseModel):
    industry: str
    company_count: int
    percentage: float
    total_value_usd: Optional[float]

class HoldingSummary(BaseModel):
    company_name: str
    industry: Optional[str]
    market_value_usd: Optional[float]
    shares_held: Optional[int]
    source_type: str
    confidence_level: Optional[float]

class DataQualityScore(BaseModel):
    overall_score: int  # 0-100
    completeness: int   # % of fields populated
    freshness: int      # Based on data age
    source_diversity: int  # Multiple sources = higher
    confidence_avg: float
    issues: List[str]   # ["Missing industry for 5 companies", ...]

class CollectionEvent(BaseModel):
    job_id: int
    date: datetime
    status: str
    companies_found: int
    strategies_used: List[str]

class TrendPoint(BaseModel):
    date: str  # YYYY-MM-DD
    company_count: int
    total_value_usd: Optional[float]
```

**SQL Queries:**
```sql
-- Portfolio summary
SELECT
    COUNT(*) as total_companies,
    SUM(market_value_usd) as total_value,
    array_agg(DISTINCT source_type) as sources,
    MAX(collected_date) as last_updated
FROM portfolio_companies
WHERE investor_id = :id AND investor_type = :type;

-- Industry distribution
SELECT
    COALESCE(company_industry, 'Unknown') as industry,
    COUNT(*) as count,
    SUM(market_value_usd) as value
FROM portfolio_companies
WHERE investor_id = :id AND investor_type = :type
GROUP BY company_industry
ORDER BY count DESC;

-- Top holdings
SELECT company_name, company_industry, market_value_usd, shares_held,
       source_type, confidence_level
FROM portfolio_companies
WHERE investor_id = :id AND investor_type = :type
ORDER BY market_value_usd DESC NULLS LAST
LIMIT 10;

-- Collection history
SELECT id, created_at, status, companies_found, strategies_used
FROM agentic_collection_jobs
WHERE target_investor_id = :id AND target_investor_type = :type
ORDER BY created_at DESC
LIMIT 10;

-- Data quality metrics
SELECT
    COUNT(*) as total,
    COUNT(company_industry) as has_industry,
    COUNT(market_value_usd) as has_value,
    AVG(confidence_level) as avg_confidence,
    COUNT(DISTINCT source_type) as source_count
FROM portfolio_companies
WHERE investor_id = :id AND investor_type = :type;
```

---

### 3. GET `/api/v1/analytics/trends`
**Purpose:** Time-series data for charts showing system activity over time.

**Query Parameters:**
- `period`: str - "7d", "30d", "90d" (default: "30d")
- `metric`: str - "collections", "companies", "alerts" (default: "collections")

**Response Model:**
```python
class TrendsResponse(BaseModel):
    period: str
    metric: str
    data_points: List[TrendDataPoint]
    summary: TrendSummary

class TrendDataPoint(BaseModel):
    date: str  # YYYY-MM-DD
    value: int
    details: Optional[Dict]  # Additional context

class TrendSummary(BaseModel):
    total: int
    average: float
    min: int
    max: int
    trend_direction: str  # "up", "down", "stable"
    change_percentage: float  # vs previous period
```

**SQL Queries:**
```sql
-- Collection jobs per day (last 30 days)
SELECT
    DATE(created_at) as date,
    COUNT(*) as job_count,
    COUNT(*) FILTER (WHERE status = 'success') as successful,
    SUM(companies_found) as companies_found
FROM agentic_collection_jobs
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date;

-- New portfolio companies per day
SELECT
    DATE(collected_date) as date,
    COUNT(*) as new_companies
FROM portfolio_companies
WHERE collected_date > NOW() - INTERVAL '30 days'
GROUP BY DATE(collected_date)
ORDER BY date;

-- Alerts triggered per day
SELECT
    DATE(created_at) as date,
    COUNT(*) as alert_count
FROM portfolio_alerts
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date;
```

---

### 4. GET `/api/v1/analytics/top-movers`
**Purpose:** Recent significant portfolio changes for activity feed/alerts widget.

**Query Parameters:**
- `limit`: int (default: 20, max: 100)
- `change_type`: Optional[str] - "new_holding", "removed_holding", "value_change"

**Response Model:**
```python
class TopMoversResponse(BaseModel):
    movers: List[PortfolioMover]
    generated_at: datetime

class PortfolioMover(BaseModel):
    investor_id: int
    investor_type: str
    investor_name: str
    change_type: str
    company_name: str
    details: Dict  # {old_value, new_value, change_pct} or {market_value, shares}
    detected_at: datetime
```

**SQL Queries:**
```sql
-- Recent alerts as movers (uses T11 alerts table)
SELECT
    a.investor_id, a.investor_type, a.investor_name,
    a.change_type, a.company_name, a.details, a.created_at
FROM portfolio_alerts a
WHERE a.created_at > NOW() - INTERVAL '7 days'
ORDER BY a.created_at DESC
LIMIT :limit;

-- Alternative: Recent new holdings
SELECT
    pc.investor_id, pc.investor_type,
    CASE WHEN pc.investor_type = 'lp' THEN lp.name ELSE fo.name END as investor_name,
    'new_holding' as change_type,
    pc.company_name,
    json_build_object('market_value_usd', pc.market_value_usd, 'source', pc.source_type) as details,
    pc.collected_date
FROM portfolio_companies pc
LEFT JOIN lp_fund lp ON pc.investor_type = 'lp' AND pc.investor_id = lp.id
LEFT JOIN family_offices fo ON pc.investor_type = 'family_office' AND pc.investor_id = fo.id
WHERE pc.collected_date > NOW() - INTERVAL '7 days'
ORDER BY pc.collected_date DESC
LIMIT :limit;
```

---

### 5. GET `/api/v1/analytics/industry-breakdown`
**Purpose:** Aggregate industry distribution across all portfolios.

**Query Parameters:**
- `investor_type`: Optional[str] - Filter by "lp" or "family_office"
- `limit`: int (default: 20) - Top N industries

**Response Model:**
```python
class IndustryBreakdownResponse(BaseModel):
    total_companies: int
    industries: List[IndustryStats]
    other_count: int  # Companies in industries beyond top N

class IndustryStats(BaseModel):
    industry: str
    company_count: int
    percentage: float
    investor_count: int  # How many investors hold companies in this industry
    total_value_usd: Optional[float]
    top_companies: List[str]  # Top 3 company names
```

**SQL Queries:**
```sql
-- Industry breakdown
SELECT
    COALESCE(company_industry, 'Unknown') as industry,
    COUNT(*) as company_count,
    COUNT(DISTINCT investor_id || '-' || investor_type) as investor_count,
    SUM(market_value_usd) as total_value,
    array_agg(DISTINCT company_name ORDER BY company_name) as companies
FROM portfolio_companies
GROUP BY company_industry
ORDER BY company_count DESC
LIMIT :limit;
```

---

## Implementation Details

### Dashboard Analytics Engine (`app/analytics/dashboard.py`)

```python
class DashboardAnalytics:
    """
    Analytics computation engine for dashboard endpoints.

    Design principles:
    - All methods are read-only (no writes)
    - Queries are optimized with appropriate indexes
    - Results can be cached (future enhancement)
    - Graceful handling of missing data
    """

    def __init__(self, db: Session):
        self.db = db

    async def get_system_overview(self) -> dict:
        """Compute system-wide statistics."""
        ...

    async def get_investor_analytics(
        self,
        investor_id: int,
        investor_type: str
    ) -> dict:
        """Compute analytics for a single investor."""
        ...

    async def get_trends(
        self,
        period: str,
        metric: str
    ) -> dict:
        """Compute time-series trend data."""
        ...

    async def get_top_movers(
        self,
        limit: int,
        change_type: Optional[str]
    ) -> List[dict]:
        """Get recent significant portfolio changes."""
        ...

    async def get_industry_breakdown(
        self,
        investor_type: Optional[str],
        limit: int
    ) -> dict:
        """Compute aggregate industry distribution."""
        ...

    def _calculate_data_quality_score(
        self,
        total: int,
        has_industry: int,
        has_value: int,
        avg_confidence: float,
        source_count: int,
        data_age_days: int
    ) -> DataQualityScore:
        """
        Calculate data quality score (0-100).

        Scoring:
        - Completeness (40 points): % of fields populated
        - Freshness (25 points): Data age penalty
        - Source diversity (20 points): Multiple sources = higher
        - Confidence (15 points): Average confidence level
        """
        ...
```

---

## Data Quality Score Algorithm

```python
def calculate_quality_score(metrics: dict) -> int:
    """
    Calculate overall data quality score (0-100).

    Components:
    1. Completeness (40 points max):
       - Has industry: +15 points if > 80%
       - Has market value: +15 points if > 50%
       - Has confidence level: +10 points if > 70%

    2. Freshness (25 points max):
       - < 7 days: +25 points
       - 7-30 days: +15 points
       - 30-90 days: +5 points
       - > 90 days: 0 points

    3. Source Diversity (20 points max):
       - 3+ sources: +20 points
       - 2 sources: +12 points
       - 1 source: +5 points

    4. Confidence (15 points max):
       - avg_confidence * 15
    """
    score = 0
    issues = []

    # Completeness
    industry_pct = metrics['has_industry'] / metrics['total'] * 100
    if industry_pct >= 80:
        score += 15
    elif industry_pct >= 50:
        score += 10
    else:
        issues.append(f"Missing industry for {100-industry_pct:.0f}% of companies")

    # ... similar for other components

    return score, issues
```

---

## Testing Plan

### Manual Testing
```bash
# System overview
curl http://localhost:8001/api/v1/analytics/overview | python -m json.tool

# Investor analytics
curl "http://localhost:8001/api/v1/analytics/investor/1?investor_type=lp" | python -m json.tool

# Trends
curl "http://localhost:8001/api/v1/analytics/trends?period=30d&metric=collections" | python -m json.tool

# Top movers
curl "http://localhost:8001/api/v1/analytics/top-movers?limit=10" | python -m json.tool

# Industry breakdown
curl "http://localhost:8001/api/v1/analytics/industry-breakdown?limit=15" | python -m json.tool
```

### Edge Cases
- Investor with no portfolio data → Return empty/zero values gracefully
- No collection jobs → Return empty trends
- Missing fields → Handle NULLs in aggregations
- Large datasets → Ensure queries use indexes

---

## Dependencies

- No new packages required
- Uses existing tables: `portfolio_companies`, `agentic_collection_jobs`, `portfolio_alerts`, `alert_subscriptions`, `lp_fund`, `family_offices`

---

## Future Enhancements (Not in T13)

- Response caching with TTL (Redis)
- Pre-computed materialized views for heavy aggregations
- Async background computation for expensive analytics
- Export analytics to CSV/PDF

---

## Approval

- [ ] Approved by user
