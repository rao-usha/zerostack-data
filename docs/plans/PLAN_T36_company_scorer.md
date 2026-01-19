# T36: Company Scoring Model

**Status:** [x] Approved - Implemented 2026-01-19
**Owner:** Tab 2
**Dependencies:** T22 (Company Data Enrichment) - COMPLETE

---

## Goal

Build an ML-based scoring model that quantifies portfolio company health into actionable 0-100 scores. The model aggregates signals from multiple data sources (T22 enrichment, T34 GitHub, T35 web traffic) into composite scores with category breakdowns and explainability.

---

## Data Sources

### From T22 Company Enrichment (`company_enrichment` table)
- `latest_revenue`, `latest_assets`, `latest_net_income` - Financial health
- `employee_count`, `employee_growth_yoy` - Team growth
- `total_funding`, `last_funding_amount`, `last_funding_date` - Funding momentum
- `company_status` - Active/acquired/IPO/bankrupt
- `confidence_score` - Data quality indicator

### From T34 GitHub (`github_organizations` table)
- `velocity_score` - Developer activity (0-100)
- `total_stars`, `total_forks` - Community engagement
- `total_contributors` - Team size proxy

### From T35 Web Traffic (`company_enrichment` or live lookup)
- Tranco rank - Market presence (lower = better)
- Traffic trends - Growth trajectory

---

## Scoring Model Design

### Category Scores (0-100 each)

| Category | Weight | Signals |
|----------|--------|---------|
| **Growth** | 30% | Employee growth YoY, funding recency, web traffic trend |
| **Stability** | 25% | Company age, revenue consistency, funding runway |
| **Market Position** | 25% | Web traffic rank, GitHub stars, industry presence |
| **Tech Velocity** | 20% | GitHub velocity score, contributor count, release frequency |

### Composite Score Formula
```
composite = (growth * 0.30) + (stability * 0.25) + (market * 0.25) + (tech * 0.20)
```

### Confidence Score
Based on data availability:
- Full enrichment data: 100%
- Missing GitHub: -20%
- Missing web traffic: -15%
- Missing financials: -25%
- Missing funding: -15%

### Score Tiers
| Score | Tier | Interpretation |
|-------|------|----------------|
| 80-100 | A | Strong performance across all metrics |
| 60-79 | B | Solid fundamentals, some areas for improvement |
| 40-59 | C | Average performance, mixed signals |
| 20-39 | D | Weak performance, concerns present |
| 0-19 | F | Critical issues, high risk |

---

## Files to Create

### `app/ml/__init__.py`
Module initialization.

### `app/ml/company_scorer.py`
Core scoring engine with:
- `CompanyScorer` class
- Feature extraction from multiple sources
- Category score calculation
- Composite score computation
- Score explanation generation
- Batch scoring support

### `app/api/v1/scores.py`
FastAPI router with endpoints:
- `GET /api/v1/scores/company/{name}` - Single company score
- `GET /api/v1/scores/portfolio/{investor_id}` - Portfolio aggregate scores
- `GET /api/v1/scores/rankings` - Top/bottom scored companies
- `GET /api/v1/scores/methodology` - Scoring methodology docs

### Database Table: `company_scores`
```sql
CREATE TABLE company_scores (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    composite_score FLOAT NOT NULL,
    growth_score FLOAT,
    stability_score FLOAT,
    market_score FLOAT,
    tech_score FLOAT,
    confidence FLOAT,
    tier VARCHAR(1),
    explanation JSONB,
    data_sources JSONB,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_version VARCHAR(20) DEFAULT 'v1.0',
    UNIQUE(company_name, model_version)
);
```

---

## API Endpoints

### 1. `GET /api/v1/scores/company/{name}`
Get score for a single company.

**Response:**
```json
{
  "company_name": "Stripe",
  "composite_score": 85.2,
  "tier": "A",
  "category_scores": {
    "growth": 92.0,
    "stability": 78.5,
    "market_position": 88.0,
    "tech_velocity": 82.0
  },
  "confidence": 0.85,
  "explanation": {
    "top_strengths": ["Strong employee growth", "High web traffic rank"],
    "areas_for_improvement": ["No recent funding data"],
    "data_sources_used": ["enrichment", "github", "tranco"]
  },
  "scored_at": "2026-01-19T10:30:00Z",
  "model_version": "v1.0"
}
```

### 2. `GET /api/v1/scores/portfolio/{investor_id}`
Get scores for all companies in an investor's portfolio.

**Query Params:**
- `min_score`: Filter by minimum score
- `tier`: Filter by tier (A, B, C, D, F)

**Response:**
```json
{
  "investor_id": 123,
  "investor_name": "Sequoia Capital",
  "portfolio_summary": {
    "total_companies": 45,
    "scored_companies": 38,
    "average_score": 72.4,
    "tier_distribution": {"A": 8, "B": 15, "C": 12, "D": 3, "F": 0}
  },
  "companies": [
    {"company_name": "Stripe", "score": 85.2, "tier": "A"},
    {"company_name": "Notion", "score": 78.5, "tier": "B"}
  ]
}
```

### 3. `GET /api/v1/scores/rankings`
Get top/bottom scored companies.

**Query Params:**
- `order`: "top" (default) or "bottom"
- `limit`: Number of results (default 20)
- `sector`: Filter by sector
- `min_confidence`: Minimum confidence threshold

**Response:**
```json
{
  "order": "top",
  "limit": 20,
  "rankings": [
    {"rank": 1, "company_name": "Stripe", "score": 85.2, "tier": "A", "sector": "Fintech"},
    {"rank": 2, "company_name": "OpenAI", "score": 83.7, "tier": "A", "sector": "Technology"}
  ]
}
```

### 4. `GET /api/v1/scores/methodology`
Return scoring methodology documentation.

**Response:**
```json
{
  "model_version": "v1.0",
  "last_updated": "2026-01-19",
  "categories": [
    {
      "name": "growth",
      "weight": 0.30,
      "description": "Measures company growth trajectory",
      "signals": ["employee_growth_yoy", "funding_recency", "traffic_trend"]
    }
  ],
  "tier_definitions": {...},
  "confidence_calculation": {...}
}
```

---

## Implementation Notes

1. **No ML Libraries Required**: Use weighted scoring formula (no sklearn/pytorch needed). This keeps deployment simple.

2. **Graceful Degradation**: Score companies even with partial data, but reduce confidence appropriately.

3. **Caching**: Store computed scores in `company_scores` table. Recompute on:
   - Explicit refresh request
   - Score older than 7 days
   - Underlying data updated

4. **Explainability**: Always provide human-readable explanation of score drivers.

5. **Version Control**: Track model version to support A/B testing and rollbacks.

---

## Test Plan

1. **Unit Tests**:
   - Score calculation with full data
   - Score calculation with partial data
   - Confidence calculation
   - Tier assignment

2. **Integration Tests**:
   - Score company with enrichment data
   - Score portfolio with multiple companies
   - Rankings endpoint with filters

3. **Manual Testing**:
   ```bash
   # Score a company
   curl http://localhost:8001/api/v1/scores/company/Stripe

   # Portfolio scores
   curl http://localhost:8001/api/v1/scores/portfolio/1

   # Top companies
   curl http://localhost:8001/api/v1/scores/rankings?limit=10

   # Methodology
   curl http://localhost:8001/api/v1/scores/methodology
   ```

---

## Approval

- [ ] User approves plan
- [ ] Ready to implement
