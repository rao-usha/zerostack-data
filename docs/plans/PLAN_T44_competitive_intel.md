# Plan T44: Agentic Competitive Intel

**Task ID:** T44
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19)

---

## Goal

Build an AI-powered competitive intelligence agent that automatically identifies competitors, builds comparison matrices, tracks competitive movements, and generates moat assessments.

---

## Dependencies

- **T41 (Company Researcher)**: Provides `CompanyResearchAgent` for deep company profiles
- **T35 (Web Traffic)**: Provides traffic data for competitive comparison
- **T36 (Company Scoring)**: Provides health scores for comparison
- **T22 (Enrichment)**: Provides company data (sector, industry, employees, funding)

---

## Design

### Core Components

1. **Competitor Discovery**: Find competitors via multiple signals
2. **Comparison Matrix**: Build standardized metrics table
3. **Movement Tracking**: Track funding, hires, product launches
4. **Moat Assessment**: Analyze competitive advantages
5. **Caching**: Store analyses with TTL for efficiency

### Competitor Discovery Signals

| Signal | Weight | Source |
|--------|--------|--------|
| Same sector/industry | 0.30 | Enrichment data |
| Similar employee count (±50%) | 0.15 | Enrichment data |
| Similar funding stage | 0.20 | Form D, enrichment |
| Overlapping investors | 0.20 | Portfolio data |
| Similar tech stack (GitHub) | 0.15 | GitHub data |

### Comparison Matrix Metrics

| Metric | Source | Normalization |
|--------|--------|---------------|
| Health Score | company_scores | 0-100 |
| Growth Score | company_scores | 0-100 |
| Employee Count | enrichment | Log scale |
| Total Funding | enrichment/form_d | Log scale |
| Web Traffic Rank | web_traffic (Tranco) | Inverted log |
| GitHub Stars | github | Log scale |
| Glassdoor Rating | glassdoor | 1-5 |

### Moat Assessment Categories

1. **Network Effects**: User base, integrations, partnerships
2. **Switching Costs**: Enterprise lock-in, data migration complexity
3. **Brand Recognition**: Traffic rank, media mentions, awards
4. **Cost Advantages**: Funding, efficiency metrics
5. **Technology Lead**: GitHub activity, patents, tech talent

---

## Database Schema

```sql
-- Competitive analysis results
CREATE TABLE competitive_analyses (
    id SERIAL PRIMARY KEY,

    -- Target company
    company_name VARCHAR(255) NOT NULL,
    company_sector VARCHAR(100),

    -- Analysis results
    competitors JSONB,  -- List of identified competitors
    comparison_matrix JSONB,  -- Metrics comparison table
    moat_assessment JSONB,  -- Competitive advantages analysis
    market_position VARCHAR(20),  -- leader, challenger, follower, niche

    -- Metadata
    confidence FLOAT,
    data_sources JSONB,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,

    UNIQUE(company_name)
);

-- Competitive movements (changes over time)
CREATE TABLE competitive_movements (
    id SERIAL PRIMARY KEY,

    company_name VARCHAR(255) NOT NULL,
    movement_type VARCHAR(50) NOT NULL,  -- funding, hiring, product, partnership
    description TEXT,
    impact_score FLOAT,  -- 0-1 business impact
    source VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_movements_company (company_name),
    INDEX idx_movements_type (movement_type)
);
```

---

## API Endpoints

### 1. Start Analysis

**POST /api/v1/competitive/analyze**

```json
// Request
{
    "company_name": "Stripe",
    "max_competitors": 10,  // optional, default 10
    "include_movements": true  // optional, include recent changes
}

// Response
{
    "job_id": "comp_abc123",
    "company_name": "Stripe",
    "status": "running",
    "estimated_time_seconds": 30
}
```

### 2. Get Competitive Landscape

**GET /api/v1/competitive/{company}**

```json
// Response
{
    "company": "Stripe",
    "sector": "fintech",
    "market_position": "leader",
    "competitors": [
        {
            "name": "Square",
            "similarity_score": 0.85,
            "relationship": "direct",
            "strengths": ["mobile payments", "hardware"],
            "weaknesses": ["enterprise market"]
        },
        {
            "name": "Adyen",
            "similarity_score": 0.78,
            "relationship": "direct",
            "strengths": ["enterprise", "global reach"],
            "weaknesses": ["developer experience"]
        }
    ],
    "comparison_matrix": {
        "metrics": ["health_score", "employees", "funding", "traffic_rank"],
        "data": {
            "Stripe": [92, 8000, 8700000000, 1500],
            "Square": [85, 12000, 590000000, 2100],
            "Adyen": [88, 4500, 266000000, 15000]
        }
    },
    "moat_assessment": {
        "overall_moat": "strong",
        "scores": {
            "network_effects": 85,
            "switching_costs": 90,
            "brand": 88,
            "cost_advantages": 75,
            "technology": 92
        },
        "summary": "Stripe has a strong competitive moat driven by..."
    },
    "analyzed_at": "2026-01-19T12:00:00Z",
    "confidence": 0.85
}
```

### 3. Get Competitive Movements

**GET /api/v1/competitive/{company}/movements**

Query params:
- `days`: Time range (default: 30)
- `type`: Filter by movement type
- `include_competitors`: Include competitor movements (default: true)

```json
// Response
{
    "company": "Stripe",
    "movements": [
        {
            "company": "Stripe",
            "type": "funding",
            "description": "Raised $6.5B at $50B valuation",
            "impact_score": 0.9,
            "detected_at": "2026-01-15T10:00:00Z"
        },
        {
            "company": "Square",
            "type": "product",
            "description": "Launched new payment terminal",
            "impact_score": 0.6,
            "detected_at": "2026-01-10T14:00:00Z"
        }
    ],
    "competitor_activity_summary": {
        "funding_total": 150000000,
        "hires_announced": 3,
        "products_launched": 2
    }
}
```

### 4. Compare Specific Companies

**POST /api/v1/competitive/compare**

```json
// Request
{
    "companies": ["Stripe", "Square", "Adyen"]
}

// Response
{
    "companies": ["Stripe", "Square", "Adyen"],
    "comparison_matrix": {...},
    "rankings": {
        "overall": ["Stripe", "Adyen", "Square"],
        "by_metric": {
            "health_score": ["Stripe", "Adyen", "Square"],
            "growth": ["Stripe", "Square", "Adyen"],
            "market_reach": ["Square", "Stripe", "Adyen"]
        }
    }
}
```

---

## Implementation

### CompetitiveIntelAgent Class

```python
class CompetitiveIntelAgent:
    """Competitive intelligence analysis agent."""

    def __init__(self, db: Session):
        self.db = db
        self.researcher = CompanyResearchAgent(db)
        self.scorer = CompanyScorer(db)

    # Discovery
    def find_competitors(self, company_name: str, max_results: int = 10) -> List[Dict]
    def _score_similarity(self, company: Dict, candidate: Dict) -> float

    # Comparison
    def build_comparison_matrix(self, companies: List[str]) -> Dict
    def _get_company_metrics(self, company_name: str) -> Dict
    def _normalize_metrics(self, raw_metrics: Dict) -> Dict

    # Moat Analysis
    def assess_moat(self, company_name: str, competitors: List[str]) -> Dict
    def _score_network_effects(self, company: Dict) -> float
    def _score_switching_costs(self, company: Dict) -> float
    def _score_brand(self, company: Dict) -> float

    # Movements
    def detect_movements(self, company_name: str, days: int = 30) -> List[Dict]
    def track_competitor_movements(self, companies: List[str], days: int) -> List[Dict]

    # Main entry points
    def analyze(self, company_name: str, max_competitors: int = 10) -> Dict
    def get_cached_analysis(self, company_name: str) -> Optional[Dict]
```

### Competitor Discovery Logic

```python
def find_competitors(self, company_name: str, max_results: int = 10) -> List[Dict]:
    """Find competitors using multiple signals."""

    # 1. Get target company profile
    target = self._get_company_profile(company_name)
    if not target:
        return []

    # 2. Query candidates from same sector/industry
    candidates = self._query_sector_companies(
        sector=target.get("sector"),
        industry=target.get("industry"),
        limit=50
    )

    # 3. Score each candidate
    scored = []
    for candidate in candidates:
        if candidate["name"].lower() == company_name.lower():
            continue

        similarity = self._score_similarity(target, candidate)
        if similarity >= 0.3:  # Minimum threshold
            scored.append({
                "name": candidate["name"],
                "similarity_score": similarity,
                "signals": self._explain_similarity(target, candidate)
            })

    # 4. Sort and return top N
    scored.sort(key=lambda x: x["similarity_score"], reverse=True)
    return scored[:max_results]
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/agents/competitive_intel.py` | Core competitive analysis logic |
| `app/api/v1/competitive.py` | API endpoints |

---

## Test Plan

1. **Unit Tests**
   - Competitor discovery with various signals
   - Similarity scoring
   - Moat assessment calculations
   - Movement detection

2. **Integration Tests**
   - Full analysis pipeline
   - Comparison with known competitors
   - Caching behavior

3. **Manual Testing**
   ```bash
   # Start analysis
   curl -X POST http://localhost:8001/api/v1/competitive/analyze \
     -H "Content-Type: application/json" \
     -d '{"company_name": "Stripe"}'

   # Get landscape
   curl http://localhost:8001/api/v1/competitive/Stripe

   # Get movements
   curl http://localhost:8001/api/v1/competitive/Stripe/movements

   # Compare specific companies
   curl -X POST http://localhost:8001/api/v1/competitive/compare \
     -H "Content-Type: application/json" \
     -d '{"companies": ["Stripe", "Square", "Adyen"]}'
   ```

---

## Success Criteria

- [x] Competitor discovery returns relevant companies
- [x] Comparison matrix with standardized metrics
- [x] Moat assessment generates meaningful scores
- [x] Movement tracking detects recent changes
- [x] Analysis cached with appropriate TTL
- [x] All endpoints functional

